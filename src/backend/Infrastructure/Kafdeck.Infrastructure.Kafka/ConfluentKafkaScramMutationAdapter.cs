using System.Globalization;
using System.Security.Cryptography;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Kafka;

/// <summary>
/// Typed W43 SCRAM write adapter. It accepts one exact user/mechanism effect
/// and a separate request-scoped password buffer. No generic AdminClient
/// surface escapes this boundary.
/// </summary>
public sealed class ConfluentKafkaScramMutationAdapter :
    IScramMutationPort,
    IDisposable
{
    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeProvider _timeProvider;

    public ConfluentKafkaScramMutationAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
    {
        _clients = new KafkaAdminClientRegistry(
            clusterProfiles ?? throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ?? throw new ArgumentNullException(nameof(secretResolver)));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationProviderResult> UpsertAsync(
        ScramUpsertMutation request,
        ReadOnlyMemory<byte> password,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        ScramCredentialBindingDescriptor descriptor;
        try
        {
            descriptor = ScramCredentialMaterialBinding.Normalize(
                new ScramCredentialBindingDescriptor(
                    NormalizeCluster(request.ClusterId),
                    request.User,
                    request.Mechanism,
                    request.Iterations));
            if (password.Length is < 1 or >
                ScramCredentialExecutionMaterial.HardMaxPasswordBytes)
            {
                return Failed("scram_upsert_invalid_password_material");
            }
        }
        catch (ArgumentException)
        {
            return Failed("scram_upsert_invalid_request");
        }

        if (!TryPrepareCall(
                descriptor.ClusterId,
                operation,
                cancellationToken,
                out var client,
                out var remaining,
                out var failure))
        {
            return failure!;
        }

        var providerPassword = password.ToArray();
        try
        {
            var alteration = new UserScramCredentialUpsertion
            {
                User = descriptor.User,
                ScramCredentialInfo = new ScramCredentialInfo
                {
                    Mechanism = ConfluentKafkaScramMapper.ToProviderMechanism(
                        descriptor.Mechanism),
                    Iterations = descriptor.Iterations,
                },
                Password = providerPassword,
                Salt = null!,
            };

            // Deliberately do not wrap this provider task in WaitAsync. If the
            // call outlives the common executor deadline, the common executor
            // retains its late task/material lease until this method actually
            // completes, preventing premature password-buffer disposal.
            await client!.AlterUserScramCredentialsAsync(
                    new UserScramCredentialAlteration[] { alteration },
                    new AlterUserScramCredentialsOptions
                    {
                        RequestTimeout = remaining,
                    })
                .ConfigureAwait(false);

            return Accepted("scram_upsert_accepted");
        }
        catch (AlterUserScramCredentialsException exception)
        {
            return ConfluentKafkaScramMutationResultClassifier.ClassifyException(
                "scram_upsert",
                descriptor.User,
                exception.Results);
        }
        catch (OperationCanceledException)
        {
            return Unknown("scram_upsert_cancelled_or_timeout");
        }
        catch (TimeoutException)
        {
            return Unknown("scram_upsert_timeout");
        }
        catch (KafkaException exception)
        {
            return FromError("scram_upsert", exception.Error);
        }
        catch (KafdeckConfigurationException)
        {
            return Failed("scram_upsert_invalid_configuration");
        }
        catch (KeyNotFoundException)
        {
            return Failed("scram_upsert_cluster_not_configured");
        }
        catch (ArgumentException)
        {
            return Failed("scram_upsert_invalid_request");
        }
        catch (InvalidOperationException)
        {
            return Failed("scram_upsert_invalid_operation");
        }
        catch (Exception)
        {
            return Unknown("scram_upsert_provider_exception");
        }
        finally
        {
            CryptographicOperations.ZeroMemory(providerPassword);
        }
    }

    public async Task<MutationProviderResult> DeleteAsync(
        ScramDeleteMutation request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string clusterId;
        string user;
        ScramMechanism mechanism;
        try
        {
            clusterId = NormalizeCluster(request.ClusterId);
            user = ScramCredentialPolicy.NormalizeUser(request.User);
            mechanism = ConfluentKafkaScramMapper.ToProviderMechanism(
                request.Mechanism);
        }
        catch (ArgumentException)
        {
            return Failed("scram_delete_invalid_request");
        }
        catch (NotSupportedException)
        {
            return Failed("scram_delete_capability_unsupported");
        }

        if (!TryPrepareCall(
                clusterId,
                operation,
                cancellationToken,
                out var client,
                out var remaining,
                out var failure))
        {
            return failure!;
        }

        try
        {
            var alteration = new UserScramCredentialDeletion
            {
                User = user,
                Mechanism = mechanism,
            };

            await client!.AlterUserScramCredentialsAsync(
                    new UserScramCredentialAlteration[] { alteration },
                    new AlterUserScramCredentialsOptions
                    {
                        RequestTimeout = remaining,
                    })
                .ConfigureAwait(false);

            return Accepted("scram_delete_accepted");
        }
        catch (AlterUserScramCredentialsException exception)
        {
            return ConfluentKafkaScramMutationResultClassifier.ClassifyException(
                "scram_delete",
                user,
                exception.Results);
        }
        catch (OperationCanceledException)
        {
            return Unknown("scram_delete_cancelled_or_timeout");
        }
        catch (TimeoutException)
        {
            return Unknown("scram_delete_timeout");
        }
        catch (KafkaException exception)
        {
            return FromError("scram_delete", exception.Error);
        }
        catch (KafdeckConfigurationException)
        {
            return Failed("scram_delete_invalid_configuration");
        }
        catch (KeyNotFoundException)
        {
            return Failed("scram_delete_cluster_not_configured");
        }
        catch (ArgumentException)
        {
            return Failed("scram_delete_invalid_request");
        }
        catch (InvalidOperationException)
        {
            return Failed("scram_delete_invalid_operation");
        }
        catch (Exception)
        {
            return Unknown("scram_delete_provider_exception");
        }
    }

    public void Dispose() => _clients.Dispose();

    private bool TryPrepareCall(
        string clusterId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken,
        out IAdminClient? client,
        out TimeSpan remaining,
        out MutationProviderResult? failure)
    {
        client = null;
        remaining = TimeSpan.Zero;
        failure = null;

        if (cancellationToken.IsCancellationRequested)
        {
            failure = Failed("scram_mutation_cancelled_before_dispatch");
            return false;
        }

        if (!_clients.ContainsCluster(clusterId))
        {
            failure = Failed("scram_mutation_cluster_not_configured");
            return false;
        }

        remaining = operation.Remaining(_timeProvider.GetUtcNow());
        if (remaining <= TimeSpan.Zero)
        {
            failure = Failed("scram_mutation_deadline_exhausted_before_dispatch");
            return false;
        }

        try
        {
            client = _clients.GetClient(clusterId);
            return true;
        }
        catch (KafdeckConfigurationException)
        {
            failure = Failed("scram_mutation_invalid_configuration");
            return false;
        }
        catch (KeyNotFoundException)
        {
            failure = Failed("scram_mutation_cluster_not_configured");
            return false;
        }
    }

    private static string NormalizeCluster(string clusterId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        if (clusterId.Length > 256 ||
            clusterId.Any(char.IsControl) ||
            !string.Equals(clusterId, clusterId.Trim(), StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "SCRAM physical cluster ID is invalid.",
                nameof(clusterId));
        }

        return clusterId;
    }

    private static MutationProviderResult FromError(
        string operationCode,
        Error error)
    {
        var failure = KafkaFailureMapper.FromKafka(error);
        var code = $"{operationCode}_{failure.Code}";
        return IsAmbiguous(failure.Category)
            ? Unknown(code)
            : Failed(code);
    }

    internal static bool IsAmbiguous(KafkaFailureCategory category) =>
        category is
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Unknown;

    private static MutationProviderResult Accepted(string code) =>
        new(
            MutationExecutionResultKind.AppliedUnverified,
            code,
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["provider.accepted"] = "true",
                ["acknowledged.count"] = 1.ToString(
                    CultureInfo.InvariantCulture),
            });

    private static MutationProviderResult Failed(string code) =>
        new(MutationExecutionResultKind.FailedDefinitive, code);

    private static MutationProviderResult Unknown(string code) =>
        new(MutationExecutionResultKind.ExecutionUnknown, code);
}

internal static class ConfluentKafkaScramMutationResultClassifier
{
    public static MutationProviderResult ClassifyException(
        string operationCode,
        string expectedUser,
        IReadOnlyList<AlterUserScramCredentialsReport> reports)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationCode);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedUser);
        ArgumentNullException.ThrowIfNull(reports);

        if (reports.Count != 1 ||
            !string.Equals(
                reports[0].User,
                expectedUser,
                StringComparison.Ordinal) ||
            !reports[0].Error.IsError)
        {
            return Unknown($"{operationCode}_unexpected_provider_report");
        }

        var failure = KafkaFailureMapper.FromKafka(reports[0].Error);
        return ConfluentKafkaScramMutationAdapter.IsAmbiguous(
                failure.Category)
            ? Unknown($"{operationCode}_result_ambiguous")
            : Failed($"{operationCode}_failed_definitive");
    }

    private static MutationProviderResult Failed(string code) =>
        new(MutationExecutionResultKind.FailedDefinitive, code);

    private static MutationProviderResult Unknown(string code) =>
        new(MutationExecutionResultKind.ExecutionUnknown, code);
}
