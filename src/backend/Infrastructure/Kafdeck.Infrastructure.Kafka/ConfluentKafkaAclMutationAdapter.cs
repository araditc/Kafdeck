using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Kafka;

/// <summary>
/// Typed W42 ACL write adapter. The adapter accepts only exact ACL bindings
/// frozen by the governed planner. It exposes no arbitrary provider filter,
/// AdminClient, CLI, REST or protocol passthrough.
/// </summary>
public sealed class ConfluentKafkaAclMutationAdapter :
    IAclMutationPort,
    IDisposable
{
    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeProvider _timeProvider;

    public ConfluentKafkaAclMutationAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
    {
        _clients = new KafkaAdminClientRegistry(
            clusterProfiles ?? throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ?? throw new ArgumentNullException(nameof(secretResolver)));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationProviderResult> CreateAsync(
        AclCreateMutation request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        IReadOnlyList<KafkaAclBinding> bindings;
        IReadOnlyList<AclBinding> providerBindings;
        try
        {
            ValidateClusterId(request.ClusterId);
            bindings = AclMutationPolicy.NormalizeExactBindings(
                request.Bindings,
                AclMutationPolicy.HardMaxBindings);
            providerBindings = bindings
                .Select(ConfluentKafkaAclMapper.ToProviderBinding)
                .ToArray();
        }
        catch (NotSupportedException)
        {
            return Failed("acl_create_capability_unsupported");
        }
        catch (ArgumentException)
        {
            return Failed("acl_create_invalid_request");
        }

        if (!TryPrepareCall(
                request.ClusterId,
                operation,
                cancellationToken,
                out var client,
                out var remaining,
                out var deadline,
                out var failure))
        {
            return failure!;
        }

        using (deadline)
        {
            try
            {
                await client!.CreateAclsAsync(
                        providerBindings,
                        new CreateAclsOptions
                        {
                            RequestTimeout = remaining,
                        })
                    .WaitAsync(deadline!.Token)
                    .ConfigureAwait(false);

                return Accepted(
                    "acl_create_accepted",
                    bindings.Count);
            }
            catch (CreateAclsException exception)
            {
                return FromCreateReports(exception.Results);
            }
            catch (OperationCanceledException)
            {
                return Unknown("acl_create_cancelled_or_timeout");
            }
            catch (TimeoutException)
            {
                return Unknown("acl_create_timeout");
            }
            catch (KafkaException exception)
            {
                return FromError("acl_create", exception.Error);
            }
            catch (KafdeckConfigurationException)
            {
                return Failed("acl_create_invalid_configuration");
            }
            catch (KeyNotFoundException)
            {
                return Failed("acl_create_cluster_not_configured");
            }
            catch (ArgumentException)
            {
                return Failed("acl_create_invalid_request");
            }
            catch (InvalidOperationException)
            {
                return Failed("acl_create_invalid_operation");
            }
            catch (Exception)
            {
                return Unknown("acl_create_provider_exception");
            }
        }
    }

    public async Task<MutationProviderResult> RemoveAsync(
        AclRemoveMutation request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        IReadOnlyList<KafkaAclBinding> bindings;
        IReadOnlyList<AclBindingFilter> filters;
        try
        {
            ValidateClusterId(request.ClusterId);
            bindings = AclMutationPolicy.NormalizeExactBindings(
                request.Bindings,
                AclMutationPolicy.HardMaxBindings);
            filters = bindings
                .Select(AclMutationPolicy.ExactFilter)
                .Select(ConfluentKafkaAclMapper.ToProviderFilter)
                .ToArray();
        }
        catch (NotSupportedException)
        {
            return Failed("acl_remove_capability_unsupported");
        }
        catch (ArgumentException)
        {
            return Failed("acl_remove_invalid_request");
        }

        if (!TryPrepareCall(
                request.ClusterId,
                operation,
                cancellationToken,
                out var client,
                out var remaining,
                out var deadline,
                out var failure))
        {
            return failure!;
        }

        using (deadline)
        {
            try
            {
                var results = await client!.DeleteAclsAsync(
                        filters,
                        new DeleteAclsOptions
                        {
                            RequestTimeout = remaining,
                        })
                    .WaitAsync(deadline!.Token)
                    .ConfigureAwait(false);

                if (!TryValidateDeletedBindings(
                        bindings,
                        results.SelectMany(result => result.AclBindings),
                        out var deletedCount))
                {
                    return Unknown("acl_remove_unexpected_provider_result");
                }

                return Accepted(
                    "acl_remove_accepted",
                    deletedCount);
            }
            catch (DeleteAclsException exception)
            {
                return FromDeleteReports(bindings, exception.Results);
            }
            catch (OperationCanceledException)
            {
                return Unknown("acl_remove_cancelled_or_timeout");
            }
            catch (TimeoutException)
            {
                return Unknown("acl_remove_timeout");
            }
            catch (KafkaException exception)
            {
                return FromError("acl_remove", exception.Error);
            }
            catch (KafdeckConfigurationException)
            {
                return Failed("acl_remove_invalid_configuration");
            }
            catch (KeyNotFoundException)
            {
                return Failed("acl_remove_cluster_not_configured");
            }
            catch (ArgumentException)
            {
                return Failed("acl_remove_invalid_request");
            }
            catch (InvalidOperationException)
            {
                return Failed("acl_remove_invalid_operation");
            }
            catch (Exception)
            {
                return Unknown("acl_remove_provider_exception");
            }
        }
    }

    public void Dispose() => _clients.Dispose();

    private bool TryPrepareCall(
        string clusterId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken,
        out IAdminClient? client,
        out TimeSpan remaining,
        out CancellationTokenSource? deadline,
        out MutationProviderResult? failure)
    {
        client = null;
        remaining = TimeSpan.Zero;
        deadline = null;
        failure = null;

        if (cancellationToken.IsCancellationRequested)
        {
            failure = Unknown("acl_mutation_cancelled");
            return false;
        }

        if (!_clients.ContainsCluster(clusterId))
        {
            failure = Failed("acl_mutation_cluster_not_configured");
            return false;
        }

        remaining = operation.Remaining(_timeProvider.GetUtcNow());
        if (remaining <= TimeSpan.Zero)
        {
            failure = Unknown("acl_mutation_deadline_exhausted");
            return false;
        }

        try
        {
            client = _clients.GetClient(clusterId);
        }
        catch (KafdeckConfigurationException)
        {
            failure = Failed("acl_mutation_invalid_configuration");
            return false;
        }
        catch (KeyNotFoundException)
        {
            failure = Failed("acl_mutation_cluster_not_configured");
            return false;
        }

        deadline = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);
        deadline.CancelAfter(remaining);
        return true;
    }

    private static MutationProviderResult FromCreateReports(
        IReadOnlyList<CreateAclReport> reports)
    {
        if (reports.Count == 0)
        {
            return Unknown("acl_create_empty_provider_report");
        }

        var successful = reports.Count(report => !report.Error.IsError);
        var failures = reports
            .Where(report => report.Error.IsError)
            .Select(report => KafkaFailureMapper.FromKafka(report.Error).Category)
            .ToArray();

        return ConfluentKafkaAclMutationResultClassifier.ClassifyCreateException(
            successful,
            failures);
    }

    private static MutationProviderResult FromDeleteReports(
        IReadOnlyList<KafkaAclBinding> expectedBindings,
        IReadOnlyList<DeleteAclsReport> reports)
    {
        if (reports.Count == 0)
        {
            return Unknown("acl_remove_empty_provider_report");
        }

        var failures = reports
            .Where(report => report.Error.IsError)
            .Select(report => report.Error)
            .ToArray();

        var successfulReports = reports
            .Where(report => !report.Error.IsError)
            .ToArray();

        if (!TryValidateDeletedBindings(
                expectedBindings,
                successfulReports.SelectMany(report => report.AclBindings),
                out var deletedCount))
        {
            return Unknown("acl_remove_unexpected_provider_result");
        }

        return ConfluentKafkaAclMutationResultClassifier.ClassifyDeleteException(
            deletedCount,
            failures
                .Select(KafkaFailureMapper.FromKafka)
                .Select(failure => failure.Category)
                .ToArray());
    }

    private static bool TryValidateDeletedBindings(
        IReadOnlyList<KafkaAclBinding> expectedBindings,
        IEnumerable<AclBinding> deletedBindings,
        out int deletedCount)
    {
        var expected = expectedBindings
            .Select(AclBindingIdentity.Canonical)
            .ToHashSet(StringComparer.Ordinal);
        var observed = new HashSet<string>(StringComparer.Ordinal);

        foreach (var providerBinding in deletedBindings)
        {
            if (!ConfluentKafkaAclMapper.TryFromProvider(
                    providerBinding,
                    out var binding) ||
                binding is null)
            {
                deletedCount = 0;
                return false;
            }

            var canonical = AclBindingIdentity.Canonical(binding);
            if (!expected.Contains(canonical))
            {
                deletedCount = 0;
                return false;
            }

            observed.Add(canonical);
        }

        deletedCount = observed.Count;
        return true;
    }

    private static void ValidateClusterId(string clusterId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        if (!string.Equals(clusterId, clusterId.Trim(), StringComparison.Ordinal) ||
            clusterId.Length > 256 ||
            clusterId.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(
                nameof(clusterId),
                "ACL cluster ID is invalid or exceeds the admitted bound.");
        }
    }

    private static MutationProviderResult FromError(
        string operationCode,
        Error error)
    {
        var failure = KafkaFailureMapper.FromKafka(error);
        var code = $"{operationCode}_{failure.Code}";

        return failure.Category is
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Unknown
            ? Unknown(code)
            : Failed(code);
    }

    private static MutationProviderResult Accepted(
        string code,
        int acknowledgedCount) =>
        new(
            MutationExecutionResultKind.AppliedUnverified,
            code,
            Evidence(
                providerAccepted: true,
                acknowledgedCount));

    private static MutationProviderResult PartiallyApplied(
        string code,
        int acknowledgedCount) =>
        new(
            MutationExecutionResultKind.PartiallyApplied,
            code,
            Evidence(
                providerAccepted: false,
                acknowledgedCount));

    private static MutationProviderResult Failed(string code) =>
        new(
            MutationExecutionResultKind.FailedDefinitive,
            code);

    private static MutationProviderResult Unknown(
        string code,
        int acknowledgedCount = 0) =>
        acknowledgedCount > 0
            ? new(
                MutationExecutionResultKind.ExecutionUnknown,
                code,
                Evidence(
                    providerAccepted: false,
                    acknowledgedCount))
            : new(
                MutationExecutionResultKind.ExecutionUnknown,
                code);

    private static IReadOnlyDictionary<string, string> Evidence(
        bool providerAccepted,
        int acknowledgedCount) =>
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["provider.accepted"] = providerAccepted ? "true" : "false",
            ["acknowledged.count"] = acknowledgedCount.ToString(
                CultureInfo.InvariantCulture),
        };
}


internal static class ConfluentKafkaAclMutationResultClassifier
{
    internal static MutationProviderResult ClassifyCreateException(
        int successfulCount,
        IReadOnlyList<KafkaFailureCategory> failureCategories)
    {
        ArgumentNullException.ThrowIfNull(failureCategories);
        if (successfulCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(successfulCount));
        }

        if (failureCategories.Count == 0)
        {
            return Unknown("acl_create_unexpected_provider_report");
        }

        if (failureCategories.Any(IsAmbiguous))
        {
            return Unknown(
                "acl_create_result_ambiguous",
                successfulCount);
        }

        return successfulCount > 0
            ? PartiallyApplied(
                "acl_create_partially_applied",
                successfulCount)
            : Failed("acl_create_failed_definitive");
    }

    internal static MutationProviderResult ClassifyDeleteException(
        int deletedCount,
        IReadOnlyList<KafkaFailureCategory> failureCategories)
    {
        ArgumentNullException.ThrowIfNull(failureCategories);
        if (deletedCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(deletedCount));
        }

        if (failureCategories.Count == 0)
        {
            return Unknown("acl_remove_unexpected_provider_report");
        }

        if (failureCategories.Any(IsAmbiguous))
        {
            return Unknown(
                "acl_remove_result_ambiguous",
                deletedCount);
        }

        // A successful delete filter that matched no binding is not evidence
        // that any provider state changed. Partial application is claimed only
        // when the provider report proves at least one exact binding deletion.
        return deletedCount > 0
            ? PartiallyApplied(
                "acl_remove_partially_applied",
                deletedCount)
            : Failed("acl_remove_failed_definitive");
    }

    private static bool IsAmbiguous(KafkaFailureCategory category) =>
        category is
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Unknown;

    private static MutationProviderResult Failed(string code) =>
        new(
            MutationExecutionResultKind.FailedDefinitive,
            code);

    private static MutationProviderResult Unknown(
        string code,
        int acknowledgedCount = 0) =>
        acknowledgedCount > 0
            ? new(
                MutationExecutionResultKind.ExecutionUnknown,
                code,
                Evidence(acknowledgedCount))
            : new(
                MutationExecutionResultKind.ExecutionUnknown,
                code);

    private static MutationProviderResult PartiallyApplied(
        string code,
        int acknowledgedCount) =>
        new(
            MutationExecutionResultKind.PartiallyApplied,
            code,
            Evidence(acknowledgedCount));

    private static IReadOnlyDictionary<string, string> Evidence(
        int acknowledgedCount) =>
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["acknowledged.count"] = acknowledgedCount.ToString(
                CultureInfo.InvariantCulture),
        };
}
