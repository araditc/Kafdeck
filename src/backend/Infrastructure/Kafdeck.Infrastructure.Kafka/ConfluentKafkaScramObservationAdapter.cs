using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Kafka;

/// <summary>
/// Pinned-client SCRAM metadata adapter. It requests one exact user and projects
/// only mechanism/iteration metadata; provider credential material is never
/// returned by this boundary.
/// </summary>
public sealed class ConfluentKafkaScramObservationAdapter :
    IScramObservationPort,
    IDisposable
{
    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeProvider _timeProvider;

    public ConfluentKafkaScramObservationAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
    {
        _clients = new KafkaAdminClientRegistry(
            clusterProfiles ?? throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ?? throw new ArgumentNullException(nameof(secretResolver)));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>> DescribeUserAsync(
        string clusterId,
        string user,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        string normalizedUser;
        try
        {
            normalizedUser = ScramCredentialPolicy.NormalizeUser(user);
        }
        catch (ArgumentException)
        {
            return Failed(KafkaFailureMapper.InvalidConfiguration());
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return Failed(KafkaFailureMapper.Cancelled());
        }

        var now = _timeProvider.GetUtcNow();
        if (operation.IsExpired(now))
        {
            return Failed(KafkaFailureMapper.DeadlineExceeded());
        }

        if (!_clients.ContainsCluster(clusterId))
        {
            return Failed(KafkaFailureMapper.ClusterNotConfigured());
        }

        try
        {
            var remaining = operation.Remaining(_timeProvider.GetUtcNow());
            if (remaining <= TimeSpan.Zero)
            {
                return Failed(KafkaFailureMapper.DeadlineExceeded());
            }

            using var deadline =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(remaining);

            var result = await _clients.GetClient(clusterId)
                .DescribeUserScramCredentialsAsync(
                    new[] { normalizedUser },
                    new DescribeUserScramCredentialsOptions
                    {
                        RequestTimeout = remaining,
                    })
                .WaitAsync(deadline.Token)
                .ConfigureAwait(false);

            if (result.UserScramCredentialsDescriptions.Count != 1)
            {
                return Failed(new KafkaFailure(
                    KafkaFailureCategory.ProtocolError,
                    "scram_description_cardinality_invalid",
                    "Kafka returned an unexpected SCRAM metadata result shape.",
                    false));
            }

            var description = result.UserScramCredentialsDescriptions[0];
            if (!string.Equals(
                    description.User,
                    normalizedUser,
                    StringComparison.Ordinal))
            {
                return Failed(new KafkaFailure(
                    KafkaFailureCategory.ProtocolError,
                    "scram_description_user_mismatch",
                    "Kafka returned SCRAM metadata for an unexpected user identity.",
                    false));
            }

            if (description.Error.IsError)
            {
                if (description.Error.Code == ErrorCode.ResourceNotFound)
                {
                    return KafkaResult<
                        IReadOnlyList<KafkaScramCredentialMetadata>>.Success(
                            Array.Empty<KafkaScramCredentialMetadata>(),
                            LiveObservation());
                }

                return Failed(KafkaFailureMapper.FromKafka(description.Error));
            }

            var metadata = new List<KafkaScramCredentialMetadata>(
                description.ScramCredentialInfos.Count);
            foreach (var credential in description.ScramCredentialInfos)
            {
                if (!ConfluentKafkaScramMapper.TryFromProvider(
                        normalizedUser,
                        credential,
                        out var converted))
                {
                    return Failed(new KafkaFailure(
                        KafkaFailureCategory.NotSupported,
                        "scram_metadata_shape_unsupported",
                        "Kafka returned a SCRAM metadata shape that the admitted W43 contract does not support.",
                        false));
                }

                metadata.Add(converted!);
            }

            IReadOnlyList<KafkaScramCredentialMetadata> normalized;
            try
            {
                normalized = ScramCredentialPolicy.NormalizeMetadataSet(
                    normalizedUser,
                    metadata);
            }
            catch (ArgumentException)
            {
                return Failed(new KafkaFailure(
                    KafkaFailureCategory.ProtocolError,
                    "scram_metadata_invalid",
                    "Kafka returned invalid SCRAM metadata.",
                    false));
            }

            return KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>.Success(
                normalized,
                LiveObservation());
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return Failed(KafkaFailureMapper.Cancelled());
        }
        catch (OperationCanceledException)
        {
            return Failed(KafkaFailureMapper.DeadlineExceeded());
        }
        catch (TimeoutException)
        {
            return Failed(KafkaFailureMapper.DeadlineExceeded());
        }
        catch (DescribeUserScramCredentialsException exception)
        {
            // Confluent surfaces per-user Describe errors through a
            // Local_Partial exception. The underlying ResourceNotFound lives
            // in Results, not exception.Error. Because this adapter always
            // requests exactly one normalized user, only that exact one-user
            // ResourceNotFound is absence; every other shape remains a
            // provider/protocol failure.
            var descriptions =
                exception.Results?.UserScramCredentialsDescriptions;
            if (descriptions is { Count: 1 } &&
                string.Equals(
                    descriptions[0].User,
                    normalizedUser,
                    StringComparison.Ordinal))
            {
                var userError = descriptions[0].Error;
                if (userError.Code == ErrorCode.ResourceNotFound)
                {
                    return KafkaResult<
                        IReadOnlyList<KafkaScramCredentialMetadata>>.Success(
                            Array.Empty<KafkaScramCredentialMetadata>(),
                            LiveObservation());
                }

                return Failed(KafkaFailureMapper.FromKafka(userError));
            }

            return Failed(new KafkaFailure(
                KafkaFailureCategory.ProtocolError,
                "scram_description_exception_shape_invalid",
                "Kafka returned an unexpected SCRAM error result shape.",
                false));
        }
        catch (KafkaException exception)
        {
            return Failed(KafkaFailureMapper.FromKafka(exception.Error));
        }
        catch (KafdeckConfigurationException)
        {
            return Failed(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (KeyNotFoundException)
        {
            return Failed(KafkaFailureMapper.ClusterNotConfigured());
        }
        catch (ArgumentException)
        {
            return Failed(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (InvalidOperationException)
        {
            return Failed(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (Exception)
        {
            return Failed(KafkaFailureMapper.Unknown());
        }
    }

    public void Dispose() => _clients.Dispose();

    private KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>> Failed(
        KafkaFailure failure) =>
        KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>.Failed(
            failure,
            LiveObservation());

    private ObservationMetadata LiveObservation()
    {
        var now = _timeProvider.GetUtcNow();
        return new ObservationMetadata(now, now, now, ObservationSource.Live);
    }
}

internal static class ConfluentKafkaScramMapper
{
    public static bool TryFromProvider(
        string user,
        ScramCredentialInfo credential,
        out KafkaScramCredentialMetadata? metadata)
    {
        metadata = null;
        if (credential is null || credential.Iterations <= 0)
        {
            return false;
        }

        var mechanism = credential.Mechanism switch
        {
            ScramMechanism.ScramSha256 => KafkaScramMechanism.ScramSha256,
            ScramMechanism.ScramSha512 => KafkaScramMechanism.ScramSha512,
            _ => (KafkaScramMechanism?)null,
        };
        if (mechanism is null)
        {
            return false;
        }

        try
        {
            metadata = ScramCredentialPolicy.NormalizeMetadata(
                new KafkaScramCredentialMetadata(
                    user,
                    mechanism.Value,
                    credential.Iterations));
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    public static ScramMechanism ToProviderMechanism(
        KafkaScramMechanism mechanism) =>
        mechanism switch
        {
            KafkaScramMechanism.ScramSha256 => ScramMechanism.ScramSha256,
            KafkaScramMechanism.ScramSha512 => ScramMechanism.ScramSha512,
            _ => throw new NotSupportedException(
                "SCRAM mechanism is not supported by the pinned provider contract."),
        };
}
