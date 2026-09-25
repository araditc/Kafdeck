using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Kafka;

/// <summary>
/// Closed W44 broker/default-broker dynamic configuration boundary. Only one
/// exact typed key/scope is accepted per call. Policy/allowlist decisions stay
/// in the Administration module and no generic AdminClient surface is exposed.
/// </summary>
public sealed class ConfluentKafkaDynamicConfigurationAdapter :
    IDynamicConfigurationPort,
    IDisposable
{
    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeProvider _timeProvider;

    public ConfluentKafkaDynamicConfigurationAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
    {
        _clients = new KafkaAdminClientRegistry(
            clusterProfiles ??
            throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ??
            throw new ArgumentNullException(nameof(secretResolver)));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<KafkaResult<DynamicConfigurationObservation>>
        DescribeAsync(
            DynamicConfigurationTarget target,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
    {
        DynamicConfigurationTarget normalized;
        try
        {
            normalized =
                DynamicConfigurationPolicy.NormalizeTarget(target);
        }
        catch (ArgumentException)
        {
            return FailedObservation(
                new KafkaFailure(
                    KafkaFailureCategory.InvalidConfiguration,
                    "dynamic_config_invalid_target",
                    "Dynamic configuration target is invalid.",
                    IsRetryable: false));
        }

        if (!TryPrepareCall(
                normalized.ClusterId,
                operation,
                cancellationToken,
                out var client,
                out var remaining,
                out var failure))
        {
            return FailedObservation(failure!);
        }

        try
        {
            var resource = Resource(normalized);
            var results = await client!.DescribeConfigsAsync(
                    new[] { resource },
                    new DescribeConfigsOptions
                    {
                        RequestTimeout = remaining,
                    })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            var result = results.Single();
            if (!result.Entries.TryGetValue(
                    normalized.Key,
                    out var entry))
            {
                return FailedObservation(
                    new KafkaFailure(
                        KafkaFailureCategory.NotSupported,
                        "dynamic_config_key_not_observed",
                        "Kafka did not expose the requested configuration key.",
                        IsRetryable: false));
            }

            var synonyms = (entry.Synonyms ??
                            new List<ConfigSynonym>())
                .Select(item =>
                    new DynamicConfigurationSynonym(
                        item.Source.ToString(),
                        entry.IsSensitive ? null : item.Value))
                .ToArray();

            return KafkaResult<DynamicConfigurationObservation>.Success(
                new DynamicConfigurationObservation(
                    normalized,
                    entry.IsSensitive ? null : entry.Value,
                    entry.Source.ToString(),
                    entry.IsSensitive,
                    entry.IsReadOnly,
                    Array.AsReadOnly(synonyms)),
                LiveObservation());
        }
        catch (DescribeConfigsException exception)
            when (exception.Results.Count == 1 &&
                  exception.Results[0].Error.IsError)
        {
            return FailedObservation(
                KafkaFailureMapper.FromKafka(
                    exception.Results[0].Error));
        }
        catch (OperationCanceledException)
        {
            return FailedObservation(
                cancellationToken.IsCancellationRequested
                    ? KafkaFailureMapper.Cancelled()
                    : KafkaFailureMapper.DeadlineExceeded());
        }
        catch (TimeoutException)
        {
            return FailedObservation(
                KafkaFailureMapper.DeadlineExceeded());
        }
        catch (KafkaException exception)
        {
            return FailedObservation(
                KafkaFailureMapper.FromKafka(exception.Error));
        }
        catch (KafdeckConfigurationException)
        {
            return FailedObservation(
                KafkaFailureMapper.InvalidConfiguration());
        }
        catch (KeyNotFoundException)
        {
            return FailedObservation(
                KafkaFailureMapper.ClusterNotConfigured());
        }
        catch (ArgumentException)
        {
            return FailedObservation(
                KafkaFailureMapper.InvalidConfiguration());
        }
        catch (InvalidOperationException)
        {
            return FailedObservation(
                KafkaFailureMapper.InvalidConfiguration());
        }
        catch (Exception)
        {
            return FailedObservation(
                KafkaFailureMapper.Unknown());
        }
    }

    public async Task<MutationProviderResult> AlterAsync(
        DynamicConfigurationMutation mutation,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mutation);

        DynamicConfigurationTarget target;
        try
        {
            target =
                DynamicConfigurationPolicy.NormalizeTarget(
                    mutation.Target);
            if (mutation.Value is { Length: > 4096 } ||
                mutation.Value?.Any(char.IsControl) == true)
            {
                return FailedMutation(
                    "dynamic_config_invalid_value");
            }
        }
        catch (ArgumentException)
        {
            return FailedMutation(
                "dynamic_config_invalid_target");
        }

        if (!TryPrepareCall(
                target.ClusterId,
                operation,
                cancellationToken,
                out var client,
                out var remaining,
                out _))
        {
            return FailedMutation(
                "dynamic_config_not_dispatchable");
        }

        try
        {
            var resource = Resource(target);
            var entry = new ConfigEntry
            {
                Name = target.Key,
                Value = mutation.Value!,
                IncrementalOperation = mutation.Value is null
                    ? AlterConfigOpType.Delete
                    : AlterConfigOpType.Set,
            };

            await client!.IncrementalAlterConfigsAsync(
                    new Dictionary<ConfigResource, List<ConfigEntry>>
                    {
                        [resource] = new() { entry },
                    },
                    new IncrementalAlterConfigsOptions
                    {
                        RequestTimeout = remaining,
                        ValidateOnly = false,
                    })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedUnverified,
                "dynamic_config_alter_accepted",
                new Dictionary<string, string>(
                    StringComparer.Ordinal)
                {
                    ["provider.accepted"] = "true",
                    ["target.scope"] =
                        target.Scope.ToString(),
                    ["target.broker"] =
                        target.BrokerId?.ToString(
                            CultureInfo.InvariantCulture) ??
                        "default",
                    ["config.key"] = target.Key,
                });
        }
        catch (IncrementalAlterConfigsException exception)
            when (exception.Results.Count == 1)
        {
            return FromError(
                "dynamic_config_alter",
                exception.Results[0].Error);
        }
        catch (OperationCanceledException)
        {
            return UnknownMutation(
                "dynamic_config_alter_cancelled_or_timeout");
        }
        catch (TimeoutException)
        {
            return UnknownMutation(
                "dynamic_config_alter_timeout");
        }
        catch (KafkaException exception)
        {
            return FromError(
                "dynamic_config_alter",
                exception.Error);
        }
        catch (KafdeckConfigurationException)
        {
            return FailedMutation(
                "dynamic_config_alter_invalid_configuration");
        }
        catch (KeyNotFoundException)
        {
            return FailedMutation(
                "dynamic_config_alter_cluster_not_configured");
        }
        catch (ArgumentException)
        {
            return FailedMutation(
                "dynamic_config_alter_invalid_request");
        }
        catch (InvalidOperationException)
        {
            return FailedMutation(
                "dynamic_config_alter_invalid_operation");
        }
        catch (Exception)
        {
            // Once IncrementalAlterConfigsAsync is invoked an unclassified
            // exception is not proof of non-application. Never blind-retry.
            return UnknownMutation(
                "dynamic_config_alter_provider_exception");
        }
    }

    public void Dispose() => _clients.Dispose();

    private bool TryPrepareCall(
        string clusterId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken,
        out IAdminClient? client,
        out TimeSpan remaining,
        out KafkaFailure? failure)
    {
        client = null;
        remaining = TimeSpan.Zero;
        failure = null;

        if (cancellationToken.IsCancellationRequested)
        {
            failure = KafkaFailureMapper.Cancelled();
            return false;
        }

        if (!_clients.ContainsCluster(clusterId))
        {
            failure = KafkaFailureMapper.ClusterNotConfigured();
            return false;
        }

        remaining = operation.Remaining(
            _timeProvider.GetUtcNow());
        if (remaining <= TimeSpan.Zero)
        {
            failure = KafkaFailureMapper.DeadlineExceeded();
            return false;
        }

        try
        {
            client = _clients.GetClient(clusterId);
            return true;
        }
        catch (KafdeckConfigurationException)
        {
            failure = KafkaFailureMapper.InvalidConfiguration();
            return false;
        }
        catch (KeyNotFoundException)
        {
            failure = KafkaFailureMapper.ClusterNotConfigured();
            return false;
        }
    }

    private static ConfigResource Resource(
        DynamicConfigurationTarget target) =>
        new()
        {
            Type = ResourceType.Broker,
            Name = target.Scope ==
                   DynamicConfigurationScope.ClusterDefault
                ? string.Empty
                : target.BrokerId!.Value.ToString(
                    CultureInfo.InvariantCulture),
        };

    private KafkaResult<DynamicConfigurationObservation>
        FailedObservation(KafkaFailure failure) =>
        KafkaResult<DynamicConfigurationObservation>.Failed(
            failure,
            LiveObservation());

    private ObservationMetadata LiveObservation()
    {
        var now = _timeProvider.GetUtcNow();
        return new ObservationMetadata(
            now,
            now,
            now,
            ObservationSource.Live);
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
            ? UnknownMutation(code)
            : FailedMutation(code);
    }

    private static MutationProviderResult FailedMutation(
        string code) =>
        new(
            MutationExecutionResultKind.FailedDefinitive,
            code);

    private static MutationProviderResult UnknownMutation(
        string code) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            code);
}
