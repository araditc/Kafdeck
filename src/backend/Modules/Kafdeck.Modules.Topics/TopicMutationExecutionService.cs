using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Topics;

public sealed record TopicMutationVerificationPolicy
{
    public TopicMutationVerificationPolicy(
        TimeSpan timeout,
        TimeSpan pollInterval)
    {
        if (timeout < TimeSpan.FromSeconds(1) ||
            timeout > TimeSpan.FromSeconds(15))
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }

        if (pollInterval < TimeSpan.FromMilliseconds(50) ||
            pollInterval > TimeSpan.FromSeconds(2) ||
            pollInterval >= timeout)
        {
            throw new ArgumentOutOfRangeException(nameof(pollInterval));
        }

        Timeout = timeout;
        PollInterval = pollInterval;
    }

    public TimeSpan Timeout { get; }
    public TimeSpan PollInterval { get; }

    public static TopicMutationVerificationPolicy Default { get; } =
        new(
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(250));
}

public sealed class TopicMutationExecutionService
{
    private readonly ITopicMutationPort _mutations;
    private readonly IKafkaAdministrationPort _reads;
    private readonly TopicMutationVerificationPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public TopicMutationExecutionService(
        ITopicMutationPort mutations,
        IKafkaAdministrationPort reads,
        TopicMutationVerificationPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _mutations = mutations ?? throw new ArgumentNullException(nameof(mutations));
        _reads = reads ?? throw new ArgumentNullException(nameof(reads));
        _policy = policy ?? TopicMutationVerificationPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationProviderResult> CreateAsync(
        TopicCreateMutation mutation,
        CancellationToken cancellationToken = default)
    {
        var accepted = await _mutations.CreateTopicAsync(
                mutation,
                cancellationToken)
            .ConfigureAwait(false);

        if (accepted.ResultKind != MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        return await VerifyUntilAsync(
                mutation.ClusterId,
                async (operation, token) =>
                {
                    var metadata = await _reads.GetTopicMetadataAsync(
                            mutation.ClusterId,
                            mutation.TopicName,
                            operation,
                            token)
                        .ConfigureAwait(false);

                    if (!metadata.IsSuccess || metadata.Value is null)
                    {
                        return false;
                    }

                    if (metadata.Value.IsInternal ||
                        metadata.Value.Partitions.Count != mutation.PartitionCount)
                    {
                        return false;
                    }

                    if (mutation.Configurations.Count == 0)
                    {
                        return true;
                    }

                    var configuration = await _reads.GetTopicConfigurationAsync(
                            mutation.ClusterId,
                            mutation.TopicName,
                            operation,
                            token)
                        .ConfigureAwait(false);

                    if (!configuration.IsSuccess || configuration.Value is null)
                    {
                        return false;
                    }

                    var byName = configuration.Value.ToDictionary(
                        entry => entry.Name,
                        StringComparer.Ordinal);

                    return mutation.Configurations.All(pair =>
                        byName.TryGetValue(pair.Key, out var entry) &&
                        !entry.IsSensitive &&
                        string.Equals(entry.Value, pair.Value, StringComparison.Ordinal) &&
                        string.Equals(
                            entry.Source,
                            "DynamicTopicConfig",
                            StringComparison.Ordinal));
                },
                Verified(
                    "topic_create_verified",
                    accepted,
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["resource.exists"] = "true",
                        ["partition.count"] = mutation.PartitionCount.ToString(
                            System.Globalization.CultureInfo.InvariantCulture),
                        ["replication.factor"] = mutation.ReplicationFactor.ToString(
                            System.Globalization.CultureInfo.InvariantCulture),
                    }),
                Unverified("topic_create_verification_inconclusive", accepted),
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<MutationProviderResult> AlterAsync(
        TopicAlterMutation mutation,
        CancellationToken cancellationToken = default)
    {
        var accepted = await _mutations.AlterTopicAsync(
                mutation,
                cancellationToken)
            .ConfigureAwait(false);

        if (accepted.ResultKind != MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        string? observedFingerprint = null;
        var result = await VerifyUntilAsync(
                mutation.ClusterId,
                async (operation, token) =>
                {
                    var configuration = await _reads.GetTopicConfigurationAsync(
                            mutation.ClusterId,
                            mutation.TopicName,
                            operation,
                            token)
                        .ConfigureAwait(false);

                    if (!configuration.IsSuccess || configuration.Value is null)
                    {
                        return false;
                    }

                    var byName = configuration.Value.ToDictionary(
                        entry => entry.Name,
                        StringComparer.Ordinal);

                    foreach (var change in mutation.Changes)
                    {
                        if (!byName.TryGetValue(change.Key, out var entry) ||
                            entry.IsSensitive)
                        {
                            return false;
                        }

                        if (change.Value is not null)
                        {
                            if (!string.Equals(
                                    entry.Value,
                                    change.Value,
                                    StringComparison.Ordinal) ||
                                !string.Equals(
                                    entry.Source,
                                    "DynamicTopicConfig",
                                    StringComparison.Ordinal))
                            {
                                return false;
                            }
                        }
                        else if (string.Equals(
                                     entry.Source,
                                     "DynamicTopicConfig",
                                     StringComparison.Ordinal))
                        {
                            return false;
                        }
                    }

                    observedFingerprint =
                        TopicMutationPolicy.FingerprintConfigurations(
                            mutation.TopicName,
                            configuration.Value,
                            mutation.Changes.Keys);
                    return true;
                },
                verifiedFactory: () =>
                {
                    var evidence = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["resource.exists"] = "true",
                    };
                    if (!string.IsNullOrWhiteSpace(observedFingerprint))
                    {
                        evidence["configuration.fingerprint"] = observedFingerprint;
                    }

                    return Verified(
                        "topic_alter_verified",
                        accepted,
                        evidence);
                },
                Unverified("topic_alter_verification_inconclusive", accepted),
                cancellationToken)
            .ConfigureAwait(false);

        return result;
    }

    public async Task<MutationProviderResult> IncreasePartitionsAsync(
        TopicIncreasePartitionsMutation mutation,
        CancellationToken cancellationToken = default)
    {
        var accepted = await _mutations.IncreasePartitionsAsync(
                mutation,
                cancellationToken)
            .ConfigureAwait(false);

        if (accepted.ResultKind != MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        return await VerifyUntilAsync(
                mutation.ClusterId,
                async (operation, token) =>
                {
                    var metadata = await _reads.GetTopicMetadataAsync(
                            mutation.ClusterId,
                            mutation.TopicName,
                            operation,
                            token)
                        .ConfigureAwait(false);

                    return metadata.IsSuccess &&
                           metadata.Value is not null &&
                           !metadata.Value.IsInternal &&
                           metadata.Value.Partitions.Count == mutation.NewPartitionCount;
                },
                Verified(
                    "topic_partitions_verified",
                    accepted,
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["resource.exists"] = "true",
                        ["partition.count"] = mutation.NewPartitionCount.ToString(
                            System.Globalization.CultureInfo.InvariantCulture),
                    }),
                Unverified("topic_partitions_verification_inconclusive", accepted),
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<MutationProviderResult> DeleteAsync(
        TopicDeleteMutation mutation,
        CancellationToken cancellationToken = default)
    {
        var accepted = await _mutations.DeleteTopicAsync(
                mutation,
                cancellationToken)
            .ConfigureAwait(false);

        if (accepted.ResultKind != MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        return await VerifyUntilAsync(
                mutation.ClusterId,
                async (operation, token) =>
                {
                    var metadata = await _reads.GetTopicMetadataAsync(
                            mutation.ClusterId,
                            mutation.TopicName,
                            operation,
                            token)
                        .ConfigureAwait(false);

                    return !metadata.IsSuccess &&
                           metadata.Failure is not null &&
                           IsTopicMissing(metadata.Failure);
                },
                Verified(
                    "topic_delete_verified",
                    accepted,
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["resource.exists"] = "false",
                    }),
                Unverified("topic_delete_verification_inconclusive", accepted),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<MutationProviderResult> VerifyUntilAsync(
        string clusterId,
        Func<KafkaOperationContext, CancellationToken, Task<bool>> observe,
        MutationProviderResult verified,
        MutationProviderResult unverified,
        CancellationToken cancellationToken) =>
        await VerifyUntilAsync(
                clusterId,
                observe,
                () => verified,
                unverified,
                cancellationToken)
            .ConfigureAwait(false);

    private async Task<MutationProviderResult> VerifyUntilAsync(
        string clusterId,
        Func<KafkaOperationContext, CancellationToken, Task<bool>> observe,
        Func<MutationProviderResult> verifiedFactory,
        MutationProviderResult unverified,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentNullException.ThrowIfNull(observe);
        ArgumentNullException.ThrowIfNull(verifiedFactory);

        var deadline = _timeProvider.GetUtcNow().Add(_policy.Timeout);

        while (_timeProvider.GetUtcNow() < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var operation = new KafkaOperationContext(deadline);
            try
            {
                if (await observe(operation, cancellationToken).ConfigureAwait(false))
                {
                    return verifiedFactory();
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                // Provider acceptance is already known. Verification exceptions are
                // evidence gaps, not proof that the mutation failed.
            }

            var remaining = deadline - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                break;
            }

            var delay = remaining < _policy.PollInterval
                ? remaining
                : _policy.PollInterval;

            await Task.Delay(delay, _timeProvider, cancellationToken)
                .ConfigureAwait(false);
        }

        return unverified;
    }

    private static MutationProviderResult Verified(
        string code,
        MutationProviderResult accepted,
        IReadOnlyDictionary<string, string> additionalEvidence)
    {
        var evidence = MergeEvidence(accepted.SafeEvidence, additionalEvidence);
        evidence["verification.state"] = "observed";

        return new MutationProviderResult(
            MutationExecutionResultKind.AppliedVerified,
            code,
            evidence);
    }

    private static MutationProviderResult Unverified(
        string code,
        MutationProviderResult accepted)
    {
        var evidence = MergeEvidence(
            accepted.SafeEvidence,
            new Dictionary<string, string>(StringComparer.Ordinal));
        evidence["verification.state"] = "inconclusive";

        return new MutationProviderResult(
            MutationExecutionResultKind.AppliedUnverified,
            code,
            evidence);
    }

    private static Dictionary<string, string> MergeEvidence(
        IReadOnlyDictionary<string, string>? left,
        IReadOnlyDictionary<string, string> right)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (left is not null)
        {
            foreach (var pair in left)
            {
                result[pair.Key] = pair.Value;
            }
        }

        foreach (var pair in right)
        {
            result[pair.Key] = pair.Value;
        }

        return result;
    }

    private static bool IsTopicMissing(KafkaFailure failure) =>
        failure.Code is
            "kafka_unknowntopicorpart" or
            "kafka_local_unknowntopic" or
            "kafka_resourcenotfound";
}

public abstract class TopicMutationExecutionHandlerBase<TMutation> :
    IMutationExecutionHandler
    where TMutation : class
{
    private readonly TopicMutationExecutionService _service;

    protected TopicMutationExecutionHandlerBase(
        TopicMutationExecutionService service)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
    }

    public abstract MutationOperationKind OperationKind { get; }

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Operation.OperationKind != OperationKind)
        {
            throw new MutationStateException(
                $"Topic mutation handler '{OperationKind}' cannot execute '{context.Operation.OperationKind}'.");
        }

        var mutation = TopicMutationPolicy.Deserialize<TMutation>(
            context.Operation.CanonicalIntent);
        return ExecuteTypedAsync(
            _service,
            mutation,
            cancellationToken);
    }

    protected abstract Task<MutationProviderResult> ExecuteTypedAsync(
        TopicMutationExecutionService service,
        TMutation mutation,
        CancellationToken cancellationToken);
}

public sealed class TopicCreateExecutionHandler :
    TopicMutationExecutionHandlerBase<TopicCreateMutation>
{
    public TopicCreateExecutionHandler(TopicMutationExecutionService service)
        : base(service)
    {
    }

    public override MutationOperationKind OperationKind =>
        MutationOperationKind.TopicCreate;

    protected override Task<MutationProviderResult> ExecuteTypedAsync(
        TopicMutationExecutionService service,
        TopicCreateMutation mutation,
        CancellationToken cancellationToken) =>
        service.CreateAsync(mutation, cancellationToken);
}

public sealed class TopicAlterExecutionHandler :
    TopicMutationExecutionHandlerBase<TopicAlterMutation>
{
    public TopicAlterExecutionHandler(TopicMutationExecutionService service)
        : base(service)
    {
    }

    public override MutationOperationKind OperationKind =>
        MutationOperationKind.TopicAlter;

    protected override Task<MutationProviderResult> ExecuteTypedAsync(
        TopicMutationExecutionService service,
        TopicAlterMutation mutation,
        CancellationToken cancellationToken) =>
        service.AlterAsync(mutation, cancellationToken);
}

public sealed class TopicIncreasePartitionsExecutionHandler :
    TopicMutationExecutionHandlerBase<TopicIncreasePartitionsMutation>
{
    public TopicIncreasePartitionsExecutionHandler(
        TopicMutationExecutionService service)
        : base(service)
    {
    }

    public override MutationOperationKind OperationKind =>
        MutationOperationKind.TopicIncreasePartitions;

    protected override Task<MutationProviderResult> ExecuteTypedAsync(
        TopicMutationExecutionService service,
        TopicIncreasePartitionsMutation mutation,
        CancellationToken cancellationToken) =>
        service.IncreasePartitionsAsync(mutation, cancellationToken);
}

public sealed class TopicDeleteExecutionHandler :
    TopicMutationExecutionHandlerBase<TopicDeleteMutation>
{
    public TopicDeleteExecutionHandler(TopicMutationExecutionService service)
        : base(service)
    {
    }

    public override MutationOperationKind OperationKind =>
        MutationOperationKind.TopicDelete;

    protected override Task<MutationProviderResult> ExecuteTypedAsync(
        TopicMutationExecutionService service,
        TopicDeleteMutation mutation,
        CancellationToken cancellationToken) =>
        service.DeleteAsync(mutation, cancellationToken);
}
