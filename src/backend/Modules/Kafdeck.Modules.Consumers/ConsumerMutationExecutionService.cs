using System.Globalization;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Consumers;

public sealed record ConsumerMutationVerificationPolicy
{
    public ConsumerMutationVerificationPolicy(
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

    public static ConsumerMutationVerificationPolicy Default { get; } =
        new(
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(250));
}

public sealed class ConsumerMutationExecutionService
{
    private readonly IConsumerMutationPort _mutations;
    private readonly IConsumerMutationObservationPort _observations;
    private readonly ConsumerMutationVerificationPolicy _verification;
    private readonly TimeProvider _timeProvider;

    public ConsumerMutationExecutionService(
        IConsumerMutationPort mutations,
        IConsumerMutationObservationPort observations,
        ConsumerMutationVerificationPolicy? verification = null,
        TimeProvider? timeProvider = null)
    {
        _mutations = mutations ?? throw new ArgumentNullException(nameof(mutations));
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
        _verification =
            verification ?? ConsumerMutationVerificationPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationProviderResult> AlterOffsetsAsync(
        ConsumerOffsetAlterCanonicalIntent canonical,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(canonical);
        var canonicalTargetCount = canonical.Targets?.Count ?? 0;
        if (!TryBuildAlterTargets(canonical, out var targets))
        {
            return Unknown(
                "consumer_offset_canonical_invalid",
                canonicalTargetCount);
        }

        MutationProviderResult accepted;
        try
        {
            accepted = await _mutations.AlterOffsetsAsync(
                    new ConsumerOffsetAlterMutation(
                        canonical.ClusterId,
                        canonical.GroupId,
                        targets),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return Unknown("consumer_offset_alter_cancelled_or_timeout", targets.Length);
        }
        catch
        {
            return Unknown("consumer_offset_alter_provider_exception", targets.Length);
        }

        if (accepted.ResultKind != MutationExecutionResultKind.AppliedUnverified)
            return accepted;

        var expected = targets.ToDictionary(
            target => (target.TopicName, target.Partition),
            target => target.Offset);

        var verification = await VerifyUntilAsync(
                canonical.ClusterId,
                canonical.GroupId,
                targets.Select(target => new ConsumerMutationObservationTarget(
                    target.TopicName,
                    target.Partition))
                    .ToArray(),
                observation =>
                {
                    if (!observation.Exists)
                        return (false, 0);

                    var actual = observation.Partitions.ToDictionary(
                        item => (item.Topic, item.Partition),
                        item => item.CommittedOffset);
                    var matches = expected.Count(pair =>
                        actual.TryGetValue(pair.Key, out var observed) &&
                        observed == pair.Value);

                    return (matches == expected.Count, matches);
                },
                cancellationToken)
            .ConfigureAwait(false);

        if (verification.Verified)
        {
            return Verified(
                "consumer_offset_alter_verified",
                accepted,
                targets.Length);
        }

        if (verification.MatchedCount is > 0 &&
            verification.MatchedCount < targets.Length)
        {
            return Partial(
                "consumer_offset_alter_partially_verified",
                accepted,
                targets.Length,
                verification.MatchedCount);
        }

        return Unverified(
            "consumer_offset_alter_verification_inconclusive",
            accepted,
            targets.Length);
    }

    public async Task<MutationProviderResult> DeleteAsync(
        ConsumerDeleteCanonicalIntent canonical,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(canonical);
        var canonicalTargetCount = canonical.Targets?.Count ?? 0;
        if (!TryValidateCanonicalIdentity(
                canonical.ClusterId,
                canonical.GroupId,
                canonical.GroupState,
                canonical.GroupFingerprint) ||
            !Enum.IsDefined(canonical.Mode) ||
            canonical.Targets is null ||
            canonical.Targets.Count > ConsumerMutationPolicy.HardMaxTargets)
        {
            return Unknown(
                "consumer_delete_canonical_invalid",
                canonicalTargetCount);
        }

        if (canonical.Mode == ConsumerDeleteMode.Group)
        {
            if (canonical.Targets.Count != 0)
                return Unknown("consumer_group_delete_canonical_invalid", 0);

            MutationProviderResult accepted;
            try
            {
                accepted = await _mutations.DeleteAsync(
                        new ConsumerDeleteMutation(
                            canonical.ClusterId,
                            canonical.GroupId),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return Unknown("consumer_group_delete_cancelled_or_timeout", 1);
            }
            catch
            {
                return Unknown("consumer_group_delete_provider_exception", 1);
            }

            if (accepted.ResultKind != MutationExecutionResultKind.AppliedUnverified)
                return accepted;

            var verification = await VerifyUntilAsync(
                    canonical.ClusterId,
                    canonical.GroupId,
                    Array.Empty<ConsumerMutationObservationTarget>(),
                    observation => (!observation.Exists, observation.Exists ? 0 : 1),
                    cancellationToken)
                .ConfigureAwait(false);

            return verification.Verified
                ? Verified("consumer_group_delete_verified", accepted, 1)
                : Unverified(
                    "consumer_group_delete_verification_inconclusive",
                    accepted,
                    1);
        }

        if (!TryBuildDeleteTargets(canonical, out var targets))
        {
            return Unknown(
                "consumer_offset_delete_canonical_invalid",
                canonicalTargetCount);
        }

        MutationProviderResult deleteAccepted;
        try
        {
            deleteAccepted = await _mutations.DeleteAsync(
                    new ConsumerDeleteMutation(
                        canonical.ClusterId,
                        canonical.GroupId,
                        targets),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return Unknown("consumer_offset_delete_cancelled_or_timeout", targets.Length);
        }
        catch
        {
            return Unknown("consumer_offset_delete_provider_exception", targets.Length);
        }

        if (deleteAccepted.ResultKind != MutationExecutionResultKind.AppliedUnverified)
            return deleteAccepted;

        var expectedPartitions = targets
            .Select(target => (target.TopicName, target.Partition))
            .ToHashSet();

        var offsetVerification = await VerifyUntilAsync(
                canonical.ClusterId,
                canonical.GroupId,
                targets.Select(target => new ConsumerMutationObservationTarget(
                    target.TopicName,
                    target.Partition,
                    RequireStableOffset: false))
                    .ToArray(),
                observation =>
                {
                    if (!observation.Exists)
                        return (false, 0);

                    var missing = observation.Partitions.Count(item =>
                        expectedPartitions.Contains((item.Topic, item.Partition)) &&
                        item.CommittedOffset is null);
                    return (missing == expectedPartitions.Count, missing);
                },
                cancellationToken)
            .ConfigureAwait(false);

        if (offsetVerification.Verified)
        {
            return Verified(
                "consumer_offset_delete_verified",
                deleteAccepted,
                targets.Length);
        }

        if (offsetVerification.MatchedCount is > 0 &&
            offsetVerification.MatchedCount < targets.Length)
        {
            return Partial(
                "consumer_offset_delete_partially_verified",
                deleteAccepted,
                targets.Length,
                offsetVerification.MatchedCount);
        }

        return Unverified(
            "consumer_offset_delete_verification_inconclusive",
            deleteAccepted,
            targets.Length);
    }

    private static bool TryBuildAlterTargets(
        ConsumerOffsetAlterCanonicalIntent canonical,
        out ConsumerOffsetTarget[] targets)
    {
        targets = Array.Empty<ConsumerOffsetTarget>();

        if (!TryValidateCanonicalIdentity(
                canonical.ClusterId,
                canonical.GroupId,
                canonical.GroupState,
                canonical.GroupFingerprint) ||
            canonical.Targets is null ||
            canonical.Targets.Count is < 1 or > ConsumerMutationPolicy.HardMaxTargets ||
            canonical.Targets.Any(static target => target is null) ||
            canonical.TotalBackwardDistance < 0 ||
            canonical.TotalForwardDistance < 0 ||
            canonical.MissingCommittedOffsetCount < 0 ||
            canonical.MissingCommittedOffsetCount > canonical.Targets.Count)
        {
            return false;
        }

        try
        {
            var seen = new HashSet<(string Topic, int Partition)>();
            var result = new List<ConsumerOffsetTarget>(canonical.Targets.Count);
            long backwardDistance = 0;
            long forwardDistance = 0;
            var missingCount = 0;

            foreach (var target in canonical.Targets.OrderBy(item => item.Ordinal))
            {
                if (target is null ||
                    target.Ordinal != result.Count ||
                    target.Selector is null ||
                    !Enum.IsDefined(target.Selector.Kind) ||
                    !Enum.IsDefined(target.Movement) ||
                    target.Partition < 0 ||
                    target.ResolvedOffset < 0 ||
                    target.LowWatermark < 0 ||
                    target.HighWatermark < target.LowWatermark ||
                    target.ResolvedOffset < target.LowWatermark ||
                    target.ResolvedOffset > target.HighWatermark ||
                    target.CommittedOffset is < 0 ||
                    target.CommittedOffsetMissing != !target.CommittedOffset.HasValue)
                {
                    return false;
                }

                var topic = ConsumerMutationCanonicalization.RequireTopicName(
                    target.TopicName);
                if (!seen.Add((topic, target.Partition)))
                    return false;

                long? expectedDelta = target.CommittedOffset.HasValue
                    ? checked(target.ResolvedOffset - target.CommittedOffset.Value)
                    : null;
                if (target.DeltaFromCommitted != expectedDelta)
                    return false;

                var expectedMovement = expectedDelta switch
                {
                    null => ConsumerOffsetMovement.MissingCurrent,
                    < 0 => ConsumerOffsetMovement.BackwardReplay,
                    > 0 => ConsumerOffsetMovement.ForwardSkip,
                    _ => ConsumerOffsetMovement.Unchanged,
                };
                if (target.Movement != expectedMovement)
                    return false;

                if (!expectedDelta.HasValue)
                {
                    missingCount++;
                }
                else if (expectedDelta.Value < 0)
                {
                    backwardDistance = checked(
                        backwardDistance - expectedDelta.Value);
                }
                else if (expectedDelta.Value > 0)
                {
                    forwardDistance = checked(
                        forwardDistance + expectedDelta.Value);
                }

                result.Add(
                    new ConsumerOffsetTarget(
                        topic,
                        target.Partition,
                        target.ResolvedOffset));
            }

            if (backwardDistance != canonical.TotalBackwardDistance ||
                forwardDistance != canonical.TotalForwardDistance ||
                missingCount != canonical.MissingCommittedOffsetCount)
            {
                return false;
            }

            targets = result.ToArray();
            return true;
        }
        catch (Exception exception) when (
            exception is ArgumentException or
            OverflowException)
        {
            return false;
        }
    }

    private static bool TryBuildDeleteTargets(
        ConsumerDeleteCanonicalIntent canonical,
        out ConsumerOffsetTarget[] targets)
    {
        targets = Array.Empty<ConsumerOffsetTarget>();
        if (canonical.Mode != ConsumerDeleteMode.Offsets ||
            canonical.Targets is null ||
            canonical.Targets.Count is < 1 or > ConsumerMutationPolicy.HardMaxTargets ||
            canonical.Targets.Any(static target => target is null))
        {
            return false;
        }

        try
        {
            var seen = new HashSet<(string Topic, int Partition)>();
            var result = new List<ConsumerOffsetTarget>(canonical.Targets.Count);

            foreach (var target in canonical.Targets.OrderBy(item => item.Ordinal))
            {
                if (target is null ||
                    target.Ordinal != result.Count ||
                    target.Partition < 0 ||
                    target.CommittedOffsetMissing ||
                    target.CommittedOffset is null or < 0 ||
                    target.LowWatermark < 0 ||
                    target.HighWatermark < target.LowWatermark)
                {
                    return false;
                }

                var topic = ConsumerMutationCanonicalization.RequireTopicName(
                    target.TopicName);
                if (!seen.Add((topic, target.Partition)))
                    return false;

                result.Add(
                    new ConsumerOffsetTarget(
                        topic,
                        target.Partition,
                        target.CommittedOffset.Value));
            }

            targets = result.ToArray();
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static bool TryValidateCanonicalIdentity(
        string clusterId,
        string groupId,
        ConsumerGroupState groupState,
        string groupFingerprint)
    {
        if (groupState != ConsumerGroupState.Empty)
            return false;

        try
        {
            _ = ConsumerMutationCanonicalization.RequireIdentifier(
                clusterId,
                "Cluster ID",
                256);
            _ = ConsumerMutationCanonicalization.RequireIdentifier(
                groupId,
                "Consumer group ID",
                255);
            var fingerprint =
                ConsumerMutationCanonicalization.RequireIdentifier(
                    groupFingerprint,
                    "Consumer group fingerprint",
                    128);

            return fingerprint.Length == 64 &&
                   fingerprint.All(char.IsAsciiHexDigit);
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private async Task<(bool Verified, int MatchedCount)> VerifyUntilAsync(
        string clusterId,
        string groupId,
        IReadOnlyList<ConsumerMutationObservationTarget> targets,
        Func<ConsumerMutationObservation, (bool Verified, int MatchedCount)> verify,
        CancellationToken cancellationToken)
    {
        var deadline = _timeProvider.GetUtcNow().Add(_verification.Timeout);
        var matchedCount = 0;

        while (_timeProvider.GetUtcNow() < deadline)
        {
            if (cancellationToken.IsCancellationRequested)
                return (false, matchedCount);

            try
            {
                var observed = await _observations.ObserveAsync(
                        clusterId,
                        groupId,
                        targets,
                        new KafkaOperationContext(deadline),
                        cancellationToken)
                    .ConfigureAwait(false);

                if (observed.IsSuccess && observed.Value is not null)
                {
                    var result = verify(observed.Value);
                    matchedCount = Math.Max(matchedCount, result.MatchedCount);
                    if (result.Verified)
                        return (true, result.MatchedCount);
                }
            }
            catch (OperationCanceledException)
            {
                return (false, matchedCount);
            }
            catch
            {
                // Provider acceptance is already known. A read-back failure is an
                // evidence gap, never proof that the mutation failed.
            }

            var remaining = deadline - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
                break;

            var delay = remaining < _verification.PollInterval
                ? remaining
                : _verification.PollInterval;

            try
            {
                await Task.Delay(delay, _timeProvider, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return (false, matchedCount);
            }
        }

        return (false, matchedCount);
    }

    private static MutationProviderResult Verified(
        string code,
        MutationProviderResult accepted,
        int targetCount)
    {
        var evidence = MergeEvidence(accepted.SafeEvidence);
        evidence["verification.state"] = "observed";
        evidence["verified.count"] =
            targetCount.ToString(CultureInfo.InvariantCulture);

        return new(
            MutationExecutionResultKind.AppliedVerified,
            code,
            evidence);
    }

    private static MutationProviderResult Partial(
        string code,
        MutationProviderResult accepted,
        int targetCount,
        int verifiedCount)
    {
        var evidence = MergeEvidence(accepted.SafeEvidence);
        evidence["verification.state"] = "partial";
        evidence["target.count"] =
            targetCount.ToString(CultureInfo.InvariantCulture);
        evidence["verified.count"] =
            verifiedCount.ToString(CultureInfo.InvariantCulture);

        return new(
            MutationExecutionResultKind.PartiallyApplied,
            code,
            evidence);
    }

    private static MutationProviderResult Unverified(
        string code,
        MutationProviderResult accepted,
        int targetCount)
    {
        var evidence = MergeEvidence(accepted.SafeEvidence);
        evidence["verification.state"] = "inconclusive";
        evidence["target.count"] =
            targetCount.ToString(CultureInfo.InvariantCulture);

        return new(
            MutationExecutionResultKind.AppliedUnverified,
            code,
            evidence);
    }

    private static MutationProviderResult Unknown(
        string code,
        int targetCount) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            code,
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["target.count"] =
                    Math.Max(0, targetCount)
                        .ToString(CultureInfo.InvariantCulture),
                ["verification.state"] = "unknown",
            });

    private static Dictionary<string, string> MergeEvidence(
        IReadOnlyDictionary<string, string>? source)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (source is not null)
        {
            foreach (var pair in source)
                result[pair.Key] = pair.Value;
        }

        return result;
    }
}

public sealed class ConsumerOffsetAlterExecutionHandler :
    IMutationExecutionHandler
{
    private readonly ConsumerMutationExecutionService _service;

    public ConsumerOffsetAlterExecutionHandler(
        ConsumerMutationExecutionService service)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.ConsumerOffsetAlter;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Operation.OperationKind != OperationKind)
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "consumer_offset_operation_mismatch"));

        ConsumerOffsetAlterCanonicalIntent canonical;
        try
        {
            canonical =
                ConsumerMutationCanonicalization
                    .Deserialize<ConsumerOffsetAlterCanonicalIntent>(
                        context.Operation.CanonicalIntent);
        }
        catch
        {
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "consumer_offset_canonical_invalid"));
        }

        return _service.AlterOffsetsAsync(canonical, cancellationToken);
    }
}

public sealed class ConsumerDeleteExecutionHandler :
    IMutationExecutionHandler
{
    private readonly ConsumerMutationExecutionService _service;

    public ConsumerDeleteExecutionHandler(
        ConsumerMutationExecutionService service)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.ConsumerDelete;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Operation.OperationKind != OperationKind)
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "consumer_delete_operation_mismatch"));

        ConsumerDeleteCanonicalIntent canonical;
        try
        {
            canonical =
                ConsumerMutationCanonicalization
                    .Deserialize<ConsumerDeleteCanonicalIntent>(
                        context.Operation.CanonicalIntent);
        }
        catch
        {
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "consumer_delete_canonical_invalid"));
        }

        return _service.DeleteAsync(canonical, cancellationToken);
    }
}
