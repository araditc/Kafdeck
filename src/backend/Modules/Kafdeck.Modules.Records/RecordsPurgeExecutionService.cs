using System.Globalization;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public sealed record RecordsPurgeVerificationPolicy
{
    public RecordsPurgeVerificationPolicy(
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

    public static RecordsPurgeVerificationPolicy Default { get; } =
        new(
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(250));
}

public sealed class RecordsPurgeExecutionService
{
    private readonly IRecordsPurgeMutationPort _purge;
    private readonly IRecordsPurgeObservationPort _observations;
    private readonly RecordsPurgeVerificationPolicy _verification;
    private readonly TimeProvider _timeProvider;

    public RecordsPurgeExecutionService(
        IRecordsPurgeMutationPort purge,
        IRecordsPurgeObservationPort observations,
        RecordsPurgeVerificationPolicy? verification = null,
        TimeProvider? timeProvider = null)
    {
        _purge = purge ?? throw new ArgumentNullException(nameof(purge));
        _observations = observations ??
            throw new ArgumentNullException(nameof(observations));
        _verification =
            verification ?? RecordsPurgeVerificationPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.Operation.OperationKind !=
            MutationOperationKind.RecordsPurge)
        {
            return Unknown(
                "records_purge_operation_mismatch",
                0);
        }

        RecordsPurgeCanonicalIntent canonical;
        try
        {
            canonical =
                RecordsPurgeCanonicalization.Deserialize(
                    context.Operation.CanonicalIntent);
        }
        catch
        {
            return Unknown(
                "records_purge_canonical_invalid",
                0);
        }

        var canonicalTargetCount =
            canonical.Targets?.Count ?? 0;

        if (!RecordsPurgeCanonicalValidator.TryBuildProviderTargets(
                canonical,
                out var targets))
        {
            return Unknown(
                "records_purge_canonical_invalid",
                canonicalTargetCount);
        }

        var now = _timeProvider.GetUtcNow();
        var outerDeadline =
            context.ExecutionDeadlineUtc ??
            now.Add(_verification.Timeout);

        if (outerDeadline <= now)
        {
            return Unknown(
                "records_purge_execution_deadline_elapsed",
                targets.Length);
        }

        MutationProviderResult accepted;
        try
        {
            accepted = await _purge.PurgeAsync(
                    new RecordsPurgeMutation(
                        canonical.ClusterId,
                        targets),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return Unknown(
                "records_purge_cancelled_or_timeout",
                targets.Length);
        }
        catch
        {
            return Unknown(
                "records_purge_provider_exception",
                targets.Length);
        }

        if (accepted.ResultKind !=
            MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        var localDeadline =
            _timeProvider.GetUtcNow().Add(_verification.Timeout);
        var verificationDeadline =
            localDeadline < outerDeadline
                ? localDeadline
                : outerDeadline;

        var expected = targets.ToDictionary(
            target => (target.TopicName, target.Partition),
            target => target.BeforeOffset);

        var verification = await VerifyUntilAsync(
                canonical.ClusterId,
                targets
                    .Select(target =>
                        new RecordsPurgeObservationTarget(
                            target.TopicName,
                            target.Partition))
                    .ToArray(),
                observation =>
                {
                    var byPartition = observation.ToDictionary(
                        item => (item.Topic, item.Partition),
                        item => item.LowWatermark);

                    var matches = expected.Count(pair =>
                        byPartition.TryGetValue(
                            pair.Key,
                            out var observedLow) &&
                        observedLow >= pair.Value);

                    return (
                        matches == expected.Count,
                        matches);
                },
                verificationDeadline,
                cancellationToken)
            .ConfigureAwait(false);

        if (verification.Verified)
        {
            return Verified(
                "records_purge_verified",
                accepted,
                targets.Length);
        }

        if (verification.MatchedCount > 0)
        {
            return UnverifiedWithPartialEvidence(
                "records_purge_partially_verified",
                accepted,
                targets.Length,
                verification.MatchedCount);
        }

        return Unverified(
            "records_purge_verification_inconclusive",
            accepted,
            targets.Length);
    }

    private async Task<(bool Verified, int MatchedCount)> VerifyUntilAsync(
        string clusterId,
        IReadOnlyList<RecordsPurgeObservationTarget> targets,
        Func<
            IReadOnlyList<RecordsPurgePartitionObservation>,
            (bool Verified, int MatchedCount)> verify,
        DateTimeOffset deadline,
        CancellationToken cancellationToken)
    {
        var matchedCount = 0;

        while (_timeProvider.GetUtcNow() < deadline)
        {
            if (cancellationToken.IsCancellationRequested)
                return (false, matchedCount);

            try
            {
                var observed = await _observations.ObserveAsync(
                        clusterId,
                        targets,
                        new KafkaOperationContext(deadline),
                        cancellationToken)
                    .ConfigureAwait(false);

                if (observed.IsSuccess &&
                    observed.Value is not null)
                {
                    var result = verify(observed.Value);
                    matchedCount = Math.Max(
                        matchedCount,
                        result.MatchedCount);

                    if (result.Verified)
                        return (
                            true,
                            result.MatchedCount);
                }
            }
            catch (OperationCanceledException)
            {
                return (false, matchedCount);
            }
            catch
            {
                // Provider acceptance is already known.
                // Verification failure is an evidence gap.
            }

            var remaining =
                deadline - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
                break;

            var delay =
                remaining < _verification.PollInterval
                    ? remaining
                    : _verification.PollInterval;

            try
            {
                await Task.Delay(
                        delay,
                        _timeProvider,
                        cancellationToken)
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
        var evidence =
            MergeEvidence(accepted.SafeEvidence);
        evidence["verification.state"] = "observed";
        evidence["verified.count"] =
            targetCount.ToString(
                CultureInfo.InvariantCulture);

        return new(
            MutationExecutionResultKind.AppliedVerified,
            code,
            evidence);
    }

    private static MutationProviderResult
        UnverifiedWithPartialEvidence(
            string code,
            MutationProviderResult accepted,
            int targetCount,
            int verifiedCount)
    {
        var evidence =
            MergeEvidence(accepted.SafeEvidence);
        evidence["verification.state"] = "partial";
        evidence["target.count"] =
            targetCount.ToString(
                CultureInfo.InvariantCulture);
        evidence["verified.count"] =
            verifiedCount.ToString(
                CultureInfo.InvariantCulture);

        return new(
            MutationExecutionResultKind.AppliedUnverified,
            code,
            evidence);
    }

    private static MutationProviderResult Unverified(
        string code,
        MutationProviderResult accepted,
        int targetCount)
    {
        var evidence =
            MergeEvidence(accepted.SafeEvidence);
        evidence["verification.state"] = "inconclusive";
        evidence["target.count"] =
            targetCount.ToString(
                CultureInfo.InvariantCulture);

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
            new Dictionary<string, string>(
                StringComparer.Ordinal)
            {
                ["target.count"] =
                    Math.Max(0, targetCount)
                        .ToString(
                            CultureInfo.InvariantCulture),
                ["verification.state"] = "unknown",
            });

    private static Dictionary<string, string> MergeEvidence(
        IReadOnlyDictionary<string, string>? source)
    {
        var result =
            new Dictionary<string, string>(
                StringComparer.Ordinal);

        if (source is not null)
        {
            foreach (var pair in source)
                result[pair.Key] = pair.Value;
        }

        return result;
    }
}

public sealed class RecordsPurgeExecutionHandler :
    IMutationExecutionHandler
{
    private readonly RecordsPurgeExecutionService _service;

    public RecordsPurgeExecutionHandler(
        RecordsPurgeExecutionService service)
    {
        _service = service ??
            throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.RecordsPurge;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default) =>
        _service.ExecuteAsync(
            context,
            cancellationToken);
}
