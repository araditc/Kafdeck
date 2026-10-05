using System.Text;
using Kafdeck.Core.Kafka;

namespace Kafdeck.Core.Records;

public sealed record DataQualityMonitoringWorkerPolicy
{
    public const int DefaultMaxClustersPerCycle = 16;
    public const int HardMaxClustersPerCycle = 64;
    public const int DefaultMaxPoliciesPerCluster = 20;
    public const int HardMaxPoliciesPerCluster =
        DataQualityPolicyBudget.HardMaxActivePoliciesPerCluster;
    public const int DefaultMaxEvaluationsPerCycle = 128;
    public const int HardMaxEvaluationsPerCycle = 1_024;
    public const int DefaultMaxRecordsPerEvaluation = 200;
    public const long DefaultMaxRawBytesPerEvaluation =
        2L * 1024 * 1024;
    public static readonly TimeSpan DefaultMaxDurationPerEvaluation =
        TimeSpan.FromSeconds(10);

    public DataQualityMonitoringWorkerPolicy(
        int maxClustersPerCycle = DefaultMaxClustersPerCycle,
        int maxPoliciesPerCluster = DefaultMaxPoliciesPerCluster,
        int maxEvaluationsPerCycle = DefaultMaxEvaluationsPerCycle,
        int maxRecordsPerEvaluation = DefaultMaxRecordsPerEvaluation,
        long maxRawBytesPerEvaluation =
            DefaultMaxRawBytesPerEvaluation,
        TimeSpan? maxDurationPerEvaluation = null)
    {
        var duration =
            maxDurationPerEvaluation ??
            DefaultMaxDurationPerEvaluation;

        if (maxClustersPerCycle is < 1 or >
                HardMaxClustersPerCycle ||
            maxPoliciesPerCluster is < 1 or >
                HardMaxPoliciesPerCluster ||
            maxEvaluationsPerCycle is < 1 or >
                HardMaxEvaluationsPerCycle ||
            maxRecordsPerEvaluation is < 1 or >
                RecordOperationBudget.HardMaxRecords ||
            maxRawBytesPerEvaluation is < 1 or >
                RecordOperationBudget.HardMaxRawBytes ||
            duration <= TimeSpan.Zero ||
            duration > RecordOperationBudget.HardMaxDuration)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxClustersPerCycle),
                "Data-quality monitoring worker policy exceeds a hard server-owned bound.");
        }

        MaxClustersPerCycle = maxClustersPerCycle;
        MaxPoliciesPerCluster = maxPoliciesPerCluster;
        MaxEvaluationsPerCycle = maxEvaluationsPerCycle;
        MaxRecordsPerEvaluation = maxRecordsPerEvaluation;
        MaxRawBytesPerEvaluation = maxRawBytesPerEvaluation;
        MaxDurationPerEvaluation = duration;
    }

    public int MaxClustersPerCycle { get; }
    public int MaxPoliciesPerCluster { get; }
    public int MaxEvaluationsPerCycle { get; }
    public int MaxRecordsPerEvaluation { get; }
    public long MaxRawBytesPerEvaluation { get; }
    public TimeSpan MaxDurationPerEvaluation { get; }

    public static DataQualityMonitoringWorkerPolicy Default { get; } =
        new();
}

public sealed record DataQualityMonitoringCycleRequest
{
    public DataQualityMonitoringCycleRequest(
        IReadOnlyList<string> clusterIds)
    {
        ArgumentNullException.ThrowIfNull(clusterIds);

        if (clusterIds.Count is < 1 or >
                DataQualityMonitoringWorkerPolicy
                    .HardMaxClustersPerCycle)
        {
            throw new ArgumentOutOfRangeException(
                nameof(clusterIds));
        }

        var normalized =
            new string[clusterIds.Count];

        for (var index = 0;
             index < clusterIds.Count;
             index++)
        {
            var clusterId =
                clusterIds[index];

            DataQualityContractInputBounds.RequireRawString(
                clusterId,
                256,
                nameof(clusterIds));
            ArgumentException.ThrowIfNullOrWhiteSpace(
                clusterId);

            if (!string.Equals(
                    clusterId,
                    clusterId.Trim(),
                    StringComparison.Ordinal) ||
                clusterId.Any(char.IsControl))
            {
                throw new ArgumentException(
                    "Data-quality monitoring cluster IDs must be exact bounded identifiers.",
                    nameof(clusterIds));
            }

            normalized[index] =
                clusterId;
        }

        if (normalized
            .Distinct(StringComparer.Ordinal)
            .Count() != normalized.Length)
        {
            throw new ArgumentException(
                "Data-quality monitoring cluster IDs must be unique.",
                nameof(clusterIds));
        }

        ClusterIds =
            Array.AsReadOnly(
                normalized);
    }

    public IReadOnlyList<string> ClusterIds { get; }
}

public interface IDataQualityMonitoringAuthorizationPort
{
    Task<bool> IsAuthorizedAsync(
        DataQualityPolicyDefinition policy,
        int partition,
        CancellationToken cancellationToken);
}

public sealed record DataQualityMonitoringCycleResult(
    int PoliciesVisited,
    int EvaluationsAttempted,
    int EvaluationsPersisted,
    int AuthorizationDenied,
    int SourceFailures,
    int EvaluationFailures,
    int TruncatedPolicyClusters,
    bool EvaluationLimitReached);

public sealed class DataQualityMonitoringWorker
{
    private readonly IDataQualityLifecycleStore
        _store;
    private readonly IKafkaRecordReadPort
        _recordReadPort;
    private readonly IDataQualityBatchEvaluator
        _evaluator;
    private readonly IDataQualityMonitoringAuthorizationPort
        _authorization;
    private readonly DataQualityMonitoringWorkerPolicy
        _policy;
    private readonly TimeProvider
        _timeProvider;
    private readonly SemaphoreSlim
        _cycleGate = new(1, 1);
    private readonly object
        _cursorGate = new();
    private readonly Dictionary<string, string>
        _policyCursors = new(
            StringComparer.Ordinal);
    private readonly Dictionary<string, int>
        _partitionCursors = new(
            StringComparer.Ordinal);
    private readonly Dictionary<string, long>
        _partitionResumeOffsets = new(
            StringComparer.Ordinal);
    private readonly Dictionary<string, PolicyRateWindowState>
        _policyRateWindows = new(
            StringComparer.Ordinal);
    private string?
        _clusterCursor;

    public DataQualityMonitoringWorker(
        IDataQualityLifecycleStore store,
        IKafkaRecordReadPort recordReadPort,
        IDataQualityBatchEvaluator evaluator,
        IDataQualityMonitoringAuthorizationPort authorization,
        DataQualityMonitoringWorkerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _store =
            store ??
            throw new ArgumentNullException(
                nameof(store));
        _recordReadPort =
            recordReadPort ??
            throw new ArgumentNullException(
                nameof(recordReadPort));
        _evaluator =
            evaluator ??
            throw new ArgumentNullException(
                nameof(evaluator));
        _authorization =
            authorization ??
            throw new ArgumentNullException(
                nameof(authorization));
        _policy =
            policy ??
            DataQualityMonitoringWorkerPolicy.Default;
        _timeProvider =
            timeProvider ??
            TimeProvider.System;
    }

    public async Task<DataQualityMonitoringCycleResult>
        RunCycleAsync(
            DataQualityMonitoringCycleRequest request,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        await _cycleGate
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        try
        {
            return await RunCycleCoreAsync(
                    request,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _cycleGate.Release();
        }
    }

    private async Task<DataQualityMonitoringCycleResult>
        RunCycleCoreAsync(
            DataQualityMonitoringCycleRequest request,
            CancellationToken cancellationToken)
    {
        if (request.ClusterIds.Count >
            _policy.MaxClustersPerCycle)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "Data-quality monitoring cycle exceeds the configured cluster ceiling.");
        }

        var clusters =
            RotateClusters(
                request.ClusterIds);

        var policiesVisited = 0;
        var evaluationsAttempted = 0;
        var evaluationsPersisted = 0;
        var authorizationDenied = 0;
        var sourceFailures = 0;
        var evaluationFailures = 0;
        var truncatedPolicyClusters = 0;
        var evaluationLimitReached = false;

        foreach (var clusterId in clusters)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var page =
                await LoadPolicyPageAsync(
                        clusterId,
                        cancellationToken)
                    .ConfigureAwait(false);

            var clusterTouched =
                false;

            if (page.Truncated ||
                GetPolicyCursor(
                    clusterId) is not null)
            {
                truncatedPolicyClusters++;
            }

            foreach (var snapshot in page.Items)
            {
                cancellationToken.ThrowIfCancellationRequested();
                policiesVisited++;

                var policyNow =
                    _timeProvider
                        .GetUtcNow();
                var allowance =
                    GetPolicyRateAllowance(
                        snapshot.Definition,
                        policyNow);
                var remainingPolicyRecords =
                    allowance.RemainingRecords;
                var remainingPolicyBytes =
                    allowance.RemainingBytes;
                var fullPolicyReadByteCeiling =
                    Math.Min(
                        _policy.MaxRawBytesPerEvaluation,
                        Math.Min(
                            RecordOperationBudget.HardMaxRawBytes,
                            snapshot.Definition.Budget
                                .BytesPerSecond));
                var partitions =
                    RotatePartitions(
                        snapshot.Definition);
                var policyTouched =
                    false;

                for (var partitionIndex = 0;
                     partitionIndex <
                         partitions.Count;
                     partitionIndex++)
                {
                    if (remainingPolicyRecords <= 0 ||
                        remainingPolicyBytes <= 0)
                    {
                        break;
                    }

                    var partition =
                        partitions[partitionIndex];
                    var remainingPartitions =
                        partitions.Count -
                        partitionIndex;
                    if (evaluationsAttempted >=
                        _policy.MaxEvaluationsPerCycle)
                    {
                        evaluationLimitReached = true;
                        break;
                    }

                    policyTouched =
                        true;

                    AdvancePartitionCursor(
                        snapshot.Definition,
                        partition);

                    if (!await _authorization
                            .IsAuthorizedAsync(
                                snapshot.Definition,
                                partition,
                                cancellationToken)
                            .ConfigureAwait(false))
                    {
                        authorizationDenied++;
                        continue;
                    }

                    evaluationsAttempted++;

                    var now =
                        _timeProvider
                            .GetUtcNow();
                    var readBudget =
                        BuildReadBudget(
                            snapshot.Definition.Budget,
                            remainingPolicyRecords,
                            remainingPolicyBytes,
                            remainingPartitions);
                    var windowStart =
                        now -
                        snapshot.Definition.Budget
                            .EvaluationWindow;
                    var resumeOffset =
                        GetPartitionResumeOffset(
                            snapshot.Definition,
                            partition);
                    var readAnchor =
                        resumeOffset is not null
                            ? RecordAnchor.AtOffset(
                                resumeOffset.Value)
                            : RecordAnchor.AtTimestamp(
                                windowStart);
                    var operation =
                        new KafkaOperationContext(
                            now +
                            readBudget.MaxDuration);

                    var read =
                        await _recordReadPort
                            .ReadPageAsync(
                                new RecordReadRequest(
                                    snapshot.Definition.Scope.ClusterId,
                                    snapshot.Definition.Scope.TopicName,
                                    partition,
                                    readAnchor,
                                    RecordReadDirection.Forward,
                                    readBudget),
                                operation,
                                cancellationToken)
                            .ConfigureAwait(false);

                    if (!read.IsSuccess ||
                        read.Value is null)
                    {
                        if (cancellationToken
                                .IsCancellationRequested ||
                            read.Failure?.Category ==
                                KafkaFailureCategory.Cancelled)
                        {
                            throw new OperationCanceledException(
                                "Data-quality monitoring source read was cancelled.",
                                cancellationToken);
                        }

                        if (string.Equals(
                                read.Failure?.Code,
                                "deadline_exceeded",
                                StringComparison.Ordinal))
                        {
                            var deadlineRateRecords =
                                Math.Min(
                                    remainingPolicyRecords,
                                    readBudget.MaxRecords);
                            var deadlineRateBytes =
                                Math.Min(
                                    remainingPolicyBytes,
                                    readBudget.MaxRawBytes);

                            remainingPolicyRecords =
                                Math.Max(
                                    0,
                                    remainingPolicyRecords -
                                    deadlineRateRecords);
                            remainingPolicyBytes =
                                Math.Max(
                                    0,
                                    remainingPolicyBytes -
                                    deadlineRateBytes);

                            ConsumePolicyRate(
                                snapshot.Definition,
                                _timeProvider.GetUtcNow(),
                                deadlineRateRecords,
                                deadlineRateBytes);

                            await _store
                                .AppendEvidenceAsync(
                                    new DataQualityEvidencePoint(
                                        BuildWorkerDeadlineResult(
                                            snapshot.Definition,
                                            partition,
                                            now)),
                                    cancellationToken)
                                .ConfigureAwait(false);
                            evaluationsPersisted++;
                            continue;
                        }

                        if (resumeOffset is not null &&
                            string.Equals(
                                read.Failure?.Code,
                                "offset_out_of_range",
                                StringComparison.Ordinal))
                        {
                            ClearPartitionResumeOffset(
                                snapshot.Definition,
                                partition);
                            continue;
                        }

                        sourceFailures++;
                        continue;
                    }

                    var sourceRecords =
                        read.Value.Records.Count;
                    var sourceBytes =
                        RawBatchBytes(
                            read.Value.Records);
                    var staleResumeBatch =
                        ResumeBatchIsEntirelyBeforeWindow(
                            resumeOffset,
                            read.Value,
                            windowStart);
                    var byteLimitedEmptyRead =
                        sourceRecords == 0 &&
                        (read.Value.BudgetOutcome is
                            RecordBudgetOutcome.RawByteLimit or
                            RecordBudgetOutcome.ProjectedByteLimit);
                    var underfundedByteRead =
                        byteLimitedEmptyRead &&
                        readBudget.MaxRawBytes <
                            fullPolicyReadByteCeiling;
                    var fullCeilingOversizedRead =
                        byteLimitedEmptyRead &&
                        !underfundedByteRead;

                    var rateRecords =
                        byteLimitedEmptyRead
                            ? Math.Min(
                                1,
                                remainingPolicyRecords)
                            : sourceRecords;
                    var rateBytes =
                        byteLimitedEmptyRead
                            ? remainingPolicyBytes
                            : sourceBytes;

                    remainingPolicyRecords =
                        Math.Max(
                            0,
                            remainingPolicyRecords -
                            rateRecords);
                    remainingPolicyBytes =
                        Math.Max(
                            0,
                            remainingPolicyBytes -
                            rateBytes);

                    ConsumePolicyRate(
                        snapshot.Definition,
                        _timeProvider.GetUtcNow(),
                        rateRecords,
                        rateBytes);

                    if (staleResumeBatch)
                    {
                        ClearPartitionResumeOffset(
                            snapshot.Definition,
                            partition);
                        continue;
                    }

                    try
                    {
                        var prepared =
                            PrepareEvaluationInput(
                                snapshot.Definition,
                                partition,
                                read.Value,
                                now,
                                readBudget);
                        var result =
                            await _evaluator
                                .EvaluateAsync(
                                    snapshot.Definition,
                                    prepared.Input,
                                    operation,
                                    cancellationToken)
                                .ConfigureAwait(false);

                        if (cancellationToken
                                .IsCancellationRequested ||
                            result.Progress.Outcome ==
                                DataQualityEvaluationOutcome.Cancelled)
                        {
                            throw new OperationCanceledException(
                                "Data-quality monitoring evaluation was cancelled.",
                                cancellationToken);
                        }

                        if (prepared.SourceOutcome is
                            { } sourceOutcome)
                        {
                            result =
                                ApplySourceOutcome(
                                    result,
                                    sourceOutcome,
                                    prepared.Input.Records.Count);
                        }

                        await _store
                            .AppendEvidenceAsync(
                                new DataQualityEvidencePoint(
                                    result),
                                cancellationToken)
                            .ConfigureAwait(false);

                        UpdatePartitionResumeAfterSuccessfulRead(
                            snapshot.Definition,
                            partition,
                            resumeOffset,
                            read.Value,
                            result,
                            prepared.SafeInspectedPrefixNextOffset,
                            underfundedByteRead,
                            fullCeilingOversizedRead);

                        evaluationsPersisted++;
                    }
                    catch (OperationCanceledException)
                        when (cancellationToken
                            .IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (
                        Exception exception)
                        when (exception is
                            ArgumentException or
                            InvalidOperationException or
                            TimeoutException)
                    {
                        evaluationFailures++;
                    }

                    if (underfundedByteRead)
                    {
                        RetainPartitionCursor(
                            snapshot.Definition,
                            partition);
                        break;
                    }
                }

                if (policyTouched)
                {
                    clusterTouched =
                        true;
                    SetPolicyCursor(
                        clusterId,
                        snapshot.Definition.PolicyId);
                }

                if (evaluationLimitReached)
                {
                    break;
                }
            }

            if (clusterTouched ||
                page.Items.Count == 0)
            {
                AdvanceClusterCursor(
                    clusterId);
            }

            if (evaluationLimitReached)
            {
                break;
            }
        }

        return new DataQualityMonitoringCycleResult(
            policiesVisited,
            evaluationsAttempted,
            evaluationsPersisted,
            authorizationDenied,
            sourceFailures,
            evaluationFailures,
            truncatedPolicyClusters,
            evaluationLimitReached);
    }

    private IReadOnlyList<string> RotateClusters(
        IReadOnlyList<string> clusterIds)
    {
        lock (_cursorGate)
        {
            var start = 0;
            if (_clusterCursor is not null)
            {
                for (var index = 0;
                     index < clusterIds.Count;
                     index++)
                {
                    if (string.Equals(
                            clusterIds[index],
                            _clusterCursor,
                            StringComparison.Ordinal))
                    {
                        start =
                            (index + 1) %
                            clusterIds.Count;
                        break;
                    }
                }
            }

            var rotated =
                new string[clusterIds.Count];
            for (var index = 0;
                 index < clusterIds.Count;
                 index++)
            {
                rotated[index] =
                    clusterIds[
                        (start + index) %
                        clusterIds.Count];
            }

            return Array.AsReadOnly(
                rotated);
        }
    }

    private void AdvanceClusterCursor(
        string clusterId)
    {
        lock (_cursorGate)
        {
            _clusterCursor =
                clusterId;
        }
    }

    private async Task<DataQualityPolicyPage>
        LoadPolicyPageAsync(
            string clusterId,
            CancellationToken cancellationToken)
    {
        var cursor =
            GetPolicyCursor(
                clusterId);
        var page =
            await _store
                .ListPoliciesAsync(
                    new DataQualityPolicyListQuery(
                        clusterId,
                        _policy.MaxPoliciesPerCluster,
                        DataQualityPolicyLifecycleState.Active,
                        cursor),
                    cancellationToken)
                .ConfigureAwait(false);

        if (cursor is not null &&
            page.Items.Count == 0)
        {
            SetPolicyCursor(
                clusterId,
                null);
            page =
                await _store
                    .ListPoliciesAsync(
                        new DataQualityPolicyListQuery(
                            clusterId,
                            _policy.MaxPoliciesPerCluster,
                            DataQualityPolicyLifecycleState.Active),
                        cancellationToken)
                    .ConfigureAwait(false);
        }

        return page;
    }

    private string? GetPolicyCursor(
        string clusterId)
    {
        lock (_cursorGate)
        {
            return _policyCursors.TryGetValue(
                clusterId,
                out var cursor)
                ? cursor
                : null;
        }
    }

    private void SetPolicyCursor(
        string clusterId,
        string? policyId)
    {
        lock (_cursorGate)
        {
            if (policyId is null)
            {
                _policyCursors.Remove(
                    clusterId);
            }
            else
            {
                _policyCursors[clusterId] =
                    policyId;
            }
        }
    }

    private IReadOnlyList<int> RotatePartitions(
        DataQualityPolicyDefinition policy)
    {
        var partitions =
            policy.Scope.Partitions;
        var key =
            PartitionCursorKey(
                policy);
        int start;

        lock (_cursorGate)
        {
            start =
                _partitionCursors.TryGetValue(
                    key,
                    out var value)
                    ? Math.Clamp(
                        value,
                        0,
                        partitions.Count - 1)
                    : 0;
        }

        var rotated =
            new int[partitions.Count];
        for (var index = 0;
             index < partitions.Count;
             index++)
        {
            rotated[index] =
                partitions[
                    (start + index) %
                    partitions.Count];
        }

        return Array.AsReadOnly(
            rotated);
    }

    private void AdvancePartitionCursor(
        DataQualityPolicyDefinition policy,
        int partition)
    {
        var partitions =
            policy.Scope.Partitions;
        var index =
            -1;
        for (var candidate = 0;
             candidate < partitions.Count;
             candidate++)
        {
            if (partitions[candidate] ==
                partition)
            {
                index =
                    candidate;
                break;
            }
        }

        if (index < 0)
        {
            return;
        }

        lock (_cursorGate)
        {
            _partitionCursors[
                PartitionCursorKey(
                    policy)] =
                (index + 1) %
                partitions.Count;
        }
    }

    private void RetainPartitionCursor(
        DataQualityPolicyDefinition policy,
        int partition)
    {
        var partitions =
            policy.Scope.Partitions;
        var index =
            -1;

        for (var candidate = 0;
             candidate < partitions.Count;
             candidate++)
        {
            if (partitions[candidate] ==
                partition)
            {
                index =
                    candidate;
                break;
            }
        }

        if (index < 0)
        {
            return;
        }

        lock (_cursorGate)
        {
            _partitionCursors[
                PartitionCursorKey(
                    policy)] =
                index;
        }
    }

    private static bool ResumeBatchIsEntirelyBeforeWindow(
        long? resumeOffset,
        RecordReadBatch batch,
        DateTimeOffset windowStartUtc)
    {
        if (resumeOffset is null ||
            batch.Records.Count == 0)
        {
            return false;
        }

        foreach (var record in batch.Records)
        {
            if (record.TimestampUtc is not
                { } timestamp)
            {
                return false;
            }

            if (timestamp.ToUniversalTime() >=
                windowStartUtc)
            {
                return false;
            }
        }

        return true;
    }

    private void UpdatePartitionResumeAfterSuccessfulRead(
        DataQualityPolicyDefinition policy,
        int partition,
        long? resumeOffset,
        RecordReadBatch batch,
        DataQualityEvaluationResult result,
        long safeInspectedPrefixNextOffset,
        bool underfundedByteRead,
        bool fullCeilingOversizedRead)
    {
        if (underfundedByteRead)
        {
            return;
        }

        if (batch.BudgetOutcome ==
                RecordBudgetOutcome.Complete &&
            result.Progress.Outcome ==
                DataQualityEvaluationOutcome.Complete)
        {
            if (resumeOffset is not null)
            {
                ClearPartitionResumeOffset(
                    policy,
                    partition);
            }

            return;
        }

        if (fullCeilingOversizedRead &&
            batch.Records.Count == 0)
        {
            if (batch.NextAnchor is
                {
                    Kind: RecordAnchorKind.Offset,
                    Offset: { } oversizedContinuation
                })
            {
                SetPartitionResumeOffset(
                    policy,
                    partition,
                    oversizedContinuation);
            }

            return;
        }

        var continuation =
            Math.Max(
                result.Progress.NextOffset,
                safeInspectedPrefixNextOffset);

        if (batch.NextAnchor is
            {
                Kind: RecordAnchorKind.Offset,
                Offset: { } sourceContinuation
            })
        {
            continuation =
                Math.Min(
                    continuation,
                    sourceContinuation);
        }
        else if (batch.LastReturnedOffset is
                 { } lastReturnedOffset &&
                 lastReturnedOffset <
                    long.MaxValue)
        {
            continuation =
                Math.Min(
                    continuation,
                    lastReturnedOffset + 1);
        }

        SetPartitionResumeOffset(
            policy,
            partition,
            continuation);
    }

    private long? GetPartitionResumeOffset(
        DataQualityPolicyDefinition policy,
        int partition)
    {
        lock (_cursorGate)
        {
            return _partitionResumeOffsets.TryGetValue(
                PartitionResumeKey(
                    policy,
                    partition),
                out var offset)
                ? offset
                : null;
        }
    }

    private void SetPartitionResumeOffset(
        DataQualityPolicyDefinition policy,
        int partition,
        long offset)
    {
        if (offset < 0)
        {
            return;
        }

        lock (_cursorGate)
        {
            _partitionResumeOffsets[
                PartitionResumeKey(
                    policy,
                    partition)] =
                offset;
        }
    }

    private void ClearPartitionResumeOffset(
        DataQualityPolicyDefinition policy,
        int partition)
    {
        lock (_cursorGate)
        {
            _partitionResumeOffsets.Remove(
                PartitionResumeKey(
                    policy,
                    partition));
        }
    }

    private static string PartitionResumeKey(
        DataQualityPolicyDefinition policy,
        int partition) =>
        string.Join(
            "\u001f",
            PartitionCursorKey(
                policy),
            partition);

    private static string PartitionCursorKey(
        DataQualityPolicyDefinition policy) =>
        string.Join(
            "\u001f",
            policy.PolicyId,
            policy.Version,
            policy.Scope.ClusterId,
            policy.Scope.TopicName);

    private sealed record PolicyRateSample(
        DateTimeOffset ObservedAtUtc,
        long Records,
        long Bytes);

    private sealed class PolicyRateWindowState
    {
        public Queue<PolicyRateSample> Samples { get; } = new();
        public long RecordsConsumed { get; set; }
        public long BytesConsumed { get; set; }
    }

    private readonly record struct PolicyRateAllowance(
        long RemainingRecords,
        long RemainingBytes);

    private sealed record PreparedEvaluationInput(
        DataQualityEvaluationInput Input,
        RecordBudgetOutcome? SourceOutcome,
        long SafeInspectedPrefixNextOffset);

    private PolicyRateAllowance GetPolicyRateAllowance(
        DataQualityPolicyDefinition policy,
        DateTimeOffset nowUtc)
    {
        lock (_cursorGate)
        {
            var state =
                GetOrCreatePolicyRateState(
                    policy);
            PrunePolicyRateState(
                state,
                nowUtc);

            return new PolicyRateAllowance(
                Math.Max(
                    0,
                    policy.Budget.RecordsPerSecond -
                    state.RecordsConsumed),
                Math.Max(
                    0,
                    policy.Budget.BytesPerSecond -
                    state.BytesConsumed));
        }
    }

    private void ConsumePolicyRate(
        DataQualityPolicyDefinition policy,
        DateTimeOffset observedAtUtc,
        long records,
        long bytes)
    {
        if (records < 0 ||
            bytes < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(records));
        }

        if (records == 0 &&
            bytes == 0)
        {
            return;
        }

        lock (_cursorGate)
        {
            var state =
                GetOrCreatePolicyRateState(
                    policy);
            PrunePolicyRateState(
                state,
                observedAtUtc);

            var nextRecords =
                checked(
                    state.RecordsConsumed +
                    records);
            var nextBytes =
                checked(
                    state.BytesConsumed +
                    bytes);

            if (nextRecords >
                    policy.Budget.RecordsPerSecond ||
                nextBytes >
                    policy.Budget.BytesPerSecond)
            {
                throw new InvalidOperationException(
                    "Data-quality source read exceeded the server-owned rolling policy rate allowance.");
            }

            state.Samples.Enqueue(
                new PolicyRateSample(
                    observedAtUtc,
                    records,
                    bytes));
            state.RecordsConsumed =
                nextRecords;
            state.BytesConsumed =
                nextBytes;
        }
    }

    private PolicyRateWindowState GetOrCreatePolicyRateState(
        DataQualityPolicyDefinition policy)
    {
        var key =
            PolicyRateKey(
                policy);

        if (!_policyRateWindows.TryGetValue(
                key,
                out var state))
        {
            state =
                new PolicyRateWindowState();
            _policyRateWindows[key] =
                state;
        }

        return state;
    }

    private static void PrunePolicyRateState(
        PolicyRateWindowState state,
        DateTimeOffset nowUtc)
    {
        var cutoff =
            nowUtc -
            TimeSpan.FromSeconds(1);

        while (state.Samples.Count > 0 &&
               state.Samples.Peek().ObservedAtUtc <=
                   cutoff)
        {
            var expired =
                state.Samples.Dequeue();
            state.RecordsConsumed -=
                expired.Records;
            state.BytesConsumed -=
                expired.Bytes;
        }
    }

    private static string PolicyRateKey(
        DataQualityPolicyDefinition policy) =>
        policy.PolicyId;

    private RecordOperationBudget BuildReadBudget(
        DataQualityPolicyBudget policyBudget,
        long remainingPolicyRecords,
        long remainingPolicyBytes,
        int remainingPartitions)
    {
        if (remainingPolicyRecords < 1 ||
            remainingPolicyBytes < 1 ||
            remainingPartitions < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(remainingPolicyRecords));
        }

        var fairRecordShare =
            Math.Max(
                1,
                (remainingPolicyRecords +
                 remainingPartitions - 1) /
                remainingPartitions);
        var maxRecords =
            (int)Math.Min(
                _policy.MaxRecordsPerEvaluation,
                Math.Min(
                    RecordOperationBudget
                        .HardMaxRecords,
                    fairRecordShare));

        var maxRawBytes =
            Math.Min(
                _policy.MaxRawBytesPerEvaluation,
                Math.Min(
                    RecordOperationBudget
                        .HardMaxRawBytes,
                    remainingPolicyBytes));

        return new RecordOperationBudget(
            maxRecords,
            maxRawBytes,
            maxRawBytes,
            _policy.MaxDurationPerEvaluation,
            Math.Min(
                policyBudget.RecordsPerSecond,
                RecordOperationBudget
                    .HardMaxRecordsPerSecond));
    }

    private static PreparedEvaluationInput
        PrepareEvaluationInput(
            DataQualityPolicyDefinition policy,
            int partition,
            RecordReadBatch batch,
            DateTimeOffset now,
            RecordOperationBudget readBudget)
    {
        var windowStart =
            now -
            policy.Budget.EvaluationWindow;
        var ordered =
            batch.Records
                .OrderBy(
                    record =>
                        record.Offset)
                .ToArray();

        var accepted =
            new List<KafkaRawRecord>(
                ordered.Length);
        var timestampUncertainty =
            false;
        var acceptedSeen =
            false;
        long? safeInspectedPrefixNextOffset =
            null;

        foreach (var record in ordered)
        {
            if (record.TimestampUtc is not
                { } timestamp)
            {
                timestampUncertainty = true;

                if (!acceptedSeen &&
                    record.Offset <
                        long.MaxValue)
                {
                    safeInspectedPrefixNextOffset =
                        record.Offset + 1;
                }

                continue;
            }

            var utcTimestamp =
                timestamp.ToUniversalTime();
            if (utcTimestamp <
                    windowStart ||
                utcTimestamp >=
                    now)
            {
                timestampUncertainty = true;

                if (!acceptedSeen &&
                    record.Offset <
                        long.MaxValue)
                {
                    safeInspectedPrefixNextOffset =
                        record.Offset + 1;
                }

                continue;
            }

            acceptedSeen =
                true;
            accepted.Add(
                record);
        }

        var previousResolvedOffset =
            batch.PreviousAnchor is
            {
                Kind: RecordAnchorKind.Offset,
                Offset: { } previousOffset
            }
                ? previousOffset
                : (long?)null;
        var continuationLowerBound =
            batch.NextAnchor is
            {
                Kind: RecordAnchorKind.Offset,
                Offset: { } nextOffset
            }
                ? Math.Max(
                    batch.LowWatermark,
                    nextOffset - 1)
                : (long?)null;

        var startOffset =
            accepted.Count > 0
                ? accepted[0].Offset
                : batch.FirstReturnedOffset ??
                  previousResolvedOffset ??
                  continuationLowerBound ??
                  (batch.BudgetOutcome ==
                       RecordBudgetOutcome.Complete
                      ? batch.HighWatermark
                      : batch.LowWatermark);

        var observedUpperBound =
            ordered.Length == 0
                ? batch.HighWatermark
                : Math.Max(
                    batch.HighWatermark,
                    checked(
                        ordered[^1].Offset + 1));

        var sourceIncomplete =
            batch.BudgetOutcome !=
                RecordBudgetOutcome.Complete ||
            timestampUncertainty;

        long endOffsetExclusive;
        if (sourceIncomplete)
        {
            endOffsetExclusive =
                Math.Max(
                    startOffset,
                    observedUpperBound);
        }
        else if (accepted.Count == 0)
        {
            endOffsetExclusive =
                startOffset;
        }
        else
        {
            endOffsetExclusive =
                checked(
                    accepted[^1].Offset + 1);
        }

        var sourceOutcome =
            batch.BudgetOutcome !=
                RecordBudgetOutcome.Complete
                ? batch.BudgetOutcome
                : timestampUncertainty
                    ? RecordBudgetOutcome.RecordLimit
                    : (RecordBudgetOutcome?)null;

        var input =
            new DataQualityEvaluationInput(
                policy.Scope.ClusterId,
                policy.Scope.TopicName,
                partition,
                windowStart,
                now,
                startOffset,
                endOffsetExclusive,
                accepted,
                new DataQualityEvaluationCycleBudget(
                    readBudget.MaxRecords,
                    readBudget.MaxRawBytes,
                    readBudget.MaxDuration));

        return new PreparedEvaluationInput(
            input,
            sourceOutcome,
            safeInspectedPrefixNextOffset ??
            startOffset);
    }

    private static DataQualityEvaluationResult
        BuildWorkerDeadlineResult(
            DataQualityPolicyDefinition policy,
            int partition,
            DateTimeOffset windowEndUtc)
    {
        var windowStartUtc =
            windowEndUtc -
            policy.Budget.EvaluationWindow;
        var violations =
            policy.Rules
                .Select(
                    rule =>
                        new DataQualityRuleViolationCount(
                            rule.RuleId,
                            0))
                .ToArray();

        var evidence =
            new DataQualityAggregateEvidence(
                policy.PolicyId,
                policy.Version,
                windowStartUtc,
                windowEndUtc,
                0,
                0,
                0,
                violations,
                DataQualityEvidenceState.Unknown,
                DataQualityEvidencePoint
                    .BoundedEvaluatorSource);
        var progress =
            new DataQualityEvaluationProgress(
                policy.PolicyId,
                policy.Version,
                policy.Scope.ClusterId,
                policy.Scope.TopicName,
                partition,
                windowStartUtc,
                windowEndUtc,
                0,
                0,
                0,
                0,
                0,
                DataQualityEvidenceState.Unknown,
                DataQualityEvaluationOutcome.DurationLimit,
                windowEndUtc);

        return new DataQualityEvaluationResult(
            evidence,
            progress);
    }

    private static DataQualityEvaluationResult
        ApplySourceOutcome(
            DataQualityEvaluationResult result,
            RecordBudgetOutcome sourceOutcome,
            int admittedRecordCount)
    {
        if (admittedRecordCount < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(admittedRecordCount));
        }
        var mappedOutcome =
            sourceOutcome switch
            {
                RecordBudgetOutcome.RecordLimit =>
                    DataQualityEvaluationOutcome.RecordLimit,
                RecordBudgetOutcome.RawByteLimit or
                RecordBudgetOutcome.ProjectedByteLimit =>
                    DataQualityEvaluationOutcome.ByteLimit,
                RecordBudgetOutcome.DurationLimit or
                RecordBudgetOutcome.RateLimit =>
                    DataQualityEvaluationOutcome.DurationLimit,
                RecordBudgetOutcome.Complete =>
                    result.Progress.Outcome,
                _ => throw new ArgumentOutOfRangeException(
                    nameof(sourceOutcome)),
            };

        if (sourceOutcome ==
                RecordBudgetOutcome.Complete ||
            result.Evidence.EvaluatedRecords <
                admittedRecordCount)
        {
            return result;
        }

        var hasEvaluatedRecords =
            result.Evidence.EvaluatedRecords > 0;
        var state =
            hasEvaluatedRecords
                ? DataQualityEvidenceState.Partial
                : DataQualityEvidenceState.Unknown;

        var violations =
            hasEvaluatedRecords
                ? result.Evidence
                    .ViolationsByRule
                    .ToArray()
                : result.Evidence
                    .ViolationsByRule
                    .Select(
                        item =>
                            new DataQualityRuleViolationCount(
                                item.RuleId,
                                0))
                    .ToArray();
        var evaluatedRecords =
            hasEvaluatedRecords
                ? result.Evidence.EvaluatedRecords
                : 0;
        var evaluatedBytes =
            hasEvaluatedRecords
                ? result.Evidence.EvaluatedBytes
                : 0;
        var violationCount =
            hasEvaluatedRecords
                ? result.Evidence.ViolationCount
                : 0;

        var evidence =
            new DataQualityAggregateEvidence(
                result.Evidence.PolicyId,
                result.Evidence.PolicyVersion,
                result.Evidence.WindowStartUtc,
                result.Evidence.WindowEndUtc,
                evaluatedRecords,
                evaluatedBytes,
                violationCount,
                violations,
                state,
                result.Evidence.Source);

        var progress =
            new DataQualityEvaluationProgress(
                result.Progress.PolicyId,
                result.Progress.PolicyVersion,
                result.Progress.ClusterId,
                result.Progress.TopicName,
                result.Progress.Partition,
                result.Progress.WindowStartUtc,
                result.Progress.WindowEndUtc,
                result.Progress.StartOffset,
                result.Progress.EndOffsetExclusive,
                result.Progress.NextOffset,
                evaluatedRecords,
                evaluatedBytes,
                state,
                mappedOutcome,
                result.Progress.UpdatedAtUtc);

        return new DataQualityEvaluationResult(
            evidence,
            progress);
    }

    private static long RawBatchBytes(
        IReadOnlyList<KafkaRawRecord> records)
    {
        long total = 0;

        foreach (var record in records)
        {
            total =
                checked(
                    total +
                    (record.Key?.Length ?? 0) +
                    (record.Value?.Length ?? 0));

            foreach (var header in
                     record.Headers)
            {
                total =
                    checked(
                        total +
                        Encoding.UTF8.GetByteCount(
                            header.Name) +
                        header.Value.Length);
            }
        }

        return total;
    }

}
