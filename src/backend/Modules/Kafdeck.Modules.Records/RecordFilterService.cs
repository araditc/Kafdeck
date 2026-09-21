using System.Runtime.CompilerServices;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;

namespace Kafdeck.Modules.Records;

public sealed class RecordFilterService
{
    private readonly IKafkaRecordReadPort _reader;
    private readonly IRecordDecodePort? _decoder;
    private readonly RecordFilterEvaluator _evaluator;
    private readonly TimeProvider _timeProvider;

    public RecordFilterService(
        IKafkaRecordReadPort reader,
        IRecordDecodePort? decoder = null,
        RecordFilterEvaluator? evaluator = null,
        TimeProvider? timeProvider = null)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _decoder = decoder;
        _evaluator = evaluator ?? new RecordFilterEvaluator();
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Task<KafkaResult<RecordFilterPage>> FilterPageAsync(
        RecordReadRequest readRequest,
        RecordFilterPlan plan,
        KafkaOperationContext operation,
        CancellationToken cancellationToken) =>
        FilterPageAsync(readRequest, plan, operation, requireDecodedValue: false, cancellationToken);

    public async Task<KafkaResult<RecordFilterPage>> FilterPageAsync(
        RecordReadRequest readRequest,
        RecordFilterPlan plan,
        KafkaOperationContext operation,
        bool requireDecodedValue,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(readRequest);
        ArgumentNullException.ThrowIfNull(plan);

        var read = await _reader
            .ReadPageAsync(readRequest, operation, cancellationToken)
            .ConfigureAwait(false);

        if (!read.IsSuccess || read.Value is null)
        {
            return KafkaResult<RecordFilterPage>.Failed(
                read.Failure!,
                read.Observation);
        }

        var batch = read.Value;
        var filterStartedAt = _timeProvider.GetUtcNow();
        var filterDeadline = filterStartedAt + plan.Budget.MaxDuration;
        var matched = new List<RecordFilteredItem>();
        var decodeFailed = 0;
        var decodeUnavailable = 0;
        var evaluatedRecords = 0;
        long evaluatedBytes = 0;
        var outcome = RecordFilterBudgetOutcome.Complete;
        long? firstUnevaluatedOffset = null;

        foreach (var record in batch.Records)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var now = _timeProvider.GetUtcNow();
            if (operation.IsExpired(now) || now >= filterDeadline)
            {
                outcome = RecordFilterBudgetOutcome.DurationLimit;
                firstUnevaluatedOffset = record.Offset;
                break;
            }

            if (evaluatedRecords >= plan.Budget.MaxEvaluatedRecords)
            {
                outcome = RecordFilterBudgetOutcome.RecordLimit;
                firstUnevaluatedOffset = record.Offset;
                break;
            }

            var recordBytes = RecordFilterEvaluator.EstimateRawBytes(record);
            if (evaluatedBytes + recordBytes > plan.Budget.MaxEvaluatedBytes)
            {
                outcome = RecordFilterBudgetOutcome.ByteLimit;
                firstUnevaluatedOffset = record.Offset;
                break;
            }

            evaluatedRecords++;
            evaluatedBytes += recordBytes;

            if (!_evaluator.MatchesPreFilter(record, plan.PreFilter))
            {
                continue;
            }

            RecordDecodedValue? decoded = null;
            var needsDecode = plan.RequiresStructuredValue || requireDecodedValue;

            if (needsDecode)
            {
                if (_decoder is null || !record.Value.HasValue)
                {
                    decodeUnavailable++;
                    if (plan.RequiresStructuredValue)
                    {
                        continue;
                    }

                    matched.Add(new RecordFilteredItem(record, null));
                    continue;
                }

                var decodedResult = await _decoder.DecodeAsync(
                        new RecordDecodeRequest(
                            readRequest.ClusterId,
                            readRequest.TopicName,
                            readRequest.Partition,
                            record.Offset,
                            isKey: false,
                            record.Value.Value),
                        operation,
                        cancellationToken)
                    .ConfigureAwait(false);

                if (!decodedResult.IsSuccess || decodedResult.Value is null)
                {
                    if (decodedResult.Failure?.Category is
                        RecordSchemaFailureCategory.Unavailable or
                        RecordSchemaFailureCategory.Timeout or
                        RecordSchemaFailureCategory.RegistryNotConfigured or
                        RecordSchemaFailureCategory.Unauthorized)
                    {
                        decodeUnavailable++;
                    }
                    else
                    {
                        decodeFailed++;
                    }

                    if (plan.RequiresStructuredValue)
                    {
                        continue;
                    }

                    matched.Add(new RecordFilteredItem(record, null));
                    continue;
                }

                decoded = decodedResult.Value;

                if (plan.RequiresStructuredValue &&
                    !_evaluator.MatchesStructuredValue(
                        decoded.StructuredValue,
                        plan))
                {
                    continue;
                }
            }

            matched.Add(new RecordFilteredItem(record, decoded));
        }

        var limitations = new List<RecordFilterLimitation>();
        if (decodeFailed > 0)
        {
            limitations.Add(
                new RecordFilterLimitation(
                    RecordFilterLimitationCode.DecodeFailed,
                    decodeFailed));
        }

        if (decodeUnavailable > 0)
        {
            limitations.Add(
                new RecordFilterLimitation(
                    RecordFilterLimitationCode.DecodeUnavailable,
                    decodeUnavailable));
        }

        if (outcome != RecordFilterBudgetOutcome.Complete)
        {
            limitations.Add(
                new RecordFilterLimitation(
                    RecordFilterLimitationCode.FilterBudgetExhausted,
                    1));
        }

        var nextAnchor = firstUnevaluatedOffset.HasValue
            ? RecordAnchor.AtOffset(firstUnevaluatedOffset.Value)
            : batch.NextAnchor;

        var page = new RecordFilterPage(
            matched,
            batch.LowWatermark,
            batch.HighWatermark,
            nextAnchor,
            batch.PreviousAnchor,
            batch.BudgetOutcome,
            outcome,
            evaluatedRecords,
            evaluatedBytes,
            limitations);

        return KafkaResult<RecordFilterPage>.Success(page, read.Observation);
    }
}

public sealed class RecordLiveTailOptions
{
    public const int DefaultGlobalLimit = 32;
    public const int DefaultPerClusterLimit = 8;
    public const int DefaultPerIdentityLimit = 2;
    public const int HardMaxGlobalLimit = 256;
    public const int HardMaxPerClusterLimit = 64;
    public const int HardMaxPerIdentityLimit = 8;
    public static readonly TimeSpan DefaultEmptyPollDelay = TimeSpan.FromMilliseconds(250);
    public static readonly TimeSpan HardMaxEmptyPollDelay = TimeSpan.FromSeconds(2);

    public RecordLiveTailOptions(
        int globalLimit = DefaultGlobalLimit,
        int perClusterLimit = DefaultPerClusterLimit,
        int perIdentityLimit = DefaultPerIdentityLimit,
        TimeSpan? emptyPollDelay = null)
    {
        if (globalLimit < 1 || globalLimit > HardMaxGlobalLimit)
        {
            throw new ArgumentOutOfRangeException(nameof(globalLimit));
        }

        if (perClusterLimit < 1 ||
            perClusterLimit > HardMaxPerClusterLimit ||
            perClusterLimit > globalLimit)
        {
            throw new ArgumentOutOfRangeException(nameof(perClusterLimit));
        }

        if (perIdentityLimit < 1 ||
            perIdentityLimit > HardMaxPerIdentityLimit ||
            perIdentityLimit > globalLimit)
        {
            throw new ArgumentOutOfRangeException(nameof(perIdentityLimit));
        }

        var delay = emptyPollDelay ?? DefaultEmptyPollDelay;
        if (delay < TimeSpan.Zero || delay > HardMaxEmptyPollDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(emptyPollDelay));
        }

        GlobalLimit = globalLimit;
        PerClusterLimit = perClusterLimit;
        PerIdentityLimit = perIdentityLimit;
        EmptyPollDelay = delay;
    }

    public int GlobalLimit { get; }
    public int PerClusterLimit { get; }
    public int PerIdentityLimit { get; }
    public TimeSpan EmptyPollDelay { get; }
}

public sealed class RecordLiveTailService
{
    private static readonly TimeSpan RateWindowDelay = TimeSpan.FromSeconds(1);
    private readonly RecordFilterService _filter;
    private readonly RecordTailAdmissionController _admission;
    private readonly RecordLiveTailOptions _options;
    private readonly TimeProvider _timeProvider;

    public RecordLiveTailService(
        RecordFilterService filter,
        RecordLiveTailOptions? options = null,
        TimeProvider? timeProvider = null)
    {
        _filter = filter ?? throw new ArgumentNullException(nameof(filter));
        _options = options ?? new RecordLiveTailOptions();
        _admission = new RecordTailAdmissionController(_options);
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public IAsyncEnumerable<RecordTailFrame> TailAsync(
        RecordTailRequest request,
        CancellationToken cancellationToken = default) =>
        TailAsync(request, requireDecodedValue: false, cancellationToken);

    public async IAsyncEnumerable<RecordTailFrame> TailAsync(
        RecordTailRequest request,
        bool requireDecodedValue,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request.InitialRead.Direction != RecordReadDirection.Forward)
        {
            throw new ArgumentException(
                "Live tail requires forward record-read direction.",
                nameof(request));
        }

        if (!_admission.TryAcquire(
                request.AdmissionIdentity,
                request.InitialRead.ClusterId,
                out var lease))
        {
            yield return new RecordTailFrame(RecordTailFrameKind.AdmissionDenied);
            yield break;
        }

        using (lease)
        {
            var plan = RecordFilterCompiler.Compile(request.Filter);
            var startedAt = _timeProvider.GetUtcNow();
            var deadline = startedAt + request.Budget.MaxDuration;
            var anchor = request.InitialRead.Anchor;
            var totalEvaluatedRecords = 0;
            long totalEvaluatedBytes = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var now = _timeProvider.GetUtcNow();
                if (now >= deadline ||
                    totalEvaluatedRecords >= request.Budget.MaxRecords ||
                    totalEvaluatedBytes >= request.Budget.MaxRawBytes)
                {
                    yield return new RecordTailFrame(RecordTailFrameKind.Completed);
                    yield break;
                }

                var remainingRecords = request.Budget.MaxRecords - totalEvaluatedRecords;
                var remainingBytes = request.Budget.MaxRawBytes - totalEvaluatedBytes;
                var remainingDuration = deadline - now;

                if (remainingRecords <= 0 ||
                    remainingBytes <= 0 ||
                    remainingDuration <= TimeSpan.Zero)
                {
                    yield return new RecordTailFrame(RecordTailFrameKind.Completed);
                    yield break;
                }

                var pageBudget = new RecordOperationBudget(
                    maxRecords: Math.Min(
                        request.InitialRead.Budget.MaxRecords,
                        remainingRecords),
                    maxRawBytes: Math.Min(
                        request.InitialRead.Budget.MaxRawBytes,
                        remainingBytes),
                    maxProjectedBytes: request.InitialRead.Budget.MaxProjectedBytes,
                    maxDuration: Min(
                        request.InitialRead.Budget.MaxDuration,
                        remainingDuration),
                    maxRecordsPerSecond: Math.Min(
                        request.InitialRead.Budget.MaxRecordsPerSecond,
                        request.Budget.MaxRecordsPerSecond));

                var pageRequest = new RecordReadRequest(
                    request.InitialRead.ClusterId,
                    request.InitialRead.TopicName,
                    request.InitialRead.Partition,
                    anchor,
                    RecordReadDirection.Forward,
                    pageBudget);

                var operation = new KafkaOperationContext(
                    _timeProvider.GetUtcNow() + pageBudget.MaxDuration);

                var pageResult = await _filter
                    .FilterPageAsync(
                        pageRequest,
                        plan,
                        operation,
                        requireDecodedValue,
                        cancellationToken)
                    .ConfigureAwait(false);

                if (!pageResult.IsSuccess || pageResult.Value is null)
                {
                    yield return new RecordTailFrame(
                        RecordTailFrameKind.KafkaFailure,
                        Failure: pageResult.Failure);
                    yield break;
                }

                var page = pageResult.Value;
                totalEvaluatedRecords += page.EvaluatedRecordCount;
                totalEvaluatedBytes += page.EvaluatedByteCount;

                if (page.Records.Count > 0 ||
                    page.Limitations.Count > 0)
                {
                    yield return new RecordTailFrame(
                        RecordTailFrameKind.Records,
                        Page: page);
                }

                anchor = page.NextAnchor
                    ?? RecordAnchor.AtOffset(page.HighWatermark);

                if (page.FilterBudgetOutcome != RecordFilterBudgetOutcome.Complete ||
                    page.ReadBudgetOutcome is
                        RecordBudgetOutcome.RawByteLimit or
                        RecordBudgetOutcome.DurationLimit)
                {
                    yield return new RecordTailFrame(RecordTailFrameKind.Completed);
                    yield break;
                }

                if (page.ReadBudgetOutcome == RecordBudgetOutcome.RateLimit)
                {
                    var remainingAfterPage = deadline - _timeProvider.GetUtcNow();
                    if (remainingAfterPage <= TimeSpan.Zero)
                    {
                        yield return new RecordTailFrame(RecordTailFrameKind.Completed);
                        yield break;
                    }

                    await Task.Delay(
                            Min(RateWindowDelay, remainingAfterPage),
                            _timeProvider,
                            cancellationToken)
                        .ConfigureAwait(false);
                    continue;
                }

                if (page.EvaluatedRecordCount == 0 &&
                    _options.EmptyPollDelay > TimeSpan.Zero)
                {
                    await Task.Delay(
                            _options.EmptyPollDelay,
                            _timeProvider,
                            cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }
    }

    private static TimeSpan Min(TimeSpan left, TimeSpan right) =>
        left <= right ? left : right;
}

internal sealed class RecordTailAdmissionController
{
    private readonly RecordLiveTailOptions _options;
    private readonly object _gate = new();
    private readonly Dictionary<string, int> _identityCounts = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _clusterCounts = new(StringComparer.Ordinal);
    private int _globalCount;

    public RecordTailAdmissionController(RecordLiveTailOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public bool TryAcquire(
        string identity,
        string clusterId,
        out IDisposable lease)
    {
        lock (_gate)
        {
            var identityCount = _identityCounts.GetValueOrDefault(identity);
            var clusterCount = _clusterCounts.GetValueOrDefault(clusterId);

            if (_globalCount >= _options.GlobalLimit ||
                identityCount >= _options.PerIdentityLimit ||
                clusterCount >= _options.PerClusterLimit)
            {
                lease = NoopLease.Instance;
                return false;
            }

            _globalCount++;
            _identityCounts[identity] = identityCount + 1;
            _clusterCounts[clusterId] = clusterCount + 1;

            lease = new Lease(this, identity, clusterId);
            return true;
        }
    }

    private void Release(string identity, string clusterId)
    {
        lock (_gate)
        {
            _globalCount--;

            DecrementOrRemove(_identityCounts, identity);
            DecrementOrRemove(_clusterCounts, clusterId);
        }
    }

    private static void DecrementOrRemove(
        IDictionary<string, int> counts,
        string key)
    {
        if (!counts.TryGetValue(key, out var current))
        {
            return;
        }

        if (current <= 1)
        {
            counts.Remove(key);
        }
        else
        {
            counts[key] = current - 1;
        }
    }

    private sealed class Lease : IDisposable
    {
        private RecordTailAdmissionController? _owner;
        private readonly string _identity;
        private readonly string _clusterId;

        public Lease(
            RecordTailAdmissionController owner,
            string identity,
            string clusterId)
        {
            _owner = owner;
            _identity = identity;
            _clusterId = clusterId;
        }

        public void Dispose()
        {
            Interlocked.Exchange(ref _owner, null)
                ?.Release(_identity, _clusterId);
        }
    }

    private sealed class NoopLease : IDisposable
    {
        public static NoopLease Instance { get; } = new();

        public void Dispose()
        {
        }
    }
}
