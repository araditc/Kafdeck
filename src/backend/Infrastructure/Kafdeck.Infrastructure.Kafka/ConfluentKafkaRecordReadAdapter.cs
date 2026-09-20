using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class KafkaRecordReadConcurrencyOptions
{
    public const int DefaultPerClusterLimit = 4;
    public const int DefaultGlobalLimit = 16;
    public const int HardMaxPerClusterLimit = 32;
    public const int HardMaxGlobalLimit = 128;

    public KafkaRecordReadConcurrencyOptions(
        int perClusterLimit = DefaultPerClusterLimit,
        int globalLimit = DefaultGlobalLimit)
    {
        if (perClusterLimit < 1 || perClusterLimit > HardMaxPerClusterLimit)
        {
            throw new ArgumentOutOfRangeException(nameof(perClusterLimit));
        }

        if (globalLimit < perClusterLimit || globalLimit > HardMaxGlobalLimit)
        {
            throw new ArgumentOutOfRangeException(nameof(globalLimit));
        }

        PerClusterLimit = perClusterLimit;
        GlobalLimit = globalLimit;
    }

    public int PerClusterLimit { get; }

    public int GlobalLimit { get; }
}

public sealed class ConfluentKafkaRecordReadAdapter : IKafkaRecordReadPort, IDisposable
{
    private static readonly TimeSpan ConsumePollInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan SetupPollInterval = TimeSpan.FromMilliseconds(100);

    private readonly IReadOnlyDictionary<string, ClusterProfile> _profiles;
    private readonly IKafkaRecordConsumerFactory _consumerFactory;
    private readonly SemaphoreSlim _globalGate;
    private readonly IReadOnlyDictionary<string, SemaphoreSlim> _clusterGates;
    private readonly TimeProvider _timeProvider;

    public ConfluentKafkaRecordReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        KafkaRecordReadConcurrencyOptions? concurrencyOptions = null,
        TimeProvider? timeProvider = null)
        : this(
            clusterProfiles,
            new ConfluentKafkaRecordConsumerFactory(
                secretResolver ?? throw new ArgumentNullException(nameof(secretResolver))),
            concurrencyOptions,
            timeProvider)
    {
    }

    internal ConfluentKafkaRecordReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        IKafkaRecordConsumerFactory consumerFactory,
        KafkaRecordReadConcurrencyOptions? concurrencyOptions = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(clusterProfiles);

        _profiles = clusterProfiles.ToDictionary(profile => profile.Id, StringComparer.Ordinal);
        _consumerFactory = consumerFactory ?? throw new ArgumentNullException(nameof(consumerFactory));
        _timeProvider = timeProvider ?? TimeProvider.System;

        var options = concurrencyOptions ?? new KafkaRecordReadConcurrencyOptions();
        _globalGate = new SemaphoreSlim(options.GlobalLimit, options.GlobalLimit);
        _clusterGates = _profiles.Keys.ToDictionary(
            clusterId => clusterId,
            _ => new SemaphoreSlim(options.PerClusterLimit, options.PerClusterLimit),
            StringComparer.Ordinal);
    }

    public async Task<KafkaResult<RecordReadBatch>> ReadPageAsync(
        RecordReadRequest request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (cancellationToken.IsCancellationRequested)
        {
            return Failed(KafkaFailureMapper.Cancelled());
        }

        var now = _timeProvider.GetUtcNow();
        if (operation.IsExpired(now))
        {
            return Failed(KafkaFailureMapper.DeadlineExceeded());
        }

        if (!_profiles.TryGetValue(request.ClusterId, out var profile) ||
            !_clusterGates.TryGetValue(request.ClusterId, out var clusterGate))
        {
            return Failed(KafkaFailureMapper.ClusterNotConfigured());
        }

        var globalAcquired = false;
        var clusterAcquired = false;

        try
        {
            using var admissionDeadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            admissionDeadline.CancelAfter(operation.Remaining(now));

            // Acquire the cluster bulkhead first. Waiting work for one cluster must
            // not consume global permits and starve unrelated clusters.
            await clusterGate.WaitAsync(admissionDeadline.Token).ConfigureAwait(false);
            clusterAcquired = true;

            await _globalGate.WaitAsync(admissionDeadline.Token).ConfigureAwait(false);
            globalAcquired = true;

            return await Task.Run(
                    () => ReadPageCore(profile, request, operation, cancellationToken),
                    CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return Failed(KafkaFailureMapper.Cancelled());
        }
        catch (OperationCanceledException)
        {
            return Failed(KafkaFailureMapper.DeadlineExceeded());
        }
        catch (KafdeckConfigurationException)
        {
            return Failed(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (KafkaException exception)
        {
            return Failed(KafkaFailureMapper.FromKafka(exception.Error));
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
        finally
        {
            if (clusterAcquired)
            {
                clusterGate.Release();
            }

            if (globalAcquired)
            {
                _globalGate.Release();
            }
        }
    }

    public void Dispose()
    {
        _globalGate.Dispose();
        foreach (var gate in _clusterGates.Values)
        {
            gate.Dispose();
        }
    }

    private KafkaResult<RecordReadBatch> ReadPageCore(
        ClusterProfile profile,
        RecordReadRequest request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        using var consumer = _consumerFactory.Create(profile);

        var startedAt = _timeProvider.GetUtcNow();
        var budgetDeadlineUtc = startedAt + request.Budget.MaxDuration;
        var topicPartition = new TopicPartition(request.TopicName, new Partition(request.Partition));

        var watermarkResult = QueryWatermarksWithCancellation(
            consumer,
            topicPartition,
            operation,
            budgetDeadlineUtc,
            cancellationToken);

        if (!watermarkResult.IsSuccess)
        {
            return watermarkResult.Failure is not null
                ? Failed(watermarkResult.Failure)
                : EmptyBudgetDurationBatch();
        }

        var watermarks = watermarkResult.Value!;
        var low = watermarks.Low.Value;
        var high = watermarks.High.Value;

        var anchorResult = ResolveAnchorOffset(
            consumer,
            topicPartition,
            request.Anchor,
            low,
            high,
            operation,
            budgetDeadlineUtc,
            cancellationToken);

        if (!anchorResult.IsSuccess)
        {
            if (anchorResult.Failure is not null)
            {
                return Failed(anchorResult.Failure);
            }

            return Success(BuildBatch(
                [],
                low,
                high,
                RecordBudgetOutcome.DurationLimit,
                low,
                high));
        }

        var resolvedAnchor = anchorResult.Value!;

        if (resolvedAnchor < low || resolvedAnchor > high)
        {
            return Failed(KafkaFailureMapper.OffsetOutOfRange());
        }

        long startOffset;
        long? stopExclusive = null;

        if (request.Direction == RecordReadDirection.Previous)
        {
            stopExclusive = resolvedAnchor;
            if (resolvedAnchor <= low)
            {
                return Success(BuildBatch([], low, high, RecordBudgetOutcome.Complete, low, high));
            }

            startOffset = Math.Max(low, resolvedAnchor - request.Budget.MaxRecords);
        }
        else
        {
            startOffset = resolvedAnchor;
            if (startOffset >= high)
            {
                return Success(BuildBatch([], low, high, RecordBudgetOutcome.Complete, startOffset, high));
            }
        }

        consumer.Assign(new TopicPartitionOffset(topicPartition, new Offset(startOffset)));

        var records = new List<KafkaRawRecord>(Math.Min(request.Budget.MaxRecords, 256));
        long rawBytes = 0;
        var outcome = RecordBudgetOutcome.Complete;
        var rateWindowStartedAt = _timeProvider.GetUtcNow();
        var rateWindowRecords = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var now = _timeProvider.GetUtcNow();
            if (operation.IsExpired(now))
            {
                return Failed(KafkaFailureMapper.DeadlineExceeded());
            }

            if (now >= budgetDeadlineUtc)
            {
                outcome = RecordBudgetOutcome.DurationLimit;
                break;
            }

            if (records.Count >= request.Budget.MaxRecords)
            {
                outcome = RecordBudgetOutcome.RecordLimit;
                break;
            }

            if (now - rateWindowStartedAt >= TimeSpan.FromSeconds(1))
            {
                rateWindowStartedAt = now;
                rateWindowRecords = 0;
            }
            else if (rateWindowRecords >= request.Budget.MaxRecordsPerSecond)
            {
                outcome = RecordBudgetOutcome.RateLimit;
                break;
            }

            var remaining = RemainingIoTime(operation, budgetDeadlineUtc);
            var pollTimeout = remaining < ConsumePollInterval ? remaining : ConsumePollInterval;
            if (pollTimeout <= TimeSpan.Zero)
            {
                outcome = RecordBudgetOutcome.DurationLimit;
                break;
            }

            var consumed = consumer.Consume(pollTimeout);
            if (consumed is null)
            {
                continue;
            }

            if (consumed.IsPartitionEOF)
            {
                break;
            }

            var offset = consumed.Offset.Value;
            if (stopExclusive.HasValue && offset >= stopExclusive.Value)
            {
                break;
            }

            if (offset < startOffset)
            {
                continue;
            }

            var rawSize = RawRecordSize(consumed);
            if (rawBytes + rawSize > request.Budget.MaxRawBytes)
            {
                outcome = RecordBudgetOutcome.RawByteLimit;
                break;
            }

            records.Add(ToCoreRecord(consumed));
            rawBytes += rawSize;
            rateWindowRecords++;
        }

        return Success(BuildBatch(records, low, high, outcome, startOffset, high));
    }

    private SetupResult<long> ResolveAnchorOffset(
        IKafkaRecordConsumerSession consumer,
        TopicPartition topicPartition,
        RecordAnchor anchor,
        long low,
        long high,
        KafkaOperationContext operation,
        DateTimeOffset budgetDeadlineUtc,
        CancellationToken cancellationToken)
    {
        return anchor.Kind switch
        {
            RecordAnchorKind.Earliest => SetupResult<long>.Success(low),
            RecordAnchorKind.Latest => SetupResult<long>.Success(high),
            RecordAnchorKind.Offset => SetupResult<long>.Success(anchor.Offset!.Value),
            RecordAnchorKind.Timestamp => ResolveTimestampOffsetWithCancellation(
                consumer,
                topicPartition,
                anchor.TimestampUtc!.Value,
                high,
                operation,
                budgetDeadlineUtc,
                cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(anchor)),
        };
    }

    private SetupResult<WatermarkOffsets> QueryWatermarksWithCancellation(
        IKafkaRecordConsumerSession consumer,
        TopicPartition topicPartition,
        KafkaOperationContext operation,
        DateTimeOffset budgetDeadlineUtc,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var remaining = RemainingIoTime(operation, budgetDeadlineUtc);
            if (remaining <= TimeSpan.Zero)
            {
                return SetupExpired<WatermarkOffsets>(operation, budgetDeadlineUtc);
            }

            var slice = remaining < SetupPollInterval ? remaining : SetupPollInterval;

            try
            {
                return SetupResult<WatermarkOffsets>.Success(
                    consumer.QueryWatermarkOffsets(topicPartition, slice));
            }
            catch (KafkaException exception) when (IsSetupSliceTimeout(exception.Error))
            {
                // A short timeout is used as an interruption point so request
                // cancellation is observed without waiting for the full operation deadline.
            }
        }
    }

    private SetupResult<long> ResolveTimestampOffsetWithCancellation(
        IKafkaRecordConsumerSession consumer,
        TopicPartition topicPartition,
        DateTimeOffset timestampUtc,
        long high,
        KafkaOperationContext operation,
        DateTimeOffset budgetDeadlineUtc,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var remaining = RemainingIoTime(operation, budgetDeadlineUtc);
            if (remaining <= TimeSpan.Zero)
            {
                return SetupExpired<long>(operation, budgetDeadlineUtc);
            }

            var slice = remaining < SetupPollInterval ? remaining : SetupPollInterval;

            try
            {
                var resolved = consumer.OffsetForTimestamp(topicPartition, timestampUtc, slice);
                return SetupResult<long>.Success(resolved == Offset.Unset ? high : resolved.Value);
            }
            catch (KafkaException exception) when (IsSetupSliceTimeout(exception.Error))
            {
                // Retry only before any record has been observed. Each slice is
                // bounded so cancellation/deadline/budget state is re-evaluated.
            }
        }
    }

    private SetupResult<T> SetupExpired<T>(
        KafkaOperationContext operation,
        DateTimeOffset budgetDeadlineUtc)
    {
        var now = _timeProvider.GetUtcNow();
        if (operation.IsExpired(now))
        {
            return SetupResult<T>.Failed(KafkaFailureMapper.DeadlineExceeded());
        }

        if (now >= budgetDeadlineUtc)
        {
            return SetupResult<T>.BudgetExhausted();
        }

        return SetupResult<T>.Failed(KafkaFailureMapper.DeadlineExceeded());
    }

    private static bool IsSetupSliceTimeout(Error error) =>
        error.Code is ErrorCode.Local_TimedOut or
            ErrorCode.Local_TimedOutQueue or
            ErrorCode.RequestTimedOut;

    private static KafkaRawRecord ToCoreRecord(ConsumeResult<byte[], byte[]> consumed)
    {
        var timestamp = consumed.Message.Timestamp.Type == TimestampType.NotAvailable
            ? (DateTimeOffset?)null
            : new DateTimeOffset(consumed.Message.Timestamp.UtcDateTime);

        var headers = consumed.Message.Headers is null
            ? Array.Empty<KafkaRecordHeader>()
            : consumed.Message.Headers
                .Select(header => new KafkaRecordHeader(
                    header.Key,
                    header.GetValueBytes() is { } value
                        ? new ReadOnlyMemory<byte>(value)
                        : ReadOnlyMemory<byte>.Empty))
                .ToArray();

        return new KafkaRawRecord(
            consumed.Offset.Value,
            timestamp,
            consumed.Message.Key is null ? null : new ReadOnlyMemory<byte>(consumed.Message.Key),
            consumed.Message.Value is null ? null : new ReadOnlyMemory<byte>(consumed.Message.Value),
            headers);
    }

    private static long RawRecordSize(ConsumeResult<byte[], byte[]> consumed)
    {
        long size = consumed.Message.Key?.LongLength ?? 0;
        size += consumed.Message.Value?.LongLength ?? 0;

        if (consumed.Message.Headers is not null)
        {
            foreach (var header in consumed.Message.Headers)
            {
                size += Encoding.UTF8.GetByteCount(header.Key);
                size += header.GetValueBytes()?.LongLength ?? 0;
            }
        }

        return size;
    }

    private RecordReadBatch BuildBatch(
        IReadOnlyList<KafkaRawRecord> records,
        long low,
        long high,
        RecordBudgetOutcome outcome,
        long startOffset,
        long highWatermark)
    {
        var first = records.Count == 0 ? (long?)null : records[0].Offset;
        var last = records.Count == 0 ? (long?)null : records[^1].Offset;

        RecordAnchor? next = null;
        if (last.HasValue && last.Value < highWatermark - 1)
        {
            next = RecordAnchor.AtOffset(last.Value + 1);
        }

        RecordAnchor? previous = null;
        var previousAnchor = first ?? startOffset;
        if (previousAnchor > low)
        {
            previous = RecordAnchor.AtOffset(previousAnchor);
        }

        return new RecordReadBatch(
            records,
            low,
            high,
            first,
            last,
            next,
            previous,
            outcome);
    }

    private KafkaResult<RecordReadBatch> EmptyBudgetDurationBatch() =>
        Success(new RecordReadBatch(
            Array.Empty<KafkaRawRecord>(),
            0,
            0,
            null,
            null,
            null,
            null,
            RecordBudgetOutcome.DurationLimit));

    private TimeSpan RemainingIoTime(
        KafkaOperationContext operation,
        DateTimeOffset budgetDeadlineUtc)
    {
        var now = _timeProvider.GetUtcNow();
        var operationRemaining = operation.Remaining(now);
        var budgetRemaining = budgetDeadlineUtc <= now
            ? TimeSpan.Zero
            : budgetDeadlineUtc - now;

        return operationRemaining <= budgetRemaining ? operationRemaining : budgetRemaining;
    }

    private sealed record SetupResult<T>
    {
        private SetupResult(T? value, KafkaFailure? failure, bool budgetExhausted)
        {
            Value = value;
            Failure = failure;
            IsBudgetExhausted = budgetExhausted;
        }

        public T? Value { get; }

        public KafkaFailure? Failure { get; }

        public bool IsBudgetExhausted { get; }

        public bool IsSuccess => Failure is null && !IsBudgetExhausted;

        public static SetupResult<T> Success(T value) => new(value, null, false);

        public static SetupResult<T> Failed(KafkaFailure failure) => new(default, failure, false);

        public static SetupResult<T> BudgetExhausted() => new(default, null, true);
    }

    private KafkaResult<RecordReadBatch> Failed(KafkaFailure failure) =>
        KafkaResult<RecordReadBatch>.Failed(failure, LiveObservation());

    private KafkaResult<RecordReadBatch> Success(RecordReadBatch batch) =>
        KafkaResult<RecordReadBatch>.Success(batch, LiveObservation());

    private ObservationMetadata LiveObservation()
    {
        var observedAt = _timeProvider.GetUtcNow();
        return new ObservationMetadata(
            observedAt,
            observedAt,
            observedAt,
            ObservationSource.Live);
    }
}

internal interface IKafkaRecordConsumerFactory
{
    IKafkaRecordConsumerSession Create(ClusterProfile profile);
}

internal interface IKafkaRecordConsumerSession : IDisposable
{
    WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout);

    Offset OffsetForTimestamp(
        TopicPartition topicPartition,
        DateTimeOffset timestampUtc,
        TimeSpan timeout);

    void Assign(TopicPartitionOffset offset);

    ConsumeResult<byte[], byte[]>? Consume(TimeSpan timeout);
}

internal sealed class ConfluentKafkaRecordConsumerFactory : IKafkaRecordConsumerFactory
{
    private readonly SecretResolver _secretResolver;

    public ConfluentKafkaRecordConsumerFactory(SecretResolver secretResolver)
    {
        _secretResolver = secretResolver ?? throw new ArgumentNullException(nameof(secretResolver));
    }

    public IKafkaRecordConsumerSession Create(ClusterProfile profile)
    {
        var config = KafkaClientConfigFactory.CreateRecordConsumer(profile, _secretResolver);
        var consumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
        return new ConfluentKafkaRecordConsumerSession(consumer);
    }
}

internal sealed class ConfluentKafkaRecordConsumerSession : IKafkaRecordConsumerSession
{
    private readonly IConsumer<byte[], byte[]> _consumer;

    public ConfluentKafkaRecordConsumerSession(IConsumer<byte[], byte[]> consumer)
    {
        _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
    }

    public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) =>
        _consumer.QueryWatermarkOffsets(topicPartition, timeout);

    public Offset OffsetForTimestamp(
        TopicPartition topicPartition,
        DateTimeOffset timestampUtc,
        TimeSpan timeout)
    {
        var result = _consumer.OffsetsForTimes(
            [new TopicPartitionTimestamp(topicPartition, new Timestamp(timestampUtc.UtcDateTime))],
            timeout);

        return result.Single().Offset;
    }

    public void Assign(TopicPartitionOffset offset) => _consumer.Assign(offset);

    public ConsumeResult<byte[], byte[]>? Consume(TimeSpan timeout) => _consumer.Consume(timeout);

    public void Dispose() => _consumer.Dispose();
}
