using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class ConfluentKafkaRecordReadAdapter : IKafkaRecordReadPort, IDisposable
{
    public const int DefaultGlobalConcurrency = 16;
    public const int DefaultPerClusterConcurrency = 4;

    private readonly KafkaRecordConsumerFactory _consumers;
    private readonly SemaphoreSlim _globalBulkhead;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _clusterBulkheads = new(StringComparer.Ordinal);
    private readonly int _perClusterConcurrency;
    private readonly TimeProvider _timeProvider;
    private bool _disposed;

    public ConfluentKafkaRecordReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        int globalConcurrency = DefaultGlobalConcurrency,
        int perClusterConcurrency = DefaultPerClusterConcurrency,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(clusterProfiles);
        ArgumentNullException.ThrowIfNull(secretResolver);

        if (globalConcurrency <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(globalConcurrency));
        }

        if (perClusterConcurrency <= 0 || perClusterConcurrency > globalConcurrency)
        {
            throw new ArgumentOutOfRangeException(nameof(perClusterConcurrency));
        }

        _consumers = new KafkaRecordConsumerFactory(clusterProfiles, secretResolver);
        _globalBulkhead = new SemaphoreSlim(globalConcurrency, globalConcurrency);
        _perClusterConcurrency = perClusterConcurrency;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<KafkaResult<RecordReadBatch>> ReadPageAsync(
        RecordReadRequest request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(request);

        if (cancellationToken.IsCancellationRequested)
        {
            return Failed<RecordReadBatch>(KafkaFailureMapper.Cancelled());
        }

        var startedAt = _timeProvider.GetUtcNow();
        if (operation.IsExpired(startedAt))
        {
            return Failed<RecordReadBatch>(KafkaFailureMapper.DeadlineExceeded());
        }

        if (!_consumers.ContainsCluster(request.ClusterId))
        {
            return Failed<RecordReadBatch>(KafkaFailureMapper.ClusterNotConfigured());
        }

        var budgetDeadline = startedAt + request.Budget.MaxDuration;
        var effectiveDeadline = budgetDeadline < operation.DeadlineUtc ? budgetDeadline : operation.DeadlineUtc;
        var durationBudgetOwnsDeadline = budgetDeadline < operation.DeadlineUtc;
        var remaining = effectiveDeadline - startedAt;
        if (remaining <= TimeSpan.Zero)
        {
            return durationBudgetOwnsDeadline
                ? Success(EmptyBatch(RecordBudgetOutcome.DurationLimit))
                : Failed<RecordReadBatch>(KafkaFailureMapper.DeadlineExceeded());
        }

        using var boundedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        boundedCancellation.CancelAfter(remaining);

        var clusterBulkhead = _clusterBulkheads.GetOrAdd(
            request.ClusterId,
            _ => new SemaphoreSlim(_perClusterConcurrency, _perClusterConcurrency));

        var globalAcquired = false;
        var clusterAcquired = false;

        try
        {
            await _globalBulkhead.WaitAsync(boundedCancellation.Token).ConfigureAwait(false);
            globalAcquired = true;

            await clusterBulkhead.WaitAsync(boundedCancellation.Token).ConfigureAwait(false);
            clusterAcquired = true;

            using var consumer = _consumers.Create(request.ClusterId);
            return ReadBounded(
                consumer,
                request,
                operation,
                effectiveDeadline,
                durationBudgetOwnsDeadline,
                cancellationToken,
                boundedCancellation.Token);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return Failed<RecordReadBatch>(KafkaFailureMapper.Cancelled());
        }
        catch (OperationCanceledException)
        {
            return durationBudgetOwnsDeadline
                ? Success(EmptyBatch(RecordBudgetOutcome.DurationLimit))
                : Failed<RecordReadBatch>(KafkaFailureMapper.DeadlineExceeded());
        }
        catch (KafdeckConfigurationException)
        {
            return Failed<RecordReadBatch>(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (KeyNotFoundException)
        {
            return Failed<RecordReadBatch>(KafkaFailureMapper.ClusterNotConfigured());
        }
        catch (KafkaException exception)
        {
            return Failed<RecordReadBatch>(KafkaFailureMapper.FromKafka(exception.Error));
        }
        catch (ArgumentException)
        {
            return Failed<RecordReadBatch>(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (InvalidOperationException)
        {
            return Failed<RecordReadBatch>(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (Exception)
        {
            return Failed<RecordReadBatch>(KafkaFailureMapper.Unknown());
        }
        finally
        {
            if (clusterAcquired)
            {
                clusterBulkhead.Release();
            }

            if (globalAcquired)
            {
                _globalBulkhead.Release();
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _globalBulkhead.Dispose();
        foreach (var bulkhead in _clusterBulkheads.Values)
        {
            bulkhead.Dispose();
        }

        _clusterBulkheads.Clear();
    }

    private KafkaResult<RecordReadBatch> ReadBounded(
        IConsumer<byte[], byte[]> consumer,
        RecordReadRequest request,
        KafkaOperationContext operation,
        DateTimeOffset effectiveDeadline,
        bool durationBudgetOwnsDeadline,
        CancellationToken callerCancellation,
        CancellationToken boundedCancellation)
    {
        var topicPartition = new TopicPartition(request.TopicName, new Partition(request.Partition));
        var timeout = RemainingTimeout(effectiveDeadline);
        var watermarks = consumer.QueryWatermarkOffsets(topicPartition, timeout);
        var low = watermarks.Low.Value;
        var high = watermarks.High.Value;

        var anchorOffset = ResolveAnchorOffset(consumer, topicPartition, request.Anchor, high, effectiveDeadline);
        if (anchorOffset < low || anchorOffset > high)
        {
            return Failed<RecordReadBatch>(new KafkaFailure(
                KafkaFailureCategory.ProtocolError,
                "offset_out_of_range",
                "The requested record offset is outside the current partition range.",
                false));
        }

        var endExclusive = high;
        var startOffset = anchorOffset;
        if (request.Direction == RecordReadDirection.Previous)
        {
            endExclusive = anchorOffset;
            startOffset = Math.Max(low, endExclusive - request.Budget.MaxRecords);
        }

        if (startOffset >= endExclusive || startOffset >= high)
        {
            return Success(new RecordReadBatch(
                Array.Empty<KafkaRawRecord>(),
                low,
                high,
                null,
                null,
                null,
                startOffset > low ? RecordAnchor.AtOffset(startOffset) : null,
                RecordBudgetOutcome.Complete));
        }

        consumer.Assign(new TopicPartitionOffset(topicPartition, new Offset(startOffset)));

        var records = new List<KafkaRawRecord>(Math.Min(request.Budget.MaxRecords, 256));
        long rawBytes = 0;
        var outcome = RecordBudgetOutcome.Complete;

        while (records.Count < request.Budget.MaxRecords)
        {
            if (_timeProvider.GetUtcNow() >= effectiveDeadline)
            {
                outcome = durationBudgetOwnsDeadline
                    ? RecordBudgetOutcome.DurationLimit
                    : throw new OperationCanceledException(boundedCancellation);
                break;
            }

            ConsumeResult<byte[], byte[]> consumed;
            try
            {
                consumed = consumer.Consume(boundedCancellation);
            }
            catch (OperationCanceledException) when (callerCancellation.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException) when (durationBudgetOwnsDeadline)
            {
                outcome = RecordBudgetOutcome.DurationLimit;
                break;
            }

            if (consumed.IsPartitionEOF)
            {
                break;
            }

            var offset = consumed.Offset.Value;
            if (offset >= endExclusive || offset >= high)
            {
                break;
            }

            var recordBytes = CountRawBytes(consumed.Message);
            if (recordBytes > request.Budget.MaxRawBytes - rawBytes)
            {
                outcome = RecordBudgetOutcome.RawByteLimit;
                break;
            }

            rawBytes += recordBytes;
            records.Add(ToCoreRecord(consumed));

            if (records.Count == request.Budget.MaxRecords && offset + 1 < endExclusive)
            {
                outcome = RecordBudgetOutcome.RecordLimit;
            }
        }

        var firstOffset = records.Count == 0 ? null : records[0].Offset;
        var lastOffset = records.Count == 0 ? null : records[^1].Offset;

        RecordAnchor? nextAnchor = null;
        RecordAnchor? previousAnchor = null;
        if (lastOffset.HasValue && lastOffset.Value < high - 1)
        {
            nextAnchor = RecordAnchor.AtOffset(lastOffset.Value + 1);
        }

        if (firstOffset.HasValue && firstOffset.Value > low)
        {
            previousAnchor = RecordAnchor.AtOffset(firstOffset.Value);
        }

        return Success(new RecordReadBatch(
            records,
            low,
            high,
            firstOffset,
            lastOffset,
            nextAnchor,
            previousAnchor,
            outcome));
    }

    private long ResolveAnchorOffset(
        IConsumer<byte[], byte[]> consumer,
        TopicPartition topicPartition,
        RecordAnchor anchor,
        long highWatermark,
        DateTimeOffset effectiveDeadline) => anchor.Kind switch
    {
        RecordAnchorKind.Earliest => consumer.QueryWatermarkOffsets(topicPartition, RemainingTimeout(effectiveDeadline)).Low.Value,
        RecordAnchorKind.Latest => highWatermark,
        RecordAnchorKind.Offset => anchor.Offset!.Value,
        RecordAnchorKind.Timestamp => ResolveTimestampOffset(consumer, topicPartition, anchor.TimestampUtc!.Value, highWatermark, effectiveDeadline),
        _ => throw new ArgumentOutOfRangeException(nameof(anchor)),
    };

    private long ResolveTimestampOffset(
        IConsumer<byte[], byte[]> consumer,
        TopicPartition topicPartition,
        DateTimeOffset timestampUtc,
        long highWatermark,
        DateTimeOffset effectiveDeadline)
    {
        var result = consumer.OffsetsForTimes(
            [new TopicPartitionTimestamp(topicPartition, new Timestamp(timestampUtc.UtcDateTime))],
            RemainingTimeout(effectiveDeadline)).Single();

        return result.Offset == Offset.Unset ? highWatermark : result.Offset.Value;
    }

    private TimeSpan RemainingTimeout(DateTimeOffset effectiveDeadline)
    {
        var remaining = effectiveDeadline - _timeProvider.GetUtcNow();
        if (remaining <= TimeSpan.Zero)
        {
            throw new OperationCanceledException();
        }

        return remaining;
    }

    private static long CountRawBytes(Message<byte[], byte[]> message)
    {
        long total = (message.Key?.LongLength ?? 0) + (message.Value?.LongLength ?? 0);
        if (message.Headers is null)
        {
            return total;
        }

        foreach (var header in message.Headers)
        {
            total += Encoding.UTF8.GetByteCount(header.Key);
            total += header.GetValueBytes()?.LongLength ?? 0;
        }

        return total;
    }

    private static KafkaRawRecord ToCoreRecord(ConsumeResult<byte[], byte[]> consumed)
    {
        var headers = consumed.Message.Headers is null
            ? Array.Empty<KafkaRecordHeader>()
            : consumed.Message.Headers
                .Select(header => new KafkaRecordHeader(
                    header.Key,
                    header.GetValueBytes() ?? Array.Empty<byte>()))
                .ToArray();

        DateTimeOffset? timestampUtc = consumed.Message.Timestamp.Type == TimestampType.NotAvailable
            ? null
            : new DateTimeOffset(consumed.Message.Timestamp.UtcDateTime, TimeSpan.Zero);

        return new KafkaRawRecord(
            consumed.Offset.Value,
            timestampUtc,
            consumed.Message.Key is null ? null : consumed.Message.Key,
            consumed.Message.Value is null ? null : consumed.Message.Value,
            headers);
    }

    private KafkaResult<T> Success<T>(T value) => KafkaResult<T>.Success(value, LiveObservation());

    private KafkaResult<T> Failed<T>(KafkaFailure failure) => KafkaResult<T>.Failed(failure, LiveObservation());

    private RecordReadBatch EmptyBatch(RecordBudgetOutcome outcome) =>
        new(Array.Empty<KafkaRawRecord>(), 0, 0, null, null, null, null, outcome);

    private ObservationMetadata LiveObservation()
    {
        var observedAt = _timeProvider.GetUtcNow();
        return new ObservationMetadata(observedAt, observedAt, observedAt, ObservationSource.Live);
    }
}
