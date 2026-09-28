using Kafdeck.Core.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W65DataQualityEvaluatorContractsTests
{
    [Fact]
    public void Evaluation_input_rejects_oversized_list_before_enumeration()
    {
        var budget =
            new DataQualityEvaluationCycleBudget(
                maxRecords: 2);
        var records =
            new CountOnlyReadOnlyList<KafkaRawRecord>(
                3);

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationInput(
                "prod",
                "orders",
                0,
                DateTimeOffset.UtcNow.AddMinutes(-1),
                DateTimeOffset.UtcNow,
                0,
                10,
                records,
                budget));
    }

    [Fact]
    public void Evaluation_input_reuses_the_admitted_record_count_once()
    {
        var now =
            DateTimeOffset.UtcNow;
        var record =
            new KafkaRawRecord(
                1,
                now,
                null,
                null,
                Array.Empty<KafkaRecordHeader>());
        var records =
            new FirstCountThenHugeReadOnlyList<KafkaRawRecord>(
                [record],
                hugeCount: 10_000);

        var input =
            new DataQualityEvaluationInput(
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                0,
                10,
                records,
                new DataQualityEvaluationCycleBudget());

        Assert.Single(
            input.Records);
        Assert.Equal(
            1,
            records.CountReads);
    }

    [Fact]
    public void Evaluation_input_materializes_only_admitted_indexed_entries()
    {
        var now =
            DateTimeOffset.UtcNow;
        var record =
            new KafkaRawRecord(
                1,
                now,
                null,
                null,
                Array.Empty<KafkaRecordHeader>());
        var records =
            new IndexedOnlyReadOnlyList<KafkaRawRecord>(
                [record]);

        var input =
            new DataQualityEvaluationInput(
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                0,
                10,
                records,
                new DataQualityEvaluationCycleBudget());

        Assert.Single(
            input.Records);
        Assert.Equal(
            1,
            input.Records[0].Offset);
    }

    [Fact]
    public void Evaluation_input_enforces_raw_byte_budget()
    {
        var now =
            DateTimeOffset.UtcNow;
        var budget =
            new DataQualityEvaluationCycleBudget(
                maxRecords: 10,
                maxRawBytes: 8);

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationInput(
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                0,
                10,
                [
                    new KafkaRawRecord(
                        1,
                        now,
                        new byte[4],
                        new byte[5],
                        Array.Empty<KafkaRecordHeader>()),
                ],
                budget));
    }

    [Fact]
    public void Evaluation_input_snapshots_mutable_payload_and_headers()
    {
        var now =
            DateTimeOffset.UtcNow;
        var key =
            new byte[] { 1, 2 };
        var value =
            new byte[] { 3, 4 };
        var headerValue =
            new byte[] { 5 };
        var headers =
            new List<KafkaRecordHeader>
            {
                new(
                    "trace-id",
                    headerValue),
            };

        var input =
            new DataQualityEvaluationInput(
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                0,
                10,
                [
                    new KafkaRawRecord(
                        1,
                        now,
                        key,
                        value,
                        headers),
                ],
                new DataQualityEvaluationCycleBudget());

        var rawBytes =
            input.RawByteCount;

        key[0] = 99;
        value[0] = 99;
        headerValue[0] = 99;
        headers.Add(
            new KafkaRecordHeader(
                new string('x', 200),
                new byte[1024]));

        var snapshot =
            Assert.Single(
                input.Records);

        Assert.Equal(
            1,
            snapshot.Key!.Value.Span[0]);
        Assert.Equal(
            3,
            snapshot.Value!.Value.Span[0]);
        Assert.Single(
            snapshot.Headers);
        Assert.Equal(
            5,
            snapshot.Headers[0].Value.Span[0]);
        Assert.Equal(
            rawBytes,
            input.RawByteCount);
    }

    [Fact]
    public void Evaluation_input_reuses_the_admitted_header_count_once()
    {
        var now =
            DateTimeOffset.UtcNow;
        var headers =
            new FirstCountThenHugeReadOnlyList<KafkaRecordHeader>(
                [
                    new KafkaRecordHeader(
                        "trace-id",
                        new byte[] { 7 }),
                ],
                hugeCount: 10_000);

        var input =
            new DataQualityEvaluationInput(
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                0,
                10,
                [
                    new KafkaRawRecord(
                        1,
                        now,
                        null,
                        null,
                        headers),
                ],
                new DataQualityEvaluationCycleBudget());

        var snapshot =
            Assert.Single(
                input.Records);

        Assert.Single(
            snapshot.Headers);
        Assert.Equal(
            1,
            headers.CountReads);
    }

    [Fact]
    public void Evaluation_input_bounds_header_name_before_byte_scan()
    {
        var now =
            DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationInput(
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                0,
                10,
                [
                    new KafkaRawRecord(
                        1,
                        now,
                        null,
                        null,
                        [
                            new KafkaRecordHeader(
                                new string(
                                    'h',
                                    RecordHeaderMaskRule
                                        .MaxHeaderNameCharacters + 1),
                                ReadOnlyMemory<byte>.Empty),
                        ]),
                ],
                new DataQualityEvaluationCycleBudget()));
    }

    [Fact]
    public void Evaluation_input_rejects_duplicate_or_out_of_order_offsets()
    {
        var now =
            DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationInput(
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                0,
                10,
                [
                    new KafkaRawRecord(
                        2,
                        now,
                        null,
                        null,
                        Array.Empty<KafkaRecordHeader>()),
                    new KafkaRawRecord(
                        2,
                        now,
                        null,
                        null,
                        Array.Empty<KafkaRecordHeader>()),
                ],
                new DataQualityEvaluationCycleBudget()));
    }

    [Fact]
    public void Evaluation_input_rejects_record_outside_offset_range()
    {
        var now =
            DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationInput(
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                10,
                20,
                [
                    new KafkaRawRecord(
                        20,
                        now,
                        null,
                        null,
                        Array.Empty<KafkaRecordHeader>()),
                ],
                new DataQualityEvaluationCycleBudget()));
    }

    [Fact]
    public void Progress_state_contains_metadata_only()
    {
        var now =
            DateTimeOffset.UtcNow;
        var progress =
            new DataQualityEvaluationProgress(
                "orders-quality",
                1,
                "prod",
                "orders",
                0,
                now.AddMinutes(-5),
                now,
                100,
                200,
                120,
                20,
                4096,
                DataQualityEvidenceState.Partial,
                DataQualityEvaluationOutcome.RecordLimit,
                now);

        Assert.Equal(
            120,
            progress.NextOffset);

        var properties =
            typeof(DataQualityEvaluationProgress)
                .GetProperties();

        Assert.DoesNotContain(
            properties,
            property =>
                property.PropertyType ==
                    typeof(KafkaRawRecord) ||
                property.Name.Contains(
                    "Payload",
                    StringComparison.OrdinalIgnoreCase) ||
                property.Name.Contains(
                    "Key",
                    StringComparison.OrdinalIgnoreCase) ||
                property.Name.Contains(
                    "Value",
                    StringComparison.OrdinalIgnoreCase) ||
                property.Name.Contains(
                    "Header",
                    StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Completed_progress_requires_cursor_at_range_end()
    {
        var now =
            DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationProgress(
                "orders-quality",
                1,
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                0,
                100,
                99,
                99,
                1024,
                DataQualityEvidenceState.Available,
                DataQualityEvaluationOutcome.Complete,
                now));
    }

    [Fact]
    public void Progress_cannot_claim_more_records_than_cursor_traversed()
    {
        var now =
            DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationProgress(
                "orders-quality",
                1,
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                100,
                200,
                100,
                1,
                10,
                DataQualityEvidenceState.Partial,
                DataQualityEvaluationOutcome.RecordLimit,
                now));
    }

    [Fact]
    public void Zero_evaluated_records_cannot_claim_bytes()
    {
        var now =
            DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationProgress(
                "orders-quality",
                1,
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                100,
                110,
                100,
                0,
                1,
                DataQualityEvidenceState.Partial,
                DataQualityEvaluationOutcome.RecordLimit,
                now));
    }

    [Theory]
    [InlineData(DataQualityEvidenceState.Unknown)]
    [InlineData(DataQualityEvidenceState.Unavailable)]
    public void Unknown_or_unavailable_progress_cannot_claim_counters(
        DataQualityEvidenceState state)
    {
        var now =
            DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationProgress(
                "orders-quality",
                1,
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                0,
                10,
                1,
                1,
                10,
                state,
                DataQualityEvaluationOutcome.SourceUnavailable,
                now));
    }

    [Fact]
    public void Progress_rejects_offset_or_throughput_claims_outside_admitted_bounds()
    {
        var now =
            DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationProgress(
                "orders-quality",
                1,
                "prod",
                "orders",
                0,
                now.AddSeconds(-1),
                now,
                100,
                110,
                111,
                0,
                0,
                DataQualityEvidenceState.Partial,
                DataQualityEvaluationOutcome.RecordLimit,
                now));

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationProgress(
                "orders-quality",
                1,
                "prod",
                "orders",
                0,
                now.AddSeconds(-1),
                now,
                0,
                2_000,
                1_001,
                1_001,
                0,
                DataQualityEvidenceState.Partial,
                DataQualityEvaluationOutcome.RecordLimit,
                now));
    }

    [Fact]
    public void Evaluation_result_rejects_mismatched_evidence_and_progress()
    {
        var now =
            DateTimeOffset.UtcNow;
        var evidence =
            new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                now.AddMinutes(-1),
                now,
                10,
                100,
                0,
                Array.Empty<DataQualityRuleViolationCount>(),
                DataQualityEvidenceState.Available,
                "record-monitor");
        var progress =
            new DataQualityEvaluationProgress(
                "orders-quality",
                1,
                "prod",
                "orders",
                0,
                now.AddMinutes(-1),
                now,
                0,
                20,
                10,
                9,
                100,
                DataQualityEvidenceState.Available,
                DataQualityEvaluationOutcome.RecordLimit,
                now);

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationResult(
                evidence,
                progress));
    }

    [Fact]
    public void Evaluation_budget_stays_inside_existing_record_port_hard_caps()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityEvaluationCycleBudget(
                maxRecords:
                    RecordOperationBudget.HardMaxRecords + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityEvaluationCycleBudget(
                maxRawBytes:
                    RecordOperationBudget.HardMaxRawBytes + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityEvaluationCycleBudget(
                maxDuration:
                    RecordOperationBudget.HardMaxDuration +
                    TimeSpan.FromMilliseconds(1)));
    }

    private sealed class FirstCountThenHugeReadOnlyList<T> :
        IReadOnlyList<T>
    {
        private readonly IReadOnlyList<T>
            _items;
        private readonly int _hugeCount;

        public FirstCountThenHugeReadOnlyList(
            IReadOnlyList<T> items,
            int hugeCount)
        {
            _items = items;
            _hugeCount = hugeCount;
        }

        public int CountReads { get; private set; }

        public int Count
        {
            get
            {
                CountReads++;
                return CountReads == 1
                    ? _items.Count
                    : _hugeCount;
            }
        }

        public T this[int index] =>
            _items[index];

        public IEnumerator<T> GetEnumerator() =>
            throw new InvalidOperationException(
                "Bounded materialization must not enumerate caller-owned stateful collections.");

        System.Collections.IEnumerator
            System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }

    private sealed class IndexedOnlyReadOnlyList<T> :
        IReadOnlyList<T>
    {
        private readonly IReadOnlyList<T>
            _items;

        public IndexedOnlyReadOnlyList(
            IReadOnlyList<T> items)
        {
            _items = items;
        }

        public int Count =>
            _items.Count;

        public T this[int index] =>
            _items[index];

        public IEnumerator<T> GetEnumerator() =>
            throw new InvalidOperationException(
                "Bounded materialization must use admitted indexed entries, not the caller enumerator.");

        System.Collections.IEnumerator
            System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }

    private sealed class CountOnlyReadOnlyList<T> :
        IReadOnlyList<T>
    {
        public CountOnlyReadOnlyList(
            int count)
        {
            Count = count;
        }

        public int Count { get; }

        public T this[int index] =>
            throw new InvalidOperationException(
                "Oversized input must be rejected before enumeration.");

        public IEnumerator<T> GetEnumerator() =>
            throw new InvalidOperationException(
                "Oversized input must be rejected before enumeration.");

        System.Collections.IEnumerator
            System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }
}
