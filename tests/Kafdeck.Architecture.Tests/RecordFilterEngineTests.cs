using System.Text;
using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class RecordFilterEngineTests
{
    [Fact]
    public void Cel_subset_compiles_and_evaluates_paths_functions_and_regex()
    {
        var plan = RecordFilterCompiler.Compile(
            new RecordFilterRequest(
                structuredFilter: new RecordStructuredFilter(
                    RecordFilterLanguage.Cel,
                    """has(value.customer.name) && startsWith(value.customer.name, "al") && value.count >= 7 && regex(value.customer.name, "^alpha$")""")));

        using var document = JsonDocument.Parse(
            """{"customer":{"name":"alpha"},"count":7}""");

        var evaluator = new RecordFilterEvaluator();

        Assert.True(evaluator.MatchesStructuredValue(document.RootElement, plan));
        Assert.True(plan.RequiresStructuredValue);
    }

    [Fact]
    public void Jq_style_pipe_and_select_share_bounded_runtime()
    {
        var plan = RecordFilterCompiler.Compile(
            new RecordFilterRequest(
                structuredFilter: new RecordStructuredFilter(
                    RecordFilterLanguage.JqStyle,
                    """.customer | select(.age >= 18 && contains(.name, "li"))""")));

        using var allowed = JsonDocument.Parse(
            """{"customer":{"name":"alice","age":21}}""");
        using var denied = JsonDocument.Parse(
            """{"customer":{"name":"bob","age":21}}""");

        var evaluator = new RecordFilterEvaluator();

        Assert.True(evaluator.MatchesStructuredValue(allowed.RootElement, plan));
        Assert.False(evaluator.MatchesStructuredValue(denied.RootElement, plan));
    }

    [Fact]
    public void Unsupported_regex_construct_fails_at_compile_time()
    {
        var exception = Assert.Throws<RecordFilterCompilationException>(
            () => RecordFilterCompiler.Compile(
                new RecordFilterRequest(
                    structuredFilter: new RecordStructuredFilter(
                        RecordFilterLanguage.Cel,
                        """regex(value.name, "(?=secret)")"""))));

        Assert.Contains("unsupported", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Ast_and_expression_budgets_fail_closed()
    {
        var tinyExpressionBudget = new RecordFilterBudget(
            maxExpressionCharacters: 8,
            maxAstNodes: 128,
            maxAstDepth: 16,
            maxRegexCount: 4,
            maxEvaluatedRecords: 100,
            maxEvaluatedBytes: 1024,
            maxDuration: TimeSpan.FromSeconds(1));

        Assert.Throws<RecordFilterCompilationException>(
            () => RecordFilterCompiler.Compile(
                new RecordFilterRequest(
                    structuredFilter: new RecordStructuredFilter(
                        RecordFilterLanguage.Cel,
                        "value.name == \"alpha\""),
                    budget: tinyExpressionBudget)));

        var tinyAstBudget = new RecordFilterBudget(
            maxExpressionCharacters: 2048,
            maxAstNodes: 3,
            maxAstDepth: 16,
            maxRegexCount: 4,
            maxEvaluatedRecords: 100,
            maxEvaluatedBytes: 1024,
            maxDuration: TimeSpan.FromSeconds(1));

        Assert.Throws<RecordFilterCompilationException>(
            () => RecordFilterCompiler.Compile(
                new RecordFilterRequest(
                    structuredFilter: new RecordStructuredFilter(
                        RecordFilterLanguage.Cel,
                        "value.a == 1 && value.b == 2"),
                    budget: tinyAstBudget)));
    }

    [Fact]
    public void Pre_filter_uses_strict_utf8_and_all_header_predicates()
    {
        var evaluator = new RecordFilterEvaluator();
        var filter = new RecordPreFilter(
            minimumOffset: 10,
            maximumOffset: 20,
            keyPrefixUtf8: "acct:",
            headers:
            [
                new RecordHeaderPredicate("tenant", equalsUtf8: "bank-a"),
                new RecordHeaderPredicate("trace", prefixUtf8: "abc"),
            ]);

        var allowed = Raw(
            offset: 12,
            key: "acct:42",
            value: "{}",
            ("tenant", "bank-a"),
            ("trace", "abc-123"));

        Assert.True(evaluator.MatchesPreFilter(allowed, filter));

        var invalidUtf8 = allowed with
        {
            Key = new ReadOnlyMemory<byte>([0xFF, 0xFF]),
        };

        Assert.False(evaluator.MatchesPreFilter(invalidUtf8, filter));
    }

    [Fact]
    public async Task Filter_budget_continuation_stops_at_first_unevaluated_record()
    {
        var observation = LiveObservation();
        var reader = new StubRecordReader(
            KafkaResult<RecordReadBatch>.Success(
                new RecordReadBatch(
                    [
                        Raw(0, "a", """{"match":true}"""),
                        Raw(1, "b", """{"match":true}"""),
                        Raw(2, "c", """{"match":true}"""),
                    ],
                    0,
                    3,
                    0,
                    2,
                    null,
                    null,
                    RecordBudgetOutcome.Complete),
                observation));

        var budget = new RecordFilterBudget(
            maxExpressionCharacters: 2048,
            maxAstNodes: 128,
            maxAstDepth: 16,
            maxRegexCount: 4,
            maxEvaluatedRecords: 1,
            maxEvaluatedBytes: 1024 * 1024,
            maxDuration: TimeSpan.FromSeconds(5));

        var service = new RecordFilterService(reader);
        var plan = RecordFilterCompiler.Compile(
            new RecordFilterRequest(
                preFilter: new RecordPreFilter(minimumOffset: 0),
                budget: budget));

        var result = await service.FilterPageAsync(
            ReadRequest(),
            plan,
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess);
        Assert.NotNull(result.Value);
        Assert.Single(result.Value.Records);
        Assert.Equal(RecordFilterBudgetOutcome.RecordLimit, result.Value.FilterBudgetOutcome);
        Assert.Equal(1, result.Value.EvaluatedRecordCount);
        Assert.Equal(RecordAnchorKind.Offset, result.Value.NextAnchor!.Value.Kind);
        Assert.Equal(1, result.Value.NextAnchor.Value.Offset);
    }

    [Fact]
    public async Task Structured_filter_reports_decode_failure_without_leaking_payload()
    {
        var secret = "must-not-leak";
        var reader = new StubRecordReader(
            KafkaResult<RecordReadBatch>.Success(
                new RecordReadBatch(
                    [Raw(0, "key", JsonSerializer.Serialize(new { secret }))],
                    0,
                    1,
                    0,
                    0,
                    null,
                    null,
                    RecordBudgetOutcome.Complete),
                LiveObservation()));

        var decoder = new FailingDecoder();
        var service = new RecordFilterService(reader, decoder);
        var plan = RecordFilterCompiler.Compile(
            new RecordFilterRequest(
                structuredFilter: new RecordStructuredFilter(
                    RecordFilterLanguage.Cel,
                    "value.secret == \"x\"")));

        var result = await service.FilterPageAsync(
            ReadRequest(),
            plan,
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess);
        Assert.Empty(result.Value!.Records);
        Assert.Contains(
            result.Value.Limitations,
            limitation => limitation.Code == RecordFilterLimitationCode.DecodeFailed);
        Assert.DoesNotContain(
            secret,
            JsonSerializer.Serialize(result.Value.Limitations),
            StringComparison.Ordinal);
    }

    [Fact]
    public void Tail_admission_is_bounded_and_releases_identity_state()
    {
        var options = new RecordLiveTailOptions(
            globalLimit: 2,
            perClusterLimit: 2,
            perIdentityLimit: 1,
            emptyPollDelay: TimeSpan.Zero);

        var controller = new RecordTailAdmissionController(options);

        Assert.True(controller.TryAcquire("issuer|alice", "prod", out var first));
        Assert.False(controller.TryAcquire("issuer|alice", "prod", out var denied));

        denied.Dispose();
        first.Dispose();

        Assert.True(controller.TryAcquire("issuer|alice", "prod", out var afterRelease));
        afterRelease.Dispose();
    }

    [Fact]
    public async Task Live_tail_second_session_for_same_identity_is_admission_denied()
    {
        var reader = new BlockingTailReader();
        var filter = new RecordFilterService(reader);
        var tail = new RecordLiveTailService(
            filter,
            new RecordLiveTailOptions(
                globalLimit: 2,
                perClusterLimit: 2,
                perIdentityLimit: 1,
                emptyPollDelay: TimeSpan.Zero));

        var request = new RecordTailRequest(
            "issuer|alice",
            new RecordReadRequest(
                "cluster-a",
                "orders",
                0,
                RecordAnchor.Latest(),
                RecordReadDirection.Forward,
                RecordOperationBudget.Default),
            new RecordFilterRequest(),
            new RecordTailBudget(
                maxRecords: 10,
                maxRawBytes: 1024,
                maxDuration: TimeSpan.FromSeconds(5),
                maxRecordsPerSecond: 10));

        using var firstCancellation = new CancellationTokenSource();
        var firstEnumerator = tail
            .TailAsync(request, firstCancellation.Token)
            .GetAsyncEnumerator(firstCancellation.Token);

        var firstMove = firstEnumerator.MoveNextAsync().AsTask();
        await reader.Started.Task.WaitAsync(TimeSpan.FromSeconds(1));

        await using var secondEnumerator = tail
            .TailAsync(request)
            .GetAsyncEnumerator();

        Assert.True(await secondEnumerator.MoveNextAsync());
        Assert.Equal(
            RecordTailFrameKind.AdmissionDenied,
            secondEnumerator.Current.Kind);

        firstCancellation.Cancel();
        reader.Release.TrySetResult();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await firstMove);

        await firstEnumerator.DisposeAsync();
    }

    [Fact]
    public async Task Live_tail_emits_first_kafka_failure_and_does_not_retry()
    {
        var now = DateTimeOffset.UtcNow;
        var reader = new CountingFailureReader(
            KafkaResult<RecordReadBatch>.Failed(
                new KafkaFailure(
                    KafkaFailureCategory.Unavailable,
                    "kafka_unavailable",
                    "Kafka is temporarily unavailable.",
                    true),
                new ObservationMetadata(now, now, now, ObservationSource.Live)));

        var filter = new RecordFilterService(reader);
        var tail = new RecordLiveTailService(
            filter,
            new RecordLiveTailOptions(emptyPollDelay: TimeSpan.Zero));

        var request = new RecordTailRequest(
            "issuer|alice",
            new RecordReadRequest(
                "cluster-a",
                "orders",
                0,
                RecordAnchor.Latest(),
                RecordReadDirection.Forward,
                RecordOperationBudget.Default),
            new RecordFilterRequest());

        var frames = new List<RecordTailFrame>();
        await foreach (var frame in tail.TailAsync(request))
        {
            frames.Add(frame);
        }

        Assert.Single(frames);
        Assert.Equal(RecordTailFrameKind.KafkaFailure, frames[0].Kind);
        Assert.Equal("kafka_unavailable", frames[0].Failure!.Code);
        Assert.Equal(1, reader.CallCount);
    }

    [Fact]
    public async Task Live_tail_rejects_previous_direction()
    {
        var service = new RecordFilterService(
            new StubRecordReader(
                KafkaResult<RecordReadBatch>.Success(
                    new RecordReadBatch(
                        [],
                        0,
                        0,
                        null,
                        null,
                        null,
                        null,
                        RecordBudgetOutcome.Complete),
                    LiveObservation())));

        var tail = new RecordLiveTailService(service);

        var request = new RecordTailRequest(
            "issuer|alice",
            new RecordReadRequest(
                "cluster-a",
                "orders",
                0,
                RecordAnchor.Latest(),
                RecordReadDirection.Previous,
                RecordOperationBudget.Default),
            new RecordFilterRequest());

        await Assert.ThrowsAsync<ArgumentException>(
            async () =>
            {
                await foreach (var _ in tail.TailAsync(request))
                {
                }
            });
    }

    private static RecordReadRequest ReadRequest() =>
        new(
            "cluster-a",
            "orders",
            0,
            RecordAnchor.Earliest(),
            RecordReadDirection.Forward,
            RecordOperationBudget.Default);

    private static KafkaOperationContext Operation() =>
        new(DateTimeOffset.UtcNow.AddSeconds(10));

    private static ObservationMetadata LiveObservation()
    {
        var now = DateTimeOffset.UtcNow;
        return new ObservationMetadata(now, now, now, ObservationSource.Live);
    }

    private static KafkaRawRecord Raw(
        long offset,
        string key,
        string value,
        params (string Name, string Value)[] headers) =>
        new(
            offset,
            DateTimeOffset.UtcNow,
            Encoding.UTF8.GetBytes(key),
            Encoding.UTF8.GetBytes(value),
            headers
                .Select(header => new KafkaRecordHeader(
                    header.Name,
                    Encoding.UTF8.GetBytes(header.Value)))
                .ToArray());

    private sealed class BlockingTailReader : IKafkaRecordReadPort
    {
        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<KafkaResult<RecordReadBatch>> ReadPageAsync(
            RecordReadRequest request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            Started.TrySetResult();
            await Release.Task.WaitAsync(cancellationToken);

            var now = DateTimeOffset.UtcNow;
            return KafkaResult<RecordReadBatch>.Success(
                new RecordReadBatch(
                    [],
                    0,
                    0,
                    null,
                    null,
                    null,
                    null,
                    RecordBudgetOutcome.Complete),
                new ObservationMetadata(now, now, now, ObservationSource.Live));
        }
    }

    private sealed class CountingFailureReader : IKafkaRecordReadPort
    {
        private readonly KafkaResult<RecordReadBatch> _result;

        public CountingFailureReader(KafkaResult<RecordReadBatch> result)
        {
            _result = result;
        }

        public int CallCount { get; private set; }

        public Task<KafkaResult<RecordReadBatch>> ReadPageAsync(
            RecordReadRequest request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            CallCount++;
            return Task.FromResult(_result);
        }
    }

    private sealed class StubRecordReader : IKafkaRecordReadPort
    {
        private readonly KafkaResult<RecordReadBatch> _result;

        public StubRecordReader(KafkaResult<RecordReadBatch> result)
        {
            _result = result;
        }

        public Task<KafkaResult<RecordReadBatch>> ReadPageAsync(
            RecordReadRequest request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(_result);
    }

    private sealed class FailingDecoder : IRecordDecodePort
    {
        public Task<RecordSchemaResult<RecordDecodedValue>> DecodeAsync(
            RecordDecodeRequest request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(
                RecordSchemaResult<RecordDecodedValue>.Failed(
                    new RecordSchemaFailure(
                        RecordSchemaFailureCategory.DecodeFailed,
                        "record_decode_failed",
                        "Record payload could not be decoded safely.",
                        false)));
    }
}
