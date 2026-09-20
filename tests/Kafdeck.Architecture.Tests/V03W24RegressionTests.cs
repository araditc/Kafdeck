using System.Text;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Records;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V03W24RegressionTests
{
    [Fact]
    public void Malformed_mask_key_configuration_fails_closed()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafdeck:Records:Masking:MaskKey"] = "ture",
            })
            .Build();

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationLoader.Load(configuration));

        Assert.Contains("MaskKey", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("ture", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Caller_cancellation_after_export_write_starts_returns_indeterminate_summary()
    {
        var service = new RecordExportService();
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new CancellationObservedWriteStream();
        using var cancellation = new CancellationTokenSource();

        var export = service.ExportAsync(
            SafePage(),
            new RecordExportRequest(
                RecordExportFormat.Ndjson,
                new RecordExportBudget(
                    maxRows: 10,
                    maxBytes: 4096,
                    maxDuration: TimeSpan.FromSeconds(5))),
            evaluator,
            identity,
            destination,
            cancellation.Token);

        await destination.Started.Task.WaitAsync(TimeSpan.FromSeconds(1));
        cancellation.Cancel();

        var summary = await export.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(RecordExportBudgetOutcome.Indeterminate, summary.Outcome);
        Assert.Equal(0, summary.RowCount);
        Assert.Equal(0, summary.ByteCount);
    }

    [Fact]
    public async Task Live_tail_resumes_after_rate_limit_until_total_tail_budget_is_reached()
    {
        var reader = new RateLimitedThenCompleteReader();
        var filter = new RecordFilterService(reader);
        var tail = new RecordLiveTailService(
            filter,
            new RecordLiveTailOptions(emptyPollDelay: TimeSpan.Zero));

        var readBudget = new RecordOperationBudget(
            maxRecords: 10,
            maxRawBytes: 1024,
            maxProjectedBytes: 2048,
            maxDuration: TimeSpan.FromSeconds(2),
            maxRecordsPerSecond: 1);
        var request = new RecordTailRequest(
            "issuer|alice",
            new RecordReadRequest(
                "cluster-a",
                "orders",
                0,
                RecordAnchor.AtOffset(0),
                RecordReadDirection.Forward,
                readBudget),
            new RecordFilterRequest(),
            new RecordTailBudget(
                maxRecords: 2,
                maxRawBytes: 1024,
                maxDuration: TimeSpan.FromSeconds(3),
                maxRecordsPerSecond: 1));

        var frames = new List<RecordTailFrame>();
        await foreach (var frame in tail.TailAsync(request))
        {
            frames.Add(frame);
        }

        Assert.Equal(2, reader.CallCount);
        Assert.Equal(3, frames.Count);
        Assert.Equal(RecordTailFrameKind.Records, frames[0].Kind);
        Assert.Equal(RecordBudgetOutcome.RateLimit, frames[0].Page!.ReadBudgetOutcome);
        Assert.Equal(0, frames[0].Page.Records.Single().RawRecord.Offset);
        Assert.Equal(RecordTailFrameKind.Records, frames[1].Kind);
        Assert.Equal(1, frames[1].Page!.Records.Single().RawRecord.Offset);
        Assert.Equal(RecordTailFrameKind.Completed, frames[2].Kind);
        Assert.Equal(1, reader.SecondAnchorOffset);
    }

    private static (AuthorizationPolicyEvaluator Evaluator, OperatorIdentity Identity) ExportAuthorization()
    {
        var policy = AuthorizationPolicyCompiler.Compile(
            new AuthorizationPolicyDefinition(
                [
                    new AuthorizationRoleDefinition(
                        "exporter",
                        [
                            new AuthorizationPermissionDefinition(
                                AuthorizationAction.RecordExport,
                                ["cluster-a"],
                                ["orders"]),
                        ]),
                ],
                [
                    new AuthorizationSubjectBindingDefinition(
                        "https://issuer.example",
                        "operator-1",
                        ["exporter"]),
                ],
                []));

        return (
            new AuthorizationPolicyEvaluator(policy),
            new OperatorIdentity(new OperatorIdentityKey("https://issuer.example", "operator-1")));
    }

    private static RecordSafePage SafePage()
    {
        var record = new RecordSafeProjection(
            0,
            1,
            DateTimeOffset.UtcNow,
            Encoding.UTF8.GetBytes("safe-key"),
            false,
            RecordPayloadProjectionKind.Raw,
            Encoding.UTF8.GetBytes("safe-value"),
            null,
            [],
            "none",
            1,
            []);

        return new RecordSafePage(
            "cluster-a",
            "orders",
            0,
            [record],
            0,
            100,
            null,
            null,
            RecordBudgetOutcome.Complete,
            RecordFilterBudgetOutcome.Complete,
            [],
            "none",
            1);
    }

    private static KafkaRawRecord Raw(long offset) =>
        new(
            offset,
            DateTimeOffset.UtcNow,
            Encoding.UTF8.GetBytes($"key-{offset}"),
            Encoding.UTF8.GetBytes($"value-{offset}"),
            []);

    private static ObservationMetadata LiveObservation()
    {
        var now = DateTimeOffset.UtcNow;
        return new ObservationMetadata(now, now, now, ObservationSource.Live);
    }

    private sealed class RateLimitedThenCompleteReader : IKafkaRecordReadPort
    {
        public int CallCount { get; private set; }
        public long? SecondAnchorOffset { get; private set; }

        public Task<KafkaResult<RecordReadBatch>> ReadPageAsync(
            RecordReadRequest request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            CallCount++;
            if (CallCount == 1)
            {
                return Task.FromResult(
                    KafkaResult<RecordReadBatch>.Success(
                        new RecordReadBatch(
                            [Raw(0)],
                            0,
                            10,
                            0,
                            0,
                            RecordAnchor.AtOffset(1),
                            null,
                            RecordBudgetOutcome.RateLimit),
                        LiveObservation()));
            }

            SecondAnchorOffset = request.Anchor.Offset;
            return Task.FromResult(
                KafkaResult<RecordReadBatch>.Success(
                    new RecordReadBatch(
                        [Raw(1)],
                        0,
                        10,
                        1,
                        1,
                        RecordAnchor.AtOffset(2),
                        RecordAnchor.AtOffset(1),
                        RecordBudgetOutcome.Complete),
                    LiveObservation()));
        }
    }

    private sealed class CancellationObservedWriteStream : Stream
    {
        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            Started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        }
    }
}
