using System.Diagnostics;
using System.Text;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class RecordExportOwnershipTests
{
    [Fact]
    public async Task Export_deadline_isolated_from_synchronously_blocking_write_invocation()
    {
        var service = new RecordExportService();
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new SynchronouslyBlockingWriteStream();
        var stopwatch = Stopwatch.StartNew();

        var summary = await service.ExportAsync(
            SafePage(),
            new RecordExportRequest(RecordExportFormat.Ndjson, new RecordExportBudget(10, 4096, TimeSpan.FromMilliseconds(20))),
            evaluator,
            identity,
            destination);

        Assert.Equal(RecordExportBudgetOutcome.DurationLimit, summary.Outcome);
        Assert.Equal(0, summary.RowCount);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Export_reports_indeterminate_while_owned_noncooperative_write_may_commit_later()
    {
        var service = new RecordExportService();
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new LateCommitWriteStream(TimeSpan.FromMilliseconds(300));
        var stopwatch = Stopwatch.StartNew();

        var summary = await service.ExportAsync(
            SafePage(),
            new RecordExportRequest(RecordExportFormat.Ndjson, new RecordExportBudget(10, 4096, TimeSpan.FromMilliseconds(20))),
            evaluator,
            identity,
            destination);

        Assert.Equal(RecordExportBudgetOutcome.Indeterminate, summary.Outcome);
        Assert.Equal(0, summary.RowCount);
        Assert.Equal(0, summary.ByteCount);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1));
        Assert.True(await destination.Committed.WaitAsync(TimeSpan.FromSeconds(2)) > 0);
    }

    private static (AuthorizationPolicyEvaluator Evaluator, OperatorIdentity Identity) ExportAuthorization()
    {
        var policy = AuthorizationPolicyCompiler.Compile(new AuthorizationPolicyDefinition(
            [new AuthorizationRoleDefinition("exporter", [new AuthorizationPermissionDefinition(AuthorizationAction.RecordExport, ["cluster-a"], ["orders"])])],
            [new AuthorizationSubjectBindingDefinition("https://issuer.example", "operator-1", ["exporter"])], []));
        return (new AuthorizationPolicyEvaluator(policy), new OperatorIdentity(new OperatorIdentityKey("https://issuer.example", "operator-1")));
    }

    private static RecordSafePage SafePage()
    {
        var record = new RecordSafeProjection(0, 1, DateTimeOffset.UtcNow, Encoding.UTF8.GetBytes("safe-key"), false,
            RecordPayloadProjectionKind.Raw, Encoding.UTF8.GetBytes("safe-value"), null, [], "none", 1, []);
        return new RecordSafePage("cluster-a", "orders", 0, [record], 0, 100, null, null,
            RecordBudgetOutcome.Complete, RecordFilterBudgetOutcome.Complete, [], "none", 1);
    }

    private sealed class SynchronouslyBlockingWriteStream : Stream
    {
        private readonly ManualResetEventSlim _release = new(false);
        private bool _disposed;
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => !_disposed;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _release.Wait();
            if (_disposed) throw new ObjectDisposedException(nameof(SynchronouslyBlockingWriteStream));
            return ValueTask.CompletedTask;
        }
        protected override void Dispose(bool disposing)
        {
            if (disposing && !_disposed) { _disposed = true; _release.Set(); }
            base.Dispose(disposing);
        }
        public override ValueTask DisposeAsync() { Dispose(true); GC.SuppressFinalize(this); return ValueTask.CompletedTask; }
    }

    private sealed class LateCommitWriteStream : Stream
    {
        private readonly TimeSpan _delay;
        private readonly TaskCompletionSource<int> _committed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public LateCommitWriteStream(TimeSpan delay) => _delay = delay;
        public Task<int> Committed => _committed.Task;
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Delay(_delay, CancellationToken.None).ConfigureAwait(false);
            _committed.TrySetResult(buffer.Length);
        }
    }
}
