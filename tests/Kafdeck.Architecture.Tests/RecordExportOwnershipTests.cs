using System.Diagnostics;
using System.Text;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class RecordExportOwnershipTests
{
    private static readonly TimeSpan WriteStartBudget = TimeSpan.FromMilliseconds(250);

    [Fact]
    public async Task Export_deadline_isolated_from_synchronously_blocking_write_invocation()
    {
        var service = new RecordExportService();
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new SynchronouslyBlockingWriteStream();
        var summary = await service.ExportAsync(
            SafePage(),
            new RecordExportRequest(
                RecordExportFormat.Ndjson,
                new RecordExportBudget(maxRows: 10, maxBytes: 4096, maxDuration: WriteStartBudget)),
            evaluator,
            identity,
            destination).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(RecordExportBudgetOutcome.Indeterminate, summary.Outcome);
        Assert.Equal(0, summary.RowCount);
    }

    [Fact]
    public async Task Export_reports_indeterminate_while_owned_noncooperative_write_may_commit_later()
    {
        var service = new RecordExportService();
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new LateCommitWriteStream(TimeSpan.FromSeconds(1));
        var summary = await service.ExportAsync(
            SafePage(),
            new RecordExportRequest(
                RecordExportFormat.Ndjson,
                new RecordExportBudget(maxRows: 10, maxBytes: 4096, maxDuration: WriteStartBudget)),
            evaluator,
            identity,
            destination).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(RecordExportBudgetOutcome.Indeterminate, summary.Outcome);
        Assert.Equal(0, summary.RowCount);
        Assert.Equal(0, summary.ByteCount);
        Assert.False(destination.Committed.IsCompleted);

        var committedBytes = await destination.Committed.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.True(committedBytes > 0);
    }

    [Fact]
    public async Task Export_does_not_treat_unchanged_length_as_non_commit_proof()
    {
        var service = new RecordExportService();
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new OverwriteThenCancelStream();

        var summary = await service.ExportAsync(
            SafePage(),
            new RecordExportRequest(
                RecordExportFormat.Ndjson,
                new RecordExportBudget(maxRows: 10, maxBytes: 4096, maxDuration: WriteStartBudget)),
            evaluator,
            identity,
            destination);

        Assert.Equal(RecordExportBudgetOutcome.Indeterminate, summary.Outcome);
        Assert.Equal(16, destination.Length);
        Assert.True(await destination.Committed.WaitAsync(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task Export_stays_indeterminate_when_write_commits_before_object_disposed_exception()
    {
        var service = new RecordExportService();
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new CommitThenDisposedStream();

        var summary = await service.ExportAsync(
            SafePage(),
            new RecordExportRequest(
                RecordExportFormat.Ndjson,
                new RecordExportBudget(maxRows: 10, maxBytes: 4096, maxDuration: WriteStartBudget)),
            evaluator,
            identity,
            destination);

        Assert.Equal(RecordExportBudgetOutcome.Indeterminate, summary.Outcome);
        Assert.True(await destination.Committed.WaitAsync(TimeSpan.FromSeconds(1)));
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

    private sealed class SynchronouslyBlockingWriteStream : Stream
    {
        private readonly ManualResetEventSlim _release = new(false);
        private bool _disposed;

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => !_disposed;
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

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _release.Wait();
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(SynchronouslyBlockingWriteStream));
            }

            return ValueTask.CompletedTask;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                _disposed = true;
                _release.Set();
                _release.Dispose();
            }

            base.Dispose(disposing);
        }

        public override ValueTask DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class LateCommitWriteStream : Stream
    {
        private readonly TimeSpan _delay;
        private readonly TaskCompletionSource<int> _committed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public LateCommitWriteStream(TimeSpan delay)
        {
            _delay = delay;
        }

        public Task<int> Committed => _committed.Task;
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

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Delay(_delay, CancellationToken.None).ConfigureAwait(false);
            _committed.TrySetResult(buffer.Length);
        }

        protected override void Dispose(bool disposing)
        {
            // Deliberately non-cooperative: disposal does not cancel or terminate the pending write.
            base.Dispose(disposing);
        }
    }

    private sealed class OverwriteThenCancelStream : Stream
    {
        private readonly TaskCompletionSource<bool> _committed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task<bool> Committed => _committed.Task;
        public override bool CanRead => false;
        public override bool CanSeek => true;
        public override bool CanWrite => true;
        public override long Length => 16;
        public override long Position { get; set; }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => 0;
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _committed.TrySetResult(true); // Simulates an in-place overwrite; length is unchanged.
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class CommitThenDisposedStream : Stream
    {
        private readonly TaskCompletionSource<bool> _committed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task<bool> Committed => _committed.Task;
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

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _committed.TrySetResult(true);
            await _disposed.Task.ConfigureAwait(false);
            throw new ObjectDisposedException(nameof(CommitThenDisposedStream));
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _disposed.TrySetResult();
            }
            base.Dispose(disposing);
        }
    }
}
