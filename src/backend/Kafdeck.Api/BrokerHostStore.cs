using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Security;

namespace Kafdeck.Api;

internal sealed record BrokerHostState(int Version, long Revision, BrokerHost[] Hosts,
    BrokerHostBinding[] Bindings, SecurityAuditEvent[] PendingAudit);

// Separate, versioned sidecar keeps the frozen managed-identity representation
// compatible. Its location is beside the identity state, in the same backup scope.
internal sealed class BrokerHostStore(string path)
{
    private static readonly JsonSerializerOptions Json = new()
    { UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow };

    internal BrokerHostState Read()
    {
        try
        {
            using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);
            if (input.Length > 2 * 1024 * 1024) throw new InvalidDataException("Host registry exceeds its bound.");
            var state = JsonSerializer.Deserialize<BrokerHostState>(input, Json);
            if (state is null || state.Version != 1 || state.Revision < 0 || state.Hosts is null ||
                state.Bindings is null || state.PendingAudit is null || state.Hosts.Length > 64 ||
                state.Bindings.Length > 1024 || state.PendingAudit.Length > 128 ||
                state.Hosts.Select(host => host.Id).Distinct().Count() != state.Hosts.Length ||
                state.Bindings.Any(binding => !state.Hosts.Any(host => host.Id == binding.HostId)) ||
                state.Bindings.Select(binding => (binding.ClusterId, binding.BrokerId)).Distinct().Count() != state.Bindings.Length)
                throw new InvalidDataException("Host registry is invalid or uses an unsupported format.");
            return state;
        }
        catch (FileNotFoundException) { return new(1, 0, [], [], []); }
    }

    internal BrokerHostState Commit(long revision, Func<BrokerHostState, BrokerHostState> update, SecurityAuditEvent audit)
    {
        using var lease = Lock();
        var state = Read();
        if (revision != state.Revision) throw new BrokerHostFailure("host_revision_conflict");
        if (state.PendingAudit.Length >= 128) throw new BrokerHostFailure("host_audit_unavailable");
        var changed = update(state) with
        {
            Revision = checked(state.Revision + 1),
            PendingAudit = [.. state.PendingAudit, audit],
        };
        Write(changed);
        return changed;
    }

    internal async Task DrainAuditAsync(ISecurityAuditSink sink, CancellationToken cancellation)
    {
        using var lease = Lock();
        var state = Read();
        if (state.PendingAudit.Length == 0) return;
        foreach (var item in state.PendingAudit) await sink.WriteAsync(item, cancellation);
        Write(state with { PendingAudit = [] });
    }

    private FileStream Lock()
    {
        try { return new FileStream(path + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None); }
        catch (IOException) { throw new BrokerHostFailure("host_revision_conflict"); }
    }

    private void Write(BrokerHostState state)
    {
        var temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            using (var output = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(output, state, Json);
                output.Flush(true);
            }
            File.Move(temporary, path, true);
        }
        finally { File.Delete(temporary); }
    }
}
