using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kafdeck.Api;

// Only protected plan tokens and scoped operation IDs are persisted. The
// host journal remains authoritative for dispatch and reconciliation.
internal enum BrokerAgentCheckpoint { DispatchAuthorized, PackagePublishing, RestartIntent, RestartObserved, KafkaHealthy }

internal sealed record BrokerAgentRecovery(string ClusterId, string OperationId, string Principal, string Token,
    BrokerAgentCheckpoint Checkpoint = BrokerAgentCheckpoint.DispatchAuthorized);

internal sealed class BrokerAgentRecoveryStore(string path)
{
    private static readonly JsonSerializerOptions Format = new() { UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow };

    internal BrokerAgentRecovery? Find(string clusterId) => Read().SingleOrDefault(row => row.ClusterId == clusterId);

    internal void Begin(BrokerAgentRecovery entry, Func<bool>? admitted = null)
    {
        using var lease = Lock();
        if (admitted?.Invoke() == false) throw new BrokerHostFailure("agent_install_denied");
        var rows = Read();
        var previous = rows.SingleOrDefault(row => row.ClusterId == entry.ClusterId);
        if (previous is not null)
        {
            if (previous.OperationId != entry.OperationId || previous.Principal != entry.Principal)
                throw new BrokerHostFailure("agent_previous_operation_pending");
            if (previous.Token != entry.Token)
                Write(rows.Select(row => row == previous ? previous with { Token = entry.Token } : row).ToArray());
            return;
        }
        if (rows.Length >= 32) throw new BrokerHostFailure("agent_recovery_limit");
        Write([.. rows, entry]);
    }

    internal void Advance(string clusterId, string operationId, BrokerAgentCheckpoint checkpoint)
    {
        using var lease = Lock();
        var rows = Read();
        var current = rows.SingleOrDefault(row => row.ClusterId == clusterId && row.OperationId == operationId);
        if (current is not null && current.Checkpoint < checkpoint)
            Write(rows.Select(row => row == current ? current with { Checkpoint = checkpoint } : row).ToArray());
    }

    internal void Finish(string clusterId, string operationId)
    {
        using var lease = Lock();
        var rows = Read();
        if (rows.Any(row => row.ClusterId == clusterId && row.OperationId == operationId))
            Write(rows.Where(row => row.ClusterId != clusterId || row.OperationId != operationId).ToArray());
    }

    private BrokerAgentRecovery[] Read()
    {
        try
        {
            using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);
            if (input.Length > 256 * 1024) throw new BrokerHostFailure("agent_recovery_invalid");
            var rows = JsonSerializer.Deserialize<BrokerAgentRecovery[]>(input, Format);
            if (rows is null || rows.Length > 32 || rows.Any(row => !Enum.IsDefined(row.Checkpoint)) ||
                rows.Select(row => row.ClusterId).Distinct().Count() != rows.Length)
                throw new BrokerHostFailure("agent_recovery_invalid");
            return rows;
        }
        catch (FileNotFoundException) { return []; }
        catch (JsonException) { throw new BrokerHostFailure("agent_recovery_invalid"); }
    }

    internal IDisposable ConfigurationLease() => Lock();

    private FileStream Lock()
    {
        try { return new(path + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None); }
        catch (IOException) { throw new BrokerHostFailure("agent_recovery_busy"); }
    }

    private void Write(BrokerAgentRecovery[] rows)
    {
        var temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            using (var output = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(output, rows, Format);
                output.Flush(true);
            }
            File.Move(temporary, path, true);
        }
        finally { File.Delete(temporary); }
    }
}
