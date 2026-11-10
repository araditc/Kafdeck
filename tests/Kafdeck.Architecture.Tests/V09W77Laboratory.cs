using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Cli;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class W77LiveFactAttribute : FactAttribute
{
    public W77LiveFactAttribute(bool requirePostgres = true)
    {
        if (Environment.GetEnvironmentVariable("KAFDECK_RUN_W77") != "1" ||
            string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")) ||
            string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")) ||
            (requirePostgres && string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES"))))
            Skip = "Requires explicit W77 opt-in and the isolated Keycloak, Kafka and applicable persistence fixtures.";
    }
}

[CollectionDefinition("W77 bounded laboratory", DisableParallelization = true)]
public sealed class W77LaboratoryCollection { }

internal sealed class W77Laboratory : IAsyncDisposable
{
    internal G2HumanCliLaboratory Api { get; }
    internal G2CliPostgreSqlDatabase? Database { get; private set; }
    internal string DirectoryPath { get; }
    internal string SqlitePath => Path.Combine(DirectoryPath, "mutations.db");
    internal string TeamId { get; private set; } = "";
    internal Guid OperationId { get; private set; }
    private W77Laboratory(string directory) { DirectoryPath = directory; Api = new(Path.Combine(directory, "api")); }

    internal static async Task<W77Laboratory> CreateAsync(bool postgres, CancellationToken token, string? bootstrap = null,
        Func<string, string>? databaseRoute = null, bool highAvailability = false, IReadOnlyList<string>? additionalClusters = null,
        bool useFaultInjectionTimeouts = true)
    {
        var directory = Path.Combine(Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.GetTempPath(),
            "w77-private-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var result = new W77Laboratory(directory);
        try
        {
            result.Database = postgres ? await G2CliPostgreSqlDatabase.CreateAsync() : null;
            var api = result.Api;
            api.RegisterHumans(["CatalogManage", "TopicCreate", "RequestSubmit", "RequestDecide", "GovernancePolicyManage", "GovernancePolicyActivate", "RateCardManage"],
                ["SystemRead", "ClusterRead", "TopicList", "TopicRead", "CatalogRead", "RequestRead", "AttributionRead", "GovernancePolicyRead"], ["GovernancePolicyActivate"], "permitted");
            var settings = api.Settings;
            foreach (var feature in new[] { "CatalogEnabled", "RequestsEnabled", "PoliciesEnabled", "AttributionEnabled" }) settings["Kafdeck:Governance:" + feature] = "true";
            settings["Kafdeck:Governance:DeploymentGeneration"] = Guid.NewGuid().ToString("D");
            settings["Kafdeck:Clusters:0:Id"] = "permitted";
            settings["Kafdeck:Clusters:0:BootstrapServers:0"] = bootstrap ?? Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!;
            settings["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext";
            if (additionalClusters is not null)
            {
                for (var index = 0; index < additionalClusters.Count; index++)
                {
                    var prefix = $"Kafdeck:Clusters:{index + 1}:";
                    settings[prefix + "Id"] = $"w82-reference-{index + 1:D2}";
                    settings[prefix + "BootstrapServers:0"] = additionalClusters[index];
                    settings[prefix + "SecurityProtocol"] = "Plaintext";
                }
            }
            settings["Kafdeck:Administration:Mutations:Enabled"] = "true";
            settings["Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled"] = "false";
            settings["Kafdeck:Administration:Mutations:MaterialDigestKey"] = "env:KAFDECK_W77_DIGEST";
            api.Secret("KAFDECK_W77_DIGEST", Convert.ToHexString(RandomNumberGenerator.GetBytes(32)));
            settings["Kafdeck:Administration:Mutations:Persistence:Provider"] = postgres ? "PostgreSql" : "Sqlite";
            settings["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"] = highAvailability ? "HighAvailability" : "Standalone";
            if (result.Database is null) settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"] = result.SqlitePath;
            else
            {
                var connection = new NpgsqlConnectionStringBuilder(result.Database.ConnectionString)
                {
                    Pooling = additionalClusters is { Count: > 0 }
                };
                if (useFaultInjectionTimeouts)
                {
                    connection.Timeout = 3;
                    connection.CommandTimeout = 3;
                }
                settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"] = "env:KAFDECK_W77_PG";
                api.Secret("KAFDECK_W77_PG", databaseRoute?.Invoke(connection.ConnectionString) ?? connection.ConnectionString);
            }
            await api.StartAsync(token);
            for (var index = 0; index < 8; index++)
            {
                var request = await api.RequestFileAsync(new { clusterId = "permitted", changes = new[]
                    { new { kind = "Team", expectedRevision = 0, name = "W77 fixed team " + index } } }, token);
                Assert.Equal(CliApplication.Success, await api.RunAsync(token, "catalog", "change-preview", "permitted", "teams", "--request-file", request, "--idempotency-key", Guid.NewGuid().ToString("N")));
                using var preview = JsonDocument.Parse(api.Output);
                var operation = preview.RootElement.GetProperty("operation"); var operationId = operation.GetProperty("operationId").GetGuid();
                if (index == 0) { result.TeamId = preview.RootElement.GetProperty("changes")[0].GetProperty("id").GetString()!; result.OperationId = operationId; }
                Assert.Equal(CliApplication.Success, await api.RunAsync(token, "operations", "confirm", operationId.ToString("D"), operation.GetProperty("previewHash").GetString()!));
                Assert.Equal(CliApplication.Success, await api.RunAsync(token, "operations", "execute", operationId.ToString("D")));
                using var committed = JsonDocument.Parse(api.Output); Assert.Equal("appliedVerified", committed.RootElement.GetProperty("state").GetString());
            }
            return result;
        }
        catch { await result.DisposeAsync(); throw; }
    }

    public async ValueTask DisposeAsync()
    {
        try { await Api.DisposeAsync(); }
        finally
        {
            try { if (Database is not null) { await Database.DisposeAsync(); Database = null; } }
            finally { RemovePrivateDirectory(); }
        }
    }

    internal void RemovePrivateDirectory()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        var target = Path.GetFullPath(DirectoryPath);
        var root = Path.GetFullPath(Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.GetTempPath()).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        Assert.StartsWith(root, target, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
        var name = Path.GetFileName(target); Assert.StartsWith("w77-private-", name, StringComparison.Ordinal);
        Assert.True(Guid.TryParseExact(name["w77-private-".Length..], "N", out _));
        if (Directory.Exists(target)) Directory.Delete(target, true);
        Assert.False(Directory.Exists(target));
    }

    internal async Task<string> DatabaseVersionAsync(CancellationToken token)
    {
        if (Database is not null)
        {
            await using var connection = new NpgsqlConnection(Database.ConnectionString); await connection.OpenAsync(token);
            return connection.ServerVersion;
        }
        await using var sqlite = new Microsoft.Data.Sqlite.SqliteConnection(new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder { DataSource = SqlitePath, Pooling = false }.ToString());
        await sqlite.OpenAsync(token); return sqlite.ServerVersion;
    }

    internal static async Task EvidenceAsync(string name, object body, CancellationToken token)
    {
        var directory = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.GetTempPath();
        Directory.CreateDirectory(directory);
        await File.WriteAllBytesAsync(Path.Combine(directory, name + "-evidence.json"), JsonSerializer.SerializeToUtf8Bytes(body,
            new JsonSerializerOptions { WriteIndented = true }), token);
    }
}

internal sealed record W77ProcessSample(double ElapsedSeconds, double CpuSeconds, long RssBytes, long PrivateBytes, int Handles, int Threads, int? FileDescriptors)
{
    internal static W77ProcessSample Read(int id, Stopwatch clock)
    {
        using var process = Process.GetProcessById(id); process.Refresh();
        var fds = OperatingSystem.IsLinux() ? Directory.GetFiles($"/proc/{id}/fd").Length : (int?)null;
        var sample = new W77ProcessSample(clock.Elapsed.TotalSeconds, process.TotalProcessorTime.TotalSeconds,
            process.WorkingSet64, process.PrivateMemorySize64, process.HandleCount, process.Threads.Count, fds);
        // Emergency fixture stops only; these are NOT admitted W82 product budgets.
        Assert.InRange(sample.RssBytes, 1, 2L * 1024 * 1024 * 1024);
        Assert.InRange(sample.Handles, 0, 10000); Assert.InRange(sample.Threads, 1, 1000);
        return sample;
    }
}

/// <summary>Loopback-only, test-owned TCP transport. Pause drops existing sockets and rejects new ones.
/// No host firewall, external endpoint, or application fault hook is used.</summary>
internal sealed class W77TcpProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentDictionary<int, (TcpClient Incoming, TcpClient Outgoing)> _connections = new();
    private readonly ConcurrentBag<Task> _transfers = [];
    private readonly Task _accept;
    private readonly int _destinationPort;
    private int _paused;
    private int _sequence;
    internal int Port { get; }
    internal int ActiveConnections => _connections.Count;
    internal W77TcpProxy(int destinationPort)
    {
        _destinationPort = destinationPort; _listener.Start(); Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _accept = AcceptAsync();
    }
    internal void Pause()
    {
        Interlocked.Exchange(ref _paused, 1);
        foreach (var pair in _connections.Values) { pair.Incoming.Dispose(); pair.Outgoing.Dispose(); }
    }
    internal void Resume() => Volatile.Write(ref _paused, 0);
    private async Task AcceptAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var incoming = await _listener.AcceptTcpClientAsync(_stop.Token);
                if (Volatile.Read(ref _paused) != 0) { incoming.Dispose(); continue; }
                _transfers.Add(TransferAsync(incoming));
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        catch (SocketException) when (_stop.IsCancellationRequested) { }
    }
    private async Task TransferAsync(TcpClient incoming)
    {
        using (incoming)
        using (var outgoing = new TcpClient())
        {
            var id = Interlocked.Increment(ref _sequence);
            _connections[id] = (incoming, outgoing);
            try
            {
                await outgoing.ConnectAsync(IPAddress.Loopback, _destinationPort, _stop.Token);
                if (Volatile.Read(ref _paused) != 0) return;
                using var connection = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
                var forward = incoming.GetStream().CopyToAsync(outgoing.GetStream(), connection.Token);
                var backward = outgoing.GetStream().CopyToAsync(incoming.GetStream(), connection.Token);
                await Task.WhenAny(forward, backward); await connection.CancelAsync();
                incoming.Dispose(); outgoing.Dispose();
                await Task.WhenAll(forward, backward);
            }
            catch (Exception exception) when (exception is IOException or SocketException or OperationCanceledException or ObjectDisposedException or InvalidOperationException) { }
            finally { _connections.TryRemove(id, out _); }
        }
    }
    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync(); _listener.Stop(); Pause();
        await _accept.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.WhenAll(_transfers).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, ActiveConnections); _stop.Dispose();
    }
}
