using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Npgsql;

namespace Kafdeck.Architecture.Tests;

internal sealed class PostgreSqlRecoveryScenario(string repository, string directory, string schema) : IAsyncDisposable
{
    private string Schema => schema;
    internal string DirectoryPath => directory;
    internal Guid InitialGeneration { get; } = Guid.NewGuid();
    internal string AnchorPath => Path.Combine(directory, "operator-anchor.json");
    private readonly List<RecoverySenderProcess> _senders = [];

    internal static async Task<PostgreSqlRecoveryScenario> CreateAsync(CancellationToken token)
    {
        var root = Environment.GetEnvironmentVariable("KAFDECK_G2_RECOVERY_ROOT")!;
        if (!Path.IsPathFullyQualified(root)) throw new InvalidOperationException("Recovery root must be an absolute explicit scratch path.");
        var repository = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(repository.FullName, "Kafdeck.slnx")))
            repository = repository.Parent ?? throw new InvalidOperationException("Repository root unavailable.");
        System.IO.Directory.CreateDirectory(root);
        var scenario = new PostgreSqlRecoveryScenario(repository.FullName, Path.Combine(root, "scenario-" + Guid.NewGuid().ToString("N")), "recovery_" + Guid.NewGuid().ToString("N"));
        try
        {
            await scenario.ActionAsync("Prepare", token);
            await using var connection = new NpgsqlConnection(await scenario.ConnectionStringAsync("primary", token));
            await connection.OpenAsync(token);
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE SCHEMA \"{scenario.Schema}\"; CREATE TABLE \"{scenario.Schema}\".notification_delivery_generation (id INTEGER PRIMARY KEY CHECK (id=1), generation TEXT NOT NULL); INSERT INTO \"{scenario.Schema}\".notification_delivery_generation VALUES (1,@generation)";
            command.Parameters.AddWithValue("generation", scenario.InitialGeneration.ToString("D"));
            await command.ExecuteNonQueryAsync(token);
            await scenario.WriteAnchorAsync(scenario.InitialGeneration, "active", token);
            return scenario;
        }
        catch { await scenario.DisposeAsync(); throw; }
    }

    internal async Task ActionAsync(string action, CancellationToken token, Guid? targetGeneration = null, string? anchorSha256 = null)
    {
        var start = new ProcessStartInfo("pwsh")
        {
            WorkingDirectory = repository, UseShellExecute = false, CreateNoWindow = true,
            RedirectStandardOutput = true, RedirectStandardError = true,
        };
        foreach (var argument in new[] { "-NoProfile", "-File", Path.Combine(repository, "deploy", "dev", "prepare-postgres-g2-recovery.ps1"), "-Action", action, "-OutputDirectory", directory })
            start.ArgumentList.Add(argument);
        var binary = Environment.GetEnvironmentVariable("KAFDECK_G2_RECOVERY_BIN");
        if (!string.IsNullOrWhiteSpace(binary)) { start.ArgumentList.Add("-BinDirectory"); start.ArgumentList.Add(binary); }
        if (action == "Failover") { start.ArgumentList.Add("-PrimaryStopMode"); start.ArgumentList.Add("immediate"); }
        if (targetGeneration is not null) { start.ArgumentList.Add("-TargetGeneration"); start.ArgumentList.Add(targetGeneration.Value.ToString("D")); }
        if (anchorSha256 is not null) { start.ArgumentList.Add("-ExternalAnchorSha256"); start.ArgumentList.Add(anchorSha256); }
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Recovery tool process did not start.");
        using var captureCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        var output = Task.WhenAll(ReadBoundedAsync(process.StandardOutput, captureCancellation.Token),
            ReadBoundedAsync(process.StandardError, captureCancellation.Token));
        try
        {
            var exited = WaitForExactProcessExitAsync(process, token);
            // EOF belongs to every inherited pipe writer, not just the pwsh PID.
            // A detached PostgreSQL descendant may legitimately keep it open.
            // Output is a bounded diagnostic sink, never promotion/restore proof.
            await Task.WhenAny(exited, output).WaitAsync(token);
            if (output.IsCompleted) await output;
            await exited;
            await DrainOutputAsync(process, output, captureCancellation, token);
            if (process.ExitCode != 0) throw new InvalidOperationException("Recovery action failed: " + action + "; inspect retained private laboratory logs.");
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(true);
                using var reap = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await WaitForExactProcessExitAsync(process, reap.Token);
            }
        }
    }

    private static async Task WaitForExactProcessExitAsync(Process process, CancellationToken token)
    {
        // HasExited observes the exact control PID without waiting for inherited
        // redirected-stream EOF (WaitForExitAsync may include stream drainage).
        while (!process.HasExited) await Task.Delay(25, token);
        token.ThrowIfCancellationRequested();
    }

    private static async Task DrainOutputAsync(Process process, Task output, CancellationTokenSource captureCancellation,
        CancellationToken token)
    {
        if (await Task.WhenAny(output, Task.Delay(250, token)) == output) { await output; return; }
        token.ThrowIfCancellationRequested();
        await captureCancellation.CancelAsync();
        process.StandardOutput.Dispose();
        process.StandardError.Dispose();
        try { await output.WaitAsync(TimeSpan.FromSeconds(2), token); }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { }
        catch (ObjectDisposedException) { }
        catch (IOException) { }
        catch (TimeoutException)
        {
            // Both parent pipe readers are disposed. Observe any eventual fault;
            // a retained grandchild handle must not block the control process.
            _ = output.ContinueWith(static task => { _ = task.Exception; },
                CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    internal async Task<string> ConnectionStringAsync(string endpoint, CancellationToken token)
    {
        using var manifest = JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(directory, "lab.json"), token));
        var locator = manifest.RootElement.GetProperty(endpoint).GetProperty("connectionStringFile").GetString()!;
        return new NpgsqlConnectionStringBuilder((await File.ReadAllTextAsync(locator, token)).Trim())
            { SearchPath = schema, Pooling = false, IncludeErrorDetail = false }.ConnectionString;
    }
    internal async Task<AdoNotificationDeliveryStore> StoreAsync(string endpoint, CancellationToken token) =>
        new(new PostgreSqlNotificationDeliveryDbConnectionFactory(await ConnectionStringAsync(endpoint, token)));
    internal async Task<INotificationDeliveryGenerationReader> GenerationReaderAsync(string endpoint, CancellationToken token) =>
        new AdoNotificationDeliveryGenerationReader(new PostgreSqlNotificationDeliveryDbConnectionFactory(await ConnectionStringAsync(endpoint, token)));

    internal async Task<Guid> SeedAsync(NotificationRecoveryTlsPeer peer, CancellationToken token)
    {
        var connections = new PostgreSqlNotificationDeliveryDbConnectionFactory(await ConnectionStringAsync("primary", token));
        var routing = new AdoNotificationRoutingStore(connections);
        var deliveries = new AdoNotificationDeliveryStore(connections);
        await routing.InitializeAsync(token);
        await deliveries.InitializeAsync(token);
        var now = await deliveries.GetCoordinationUtcNowAsync(DateTimeOffset.UtcNow, token);
        var notification = NotificationSafeEvent.Approved(Guid.NewGuid(), NotificationApprovedEventKind.ConsumerLagAlert, now);
        var profile = new NotificationDestinationProfile("recovery-webhook", NotificationProviderKind.Webhook, "Recovery fixture",
            [NotificationEventClass.Operational, NotificationEventClass.Mutation], new Uri(peer.Endpoint));
        await routing.CreateOrGetEventAsync(notification, now, token);
        await deliveries.CreateOrGetAsync(new NotificationDeliverySnapshot(notification.EventId, profile.DestinationId,
            notification.PayloadFingerprint, NotificationDeliveryState.Pending, 0, now,
            routedProfileRevisionFingerprint: profile.RevisionFingerprint), now, token);
        if (!await new AdoNotificationRecoveryStore(connections).CheckAsync(notification.EventId, profile.DestinationId,
            notification.OccurredAtUtc, token))
            throw new InvalidOperationException("Legacy recovery fixture unexpectedly denied before sender startup.");
        return notification.EventId;
    }

    internal async Task<string> WriteAnchorAsync(Guid generation, string state, CancellationToken token)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(new { version = 1, generation, state });
        var temporary = AnchorPath + ".tmp-" + Guid.NewGuid().ToString("N");
        await File.WriteAllBytesAsync(temporary, bytes, token);
        File.Move(temporary, AnchorPath, true);
        return Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    }
    internal async Task<string> AnchorHashAsync(CancellationToken token) =>
        Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(AnchorPath, token))).ToLowerInvariant();

    internal async Task<RecoverySenderProcess> StartSenderAsync(string endpoint, NotificationRecoveryTlsPeer peer,
        string fingerprint, CancellationToken token)
    {
        var id = Guid.NewGuid().ToString("N");
        var connectionFile = Path.Combine(directory, "sender-connection-" + id + ".txt");
        await File.WriteAllTextAsync(connectionFile, await ConnectionStringAsync(endpoint, token), token);
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(connectionFile, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        var certificate = Path.Combine(directory, "ca.cer");
        await File.WriteAllBytesAsync(certificate, peer.PublicCa, token);
        var input = Path.Combine(directory, "sender-" + id + ".json");
        var ready = Path.Combine(directory, "sender-ready-" + id);
        await File.WriteAllBytesAsync(input, JsonSerializer.SerializeToUtf8Bytes(new
        {
            ConnectionStringFile = connectionFile, Endpoint = peer.Endpoint, CaCertificateFile = certificate,
            PhysicalFixturePort = peer.Port, ReadyFile = ready, InterlockPath = AnchorPath, InterlockSha256 = fingerprint,
        }), token);
        var configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent!.Name;
        var framework = new DirectoryInfo(AppContext.BaseDirectory).Name;
        var binary = Path.Combine(repository, "tests", "Kafdeck.NotificationSender.Lab", "bin", configuration, framework, "Kafdeck.NotificationSender.Lab.dll");
        if (configuration != "Release" || !File.Exists(binary)) throw new InvalidOperationException("Build the independent sender harness in Release before recovery proofs.");
        var sender = RecoverySenderProcess.Start(binary, input, ready);
        _senders.Add(sender);
        return sender;
    }

    internal async Task WriteEvidenceAsync(string name, object value, CancellationToken token)
    {
        var target = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.Combine(repository, "artifacts", "g2-recovery");
        System.IO.Directory.CreateDirectory(target);
        await File.WriteAllBytesAsync(Path.Combine(target, name), JsonSerializer.SerializeToUtf8Bytes(value,
            new JsonSerializerOptions { WriteIndented = true }), token);
    }
    internal async Task<JsonDocument> ReadEvidenceAsync(string name, CancellationToken token) =>
        JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(directory, name), token));

    internal static async Task ReadBoundedAsync(StreamReader reader, CancellationToken token)
    {
        var buffer = new char[1024];
        var count = 0;
        while (true)
        {
            var read = await reader.ReadAsync(buffer.AsMemory(), token);
            if (read == 0) return;
            count += read;
            if (count > 16384) throw new InvalidOperationException("Laboratory process output exceeded its finite budget.");
        }
    }
    public async ValueTask DisposeAsync()
    {
        foreach (var sender in _senders) await sender.DisposeAsync();
        if (File.Exists(Path.Combine(directory, "lab.json")))
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(100));
            await ActionAsync("Stop", deadline.Token);
        }
    }
}

internal sealed class RecoverySenderProcess(Process process, string readyFile, Task stdout, Task stderr) : IAsyncDisposable
{
    internal static RecoverySenderProcess Start(string binary, string configuration, string ready)
    {
        var start = new ProcessStartInfo("dotnet")
        {
            WorkingDirectory = Path.GetDirectoryName(binary)!, UseShellExecute = false, CreateNoWindow = true,
            RedirectStandardOutput = true, RedirectStandardError = true,
        };
        start.ArgumentList.Add(binary);
        start.ArgumentList.Add(configuration);
        var child = Process.Start(start) ?? throw new InvalidOperationException("Independent sender process did not start.");
        return new(child, ready, PostgreSqlRecoveryScenario.ReadBoundedAsync(child.StandardOutput, CancellationToken.None),
            PostgreSqlRecoveryScenario.ReadBoundedAsync(child.StandardError, CancellationToken.None));
    }
    internal async Task WaitReadyAsync(CancellationToken token)
    {
        while (!File.Exists(readyFile))
        {
            if (process.HasExited) throw new InvalidOperationException("Independent sender exited before startup; private output is not exported.");
            await Task.Delay(25, token);
        }
    }
    internal async Task AssertRefusedAsync(CancellationToken token)
    {
        await process.WaitForExitAsync(token);
        if (process.ExitCode != 3 || File.Exists(readyFile))
            throw new InvalidOperationException("Quarantined sender did not fail closed before startup.");
        await Task.WhenAll(stdout, stderr);
    }
    internal async Task KillAbruptlyAsync(CancellationToken token)
    {
        if (process.HasExited) throw new InvalidOperationException("Sender must still be alive at the explicit process-crash boundary.");
        process.Kill(true);
        await process.WaitForExitAsync(token);
        await Task.WhenAll(stdout, stderr);
    }
    public async ValueTask DisposeAsync()
    {
        if (!process.HasExited) { process.Kill(true); await process.WaitForExitAsync(CancellationToken.None); }
        await Task.WhenAll(stdout, stderr);
        process.Dispose();
    }
}
