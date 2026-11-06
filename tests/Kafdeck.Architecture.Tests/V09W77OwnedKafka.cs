using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Confluent.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

/// <summary>Fresh native broker using the existing verified G2 formatter, exclusively owned by this test.
/// Shared G2 brokers and unrelated PIDs are never stopped.</summary>
internal sealed class W77OwnedKafka : IAsyncDisposable
{
    private readonly string _directory;
    private readonly string _ownedRoot;
    private readonly ProcessStartInfo _start;
    private Process? _process;
    private Task<string>? _stdout;
    private Task<string>? _stderr;
    private bool _successful;
    private string _lastPrivateConsole = "";
    internal string Bootstrap { get; }
    internal string Version => "4.3.1";
    private W77OwnedKafka(string directory, string ownedRoot, string bootstrap, ProcessStartInfo start)
    { _directory = Path.GetFullPath(directory); _ownedRoot = Path.GetFullPath(ownedRoot); Bootstrap = bootstrap; _start = start; }
    internal void MarkSuccessful() => _successful = true;
    internal static async Task<W77OwnedKafka> CreateAsync(CancellationToken token)
    {
        var home = Environment.GetEnvironmentVariable("KAFDECK_W77_KAFKA_HOME") ?? throw new InvalidOperationException("Explicit verified W77 Kafka home is required.");
        var javaHome = Environment.GetEnvironmentVariable("KAFDECK_W77_JAVA_HOME") ?? throw new InvalidOperationException("Explicit W77 Java 21 home is required.");
        Assert.False(string.IsNullOrWhiteSpace(home), "phase=owned-kafka-preflight; code=kafka_home_empty");
        Assert.False(string.IsNullOrWhiteSpace(javaHome), "phase=owned-kafka-preflight; code=java_home_empty");
        home = Path.GetFullPath(home); javaHome = Path.GetFullPath(javaHome);
        var java = Path.Combine(javaHome, "bin", OperatingSystem.IsWindows() ? "java.exe" : "java");
        Assert.True(File.Exists(java), "phase=owned-kafka-preflight; code=java_executable_missing");
        Assert.True(File.Exists(Path.Combine(home, "libs/kafka_2.13-4.3.1.jar")), "phase=owned-kafka-preflight; code=kafka_jar_missing");
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(root.FullName, "Kafdeck.slnx"))) root = root.Parent!;
        var ownedRoot = Path.GetFullPath(Path.Combine(root.FullName, "deploy/dev/.generated/w77"));
        var directory = Path.Combine(ownedRoot, Guid.NewGuid().ToString("N"));
        var port = FreePort(); var controller = FreePort(); while (controller == port) controller = FreePort();
        var prepare = new ProcessStartInfo("pwsh") { WorkingDirectory = root.FullName, UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
        prepare.Environment["JAVA_HOME"] = javaHome;
        foreach (var argument in new[] { "-NoProfile", "-File", Path.Combine(root.FullName, "deploy/dev/prepare-kafka-g2.ps1"), "-KafkaHome", home,
            "-JavaHome", javaHome, "-OutputDirectory", directory, "-BrokerPort", port.ToString(System.Globalization.CultureInfo.InvariantCulture), "-ControllerPort", controller.ToString(System.Globalization.CultureInfo.InvariantCulture) }) prepare.ArgumentList.Add(argument);
        using (var formatter = Process.Start(prepare) ?? throw new InvalidOperationException("Owned formatter did not start."))
        {
            var stdout = formatter.StandardOutput.ReadToEndAsync(token); var stderr = formatter.StandardError.ReadToEndAsync(token);
            try { await formatter.WaitForExitAsync(token).WaitAsync(TimeSpan.FromSeconds(45), token); }
            finally { if (!formatter.HasExited) { formatter.Kill(true); await formatter.WaitForExitAsync(CancellationToken.None); } }
            await Task.WhenAll(stdout, stderr);
            var privateOutput = (await stdout) + (await stderr);
            if (formatter.ExitCode != 0)
            {
                Directory.CreateDirectory(directory);
                await File.WriteAllTextAsync(Path.Combine(directory, "formatter-private.log"), privateOutput, token);
            }
            Assert.True(formatter.ExitCode == 0, "phase=owned-kafka-format; exit=" + formatter.ExitCode + "; code=" + FormatterFailureCode(privateOutput) +
                "; configCreated=" + File.Exists(Path.Combine(directory, "server.properties")));
        }
        using var manifest = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(directory, "lab.json"), token));
        var start = new ProcessStartInfo(Path.GetFullPath(manifest.RootElement.GetProperty("javaExecutable").GetString()!))
            { WorkingDirectory = directory, UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var argument in new[] { "-Xms64m", "-Xmx256m", "-Dlog4j2.configurationFile=" + Path.Combine(home, "config/log4j2.yaml"),
            "-cp", Path.Combine(home, "libs/*"), "kafka.Kafka", Path.Combine(directory, "server.properties") }) start.ArgumentList.Add(argument);
        var result = new W77OwnedKafka(directory, ownedRoot, "127.0.0.1:" + port, start);
        try { await result.StartAsync(token); return result; }
        catch { await result.DisposeAsync(); throw; }
    }
    private static int FreePort() { using var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start(); return ((IPEndPoint)listener.LocalEndpoint).Port; }
    private static string FormatterFailureCode(string privateOutput)
    {
        // Only fixed allowlisted classifications leave the private, credential-free broker log.
        (string Match, string Code)[] known = [
            ("Require Java 21 and", "runtime_files_missing"), ("Select a fresh ignored", "instance_not_fresh"),
            ("UnsupportedClassVersionError", "java_version_incompatible"), ("OutOfMemoryError", "java_memory_exhausted"),
            ("Could not reserve enough space", "java_memory_reservation_failed"), ("Could not find or load main class", "storage_classpath_invalid"),
            ("ClassNotFoundException", "storage_classpath_invalid"), ("unrecognized arguments", "storage_arguments_rejected"),
            ("argument --cluster-id", "storage_cluster_id_rejected"), ("ConfigException", "storage_configuration_rejected"),
            ("Address already in use", "port_unavailable"), ("SocketException", "port_unavailable"),
            ("Permission denied", "filesystem_denied"), ("UnauthorizedAccessException", "filesystem_denied"),
            ("PositionalParameterNotFound", "powershell_arguments_rejected"), ("ParameterBindingException", "powershell_arguments_rejected"),
            ("cannot be found that accepts argument", "powershell_arguments_rejected"),
            ("does not exist", "required_path_missing"), ("Kafka storage format failed", "storage_tool_failed") ];
        foreach (var entry in known)
            if (privateOutput.Contains(entry.Match, StringComparison.OrdinalIgnoreCase)) return entry.Code;
        return "unclassified_private_diagnostic_retained";
    }
    internal async Task StartAsync(CancellationToken token)
    {
        Assert.Null(_process);
        _process = Process.Start(_start) ?? throw new InvalidOperationException("Owned broker did not start.");
        _stdout = _process.StandardOutput.ReadToEndAsync(); _stderr = _process.StandardError.ReadToEndAsync();
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap, SocketTimeoutMs = 1000 }).Build();
        for (var attempt = 0; attempt < 60 && !_process.HasExited; attempt++)
        {
            token.ThrowIfCancellationRequested();
            try { if (admin.GetMetadata(TimeSpan.FromSeconds(1)).Brokers.Count == 1) return; }
            catch (KafkaException) { }
            await Task.Delay(250, token);
        }
        throw new InvalidOperationException("phase=owned-kafka-readiness; exit=" + (_process.HasExited ? _process.ExitCode : -1) + "; private broker console retained; no shared service modified.");
    }
    internal async Task CrashAsync()
    {
        if (_process is null) return;
        if (!_process.HasExited) { _process.Kill(true); await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10)); }
        // Private native console is retained only until fixture cleanup, never uploaded.
        _lastPrivateConsole = (await _stdout!) + (await _stderr!);
        await File.WriteAllTextAsync(Path.Combine(_directory, "broker-private.log"), _lastPrivateConsole);
        _process.Dispose(); _process = null; _stdout = null; _stderr = null;
    }
    public async ValueTask DisposeAsync()
    {
        await CrashAsync();
        if (_successful)
        {
            // Resolve/check the final absolute recursive target before any deletion.
            var target = Path.GetFullPath(_directory);
            var parent = Path.GetFullPath(_ownedRoot).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
            Assert.StartsWith(parent, target, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
            Assert.True(Guid.TryParseExact(Path.GetFileName(target), "N", out _));
            Assert.Null(_process); // CrashAsync waited for the owned JVM and console streams to exit.
            for (var attempt = 0; Directory.Exists(target); attempt++)
            {
                try
                {
                    ClearOwnedReadOnlyFiles(target, parent);
                    Directory.Delete(target, true);
                }
                catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
                {
                    if (attempt >= 9)
                    {
                        // A partially completed recursive delete must not erase the only diagnostic.
                        Directory.CreateDirectory(target);
                        await File.WriteAllTextAsync(Path.Combine(target, "broker-private.log"), _lastPrivateConsole);
                        throw;
                    }
                    await Task.Delay(250);
                }
            }
            Assert.False(Directory.Exists(target));
        }
    }

    private static void ClearOwnedReadOnlyFiles(string directory, string ownedPrefix)
    {
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        Assert.StartsWith(ownedPrefix, Path.GetFullPath(directory), comparison);
        Assert.False((File.GetAttributes(directory) & FileAttributes.ReparsePoint) != 0, "Owned Kafka cleanup refuses filesystem links.");
        foreach (var entry in Directory.EnumerateFileSystemEntries(directory))
        {
            var absolute = Path.GetFullPath(entry); Assert.StartsWith(ownedPrefix, absolute, comparison);
            var attributes = File.GetAttributes(absolute);
            Assert.False((attributes & FileAttributes.ReparsePoint) != 0, "Owned Kafka cleanup refuses filesystem links.");
            if ((attributes & FileAttributes.Directory) != 0) ClearOwnedReadOnlyFiles(absolute, ownedPrefix);
            else if ((attributes & FileAttributes.ReadOnly) != 0)
                File.SetAttributes(absolute, attributes & ~FileAttributes.ReadOnly);
        }
    }
}
