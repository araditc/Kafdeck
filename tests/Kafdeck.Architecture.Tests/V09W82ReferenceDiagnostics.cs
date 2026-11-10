using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;

namespace Kafdeck.Architecture.Tests;

internal sealed class V09W82ReferenceDiagnostics : IAsyncDisposable
{
    private readonly Process process;
    private readonly Task<string> stdout;
    private readonly Task<string> stderr;
    private readonly string csv;

    internal V09W82ReferenceDiagnostics(string executable, int pid, string evidenceDirectory, string node)
    {
        csv = Path.Combine(evidenceDirectory, node + "-runtime-counters.csv");
        var start = new ProcessStartInfo(executable)
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        foreach (var argument in new[]
        {
            "collect", "--process-id", pid.ToString(CultureInfo.InvariantCulture), "--refresh-interval", "1",
            "--counters", "System.Runtime[dotnet.gc.collections,dotnet.gc.heap.total_allocated,dotnet.gc.pause.time,dotnet.thread_pool.queue.length]",
            "--format", "csv", "--output", csv
        })
            start.ArgumentList.Add(argument);
        process = Process.Start(start) ?? throw new InvalidOperationException("Runtime counter collector did not start.");
        stdout = process.StandardOutput.ReadToEndAsync();
        stderr = process.StandardError.ReadToEndAsync();
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(25));
            try { await process.WaitForExitAsync(stop.Token); }
            finally
            {
                if (!process.HasExited)
                {
                    process.Kill(entireProcessTree: true);
                    await process.WaitForExitAsync();
                }
            }
            await Task.WhenAll(stdout, stderr);
            if (process.ExitCode != 0 || !File.Exists(csv) || new FileInfo(csv).Length < 512)
                throw new InvalidOperationException("Runtime counter collector did not retain samples.");
        }
        finally { process.Dispose(); }
    }

    internal static (ulong Idle, ulong Total)? HostCpu()
    {
        if (!OperatingSystem.IsWindows() || !GetSystemTimes(out var idle, out var kernel, out var user)) return null;
        return (idle, kernel + user);
    }

    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetSystemTimes(out ulong idle, out ulong kernel, out ulong user);
}
