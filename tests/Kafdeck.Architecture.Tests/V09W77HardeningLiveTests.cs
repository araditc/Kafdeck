using System.Diagnostics;
using System.Net;
using System.Text.Json;
using Kafdeck.Cli;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W77 bounded laboratory")]
public sealed class V09W77HardeningLiveTests
{
    [W77LiveFact] public Task SQLite_fixed_load_soak_resources_and_cleanup() => SoakAsync(false);
    [W77LiveFact] public Task PostgreSQL_fixed_load_soak_resources_and_cleanup() => SoakAsync(true);

    private static async Task SoakAsync(bool postgres)
    {
        // Finite reproducibility fixture, not the W82 2h reference soak or a production SLA.
        const int warmSamples = 10, loadSamples = 300, coolSamples = 10, concurrency = 4;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(12)); var token = timeout.Token;
        await using var broker = await W77OwnedKafka.CreateAsync(token);
        await using var lab = await W77Laboratory.CreateAsync(postgres, token, broker.Bootstrap);
        var dataset = await W77GovernanceDataset.CreateAsync(lab, token);
        var databaseVersion = await lab.DatabaseVersionAsync(token);
        var pid = lab.Api.OwnedProcessId; var clock = Stopwatch.StartNew();
        var samples = new List<object>(); var apiSamples = new List<W77ProcessSample>(); var latency = new List<double>(); var requests = 0; var metadataRequests = 0;
        using var http = await lab.Api.AuthenticateHumanHttpAsync(token);
        var from = Uri.EscapeDataString(dataset.FromUtc); var to = Uri.EscapeDataString(dataset.ToUtc);
        string[] routes = ["/api/v1/governance/catalog/teams?clusterId=permitted&pageSize=5",
            "/api/v1/governance/requests?clusterId=permitted&pageSize=5",
            "/api/v1/governance/policies/versions?clusterId=permitted&pageSize=5",
            $"/api/v1/governance/attribution?clusterId=permitted&pageSize=5&resourceId={dataset.Topic}&fromUtc={from}&toUtc={to}"];
        async Task<double> ReadAsync(string route)
        {
            var watch = Stopwatch.StartNew();
            using var response = await http.GetAsync(route, token);
            Assert.True(response.IsSuccessStatusCode, "Actual authenticated W77 workload returned HTTP " + (int)response.StatusCode);
            var body = await response.Content.ReadAsByteArrayAsync(token);
            Assert.InRange(body.Length, 2, 256 * 1024);
            using var parsed = JsonDocument.Parse(body);
            Assert.Equal(JsonValueKind.Object, parsed.RootElement.ValueKind);
            if (route == routes[0])
            {
                Assert.Equal(5, parsed.RootElement.GetProperty("items").GetArrayLength());
                Assert.Equal(JsonValueKind.String, parsed.RootElement.GetProperty("nextCursor").ValueKind);
            }
            else if (routes.Contains(route)) Assert.NotEmpty(parsed.RootElement.GetProperty("items").EnumerateArray());
            return watch.Elapsed.TotalMilliseconds;
        }
        try
        {
            foreach (var phase in new[] { (Name: "warm", Count: warmSamples), (Name: "load", Count: loadSamples), (Name: "cool", Count: coolSamples) })
            {
                for (var tick = 0; tick < phase.Count; tick++)
                {
                    var begin = Stopwatch.StartNew();
                    if (phase.Name == "load")
                    {
                        // Renew from the actual IdP expiry between batches; fixture token
                        // lifetimes and backend authentication remain unchanged.
                        if (lab.Api.HumanHttpTokenExpiresAtUtc <= DateTimeOffset.UtcNow.AddSeconds(20))
                            await lab.Api.RefreshHumanHttpAsync(http, token);
                        latency.AddRange(await Task.WhenAll(routes.Select(ReadAsync))); requests += concurrency;
                        if (tick % 30 == 0)
                        {
                            await ReadAsync("/api/v1/clusters/permitted"); requests++; metadataRequests++;
                        }
                    }
                    else if (phase.Name == "warm") { await ReadAsync(routes[0]); requests++; }
                    var apiSample = W77ProcessSample.Read(pid, clock); apiSamples.Add(apiSample);
                    samples.Add(new { phase = phase.Name, tick, api = apiSample, testhost = W77ProcessSample.Read(Environment.ProcessId, clock),
                        sqliteBytes = File.Exists(lab.SqlitePath) ? new FileInfo(lab.SqlitePath).Length : (long?)null,
                        walBytes = File.Exists(lab.SqlitePath + "-wal") ? new FileInfo(lab.SqlitePath + "-wal").Length : (long?)null });
                    var remaining = TimeSpan.FromSeconds(1) - begin.Elapsed;
                    if (remaining > TimeSpan.Zero) await Task.Delay(remaining, token);
                }
            }
            Assert.Equal(loadSamples * concurrency + warmSamples + 10, requests);
            using (var denied = await http.GetAsync("/api/v1/governance/catalog/teams?clusterId=hidden", token)) Assert.Equal(HttpStatusCode.Forbidden, denied.StatusCode);
            // Actual CLI after sustained HTTP load and real abrupt API restart.
            Assert.Equal(CliApplication.Success, await lab.Api.RunAsync(token, "catalog", "get", "permitted", "teams", lab.TeamId));
            await lab.Api.RestartAsync(token);
            Assert.NotEqual(pid, lab.Api.OwnedProcessId);
            Assert.Equal(CliApplication.Success, await lab.Api.RunAsync(token, "catalog", "get", "permitted", "teams", lab.TeamId));
            using (var durable = JsonDocument.Parse(lab.Api.Output)) Assert.Equal(1, durable.RootElement.GetProperty("revision").GetInt64());
            broker.MarkSuccessful();
        }
        finally { await lab.DisposeAsync(); await broker.DisposeAsync(); lab.RemovePrivateDirectory(); }
        latency.Sort();
        var load = apiSamples.Skip(warmSamples).Take(loadSamples).ToArray();
        var meanTime = load.Average(sample => sample.ElapsedSeconds); var meanRss = load.Average(sample => sample.RssBytes);
        var slope = load.Sum(sample => (sample.ElapsedSeconds - meanTime) * (sample.RssBytes - meanRss)) /
            load.Sum(sample => Math.Pow(sample.ElapsedSeconds - meanTime, 2));
        await W77Laboratory.EvidenceAsync((postgres ? "postgres" : "sqlite") + "-w77-h1", new
        {
            acceptance = "H1", sourceSha = Environment.GetEnvironmentVariable("KAFDECK_SOURCE_SHA") ?? Environment.GetEnvironmentVariable("GITHUB_SHA"), runtime = Environment.Version.ToString(),
            os = System.Runtime.InteropServices.RuntimeInformation.OSDescription, cpuCount = Environment.ProcessorCount,
            elapsedSeconds = clock.Elapsed.TotalSeconds, provider = postgres ? "PostgreSQL" : "SQLite", seed = 77,
            databaseVersion, kafkaVersion = broker.Version, architecture = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString(),
            memoryAvailableBytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
            dataset = new { teams = 8, pageSize = 5, approvedRequests = 1, activePolicyVersions = 1, measurementWindows = 1, controlledMeasuredBytes = 98304, topic = dataset.Topic, windowId = dataset.WindowId },
            workload = new { warmSamples, loadSamples, coolSamples, concurrency, nominalIntervalSeconds = 1, requests, metadataRequests, failed = 0, skipped = 0 },
            latencyMs = new { median = latency[latency.Count / 2], p95 = latency[(int)(latency.Count * .95)], p99 = latency[(int)(latency.Count * .99)], max = latency[^1] },
            resources = new { peakRssBytes = apiSamples.Max(sample => sample.RssBytes), loadRssSlopeBytesPerSecond = slope,
                cpuSeconds = apiSamples[^1].CpuSeconds - apiSamples[0].CpuSeconds, idleRssDeltaBytes = apiSamples[^1].RssBytes - apiSamples[warmSamples - 1].RssBytes,
                idleHandleDelta = apiSamples[^1].Handles - apiSamples[warmSamples - 1].Handles, idleThreadDelta = apiSamples[^1].Threads - apiSamples[warmSamples - 1].Threads },
            achievedLoadRequestsPerSecond = (loadSamples * concurrency + metadataRequests) / (load[^1].ElapsedSeconds - load[0].ElapsedSeconds + 1),
            samples, actualKeycloak = true, actualApiAndCli = true, durableRestart = true, privateDirectoryRemoved = true, apiProcessesStopped = true,
            admittedPerformanceBudget = false, leakFreedomClaimed = false,
            limitations = "Five-minute finite exploratory soak on bounded populated governance collections and a real controlled HTTPS body-byte meter, not Kafka billable traffic. Samples expose growth and idle residuals; no W82 two-hour/reference-dataset/budget or production SLA claim."
        }, token);
    }
}
