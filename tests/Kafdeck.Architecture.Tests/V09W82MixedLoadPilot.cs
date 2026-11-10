using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W77 bounded laboratory")]
public sealed class V09W82MixedLoadPilot
{
    private const int SteadyBatches = 40;
    private static readonly int[] Concurrency = [1, 10, 50];
    private static readonly string[] ReadRoutes = [
        "/api/v1/governance/catalog/teams?clusterId=permitted&pageSize=5",
        "/api/v1/governance/requests?clusterId=permitted&pageSize=5",
        "/api/v1/governance/policies/versions?clusterId=permitted&pageSize=5"
    ];
    private const string Catalog = "/api/v1/governance/catalog/teams";
    private const string Explain = "/api/v1/governance/policies/explain";
    private static readonly int[] CatalogSuccess = [200, 201];

    [W77LiveFact] public Task SQLite_mixed_authenticated_governance() => RunAsync(false);
    [W77LiveFact] public Task PostgreSQL_mixed_authenticated_governance() => RunAsync(true);

    private static async Task RunAsync(bool postgres)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(15));
        var token = deadline.Token;
        var setupClock = Stopwatch.StartNew();
        var broker = await W77OwnedKafka.CreateAsync(token);
        W77Laboratory? lab = null;
        var profiles = new List<object>();
        var readiness = new List<Wire>();
        var failures = 0;
        var committed = 0;
        var omittedWrites = 0;
        var allIds = new HashSet<string>(StringComparer.Ordinal);
        string? databaseVersion = null;
        var measurements = new List<string>();
        try
        {
            lab = await W77Laboratory.CreateAsync(postgres, token, broker.Bootstrap);
            var data = await W77GovernanceDataset.CreateAsync(lab, token);
            databaseVersion = await lab.DatabaseVersionAsync(token);
            var api = lab.Api;
            var attribution = "/api/v1/governance/attribution?clusterId=permitted&pageSize=5&resourceId=" +
                Uri.EscapeDataString(data.Topic) + "&fromUtc=" + Uri.EscapeDataString(data.FromUtc) +
                "&toUtc=" + Uri.EscapeDataString(data.ToUtc);
            var readRoutes = ReadRoutes.Append(attribution).ToArray();
            using var session = await api.AuthenticateHumanHttpAsync(token);
            using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false, MaxConnectionsPerServer = 50 })
                { BaseAddress = api.Api, Timeout = TimeSpan.FromSeconds(30) };
            client.DefaultRequestHeaders.Authorization = session.DefaultRequestHeaders.Authorization;
            using var unauthorized = new HttpClient(new SocketsHttpHandler { UseProxy = false })
                { BaseAddress = api.Api, Timeout = TimeSpan.FromSeconds(5) };
            using (var denied = await unauthorized.GetAsync(ReadRoutes[0], token))
                Assert.Equal(HttpStatusCode.Unauthorized, denied.StatusCode);
            using (var denied = await client.GetAsync("/api/v1/governance/catalog/teams?clusterId=hidden", token))
                Assert.Equal(HttpStatusCode.Forbidden, denied.StatusCode);
            foreach (var route in readRoutes)
                await ReadAsync(client, route, readiness, token);
            var explained = await ExplainAsync(client, data.Topic, readiness, token);
            _ = explained.GetProperty("allowed").GetBoolean();
            string? setupTeam = null;
            string? preflightWriteError = null;
            try { setupTeam = await CommitCatalogAsync(client, 0, 0, readiness, token); }
            catch (Exception ex) when (!token.IsCancellationRequested &&
                ex is HttpRequestException or OperationCanceledException or InvalidOperationException)
            {
                preflightWriteError = ex.GetType().Name;
                await W77Laboratory.EvidenceAsync(postgres ? "postgres-w82-preflight-failure" : "sqlite-w82-preflight-failure", new
                {
                    sourceSha = Environment.GetEnvironmentVariable("KAFDECK_SOURCE_SHA"),
                    stage = "preflight catalog commit",
                    error = preflightWriteError,
                    httpTimeoutSeconds = client.Timeout.TotalSeconds,
                    readiness
                }, CancellationToken.None);
            }
            if (setupTeam is not null)
            {
                var setupReadback = await SendAsync(client, HttpMethod.Get,
                    Catalog + "/" + Uri.EscapeDataString(setupTeam) + "?clusterId=permitted", null, null, readiness, token);
                Assert.Equal(setupTeam, setupReadback.GetProperty("id").GetString());
            }
            var started = DateTimeOffset.UtcNow;
            var clock = Stopwatch.StartNew();
            var warm = new List<W77ProcessSample>();
            for (var second = 0; second < 10; second++)
            {
                var tick = Stopwatch.StartNew();
                await ReadAsync(client, readRoutes[second % readRoutes.Length], readiness, token);
                warm.Add(W77ProcessSample.Read(api.OwnedProcessId, clock));
                await PaceAsync(tick, token);
            }

            foreach (var workers in Concurrency)
            {
                var results = new List<OperationSample>(workers * SteadyBatches);
                var resources = new List<W77ProcessSample>(SteadyBatches);
                var refreshMs = new List<double>();
                var phaseStart = clock.Elapsed.TotalSeconds;
                for (var batch = 0; batch < SteadyBatches; batch++)
                {
                    var tick = Stopwatch.StartNew();
                    if (api.HumanHttpTokenExpiresAtUtc <= DateTimeOffset.UtcNow.AddSeconds(30))
                    {
                        await api.RefreshHumanHttpAsync(client, token);
                        refreshMs.Add(tick.Elapsed.TotalMilliseconds);
                    }
                    var launched = clock.Elapsed.TotalSeconds;
                    var jobs = Enumerable.Range(0, workers).Select(worker =>
                        MeasureAsync(client, data.Topic, readRoutes, workers, batch, worker, launched, setupTeam is not null, token));
                    results.AddRange(await Task.WhenAll(jobs));
                    resources.Add(W77ProcessSample.Read(api.OwnedProcessId, clock));
                    await PaceAsync(tick, token);
                }
                var elapsed = clock.Elapsed.TotalSeconds - phaseStart;
                var applied = results.Where(sample => sample.CatalogTeamId is not null).ToArray();
                foreach (var sample in applied)
                    Assert.True(allIds.Add(sample.CatalogTeamId!), "Duplicate committed catalog team identifier.");
                foreach (var item in applied.Take(1))
                {
                    var wire = new List<Wire>();
                    var readback = await SendAsync(client, HttpMethod.Get,
                        Catalog + "/" + Uri.EscapeDataString(item.CatalogTeamId!) + "?clusterId=permitted", null, null, wire, token);
                    Assert.Equal(item.CatalogTeamId, readback.GetProperty("id").GetString());
                    measurements.Add(item.CatalogTeamId!);
                }
                committed += applied.Length;
                failures += results.Count(result => result.Error is not null && result.Http.Count > 0);
                omittedWrites += results.Count(result => result.Error == "write-preflight-blocked");
                var attempted = results.Count(result => result.Http.Count > 0);
                profiles.Add(new
                {
                    workers, startedAtSeconds = phaseStart, elapsedSeconds = elapsed, refreshMs,
                    plannedBatches = SteadyBatches,
                    attemptedOperations = attempted,
                    operationsPerSecond = attempted / elapsed,
                    transportRequests = results.Sum(result => result.Http.Count),
                    transport429 = results.Sum(result => result.Http.Count(wire => wire.Status == 429)),
                    committedAndVerified = applied.Length,
                    counts = results.GroupBy(result => result.Kind).Select(group => new
                    {
                        kind = group.Key, planned = group.Count(), attempted = group.Count(sample => sample.Http.Count > 0),
                        omitted = group.Count(sample => sample.Error == "write-preflight-blocked"),
                        errors = group.Count(sample => sample.Error is not null && sample.Http.Count > 0),
                        latencyMs = Summarize(group.Where(sample => sample.Error is null).Select(sample => sample.ElapsedMs))
                    }).ToArray(),
                    results, resources
                });
            }

            var cool = new List<W77ProcessSample>();
            for (var second = 0; second < 10; second++)
            {
                cool.Add(W77ProcessSample.Read(api.OwnedProcessId, clock));
                await Task.Delay(TimeSpan.FromSeconds(1), token);
            }
            broker.MarkSuccessful();
            await lab.DisposeAsync();
            await broker.DisposeAsync();
            Assert.False(Directory.Exists(lab.DirectoryPath));
            lab = null;
            await W77Laboratory.EvidenceAsync(postgres ? "postgres-w82-mixed-pilot" : "sqlite-w82-mixed-pilot", new
            {
                sourceSha = Environment.GetEnvironmentVariable("KAFDECK_SOURCE_SHA"),
                environment = new
                {
                    runtime = Environment.Version.ToString(),
                    os = System.Runtime.InteropServices.RuntimeInformation.OSDescription,
                    architecture = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString(),
                    cpu = Environment.GetEnvironmentVariable("KAFDECK_W82_CPU_MODEL"),
                    logicalCpuCount = Environment.ProcessorCount,
                    memoryAvailableBytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
                    database = postgres ? "PostgreSQL standalone" : "SQLite standalone",
                    databaseVersion, kafkaVersion = broker.Version, keycloakVersion = "26.8.1",
                    network = "isolated loopback on a shared workstation"
                },
                dataset = new
                {
                    clusters = 1, initialTeams = 8, verifiedPilotPreflightTeams = setupTeam is null ? 0 : 1,
                    approvedRequests = 1, activePolicyVersions = 1,
                    measurementWindows = 1, verifiedBrokerTopic = data.Topic, committedCatalogTeams = committed
                },
                started, setupSeconds = setupClock.Elapsed.TotalSeconds - clock.Elapsed.TotalSeconds,
                readiness, warm, profiles, cool, failures, omittedWrites, preflightWriteError, httpTimeoutSeconds = client.Timeout.TotalSeconds,
                catalogReadbackIds = measurements, isolatedCleanup = true,
                scope = "Paced application pilot, 40 batches per level and one scheduled logical operation per worker per batch. The planned 80/15/5 mix is reads, policy explain, and team mutations through preview/confirm/execute. A failed write preflight omits all scheduled writes and preserves the read/explain metrics as an incomplete workload. Broker traffic occurs in setup. Each provider runs standalone with one registered Keycloak identity. Full W82 reference, GC/container metrics and numerical budget admission remain open."
            }, token);
        }
        finally
        {
            if (lab is not null) await lab.DisposeAsync();
            await broker.DisposeAsync();
        }
        Assert.Equal(0, omittedWrites);
        Assert.Equal(0, failures);
        Assert.Equal(122, committed);
        Assert.Equal(3, measurements.Count);
    }

    private static async Task PaceAsync(Stopwatch tick, CancellationToken token)
    {
        var delay = TimeSpan.FromSeconds(1) - tick.Elapsed;
        if (delay > TimeSpan.Zero) await Task.Delay(delay, token);
    }

    internal static async Task<OperationSample> MeasureAsync(HttpClient client, string topic, string[] reads, int workers,
        int batch, int worker, double launched, bool writesAdmitted, CancellationToken token, string phase = "pilot",
        string? existingTeam = null)
    {
        var sequence = batch * workers + worker;
        var slot = sequence * 7 % 20;
        var kind = slot < 16 ? "read" : slot < 19 ? "explain" : "catalogCommit";
        var watch = Stopwatch.StartNew();
        var wire = new List<Wire>();
        if (kind == "catalogCommit" && !writesAdmitted)
            return new OperationSample(batch, worker, kind, launched, 0, "write-preflight-blocked", null, wire);
        string? error = null, teamId = null;
        try
        {
            if (kind == "read")
                await ReadAsync(client, reads[(sequence / 20 + slot) % reads.Length], wire, token);
            else if (kind == "explain")
            {
                var explain = await ExplainAsync(client, topic, wire, token);
                _ = explain.GetProperty("allowed").GetBoolean();
            }
            else
                teamId = await CommitCatalogAsync(client, workers, sequence, wire, token, phase, existingTeam);
        }
        catch (HttpRequestException ex)
        {
            error = ex.StatusCode is HttpStatusCode status ? "http-" + ((int)status).ToString(CultureInfo.InvariantCulture) : "transport";
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { error = "request-timeout"; }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or KeyNotFoundException)
        {
            error = "response-shape-" + ex.GetType().Name;
        }
        return new OperationSample(batch, worker, kind, launched, watch.Elapsed.TotalMilliseconds, error, teamId, wire);
    }

    internal static async Task ReadAsync(HttpClient client, string route, List<Wire> wire, CancellationToken token)
    {
        var response = await SendAsync(client, HttpMethod.Get, route, null, null, wire, token);
        if (response.GetProperty("items").GetArrayLength() == 0) throw new InvalidOperationException("Missing seeded read data.");
    }

    internal static Task<JsonElement> ExplainAsync(HttpClient client, string topic, List<Wire> wire, CancellationToken token) =>
        SendAsync(client, HttpMethod.Post, Explain, new
        {
            clusterId = "permitted", resourceKind = "topic", resourceId = topic, action = "TopicCreate", risk = "Moderate"
        }, null, wire, token);

    internal static async Task<string> CommitCatalogAsync(HttpClient client, int workers, int sequence, List<Wire> wire,
        CancellationToken token, string phase = "pilot", string? existingTeam = null)
    {
        var key = "w82-" + phase + "-" + workers + "-" + sequence;
        long expectedRevision = 0;
        if (existingTeam is not null)
        {
            var existing = await SendAsync(client, HttpMethod.Get, Catalog + "/" + existingTeam + "?clusterId=permitted", null, null, wire, token);
            expectedRevision = existing.GetProperty("revision").GetInt64();
        }
        var preview = await SendAsync(client, HttpMethod.Post, Catalog + "/preview", new
        {
            clusterId = "permitted",
            changes = new[] { new { kind = "Team", id = existingTeam, expectedRevision, name = CatalogWriteName(phase, workers, sequence) } }
        }, key, wire, token);
        var op = preview.GetProperty("operation");
        var id = op.GetProperty("operationId").GetGuid();
        var previewHash = op.GetProperty("previewHash").GetString();
        var teamId = preview.GetProperty("changes")[0].GetProperty("id").GetString()!;
        var route = "/api/v1/automation/mutations/" + id.ToString("D");
        await SendAsync(client, HttpMethod.Post, route + "/confirm", new { previewHash }, null, wire, token);
        var executed = await SendAsync(client, HttpMethod.Post, route + "/execute", null, null, wire, token);
        if (executed.GetProperty("state").GetString() != "appliedVerified")
            throw new InvalidOperationException("Catalog operation lacked verified applied state.");
        return teamId;
    }

    internal static string CatalogWriteName(string phase, int workers, int sequence) => $"W82 {phase} team {workers} {sequence}";

    internal static async Task<JsonElement> SendAsync(HttpClient client, HttpMethod method, string route, object? body,
        string? key, List<Wire> wire, CancellationToken token)
    {
        var watch = Stopwatch.StartNew();
        using var request = new HttpRequestMessage(method, route);
        if (body is not null) request.Content = JsonContent.Create(body);
        if (key is not null) request.Headers.Add("Idempotency-Key", key);
        HttpResponseMessage response;
        try { response = await client.SendAsync(request, token); }
        catch (Exception ex) when (ex is OperationCanceledException or HttpRequestException)
        {
            wire.Add(new Wire(route.Split('?')[0], 0, watch.Elapsed.TotalMilliseconds, 0));
            throw;
        }
        using var owned = response;
        var data = await response.Content.ReadAsByteArrayAsync(token);
        var status = (int)response.StatusCode;
        wire.Add(new Wire(route.Split('?')[0], status, watch.Elapsed.TotalMilliseconds, data.Length));
        if (!CatalogSuccess.Contains(status))
            throw new HttpRequestException("Unexpected pilot HTTP status.", null, response.StatusCode);
        using var parsed = JsonDocument.Parse(data);
        return parsed.RootElement.Clone();
    }

    internal static object? Summarize(IEnumerable<double> source)
    {
        var values = source.Order().ToArray();
        if (values.Length == 0) return null;
        double Percentile(double p) => values[(int)Math.Ceiling(values.Length * p) - 1];
        return new { median = Percentile(.5), p95 = Percentile(.95), p99 = Percentile(.99), max = values[^1] };
    }

    internal sealed record Wire(string Route, int Status, double ElapsedMs, int ResponseBytes);
    internal sealed record OperationSample(int Batch, int Worker, string Kind, double StartedAtSeconds, double ElapsedMs,
        string? Error, string? CatalogTeamId, List<Wire> Http);
}
