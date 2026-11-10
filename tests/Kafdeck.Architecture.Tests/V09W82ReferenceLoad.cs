using System.Diagnostics;
using System.Net;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;
using Pilot = Kafdeck.Architecture.Tests.V09W82MixedLoadPilot;

namespace Kafdeck.Architecture.Tests;

[Collection("W77 bounded laboratory")]
public sealed class V09W82ReferenceLoad
{
    private const int TopicCount = 1_000;
    private const long FixtureRssSafetyBytes = 1_610_612_736;
    private const long FixtureDiskSafetyBytes = 2_147_483_648;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    [W77LiveFact(requirePostgres: false)] public Task SQLite_reference_schedule() => RunAsync(false);
    [W77LiveFact] public Task PostgreSQL_reference_schedule() => RunAsync(true);

    private sealed record Phase(string Name, int Workers, int Seconds);
    private sealed record ColdStart(int Trial, double AuthenticatedReadyMs, int Status, long RssBytes);

    private static Phase[] Schedule(string schedule, bool fullDataset)
    {
        var preflight = new Phase("preflight", 1, 20);
        var warm = new Phase("warmup", 1, 300);
        if (schedule == "preflight")
            return fullDataset ? [preflight, new("preflight-10", 10, 20), new("preflight-50", 50, 20)] : [preflight];
        if (schedule == "first") return [preflight, warm, new Phase("steady-01-1", 1, 900)];
        if (schedule == "soak") return [preflight, warm, new Phase("soak-50", 50, 7200)];
        if (schedule != "reference") throw new ArgumentException("Unknown W82 reference schedule.");
        var steady = Enumerable.Range(1, 3).SelectMany(repetition => new[] { 1, 10, 50 }
            .Select(workers => new Phase($"steady-{repetition:D2}-{workers}", workers, 900)));
        return [preflight, warm, ..steady, new Phase("soak-50", 50, 7200)];
    }

    private static async Task RunAsync(bool postgres)
    {
        var schedule = Environment.GetEnvironmentVariable("KAFDECK_W82_REFERENCE_SCHEDULE") ?? "preflight";
        var fullDataset = Environment.GetEnvironmentVariable("KAFDECK_W82_FULL_DATASET") == "1";
        var phases = Schedule(schedule, fullDataset);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(phases.Sum(phase => phase.Seconds) + 2_400));
        var token = deadline.Token;
        var provider = postgres ? "postgres" : "sqlite";
        var clusterCount = fullDataset ? 10 : 1;
        var postgresPair = postgres && Environment.GetEnvironmentVariable("KAFDECK_W82_POSTGRES_TWO_NODE") == "1";
        var physicalPostgresPair = postgresPair && Environment.GetEnvironmentVariable("KAFDECK_W82_POSTGRES_PHYSICAL_PAIR") == "1";
        var pgSettings = postgres
            ? new Npgsql.NpgsqlConnectionStringBuilder(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")!)
            : null;
        var path = Path.Combine(Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY")!, provider + "-reference");
        Directory.CreateDirectory(path);
        var snapshots = new List<object>();
        var cold = new List<ColdStart>();
        var fixtureClock = Stopwatch.StartNew();
        var completed = false;
        var cleaned = false;
        var phaseName = "fixture-bootstrap";
        var failure = (string?)null;
        object? dataset = null;
        string? databaseVersion = null;
        string? brokerVersion = null;
        var verifiedTopics = 0;
        var errors = 0;
        var gate = new object();
        using var samplesStop = new CancellationTokenSource();
        Task? sampler = null;
        var diagnostics = new List<V09W82ReferenceDiagnostics>();
        void Checkpoint()
        {
            var data = new
            {
                sourceSha = Environment.GetEnvironmentVariable("KAFDECK_SOURCE_SHA"),
                provider, schedule, phase = phaseName, completed, cleaned, errors, failure, elapsedSeconds = fixtureClock.Elapsed.TotalSeconds,
                updatedUtc = DateTimeOffset.UtcNow,
                environment = new
                {
                    runtime = Environment.Version.ToString(), os = System.Runtime.InteropServices.RuntimeInformation.OSDescription,
                    architecture = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString(),
                    cpu = Environment.GetEnvironmentVariable("KAFDECK_W82_CPU_MODEL"), logicalProcessors = Environment.ProcessorCount,
                    memoryAvailableBytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes, databaseVersion, brokerVersion, keycloakVersion = "26.8.1",
                    network = Environment.GetEnvironmentVariable("KAFDECK_W82_NETWORK") ?? "single-workstation isolated loopback",
                    databaseConnectTimeoutSeconds = pgSettings?.Timeout, databaseCommandTimeoutSeconds = pgSettings?.CommandTimeout,
                    apiNodeCount = postgresPair ? 2 : 1,
                    persistenceNodeCount = physicalPostgresPair ? 2 : 1,
                    providerTopology = postgresPair ? "high-availability" : "standalone"
                },
                dataset, verifiedTopics, cold, phases, completedPhases = snapshots
            };
            var temporary = Path.Combine(path, "checkpoint.tmp");
            var final = Path.Combine(path, "checkpoint.json");
            File.WriteAllText(temporary, JsonSerializer.Serialize(data, Json));
            File.Move(temporary, final, true);
        }
        Checkpoint();
        var brokers = new List<W77OwnedKafka>();
        W77Laboratory? lab = null;
        G2HumanCliLaboratory? peer = null;
        HttpClient? peerClient = null;
        try
        {
            for (var index = 0; index < clusterCount; index++)
                brokers.Add(await W77OwnedKafka.CreateAsync(token, referenceDensity: fullDataset));
            var broker = brokers[0];
            brokerVersion = broker.Version;
            phaseName = "governed-seed";
            Checkpoint();
            lab = await W77Laboratory.CreateAsync(postgres, token, broker.Bootstrap, highAvailability: postgresPair,
                additionalClusters: brokers.Skip(1).Select(cluster => cluster.Bootstrap).ToArray(), useFaultInjectionTimeouts: false);
            var seed = await W77GovernanceDataset.CreateAsync(lab, token, ruleCount: 100);
            var api = lab.Api;
            databaseVersion = await lab.DatabaseVersionAsync(token);
            var reads = new[]
            {
                "/api/v1/governance/catalog/teams?clusterId=permitted&pageSize=25",
                "/api/v1/governance/catalog/applications?clusterId=permitted&pageSize=25",
                "/api/v1/governance/requests?clusterId=permitted&pageSize=25",
                "/api/v1/governance/attribution?clusterId=permitted&pageSize=25&resourceId=" +
                    Uri.EscapeDataString(seed.Topic) + "&fromUtc=" + Uri.EscapeDataString(seed.FromUtc) + "&toUtc=" + Uri.EscapeDataString(seed.ToUtc)
            };
            using var session = await api.AuthenticateHumanHttpAsync(token);
            using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false, MaxConnectionsPerServer = 50 })
                { BaseAddress = api.Api, Timeout = TimeSpan.FromSeconds(30) };
            client.DefaultRequestHeaders.Authorization = session.DefaultRequestHeaders.Authorization;
            phaseName = "catalog-and-broker-volume-seed";
            Checkpoint();
            var mutableTeams = await ImportAsync(client, 92, "Team", null, api, token);
            await ImportAsync(client, 200, "Application", lab.TeamId, api, token);
            var clusterIds = new HashSet<string>(StringComparer.Ordinal);
            for (var index = 0; index < brokers.Count; index++)
            {
                var existing = index == 0 ? seed.Topic : null;
                var committed = verifiedTopics;
                var count = TopicCount / clusterCount;
                var clusterId = await CreateTopicsAsync(brokers[index].Bootstrap, existing, index * count, count,
                    observed => { verifiedTopics = committed + observed; Checkpoint(); }, token);
                Assert.True(clusterIds.Add(clusterId), "Duplicate physical Kafka cluster identity.");
            }
            if (fullDataset)
            {
                phaseName = "historical-corpus";
                Checkpoint();
                var range = await V09W82ReferenceCorpus.SeedAsync(lab, seed,
                    () => RenewIfDueAsync(api, client, token), client, token);
                reads[3] = "/api/v1/governance/attribution?clusterId=permitted&pageSize=25" + range;
            }
            dataset = new
            {
                physicalKafkaClusters = clusterIds.Count, actualBrokerTopics = verifiedTopics, initialTeams = 100,
                applications = 200, policyRules = 100, historicalRequests = fullDataset ? V09W82ReferenceCorpus.RequestCount : 1,
                historicalAttributionWindows = fullDataset ? V09W82ReferenceCorpus.WindowCount : 0,
                measuredAttributionWindows = 1, initializedByApiAndBroker = true, historicalCorpusIsSynthetic = fullDataset
            };
            phaseName = "authenticated-starts";
            Checkpoint();

            // Long-lived fixture registrations cover the planned schedule. Keycloak tokens still expire and refresh normally.
            for (var person = 0; person < 2; person++)
                api.Settings[$"Kafdeck:NonBrowserRead:HumanRegistrations:{person}:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddHours(8).ToString("O");
            if (postgresPair)
            {
                peer = await api.StartPeerAsync(Path.Combine(lab.DirectoryPath, "peer"), token);
                peerClient = new HttpClient(new SocketsHttpHandler { UseProxy = false, MaxConnectionsPerServer = 50 })
                    { BaseAddress = peer.Api, Timeout = TimeSpan.FromSeconds(30) };
                peerClient.DefaultRequestHeaders.Authorization = client.DefaultRequestHeaders.Authorization;
            }
            for (var trial = 1; trial <= 5; trial++)
            {
                await RenewIfDueAsync(api, client, token);
                await api.StopAsync(token);
                var stopwatch = Stopwatch.StartNew();
                await api.StartAsync(token);
                using var read = await client.GetAsync("/api/v1/system/health", token);
                Assert.Equal(HttpStatusCode.OK, read.StatusCode);
                using var verified = JsonDocument.Parse(await read.Content.ReadAsByteArrayAsync(token));
                Assert.Equal("ok", verified.RootElement.GetProperty("status").GetString());
                var sample = W77ProcessSample.Read(api.OwnedProcessId, fixtureClock);
                cold.Add(new(trial, stopwatch.Elapsed.TotalMilliseconds, (int)read.StatusCode, sample.RssBytes));
                Checkpoint();
            }
            using var unauthenticated = new HttpClient(new SocketsHttpHandler { UseProxy = false }) { BaseAddress = api.Api };
            using (var denied = await unauthenticated.GetAsync(reads[0], token)) Assert.Equal(HttpStatusCode.Unauthorized, denied.StatusCode);
            using (var denied = await client.GetAsync("/api/v1/governance/catalog/teams?clusterId=hidden", token))
                Assert.Equal(HttpStatusCode.Forbidden, denied.StatusCode);
            foreach (var route in reads) await Pilot.ReadAsync(client, route, [], token);
            _ = (await Pilot.ExplainAsync(client, seed.Topic, [], token)).GetProperty("allowed").GetBoolean();
            if (peerClient is not null) peerClient.DefaultRequestHeaders.Authorization = client.DefaultRequestHeaders.Authorization;
            var initialWrite = await Pilot.CommitCatalogAsync(peerClient ?? client, 0, 0, [], token, "reference-preflight",
                fullDataset ? mutableTeams[0] : null);
            if (peerClient is not null)
            {
                var row = await Pilot.SendAsync(client, HttpMethod.Get, "/api/v1/governance/catalog/teams/" + initialWrite + "?clusterId=permitted", null, null, [], token);
                Assert.Equal(initialWrite, row.GetProperty("id").GetString());
                Assert.Equal(Pilot.CatalogWriteName("reference-preflight", 0, 0), row.GetProperty("name").GetString());
            }

            var apiPid = api.OwnedProcessId;
            var sqlitePath = lab.SqlitePath;
            var drive = new DriveInfo(Path.GetPathRoot(path)!);
            if (Environment.GetEnvironmentVariable("KAFDECK_W82_COUNTERS") is { Length: > 0 } counters)
            {
                diagnostics.Add(new(counters, apiPid, path, "primary"));
                if (peer is not null) diagnostics.Add(new(counters, peer.OwnedProcessId, path, "peer"));
            }
            sampler = SampleAsync(path, apiPid, peer?.OwnedProcessId, postgres ? null : sqlitePath, fixtureClock,
                () => { lock (gate) return phaseName; }, deadline, samplesStop.Token);
            await using var raw = new StreamWriter(Path.Combine(path, "operations.ndjson"));
            foreach (var phase in phases)
            {
                lock (gate) phaseName = phase.Name;
                Checkpoint();
                var phaseClock = Stopwatch.StartNew();
                var latency = new Dictionary<string, List<double>>(StringComparer.Ordinal);
                var outcomes = new Dictionary<string, int>(StringComparer.Ordinal);
                var batches = 0;
                var scheduled = 0;
                var httpCount = 0;
                var rejected = 0;
                var commits = 0;
                (string Id, string Name)? readback = null;
                while (phaseClock.Elapsed.TotalSeconds < phase.Seconds || (phase.Name == "preflight" && batches < 20))
                {
                    var batchClock = Stopwatch.StartNew();
                    await RenewIfDueAsync(api, client, token);
                    if (peerClient is not null) peerClient.DefaultRequestHeaders.Authorization = client.DefaultRequestHeaders.Authorization;
                    var started = fixtureClock.Elapsed.TotalSeconds;
                    var batch = await Task.WhenAll(Enumerable.Range(0, phase.Workers).Select(async worker =>
                    {
                        var viaPeer = peerClient is not null && ((batches + worker) & 1) == 1;
                        var item = await Pilot.MeasureAsync(viaPeer ? peerClient! : client, seed.Topic, reads, phase.Workers, batches,
                            worker, started, true, token, phase.Name, fullDataset ? mutableTeams[worker] : null);
                        return (item, node: viaPeer ? "peer" : "primary");
                    }));
                    batches++;
                    foreach (var (item, node) in batch)
                    {
                        await raw.WriteLineAsync(JsonSerializer.Serialize(new { phase = phase.Name, node, item }, Json));
                        scheduled++;
                        httpCount += item.Http.Count;
                        rejected += item.Http.Count(wire => wire.Status == 429);
                        if (item.Error is not null)
                        {
                            errors++;
                            outcomes[item.Error] = outcomes.GetValueOrDefault(item.Error) + 1;
                        }
                        else
                        {
                            if (!latency.TryGetValue(item.Kind, out var values)) latency[item.Kind] = values = [];
                            values.Add(item.ElapsedMs);
                            if (item.CatalogTeamId is not null)
                            {
                                commits++;
                                readback = (item.CatalogTeamId, Pilot.CatalogWriteName(phase.Name, phase.Workers,
                                    item.Batch * phase.Workers + item.Worker));
                            }
                        }
                    }
                    await raw.FlushAsync(token);
                    var delay = TimeSpan.FromSeconds(1) - batchClock.Elapsed;
                    if (delay > TimeSpan.Zero) await Task.Delay(delay, token);
                    if (drive.AvailableFreeSpace < FixtureDiskSafetyBytes)
                        throw new InvalidOperationException("Fixture disk safety floor reached.");
                }
                var elapsed = phaseClock.Elapsed.TotalSeconds;
                if (readback is { } write)
                {
                    var route = "/api/v1/governance/catalog/teams/" + Uri.EscapeDataString(write.Id) + "?clusterId=permitted";
                    var row = await Pilot.SendAsync(client, HttpMethod.Get, route, null, null, [], token);
                    Assert.Equal(write.Id, row.GetProperty("id").GetString());
                    Assert.Equal(write.Name, row.GetProperty("name").GetString());
                }
                snapshots.Add(new
                {
                    phase.Name, phase.Workers, targetSeconds = phase.Seconds, elapsedSeconds = elapsed, batches, scheduled, httpCount, rejected,
                    operationsPerSecond = scheduled / elapsed, successfulOperationsPerSecond = (scheduled - outcomes.Values.Sum()) / elapsed,
                    verifiedCatalogCommits = commits,
                    outcomes, latency = latency.Select(pair => new { kind = pair.Key, count = pair.Value.Count, ms = Pilot.Summarize(pair.Value) }).ToArray()
                });
                Checkpoint();
                if (phase.Name.StartsWith("preflight", StringComparison.Ordinal) && (errors > 0 || commits == 0))
                    throw new InvalidOperationException("Mixed reference preflight did not verify reads, policy explain and catalog writes.");
            }
            if (schedule is "soak" or "reference")
            {
                lock (gate) phaseName = "cancellation-check";
                await RenewIfDueAsync(api, client, token);
                var cancelled = await Task.WhenAll(Enumerable.Range(0, 20).Select(async _ =>
                {
                    using var abort = CancellationTokenSource.CreateLinkedTokenSource(token);
                    abort.CancelAfter(TimeSpan.FromMilliseconds(50));
                    try
                    {
                        using var response = await client.GetAsync(reads[3], abort.Token);
                        response.EnsureSuccessStatusCode();
                        return false;
                    }
                    catch (OperationCanceledException) when (!token.IsCancellationRequested) { return true; }
                }));
                using var ready = await client.GetAsync("/api/v1/system/health", token);
                Assert.Equal(HttpStatusCode.OK, ready.StatusCode);
                Assert.Contains(true, cancelled);
                await File.WriteAllTextAsync(Path.Combine(path, "cancellation.json"),
                    JsonSerializer.Serialize(new { attempts = cancelled.Length, cancelled = cancelled.Count(value => value), status = (int)ready.StatusCode }, Json), token);
            }
            lock (gate) phaseName = "postload-idle";
            await Task.Delay(TimeSpan.FromSeconds(30), token);
            completed = true;
            foreach (var ownedBroker in brokers) ownedBroker.MarkSuccessful();
        }
        catch (Exception error)
        {
            failure = error.GetType().Name;
            throw;
        }
        finally
        {
            samplesStop.Cancel();
            try
            {
                if (sampler is not null) await sampler;
            }
            finally
            {
                peerClient?.Dispose();
                try
                {
                    try { if (peer is not null) await peer.DisposeAsync(); }
                    finally { if (lab is not null) await lab.DisposeAsync(); }
                }
                finally
                {
                    try { await Task.WhenAll(diagnostics.Select(collector => collector.DisposeAsync().AsTask())); }
                    finally { await Task.WhenAll(brokers.Select(ownedBroker => ownedBroker.DisposeAsync().AsTask())); }
                }
                cleaned = true;
                Checkpoint();
            }
        }
        Assert.Equal(0, errors);
    }

    private static async Task RenewIfDueAsync(G2HumanCliLaboratory api, HttpClient client, CancellationToken token)
    {
        if (api.HumanHttpTokenExpiresAtUtc <= DateTimeOffset.UtcNow.AddSeconds(35))
            await api.RefreshHumanHttpAsync(client, token);
    }

    private static async Task SampleAsync(string path, int pid, int? peerPid, string? sqlite, Stopwatch clock, Func<string> phase,
        CancellationTokenSource deadline, CancellationToken stop)
    {
        await using var output = new StreamWriter(Path.Combine(path, "resources.ndjson"));
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        var hostCpu = V09W82ReferenceDiagnostics.HostCpu();
        try
        {
            while (await timer.WaitForNextTickAsync(stop))
            {
                var sample = W77ProcessSample.Read(pid, clock);
                var peerSample = peerPid is int id ? W77ProcessSample.Read(id, clock) : null;
                var latestHostCpu = V09W82ReferenceDiagnostics.HostCpu();
                double? hostBusyPercent = hostCpu is { } prior && latestHostCpu is { } latest &&
                    latest.Total > prior.Total && latest.Idle >= prior.Idle
                    ? 100.0 * (1.0 - (double)(latest.Idle - prior.Idle) / (latest.Total - prior.Total))
                    : null;
                hostCpu = latestHostCpu;
                static long? FileSize(string? file)
                {
                    if (file is null) return null;
                    try { return new FileInfo(file).Length; }
                    catch (FileNotFoundException) { return null; }
                }
                await output.WriteLineAsync(JsonSerializer.Serialize(new
                {
                    observedAtUtc = DateTimeOffset.UtcNow, phase = phase(), process = sample, peerProcess = peerSample,
                    hostBusyPercent, sqliteBytes = FileSize(sqlite),
                    walBytes = FileSize(sqlite is null ? null : sqlite + "-wal")
                }, Json));
                await output.FlushAsync(stop);
                if (sample.RssBytes > FixtureRssSafetyBytes || peerSample?.RssBytes > FixtureRssSafetyBytes)
                {
                    deadline.Cancel();
                    throw new InvalidOperationException("Fixture process RSS safety ceiling reached.");
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }

    private static async Task<IReadOnlyList<string>> ImportAsync(HttpClient client, int count, string kind, string? team, G2HumanCliLaboratory api,
        CancellationToken token)
    {
        const string prefix = "/api/v1/governance/catalog/imports/preview";
        var ids = new List<string>(count);
        foreach (var indices in Enumerable.Range(0, count).Chunk(25))
        {
            await RenewIfDueAsync(api, client, token);
            var key = $"w82-reference-seed-{kind}-{indices[0]}";
            var changes = indices.Select(index => new { kind, expectedRevision = 0, name = $"W82 reference seed {kind} {index:D4}", teamId = team }).ToArray();
            var preview = await Pilot.SendAsync(client, HttpMethod.Post, prefix, new { clusterId = "permitted", changes }, key, [], token);
            Assert.Equal(changes.Length, preview.GetProperty("changes").GetArrayLength());
            ids.AddRange(preview.GetProperty("changes").EnumerateArray().Select(change => change.GetProperty("id").GetString()!));
            var operation = preview.GetProperty("operation");
            var id = operation.GetProperty("operationId").GetGuid();
            var previewHash = operation.GetProperty("previewHash").GetString();
            var typedTargetChallenge = operation.GetProperty("confirmationChallenge").GetString();
            var route = "/api/v1/automation/mutations/" + id.ToString("D");
            await Pilot.SendAsync(client, HttpMethod.Post, route + "/confirm", new { previewHash, typedTargetChallenge }, null, [], token);
            var applied = await Pilot.SendAsync(client, HttpMethod.Post, route + "/execute", null, null, [], token);
            Assert.Equal("appliedVerified", applied.GetProperty("state").GetString());
        }
        return ids;
    }

    private static async Task<string> CreateTopicsAsync(string bootstrap, string? seededTopic, int offset, int count,
        Action<int> progress, CancellationToken token)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        var observed = new HashSet<string>(StringComparer.Ordinal);
        var cluster = await admin.DescribeClusterAsync(new DescribeClusterOptions { RequestTimeout = TimeSpan.FromSeconds(10) }).WaitAsync(token);
        Assert.False(string.IsNullOrEmpty(cluster.ClusterId));

        async Task ObserveAsync(string[] expected)
        {
            var clock = Stopwatch.StartNew();
            do
            {
                IReadOnlyList<TopicDescription> descriptions;
                try
                {
                    var result = await admin.DescribeTopicsAsync(TopicCollection.OfTopicNames(expected),
                        new DescribeTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(5) }).WaitAsync(token);
                    descriptions = result.TopicDescriptions;
                }
                catch (DescribeTopicsException ex) when (ex.Results.TopicDescriptions.All(topic =>
                    !topic.Error.IsError || topic.Error.Code is ErrorCode.UnknownTopicOrPart or ErrorCode.LeaderNotAvailable))
                {
                    descriptions = ex.Results.TopicDescriptions;
                }
                foreach (var topic in descriptions)
                {
                    if (!topic.Error.IsError && !topic.IsInternal && topic.TopicId is { } id &&
                        (id.MostSignificantBits.Value != 0 || id.LeastSignificantBits.Value != 0) &&
                        topic.Partitions.Count == 1 && topic.Partitions[0].Leader is { Id: >= 0 })
                        observed.Add(topic.Name);
                }
                if (expected.All(observed.Contains)) { progress(observed.Count); return; }
                await Task.Delay(TimeSpan.FromMilliseconds(500), token);
            } while (clock.Elapsed < TimeSpan.FromSeconds(25));
            progress(observed.Count);
            Assert.Equal(expected.Length, expected.Count(observed.Contains));
        }

        var seededCount = seededTopic is null ? 0 : 1;
        if (seededTopic is not null) await ObserveAsync([seededTopic]);
        foreach (var indices in Enumerable.Range(offset + seededCount, count - seededCount).Chunk(100))
        {
            var topics = indices.Select(index => new TopicSpecification
            {
                Name = $"w82-ref-topic-{index:D4}", NumPartitions = 1, ReplicationFactor = 1,
                Configs = new Dictionary<string, string> { ["segment.index.bytes"] = "32768", ["segment.bytes"] = "1048576" }
            }).ToArray();
            await admin.CreateTopicsAsync(topics, new CreateTopicsOptions
            {
                RequestTimeout = TimeSpan.FromSeconds(60), OperationTimeout = TimeSpan.FromSeconds(50)
            }).WaitAsync(token);
            await ObserveAsync(topics.Select(topic => topic.Name).ToArray());
        }
        Assert.Equal(count, observed.Count);
        return cluster.ClusterId!;
    }
}
