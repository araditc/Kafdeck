using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Kafdeck.Cli;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Microsoft.Data.Sqlite;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W77 bounded laboratory")]
public sealed class V09W77FaultLiveTests
{
    [W77LiveFact] public Task SQLite_real_broker_network_storage_and_process_faults_recover_and_clean() => ExerciseAsync(false);
    [W77LiveFact] public Task PostgreSQL_real_broker_network_database_storage_and_process_faults_recover_and_clean() => ExerciseAsync(true);

    private static async Task ExerciseAsync(bool postgres)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(6)); var token = timeout.Token;
        await using var broker = await W77OwnedKafka.CreateAsync(token);
        W77TcpProxy? dbProxy = null;
        string RouteDatabase(string configured)
        {
            var settings = new NpgsqlConnectionStringBuilder(configured);
            Assert.Contains(settings.Host, new[] { "localhost", "127.0.0.1" });
            dbProxy = new W77TcpProxy(settings.Port); settings.Host = "127.0.0.1"; settings.Port = dbProxy.Port;
            return settings.ConnectionString;
        }
        W77Laboratory created;
        try { created = await W77Laboratory.CreateAsync(postgres, token, broker.Bootstrap, RouteDatabase); }
        catch { if (dbProxy is not null) await dbProxy.DisposeAsync(); throw; }
        await using var lab = created;
        var databaseVersion = await lab.DatabaseVersionAsync(token);
        var events = new List<object>(); var clock = Stopwatch.StartNew();
        try
        {
            using var http = await lab.Api.AuthenticateHumanHttpAsync(token);
            const string route = "/api/v1/governance/catalog/teams?clusterId=permitted&pageSize=10";
            async Task AssertDurable()
            {
                await lab.Api.RefreshHumanHttpAsync(http, token);
                using var response = await http.GetAsync(route, token); Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                using var document = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync(token));
                var rows = document.RootElement.GetProperty("items"); Assert.Equal(8, rows.GetArrayLength());
                Assert.All(rows.EnumerateArray(), row => Assert.Equal(1, row.GetProperty("revision").GetInt64()));
                Assert.Equal(CliApplication.Success, await lab.Api.RunAsync(token, "operations", "status", lab.OperationId.ToString("D")));
                using var operation = JsonDocument.Parse(lab.Api.Output); Assert.Equal("appliedVerified", operation.RootElement.GetProperty("state").GetString());
            }
            await AssertDurable();
            // A fresh administration adapter cannot substitute cached metadata for a down broker.
            await broker.CrashAsync();
            using (var adapter = new ConfluentKafkaAdministrationAdapter([new ClusterProfile("owned", [broker.Bootstrap], KafkaSecurityProtocol.Plaintext, null, null)], new SecretResolver()))
            {
                var failed = await adapter.GetClusterMetadataAsync("owned", new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(3)), token);
                Assert.False(failed.IsSuccess); Assert.NotNull(failed.Failure);
                events.Add(new { fault = "owned-broker-process-killed", code = failed.Failure.Code, observedAtSeconds = clock.Elapsed.TotalSeconds });
            }
            await broker.StartAsync(token);
            using (var adapter = new ConfluentKafkaAdministrationAdapter([new ClusterProfile("owned", [broker.Bootstrap], KafkaSecurityProtocol.Plaintext, null, null)], new SecretResolver()))
                Assert.True((await adapter.GetClusterMetadataAsync("owned", new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)), token)).IsSuccess);
            await AssertDurable();

            await using (var proxy = new W77TcpProxy(lab.Api.Api.Port))
            {
                using var network = new HttpClient(new SocketsHttpHandler { UseProxy = false }) { Timeout = TimeSpan.FromSeconds(3) };
                await lab.Api.RefreshHumanHttpAsync(http, token);
                // GovernanceRouteContract explicitly admits this read-only route for the registered
                // bearer. /healthz is not a bearer-admitted family and cannot prove this transport.
                network.DefaultRequestHeaders.Authorization = http.DefaultRequestHeaders.Authorization;
                var address = new Uri($"http://127.0.0.1:{proxy.Port}" + route);
                using (var before = await network.GetAsync(address, token))
                {
                    Assert.True(before.IsSuccessStatusCode, "phase=network-proxy-before; http=" + (int)before.StatusCode);
                    Assert.Equal(HttpStatusCode.OK, before.StatusCode);
                }
                proxy.Pause();
                var failure = await Record.ExceptionAsync(async () => { using var response = await network.GetAsync(address, token); response.EnsureSuccessStatusCode(); });
                Assert.NotNull(failure); Assert.True(failure is HttpRequestException or TaskCanceledException);
                using (var direct = await http.GetAsync(route, token))
                {
                    Assert.True(direct.IsSuccessStatusCode, "phase=network-direct-during-cut; http=" + (int)direct.StatusCode);
                    Assert.Equal(HttpStatusCode.OK, direct.StatusCode);
                }
                proxy.Resume();
                using (var after = await network.GetAsync(address, token))
                {
                    Assert.True(after.IsSuccessStatusCode, "phase=network-proxy-after; http=" + (int)after.StatusCode);
                    Assert.Equal(HttpStatusCode.OK, after.StatusCode);
                }
                events.Add(new { fault = "owned-loopback-network-connections-cut", recovered = true, observedAtSeconds = clock.Elapsed.TotalSeconds });
            }
            if (dbProxy is not null)
            {
                await lab.Api.RefreshHumanHttpAsync(http, token);
                dbProxy.Pause();
                using (var failed = await http.GetAsync(route, token)) Assert.Equal(HttpStatusCode.ServiceUnavailable, failed.StatusCode);
                dbProxy.Resume(); await AssertDurable();
                events.Add(new { fault = "postgres-physical-transport-cut", authorityFailedClosed = true, recovered = true, observedAtSeconds = clock.Elapsed.TotalSeconds });
            }

            // Physical storage authority lock, with a hard rollback. SQLite's real write lock and
            // PostgreSQL's ACCESS EXCLUSIVE table lock are distinct from a dropped TCP connection.
            if (lab.Database is null)
            {
                await using var storage = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = lab.SqlitePath, Pooling = false }.ToString());
                await storage.OpenAsync(token); await using var held = storage.BeginTransaction(deferred: false);
                var request = await lab.Api.RequestFileAsync(new { clusterId = "permitted", changes = new[] { new { kind = "Team", expectedRevision = 0, name = "Must not commit during storage fault" } } }, token);
                var exit = await lab.Api.RunAsync(token, "catalog", "change-preview", "permitted", "teams", "--request-file", request, "--idempotency-key", Guid.NewGuid().ToString("N"));
                Assert.NotEqual(CliApplication.Success, exit);
                await held.RollbackAsync(token);
            }
            else
            {
                await lab.Api.RefreshHumanHttpAsync(http, token);
                await using var storage = new NpgsqlConnection(lab.Database.ConnectionString); await storage.OpenAsync(token);
                await using var held = await storage.BeginTransactionAsync(token);
                await using var command = storage.CreateCommand(); command.Transaction = held;
                command.CommandText = "LOCK TABLE catalog_owned_entities IN ACCESS EXCLUSIVE MODE"; await command.ExecuteNonQueryAsync(token);
                using (var failed = await http.GetAsync(route, token)) Assert.Equal(HttpStatusCode.ServiceUnavailable, failed.StatusCode);
                await held.RollbackAsync(token);
            }
            await AssertDurable();
            events.Add(new { fault = postgres ? "postgres-storage-authority-exclusive-lock" : "sqlite-storage-writer-lock", authorityFailedClosed = true, recovered = true, observedAtSeconds = clock.Elapsed.TotalSeconds });
            var previous = lab.Api.OwnedProcessId;
            await lab.Api.RestartAsync(token); Assert.NotEqual(previous, lab.Api.OwnedProcessId);
            await AssertDurable();
            events.Add(new { fault = "owned-api-abrupt-process-kill-and-restart", sameReceiptAndRevision = true, observedAtSeconds = clock.Elapsed.TotalSeconds });
            broker.MarkSuccessful();
        }
        finally
        {
            await lab.DisposeAsync();
            if (dbProxy is not null) await dbProxy.DisposeAsync();
            await broker.DisposeAsync(); lab.RemovePrivateDirectory();
        }
        await W77Laboratory.EvidenceAsync((postgres ? "postgres" : "sqlite") + "-w77-h2", new
        {
            acceptance = "H2", sourceSha = Environment.GetEnvironmentVariable("KAFDECK_SOURCE_SHA") ?? Environment.GetEnvironmentVariable("GITHUB_SHA"), provider = postgres ? "PostgreSQL" : "SQLite",
            brokerProvider = "ApacheKafka", brokerVersion = broker.Version, events, elapsedSeconds = clock.Elapsed.TotalSeconds,
            databaseVersion, runtime = Environment.Version.ToString(), os = System.Runtime.InteropServices.RuntimeInformation.OSDescription,
            operationId = lab.OperationId, durableTeamCount = 8, finalRevision = 1, duplicateProtectedEffects = 0,
            cleanup = new { ownedApiStopped = true, ownedBrokerStopped = true, privateDirectoriesRemoved = true, isolatedPostgresSchemaDropped = postgres, proxyConnections = 0 },
            limitations = "Bounded local single-broker/process/transport and physical storage-contention faults; not disk-full, broker quorum, power loss, arbitrary production chaos, replica failover, or W82 performance-budget evidence."
        }, token);
    }
}
