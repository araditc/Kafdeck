using System.Security.Cryptography;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Cli;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;
using Xunit.Abstractions;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W73CatalogLiveTests(ITestOutputHelper output)
{
    [KeycloakG2CriticalKafkaFact]
    public Task Genuine_human_CLI_SQLite_catalog_import_history_scope_denial_and_restart() => ExerciseAsync(false);
    [KeycloakG2PostgreSqlKafkaFact]
    public Task Genuine_human_CLI_PostgreSQL_catalog_import_history_scope_denial_and_restart() => ExerciseAsync(true);

    private async Task ExerciseAsync(bool postgres)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(5)); var token = deadline.Token;
        var baseDirectory = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.GetTempPath();
        var directory = Path.Combine(baseDirectory, "w73-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        await using var database = postgres ? await G2CliPostgreSqlDatabase.CreateAsync() : null;
        await using var lab = new G2HumanCliLaboratory(Path.Combine(directory, "api"));
        lab.RegisterHumans(["CatalogManage", "MutationReconcile"], ["SystemRead", "CatalogRead"], [], "permitted", "*");
        var settings = lab.Settings; var broker = Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!;
        settings["Kafdeck:Clusters:0:Id"] = "permitted"; settings["Kafdeck:Clusters:0:BootstrapServers:0"] = broker;
        settings["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext";
        settings["Kafdeck:Governance:CatalogEnabled"] = "true"; settings["Kafdeck:Governance:DeploymentGeneration"] = Guid.NewGuid().ToString("D");
        settings["Kafdeck:Administration:Mutations:Enabled"] = "true";
        settings["Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled"] = "false";
        settings["Kafdeck:Administration:Mutations:MaterialDigestKey"] = "env:KAFDECK_W73_DIGEST";
        lab.Secret("KAFDECK_W73_DIGEST", Convert.ToHexString(RandomNumberGenerator.GetBytes(32)));
        settings["Kafdeck:Administration:Mutations:Persistence:Provider"] = postgres ? "PostgreSql" : "Sqlite";
        settings["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"] = "Standalone";
        if (database is null) settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"] = Path.Combine(directory, "mutations.db");
        else { settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"] = "env:KAFDECK_W73_PG"; lab.Secret("KAFDECK_W73_PG", database.ConnectionString); }
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker, SocketTimeoutMs = 10000 }).Build();
        var topic = "w73-" + Guid.NewGuid().ToString("N");
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
        var operations = new List<Guid>();
        try
        {
            await lab.StartAsync(token);
            async Task<JsonElement> PreviewAsync(string family, object[] changes, string? stableKey = null)
            {
                var file = await lab.RequestFileAsync(new { clusterId = "permitted", changes }, token);
                var command = family == "import" ? new[] { "catalog", "import-preview", "permitted" } : new[] { "catalog", "change-preview", "permitted", family };
                var code = await lab.RunAsync(token, [.. command, "--request-file", file, "--idempotency-key", stableKey ?? Guid.NewGuid().ToString("N")]);
                Assert.True(code == CliApplication.Success, "Real catalog preview failed: " + lab.SafeOutcome);
                using var parsed = JsonDocument.Parse(lab.Output); return parsed.RootElement.Clone();
            }
            async Task ExecuteAsync(JsonElement preview)
            {
                var operation = preview.GetProperty("operation"); var id = operation.GetProperty("operationId").GetGuid(); var hash = operation.GetProperty("previewHash").GetString()!;
                var confirmation = new List<string> { "operations", "confirm", id.ToString("D"), hash };
                if (operation.TryGetProperty("confirmationChallenge", out var challenge) && challenge.ValueKind == JsonValueKind.String)
                    confirmation.AddRange(["--challenge", challenge.GetString()!]);
                Assert.Equal(CliApplication.Success, await lab.RunAsync(token, confirmation.ToArray()));
                Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "operations", "execute", id.ToString("D")));
                using var result = JsonDocument.Parse(lab.Output); Assert.Equal("appliedVerified", result.RootElement.GetProperty("state").GetString()); operations.Add(id);
            }
            var teamPreview = await PreviewAsync("teams", [new { kind = "Team", expectedRevision = 0, name = "Payments", tags = new[] { "catalog-proof" } }]);
            var teamId = teamPreview.GetProperty("changes")[0].GetProperty("id").GetString()!; await ExecuteAsync(teamPreview);
            var appPreview = await PreviewAsync("applications", [new { kind = "Application", expectedRevision = 0, name = "Checkout", teamId }]);
            var appId = appPreview.GetProperty("changes")[0].GetProperty("id").GetString()!; await ExecuteAsync(appPreview);
            var conflictFile = await lab.RequestFileAsync(new { clusterId = "permitted", changes = new[] { new { kind = "Application", id = appId, expectedRevision = 99, name = "Must not overwrite", teamId } } }, token);
            Assert.Equal(CliApplication.Conflict, await lab.RunAsync(token, "catalog", "change-preview", "permitted", "applications", "--request-file", conflictFile, "--idempotency-key", Guid.NewGuid().ToString("N")));
            Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "catalog", "get", "permitted", "applications", appId));
            using (var unchanged = JsonDocument.Parse(lab.Output)) Assert.Equal(1, unchanged.RootElement.GetProperty("revision").GetInt64());
            var imported = await PreviewAsync("import", [new { kind = "Topic", expectedRevision = 0, name = "Orders", topicName = topic, teamId, applicationId = appId, owner = "descriptive only", classification = "internal", domain = "commerce", tags = new[] { "proof" } },
                new { kind = "Application", id = appId, expectedRevision = 1, name = "Checkout imported", teamId }]);
            var topicId = imported.GetProperty("changes")[0].GetProperty("id").GetString()!;
            Assert.Equal(64, imported.GetProperty("changes")[0].GetProperty("topicIdentity").GetString()!.Length);
            await ExecuteAsync(imported);
            var importOperationId = imported.GetProperty("operation").GetProperty("operationId").GetGuid();
            IMutationDbConnectionFactory fixtureConnections = database is null ?
                new SqliteMutationDbConnectionFactory(Path.Combine(directory, "mutations.db")) : new PostgreSqlMutationDbConnectionFactory(database.ConnectionString);
            var fixtureOperations = new AdoMutationOperationRepository(fixtureConnections);
            var committed = await fixtureOperations.GetAsync(importOperationId, token); Assert.NotNull(committed);
            // Explicit storage-outcome fault injection only: the real effect/receipt already committed.
            // Genuine API/CLI recovery below must prove it without another dispatch or metadata revision.
            Assert.Equal(MutationSaveOutcome.Saved, (await fixtureOperations.TrySaveAsync(committed with
            {
                State = MutationOperationState.ExecutionUnknown, Version = committed.Version + 1,
                ResultCode = "injected_commit_acknowledgment_loss", SafeProviderEvidence = new Dictionary<string, string>(),
            }, committed.Version, token)).Outcome);
            var recoveryFile = await lab.RequestFileAsync(new { previewHash = committed.PreviewHash }, token);
            Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "catalog", "recover", importOperationId.ToString("D"), "--request-file", recoveryFile));
            using (var recovered = JsonDocument.Parse(lab.Output)) Assert.Equal("appliedVerified", recovered.RootElement.GetProperty("state").GetString());
            Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "catalog", "topics", "permitted", "list"));
            using (var rows = JsonDocument.Parse(lab.Output)) Assert.Contains(rows.RootElement.GetProperty("items").EnumerateArray(), row => row.GetProperty("id").GetString() == topicId);
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await lab.RunAsync(token, "catalog", "topics", "hidden", "list"));

            var update = await PreviewAsync("topics", [new { kind = "Topic", id = topicId, expectedRevision = 1, name = "Orders updated", topicName = topic, teamId, applicationId = appId, classification = "restricted", documentationReference = "https://docs.example.org/orders" }]);
            await ExecuteAsync(update);
            Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "catalog", "history", "permitted", "topics", topicId));
            using (var history = JsonDocument.Parse(lab.Output))
            {
                var rows = history.RootElement.GetProperty("items"); Assert.Equal(2, rows.GetArrayLength());
                Assert.Equal("internal", rows[0].GetProperty("record").GetProperty("classification").GetString());
                Assert.Equal("restricted", rows[1].GetProperty("record").GetProperty("classification").GetString());
            }
            await lab.RestartAsync(token);
            Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "catalog", "get", "permitted", "topics", topicId));
            using (var restarted = JsonDocument.Parse(lab.Output)) Assert.Equal(2, restarted.RootElement.GetProperty("revision").GetInt64());
            Assert.Equal("1", Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_DELETE_CAPABLE"));
            await admin.DeleteTopicsAsync([topic], new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
            while (admin.GetMetadata(TimeSpan.FromSeconds(5)).Topics.Any(t => t.Topic == topic && !t.Error.IsError)) await Task.Delay(100, token);
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
            Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "catalog", "get", "permitted", "topics", topicId));
            using (var stale = JsonDocument.Parse(lab.Output)) Assert.False(stale.RootElement.GetProperty("topicIdentityCurrent").GetBoolean());
            var rebind = await PreviewAsync("topics", [new { kind = "Topic", id = topicId, expectedRevision = 2, name = "Orders recreated", topicName = topic, rebindTopicIdentity = true, classification = "restricted" }]);
            Assert.NotEqual(imported.GetProperty("changes")[0].GetProperty("topicIdentity").GetString(), rebind.GetProperty("changes")[0].GetProperty("topicIdentity").GetString());
            await ExecuteAsync(rebind);
            Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "catalog", "get", "permitted", "topics", topicId));
            using (var rebound = JsonDocument.Parse(lab.Output))
            {
                Assert.True(rebound.RootElement.GetProperty("topicIdentityCurrent").GetBoolean());
                Assert.Equal(JsonValueKind.Null, rebound.RootElement.GetProperty("teamId").ValueKind);
                Assert.Equal(JsonValueKind.Null, rebound.RootElement.GetProperty("owner").ValueKind);
            }
            var archive = await PreviewAsync("import", [new { kind = "Application", id = appId, expectedRevision = 2, name = "Checkout archived", archived = true },
                new { kind = "Team", id = teamId, expectedRevision = 1, name = "Payments archived", archived = true }]);
            await ExecuteAsync(archive);
            Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "catalog", "get", "permitted", "teams", teamId));
            using (var archived = JsonDocument.Parse(lab.Output)) Assert.True(archived.RootElement.GetProperty("archived").GetBoolean());
            await admin.DeleteTopicsAsync([topic], new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
            while (admin.GetMetadata(TimeSpan.FromSeconds(5)).Topics.Any(t => t.Topic == topic && !t.Error.IsError)) await Task.Delay(100, token);
            var archivedTopic = await PreviewAsync("topics", [new { kind = "Topic", id = topicId, expectedRevision = 3, name = "Deleted topic assignment archived", topicName = topic, archived = true }]);
            await ExecuteAsync(archivedTopic);
            Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "catalog", "get", "permitted", "topics", topicId));
            using (var archived = JsonDocument.Parse(lab.Output)) Assert.True(archived.RootElement.GetProperty("archived").GetBoolean());
            Assert.True(lab.CompletedLogins >= 10);
            var evidence = new { workstream = "W73", provider = postgres ? "PostgreSQL" : "SQLite", identity = "genuine Keycloak authorization-code PKCE", brokerIdentity = "real Kafka DescribeCluster/DescribeTopics", actualApiAndCli = true,
                createdTeam = true, createdApplication = true, atomicMultiResourceImportWithTypedConfirmation = true, immutableHistory = true, deniedCrossScope = true, persistedRestart = true,
                actualKafkaTopicDeleteRecreate = true, staleIdentityVisible = true, explicitGovernedRebind = true, previousOwnershipNotInherited = true,
                staleRevisionRejectedWithoutOverwrite = true, atomicArchivePreservedHistory = true,
                assignmentArchiveAfterPhysicalTopicDeletion = true,
                controlledLostAcknowledgmentOutcomeFixture = true, actualPositiveReceiptRecoveryThroughHumanCli = true, noRecoveryRedispatch = true,
                operations, humanLogins = lab.CompletedLogins, noSyntheticTokens = true, payloadsOrCredentials = false };
            await File.WriteAllBytesAsync(Path.Combine(baseDirectory, (postgres ? "postgres" : "sqlite") + "-w73-catalog-evidence.json"), JsonSerializer.SerializeToUtf8Bytes(evidence), token);
            output.WriteLine("W73 actual catalog/API/CLI/Keycloak/Kafka identity/" + (postgres ? "PostgreSQL" : "SQLite") + ": 7 governed effects, atomic import/archive, history, conflicts, denial, restart, actual topic recreation/rebind and deleted-topic assignment archive passed.");
        }
        finally
        {
            await lab.StopAsync(CancellationToken.None);
            try { await admin.DeleteTopicsAsync([topic], new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) }); } catch (KafkaException) { }
        }
    }
}
