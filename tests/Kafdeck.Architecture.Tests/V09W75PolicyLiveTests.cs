using System.Security.Cryptography;
using System.Text.Json;
using System.Diagnostics;
using Confluent.Kafka;
using Kafdeck.Cli;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W75PolicyLiveTests
{
    [KeycloakG2KafkaFact]
    public Task Genuine_two_humans_SQLite_policy_blocks_stale_and_fresh_effects_and_survives_restart() => ExerciseAsync(false);
    [KeycloakG2KafkaFact]
    public Task Genuine_two_humans_PostgreSQL_policy_blocks_stale_and_fresh_effects_and_survives_restart() => ExerciseAsync(true);

    private static async Task ExerciseAsync(bool postgres)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(15)); var token = timeout.Token;
        var directory = Path.Combine(Path.GetTempPath(), "kafdeck-w75-live-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        await using var database = postgres ? await G2CliPostgreSqlDatabase.CreateAsync() : null;
        await using var lab = new G2HumanCliLaboratory(directory);
        var bootstrap = Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!;
        using var broker = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap, SocketTimeoutMs = 10000 }).Build();
        var suffix = Guid.NewGuid().ToString("N")[..12]; var blocked = "w75-denied-" + suffix; var allowed = "w75-allowed-" + suffix;
        var settings = lab.Settings;
        settings["Kafdeck:Governance:CatalogEnabled"] = "true"; settings["Kafdeck:Governance:PoliciesEnabled"] = "true";
        settings["Kafdeck:Governance:DeploymentGeneration"] = Guid.NewGuid().ToString("D");
        settings["Kafdeck:Administration:Mutations:Enabled"] = "true";
        settings["Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled"] = "false";
        settings["Kafdeck:Administration:Mutations:Persistence:Provider"] = postgres ? "PostgreSql" : "Sqlite";
        settings["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"] = "Standalone";
        var secret = Path.Combine(directory, "digest-private"); await File.WriteAllTextAsync(secret, Convert.ToHexString(RandomNumberGenerator.GetBytes(32)), token);
        settings["Kafdeck:Administration:Mutations:MaterialDigestKey"] = "file:" + secret;
        IMutationDbConnectionFactory connections;
        if (database is null)
        {
            var path = Path.Combine(directory, "mutations.db"); settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"] = path;
            connections = new SqliteMutationDbConnectionFactory(path);
        }
        else
        {
            var path = Path.Combine(directory, "connection-private"); await File.WriteAllTextAsync(path, database.ConnectionString, token);
            settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"] = "file:" + path;
            connections = new PostgreSqlMutationDbConnectionFactory(database.ConnectionString);
        }
        settings["Kafdeck:Clusters:0:Id"] = "permitted"; settings["Kafdeck:Clusters:0:BootstrapServers:0"] = bootstrap;
        settings["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext";
        var realBrowser = !postgres && Environment.GetEnvironmentVariable("KAFDECK_V09_REAL_BROWSER") == "1";
        if (realBrowser)
        {
            settings["Kafdeck:Deployment:Oidc:ClientId"] = lab.Field("browserClientId");
            var dist = Path.Combine(RepositoryRoot(), "src", "frontend", "dist");
            Assert.True(File.Exists(Path.Combine(dist, "index.html")), "Actual compiled frontend is required for the real browser acceptance.");
            foreach (var file in Directory.EnumerateFiles(dist, "*", SearchOption.AllDirectories))
            {
                var target = Path.Combine(directory, "wwwroot", Path.GetRelativePath(dist, file));
                Directory.CreateDirectory(Path.GetDirectoryName(target)!); File.Copy(file, target);
            }
        }
        lab.RegisterHumans(["TopicCreate", "CatalogManage", "GovernancePolicyManage", "GovernancePolicyActivate", "MutationReconcile"],
            ["SystemRead", "ClusterRead", "TopicList", "TopicRead", "CatalogRead", "GovernancePolicyRead"], ["GovernancePolicyActivate"], "permitted");
        try
        {
            await lab.StartAsync(token);
            async Task<JsonElement> Success(params string[] command)
            {
                var code = await lab.RunAsync(token, command); Assert.True(code == CliApplication.Success, "W75 genuine CLI failed: " + lab.SafeOutcome);
                using var document = JsonDocument.Parse(lab.Output); return document.RootElement.Clone();
            }
            async Task<JsonElement> Preview(string command, object body)
            {
                var path = await lab.RequestFileAsync(body, token);
                return await Success("policies", command, "permitted", "--request-file", path, "--idempotency-key", Guid.NewGuid().ToString("N"));
            }
            static Guid Id(JsonElement operation) => operation.GetProperty("operationId").GetGuid();
            static string Hash(JsonElement operation) => operation.GetProperty("previewHash").GetString()!;
            async Task Commit(JsonElement operation, bool critical)
            {
                var id = Id(operation).ToString("D");
                var confirmation = new List<string> { "operations", "confirm", id, Hash(operation) };
                if (operation.TryGetProperty("confirmationChallenge", out var challenge) && challenge.ValueKind == JsonValueKind.String)
                    confirmation.AddRange(["--challenge", challenge.GetString()!]);
                var confirmed = await lab.RunAsync(token, confirmation.ToArray());
                Assert.Equal(critical ? CliApplication.PendingApproval : CliApplication.Success, confirmed);
                if (critical)
                {
                    Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await lab.RunAsync(token, "operations", "approve", id, Hash(operation)));
                    lab.Approver = true;
                    try { Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "operations", "approve", id, Hash(operation))); }
                    finally { lab.Approver = false; }
                }
                Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "operations", "execute", id));
                using var result = JsonDocument.Parse(lab.Output); Assert.Equal("appliedVerified", result.RootElement.GetProperty("state").GetString());
            }

            // This preview predates policy activation and must never dispatch after the authority changes.
            var old = await Success("topics", "create-preview", "permitted", blocked, "1", "1", "--idempotency-key", Guid.NewGuid().ToString("N"));
            await Success("operations", "confirm", Id(old).ToString("D"), Hash(old));
            var policy = JsonSerializer.Serialize(new { schemaVersion = 1, rules = new[] { new { id = "deny-exact-topic", priority = 1,
                scope = new { clusterId = "permitted", resourceKind = "topic", resourceIds = new[] { blocked }, action = "TopicCreate" }, conditions = Array.Empty<object>(), outcome = "Deny" } } });
            var version = await Preview("version-preview", new { clusterId = "permitted", document = policy });
            await Commit(version, false);
            var fixtureOperations = new AdoMutationOperationRepository(connections);
            var versionCommitted = await fixtureOperations.GetAsync(Id(version), token); Assert.NotNull(versionCommitted);
            // Explicit outcome-storage fault fixture: SQL effect/immutable receipt really committed, no process-kill claim.
            Assert.Equal(MutationSaveOutcome.Saved, (await fixtureOperations.TrySaveAsync(versionCommitted with
            {
                State = MutationOperationState.ExecutionUnknown, Version = versionCommitted.Version + 1,
                ResultCode = "injected_commit_acknowledgment_loss", SafeProviderEvidence = new Dictionary<string, string>(),
            }, versionCommitted.Version, token)).Outcome);
            await Success("policies", "reconcile", Id(version).ToString("D"));
            var versionRecovered = await fixtureOperations.GetAsync(Id(version), token);
            Assert.NotNull(versionRecovered); Assert.Equal(MutationOperationState.AppliedVerified, versionRecovered.State);
            var review = await Success("policies", "plan", Id(version).ToString("D")); var versionId = review.GetProperty("versionId").GetString()!;
            var activation = await Preview("activate-preview", new { clusterId = "permitted", versionId, expectedRevision = 0, enabled = true });
            await Commit(activation, true);
            var stored = await new AdoMutationOperationRepository(connections).GetAsync(Id(activation), token);
            Assert.NotNull(stored); Assert.NotNull(stored.ApprovedByPrincipalId);
            Assert.StartsWith("oidc-cli-human:", stored.RequesterPrincipalId, StringComparison.Ordinal);
            Assert.NotEqual(stored.RequesterPrincipalId, stored.ApprovedByPrincipalId);

            var stale = await lab.RunAsync(token, "operations", "execute", Id(old).ToString("D")); Assert.Equal(CliApplication.Conflict, stale);
            var fresh = await Success("topics", "create-preview", "permitted", blocked, "1", "1", "--idempotency-key", Guid.NewGuid().ToString("N"));
            await Success("operations", "confirm", Id(fresh).ToString("D"), Hash(fresh));
            var denied = await lab.RunAsync(token, "operations", "execute", Id(fresh).ToString("D"));
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, denied);
            using (var deniedResult = JsonDocument.Parse(lab.Output))
            {
                Assert.Equal("failedBeforeDispatch", deniedResult.RootElement.GetProperty("state").GetString());
                Assert.Equal("GovernanceDenied", deniedResult.RootElement.GetProperty("resultCode").GetString());
            }
            Assert.DoesNotContain(broker.GetMetadata(TimeSpan.FromSeconds(10)).Topics, t => t.Topic == blocked && !t.Error.IsError);
            var control = await Success("topics", "create-preview", "permitted", allowed, "1", "1", "--idempotency-key", Guid.NewGuid().ToString("N"));
            await Commit(control, false);
            Assert.Contains(broker.GetMetadata(TimeSpan.FromSeconds(10)).Topics, t => t.Topic == allowed && !t.Error.IsError);

            await lab.RestartAsync(token);
            var active = await Success("policies", "state", "permitted"); Assert.True(active.GetProperty("enabled").GetBoolean());
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await lab.RunAsync(token, "policies", "state", "hidden"));
            if (realBrowser)
            {
                // Browser-only governance must not depend on optional CLI/bearer activation flags.
                settings["Kafdeck:NonBrowserRead:Enabled"] = "false";
                await lab.RestartAsync(token);
                await ProveRealBrowserAsync(lab.Api, Path.Combine(Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? directory,
                    "sqlite-w73-w75-real-browser-evidence.json"), token);
                settings["Kafdeck:NonBrowserRead:Enabled"] = "true";
                await lab.RestartAsync(token);
            }
            settings["Kafdeck:Governance:PoliciesEnabled"] = "false"; // Hiding management endpoints cannot drop current execution policy.
            await lab.RestartAsync(token);
            var afterDisable = await Success("topics", "create-preview", "permitted", blocked, "1", "1", "--idempotency-key", Guid.NewGuid().ToString("N"));
            await Success("operations", "confirm", Id(afterDisable).ToString("D"), Hash(afterDisable));
            var disabledAttempt = await lab.RunAsync(token, "operations", "execute", Id(afterDisable).ToString("D"));
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, disabledAttempt);
            Assert.DoesNotContain(broker.GetMetadata(TimeSpan.FromSeconds(10)).Topics, t => t.Topic == blocked && !t.Error.IsError);
            var evidence = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? directory;
            await File.WriteAllBytesAsync(Path.Combine(evidence, (postgres ? "postgres" : "sqlite") + "-w75-policy-evidence.json"), JsonSerializer.SerializeToUtf8Bytes(new {
                realKeycloak = true, actualShippedApiAndCli = true, actualKafka = true, provider = postgres ? "PostgreSQL" : "SQLite",
                criticalDistinctHumanApproval = true, oldPlanFenced = true, freshDeniedPlanNoBrokerEffect = true, allowedControlBrokerEffect = true,
                restartPreservesPolicy = true, scopedReadDenied = true, endpointDisableDoesNotDisableEnforcement = true,
                positiveVersionReceiptRecoveredThroughGenuineCli = true, explicitAckLossStorageFixture = true,
                browserOnlyGovernanceTested = realBrowser, actualPkceLogins = lab.CompletedLogins }), token);
        }
        finally
        {
            await lab.StopAsync(CancellationToken.None);
            try { await broker.DeleteTopicsAsync([allowed]); } catch (Exception ex) when (ex is KafkaException or TimeoutException) { }
        }
    }

    private static string RepositoryRoot()
    {
        var candidate = new DirectoryInfo(AppContext.BaseDirectory);
        while (candidate is not null && !File.Exists(Path.Combine(candidate.FullName, "Kafdeck.slnx"))) candidate = candidate.Parent;
        return candidate?.FullName ?? throw new InvalidOperationException("Repository root is missing.");
    }

    private static async Task ProveRealBrowserAsync(Uri api, string evidence, CancellationToken token)
    {
        var start = new ProcessStartInfo("node") { WorkingDirectory = RepositoryRoot(), UseShellExecute = false,
            CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(Path.Combine(RepositoryRoot(), "src/frontend/scripts/v09-governance-live-browser.cjs"));
        start.ArgumentList.Add(api.AbsoluteUri); start.ArgumentList.Add(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")!); start.ArgumentList.Add(evidence);
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Owned real browser process did not start.");
        var output = process.StandardOutput.ReadToEndAsync(token); var error = process.StandardError.ReadToEndAsync(token);
        try
        {
            await process.WaitForExitAsync(token); var stdout = await output; var stderr = await error;
            var phase = System.Text.RegularExpressions.Regex.Match(stderr, @"phase=([a-z-]{1,96})", System.Text.RegularExpressions.RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
            Assert.True(process.ExitCode == 0 && stdout.Contains("V09_GENUINE_GOVERNANCE_BROWSER_PASSED", StringComparison.Ordinal),
                "Genuine browser acceptance failed; phase=" + (phase.Success ? phase.Groups[1].Value : "withheld"));
            Assert.True(File.Exists(evidence), "Genuine browser sanitized evidence is required.");
        }
        finally { if (!process.HasExited) { process.Kill(entireProcessTree: true); await process.WaitForExitAsync(CancellationToken.None); } }
    }
}
