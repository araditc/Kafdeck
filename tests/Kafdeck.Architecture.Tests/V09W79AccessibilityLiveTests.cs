using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Confluent.Kafka;
using Kafdeck.Cli;
using Kafdeck.Core.Catalog;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

/// <summary>Real application/IdP/DB host; browser and observed native AT are separate evidence producers.</summary>
public sealed class V09W79AccessibilityLiveTests
{
    [KeycloakG2KafkaFact] public Task Genuine_SQLite_English_Persian_keyboard_accessibility_and_durable_unknown() => Exercise(false);
    [KeycloakG2KafkaFact] public Task Genuine_PostgreSQL_English_Persian_keyboard_accessibility_and_durable_unknown() => Exercise(true);

    private static async Task Exercise(bool postgres)
    {
        var manualDirectory = !postgres ? Environment.GetEnvironmentVariable("KAFDECK_W79_MANUAL_DIRECTORY") : null;
        var manual = !string.IsNullOrWhiteSpace(manualDirectory);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(manual ? 45 : 25)); var token = timeout.Token;
        var directory = Path.Combine(Path.GetTempPath(), "kafdeck-w79-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        await using var database = postgres ? await G2CliPostgreSqlDatabase.CreateAsync() : null;
        await using var lab = new G2HumanCliLaboratory(directory); var settings = lab.Settings;
        await using var ownedBroker = Environment.GetEnvironmentVariable("KAFDECK_W79_OWN_BROKER") == "1" ? await W77OwnedKafka.CreateAsync(token) : null;
        var generation = Guid.NewGuid(); var bootstrap = ownedBroker?.Bootstrap ?? Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!;
        var topic = "w79-keyboard-" + Guid.NewGuid().ToString("N")[..12];
        settings["Kafdeck:Deployment:Oidc:ClientId"] = lab.Field("browserClientId");
        foreach (var family in new[] { "Catalog", "Policies", "Requests", "Attribution" }) settings["Kafdeck:Governance:" + family + "Enabled"] = "true";
        settings["Kafdeck:Governance:DeploymentGeneration"] = generation.ToString("D");
        settings["Kafdeck:Administration:Mutations:Enabled"] = "true";
        settings["Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled"] = "false";
        settings["Kafdeck:Administration:Mutations:Persistence:Provider"] = postgres ? "PostgreSql" : "Sqlite";
        settings["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"] = "Standalone";
        var digest = Path.Combine(directory, "digest-private"); await File.WriteAllTextAsync(digest, Convert.ToHexString(RandomNumberGenerator.GetBytes(32)), token);
        settings["Kafdeck:Administration:Mutations:MaterialDigestKey"] = "file:" + digest;
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
        lab.RegisterHumans(["TopicCreate", "CatalogManage", "RequestSubmit", "RequestDecide", "GovernancePolicyManage", "GovernancePolicyActivate", "RateCardManage", "MutationReconcile"],
            ["SystemRead", "ClusterRead", "TopicList", "TopicRead", "CatalogRead", "RequestRead", "GovernancePolicyRead", "AttributionRead"], ["GovernancePolicyActivate"], "permitted");
        var dist = Path.Combine(Root(), "src/frontend/dist"); Assert.True(File.Exists(Path.Combine(dist, "index.html")), "Compiled product UI required.");
        foreach (var file in Directory.EnumerateFiles(dist, "*", SearchOption.AllDirectories))
        {
            var target = Path.Combine(directory, "wwwroot", Path.GetRelativePath(dist, file)); Directory.CreateDirectory(Path.GetDirectoryName(target)!); File.Copy(file, target);
        }
        var output = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? directory; Directory.CreateDirectory(output);
        var provider = postgres ? "postgres" : "sqlite"; var browserEvidence = Path.Combine(output, provider + "-w79-accessibility-evidence.json");
        var handshake = Path.Combine(directory, "handshake"); Directory.CreateDirectory(handshake);
        if (manual) Directory.CreateDirectory(manualDirectory!);
        var operations = new AdoMutationOperationRepository(connections);
        var catalog = new AdoCatalogStore(connections, generation.ToString("D"), new AdoGovernanceGenerationGate(connections, generation));
        await lab.StartAsync(token);
        async Task<JsonElement> Success(params string[] args)
        {
            var exit = await lab.RunAsync(token, args); Assert.True(exit == CliApplication.Success, "W79 genuine setup CLI failed; " + lab.SafeOutcome);
            using var document = JsonDocument.Parse(lab.Output); return document.RootElement.Clone();
        }
        async Task Commit(JsonElement op, bool critical = false)
        {
            var id = op.GetProperty("operationId").GetGuid().ToString("D"); var hash = op.GetProperty("previewHash").GetString()!;
            var args = new List<string> { "operations", "confirm", id, hash };
            if (op.TryGetProperty("confirmationChallenge", out var challenge) && challenge.ValueKind == JsonValueKind.String) args.AddRange(["--challenge", challenge.GetString()!]);
            Assert.Equal(critical ? CliApplication.PendingApproval : CliApplication.Success, await lab.RunAsync(token, args.ToArray()));
            if (critical) { lab.Approver = true; try { await Success("operations", "approve", id, hash); } finally { lab.Approver = false; } }
            await Success("operations", "execute", id);
        }
        // Governing policy is admitted through actual two-human CLI, never seeded as authority.
        var policy = JsonSerializer.Serialize(new { schemaVersion = 1, rules = new[] { new { id = "w79-business-request", priority = 1,
            scope = new { clusterId = "permitted", resourceKind = "topic", resourceIds = new[] { topic }, action = "TopicCreate" }, conditions = Array.Empty<object>(), outcome = "RequireApprovedRequest" } } });
        var versionFile = await lab.RequestFileAsync(new { clusterId = "permitted", document = policy }, token);
        var version = await Success("policies", "version-preview", "permitted", "--request-file", versionFile, "--idempotency-key", Guid.NewGuid().ToString("N")); await Commit(version);
        var material = await Success("policies", "plan", version.GetProperty("operationId").GetGuid().ToString("D"));
        var activationFile = await lab.RequestFileAsync(new { clusterId = "permitted", versionId = material.GetProperty("versionId").GetString(), expectedRevision = 0, enabled = true }, token);
        await Commit(await Success("policies", "activate-preview", "permitted", "--request-file", activationFile, "--idempotency-key", Guid.NewGuid().ToString("N")), true);
        var scenario = Path.Combine(directory, "browser-scenario.json");
        await File.WriteAllBytesAsync(scenario, JsonSerializer.SerializeToUtf8Bytes(new { clusterId = "permitted", topicName = topic, handshakeDirectory = handshake,
            requestExpirySeconds = manual ? 240 : 90, manualHandoffDirectory = manual ? manualDirectory : null }), token);
        if (manual) await File.WriteAllBytesAsync(Path.Combine(manualDirectory!, "host-ready.json"), JsonSerializer.SerializeToUtf8Bytes(new { apiUrl = lab.Api.AbsoluteUri, provider, topicName = topic }), token);
        var start = new ProcessStartInfo("node") { WorkingDirectory = Root(), UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var arg in new[] { Path.Combine(Root(), "src/frontend/scripts/v09-accessibility-live-browser.cjs"), lab.Api.AbsoluteUri,
            Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")!, scenario, browserEvidence }) start.ArgumentList.Add(arg);
        if (manual) start.Environment["KAFDECK_W79_HEADED"] = "1";
        using var process = Process.Start(start) ?? throw new InvalidOperationException("W79 browser failed to start.");
        var stdout = process.StandardOutput.ReadToEndAsync(token); var stderr = process.StandardError.ReadToEndAsync(token);
        var observations = new Dictionary<Guid, string>(); var expired = new HashSet<string>(StringComparer.Ordinal);
        try
        {
            while (!process.HasExited)
            {
                if (manual && File.Exists(Path.Combine(manualDirectory!, "owned-reader-stop-request.json")) &&
                    !File.Exists(Path.Combine(manualDirectory!, "owned-reader-stop-ready.json")))
                {
                    if (!await StopOwnedReader(manualDirectory))
                        throw new InvalidOperationException("W79 owned reader shutdown could not be verified; browser fixture retained.");
                    await Ready(Path.Combine(manualDirectory!, "owned-reader-stop-ready.json"), new { ownedReaderStopped = true }, CancellationToken.None);
                }
                foreach (var locale in new[] { "en", "fa" })
                {
                    var committedFile = Path.Combine(handshake, "catalog-applied-" + locale + ".json");
                    var readyFile = Path.Combine(handshake, "catalog-applied-" + locale + "-ready.json");
                    if (File.Exists(committedFile) && !File.Exists(readyFile))
                    {
                        using var data = await ReadHandshake(committedFile, token); if (data is null) continue;
                        var id = data.RootElement.GetProperty("operationId").GetGuid(); var rowId = data.RootElement.GetProperty("catalogRecordId").GetString()!;
                        var operation = await operations.GetAsync(id, token); Assert.NotNull(operation); Assert.Equal(MutationOperationKind.CatalogMutation, operation.OperationKind);
                        Assert.Equal(MutationOperationState.AppliedVerified, operation.State); Assert.Equal(data.RootElement.GetProperty("previewHash").GetString(), operation.PreviewHash);
                        var receipt = await catalog.GetReceiptAsync(id, token); Assert.NotNull(receipt); Assert.Equal(rowId, Assert.Single(receipt.Records).Id);
                        Assert.Equal(MutationSaveOutcome.Saved, (await operations.TrySaveAsync(operation with { State = MutationOperationState.ExecutionUnknown, Version = operation.Version + 1,
                            ResultCode = "w79_controlled_metadata_acknowledgement_loss", SafeProviderEvidence = new Dictionary<string, string>() }, operation.Version, token)).Outcome);
                        observations.Add(id, rowId); await Ready(readyFile, new { operationId = id }, token);
                    }
                    var expiryFile = Path.Combine(handshake, "request-submitted-" + locale + ".json");
                    var expiryReady = Path.Combine(handshake, "request-submitted-" + locale + "-ready.json");
                    if (File.Exists(expiryFile) && !File.Exists(expiryReady))
                    {
                        using var data = await ReadHandshake(expiryFile, token); if (data is null) continue;
                        var end = data.RootElement.GetProperty("expiresAtUtc").GetDateTimeOffset();
                        if (DateTimeOffset.UtcNow >= end)
                        {
                            var id = data.RootElement.GetProperty("requestId").GetString()!; expired.Add(locale);
                            await Ready(expiryReady, new { requestId = id }, token);
                        }
                    }
                }
                await Task.Delay(100, token);
            }
            _ = await stdout; var error = await stderr;
            var phase = System.Text.RegularExpressions.Regex.Match(error, @"phase=([a-z0-9-]{1,96})", System.Text.RegularExpressions.RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
            Assert.True(process.ExitCode == 0, "W79 actual browser failed; phase=" + (phase.Success ? phase.Groups[1].Value : "withheld"));
            Assert.Equal(2, observations.Count); Assert.Equal(2, expired.Count);
            foreach (var observation in observations)
            {
                Assert.Equal(MutationOperationState.AppliedVerified, (await operations.GetAsync(observation.Key, token))!.State);
                Assert.Equal(observation.Key, Assert.Single(await catalog.HistoryAsync("permitted", CatalogEntityKind.Team, observation.Value, 0, 10, token)).OperationId);
            }
            using var evidence = JsonDocument.Parse(await File.ReadAllTextAsync(browserEvidence, token));
            Assert.Equal(2, evidence.RootElement.GetProperty("localeJourneys").GetArrayLength());
            using var broker = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap, SocketTimeoutMs = 10000 }).Build();
            Assert.DoesNotContain(broker.GetMetadata(TimeSpan.FromSeconds(10)).Topics, t => t.Topic == topic && t.Error.Code == ErrorCode.NoError);
            await File.WriteAllBytesAsync(Path.Combine(output, provider + "-w79-host-evidence.json"), JsonSerializer.SerializeToUtf8Bytes(new {
                sourceSha = Environment.GetEnvironmentVariable("KAFDECK_SOURCE_SHA"), provider = postgres ? "PostgreSQL" : "SQLite", genuineKeycloak = true, genuineCompiledUiAndApi = true, actualKafkaMetadata = true,
                policyAdmittedThroughRealTwoHumanCli = true, fixturePolicySeeding = false, explicitCatalogCommitAckLossFixture = true,
                positiveCatalogReceipts = observations.Count, recoveredWithoutDuplicateHistory = true, realExpiredRequestLocales = expired.Count,
                deniedLinkedEffectAbsentFromBroker = true, manualAtEvidenceSeparate = true, completedPkceLogins = lab.CompletedLogins }), token);
            ownedBroker?.MarkSuccessful();
        }
        finally
        {
            // Stop only the observer-declared, still-identical test reader before
            // closing its Chrome fixture, including cancellation/failure exits.
            if (!await StopOwnedReader(manualDirectory))
                throw new InvalidOperationException("W79 owned reader shutdown could not be verified; browser fixture retained.");
            if (!process.HasExited) { process.Kill(true); await process.WaitForExitAsync(CancellationToken.None); }
        }
    }

    private static async Task<bool> StopOwnedReader(string? manualDirectory)
    {
        if (string.IsNullOrWhiteSpace(manualDirectory)) return true;
        var ownerFile = Path.Combine(manualDirectory, "owned-reader.json");
        if (!File.Exists(ownerFile)) return true;
        try
        {
            using var owner = JsonDocument.Parse(await File.ReadAllBytesAsync(ownerFile, CancellationToken.None));
            var pid = owner.RootElement.GetProperty("pid").GetInt32();
            var recordedPath = owner.RootElement.GetProperty("exePath").GetString();
            var recordedStart = owner.RootElement.GetProperty("processStartUtc").GetDateTimeOffset();
            if (pid <= 0 || string.IsNullOrWhiteSpace(recordedPath) || !Path.IsPathFullyQualified(recordedPath)) return false;
            var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
            var expectedPath = Path.GetFullPath(recordedPath);
            var allowedRoot = Path.GetFullPath(Path.Combine(Root(), "deploy/dev/.generated/w79-nvda")) + Path.DirectorySeparatorChar;
            if (!expectedPath.StartsWith(allowedRoot, comparison)) return false;
            Process process;
            try { process = Process.GetProcessById(pid); } catch (ArgumentException) { return true; }
            using var reader = process;
            if (reader.HasExited) return true;
            if (reader.MainModule?.FileName is not { } actualPath ||
                !string.Equals(Path.GetFullPath(actualPath), expectedPath, comparison) ||
                Math.Abs((new DateTimeOffset(reader.StartTime.ToUniversalTime()) - recordedStart).TotalSeconds) > 1) return false;
            reader.Kill();
            await reader.WaitForExitAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
            return true;
        }
        catch (Exception error) when (error is ArgumentException or InvalidOperationException or System.ComponentModel.Win32Exception or IOException or JsonException or KeyNotFoundException or FormatException or UnauthorizedAccessException or TimeoutException)
        { return false; /* Unverifiable ownership never authorizes a kill or a safe-close acknowledgment. */ }
    }

    private static async Task<JsonDocument?> ReadHandshake(string path, CancellationToken token)
    { try { return JsonDocument.Parse(await File.ReadAllBytesAsync(path, token)); } catch (IOException) { return null; } catch (JsonException) { return null; } }
    private static async Task Ready(string path, object value, CancellationToken token)
    { var temporary = path + ".tmp"; await File.WriteAllBytesAsync(temporary, JsonSerializer.SerializeToUtf8Bytes(value), token); File.Move(temporary, path); }
    private static string Root() { var d = new DirectoryInfo(AppContext.BaseDirectory); while (!File.Exists(Path.Combine(d.FullName, "Kafdeck.slnx"))) d = d.Parent!; return d.FullName; }
}
