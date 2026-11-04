using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Cli;
using Kafdeck.Api;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class G3DeclarativeLiveTheoryAttribute : TheoryAttribute
{
    public G3DeclarativeLiveTheoryAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")))
            Skip = "Requires genuine HTTPS Keycloak with two humans; no fabricated access tokens.";
    }
}

public sealed class V08G3DeclarativeCliLiveTests
{
    [KeycloakG2Fact]
    public async Task Genuine_private_key_jwt_service_controller_and_distinct_human_apply_exact_PG_plan()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")))
            throw new InvalidOperationException("Enabled service controller acceptance requires actual PostgreSQL.");
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(5)); var token = deadline.Token;
        var evidence = Environment.GetEnvironmentVariable("KAFDECK_G3_EVIDENCE_DIRECTORY") ??
            Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.GetTempPath();
        Directory.CreateDirectory(evidence);
        await using var fixture = await Fixture.CreateAsync(evidence, true, token);
        fixture.EnableService();
        var artifact = await fixture.CommitAsync("g3-service-owned", "Active", token, yaml: true);
        await fixture.Lab.StartAsync(token);
        var plan = await fixture.PlanAsync(artifact, 0, token);
        Assert.Equal(0, fixture.Lab.CompletedLogins);
        await fixture.ApproveAsync(plan, token);
        var humanLogins = fixture.Lab.CompletedLogins; Assert.True(humanLogins > 0);
        Assert.Equal(CliApplication.Success, await fixture.ApplyAsync(plan, token));
        Assert.Equal(humanLogins, fixture.Lab.CompletedLogins);
        var operation = await fixture.Operations.GetAsync(plan.Id, token); Assert.NotNull(operation);
        Assert.Equal(MutationOperationState.AppliedVerified, operation.State);
        Assert.True(operation.RequesterPrincipalId == fixture.Principal, "The durable requester must be the exact registered verified service actor.");
        Assert.True(RegisteredBearerPrincipal.TryParse(operation.RequesterPrincipalId, out var actor));
        Assert.Equal(NonBrowserPrincipalKind.Service, actor!.Kind);
        Assert.True(NonBrowserHumanPrincipalId.TryParse(operation.ApprovedByPrincipalId!, out _));
        Assert.NotEqual(operation.RequesterPrincipalId, operation.ApprovedByPrincipalId);
        Assert.Equal(1, (await fixture.Routing.GetSubscriptionAsync(artifact.Resource, token))!.Revision);
        Assert.NotNull(await fixture.Store.GetCommitReceiptAsync(plan.Id, token));
        Assert.Equal(1, await fixture.CountAsync("kafdeck_declarative_ownership_audit", token));
        Assert.True(fixture.Lab.ServiceApiCommands >= 3);
        await File.WriteAllBytesAsync(Path.Combine(evidence, "postgres-g3-service-controller-evidence.json"),
            JsonSerializer.SerializeToUtf8Bytes(new { realKeycloakPrivateKeyJwt = true, shippedApi = true, shippedCli = true,
                persistence = "PostgreSQL", exactRegisteredServiceRequester = true, sourceControllerBoundToService = true,
                criticalAwaitingIndependentApproval = true, distinctRealHumanApproval = true,
                serviceCommandBrowserCallbacks = 0, independentlyApprovedCommits = 1, durableOwnershipAudit = true,
                serviceApiCommands = fixture.Lab.ServiceApiCommands, actualHumanPkceLogins = humanLogins,
                noSyntheticAccessToken = true }), token);
    }

    [G3DeclarativeLiveTheory]
    [InlineData(false)] [InlineData(true)]
    public async Task Committed_Git_manifest_real_CLI_two_humans_and_durable_backend_preserve_governance(bool postgres)
    {
        if (postgres && string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")))
            throw new InvalidOperationException("Enabled G3 acceptance requires actual PostgreSQL.");
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(18)); var token = timeout.Token;
        var evidence = Environment.GetEnvironmentVariable("KAFDECK_G3_EVIDENCE_DIRECTORY") ??
            Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.GetTempPath();
        Directory.CreateDirectory(evidence);
        await using var fixture = await Fixture.CreateAsync(evidence, postgres, token);
        var lab = fixture.Lab; var routing = fixture.Routing; var store = fixture.Store;
        var create = await fixture.CommitAsync("g3-create", "Active", token);
        var import = await fixture.CommitAsync("g3-import", "Paused", token, yaml: true);
        await routing.CreateSubscriptionAsync(import.Manifest.Definition, import.Manifest.State, DateTimeOffset.UtcNow, token);
        await lab.StartAsync(token);

        var beforeLogin = lab.CompletedLogins;
        Assert.Equal(CliApplication.Success, await fixture.CommandAsync(create, "validate", 0, token));
        Assert.Equal(beforeLogin, lab.CompletedLogins); // Air-gap grammar path does not load OIDC credentials.
        Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "declarative", "capabilities"));
        using (var capabilities = JsonDocument.Parse(lab.Output)) Assert.True(capabilities.RootElement.GetProperty("enabled").GetBoolean());
        Assert.Equal(CliApplication.Conflict, await fixture.CommandAsync(create, "observe", 0, token));
        Assert.Equal("Missing", fixture.OutputField("drift"));
        Assert.Equal(CliApplication.PendingApproval, await fixture.CommandAsync(create, "reconcile", 0, token, yes: true));
        var created = fixture.Preview();
        Assert.Null(await routing.GetSubscriptionAsync("g3-create", token));
        await fixture.ApproveAsync(created, token, alreadyConfirmed: true);
        Assert.Equal(CliApplication.Conflict, await lab.RunAsync(token, "declarative", "apply", created.Id.ToString("D"), new string('f', 64)));
        Assert.Equal(CliApplication.Success, await fixture.ApplyAsync(created, token));
        Assert.Equal(NotificationSubscriptionState.Active, (await routing.GetSubscriptionAsync("g3-create", token))!.State);
        var firstReceipt = await store.GetCommitReceiptAsync(created.Id, token); Assert.NotNull(firstReceipt);
        Assert.Equal(1, firstReceipt.OwnershipRevision); Assert.Equal(1, firstReceipt.SubscriptionRevision);
        Assert.Equal(CliApplication.Success, await fixture.CommandAsync(create, "observe", 1, token));
        Assert.Equal("InSync", fixture.OutputField("drift"));
        var beforeNoop = await fixture.CountAsync("kafdeck_declarative_runtime_audit", token);
        Assert.Equal(CliApplication.Success, await fixture.CommandAsync(create, "reconcile", 0, token, yes: true));
        Assert.Equal("InSync", fixture.OutputField("drift")); Assert.Equal("Managed", fixture.OutputField("ownership"));
        Assert.Equal(beforeNoop, await fixture.CountAsync("kafdeck_declarative_runtime_audit", token));

        await lab.StopAsync(token);
        await fixture.MigrateOperatorAsync(token);
        await lab.StartAsync(token);
        Assert.Equal(CliApplication.Success, await fixture.ApplyAsync(created, token));
        Assert.Equal(firstReceipt, await new W68RuntimeStore(fixture.Connections, fixture.Generation).GetCommitReceiptAsync(created.Id, token));
        Assert.Equal(1, (await routing.GetSubscriptionAsync("g3-create", token))!.Revision);

        var changed = await fixture.CommitAsync("g3-create", "Paused", token, yaml: true);
        var originalCopy = await fixture.FrozenCopyAsync(create, token);
        var originalSource = await fixture.SourceAsync(create, token);
        Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await fixture.AdvanceAsync(changed, originalSource, token));
        lab.Settings[fixture.RegistrationPrefix("g3-create") + "AllowRevisionAdvance"] = "true";
        var sourceManagerSettings = new Dictionary<string, string>(lab.Settings);
        fixture.RemoveRequesterAction("DeclarativeApply"); fixture.RemoveRequesterAction("NotificationManage");
        await lab.RestartAsync(token);
        Assert.Equal(CliApplication.Success, await fixture.AdvanceAsync(changed, originalSource, token));
        var advancedB = fixture.SourceReceipt();
        Assert.Equal(changed.Manifest.Fingerprint(), advancedB.Hash);
        Assert.Equal(CliApplication.Conflict, await fixture.AdvanceAsync(changed, originalSource, token));
        Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await fixture.AdvanceAsync(changed, advancedB, token, source: "unregistered-source"));
        using (var foreignOwner = JsonDocument.Parse(Encoding.UTF8.GetString(changed.Manifest.ToCanonicalUtf8())
                   .Replace("\"ownerRef\":\"team-ops\"", "\"ownerRef\":\"foreign-owner\"", StringComparison.Ordinal)))
        {
            var foreignFile = await lab.RequestFileAsync(foreignOwner.RootElement, token);
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await fixture.AdvanceAsync(changed with { File = foreignFile }, advancedB, token));
        }
        Assert.Equal(CliApplication.Success, await fixture.AdvanceAsync(originalCopy, advancedB, token));
        var advancedA = fixture.SourceReceipt(); Assert.Equal(create.Manifest.Fingerprint(), advancedA.Hash);
        Assert.Equal(CliApplication.Conflict, await fixture.AdvanceAsync(changed, originalSource, token)); // Same hash A; obsolete revision A1 cannot authorize A3.
        Assert.Equal(CliApplication.Conflict, await fixture.AdvanceAsync(originalCopy, advancedB, token));
        Assert.Equal(CliApplication.Success, await fixture.AdvanceAsync(changed, advancedA, token));
        fixture.RestoreSettings(sourceManagerSettings);
        Assert.Equal(create.Manifest.Fingerprint(), lab.Settings[fixture.RegistrationPrefix("g3-create") + "SourceRevision"]);
        await lab.RestartAsync(token);
        Assert.Equal(CliApplication.Conflict, await fixture.CommandAsync(changed, "drift", 1, token));
        Assert.Equal("OutOfSync", fixture.OutputField("drift"));
        var update = await fixture.PlanAsync(changed, 1, token);
        await fixture.ApproveAsync(update, token); Assert.Equal(CliApplication.Success, await fixture.ApplyAsync(update, token));
        var updated = await routing.GetSubscriptionAsync("g3-create", token); Assert.NotNull(updated);
        Assert.Equal(NotificationSubscriptionState.Paused, updated.State); Assert.Equal(2, updated.Revision);
        Assert.Equal(2, (await store.GetOwnershipAsync(fixture.Key("g3-create"), token))!.Revision);

        Assert.Equal(CliApplication.Success, await fixture.CommandAsync(import, "observe", 0, token));
        Assert.Equal("ObservedOnly", fixture.OutputField("ownership"));
        var adopted = await fixture.PlanAsync(import, 0, token, "import-preview");
        await fixture.ApproveAsync(adopted, token); Assert.Equal(CliApplication.Success, await fixture.ApplyAsync(adopted, token));
        Assert.Equal(1, (await routing.GetSubscriptionAsync("g3-import", token))!.Revision);
        Assert.Equal(1, (await store.GetOwnershipAsync(fixture.Key("g3-import"), token))!.Revision);

        // Full current authority and exact source bindings, not editable owner labels.
        Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,
            await fixture.CommandAsync(changed, "observe", 2, token, source: "unregistered-source"));
        var forged = await fixture.CommitAsync("g3-forged", "Active", token, owner: "foreign-owner", register: false);
        Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await fixture.CommandAsync(forged, "plan", 0, token));
        Assert.Null(await routing.GetSubscriptionAsync("g3-forged", token));
        using (var altered = JsonDocument.Parse(Encoding.UTF8.GetString(changed.Manifest.ToCanonicalUtf8())
                   .Replace("\"state\":\"Paused\"", "\"state\":\"Active\"", StringComparison.Ordinal)))
        {
            var alteredFile = await lab.RequestFileAsync(altered.RootElement, token);
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await lab.RunAsync(token,
                "declarative", "observe", "--manifest-file", alteredFile, "--source", changed.SourceRef,
                "--source-revision", changed.Manifest.Fingerprint(), "--ownership-revision", "2"));
            Assert.Equal(2, (await routing.GetSubscriptionAsync("g3-create", token))!.Revision);
            Assert.Equal(3, await fixture.CountAsync("kafdeck_declarative_ownership_audit", token));
        }

        if (postgres)
        {
            var ha = await fixture.CommitAsync("g3-two-nodes", "Active", token);
            await fixture.ProveTwoNodesAsync(ha, token);
        }

        var rights = await fixture.CommitAsync("g3-rights", "Active", token);
        var fullSettings = new Dictionary<string, string>(lab.Settings);
        fixture.RemoveRequesterAction("DeclarativeApply"); fixture.RemoveRequesterAction("NotificationManage");
        await lab.RestartAsync(token);
        Assert.Equal(CliApplication.Conflict, await fixture.CommandAsync(rights, "observe", 0, token));
        var planOnly = await fixture.PlanAsync(rights, 0, token);
        Assert.Equal(CliApplication.Success, await lab.RunAsync(token, "declarative", "show", planOnly.Id.ToString("D")));
        using (var shown = JsonDocument.Parse(lab.Output)) Assert.Equal(planOnly.Id, shown.RootElement.GetProperty("planId").GetGuid());
        Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await fixture.ApplyAsync(planOnly, token));
        Assert.Null(await routing.GetSubscriptionAsync("g3-rights", token));
        fixture.RestoreSettings(fullSettings); fixture.RemoveRequesterAction("NotificationRead");
        await lab.RestartAsync(token);
        Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await fixture.CommandAsync(changed, "observe", 2, token));
        Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await fixture.CommandAsync(changed, "plan", 2, token));
        fixture.RestoreSettings(fullSettings);
        fixture.RemoveRequesterAction("DeclarativePlan");
        await lab.RestartAsync(token);
        Assert.Equal(CliApplication.Success, await fixture.CommandAsync(changed, "observe", 2, token));
        Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await fixture.CommandAsync(changed, "plan", 2, token));
        fixture.RestoreSettings(fullSettings);

        var sourceFence = await fixture.CommitAsync("g3-source-fence", "Active", token);
        lab.Settings[fixture.RegistrationPrefix(sourceFence.Resource) + "AllowRevisionAdvance"] = "true";
        await lab.RestartAsync(token);
        var fencedPlan = await fixture.PlanAsync(sourceFence, 0, token); await fixture.ApproveAsync(fencedPlan, token);
        var beforeSourceAdvance = await fixture.SourceAsync(sourceFence, token);
        var sourceFenceChanged = await fixture.CommitAsync("g3-source-fence", "Paused", token);
        Assert.Equal(CliApplication.Success, await fixture.AdvanceAsync(sourceFenceChanged, beforeSourceAdvance, token));
        var sourceFenceExit = await fixture.ApplyAsync(fencedPlan, token);
        Assert.True(sourceFenceExit is CliApplication.Conflict or CliApplication.UnknownEffect,
            "A source revision change must fence its older immutable plan and preserve explicit stale/unknown truth.");
        Assert.Null(await store.GetCommitReceiptAsync(fencedPlan.Id, token));
        Assert.Null(await routing.GetSubscriptionAsync(sourceFence.Resource, token));

        var policyArtifact = await fixture.CommitAsync("g3-policy", "Active", token);
        await lab.RestartAsync(token);
        var policyPlan = await fixture.PlanAsync(policyArtifact, 0, token); await fixture.ApproveAsync(policyPlan, token);
        var policySettings = new Dictionary<string, string>(lab.Settings);
        lab.Settings["Kafdeck:Authorization:Roles:0:Permissions:99:Action"] = "TopicRead";
        await lab.RestartAsync(token);
        Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await fixture.ApplyAsync(policyPlan, token));
        Assert.Null(await store.GetCommitReceiptAsync(policyPlan.Id, token));
        fixture.RestoreSettings(policySettings);

        var revokedArtifact = await fixture.CommitAsync("g3-revoked", "Active", token);
        await lab.RestartAsync(token);
        var revoked = await fixture.PlanAsync(revokedArtifact, 0, token); await fixture.ApproveAsync(revoked, token);
        lab.Settings[fixture.RegistrationPrefix("g3-revoked") + "Enabled"] = "false";
        await lab.RestartAsync(token);
        Assert.Equal(CliApplication.Conflict, await fixture.ApplyAsync(revoked, token));
        Assert.Null(await store.GetCommitReceiptAsync(revoked.Id, token));
        Assert.Null(await routing.GetSubscriptionAsync("g3-revoked", token));

        var driftArtifact = await fixture.CommitAsync("g3-external", "Active", token);
        await lab.RestartAsync(token);
        var driftPlan = await fixture.PlanAsync(driftArtifact, 0, token); await fixture.ApproveAsync(driftPlan, token);
        await routing.CreateSubscriptionAsync(driftArtifact.Manifest.Definition, NotificationSubscriptionState.Paused, DateTimeOffset.UtcNow, token);
        var interferenceExit = await fixture.ApplyAsync(driftPlan, token);
        Assert.True(interferenceExit is CliApplication.Conflict or CliApplication.UnknownEffect,
            "A concurrent writer must retain explicit stale/unknown truth, never success or blind replay.");
        Assert.Null(await store.GetCommitReceiptAsync(driftPlan.Id, token));
        if (interferenceExit == CliApplication.UnknownEffect)
            Assert.Equal(CliApplication.Conflict, await lab.RunAsync(token, "declarative", "recover", driftPlan.Id.ToString("D"), driftPlan.Hash));
        Assert.Equal(NotificationSubscriptionState.Paused, (await routing.GetSubscriptionAsync("g3-external", token))!.State);
        Assert.Equal(1, (await routing.GetSubscriptionAsync("g3-external", token))!.Revision);

        var expiryArtifact = await fixture.CommitAsync("g3-expiry", "Active", token);
        await lab.RestartAsync(token);
        var expiring = await fixture.PlanAsync(expiryArtifact, 0, token); await fixture.ApproveAsync(expiring, token);
        var stored = await store.GetPlanAsync(expiring.Id, token); Assert.NotNull(stored);
        while (await store.ReadDatabaseUtcNowAsync(token) <= stored.ExpiresAtUtc) await Task.Delay(500, token);
        Assert.Equal(CliApplication.Conflict, await fixture.ApplyAsync(expiring, token));
        Assert.Null(await store.GetCommitReceiptAsync(expiring.Id, token));
        Assert.Null(await routing.GetSubscriptionAsync("g3-expiry", token));

        var commits = await fixture.CountAsync("kafdeck_declarative_commit_receipts", token);
        Assert.Equal(postgres ? 4 : 3, commits);
        Assert.Equal(commits, await fixture.CountAsync("kafdeck_declarative_ownership_audit", token));
        Assert.True(await fixture.CountAsync("kafdeck_declarative_runtime_audit", token) >= 9);
        foreach (var id in new[] { created.Id, update.Id, adopted.Id })
        {
            var operation = await fixture.Operations.GetAsync(id, token); Assert.NotNull(operation);
            Assert.Equal(MutationOperationState.AppliedVerified, operation.State);
            Assert.NotEqual(operation.RequesterPrincipalId, operation.ApprovedByPrincipalId);
        }
        await File.WriteAllBytesAsync(Path.Combine(evidence, (postgres ? "postgres" : "sqlite") + "-g3-declarative-cli-evidence.json"),
            JsonSerializer.SerializeToUtf8Bytes(new { actualKeycloak = true, shippedApi = true, shippedCli = true,
                persistence = postgres ? "PostgreSQL" : "SQLite", actualGitRepository = true, committedArtifacts = fixture.GitCommits,
                jsonYamlCanonicalParity = true, airGapValidation = true, externalGenerationExplicitlyEnrolled = true,
                finiteReconcileStoppedForIndependentApproval = true, ownedInSyncReconcileIsNoop = true,
                independentlyApprovedCommits = commits, importPreservedSubscriptionRevision = true, updatePreservedCas = true,
                processRestartAndOldPlanReceiptReplay = true, operatorMigrationWithoutHttpBinding = true,
                actualTwoApiProcessesPgContendedOnePlanAndSurvivorAppliedOnce = postgres,
                immutableFingerprintMismatchDenied = true, wrongRequesterDenied = true, changedBodyCannotBorrowApprovedSourcePin = true,
                forgedOwnerAndSourceDenied = true, readAndPlanWithoutApplyProven = true, missingUnderlyingReadDenied = true,
                authenticatedSourceCasWithoutApplyAtoBtoA = true, configuredBaselineNotReloadedPerCommit = true,
                defaultFixedPinAdvanceDenied = true, staleSourceCasAndReplayDenied = true, abaSourceRevisionReuseDenied = true, oldSourcePlanExit = sourceFenceExit,
                changedPolicyDenied = true, revokedSourceDenied = true, readWithoutDeclarativePlanDenied = true,
                concurrentWriterExit = interferenceExit, absentReceiptRecoveryNeverReplays = interferenceExit == CliApplication.UnknownEffect,
                actualDbTimeExpiryDenied = true,
                completedPkceLogins = lab.CompletedLogins,
                limitation = "Physical backup restore, HA fencing and crash-during-commit are separate storage acceptance scenarios; no provider transport is invoked by subscription metadata changes." }), token);
    }

    internal sealed record Artifact(string Resource, string File, DeclarativeSubscriptionManifest Manifest, string SourceRef);
    internal sealed record Preview(Guid Id, string Hash, string? Challenge);
    internal sealed record SourceProof(Guid Generation, long Revision, string Hash);
    internal sealed class Fixture : IAsyncDisposable
    {
        internal G2HumanCliLaboratory Lab { get; }
        internal INotificationDeliveryDbConnectionFactory Connections { get; }
        internal AdoNotificationRoutingStore Routing { get; }
        internal W68RuntimeStore Store { get; }
        internal AdoMutationOperationRepository Operations { get; }
        internal Guid Generation { get; } = Guid.NewGuid();
        internal int GitCommits { get; private set; }
        private readonly G2CliPostgreSqlDatabase? _database;
        private readonly string _directory, _repo, _digest;
        private string _principal;
        internal string Principal => _principal;
        private readonly Dictionary<string, int> _registrations = new(StringComparer.Ordinal);
        private Fixture(string directory, G2CliPostgreSqlDatabase? database)
        {
            _directory = directory; _repo = Path.Combine(directory, "committed-source"); _database = database;
            Lab = new(Path.Combine(directory, "api"));
            _principal = "oidc-cli-human:" + string.Join('|', new[] { Lab.Field("issuer"), Lab.Field("humanSubject"), Lab.Field("cliClientId"), "family-requester" }.Select(Uri.EscapeDataString));
            _digest = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
            Connections = database is null ? new SqliteNotificationDeliveryDbConnectionFactory(Path.Combine(directory, "notifications.db"))
                : new PostgreSqlNotificationDeliveryDbConnectionFactory(database.ConnectionString);
            Routing = new(Connections); Store = new(Connections, Generation);
            Operations = new(database is null ? new SqliteMutationDbConnectionFactory(Path.Combine(directory, "mutations.db")) : new PostgreSqlMutationDbConnectionFactory(database.ConnectionString));
        }
        internal static async Task<Fixture> CreateAsync(string evidence, bool postgres, CancellationToken token)
        {
            var directory = Path.Combine(evidence, "g3-cli-private-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
            var database = postgres ? await G2CliPostgreSqlDatabase.CreateAsync() : null;
            var f = new Fixture(directory, database);
            try
            {
                Directory.CreateDirectory(f._repo); await f.GitAsync(token, "init", "--quiet");
                await f.GitAsync(token, "config", "user.email", "g3-fixture@example.invalid");
                await f.GitAsync(token, "config", "user.name", "G3 acceptance fixture");
                await f.Routing.InitializeAsync(token); await W68RuntimeSchema.InstallOrUpgradeAsync(f.Connections, token);
                var anchorFile = Path.GetFullPath(Path.Combine(evidence, "g3-external-anchor-" + Guid.NewGuid().ToString("N") + ".json"));
                var anchor = JsonSerializer.SerializeToUtf8Bytes(new { version = 1, deploymentId = "g3-deployment", generation = f.Generation, state = "active" });
                await File.WriteAllBytesAsync(anchorFile, anchor, token);
                // This fresh private schema has never hosted a writer. No previous
                // generation is silently adopted, restored or overwritten.
                var freshFence = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes("Fresh isolated test database with no prior writers: " + directory))).ToLowerInvariant();
                await W68RuntimeSchema.AdmitGenerationAsync(f.Connections, "g3-deployment", null, f.Generation, freshFence, token);
                var actions = new[] { "DeclarativePlan", "DeclarativeAdopt", "DeclarativeApply", "NotificationManage", "DeclarativeSourceManage" };
                var reads = new[] { "SystemRead", "DeclarativeRead", "NotificationRead" };
                // ApprovalActions names effects; exact read conjunctions remain
                // in ReadActions and are revalidated by the approval authorizer.
                f.Lab.RegisterHumans(actions, reads, actions.Where(action => action != "DeclarativeSourceManage").ToArray(), resource: "*");
                var settings = f.Lab.Settings;
                settings["Kafdeck:Declarative:Enabled"] = "true"; settings["Kafdeck:Declarative:DeploymentId"] = "g3-deployment";
                settings["Kafdeck:Declarative:DeploymentGeneration"] = f.Generation.ToString("D");
                settings["Kafdeck:Declarative:RecoveryAnchorFile"] = anchorFile;
                settings["Kafdeck:Declarative:RecoveryAnchorSha256"] = Convert.ToHexString(SHA256.HashData(anchor)).ToLowerInvariant();
                settings["Kafdeck:Administration:Mutations:Enabled"] = "true"; settings["Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled"] = "false";
                var digestFile = Path.Combine(directory, "digest-private"); await File.WriteAllTextAsync(digestFile, f._digest, token);
                settings["Kafdeck:Administration:Mutations:MaterialDigestKey"] = "file:" + digestFile;
                settings["Kafdeck:Administration:Mutations:Persistence:Provider"] = postgres ? "PostgreSql" : "Sqlite";
                settings["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"] = "Standalone";
                settings["Kafdeck:Notifications:Enabled"] = "true"; settings["Kafdeck:Notifications:ManagementEnabled"] = "true";
                settings["Kafdeck:Notifications:Provider"] = postgres ? "PostgreSql" : "Sqlite"; settings["Kafdeck:Notifications:ExecutionMode"] = "Standalone";
                settings["Kafdeck:Notifications:WebhookDeliveryEnabled"] = "false"; settings["Kafdeck:Notifications:HttpProviderDeliveryEnabled"] = "false";
                settings["Kafdeck:Notifications:EmailDeliveryEnabled"] = "false"; settings["Kafdeck:Notifications:ProductEventsEnabled"] = "false";
                settings["Kafdeck:Notifications:Destinations:0:DestinationId"] = "ops-notifier";
                settings["Kafdeck:Notifications:Destinations:0:Provider"] = "Webhook";
                settings["Kafdeck:Notifications:Destinations:0:DisplayName"] = "G3 metadata fixture";
                settings["Kafdeck:Notifications:Destinations:0:EnabledEvents:0"] = "Operational";
                settings["Kafdeck:Notifications:Destinations:0:ConfiguredEndpoint"] = "https://provider.example.invalid/notify";
                settings["Kafdeck:Notifications:Destinations:0:WebhookAuthentication"] = "Anonymous";
                if (database is null)
                {
                    settings["Kafdeck:Notifications:SqliteDatabasePath"] = Path.Combine(directory, "notifications.db");
                    settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"] = Path.Combine(directory, "mutations.db");
                }
                else
                {
                    var file = Path.Combine(directory, "connection-private"); await File.WriteAllTextAsync(file, database.ConnectionString, token);
                    settings["Kafdeck:Notifications:ConnectionString"] = "file:" + file;
                    settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"] = "file:" + file;
                }
                return f;
            }
            catch { await f.DisposeAsync(); throw; }
        }
        internal async Task<Artifact> CommitAsync(string resource, string state, CancellationToken token, bool yaml = false,
            string owner = "team-ops", bool register = true)
        {
            var json = JsonSerializer.Serialize(new { apiVersion = DeclarativeSubscriptionManifest.ApiVersion, kind = "NotificationSubscription",
                metadata = new { deploymentRef = "g3-deployment", name = resource, ownerRef = owner },
                spec = new { destinationRef = "ops-notifier", state, eventClasses = new[] { "Operational" }, eventTypes = new[] { "consumer.lag.alert" } } });
            var manifest = DeclarativeSubscriptionManifest.Parse(Encoding.UTF8.GetBytes(json), "json");
            var jsonFile = Path.Combine(_repo, resource + ".json"); var yamlFile = Path.Combine(_repo, resource + ".yaml");
            await File.WriteAllBytesAsync(jsonFile, manifest.ToCanonicalUtf8(), token);
            await File.WriteAllTextAsync(yamlFile, $"apiVersion: kafdeck.io/v0.8\nkind: NotificationSubscription\nmetadata:\n  deploymentRef: g3-deployment\n  name: {resource}\n  ownerRef: {owner}\nspec:\n  destinationRef: ops-notifier\n  state: {state}\n  eventClasses: [Operational]\n  eventTypes: [consumer.lag.alert]\n", token);
            Assert.Equal(manifest.Fingerprint(), DeclarativeSubscriptionManifest.Parse(await File.ReadAllBytesAsync(yamlFile, token), "yaml").Fingerprint());
            await GitAsync(token, "add", "--", resource + ".json", resource + ".yaml");
            await GitAsync(token, "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "Record closed desired subscription"); GitCommits++;
            var committed = await GitAsync(token, "show", "HEAD:" + resource + ".json");
            Assert.Equal(manifest.Fingerprint(), DeclarativeSubscriptionManifest.Parse(Encoding.UTF8.GetBytes(committed), "json").Fingerprint());
            var source = "source-" + resource;
            if (register)
            {
                if (!_registrations.ContainsKey(resource)) _registrations[resource] = _registrations.Count;
                var prefix = RegistrationPrefix(resource);
                var baseline = Lab.Settings.GetValueOrDefault(prefix + "SourceRevision") ?? manifest.Fingerprint();
                foreach (var (name, value) in new[] { ("Id", "controller-" + resource), ("OwnerId", owner), ("SourceRef", source),
                    ("SourceRevision", baseline), ("PrincipalId", _principal), ("Enabled", "true"),
                    ("ExpiresAtUtc", DateTimeOffset.UtcNow.AddHours(1).ToString("O")), ("ResourceIds:0", resource) }) Lab.Settings[prefix + name] = value;
            }
            return new(resource, yaml ? yamlFile : jsonFile, manifest, source);
        }
        internal async Task<Artifact> FrozenCopyAsync(Artifact artifact, CancellationToken token)
        {
            var file = Path.Combine(_directory, "approved-revision-" + Guid.NewGuid().ToString("N") + ".json");
            await File.WriteAllBytesAsync(file, artifact.Manifest.ToCanonicalUtf8(), token); return artifact with { File = file };
        }
        internal async Task<SourceProof> SourceAsync(Artifact artifact, CancellationToken token)
        {
            Assert.Equal(CliApplication.Success, await Lab.RunAsync(token, "declarative", "source-show", artifact.SourceRef, "--resource", artifact.Resource));
            return SourceReceipt();
        }
        internal SourceProof SourceReceipt()
        {
            using var json = JsonDocument.Parse(Lab.Output); var root = json.RootElement;
            return new(root.GetProperty("generation").GetGuid(), root.GetProperty("revision").GetInt64(), root.GetProperty("sourceHash").GetString()!);
        }
        internal Task<int> AdvanceAsync(Artifact artifact, SourceProof expected, CancellationToken token, string? source = null) =>
            Lab.RunAsync(token, "declarative", "source-advance", source ?? artifact.SourceRef, expected.Generation.ToString("D"),
                expected.Revision.ToString(System.Globalization.CultureInfo.InvariantCulture), expected.Hash, "--manifest-file", artifact.File);
        internal string RegistrationPrefix(string resource) => $"Kafdeck:Declarative:Registrations:{_registrations[resource]}:";
        internal void EnableService()
        {
            Lab.EnableServiceRequester();
            var proposal = new NonBrowserServicePrincipalProposal("g3-service", "g3-owned-laboratory", Lab.Field("issuer"),
                Lab.Field("audience"), Lab.Field("subject"), Lab.Field("clientId"),
                [AuthorizationAction.SystemRead, AuthorizationAction.DeclarativeRead, AuthorizationAction.NotificationRead]);
            _principal = "oidc-service:" + string.Join('|', new[] { proposal.Issuer, proposal.Subject, proposal.ClientId, proposal.RegistrationId }.Select(Uri.EscapeDataString));
            Assert.True(RegisteredBearerPrincipal.TryParse(_principal, out var parsed));
            Assert.Equal(NonBrowserPrincipalKind.Service, parsed!.Kind);
            var prefix = "Kafdeck:NonBrowserRead:Registrations:0:";
            foreach (var (name, value) in new[] { ("Id", proposal.RegistrationId), ("OwnerId", proposal.OwnerId),
                ("Subject", proposal.Subject), ("ClientId", proposal.ClientId), ("Enabled", "true"),
                ("ExpiresAtUtc", DateTimeOffset.UtcNow.AddHours(1).ToString("O")) }) Lab.Settings[prefix + name] = value;
            var reads = new[] { "SystemRead", "DeclarativeRead", "NotificationRead" };
            var mutations = new[] { "DeclarativePlan", "DeclarativeAdopt", "DeclarativeApply", "NotificationManage", "DeclarativeSourceManage" };
            foreach (var (action, index) in reads.Select((action, index) => (action, index))) Lab.Settings[prefix + $"ReadActions:{index}"] = action;
            foreach (var (action, index) in mutations.Select((action, index) => (action, index))) Lab.Settings[prefix + $"MutationActions:{index}"] = action;
            var actions = reads.Concat(mutations).ToArray();
            Lab.Settings["Kafdeck:Authorization:Roles:2:Id"] = "g3-service-role";
            foreach (var (action, index) in actions.Select((action, index) => (action, index)))
            {
                Lab.Settings[$"Kafdeck:Authorization:Roles:2:Permissions:{index}:Action"] = action;
                if (action != "SystemRead") Lab.Settings[$"Kafdeck:Authorization:Roles:2:Permissions:{index}:ResourcePatterns:0"] = "*";
            }
            Lab.Settings["Kafdeck:Authorization:SubjectBindings:2:Issuer"] = proposal.Issuer;
            Lab.Settings["Kafdeck:Authorization:SubjectBindings:2:Subject"] = proposal.Subject;
            Lab.Settings["Kafdeck:Authorization:SubjectBindings:2:RoleIds:0"] = "g3-service-role";
        }
        internal W68RuntimeResourceKey Key(string resource) => new("g3-deployment", "Deployment", "g3-deployment", "NotificationSubscription", resource);
        internal async Task<int> CommandAsync(Artifact file, string command, long ownership, CancellationToken token, bool yes = false, string? source = null)
        {
            var args = new List<string> { "declarative", command, "--manifest-file", file.File, "--source", source ?? file.SourceRef,
                "--source-revision", file.Manifest.Fingerprint(), "--ownership-revision", ownership.ToString(System.Globalization.CultureInfo.InvariantCulture) };
            if (command is "plan" or "import-preview" or "reconcile") args.AddRange(["--idempotency-key", "g3-" + Guid.NewGuid().ToString("N")]);
            if (yes) args.Add("--yes");
            var exit = await Lab.RunAsync(token, args.ToArray()); Assert.DoesNotContain(_digest, Lab.Output, StringComparison.Ordinal); return exit;
        }
        internal string OutputField(string field) { using var json = JsonDocument.Parse(Lab.Output); return json.RootElement.GetProperty(field).GetString()!; }
        internal Preview Preview()
        {
            using var json = JsonDocument.Parse(Lab.Output); var root = json.RootElement;
            Assert.Equal("critical", root.GetProperty("riskClass").GetString()); Assert.True(root.GetProperty("requiresIndependentApproval").GetBoolean());
            return new(root.GetProperty("operationId").GetGuid(), root.GetProperty("previewHash").GetString()!,
                root.TryGetProperty("confirmationChallenge", out var challenge) && challenge.ValueKind == JsonValueKind.String ? challenge.GetString() : null);
        }
        internal async Task<Preview> PlanAsync(Artifact artifact, long ownership, CancellationToken token, string verb = "plan")
        { Assert.Equal(CliApplication.Success, await CommandAsync(artifact, verb, ownership, token)); return Preview(); }
        internal async Task ApproveAsync(Preview preview, CancellationToken token, bool alreadyConfirmed = false, G2HumanCliLaboratory? node = null)
        {
            var actor = node ?? Lab;
            if (!alreadyConfirmed)
            {
                var args = new List<string> { "operations", "confirm", preview.Id.ToString("D"), preview.Hash };
                if (preview.Challenge is not null) args.AddRange(["--challenge", preview.Challenge]);
                Assert.Equal(CliApplication.PendingApproval, await actor.RunAsync(token, args.ToArray()));
            }
            Assert.Equal(actor.OptionalServiceRequester ? CliApplication.UsageError : CliApplication.AuthenticationOrAuthorizationError,
                await actor.RunAsync(token, "operations", "approve", preview.Id.ToString("D"), preview.Hash));
            actor.Approver = true;
            try
            {
                Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await actor.RunAsync(token, "operations", "status", preview.Id.ToString("D")));
                Assert.Equal(CliApplication.PendingApproval, await actor.RunAsync(token, "declarative", "show", preview.Id.ToString("D")));
                using (var inspection = JsonDocument.Parse(actor.Output))
                {
                    var review = inspection.RootElement;
                    Assert.Equal(preview.Hash, review.GetProperty("planFingerprint").GetString());
                    Assert.True(review.TryGetProperty("before", out _));
                    Assert.Equal(JsonValueKind.Object, review.GetProperty("after").ValueKind);
                    Assert.True(review.GetProperty("changes").GetArrayLength() > 0);
                    Assert.Equal("Critical", review.GetProperty("risk").GetString());
                    Assert.True(review.GetProperty("requiresIndependentApproval").GetBoolean());
                    Assert.Equal(64, review.GetProperty("policyFingerprint").GetString()!.Length);
                    Assert.Equal(64, review.GetProperty("sourceRevisionHash").GetString()!.Length);
                    Assert.Equal(Generation, review.GetProperty("deploymentGeneration").GetGuid());
                }
                Assert.Equal(CliApplication.PendingApproval, await actor.RunAsync(token, "operations", "approval", preview.Id.ToString("D")));
                Assert.Equal(CliApplication.Success, await actor.RunAsync(token, "operations", "approve", preview.Id.ToString("D"), preview.Hash));
                Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await ApplyAsync(preview, token, actor));
            }
            finally { actor.Approver = false; }
        }
        internal Task<int> ApplyAsync(Preview plan, CancellationToken token, G2HumanCliLaboratory? node = null) =>
            (node ?? Lab).RunAsync(token, "declarative", "apply", plan.Id.ToString("D"), plan.Hash);
        internal async Task ProveTwoNodesAsync(Artifact artifact, CancellationToken token)
        {
            await Lab.RestartAsync(token);
            await using var second = new G2HumanCliLaboratory(Path.Combine(_directory, "second-api"));
            foreach (var entry in Lab.Settings) second.Settings[entry.Key] = entry.Value;
            second.Settings["Kafdeck:Deployment:ListenUrls:0"] = second.Api.AbsoluteUri;
            await second.StartAsync(token);
            var key = "g3-shared-plan-" + Guid.NewGuid().ToString("N");
            var command = new[] { "declarative", "plan", "--manifest-file", artifact.File, "--source", artifact.SourceRef,
                "--source-revision", artifact.Manifest.Fingerprint(), "--ownership-revision", "0", "--idempotency-key", key };
            var results = await Task.WhenAll(Lab.RunAsync(token, command), second.RunAsync(token, command));
            Assert.Contains(CliApplication.Success, results);
            Assert.All(results, result => Assert.True(result is CliApplication.Success or CliApplication.Conflict,
                "The bounded concurrent plan race must produce one admitted preview or a typed CAS conflict."));
            using var winning = JsonDocument.Parse(results[0] == CliApplication.Success ? Lab.Output : second.Output);
            var root = winning.RootElement;
            var preview = new Preview(root.GetProperty("operationId").GetGuid(), root.GetProperty("previewHash").GetString()!,
                root.TryGetProperty("confirmationChallenge", out var challenge) && challenge.ValueKind == JsonValueKind.String ? challenge.GetString() : null);
            Assert.Equal(CliApplication.Success, await second.RunAsync(token, command));
            using (var replay = JsonDocument.Parse(second.Output))
            {
                Assert.Equal(preview.Id, replay.RootElement.GetProperty("operationId").GetGuid());
                Assert.Equal(preview.Hash, replay.RootElement.GetProperty("previewHash").GetString());
            }
            Assert.Null(await Routing.GetSubscriptionAsync(artifact.Resource, token));
            await Lab.StopAsync(token);
            await ApproveAsync(preview, token, node: second);
            Assert.Equal(CliApplication.Success, await ApplyAsync(preview, token, second));
            var receipt = await Store.GetCommitReceiptAsync(preview.Id, token); Assert.NotNull(receipt);
            Assert.Equal(1, receipt.SubscriptionRevision); Assert.Equal(1, receipt.OwnershipRevision);
            await Lab.StartAsync(token);
            Assert.Equal(CliApplication.Success, await ApplyAsync(preview, token));
            Assert.Equal(receipt, await Store.GetCommitReceiptAsync(preview.Id, token));
            Assert.Equal(1, (await Routing.GetSubscriptionAsync(artifact.Resource, token))!.Revision);
            Assert.Equal(4, await CountAsync("kafdeck_declarative_commit_receipts", token));
            Assert.Equal(4, await CountAsync("kafdeck_declarative_ownership_audit", token));
            var completed = await Operations.GetAsync(preview.Id, token); Assert.NotNull(completed);
            Assert.Equal(MutationOperationState.AppliedVerified, completed.State);
            Assert.NotEqual(completed.RequesterPrincipalId, completed.ApprovedByPrincipalId);
        }
        internal void RemoveRequesterAction(string action)
        {
            foreach (var entry in Lab.Settings.Where(pair => pair.Value == action &&
                (pair.Key.StartsWith("Kafdeck:NonBrowserRead:HumanRegistrations:0:", StringComparison.Ordinal) ||
                 pair.Key.StartsWith("Kafdeck:Authorization:Roles:0:Permissions:", StringComparison.Ordinal))).ToArray())
            {
                if (entry.Key.EndsWith(":Action", StringComparison.Ordinal))
                { var prefix = entry.Key[..^"Action".Length]; foreach (var key in Lab.Settings.Keys.Where(key => key.StartsWith(prefix, StringComparison.Ordinal)).ToArray()) Lab.Settings.Remove(key); }
                else Lab.Settings.Remove(entry.Key);
            }
        }
        internal void RestoreSettings(Dictionary<string, string> settings) { Lab.Settings.Clear(); foreach (var item in settings) Lab.Settings[item.Key] = item.Value; }
        internal async Task<long> CountAsync(string table, CancellationToken token)
        {
            if (table is not ("kafdeck_declarative_commit_receipts" or "kafdeck_declarative_ownership_audit" or "kafdeck_declarative_runtime_audit")) throw new InvalidOperationException();
            await using var c = await Connections.OpenAsync(token); await using var command = c.CreateCommand(); command.CommandText = "SELECT COUNT(*) FROM " + table;
            return Convert.ToInt64(await command.ExecuteScalarAsync(token), System.Globalization.CultureInfo.InvariantCulture);
        }
        internal async Task MigrateOperatorAsync(CancellationToken token)
        {
            // Occupy the configured port: a normal server startup cannot pass
            // this proof by briefly starting HTTP and then exiting.
            using var port = new TcpListener(IPAddress.Loopback, Lab.Api.Port); port.Start();
            var root = new DirectoryInfo(AppContext.BaseDirectory);
            while (!File.Exists(Path.Combine(root.FullName, "Kafdeck.slnx"))) root = root.Parent!;
            var build = typeof(V08G3DeclarativeCliLiveTests).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration ?? "Debug";
            var start = new ProcessStartInfo("dotnet") { WorkingDirectory = Path.GetDirectoryName(Lab.ConfigurationFile)!,
                UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
            start.Environment["ASPNETCORE_CONTENTROOT"] = start.WorkingDirectory;
            start.ArgumentList.Add(Path.Combine(root.FullName, "src/backend/Kafdeck.Api/bin", build, "net10.0/Kafdeck.Api.dll"));
            start.ArgumentList.Add("--declarative-schema-migrate");
            using var process = Process.Start(start) ?? throw new InvalidOperationException("Owned operator migration failed to start.");
            var stdout = process.StandardOutput.ReadToEndAsync(); var stderr = process.StandardError.ReadToEndAsync();
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(token); bound.CancelAfter(TimeSpan.FromSeconds(30));
            try { await process.WaitForExitAsync(bound.Token); }
            catch { if (!process.HasExited) process.Kill(true); throw; }
            var logs = await stdout + await stderr;
            await File.WriteAllTextAsync(Path.Combine(_directory, "operator-migration-private.log"), logs, token);
            Assert.False(logs.Contains(_digest, StringComparison.Ordinal), "Operator diagnostics exposed private fixture material.");
            Assert.True(process.ExitCode == 0, "Explicit operator migration must succeed without starting HTTP; private diagnostics retained locally.");
        }
        private async Task<string> GitAsync(CancellationToken token, params string[] args)
        {
            var start = new ProcessStartInfo("git") { WorkingDirectory = _repo, UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
            foreach (var arg in args) start.ArgumentList.Add(arg);
            using var process = Process.Start(start) ?? throw new InvalidOperationException("Owned Git fixture process failed.");
            var stdout = process.StandardOutput.ReadToEndAsync(token); var stderr = process.StandardError.ReadToEndAsync(token);
            await process.WaitForExitAsync(token); var result = await stdout; _ = await stderr;
            Assert.True(process.ExitCode == 0, "Owned temporary Git repository command failed; no source/credential output is published."); return result;
        }
        public async ValueTask DisposeAsync() { await Lab.DisposeAsync(); if (_database is not null) await _database.DisposeAsync(); }
    }
}
