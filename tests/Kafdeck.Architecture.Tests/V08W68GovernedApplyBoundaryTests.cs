using System.Security.Claims;
using System.Text;
using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Primitives;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68GovernedApplyBoundaryTests
{
    private const string Issuer = "https://idp.example";
    private static readonly string Hash = new('a', 64);

    [Fact]
    public void Effect_intent_is_only_immutable_lookup_and_keeps_old_new_destination_conjunction()
    {
        var plan = Plan(); var intent = DeclarativeSubscriptionMutation.Intent(plan);
        Assert.Equal(31, (int)intent.Kind);
        Assert.DoesNotContain("eventClasses", intent.CanonicalIntent, StringComparison.Ordinal);
        Assert.Equal((plan.PlanId, plan.PlanFingerprint), DeclarativeSubscriptionMutation.ReadReference(intent.CanonicalIntent));
        foreach (var resource in new[] { "subscription", "old", "new" })
        {
            Assert.Contains(intent.AuthorizationTargets!, t => t.Action == AuthorizationAction.NotificationRead && t.ResourceName == resource);
            var write = Assert.Single(intent.AuthorizationTargets!, t => t.Action == AuthorizationAction.NotificationManage && t.ResourceName == resource);
            Assert.Null(DeclarativeMutationAuthorization.Request(write).ClusterId);
        }
        Assert.All(intent.AuthorizationTargets!.Where(t => t.Action == AuthorizationAction.DeclarativeApply),
            target => Assert.Equal("deployment", DeclarativeMutationAuthorization.Request(target).ClusterId));
        var risk = MutationRiskClassifier.Classify(new(intent.Kind));
        Assert.Equal(MutationRiskClass.Critical, risk.RiskClass); Assert.True(risk.RequiresIndependentApproval);
        Assert.Equal(MutationConfirmationMode.TypedTarget, risk.ConfirmationMode);
    }

    [Theory]
    [InlineData(AuthorizationAction.DeclarativeApply)]
    [InlineData(AuthorizationAction.NotificationRead)]
    [InlineData(AuthorizationAction.NotificationManage)]
    public void Missing_command_or_underlying_action_cannot_normalize_to_a_weaker_effect(AuthorizationAction removed)
    {
        var plan = Plan(); var targets = DeclarativeSubscriptionMutation.Targets(plan).Where(t => t.Action != removed).ToArray();
        Assert.Throws<ArgumentException>(() => DeclarativeMutationAuthorization.Normalize("deployment", targets));
    }

    [Fact]
    public void Adoption_is_read_and_explicit_adopt_without_manufacturing_subscription_write_authority()
    {
        var plan = Seal(Plan() with { Command = W68RuntimePlanCommand.Adopt });
        var targets = DeclarativeSubscriptionMutation.Intent(plan).AuthorizationTargets!;
        Assert.Contains(targets, t => t.Action == AuthorizationAction.DeclarativeAdopt);
        Assert.DoesNotContain(targets, t => t.Action is AuthorizationAction.NotificationManage or AuthorizationAction.DeclarativeApply);
        Assert.Contains(targets, t => t.Action == AuthorizationAction.NotificationRead && t.ResourceName == "old");
    }

    [Fact]
    public void Canonical_reference_and_stored_desired_payload_cannot_be_modified_or_reconstructed()
    {
        var plan = Plan(); var intent = DeclarativeSubscriptionMutation.Intent(plan);
        Assert.Throws<MutationStateException>(() => DeclarativeSubscriptionMutation.ReadReference(intent.CanonicalIntent + " "));
        Assert.Throws<MutationStateException>(() => DeclarativeSubscriptionMutation.ReadReference(
            intent.CanonicalIntent.Replace("\"SchemaVersion\":1", "\"SchemaVersion\":1,\"SchemaVersion\":1", StringComparison.Ordinal)));
        Assert.Throws<MutationStateException>(() => DeclarativeSubscriptionMutation.Validate(plan with { OwnerId = "another-owner" }));
        var changed = Seal(plan with { DesiredState = NotificationSubscriptionState.Paused });
        Assert.Throws<MutationStateException>(() => DeclarativeSubscriptionMutation.Validate(changed));
        Assert.Throws<ArgumentException>(() => DeclarativeMutationAuthorization.Normalize("other-deployment", intent.AuthorizationTargets));
    }

    [Theory]
    [InlineData("owner")][InlineData("source")][InlineData("revision")][InlineData("resource")][InlineData("principal")]
    [InlineData("disabled")][InlineData("expired")]
    public void Server_registration_is_exact_current_and_never_inferred_from_manifest_fields(string change)
    {
        var principal = Human("alice"); MutationActor.TryCreate(principal, out var actor);
        using var configuration = Configuration(actor!.PrincipalId);
        var authorizer = Authorizer(configuration, Policy());
        Assert.True(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Apply));
        var prefix = "Kafdeck:Declarative:Registrations:0:";
        configuration[prefix + (change switch { "owner" => "OwnerId", "source" => "SourceRef", "revision" => "SourceRevision",
            "resource" => "ResourceIds:0", "principal" => "PrincipalId", "disabled" => "Enabled", _ => "ExpiresAtUtc" })] =
            change switch { "revision" => new string('b', 64), "disabled" => "false", "expired" => DateTimeOffset.UtcNow.AddMinutes(-1).ToString("O"), _ => "foreign" };
        Assert.False(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Apply));
    }

    [Fact]
    public void Plan_only_and_read_only_cannot_apply_and_current_underlying_destination_revocation_wins()
    {
        var principal = Human("alice"); MutationActor.TryCreate(principal, out var actor); using var config = Configuration(actor!.PrincipalId);
        var policy = Policy(); var evaluator = new AuthorizationPolicyEvaluator(policy, () => policy);
        var authorizer = Authorizer(config, evaluator);
        Assert.True(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Apply));
        policy = Policy(exclude: AuthorizationAction.DeclarativeApply);
        Assert.True(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Plan));
        Assert.False(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Apply));
        policy = Policy(exclude: AuthorizationAction.DeclarativePlan);
        Assert.True(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Read));
        Assert.False(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Plan));
        policy = Policy(notificationResources: ["subscription", "new"]);
        Assert.False(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Apply));
        Assert.False(Allowed(authorizer, Human("mallory"), DeclarativeRuntimeVerb.Read));
    }

    [Theory]
    [InlineData("malformed")][InlineData("oversized")][InlineData("generation")][InlineData("quarantined")]
    [InlineData("tampered")][InlineData("missing")][InlineData("extra-field")][InlineData("duplicate-field")]
    public void External_generation_anchor_is_read_only_and_fail_closed_for_actual_file_failures(string failure)
    {
        var principal = Human("alice"); MutationActor.TryCreate(principal, out var actor);
        using var configuration = Configuration(actor!.PrincipalId);
        var authorizer = Authorizer(configuration, Policy());
        Assert.True(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Apply));
        var generation = configuration["Kafdeck:Declarative:DeploymentGeneration"]!;
        var content = failure switch
        {
            "malformed" => "{",
            "oversized" => new string(' ', 65537),
            "generation" => AnchorJson(Guid.NewGuid(), "active"),
            "quarantined" => AnchorJson(Guid.Parse(generation), "quarantined"),
            "extra-field" => AnchorJson(Guid.Parse(generation), "active").Replace("}", ",\"approved\":true}", StringComparison.Ordinal),
            "duplicate-field" => AnchorJson(Guid.Parse(generation), "active").Replace("\"version\":1", "\"version\":1,\"version\":1", StringComparison.Ordinal),
            _ => AnchorJson(Guid.NewGuid(), "active"),
        };
        if (failure == "missing") File.Delete(configuration.AnchorFile);
        else
        {
            File.WriteAllText(configuration.AnchorFile, content, new UTF8Encoding(false));
            // Test schema and state validation independently of integrity mismatch.
            if (failure != "tampered") configuration["Kafdeck:Declarative:RecoveryAnchorSha256"] =
                Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(configuration.AnchorFile))).ToLowerInvariant();
        }
        var before = File.Exists(configuration.AnchorFile) ? File.ReadAllBytes(configuration.AnchorFile) : null;
        Assert.False(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Read));
        Assert.False(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Plan));
        Assert.False(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Adopt));
        Assert.False(Allowed(authorizer, principal, DeclarativeRuntimeVerb.Apply));
        if (before is null) Assert.False(File.Exists(configuration.AnchorFile));
        else Assert.Equal(before, File.ReadAllBytes(configuration.AnchorFile));
    }

    [Fact]
    public async Task Typed_subscription_write_is_part_of_callers_transaction_and_retired_is_terminal()
    {
        var path = Path.Combine(Path.GetTempPath(), "kafdeck-w68-atomic-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            var factory = new SqliteNotificationDeliveryDbConnectionFactory(path);
            var store = new AdoNotificationRoutingStore(factory); await store.InitializeAsync();
            var definition = new NotificationSubscriptionDefinition("subscription", "new", [NotificationEventClass.Operational]);
            var now = DateTimeOffset.UtcNow;
            await using (var connection = await factory.OpenAsync())
            await using (var transaction = await connection.BeginTransactionAsync())
            {
                Assert.NotNull(await AdoNotificationRoutingStore.WriteSubscriptionInTransactionAsync(connection, transaction, definition,
                    NotificationSubscriptionState.Active, null, now, false));
                var snapshot = await AdoNotificationRoutingStore.ReadSubscriptionInTransactionAsync(connection, transaction, "subscription", false);
                Assert.Equal(1, snapshot!.Revision);
                await transaction.RollbackAsync();
            }
            Assert.Null(await store.GetSubscriptionAsync("subscription"));
            await store.CreateSubscriptionAsync(definition, NotificationSubscriptionState.Active, now);
            await store.ReplaceSubscriptionAsync(definition, NotificationSubscriptionState.Retired, 1, now.AddSeconds(1));
            await using (var connection = await factory.OpenAsync())
            await using (var transaction = await connection.BeginTransactionAsync())
            {
                Assert.Null(await AdoNotificationRoutingStore.WriteSubscriptionInTransactionAsync(connection, transaction, definition,
                    NotificationSubscriptionState.Active, 2, now.AddSeconds(2), false));
                await transaction.RollbackAsync();
            }
            Assert.Equal(NotificationSubscriptionState.Retired, (await store.GetSubscriptionAsync("subscription"))!.State);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var suffix in new[] { "", "-wal", "-shm" }) if (File.Exists(path + suffix)) File.Delete(path + suffix);
        }
    }

    internal static W68StoredRuntimePlan Plan()
    {
        var canonical = """{"apiVersion":"kafdeck.io/v0.8","kind":"NotificationSubscription","metadata":{"deploymentRef":"deployment","name":"subscription","ownerRef":"owner"},"spec":{"destinationRef":"new","state":"Active","eventClasses":["Operational"],"eventTypes":[]}}""";
        var manifest = DeclarativeSubscriptionManifest.Parse(Encoding.UTF8.GetBytes(canonical), "json");
        canonical = Encoding.UTF8.GetString(manifest.ToCanonicalUtf8());
        return Seal(new(Guid.NewGuid(), W68RuntimePlanCommand.Apply, new("deployment", "Deployment", "deployment", "NotificationSubscription", "subscription"),
            "owner", "source", Guid.NewGuid(), Hash, "oidc:https://idp.example|alice", 1, Guid.NewGuid(), 1,
            Hash, "old", manifest.Definition, manifest.State, canonical, manifest.Fingerprint(), Hash,
            "controller", "scope", "holder", 1, DateTimeOffset.UtcNow.AddMinutes(2), "", DeploymentGeneration: Guid.NewGuid()));
    }
    private static W68StoredRuntimePlan Seal(W68StoredRuntimePlan plan) => plan with { PlanFingerprint = W68RuntimeFingerprints.Plan(plan) };
    private static ClaimsPrincipal Human(string subject) => OidcIdentityNormalizer.Normalize(
        new(new ClaimsIdentity([new Claim("sub", subject)], "oidc")), Issuer, null, DateTimeOffset.UtcNow);
    private static bool Allowed(DeclarativeRuntimeAuthorizer authority, ClaimsPrincipal principal, DeclarativeRuntimeVerb verb) =>
        authority.Authorize(principal, "deployment", "owner", "source", Hash, "subscription", ["old", "new"], verb);
    private static AnchorConfiguration Configuration(string principal) => new(new Dictionary<string, string?>
    {
        ["Kafdeck:Declarative:Enabled"] = "true", ["Kafdeck:Declarative:DeploymentId"] = "deployment",
        ["Kafdeck:Declarative:Registrations:0:Id"] = "controller", ["Kafdeck:Declarative:Registrations:0:OwnerId"] = "owner",
        ["Kafdeck:Declarative:Registrations:0:SourceRef"] = "source", ["Kafdeck:Declarative:Registrations:0:SourceRevision"] = Hash,
        ["Kafdeck:Declarative:Registrations:0:PrincipalId"] = principal, ["Kafdeck:Declarative:Registrations:0:Enabled"] = "true",
        ["Kafdeck:Declarative:Registrations:0:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddHours(1).ToString("O"),
        ["Kafdeck:Declarative:Registrations:0:ResourceIds:0"] = "subscription",
    });
    private static string AnchorJson(Guid generation, string state) => JsonSerializer.Serialize(new
        { version = 1, deploymentId = "deployment", generation, state });
    private sealed class AnchorConfiguration : IConfigurationRoot, IDisposable
    {
        private readonly IConfigurationRoot _configuration;
        public string AnchorFile { get; } = Path.Combine(Path.GetTempPath(), "kafdeck-w68-external-anchor-" + Guid.NewGuid().ToString("N") + ".json");
        public AnchorConfiguration(Dictionary<string, string?> values)
        {
            var generation = Guid.NewGuid();
            File.WriteAllText(AnchorFile, AnchorJson(generation, "active"), new UTF8Encoding(false));
            values["Kafdeck:Declarative:DeploymentGeneration"] = generation.ToString("D");
            values["Kafdeck:Declarative:RecoveryAnchorFile"] = AnchorFile;
            values["Kafdeck:Declarative:RecoveryAnchorSha256"] = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(AnchorFile))).ToLowerInvariant();
            _configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        }
        public string? this[string key] { get => _configuration[key]; set => _configuration[key] = value; }
        public IEnumerable<IConfigurationProvider> Providers => _configuration.Providers;
        public IEnumerable<IConfigurationSection> GetChildren() => _configuration.GetChildren();
        public IConfigurationSection GetSection(string key) => _configuration.GetSection(key);
        public IChangeToken GetReloadToken() => _configuration.GetReloadToken();
        public void Reload() => _configuration.Reload();
        public void Dispose() { (_configuration as IDisposable)?.Dispose(); if (File.Exists(AnchorFile)) File.Delete(AnchorFile); }
    }
    private static AuthorizationPolicySnapshot Policy(AuthorizationAction? exclude = null, string[]? notificationResources = null) =>
        AuthorizationPolicyCompiler.Compile(new([new("owner", new[] { AuthorizationAction.DeclarativeRead, AuthorizationAction.DeclarativePlan,
            AuthorizationAction.DeclarativeAdopt, AuthorizationAction.DeclarativeApply, AuthorizationAction.NotificationRead, AuthorizationAction.NotificationManage }
            .Where(a => a != exclude).Select(a => new AuthorizationPermissionDefinition(a,
                a is AuthorizationAction.NotificationRead or AuthorizationAction.NotificationManage ? null : ["deployment"],
                a is AuthorizationAction.NotificationRead or AuthorizationAction.NotificationManage ? notificationResources ?? ["subscription", "old", "new"] : ["subscription"])).ToArray())],
            [new(Issuer, "alice", ["owner"])], []));
    private static DeclarativeRuntimeAuthorizer Authorizer(IConfiguration config, AuthorizationPolicySnapshot policy) => Authorizer(config, new AuthorizationPolicyEvaluator(policy));
    private static DeclarativeRuntimeAuthorizer Authorizer(IConfiguration config, AuthorizationPolicyEvaluator evaluator) =>
        new(config, new(new KafdeckOptions(new("http://127.0.0.1:8080", null, AccessMode.Oidc, null), []), evaluator));
}
