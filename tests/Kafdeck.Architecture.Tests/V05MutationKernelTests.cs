using System.Text;
using System.Text.Json;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05MutationKernelTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 21, 16, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Risk_floor_marks_topic_delete_and_purge_as_critical()
    {
        var topicDelete = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.TopicDelete));
        var purge = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.RecordsPurge));

        Assert.Equal(MutationRiskClass.Critical, topicDelete.RiskClass);
        Assert.True(topicDelete.RequiresIndependentApproval);
        Assert.Equal(MutationRiskClass.Critical, purge.RiskClass);
        Assert.True(purge.RequiresIndependentApproval);
    }

    [Fact]
    public void Bulk_scope_can_only_raise_risk()
    {
        var single = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.TopicCreate, TargetCount: 1));
        var bulk = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.TopicCreate, TargetCount: 2));

        Assert.Equal(MutationRiskClass.Low, single.RiskClass);
        Assert.Equal(MutationRiskClass.Moderate, bulk.RiskClass);
    }

    [Fact]
    public void Preview_hash_is_deterministic_for_equivalent_target_order()
    {
        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.ConsumerOffsetAlter));
        var expires = Now.AddMinutes(5);
        var first = Intent(["cluster/prod/group/g1/topic/b", "cluster/prod/group/g1/topic/a"]);
        var second = Intent(["cluster/prod/group/g1/topic/a", "cluster/prod/group/g1/topic/b"]);

        var firstHash = MutationPreviewHasher.ComputeHash(first, risk, "v0.5-p1", expires);
        var secondHash = MutationPreviewHasher.ComputeHash(second, risk, "v0.5-p1", expires);

        Assert.Equal(firstHash, secondHash);
    }


    [Fact]
    public void Preview_hash_binds_risk_reasons()
    {
        var intent = Intent(["cluster/prod/topic/payments"], MutationOperationKind.TopicAlter);
        var expires = Now.AddMinutes(5);

        var first = new MutationRiskDecision(
            MutationRiskClass.High,
            new[] { "durability_sensitive_change" },
            MutationConfirmationMode.TypedTarget,
            false);
        var second = new MutationRiskDecision(
            MutationRiskClass.High,
            new[] { "bulk_escalation" },
            MutationConfirmationMode.TypedTarget,
            false);

        var firstHash = MutationPreviewHasher.ComputeHash(intent, first, "v0.5-p1", expires);
        var secondHash = MutationPreviewHasher.ComputeHash(intent, second, "v0.5-p1", expires);

        Assert.NotEqual(firstHash, secondHash);
    }

    [Fact]
    public void Critical_operation_requires_distinct_approver()
    {
        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.TopicDelete));
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            Intent(["cluster/prod/topic/payments"], MutationOperationKind.TopicDelete),
            risk,
            "v0.5-p1",
            Now.AddMinutes(5),
            Now,
            "idem-1");

        operation.OpenForConfirmation(Now.AddSeconds(1));
        operation.Confirm(
            "oidc:https://idp.example|alice",
            operation.Snapshot.PreviewHash,
            Now.AddSeconds(2));

        Assert.Equal(MutationOperationState.AwaitingApproval, operation.Snapshot.State);
        Assert.Throws<MutationStateException>(() => operation.Approve(
            "oidc:https://idp.example|alice",
            operation.Snapshot.PreviewHash,
            Now.AddSeconds(3)));

        operation.Approve(
            "oidc:https://idp.example|bob",
            operation.Snapshot.PreviewHash,
            Now.AddSeconds(3));

        Assert.Equal(MutationOperationState.Ready, operation.Snapshot.State);
        Assert.Equal("oidc:https://idp.example|bob", operation.Snapshot.ApprovedByPrincipalId);
    }

    [Fact]
    public void Pre_dispatch_failure_cannot_be_claimed_after_dispatch_started()
    {
        var operation = ReadyOperation(MutationOperationKind.TopicCreate);
        operation.ClaimExecution(Now.AddSeconds(3));
        operation.MarkDispatchStarted(Now.AddSeconds(4));

        Assert.Throws<MutationStateException>(() => operation.Complete(
            MutationExecutionResultKind.FailedBeforeDispatch,
            "network_failure",
            Now.AddSeconds(5)));
    }

    [Fact]
    public void Idempotency_key_hash_is_stable_but_not_plaintext()
    {
        const string key = "client-request-123";
        var first = MutationIdempotency.HashKey(key);
        var second = MutationIdempotency.HashKey(key);

        Assert.Equal(first, second);
        Assert.DoesNotContain(key, first, StringComparison.Ordinal);
        Assert.Equal(64, first.Length);
    }

    [Fact]
    public void Hmac_material_digest_requires_strong_key_and_is_deterministic()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new HmacMutationMaterialDigestService("too-short"));

        using var service = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var material = Encoding.UTF8.GetBytes("sensitive-connector-password");

        var first = service.ComputeDigest(material);
        var second = service.ComputeDigest(material);

        Assert.Equal(first, second);
        Assert.DoesNotContain("sensitive-connector-password", first, StringComparison.Ordinal);
        Assert.Equal(64, first.Length);
    }

    [Fact]
    public void Mutation_configuration_defaults_to_disabled()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>())
            .Build();

        var options = KafdeckConfigurationLoader.Load(configuration);

        Assert.Null(options.Administration);
        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }

    [Fact]
    public void Mutation_mode_requires_oidc_even_when_persistence_is_configured()
    {
        var options = LoadMutationOptions(accessMode: "Local");

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains("requires OIDC access mode", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Sqlite_high_availability_mutation_mode_fails_closed()
    {
        var options = LoadMutationOptions(
            accessMode: "Oidc",
            executionMode: "HighAvailability");

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains("SQLite mutation persistence supports standalone execution only", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Valid_oidc_sqlite_mutation_configuration_is_accepted_and_secret_safe()
    {
        const string digestKeyVariable = "KAFDECK_MUTATION_DIGEST_KEY";
        var options = LoadMutationOptions(accessMode: "Oidc");

        KafdeckConfigurationValidator.ValidateAndThrow(options);
        Assert.True(options.Administration!.Mutations.Enabled);
        Assert.Equal(
            MutationPersistenceProvider.Sqlite,
            options.Administration.Mutations.Persistence!.Provider);

        var json = JsonSerializer.Serialize(SafeConfigurationDiagnostics.Create(options));
        Assert.Contains("MutationModeEnabled", json, StringComparison.Ordinal);
        Assert.DoesNotContain(digestKeyVariable, json, StringComparison.Ordinal);
        Assert.DoesNotContain("MaterialDigestKey", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Authorization_loader_parses_explicit_mutation_action()
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:Authorization:Roles:0:Id"] = "topic-admin",
            ["Kafdeck:Authorization:Roles:0:Permissions:0:Action"] = "topic.delete",
            ["Kafdeck:Authorization:Roles:0:Permissions:0:ClusterIds:0"] = "prod",
            ["Kafdeck:Authorization:Roles:0:Permissions:0:ResourcePatterns:0"] = "payments.*",
            ["Kafdeck:Authorization:SubjectBindings:0:Issuer"] = "https://idp.example",
            ["Kafdeck:Authorization:SubjectBindings:0:Subject"] = "alice",
            ["Kafdeck:Authorization:SubjectBindings:0:RoleIds:0"] = "topic-admin",
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

        var evaluator = new AuthorizationPolicyEvaluator(
            AuthorizationPolicyCompiler.Compile(
                AuthorizationPolicyConfigurationLoader.Load(configuration)));

        var identity = new OperatorIdentity(
            new OperatorIdentityKey("https://idp.example", "alice"));

        Assert.True(evaluator.Evaluate(
            identity,
            new AuthorizationRequest(
                AuthorizationAction.TopicDelete,
                "prod",
                "payments.events")).IsAllowed);

        Assert.False(evaluator.Evaluate(
            identity,
            new AuthorizationRequest(
                AuthorizationAction.TopicRead,
                "prod",
                "payments.events")).IsAllowed);
    }

    [Fact]
    public void Mutation_audit_contract_contains_no_payload_or_secret_fields()
    {
        var names = typeof(MutationAuditEvent)
            .GetProperties()
            .Select(property => property.Name)
            .ToArray();

        var forbidden = new[] { "payload", "value", "keybytes", "secret", "password", "token", "cookie" };
        foreach (var name in names)
        {
            Assert.DoesNotContain(
                forbidden,
                term => name.Contains(term, StringComparison.OrdinalIgnoreCase));
        }
    }

    private static MutationOperation ReadyOperation(MutationOperationKind kind)
    {
        var risk = MutationRiskClassifier.Classify(new MutationRiskInput(kind));
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            Intent(["cluster/prod/topic/payments"], kind),
            risk,
            "v0.5-p1",
            Now.AddMinutes(5),
            Now,
            "idem-ready");

        operation.OpenForConfirmation(Now.AddSeconds(1));
        operation.Confirm(
            "oidc:https://idp.example|alice",
            operation.Snapshot.PreviewHash,
            Now.AddSeconds(2));

        if (operation.Snapshot.State == MutationOperationState.AwaitingApproval)
        {
            operation.Approve(
                "oidc:https://idp.example|bob",
                operation.Snapshot.PreviewHash,
                Now.AddSeconds(2));
        }

        Assert.Equal(MutationOperationState.Ready, operation.Snapshot.State);
        return operation;
    }

    private static MutationIntentDescriptor Intent(
        IReadOnlyList<string> resources,
        MutationOperationKind kind = MutationOperationKind.ConsumerOffsetAlter) =>
        new(
            kind,
            "prod",
            "{\"operation\":\"test\"}",
            resources,
            new[] { new MutationPrecondition("metadata", "sha256:abc") });

    private static KafdeckOptions LoadMutationOptions(
        string accessMode,
        string executionMode = "Standalone")
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://127.0.0.1:8080",
            ["Kafdeck:Deployment:AccessMode"] = accessMode,
            ["Kafdeck:Administration:Mutations:Enabled"] = "true",
            ["Kafdeck:Administration:Mutations:MaterialDigestKey"] = "env:KAFDECK_MUTATION_DIGEST_KEY",
            ["Kafdeck:Administration:Mutations:PreviewTtlSeconds"] = "300",
            ["Kafdeck:Administration:Mutations:MaxConcurrentPerCluster"] = "2",
            ["Kafdeck:Administration:Mutations:Persistence:Provider"] = "Sqlite",
            ["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"] = executionMode,
            ["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"] = "/tmp/kafdeck-mutations.db",
        };

        if (string.Equals(accessMode, "Oidc", StringComparison.OrdinalIgnoreCase))
        {
            values["Kafdeck:Deployment:Oidc:Issuer"] = "https://idp.example";
            values["Kafdeck:Deployment:Oidc:ClientId"] = "kafdeck";
            values["Kafdeck:Deployment:Oidc:Scopes:0"] = "openid";
            values["Kafdeck:Deployment:Oidc:Scopes:1"] = "profile";
        }

        return KafdeckConfigurationLoader.Load(
            new ConfigurationBuilder().AddInMemoryCollection(values).Build());
    }
}
