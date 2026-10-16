using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Cli;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Topics;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67TopicLifecycleBoundaryTests
{
    [Fact]
    public void Topic_configurations_are_closed_unique_typed_and_reset_is_alter_only()
    {
        var create = CliParser.Parse(["--service-profile", "service.json", "topics", "create-preview", "cluster", "orders", "100000", "1",
            "--config", "retention.ms=60000", "--config", "segment.bytes=1048576", "--idempotency-key", "create"]);
        Assert.Equal("60000", create.TopicConfigurations!["retention.ms"]);
        Assert.Equal(100000, create.PartitionCount);
        var alter = CliParser.Parse(["--service-profile", "service.json", "topics", "alter-preview", "cluster", "orders",
            "--config", "retention.ms=60000", "--reset-config", "segment.bytes", "--idempotency-key", "alter"]);
        Assert.Equal(CliCommand.TopicAlterPreview, alter.Command);
        Assert.Equal("/api/v1/automation/clusters/cluster/topics/orders/alter/preview", CliRouteBuilder.Build(alter));
        Assert.Null(alter.TopicConfigurations!["segment.bytes"]);
        var normalized = TopicMutationPolicy.NormalizeAlter(new("cluster", "orders", alter.TopicConfigurations));
        Assert.Equal("60000", normalized.Changes["retention.ms"]);
        Assert.Null(normalized.Changes["segment.bytes"]);
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "service.json", "topics", "create-preview", "cluster", "orders", "1", "1",
            "--reset-config", "retention.ms", "--idempotency-key", "create"]));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "service.json", "topics", "alter-preview", "cluster", "orders", "--idempotency-key", "empty"]));
    }

    [Theory]
    [InlineData("--config", "password=credential-sentinel", "--config", "retention.ms=60000")]
    [InlineData("--config", "retention.ms=60000", "--config", "retention.ms=credential-sentinel")]
    [InlineData("--reset-config", "retention.ms", "--config", "retention.ms=credential-sentinel")]
    [InlineData("--reset-config", "credential-sentinel", "--config", "retention.ms=60000")]
    [InlineData("--config", "credential-sentinel", "--config", "retention.ms=60000")]
    [InlineData("--config", "retention.ms=credential-sentinel\n", "--config", "segment.ms=60000")]
    public void Invalid_configuration_options_reject_before_auth_without_echoing_inline_values(string firstOption, string first, string secondOption, string second)
    {
        var error = Assert.Throws<CliUsageException>(() => CliParser.Parse(["--oidc-profile", "human.json", "topics", "alter-preview", "cluster", "orders",
            firstOption, first, secondOption, second, "--idempotency-key", "key"]));
        Assert.DoesNotContain("credential-sentinel", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void All_nine_CLI_configuration_keys_are_validated_by_the_same_authoritative_backend_policy()
    {
        var values = new Dictionary<string, string>
        {
            ["cleanup.policy"] = "delete", ["retention.ms"] = "60000", ["retention.bytes"] = "1048576",
            ["segment.ms"] = "60000", ["segment.bytes"] = "1048576", ["min.insync.replicas"] = "1",
            ["max.message.bytes"] = "1048576", ["delete.retention.ms"] = "60000", ["min.cleanable.dirty.ratio"] = "0.5",
        };
        var args = new List<string> { "--service-profile", "service.json", "topics", "create-preview", "cluster", "orders", "1", "1", "--idempotency-key", "create" };
        foreach (var (key, value) in values) { args.Add("--config"); args.Add(key + "=" + value); }
        var parsed = CliParser.Parse(args);
        var normalized = TopicMutationPolicy.NormalizeCreate(new("cluster", "orders", 1, 1,
            parsed.TopicConfigurations!.ToDictionary(p => p.Key, p => p.Value!, StringComparer.Ordinal)));
        Assert.Equal(9, normalized.Configurations.Count);
        var failureArgs = args.Take(10).ToList();
        failureArgs.AddRange(["--config", "retention.ms=" + new string('x', 257) + "credential-sentinel"]);
        var error = Assert.Throws<CliUsageException>(() => CliParser.Parse(failureArgs));
        Assert.DoesNotContain("credential-sentinel", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("100001")]
    [InlineData("-1")]
    [InlineData("2;provider-command")]
    public void Partition_target_is_bounded_and_cannot_be_a_provider_command(string target) =>
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "service.json", "topics", "increase-partitions-preview", "cluster", "orders", target,
            "--idempotency-key", "increase"]));

    [Fact]
    public void Increase_uses_typed_target_and_cannot_take_configuration_or_deployment_credentials()
    {
        var increase = CliParser.Parse(["--service-profile", "service.json", "topics", "increase-partitions-preview", "cluster", "orders", "2", "--idempotency-key", "increase"]);
        Assert.Equal(2, increase.PartitionCount);
        Assert.Equal("/api/v1/automation/clusters/cluster/topics/orders/increase-partitions/preview", CliRouteBuilder.Build(increase));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["topics", "increase-partitions-preview", "cluster", "orders", "2", "--idempotency-key", "increase"]));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "service.json", "topics", "increase-partitions-preview", "cluster", "orders", "2",
            "--config", "retention.ms=60000", "--idempotency-key", "increase"]));
    }

    [Theory]
    [InlineData(MutationOperationKind.TopicAlter)]
    [InlineData(MutationOperationKind.TopicIncreasePartitions)]
    public void New_topic_kinds_require_exact_alter_registration_current_resource_scope_and_policy(MutationOperationKind kind)
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Kafdeck:NonBrowserRead:Enabled"] = "true", ["Kafdeck:NonBrowserRead:GovernedMutationsEnabled"] = "true",
            ["Kafdeck:NonBrowserRead:Issuer"] = "https://sso.test/realms/g2", ["Kafdeck:NonBrowserRead:ApiAudience"] = "api",
            ["Kafdeck:NonBrowserRead:Registrations:0:Id"] = "machine", ["Kafdeck:NonBrowserRead:Registrations:0:OwnerId"] = "owner",
            ["Kafdeck:NonBrowserRead:Registrations:0:ClientId"] = "client", ["Kafdeck:NonBrowserRead:Registrations:0:Subject"] = "subject",
            ["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "true", ["Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(5).ToString("O"),
            ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0"] = "SystemRead", ["Kafdeck:NonBrowserRead:Registrations:0:MutationActions:0"] = "TopicAlter",
        }).Build();
        var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new(
            [new("alter", [new(AuthorizationAction.TopicAlter, ["allowed"], ["orders"])])], [new("https://sso.test/realms/g2", "subject", ["alter"])], [])));
        var identity = new NonBrowserReadIdentity("machine", new("https://sso.test/realms/g2", "subject", "client", "api", DateTimeOffset.UtcNow.AddMinutes(2)));
        var principal = new ClaimsPrincipal(identity);
        var authorization = new MutationRequestAuthorizationService(new(new KafdeckOptions(new("http://127.0.0.1:8080", null, AccessMode.Oidc, null), []), evaluator, new(config)));
        var now = DateTimeOffset.UtcNow;
        var operation = MutationOperation.CreatePreview(identity.AuditId, new(kind, "allowed", "{}", ["topic/orders"],
            AuthorizationTargets: [new(AuthorizationAction.TopicAlter, "allowed", "orders")]), MutationRiskClassifier.Classify(new(kind)),
            authorization.PolicyVersionFor(principal), now.AddMinutes(2), now, "alter").Snapshot;
        Assert.Equal(KafdeckAuthorizationOutcome.Allowed, authorization.AuthorizeForDispatch(principal, operation));
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, authorization.AuthorizeForDispatch(principal, operation with { PolicyVersion = "old-policy" }));
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, authorization.AuthorizeForDispatch(principal, operation with
        { AuthorizationTargets = [new(AuthorizationAction.TopicAlter, "allowed", "another-topic")] }));
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, authorization.AuthorizeForDispatch(principal, operation with { OperationKind = MutationOperationKind.ConsumerDelete }));
        config["Kafdeck:NonBrowserRead:Registrations:0:MutationActions:0"] = "TopicCreate";
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, authorization.AuthorizeForDispatch(principal, operation));
    }

    [Fact]
    public async Task Owner_can_stop_ready_critical_plan_after_approver_revocation_without_renewing_effect_authority()
    {
        var fixture = V08W67CriticalApprovalBoundaryTests.Fixture();
        var operation = V08W67CriticalApprovalBoundaryTests.Operation(fixture.Authorization, fixture.Requester);
        var repository = new V08W67CriticalApprovalBoundaryTests.MemoryRepository(operation.Snapshot);
        var commands = new MutationCommandService(repository, new(fixture.Evaluator), fixture.Authorization);
        await commands.ConfirmAsync(fixture.Requester, operation.Snapshot.OperationId, operation.Snapshot.PreviewHash, operation.Snapshot.ConfirmationChallenge);
        await commands.ApproveAsync(fixture.Approver, operation.Snapshot.OperationId, operation.Snapshot.PreviewHash);
        fixture.Configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:Enabled"] = "false";
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, fixture.Authorization.AuthorizeForDispatch(fixture.Requester, repository.Current));
        var result = await commands.CancelAsync(fixture.Requester, operation.Snapshot.OperationId, operation.Snapshot.PreviewHash);
        Assert.Equal(MutationCommandOutcome.Saved, result.Outcome);
        Assert.Equal(MutationOperationState.Cancelled, result.Operation!.State);
        Assert.Throws<MutationStateException>(() => MutationOperation.Restore(result.Operation).ClaimExecution(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddSeconds(30)));
    }

    [Fact]
    public async Task Cancellation_rechecks_exact_actor_current_registration_policy_and_hash()
    {
        var fixture = V08W67CriticalApprovalBoundaryTests.Fixture();
        var operation = V08W67CriticalApprovalBoundaryTests.Operation(fixture.Authorization, fixture.Requester);
        var repository = new V08W67CriticalApprovalBoundaryTests.MemoryRepository(operation.Snapshot);
        var commands = new MutationCommandService(repository, new(fixture.Evaluator), fixture.Authorization);
        Assert.Equal(MutationCommandOutcome.Forbidden, (await commands.CancelAsync(fixture.Approver, operation.Snapshot.OperationId, operation.Snapshot.PreviewHash)).Outcome);
        var wrongHash = (operation.Snapshot.PreviewHash[0] == 'a' ? "b" : "a") + operation.Snapshot.PreviewHash[1..];
        Assert.Equal(MutationCommandOutcome.InvalidState, (await commands.CancelAsync(fixture.Requester, operation.Snapshot.OperationId, wrongHash)).Outcome);
        fixture.Configuration["Kafdeck:NonBrowserRead:HumanRegistrations:0:Enabled"] = "false";
        Assert.Equal(MutationCommandOutcome.Forbidden, (await commands.CancelAsync(fixture.Requester, operation.Snapshot.OperationId, operation.Snapshot.PreviewHash)).Outcome);
        fixture.Configuration["Kafdeck:NonBrowserRead:HumanRegistrations:0:Enabled"] = "true";
        var staleRepository = new V08W67CriticalApprovalBoundaryTests.MemoryRepository(operation.Snapshot with { PolicyVersion = "old-policy" });
        var staleCommands = new MutationCommandService(staleRepository, new(fixture.Evaluator), fixture.Authorization);
        Assert.Equal(MutationCommandOutcome.Forbidden, (await staleCommands.CancelAsync(fixture.Requester, operation.Snapshot.OperationId, operation.Snapshot.PreviewHash)).Outcome);
        Assert.Equal(MutationOperationState.AwaitingConfirmation, repository.Current.State);
    }

    [Fact]
    public async Task Rejection_is_distinct_human_and_cancellation_never_clears_unknown_or_executing_effects()
    {
        var fixture = V08W67CriticalApprovalBoundaryTests.Fixture();
        var operation = V08W67CriticalApprovalBoundaryTests.Operation(fixture.Authorization, fixture.Requester);
        var repository = new V08W67CriticalApprovalBoundaryTests.MemoryRepository(operation.Snapshot);
        var commands = new MutationCommandService(repository, new(fixture.Evaluator), fixture.Authorization);
        await commands.ConfirmAsync(fixture.Requester, operation.Snapshot.OperationId, operation.Snapshot.PreviewHash, operation.Snapshot.ConfirmationChallenge);
        Assert.Equal(MutationCommandOutcome.Forbidden, (await commands.RejectAsync(fixture.Requester, operation.Snapshot.OperationId, operation.Snapshot.PreviewHash)).Outcome);
        var rejected = await commands.RejectAsync(fixture.Approver, operation.Snapshot.OperationId, operation.Snapshot.PreviewHash);
        Assert.Equal(MutationCommandOutcome.Saved, rejected.Outcome);
        Assert.Equal(MutationOperationState.Rejected, rejected.Operation!.State);
        Assert.Equal(((NonBrowserReadIdentity)fixture.Approver.Identity!).AuditId, rejected.Operation.RejectedByPrincipalId);
        foreach (var state in new[] { MutationOperationState.Executing, MutationOperationState.ExecutionUnknown, MutationOperationState.AppliedVerified })
        {
            var unsafeRepo = new V08W67CriticalApprovalBoundaryTests.MemoryRepository(operation.Snapshot with { State = state });
            var unsafeCommands = new MutationCommandService(unsafeRepo, new(fixture.Evaluator), fixture.Authorization);
            Assert.Equal(MutationCommandOutcome.InvalidState, (await unsafeCommands.CancelAsync(fixture.Requester, operation.Snapshot.OperationId, operation.Snapshot.PreviewHash)).Outcome);
            Assert.Equal(state, unsafeRepo.Current.State);
        }
    }

    [Theory]
    [InlineData("cancelled", CliCommand.OperationCancel)]
    [InlineData("rejected", CliCommand.OperationReject)]
    public void Intentional_stop_command_success_is_distinct_from_effect_status_success(string state, CliCommand command)
    {
        var json = JsonSerializer.Serialize(new { state });
        Assert.Equal(CliApplication.Success, CliApplication.MutationOutcomeCategory(json, command));
        Assert.Equal(CliApplication.RemoteError, CliApplication.MutationOutcomeCategory(json, CliCommand.OperationStatus));
        Assert.Equal(CliApplication.RemoteError, CliApplication.MutationOutcomeCategory(json, CliCommand.OperationExecute));
        Assert.Equal(CliApplication.UnknownEffect, CliApplication.MutationOutcomeCategory("{\"state\":\"executionUnknown\"}", command));
    }
}
