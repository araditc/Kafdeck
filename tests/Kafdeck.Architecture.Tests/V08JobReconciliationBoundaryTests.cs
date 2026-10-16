using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Cli;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Generator;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08JobReconciliationBoundaryTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-10-16T12:00:00Z");
    private static readonly string Hash = new('a', 64);
    private static DestinationDispatchBoundary Boundary => new("cluster", "physical-cluster", "destination",
        "1234567890abcdef1234567890abcdef", 0, 0, 12, 2, Now.AddMinutes(-2));

    [Fact]
    public void Review_is_critical_and_keeps_exact_parent_effect_and_reconcile_conjunction()
    {
        var parent = Parent(); var progress = Progress(parent.OperationId); var plan = Plan(parent, progress);
        var restored = JobReconciliationPlan.Deserialize(plan.Serialize());
        Assert.Equal(plan, restored);
        Assert.True(restored.Matches(parent, progress, Now));
        var intent = restored.Intent(parent);
        Assert.Contains(intent.AuthorizationTargets!, t => t.Action == AuthorizationAction.RecordProduce && t.ResourceName == "destination");
        Assert.Contains(intent.AuthorizationTargets!, t => t.Action == AuthorizationAction.MutationReconcile && t.ResourceName == "destination");
        Assert.Single(intent.ResourceKeys);
        Assert.StartsWith("job-reconciliation/", intent.ResourceKeys[0], StringComparison.Ordinal);
        var risk = MutationRiskClassifier.Classify(new(MutationOperationKind.FleetUncertaintyDisposition));
        Assert.Equal(MutationRiskClass.Critical, risk.RiskClass); Assert.True(risk.RequiresIndependentApproval);
        Assert.Equal(MutationConfirmationMode.TypedTarget, risk.ConfirmationMode);
    }

    [Theory]
    [InlineData("append")][InlineData("truncate")][InlineData("recreate")][InlineData("cluster")][InlineData("partition")]
    public void Any_changed_identity_or_offset_boundary_retains_unknown(string mutation)
    {
        var before = Boundary;
        var changed = mutation switch
        {
            "append" => before with { HighWatermark = 13 },
            "truncate" => before with { LowWatermark = 1 },
            "recreate" => before with { TopicId = new string('b', 32) },
            "cluster" => before with { KafkaClusterId = "other" },
            _ => before with { Partition = 1 },
        };
        Assert.False(before.SameDestinationAndOffsets(changed));
        var parent = Parent(); var progress = Progress(parent.OperationId);
        Assert.Throws<MutationStateException>(() => (Plan(parent, progress) with { ReviewedObservation = changed }).Validate());
        Assert.NotNull(progress.Generator!.PendingBatch);
    }

    [Fact]
    public void Legacy_missing_observation_live_lease_and_changed_progress_cannot_be_cleared()
    {
        var parent = Parent(); var progress = Progress(parent.OperationId); var plan = Plan(parent, progress);
        Assert.False(plan.Matches(parent, progress with { Version = progress.Version + 1 }, Now));
        Assert.False(plan.Matches(parent, progress with { WorkerGeneration = 3 }, Now));
        Assert.False(plan.Matches(parent, progress with { WorkerLeaseOwner = "live", WorkerLeaseExpiresAtUtc = Now.AddSeconds(1) }, Now));
        Assert.False(plan.Matches(parent with { PreviewHash = new string('b', 64) }, progress, Now));
        var legacy = progress with { SchemaVersion = 1, Generator = progress.Generator! with
            { PendingBatch = progress.Generator!.PendingBatch! with { DestinationBoundary = null } } };
        Assert.Null(JobReconciliationPlan.UnresolvedBoundary(legacy, plan.BatchId));
        Assert.False(plan.Matches(parent, legacy, Now));
        Assert.Throws<MutationStateException>(() => FleetOperationProgress.Restore(progress with { SchemaVersion = 1 }));
        Assert.Throws<MutationStateException>(() => FleetOperationProgress.Restore(progress with { SchemaVersion = FleetOperationProgressSnapshot.CurrentSchemaVersion + 1 }));
    }

    [Fact]
    public void Approved_no_effect_transition_preserves_checkpoint_and_advances_generation_only_once()
    {
        var parent = Parent(); var progress = Progress(parent.OperationId); var plan = Plan(parent, progress);
        var resolved = DataGeneratorProgress.ResolveProvenNonApplicationAndFence(progress, plan.BatchId, Now);
        Assert.Null(resolved.Generator!.PendingBatch);
        Assert.Equal(progress.Generator!.NextRecordIndex, resolved.Generator.NextRecordIndex);
        Assert.Equal(0, resolved.Generator.AcknowledgedRecords);
        Assert.Equal(3, resolved.WorkerGeneration); Assert.Equal(progress.Version + 1, resolved.Version);
        Assert.Null(resolved.WorkerLeaseOwner); Assert.Null(resolved.WorkerLeaseExpiresAtUtc);
        Assert.Throws<MutationStateException>(() => DataGeneratorProgress.ResolveProvenNonApplicationAndFence(resolved, plan.BatchId, Now));
    }

    [Theory]
    [InlineData("data-jobs")][InlineData("generator")]
    public void Cli_proof_preview_is_named_idempotent_and_has_no_authority_overrides(string family)
    {
        var invocation = CliFamilyCommands.TryParse([family, "reconcile-preview", Guid.NewGuid().ToString("D")]);
        Assert.NotNull(invocation); Assert.True(invocation.Preview); Assert.Equal(CliFamilyPayload.ReconciliationProof, invocation.Payload);
        using var valid = JsonDocument.Parse("{\"batchId\":\"id\",\"expectedProgressVersion\":2,\"proofReferenceSha256\":\"hash\",\"fencedWorkerEvidenceSha256\":\"hash\"}");
        Assert.True(CliFamilyCommands.ValidatePayload(valid.RootElement, invocation.Payload));
        using var invalid = JsonDocument.Parse("{\"batchId\":\"id\",\"force\":true}");
        Assert.False(CliFamilyCommands.ValidatePayload(invalid.RootElement, invocation.Payload));
    }

    private static FleetOperationProgressSnapshot Progress(Guid id) => new()
    {
        OperationId = id, WorkerGeneration = 2, Version = 6, Phase = FleetProgressPhase.WaitingForExternalAction, UpdatedAtUtc = Now,
        Generator = new(Hash, 0, 0, 0, 2, new(Guid.Parse("7fd338fc-0895-4588-aa83-a216297564c4"), 0, 0, 12,
            FleetGeneratorBatchState.DispatchStarted, Now.AddMinutes(-2), Now.AddMinutes(-1), Boundary)),
    };
    private static JobReconciliationPlan Plan(MutationOperationSnapshot parent, FleetOperationProgressSnapshot progress) =>
        new(1, parent.OperationId, parent.OperationKind, parent.PreviewHash, parent.PolicyVersion, parent.RequesterPrincipalId,
            progress.Generator!.PendingBatch!.BatchId, progress.Version, progress.WorkerGeneration, Boundary,
            Boundary with { ObservedAtUtc = Now.AddSeconds(-1) }, Hash, Hash);
    private static MutationOperationSnapshot Parent() => new()
    {
        OperationId = Guid.NewGuid(), RequesterPrincipalId = "oidc:exact-parent", ClusterId = "cluster",
        OperationKind = MutationOperationKind.DataGenerator, State = MutationOperationState.AppliedVerified,
        Risk = MutationRiskClassifier.Classify(new(MutationOperationKind.DataGenerator)), CanonicalIntent = "{}", CanonicalIntentHash = Hash,
        ResourceKeys = ["destination"], Preconditions = [], MaterialDigests = [],
        AuthorizationTargets = [new(AuthorizationAction.RecordProduce, "cluster", "destination")], PreviewHash = Hash,
        PreviewExpiresAtUtc = Now, PolicyVersion = "policy", IdempotencyScope = Hash, IdempotencyKeyHash = Hash,
    };
}
