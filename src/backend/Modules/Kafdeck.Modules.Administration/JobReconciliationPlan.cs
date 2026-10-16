using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Administration;

/// <summary>A reviewable no-effect decision. Neither an operator assertion nor a missing record is provider proof.</summary>
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record JobReconciliationPlan(int Version, Guid ParentOperationId, MutationOperationKind ParentKind,
    string ParentPreviewHash, string ParentPolicyVersion, string RequesterPrincipalId, Guid BatchId,
    long ExpectedProgressVersion, long WorkerGeneration, DestinationDispatchBoundary BeforeDispatch,
    DestinationDispatchBoundary ReviewedObservation, string ProofReferenceSha256, string FencedWorkerEvidenceSha256)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    { UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow, MaxDepth = 16 };

    public JobReconciliationPlan Validate()
    {
        if (Version != 1 || ParentOperationId == Guid.Empty || BatchId == Guid.Empty ||
            ParentKind is not (MutationOperationKind.DataJob or MutationOperationKind.DataGenerator) ||
            ExpectedProgressVersion < 0 || WorkerGeneration <= 0 || !IsHash(ParentPreviewHash) ||
            !IsHash(ProofReferenceSha256) || !IsHash(FencedWorkerEvidenceSha256) ||
            string.IsNullOrWhiteSpace(ParentPolicyVersion) || ParentPolicyVersion.Length > 256 ||
            string.IsNullOrWhiteSpace(RequesterPrincipalId) || RequesterPrincipalId.Length > 4096 ||
            BeforeDispatch is null || ReviewedObservation is null)
            throw new MutationStateException("Job reconciliation plan is invalid.");
        BeforeDispatch.Validate();
        ReviewedObservation.Validate();
        if (BeforeDispatch.WorkerGeneration > WorkerGeneration || ReviewedObservation.WorkerGeneration != WorkerGeneration ||
            !BeforeDispatch.SameDestinationAndOffsets(ReviewedObservation) ||
            ReviewedObservation.ObservedAtUtc < BeforeDispatch.ObservedAtUtc)
            throw new MutationStateException("Job reconciliation has no unchanged destination boundary.");
        return this;
    }

    public string Serialize() => JsonSerializer.Serialize(Validate(), Json);
    public static JobReconciliationPlan Deserialize(string canonical)
    {
        if (string.IsNullOrEmpty(canonical) || canonical.Length > 32 * 1024)
            throw new MutationStateException("Job reconciliation plan is invalid.");
        try { return (JsonSerializer.Deserialize<JobReconciliationPlan>(canonical, Json) ?? throw new MutationStateException("Missing reconciliation plan.")).Validate(); }
        catch (JsonException) { throw new MutationStateException("Job reconciliation plan is invalid."); }
    }
    public static bool IsHash(string? value) => value is { Length: 64 } && value.All(c => char.IsAsciiDigit(c) || c is >= 'a' and <= 'f');

    public IReadOnlyList<MutationAuthorizationTarget> Targets(MutationOperationSnapshot parent) =>
        parent.AuthorizationTargets.Append(new(AuthorizationAction.MutationReconcile, BeforeDispatch.ClusterId, BeforeDispatch.TopicName))
            .Distinct().OrderBy(target => target.Action).ThenBy(target => target.ClusterId, StringComparer.Ordinal)
            .ThenBy(target => target.ResourceName, StringComparer.Ordinal).ToArray();

    public MutationIntentDescriptor Intent(MutationOperationSnapshot parent) => new(
        MutationOperationKind.FleetUncertaintyDisposition, parent.ClusterId, Serialize(),
        [$"job-reconciliation/{ParentOperationId:D}/{BatchId:D}"],
        [new("destination-before-dispatch", BeforeDispatch.Fingerprint)], AuthorizationTargets: Targets(parent));

    public static DestinationDispatchBoundary? UnresolvedBoundary(FleetOperationProgressSnapshot progress, Guid batchId)
    {
        _ = FleetOperationProgress.Restore(progress);
        if (progress.SchemaVersion != FleetOperationProgressSnapshot.CurrentSchemaVersion) return null;
        if (progress.Generator?.PendingBatch is { State: FleetGeneratorBatchState.DispatchStarted } generator && generator.BatchId == batchId)
            return generator.DestinationBoundary;
        if (progress.Transfer?.PendingBatch is { State: FleetTransferBatchState.DispatchStarted } transfer && transfer.BatchId == batchId)
            return transfer.DestinationBoundary;
        return null;
    }

    public bool Matches(MutationOperationSnapshot parent, FleetOperationProgressSnapshot progress, DateTimeOffset now) =>
        parent.OperationId == ParentOperationId && parent.OperationKind == ParentKind && parent.State == MutationOperationState.AppliedVerified &&
        parent.PreviewHash == ParentPreviewHash && parent.PolicyVersion == ParentPolicyVersion && parent.RequesterPrincipalId == RequesterPrincipalId &&
        progress.OperationId == ParentOperationId && progress.Version == ExpectedProgressVersion && progress.WorkerGeneration == WorkerGeneration &&
        (progress.WorkerLeaseExpiresAtUtc is null || progress.WorkerLeaseExpiresAtUtc <= now) &&
        UnresolvedBoundary(progress, BatchId)?.Fingerprint == BeforeDispatch.Fingerprint;
}
