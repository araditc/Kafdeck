using System.Security.Claims;
using System.Text.Json.Serialization;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Generator;
using Kafdeck.Modules.Records;

namespace Kafdeck.Api;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record JobReconciliationPreviewRequest(Guid BatchId, long ExpectedProgressVersion,
    string ProofReferenceSha256, string FencedWorkerEvidenceSha256);

public sealed class JobReconciliationService(IMutationOperationRepository operations, IFleetMutationStateStore fleet,
    IDestinationDispatchObserver observer, MutationRequestAuthorizationService authorization,
    MutationExecutionRequestContextAccessor executionContext, TimeProvider? timeProvider = null) : IMutationExecutionHandler
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    public MutationOperationKind OperationKind => MutationOperationKind.FleetUncertaintyDisposition;

    public async Task<IResult> PreviewAsync(Guid parentId, MutationOperationKind expectedKind, JobReconciliationPreviewRequest request,
        HttpContext context, MutationAdmissionService admission, CancellationToken token)
    {
        var parent = await operations.GetAsync(parentId, token).ConfigureAwait(false);
        if (parent is null) return Problem(404, "reconciliation_parent_not_found");
        if (!OwnerAllowed(context.User, parent)) return Problem(403, "reconciliation_owner_denied");
        if (parent.OperationKind != expectedKind) return Problem(403, "reconciliation_parent_kind_denied");
        // Authorize parent-scoped targets before processing user-controlled reconciliation input.
        if (authorization.AuthorizeProofTargets(context.User, parent.AuthorizationTargets) != KafdeckAuthorizationOutcome.Allowed)
            return Problem(403, "reconciliation_authority_denied");
        // Caller fields validate the requested evidence, never the authenticated authority.
        if (request.BatchId == Guid.Empty || request.ExpectedProgressVersion < 0 ||
            !JobReconciliationPlan.IsHash(request.ProofReferenceSha256) || !JobReconciliationPlan.IsHash(request.FencedWorkerEvidenceSha256))
            return Problem(400, "reconciliation_proof_invalid");
        var progress = await fleet.GetProgressAsync(parentId, token).ConfigureAwait(false);
        if (progress is null) return Problem(409, "reconciliation_progress_unavailable");
        var before = JobReconciliationPlan.UnresolvedBoundary(progress, request.BatchId);
        if (before is null || progress.Version != request.ExpectedProgressVersion ||
            progress.WorkerLeaseExpiresAtUtc > _time.GetUtcNow() || parent.State != MutationOperationState.AppliedVerified)
            return Problem(409, "reconciliation_boundary_or_fence_unavailable");
        // Authorize the exact reconciliation-specific destination *before*
        // making even a read-only provider observation. The parent grant alone
        // does not grant MutationReconcile over this topic. Recheck below after
        // observation because the authorization policy can be reloaded.
        var earlyTargets = parent.AuthorizationTargets
            .Append(new MutationAuthorizationTarget(AuthorizationAction.MutationReconcile,
                before.ClusterId, before.TopicName)).Distinct().ToArray();
        if (authorization.AuthorizeProofTargets(context.User, earlyTargets) != KafdeckAuthorizationOutcome.Allowed)
            return Problem(403, "reconciliation_authority_denied");
        var observation = await observer.ObserveAsync(before.ClusterId, before.TopicName, before.Partition, progress.WorkerGeneration, token).ConfigureAwait(false);
        if (!observation.IsSuccess || !before.SameDestinationAndOffsets(observation.Boundary!))
            return Problem(409, "reconciliation_destination_not_unchanged");
        JobReconciliationPlan plan;
        try
        {
            plan = new JobReconciliationPlan(1, parentId, expectedKind, parent.PreviewHash, parent.PolicyVersion,
                parent.RequesterPrincipalId, request.BatchId, progress.Version, progress.WorkerGeneration,
                before, observation.Boundary!, request.ProofReferenceSha256, request.FencedWorkerEvidenceSha256).Validate();
        }
        catch (MutationStateException) { return Problem(409, "reconciliation_boundary_invalid"); }
        if (!OwnerAllowed(context.User, parent)) return Problem(403, "reconciliation_owner_denied");
        if (authorization.AuthorizeProofTargets(context.User, plan.Targets(parent)) != KafdeckAuthorizationOutcome.Allowed)
            return Problem(403, "reconciliation_authority_denied");
        if (!plan.Matches(parent, progress, _time.GetUtcNow())) return Problem(409, "reconciliation_progress_changed");
        return await KafdeckTopicMutationEndpoints.AdmitAsync(context, admission, plan.Intent(parent),
            MutationRiskClassifier.Classify(new(MutationOperationKind.FleetUncertaintyDisposition)), token,
            id => context.User.Identity is NonBrowserReadIdentity ? $"/api/v1/automation/mutations/{id:D}/status" : $"/api/v1/mutations/{id:D}")
            .ConfigureAwait(false);
    }

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(MutationOperationSnapshot operation, CancellationToken token = default)
    {
        var checkedState = await CheckAsync(operation, token).ConfigureAwait(false);
        return checkedState.Code is null ? MutationPreDispatchGuardResult.Allowed :
            new(MutationPreDispatchGuardOutcome.StalePreview, checkedState.Code);
    }

    public async Task<MutationProviderResult> ExecuteAsync(MutationExecutionContext context, CancellationToken cancellationToken = default)
    {
        // The common executor has claimed this exact reviewed decision. Re-observe here
        // as well as in its pre-dispatch guard; no token/session is reconstructed.
        var checkedState = await CheckAsync(context.Operation, cancellationToken).ConfigureAwait(false);
        if (checkedState.Code is not null) return new(MutationExecutionResultKind.FailedBeforeDispatch, checkedState.Code);
        var plan = checkedState.Plan!;
        var progress = checkedState.Progress!;
        FleetOperationProgressSnapshot next;
        try
        {
            next = plan.ParentKind == MutationOperationKind.DataGenerator
                ? DataGeneratorProgress.ResolveProvenNonApplicationAndFence(progress, plan.BatchId, _time.GetUtcNow())
                : GovernedDataJobProgress.ResolveProvenNonApplicationAndFence(progress, plan.BatchId, _time.GetUtcNow());
        }
        catch (MutationStateException) { return new(MutationExecutionResultKind.FailedBeforeDispatch, "reconciliation_batch_changed"); }
        // A final live authority check follows all provider observations, immediately
        // before the only durable write. The expected progress version is immutable.
        if (authorization.AuthorizeForDispatch(executionContext.CurrentPrincipal, context.Operation) != KafdeckAuthorizationOutcome.Allowed ||
            !OwnerAllowed(executionContext.CurrentPrincipal, checkedState.Parent!))
            return new(MutationExecutionResultKind.FailedBeforeDispatch, "reconciliation_authority_changed");
        FleetProgressSaveResult saved;
        try { saved = await fleet.TrySaveProgressAsync(next, plan.ExpectedProgressVersion, cancellationToken).ConfigureAwait(false); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch { return new(MutationExecutionResultKind.ExecutionUnknown, "reconciliation_persistence_unknown"); }
        if (saved.Outcome != FleetProgressSaveOutcome.Saved)
            return new(MutationExecutionResultKind.FailedBeforeDispatch, "reconciliation_progress_conflict");
        return new(MutationExecutionResultKind.AppliedVerified, "job_non_application_reconciled", new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["reconciliation.parent"] = plan.ParentOperationId.ToString("D"),
            ["reconciliation.batch"] = plan.BatchId.ToString("D"),
            ["reconciliation.before"] = plan.BeforeDispatch.Fingerprint,
            ["reconciliation.proof"] = plan.ProofReferenceSha256,
            ["reconciliation.worker-fence"] = plan.FencedWorkerEvidenceSha256,
            ["reconciliation.generation"] = next.WorkerGeneration.ToString(System.Globalization.CultureInfo.InvariantCulture),
        });
    }

    private bool OwnerAllowed(ClaimsPrincipal? principal, MutationOperationSnapshot parent) =>
        MutationActor.TryCreate(principal, out var actor) && actor?.PrincipalId == parent.RequesterPrincipalId &&
        authorization.AuthorizeProofTargets(principal, parent.AuthorizationTargets) == KafdeckAuthorizationOutcome.Allowed;

    private async Task<(JobReconciliationPlan? Plan, MutationOperationSnapshot? Parent, FleetOperationProgressSnapshot? Progress, string? Code)>
        CheckAsync(MutationOperationSnapshot operation, CancellationToken token)
    {
        JobReconciliationPlan plan;
        try { plan = JobReconciliationPlan.Deserialize(operation.CanonicalIntent); }
        catch (MutationStateException) { return (null, null, null, "reconciliation_plan_invalid"); }
        if (operation.OperationKind != OperationKind || !operation.Risk.RequiresIndependentApproval || operation.Risk.RiskClass != MutationRiskClass.Critical ||
            operation.RequesterPrincipalId != plan.RequesterPrincipalId ||
            authorization.AuthorizeForDispatch(executionContext.CurrentPrincipal, operation) != KafdeckAuthorizationOutcome.Allowed)
            return (null, null, null, "reconciliation_authority_denied");
        var parent = await operations.GetAsync(plan.ParentOperationId, token).ConfigureAwait(false);
        var progress = await fleet.GetProgressAsync(plan.ParentOperationId, token).ConfigureAwait(false);
        if (parent is null || progress is null || !plan.Matches(parent, progress, _time.GetUtcNow()) || !OwnerAllowed(executionContext.CurrentPrincipal, parent))
            return (null, null, null, "reconciliation_parent_or_progress_changed");
        var intent = plan.Intent(parent);
        if (!operation.AuthorizationTargets.SequenceEqual(MutationAuthorizationRequirements.Normalize(OperationKind, parent.ClusterId, intent.AuthorizationTargets, intent.ResourceKeys)) ||
            !operation.ResourceKeys.SequenceEqual(intent.ResourceKeys) || operation.ClusterId != parent.ClusterId)
            return (null, null, null, "reconciliation_contract_changed");
        var before = plan.BeforeDispatch;
        var observation = await observer.ObserveAsync(before.ClusterId, before.TopicName, before.Partition, plan.WorkerGeneration, token).ConfigureAwait(false);
        if (!observation.IsSuccess || !before.SameDestinationAndOffsets(observation.Boundary!))
            return (null, null, null, "reconciliation_destination_not_unchanged");
        return (plan, parent, progress, null);
    }

    private static IResult Problem(int status, string code) => Results.Problem(statusCode: status,
        type: "urn:kafdeck:problem:" + code, title: "Governed reconciliation could not establish safe non-application.");
}

public static class KafdeckJobReconciliationEndpoints
{
    public static WebApplication MapKafdeckJobReconciliationEndpoints(this WebApplication app, bool generator)
    {
        foreach (var bearer in new[] { false, true })
        {
            var original = generator ? "v08-data-generator-reconcile-preview" : "v08-data-job-reconcile-preview";
            var path = generator ? "/api/v1/data-jobs/generator/{operationId:guid}/reconcile/preview" : "/api/v1/data-jobs/{operationId:guid}/reconcile/preview";
            var kind = generator ? MutationOperationKind.DataGenerator : MutationOperationKind.DataJob;
            app.MapPost(BearerFamilyRoutes.Path(bearer, path), (Guid operationId, JobReconciliationPreviewRequest request,
                HttpContext context, [Microsoft.AspNetCore.Mvc.FromServices] JobReconciliationService reconciliation,
                [Microsoft.AspNetCore.Mvc.FromServices] MutationAdmissionService admission, CancellationToken token) =>
                    reconciliation.PreviewAsync(operationId, kind, request, context, admission, token))
                .WithName(BearerFamilyRoutes.Name(bearer, original)).RequireFamilyBoundary(bearer, original);
        }
        return app;
    }
}
