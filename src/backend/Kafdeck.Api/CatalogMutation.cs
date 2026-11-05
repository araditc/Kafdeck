using System.Text.Json;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public static class CatalogMutation
{
    public static MutationIntentDescriptor Intent(CatalogPlan plan) => new(MutationOperationKind.CatalogMutation, plan.ClusterId,
        JsonSerializer.Serialize(new CatalogMutationReference("catalog-v1", plan.PlanId, plan.ContentHash, plan.DeploymentGeneration), CatalogContract.Json),
        plan.Changes.Select(i => CatalogContract.Resource(i)).Distinct(StringComparer.Ordinal).OrderBy(i => i, StringComparer.Ordinal).ToArray(),
        [new("catalog.material", plan.ContentHash)],
        AuthorizationTargets: plan.Changes.Select(i => new MutationAuthorizationTarget(AuthorizationAction.CatalogManage, plan.ClusterId, CatalogContract.Resource(i))).Distinct().OrderBy(i => i.ResourceName, StringComparer.Ordinal).ToArray());
    public static MutationRiskDecision RiskFor(CatalogPlan plan) => MutationRiskClassifier.Classify(new(MutationOperationKind.CatalogMutation, plan.Changes.Count));
    public static Guid PlanId(MutationOperationSnapshot operation)
    {
        var reference = JsonSerializer.Deserialize<CatalogMutationReference>(operation.CanonicalIntent, CatalogContract.Json) ?? throw new ArgumentException("Invalid catalog mutation reference.");
        if (reference.Schema != "catalog-v1" || reference.PlanId != operation.OperationId) throw new ArgumentException("Invalid catalog operation binding.");
        return reference.PlanId;
    }
}
[System.Text.Json.Serialization.JsonUnmappedMemberHandling(System.Text.Json.Serialization.JsonUnmappedMemberHandling.Disallow)]
public sealed record CatalogMutationReference(string Schema, Guid PlanId, string ContentHash, string DeploymentGeneration);

public sealed class CatalogMutationHandler(AdoCatalogStore store, ICatalogTopicIdentityPort topics,
    MutationExecutionRequestContextAccessor requestContext, MutationRequestAuthorizationService authorization,
    KafdeckAuthorizationService catalogAuthorization, IGovernancePolicyTransactionGate? policyGate = null) : IMutationExecutionHandler
{
    public MutationOperationKind OperationKind => MutationOperationKind.CatalogMutation;
    public async Task<MutationPreDispatchGuardResult> ValidateAsync(MutationOperationSnapshot operation, CancellationToken token)
    {
        try
        {
            var plan = await LoadAsync(operation, token).ConfigureAwait(false);
            if (plan is null || !CurrentAllowed(plan, operation)) return new(MutationPreDispatchGuardOutcome.AuthorizationDenied, "CatalogCurrentAuthorityDenied");
            if (!await IdentityMatchesAsync(plan, token).ConfigureAwait(false)) return new(MutationPreDispatchGuardOutcome.StalePreview, "CatalogTopicIdentityChanged");
            await store.ValidateAsync(plan, token).ConfigureAwait(false);
            return MutationPreDispatchGuardResult.Allowed;
        }
        catch (CatalogConflictException ex) { return new(MutationPreDispatchGuardOutcome.StalePreview, ex.Code); }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or JsonException) { return new(MutationPreDispatchGuardOutcome.StalePreview, "CatalogPlanInvalid"); }
    }
    public async Task<MutationProviderResult> ExecuteAsync(MutationExecutionContext context, CancellationToken cancellationToken = default)
    {
        var plan = await LoadAsync(context.Operation, cancellationToken).ConfigureAwait(false);
        if (plan is null || !CurrentAllowed(plan, context.Operation) || !await IdentityMatchesAsync(plan, cancellationToken).ConfigureAwait(false))
            return new(MutationExecutionResultKind.FailedDefinitive, "CatalogCurrentAuthorityOrIdentityChanged");
        try
        {
            var receipt = await store.CommitWithPolicyAsync(plan, () => CurrentAllowed(plan, context.Operation),
                policyGate is null ? null : (db, tx, token) => policyGate.ValidateDispatchAsync(db, tx, context.Operation, token), cancellationToken).ConfigureAwait(false);
            return receipt is null ? new(MutationExecutionResultKind.FailedDefinitive, "CatalogCurrentAuthorityDenied") :
                new(MutationExecutionResultKind.AppliedVerified, "CatalogApplied", new Dictionary<string, string>(StringComparer.Ordinal)
                { ["catalog.plan"] = plan.PlanId.ToString("D"), ["catalog.content-hash"] = plan.ContentHash, ["catalog.changed"] = receipt.Records.Count.ToString(System.Globalization.CultureInfo.InvariantCulture) });
        }
        catch (CatalogConflictException ex) { return new(MutationExecutionResultKind.FailedDefinitive, ex.Code); }
        catch (GovernanceGenerationException) { return new(MutationExecutionResultKind.FailedDefinitive, "CatalogRestoreGenerationChanged"); }
        catch (GovernanceEffectRejectedException ex) { return new(MutationExecutionResultKind.FailedDefinitive, ex.Code); }
        catch
        {
            // A lost DB commit acknowledgment is unknown until the immutable receipt is positively read.
            return new(MutationExecutionResultKind.ExecutionUnknown, "CatalogCommitUnknown");
        }
    }
    private async Task<CatalogPlan?> LoadAsync(MutationOperationSnapshot operation, CancellationToken token)
    {
        if (operation.OperationKind != OperationKind) return null;
        var plan = await store.GetPlanAsync(CatalogMutation.PlanId(operation), token).ConfigureAwait(false);
        if (plan is null) return null;
        CatalogContract.ValidatePlan(plan);
        var intent = CatalogMutation.Intent(plan);
        if (operation.ClusterId != plan.ClusterId || operation.RequesterPrincipalId != plan.RequesterPrincipalId ||
            operation.CanonicalIntent != intent.CanonicalIntent || !operation.ResourceKeys.SequenceEqual(intent.ResourceKeys) ||
            !operation.AuthorizationTargets.SequenceEqual(intent.AuthorizationTargets!) || operation.MaterialDigests.Count != 0 ||
            !operation.Preconditions.Contains(new MutationPrecondition("catalog.material", plan.ContentHash)) ||
            operation.Risk.RiskClass != CatalogMutation.RiskFor(plan).RiskClass || operation.Risk.RequiresIndependentApproval != CatalogMutation.RiskFor(plan).RequiresIndependentApproval ||
            operation.Risk.ConfirmationMode != CatalogMutation.RiskFor(plan).ConfirmationMode) return null;
        return plan;
    }
    private bool CurrentAllowed(CatalogPlan plan, MutationOperationSnapshot operation)
    {
        var principal = requestContext.CurrentPrincipal;
        return MutationActor.TryCreate(principal, out var actor) && actor is not null && actor.PrincipalId == plan.RequesterPrincipalId &&
            plan.AuthorityFingerprint == catalogAuthorization.PolicyFingerprint && plan.ExpiresAtUtc > DateTimeOffset.UtcNow &&
            authorization.AuthorizeForDispatch(principal, operation) == KafdeckAuthorizationOutcome.Allowed &&
            plan.Changes.All(i => catalogAuthorization.Authorize(principal, new(AuthorizationAction.CatalogRead, plan.ClusterId, CatalogContract.Resource(i))) == KafdeckAuthorizationOutcome.Allowed &&
                (i.Change.TeamId is null || catalogAuthorization.Authorize(principal, new(AuthorizationAction.CatalogRead, plan.ClusterId, "catalog:team:" + i.Change.TeamId)) == KafdeckAuthorizationOutcome.Allowed) &&
                (i.Change.ApplicationId is null || catalogAuthorization.Authorize(principal, new(AuthorizationAction.CatalogRead, plan.ClusterId, "catalog:application:" + i.Change.ApplicationId)) == KafdeckAuthorizationOutcome.Allowed));
    }
    private async Task<bool> IdentityMatchesAsync(CatalogPlan plan, CancellationToken token)
    {
        foreach (var item in plan.Changes.Where(i => i.Change.Kind == CatalogEntityKind.Topic && !i.Change.Archived))
            if ((await topics.ObserveAsync(plan.ClusterId, item.Change.TopicName!, token).ConfigureAwait(false))?.Fingerprint != item.TopicIdentity) return false;
        return true;
    }
}
