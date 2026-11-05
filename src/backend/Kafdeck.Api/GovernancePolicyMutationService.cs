using System.Text.Json;
using Kafdeck.Core.Governance;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public sealed record PolicyMutationMaterial(string VersionId, string ContentHash, long ExpectedRevision = 0, bool Enabled = false);

public sealed class GovernancePolicyMutationService(
    AdoGovernancePolicyStore store, MutationExecutionRequestContextAccessor context,
    MutationRequestAuthorizationService authorization,KafdeckAuthorizationService currentAuthorization)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    public static PolicyMutationMaterial Material(MutationOperationSnapshot operation) =>
        JsonSerializer.Deserialize<PolicyMutationMaterial>(operation.CanonicalIntent,Json) ?? throw new ArgumentException("Invalid policy material.");

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(MutationOperationSnapshot operation, CancellationToken token)
    {
        var current = MutationCurrentRequesterAuthorization.Evaluate(context,authorization,operation);
        if (current.Outcome != MutationPreDispatchGuardOutcome.Allowed) return current;
        var material = Material(operation);
        var version = await store.GetVersionAsync(operation.ClusterId,material.VersionId,operation.OperationKind == MutationOperationKind.GovernancePolicyVersion,token).ConfigureAwait(false);
        if (version is null || version.ContentHash != material.ContentHash) return new(MutationPreDispatchGuardOutcome.StalePreview,"PolicyVersionConflict");
        if (operation.OperationKind == MutationOperationKind.GovernancePolicyActivation)
        {
            var state = await store.GetStateAsync(operation.ClusterId,token).ConfigureAwait(false);
            if (state.Revision != material.ExpectedRevision) return new(MutationPreDispatchGuardOutcome.StalePreview,"PolicyVersionConflict");
            if (operation.Risk.RiskClass != MutationRiskClass.Critical || !operation.Risk.RequiresIndependentApproval ||
                string.IsNullOrEmpty(operation.ApprovedByPrincipalId) || operation.ApprovedByPrincipalId == operation.RequesterPrincipalId ||
                string.IsNullOrEmpty(operation.ApprovalAuthorizationEvidenceHash) || operation.ApprovedAtUtc is null ||
                operation.AuthorizationTargets.Any(t=>!currentAuthorization.IsCurrentGovernanceApprovalAllowed(operation.ApprovedByPrincipalId,new(t.Action,t.ClusterId,t.ResourceName))))
                return new(MutationPreDispatchGuardOutcome.AuthorizationDenied,"PolicyCriticalApprovalRequired");
        }
        return MutationPreDispatchGuardResult.Allowed;
    }

    public async Task<MutationProviderResult> ExecuteAsync(MutationExecutionContext execution,CancellationToken token)
    {
        var operation=execution.Operation;
        var material=Material(operation);
        bool Current()=>MutationCurrentRequesterAuthorization.Evaluate(context,authorization,operation).Outcome==MutationPreDispatchGuardOutcome.Allowed&&
            (!operation.Risk.RequiresIndependentApproval||!string.IsNullOrEmpty(operation.ApprovedByPrincipalId)&&
                operation.ApprovedByPrincipalId!=operation.RequesterPrincipalId&&operation.ApprovedAtUtc is not null&&!string.IsNullOrEmpty(operation.ApprovalAuthorizationEvidenceHash)&&
                operation.AuthorizationTargets.All(t=>currentAuthorization.IsCurrentGovernanceApprovalAllowed(operation.ApprovedByPrincipalId,new(t.Action,t.ClusterId,t.ResourceName))));
        try
        {
            return operation.OperationKind == MutationOperationKind.GovernancePolicyVersion
                ? await store.CommitVersionAsync(operation,material.VersionId,material.ContentHash,Current,token).ConfigureAwait(false)
                : await store.ActivateAsync(operation,material.VersionId,material.ContentHash,material.ExpectedRevision,material.Enabled,Current,token).ConfigureAwait(false);
        }
        catch(GovernanceEffectRejectedException ex){return new(MutationExecutionResultKind.FailedDefinitive,ex.Code);}
    }
}

public sealed class GovernancePolicyExecutionHandler(MutationOperationKind kind, GovernancePolicyMutationService service) : IMutationExecutionHandler
{
    public MutationOperationKind OperationKind { get; } = kind;
    public Task<MutationProviderResult> ExecuteAsync(MutationExecutionContext context,CancellationToken token=default)=>service.ExecuteAsync(context,token);
}

public sealed class GovernanceMutationPreDispatchGuard(IMutationPreDispatchGuard inner,GovernancePolicyRuntime runtime,GovernancePolicyMutationService service,
    ServiceRequestMutationService? requests=null,AttributionMutationService? attribution=null) : IMutationPreDispatchGuard
{
    public async Task<MutationPreDispatchGuardResult> ValidateAsync(MutationOperationSnapshot operation,CancellationToken token=default)
    {
        var prior=operation.OperationKind switch
        {
            MutationOperationKind.GovernancePolicyVersion or MutationOperationKind.GovernancePolicyActivation => await service.ValidateAsync(operation,token).ConfigureAwait(false),
            MutationOperationKind.ServiceRequestChange or MutationOperationKind.ServiceRequestDecision => requests is null
                ? new(MutationPreDispatchGuardOutcome.CapabilityUnsupported,"RequestStoreUnavailable") : await requests.ValidateAsync(operation,token).ConfigureAwait(false),
            MutationOperationKind.RateCardVersion or MutationOperationKind.RateCardActivation => attribution is null
                ? new(MutationPreDispatchGuardOutcome.CapabilityUnsupported,"AttributionUnavailable") : await attribution.ValidateAsync(operation,token).ConfigureAwait(false),
            _ => await inner.ValidateAsync(operation,token).ConfigureAwait(false),
        };
        return prior.Outcome==MutationPreDispatchGuardOutcome.Allowed ? await runtime.ValidateAsync(operation,token).ConfigureAwait(false) : prior;
    }
}
