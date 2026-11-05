using System.Text.Json;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public sealed class ServiceRequestAuthority(KafdeckAuthorizationService authorization):IServiceRequestCurrentAuthority
{
    public bool IsCurrentRequesterAllowed(string principalId,AuthorizationRequest request)=>authorization.IsCurrentPrincipalAllowed(principalId,request);
    public bool IsCurrentHumanDecisionAllowed(string principalId,AuthorizationRequest request)=>authorization.IsCurrentRequestDecisionAllowed(principalId,request);
}
public static class ServiceRequestMutation
{
    public static MutationOperationKind Kind(ServiceRequestCommand command)=>ServiceRequestContract.IsDecision(command)?MutationOperationKind.ServiceRequestDecision:MutationOperationKind.ServiceRequestChange;
    public static AuthorizationAction Action(ServiceRequestCommand command)=>ServiceRequestContract.IsDecision(command)?AuthorizationAction.RequestDecide:AuthorizationAction.RequestSubmit;
    public static MutationIntentDescriptor Intent(ServiceRequestPlan plan)=>new(Kind(plan.Command),plan.ClusterId,
        JsonSerializer.Serialize(new ServiceRequestMutationReference("request-v1",plan.PlanId,plan.ContentHash,plan.DeploymentGeneration),ServiceRequestContract.Json),
        [ServiceRequestContract.Resource(plan.RequestId)],[new("request.material",plan.ContentHash)],
        AuthorizationTargets:[new(Action(plan.Command),plan.ClusterId,ServiceRequestContract.Resource(plan.RequestId))]);
    public static ServiceRequestMutationReference Reference(MutationOperationSnapshot operation)=>
        JsonSerializer.Deserialize<ServiceRequestMutationReference>(operation.CanonicalIntent,ServiceRequestContract.Json)??throw new ArgumentException("Invalid request reference.");
}
[System.Text.Json.Serialization.JsonUnmappedMemberHandling(System.Text.Json.Serialization.JsonUnmappedMemberHandling.Disallow)]
public sealed record ServiceRequestMutationReference(string Schema,Guid PlanId,string ContentHash,string DeploymentGeneration);

public sealed class ServiceRequestMutationService(AdoServiceRequestStore store,AdoGovernancePolicyStore policies,
    MutationExecutionRequestContextAccessor context,MutationRequestAuthorizationService authorization,KafdeckAuthorizationService current)
{
    public async Task<ServiceRequestPlan?> LoadAsync(MutationOperationSnapshot operation,CancellationToken token)
    {
        if(operation.OperationKind is not(MutationOperationKind.ServiceRequestChange or MutationOperationKind.ServiceRequestDecision))return null;
        var reference=ServiceRequestMutation.Reference(operation);
        if(reference.Schema!="request-v1"||reference.PlanId!=operation.OperationId)return null;
        var plan=await store.GetPlanAsync(reference.PlanId,token).ConfigureAwait(false);
        if(plan is null||reference.ContentHash!=plan.ContentHash||reference.DeploymentGeneration!=plan.DeploymentGeneration)return null;
        ServiceRequestContract.Validate(plan);var intent=ServiceRequestMutation.Intent(plan);
        return operation.OperationKind==ServiceRequestMutation.Kind(plan.Command)&&operation.ClusterId==plan.ClusterId&&operation.RequesterPrincipalId==plan.RequesterPrincipalId&&
            operation.CanonicalIntent==intent.CanonicalIntent&&operation.ResourceKeys.SequenceEqual(intent.ResourceKeys)&&operation.AuthorizationTargets.SequenceEqual(intent.AuthorizationTargets!)&&
            operation.MaterialDigests.Count==0&&operation.Preconditions.Contains(new("request.material",plan.ContentHash))&&operation.Risk.RiskClass==MutationRiskClass.Moderate&&
            !operation.Risk.RequiresIndependentApproval&&operation.Risk.ConfirmationMode==MutationConfirmationMode.Explicit?plan:null;
    }
    public bool Current(ServiceRequestPlan plan,MutationOperationSnapshot operation,bool allowExpiredReceipt=false)=>
        CurrentForPrincipal(plan,operation,context.CurrentPrincipal,allowExpiredReceipt);
    public bool CurrentForPrincipal(ServiceRequestPlan plan,MutationOperationSnapshot operation,System.Security.Claims.ClaimsPrincipal? principal,bool allowExpiredReceipt=false)
    {
        if(plan.AuthorityFingerprint!=current.PolicyFingerprint||!allowExpiredReceipt&&plan.ExpiresAtUtc<=DateTimeOffset.UtcNow||
            !MutationActor.TryCreate(principal,out var actor)||actor is null||actor.PrincipalId!=operation.RequesterPrincipalId||
            authorization.AuthorizeForDispatch(principal,operation)!=KafdeckAuthorizationOutcome.Allowed)return false;
        if(current.Authorize(principal,new(AuthorizationAction.RequestRead,plan.ClusterId,ServiceRequestContract.Resource(plan.RequestId)))!=KafdeckAuthorizationOutcome.Allowed)return false;
        if(ServiceRequestContract.IsDecision(plan.Command)&&!current.IsLiveRequestDecisionAllowed(principal,new(AuthorizationAction.RequestDecide,plan.ClusterId,ServiceRequestContract.Resource(plan.RequestId))))return false;
        return plan.Binding.AuthorizationTargets.All(t=>current.Authorize(principal,
            Visibility(plan.Binding,t))==KafdeckAuthorizationOutcome.Allowed);
    }
    public static AuthorizationRequest Visibility(ServiceRequestBinding binding,ServiceRequestTarget t)
    {
        var action=t.Action.ToString();
        var read=action.StartsWith("Topic",StringComparison.Ordinal)||action.StartsWith("Record",StringComparison.Ordinal)?AuthorizationAction.TopicRead:
            action.StartsWith("Consumer",StringComparison.Ordinal)?AuthorizationAction.ConsumerRead:action.StartsWith("Schema",StringComparison.Ordinal)?AuthorizationAction.SchemaRead:
            action.StartsWith("Connect",StringComparison.Ordinal)?AuthorizationAction.ConnectRead:action.StartsWith("Notification",StringComparison.Ordinal)?AuthorizationAction.NotificationRead:
            action.StartsWith("Declarative",StringComparison.Ordinal)?AuthorizationAction.DeclarativeRead:action.StartsWith("Catalog",StringComparison.Ordinal)?AuthorizationAction.CatalogRead:
            action.StartsWith("GovernancePolicy",StringComparison.Ordinal)?AuthorizationAction.GovernancePolicyRead:
            action.StartsWith("Request",StringComparison.Ordinal)?AuthorizationAction.RequestRead:action.StartsWith("RateCard",StringComparison.Ordinal)?AuthorizationAction.AttributionRead:AuthorizationAction.ClusterRead;
        return new(read,action.StartsWith("Notification",StringComparison.Ordinal)&&(binding.OperationKind==MutationOperationKind.DeclarativeNotificationSubscription.ToString()||t.ClusterId=="$deployment")?null:t.ClusterId,t.ResourceName);
    }
    public async Task<MutationPreDispatchGuardResult> ValidateAsync(MutationOperationSnapshot operation,CancellationToken token)
    {
        try{var plan=await LoadAsync(operation,token).ConfigureAwait(false);return plan is not null&&Current(plan,operation)?MutationPreDispatchGuardResult.Allowed:new(MutationPreDispatchGuardOutcome.AuthorizationDenied,"RequestCurrentAuthorityDenied");}
        catch(Exception ex)when(ex is ArgumentException or JsonException or InvalidOperationException){return new(MutationPreDispatchGuardOutcome.StalePreview,"RequestMaterialChanged");}
    }
    public async Task<MutationProviderResult> ExecuteAsync(MutationExecutionContext execution,CancellationToken token)
    {
        var operation=execution.Operation;var plan=await LoadAsync(operation,token).ConfigureAwait(false);
        if(plan is null||!Current(plan,operation))return new(MutationExecutionResultKind.FailedBeforeDispatch,"RequestCurrentAuthorityDenied");
        try
        {
            var receipt=await store.CommitAsync(plan,operation,()=>Current(plan,operation),policies.ValidateBindingDigestsAsync,token).ConfigureAwait(false);
            return new(MutationExecutionResultKind.AppliedVerified,"RequestApplied",new Dictionary<string,string>{
                ["request.id"]=Guid.ParseExact(receipt.RequestId,"N").ToString("D"),["request.hash"]=receipt.ContentHash,
                ["request.revision"]=receipt.Version.ToString(System.Globalization.CultureInfo.InvariantCulture),["request.receipt"]=receipt.OperationId.ToString("D")});
        }
        catch(ServiceRequestConflictException ex){return new(MutationExecutionResultKind.FailedDefinitive,ex.Code);}
        catch(GovernanceEffectRejectedException ex){return new(MutationExecutionResultKind.FailedDefinitive,ex.Code);}
        catch(GovernanceGenerationException){return new(MutationExecutionResultKind.FailedDefinitive,"RequestRestoreGenerationChanged");}
        catch(OperationCanceledException){throw;}
        catch{return new(MutationExecutionResultKind.ExecutionUnknown,"RequestCommitUnknown");}
    }
}
public sealed class ServiceRequestExecutionHandler(MutationOperationKind kind,ServiceRequestMutationService service):IMutationExecutionHandler
{
    public MutationOperationKind OperationKind {get;}=kind;
    public Task<MutationProviderResult> ExecuteAsync(MutationExecutionContext context,CancellationToken token=default)=>service.ExecuteAsync(context,token);
}
