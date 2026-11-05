using System.Text;
using System.Text.Json;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public static class KafdeckCompensationEndpoints
{
    public static WebApplication MapKafdeckCompensation(this WebApplication app)
    {
        app.MapGet("/api/v1/declarative/compensation/catalog", (HttpContext context, KafdeckAuthorizationService policy, IConfiguration config) =>
        {
            if (!MutationActor.TryCreate(context.User,out _)) return Results.Problem(statusCode:401,title:"Authentication required");
            if (policy.AuthorizeCollection(context.User,AuthorizationAction.DeclarativeRead,config["Kafdeck:Declarative:DeploymentId"]) != KafdeckAuthorizationOutcome.Allowed)
                return Results.Problem(statusCode:403,title:"Compensation catalog access denied");
            return Results.Ok(new { catalogVersion=1, family="NotificationSubscriptionStateToggle", executable=true,
                states=new[]{"Active","Paused"}, freshGovernedPlanRequired=true, independentCriticalApproval=true,
                notCompensatable=new[]{"Create","Adopt","Delete","Retired","ConfigurationChange","DeliveredNotification","ExecutionUnknown","OtherResourceFamilies"} });
        }).WithName("v08-declarative-compensation-catalog").WithMetadata(new DeclarativeRouteContract(AuthorizationAction.DeclarativeRead));
        app.MapGet("/api/v1/declarative/plans/{sourcePlanId:guid}/compensation",
            (Guid sourcePlanId,HttpContext context,DeclarativeEndpointService service,CancellationToken token)=>service.ReadCompensationAsync(sourcePlanId,context,token))
            .WithName("v08-declarative-compensation-inspect")
            .WithMetadata(new DeclarativeRouteContract(AuthorizationAction.DeclarativeRead));
        app.MapPost("/api/v1/declarative/plans/{sourcePlanId:guid}/compensation-preview",
            (Guid sourcePlanId,DeclarativeManifestRequest request,HttpContext context,DeclarativeEndpointService service,CancellationToken token)=>
                service.HandleAsync("plan",request,context,token,sourcePlanId))
            .WithName("v08-declarative-compensation-preview")
            .WithMetadata(new DeclarativeRouteContract(AuthorizationAction.DeclarativePlan,typeof(DeclarativeManifestRequest)))
            .RequireKafdeckAntiforgery();
        return app;
    }

}

public static class DeclarativeCompensationAuthority
{
    public static async Task<bool> IsVerifiedAsync(Guid sourceId,IMutationOperationRepository? operations,IW68RuntimeStore store,CancellationToken token)
    {
        if (operations is null) return false;
        var plan=await store.GetPlanAsync(sourceId,token).ConfigureAwait(false);
        return plan is not null && await IsVerifiedPlanAsync(plan,operations,token).ConfigureAwait(false);
    }
    public static async Task<bool> IsVerifiedPlanAsync(W68StoredRuntimePlan plan,IMutationOperationRepository? operations,CancellationToken token)
    {
        if(operations is null) return false;
        var operation=await operations.GetAsync(plan.PlanId,token).ConfigureAwait(false);
        return operation is {State:MutationOperationState.AppliedVerified,OperationKind:MutationOperationKind.DeclarativeNotificationSubscription} &&
            operation.CanonicalIntent==DeclarativeSubscriptionMutation.Intent(plan).CanonicalIntent && W71CompensationProof.IsToggle(plan);
    }
}

public sealed partial class DeclarativeEndpointService
{
    public async Task<IResult> ReadCompensationAsync(Guid sourceId,HttpContext context,CancellationToken token)
    {
        var source=await store.GetPlanAsync(sourceId,token).ConfigureAwait(false);
        if(source is null) return Problem(404,"compensation_source_not_found");
        if(!CanRead(source,context)) return Problem(403,"compensation_source_read_denied");
        var operation=await operations.GetAsync(sourceId,token).ConfigureAwait(false);
        if(operation?.State==MutationOperationState.ExecutionUnknown)
            return Results.Ok(new{catalogVersion=1,sourceOperationId=sourceId,eligibility="BlockedByUnknownEffect",reason="resolve_original_outcome_first"});
        if(!await DeclarativeCompensationAuthority.IsVerifiedAsync(sourceId,operations,store,token).ConfigureAwait(false))
            return Results.Ok(new{catalogVersion=1,sourceOperationId=sourceId,eligibility="NotCompensatable",reason="no_verified_admitted_state_toggle"});
        var receipt=await store.GetCommitReceiptAsync(sourceId,token).ConfigureAwait(false);
        var owner=await store.GetOwnershipAsync(source.Key,token).ConfigureAwait(false);
        var current=await routing.GetSubscriptionAsync(source.Key.ResourceId,token).ConfigureAwait(false);
        if(receipt is null || !W71CompensationProof.CurrentMatches(source,receipt,owner,current) ||
            await store.GetDeploymentGenerationAsync(source.Key.DeploymentId,token).ConfigureAwait(false)!=source.DeploymentGeneration)
            return Results.Ok(new{catalogVersion=1,sourceOperationId=sourceId,eligibility="BlockedByDrift",reason="original_commit_no_longer_exact_current_state"});
        var inverse=InverseManifest(source);
        var sourceRegistration=await store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source,source.Key.DeploymentId,source.SourceId,token).ConfigureAwait(false);
        return Results.Ok(new{catalogVersion=1,sourceOperationId=sourceId,eligibility="RequiresFreshPlan",reason="fresh_inverse_plan_and_independent_approval_required",
            inverseManifest=Encoding.UTF8.GetString(inverse.ToCanonicalUtf8()),format="json",sourceRef=source.SourceId,sourceRevision=inverse.Fingerprint(),
            expectedOwnershipRevision=owner!.Revision,requiresSourceAdvance=sourceRegistration?.PinnedRevision!=inverse.Fingerprint()});
    }
    private async Task<bool> CompensationSourceCurrentAsync(Guid sourceId,DeclarativeSubscriptionManifest inverse,HttpContext context,CancellationToken token)
    {
        var source=await store.GetPlanAsync(sourceId,token).ConfigureAwait(false);
        if(source is null || !CanRead(source,context) || !await DeclarativeCompensationAuthority.IsVerifiedAsync(sourceId,operations,store,token).ConfigureAwait(false)) return false;
        var expected=InverseManifest(source);
        var receipt=await store.GetCommitReceiptAsync(sourceId,token).ConfigureAwait(false);
        var owner=await store.GetOwnershipAsync(source.Key,token).ConfigureAwait(false);
        var current=await routing.GetSubscriptionAsync(source.Key.ResourceId,token).ConfigureAwait(false);
        return expected.Fingerprint()==inverse.Fingerprint() && receipt is not null && W71CompensationProof.CurrentMatches(source,receipt,owner,current) &&
            await store.GetDeploymentGenerationAsync(source.Key.DeploymentId,token).ConfigureAwait(false)==source.DeploymentGeneration;
    }
    private static DeclarativeSubscriptionManifest InverseManifest(W68StoredRuntimePlan source) => DeclarativeSubscriptionManifest.Parse(
        JsonSerializer.SerializeToUtf8Bytes(new {apiVersion=DeclarativeSubscriptionManifest.ApiVersion,kind="NotificationSubscription",
            metadata=new{deploymentRef=source.Key.DeploymentId,name=source.Key.ResourceId,ownerRef=source.OwnerId},
            spec=new{destinationRef=source.ObservedSubscription!.Definition.DestinationId,state=source.ObservedSubscription.State.ToString(),
                eventClasses=source.ObservedSubscription.Definition.EventClasses.Select(x=>x.ToString()).ToArray(),eventTypes=source.ObservedSubscription.Definition.EventTypes}}),"json");
}
