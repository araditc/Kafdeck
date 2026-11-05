using System.Text.Json;
using Kafdeck.Core.Attribution;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public sealed record RateCardMutationMaterial(string VersionId,string ContentHash,long ExpectedRevision=0,bool Enabled=false);
public sealed class AttributionMutationService(AdoAttributionStore store,MutationExecutionRequestContextAccessor context,MutationRequestAuthorizationService authorization)
{
    public static RateCardMutationMaterial Material(MutationOperationSnapshot op)=>JsonSerializer.Deserialize<RateCardMutationMaterial>(op.CanonicalIntent,AttributionContract.Json)??throw new ArgumentException("Invalid rate material.");
    public async Task<MutationPreDispatchGuardResult> ValidateAsync(MutationOperationSnapshot op,CancellationToken token)
    {
        var current=MutationCurrentRequesterAuthorization.Evaluate(context,authorization,op);if(current.Outcome!=MutationPreDispatchGuardOutcome.Allowed)return current;
        var material=Material(op);var version=await store.GetRateAsync(op.ClusterId,material.VersionId,op.OperationKind==MutationOperationKind.RateCardVersion,token).ConfigureAwait(false);
        if(version?.ContentHash!=material.ContentHash)return new(MutationPreDispatchGuardOutcome.StalePreview,"RateCardVersionConflict");
        try{await store.ValidateRateReferencesAsync(version,token).ConfigureAwait(false);}
        catch(AttributionConflictException ex){return new(MutationPreDispatchGuardOutcome.StalePreview,ex.Code);}
        if(op.OperationKind==MutationOperationKind.RateCardActivation&&(await store.GetStateAsync(op.ClusterId,token).ConfigureAwait(false)).Revision!=material.ExpectedRevision)return new(MutationPreDispatchGuardOutcome.StalePreview,"RateCardVersionConflict");
        return MutationPreDispatchGuardResult.Allowed;
    }
    public async Task<MutationProviderResult> ExecuteAsync(MutationExecutionContext execution,CancellationToken token)
    {
        var op=execution.Operation;var m=Material(op);
        bool Current()=>MutationCurrentRequesterAuthorization.Evaluate(context,authorization,op).Outcome==MutationPreDispatchGuardOutcome.Allowed;
        try{return await store.CommitRateAsync(op,m.VersionId,m.ContentHash,m.ExpectedRevision,m.Enabled,Current,token).ConfigureAwait(false);}
        catch(GovernanceEffectRejectedException ex){return new(MutationExecutionResultKind.FailedDefinitive,ex.Code);}
        catch(AttributionConflictException ex){return new(MutationExecutionResultKind.FailedDefinitive,ex.Code);}
    }
}
public sealed class AttributionExecutionHandler(MutationOperationKind kind,AttributionMutationService service):IMutationExecutionHandler
{
    public MutationOperationKind OperationKind{get;}=kind;
    public Task<MutationProviderResult> ExecuteAsync(MutationExecutionContext context,CancellationToken cancellationToken=default)=>service.ExecuteAsync(context,cancellationToken);
}
