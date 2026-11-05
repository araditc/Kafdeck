using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Attribution;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.DataProtection;

namespace Kafdeck.Api;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record RateCardVersionPreviewRequest(string ClusterId,RateCardDocument Document,string? IdempotencyKey=null);
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record RateCardActivationPreviewRequest(string ClusterId,string VersionId,long ExpectedRevision,bool Enabled,string? IdempotencyKey=null);
public sealed record RateCardOperationReviewData(Guid OperationId,string PreviewHash,string ClusterId,string VersionId,string ContentHash,
    RateCardDocument Document,long ExpectedRevision,bool Enabled,MutationStatusData Operation);
internal sealed record AttributionCursor(string Scope,string Actor,string Authority,string AfterId,DateTimeOffset Expires);

public static class KafdeckAttributionEndpoints
{
    private const string Prefix="/api/v1/governance";
    public static WebApplication MapKafdeckAttribution(this WebApplication app)
    {
        foreach(var family in new[]{"measurements","attribution","dashboards"})
        {
            app.MapGet(Prefix+"/"+family,async(string clusterId,DateTimeOffset fromUtc,DateTimeOffset toUtc,string? resourceId,int? pageSize,string? cursor,
                HttpContext h,KafdeckAuthorizationService auth,IAttributionStore store,IDataProtectionProvider protection)=>await Safe(async()=>
            {
                if(auth.AuthorizeCollection(h.User,AuthorizationAction.AttributionRead,clusterId)!=KafdeckAuthorizationOutcome.Allowed)return Denied();
                var size=Size(pageSize);var query=new MeasurementQuery(clusterId,fromUtc.ToUniversalTime(),toUtc.ToUniversalTime(),resourceId,size);
                AttributionContract.Query(query);var scope=Scope(family,query);query=query with{AfterId=Decode(cursor,scope,h,auth,protection)};
                bool Visible(MeasurementAccessScope w)=>Allowed(h,auth,AuthorizationAction.AttributionRead,w.ClusterId,w.ResourceId)&&
                    (w.TeamId is null||Allowed(h,auth,AuthorizationAction.CatalogRead,w.ClusterId,"catalog:team:"+w.TeamId))&&
                    (w.ApplicationId is null||Allowed(h,auth,AuthorizationAction.CatalogRead,w.ClusterId,"catalog:application:"+w.ApplicationId));
                var found=await store.QueryAuthorizedAsync(query,Visible,h.RequestAborted).ConfigureAwait(false);
                var more=found.Count>size;var rows=found.Take(size).ToArray();
                var next=more?Encode(rows[^1].WindowId,scope,h,auth,protection):null;
                var coverage=Coverage(rows,query,more||cursor is not null);
                if(family=="measurements")return Results.Ok(new AttributionPage<MeasurementWindow>(rows,next,query.FromUtc,query.ToUtc,coverage,rows.Length==0?"EvidenceIncomplete":rows.Any(w=>w.Quantity is null)?"EvidenceIncomplete":"Available"));
                var result=new List<AttributionWindow>(rows.Length);
                foreach(var row in rows)
                {
                    var card=await store.FindActiveRateAuthorizedAsync(clusterId,row.MetricKind,row.WindowStartUtc,row.WindowEndUtc,v=>Allowed(h,auth,AuthorizationAction.AttributionRead,clusterId,v.VersionId)&&WeightVisibility(h,auth,clusterId,v.Document),h.RequestAborted).ConfigureAwait(false);
                    result.Add(AttributionCalculator.Allocate(row,card,DateTimeOffset.UtcNow));
                }
                var totals=AttributionCalculator.Totals(result,coverage=="AuthorizedResourceComplete");
                return Results.Ok(new AttributionPage<AttributionWindow>(result,next,query.FromUtc,query.ToUtc,coverage,totals.Status,totals));
            }).ConfigureAwait(false)).WithName("v09-"+family).WithMetadata(new GovernanceRouteContract(AuthorizationAction.AttributionRead,true,
                QueryKeys:"clusterId,fromUtc,toUtc,resourceId,pageSize,cursor",ResponseType:family=="measurements"?typeof(AttributionPage<MeasurementWindow>):typeof(AttributionPage<AttributionWindow>),RequiredQueryKeys:"clusterId,fromUtc,toUtc"));
        }
        app.MapGet(Prefix+"/rate-cards/versions",async(string clusterId,int? pageSize,string? cursor,HttpContext h,KafdeckAuthorizationService auth,
            IAttributionStore store,IDataProtectionProvider protection)=>await Safe(async()=>
        {
            if(auth.AuthorizeCollection(h.User,AuthorizationAction.AttributionRead,clusterId)!=KafdeckAuthorizationOutcome.Allowed)return Denied();
            var size=Size(pageSize);var scope="rate-cards\n"+clusterId;var after=Decode(cursor,scope,h,auth,protection);
            var rows=await store.ListRatesAuthorizedAsync(clusterId,after,size+1,v=>Allowed(h,auth,AuthorizationAction.AttributionRead,clusterId,v.VersionId)&&WeightVisibility(h,auth,clusterId,v.Document),h.RequestAborted).ConfigureAwait(false);
            return Results.Ok(new CatalogPage<RateCardVersion>(rows.Take(size).ToArray(),rows.Count>size?Encode(rows[size-1].VersionId,scope,h,auth,protection):null));
        }).ConfigureAwait(false)).WithName("v09-rate-card-versions").WithMetadata(new GovernanceRouteContract(AuthorizationAction.AttributionRead,true,ResponseType:typeof(CatalogPage<RateCardVersion>)));

        app.MapGet(Prefix+"/rate-cards/state",async(string clusterId,HttpContext h,KafdeckAuthorizationService auth,IAttributionStore store)=>await Safe(async()=>
        {
            if(!Allowed(h,auth,AuthorizationAction.AttributionRead,clusterId,"active"))return Denied();
            var state=await store.GetStateAsync(clusterId,h.RequestAborted).ConfigureAwait(false);
            if(state.VersionId is not null)
            {
                var version=await store.GetRateAsync(clusterId,state.VersionId,false,h.RequestAborted).ConfigureAwait(false);
                if(version is null||!Allowed(h,auth,AuthorizationAction.AttributionRead,clusterId,version.VersionId)||!WeightVisibility(h,auth,clusterId,version.Document))return Results.NotFound();
            }
            return Results.Ok(state);
        }).ConfigureAwait(false)).WithName("v09-rate-card-state").WithMetadata(new GovernanceRouteContract(AuthorizationAction.AttributionRead,true,QueryKeys:"clusterId",ResponseType:typeof(RateCardState)));

        app.MapPost(Prefix+"/rate-cards/versions/preview",async(RateCardVersionPreviewRequest request,HttpContext h,KafdeckAuthorizationService auth,
            IAttributionStore store,ICatalogStore catalog,MutationAdmissionService admission)=>await Safe(async()=>
        {
            var doc=AttributionContract.Rate(request.Document);var hash=AttributionContract.Hash(JsonSerializer.Serialize(doc,AttributionContract.Json));var id=AttributionContract.Hash(request.ClusterId+"\n"+hash);
            if(!Allowed(h,auth,AuthorizationAction.RateCardManage,request.ClusterId,id)||!MutationActor.TryCreate(h.User,out var actor))return Denied();
            _=MutationIdempotency.HashKey(Key(h,request.IdempotencyKey));
            await References(doc,request.ClusterId,h,auth,catalog,h.RequestAborted).ConfigureAwait(false);
            var version=await store.StageRateAsync(request.ClusterId,doc,actor!.PrincipalId,h.RequestAborted).ConfigureAwait(false);
            var intent=Intent(MutationOperationKind.RateCardVersion,request.ClusterId,version.VersionId,new(version.VersionId,version.ContentHash));
            return Map(await admission.AdmitAsync(h.User,intent,new(MutationRiskClass.High,["rate_card_version"],MutationConfirmationMode.TypedTarget,false),Key(h,request.IdempotencyKey),h.RequestAborted).ConfigureAwait(false));
        }).ConfigureAwait(false)).WithName("v09-rate-card-version-preview").WithMetadata(new GovernanceRouteContract(AuthorizationAction.RateCardManage,false,typeof(RateCardVersionPreviewRequest),QueryKeys:"",ResponseType:typeof(MutationStatusData))).RequireKafdeckAntiforgery();

        app.MapPost(Prefix+"/rate-cards/activations/preview",async(RateCardActivationPreviewRequest request,HttpContext h,KafdeckAuthorizationService auth,IAttributionStore store,
            ICatalogStore catalog,MutationAdmissionService admission)=>await Safe(async()=>
        {
            if(!Allowed(h,auth,AuthorizationAction.RateCardManage,request.ClusterId,"active"))return Denied();
            var version=await store.GetRateAsync(request.ClusterId,request.VersionId,false,h.RequestAborted).ConfigureAwait(false);if(version is null)return Results.NotFound();
            await References(version.Document,request.ClusterId,h,auth,catalog,h.RequestAborted).ConfigureAwait(false);
            if((await store.GetStateAsync(request.ClusterId,h.RequestAborted).ConfigureAwait(false)).Revision!=request.ExpectedRevision)return Problem(409,"RateCardVersionConflict");
            var intent=Intent(MutationOperationKind.RateCardActivation,request.ClusterId,"active",new(version.VersionId,version.ContentHash,request.ExpectedRevision,request.Enabled));
            return Map(await admission.AdmitAsync(h.User,intent,new(MutationRiskClass.High,["rate_card_activation"],MutationConfirmationMode.TypedTarget,false),Key(h,request.IdempotencyKey),h.RequestAborted).ConfigureAwait(false));
        }).ConfigureAwait(false)).WithName("v09-rate-card-activation-preview").WithMetadata(new GovernanceRouteContract(AuthorizationAction.RateCardManage,false,typeof(RateCardActivationPreviewRequest),QueryKeys:"",ResponseType:typeof(MutationStatusData))).RequireKafdeckAntiforgery();

        app.MapGet(Prefix+"/rate-cards/operations/{operationId:guid}",async(Guid operationId,HttpContext h,KafdeckAuthorizationService auth,
            IMutationOperationRepository operations,IAttributionStore store)=>await Safe(async()=>
        {
            var op=await operations.GetAsync(operationId,h.RequestAborted).ConfigureAwait(false);if(op is null||op.OperationKind is not(MutationOperationKind.RateCardVersion or MutationOperationKind.RateCardActivation))return Results.NotFound();
            if(!op.AuthorizationTargets.All(t=>Allowed(h,auth,AuthorizationAction.AttributionRead,t.ClusterId,t.ResourceName)))return Denied();
            var m=AttributionMutationService.Material(op);var version=await store.GetRateAsync(op.ClusterId,m.VersionId,true,h.RequestAborted).ConfigureAwait(false);
            if(version is null||version.ContentHash!=m.ContentHash||!WeightVisibility(h,auth,op.ClusterId,version.Document))return Results.NotFound();
            return Results.Ok(new RateCardOperationReviewData(op.OperationId,op.PreviewHash,op.ClusterId,m.VersionId,m.ContentHash,version.Document,m.ExpectedRevision,m.Enabled,MutationStatusData.From(op)));
        }).ConfigureAwait(false)).WithName("v09-rate-card-operation").WithMetadata(new GovernanceRouteContract(AuthorizationAction.AttributionRead,true,QueryKeys:"",ResponseType:typeof(RateCardOperationReviewData)));

        app.MapPost(Prefix+"/rate-cards/operations/{operationId:guid}/recover",async(Guid operationId,HttpContext h,KafdeckAuthorizationService auth,
            IMutationOperationRepository operations,AdoAttributionStore store,IMutationAuditSink audit,MutationRequestAuthorizationService authorization)=>await Safe(async()=>
        {
            var op=await operations.GetAsync(operationId,h.RequestAborted).ConfigureAwait(false);if(op is null||op.OperationKind is not(MutationOperationKind.RateCardVersion or MutationOperationKind.RateCardActivation))return Results.NotFound();
            bool Current()=>MutationActor.TryCreate(h.User,out var actor)&&actor!.PrincipalId==op.RequesterPrincipalId&&
                authorization.AuthorizeForLifecycle(h.User,op)==KafdeckAuthorizationOutcome.Allowed&&
                op.AuthorizationTargets.All(t=>Allowed(h,auth,t.Action,t.ClusterId,t.ResourceName)&&Allowed(h,auth,AuthorizationAction.MutationReconcile,t.ClusterId,t.ResourceName))&&
                (!op.Risk.RequiresIndependentApproval||!string.IsNullOrEmpty(op.ApprovedByPrincipalId)&&op.ApprovedByPrincipalId!=op.RequesterPrincipalId&&
                    op.ApprovedAtUtc is not null&&!string.IsNullOrEmpty(op.ApprovalAuthorizationEvidenceHash)&&
                    op.AuthorizationTargets.All(t=>auth.IsCurrentGovernanceApprovalAllowed(op.ApprovedByPrincipalId,new(t.Action,t.ClusterId,t.ResourceName))));
            if(!Current())return Denied();
            if(op.State is not(MutationOperationState.ExecutionUnknown or MutationOperationState.AppliedVerified)||
                op.State==MutationOperationState.AppliedVerified&&op.ResultCode!="WorkflowCommitReceiptRecovered"||operations is not AdoMutationOperationRepository repository)return Problem(409,"RateCardRecoveryNotAdmitted");
            var receipt=await store.ReadReceiptAsync(operationId,h.RequestAborted).ConfigureAwait(false);var m=AttributionMutationService.Material(op);
            if(!AdoAttributionStore.Matches(receipt,op)||receipt!.VersionId!=m.VersionId||receipt.ContentHash!=m.ContentHash||op.OperationKind==MutationOperationKind.RateCardActivation&&(receipt.Revision!=m.ExpectedRevision+1||receipt.Enabled!=m.Enabled))return Problem(409,"RateCardPositiveReceiptRequired");
            if(op.State==MutationOperationState.AppliedVerified)
            {
                await store.VerifyPositiveReceiptAsync(op,receipt,Current,h.RequestAborted).ConfigureAwait(false);
                if(op.ExecutionClaimGeneration>0)await operations.ReleaseResourceClaimsAsync(operationId,op.ExecutionClaimGeneration,CancellationToken.None).ConfigureAwait(false);
                return Results.Ok(MutationStatusData.From(op));
            }
            var aggregate=MutationOperation.Restore(op);var now=DateTimeOffset.UtcNow;
            aggregate.ReconcileVerifiedWorkflowCommit(receipt.OperationId,receipt.OperationKind,receipt.MaterialHash,receipt.PreviewHash,receipt.VersionId,receipt.ContentHash,receipt.Revision,receipt.CommittedAtUtc,now);
            var saved=await repository.TrySaveGuardedAsync(aggregate.Snapshot,op.Version,(c,t,ct)=>store.ValidateRecoveryAsync(c,t,op,receipt,Current,ct),h.RequestAborted).ConfigureAwait(false);
            if(saved.Outcome!=MutationSaveOutcome.Saved)return Problem(409,"RateCardRecoveryConflict");
            if(op.ExecutionClaimGeneration>0)await operations.ReleaseResourceClaimsAsync(operationId,op.ExecutionClaimGeneration,CancellationToken.None).ConfigureAwait(false);
            await audit.WriteAsync(new(now,MutationAuditEventType.Reconciled,op.OperationId,op.RequesterPrincipalId,op.ClusterId,op.OperationKind,op.Risk.RiskClass,aggregate.Snapshot.State,op.ResourceKeys,op.PreviewHash,"WorkflowCommitReceiptRecovered"),CancellationToken.None).ConfigureAwait(false);
            return Results.Ok(MutationStatusData.From(aggregate.Snapshot));
        }).ConfigureAwait(false)).WithName("v09-rate-card-recover").WithMetadata(new GovernanceRouteContract(AuthorizationAction.MutationReconcile,false,QueryKeys:"",ResponseType:typeof(MutationStatusData))).RequireKafdeckAntiforgery();
        return app;
    }
    private static MutationIntentDescriptor Intent(MutationOperationKind kind,string cluster,string resource,RateCardMutationMaterial material)=>new(kind,cluster,JsonSerializer.Serialize(material,AttributionContract.Json),[$"governance:{cluster}:rate-card:{resource}"],AuthorizationTargets:[new(AuthorizationAction.RateCardManage,cluster,resource)]);
    private static bool Allowed(HttpContext h,KafdeckAuthorizationService auth,AuthorizationAction action,string cluster,string resource)=>auth.Authorize(h.User,new(action,cluster,resource))==KafdeckAuthorizationOutcome.Allowed;
    private static bool WeightVisibility(HttpContext h,KafdeckAuthorizationService auth,string cluster,RateCardDocument doc)=>doc.Weights.All(w=>(w.TeamId is null||Allowed(h,auth,AuthorizationAction.CatalogRead,cluster,"catalog:team:"+w.TeamId))&&(w.ApplicationId is null||Allowed(h,auth,AuthorizationAction.CatalogRead,cluster,"catalog:application:"+w.ApplicationId)));
    private static async Task References(RateCardDocument doc,string cluster,HttpContext h,KafdeckAuthorizationService auth,ICatalogStore catalog,CancellationToken token)
    {
        if(!WeightVisibility(h,auth,cluster,doc))throw new GovernanceEffectRejectedException("ScopeDenied");
        foreach(var w in doc.Weights)
        {
            if(w.TeamId is not null&&(await catalog.GetAsync(cluster,CatalogEntityKind.Team,w.TeamId,token).ConfigureAwait(false)) is not{Archived:false})throw new ArgumentException("Invalid team reference.");
            if(w.ApplicationId is not null&&(await catalog.GetAsync(cluster,CatalogEntityKind.Application,w.ApplicationId,token).ConfigureAwait(false)) is not{Archived:false})throw new ArgumentException("Invalid application reference.");
        }
    }
    private static string Coverage(IReadOnlyList<MeasurementWindow> rows,MeasurementQuery q,bool paged)
    {
        if(rows.Count==0)return "Missing";if(paged)return "PageOnly";if(q.ResourceId is null)return "AvailableMeasurementsOnly";
        foreach(var group in rows.GroupBy(w=>w.MetricKind))
        {
            var covered=q.FromUtc;foreach(var row in group.OrderBy(w=>w.WindowStartUtc)){if(row.WindowStartUtc>covered||row.Quantity is null)return "Incomplete";if(row.WindowEndUtc>covered)covered=row.WindowEndUtc;}
            if(covered<q.ToUtc)return "Incomplete";
        }
        return "AuthorizedResourceComplete";
    }
    private static string Scope(string family,MeasurementQuery q)=>string.Join('\n',family,q.ClusterId,q.FromUtc.ToString("O",CultureInfo.InvariantCulture),q.ToUtc.ToString("O",CultureInfo.InvariantCulture),q.ResourceId??"");
    private static string Actor(HttpContext h)=>MutationActor.TryCreate(h.User,out var actor)?actor!.PrincipalId:throw new ArgumentException("Authenticated attribution actor required.");
    private static string Encode(string after,string scope,HttpContext h,KafdeckAuthorizationService auth,IDataProtectionProvider provider)=>provider.CreateProtector("Kafdeck.v09.attribution-cursor.v1").Protect(JsonSerializer.Serialize(new AttributionCursor(scope,Actor(h),auth.PolicyFingerprint,after,DateTimeOffset.UtcNow.AddMinutes(15)),AttributionContract.Json));
    private static string? Decode(string? cursor,string scope,HttpContext h,KafdeckAuthorizationService auth,IDataProtectionProvider provider)
    {
        if(cursor is null)return null;if(cursor.Length>8192)throw new ArgumentException("Invalid cursor.");
        var c=JsonSerializer.Deserialize<AttributionCursor>(provider.CreateProtector("Kafdeck.v09.attribution-cursor.v1").Unprotect(cursor),AttributionContract.Json)??throw new ArgumentException("Invalid cursor.");
        if(c.Scope!=scope||c.Actor!=Actor(h)||c.Authority!=auth.PolicyFingerprint||c.Expires<DateTimeOffset.UtcNow)throw new ArgumentException("Cursor scope changed.");return AttributionContract.Digest(c.AfterId);
    }
    private static int Size(int? value)=>value is null?50:value is >=1 and <=200?value.Value:throw new ArgumentException("Invalid page size.");
    private static string Key(HttpContext h,string? body){var header=h.Request.Headers["Idempotency-Key"].FirstOrDefault();if(header is not null&&body is not null&&header!=body)throw new ArgumentException("Conflicting idempotency keys.");return header??body??"";}
    private static IResult Map(MutationAdmissionResult result)=>result.Operation is not null&&result.Outcome is MutationAdmissionOutcome.Created or MutationAdmissionOutcome.ExistingSameIntent?Results.Ok(MutationStatusData.From(result.Operation)):Problem(result.Outcome==MutationAdmissionOutcome.Forbidden?403:result.Outcome==MutationAdmissionOutcome.Unauthenticated?401:result.Outcome==MutationAdmissionOutcome.IdempotencyConflict?409:result.Outcome==MutationAdmissionOutcome.Unavailable?503:400,result.Code??result.Outcome.ToString());
    private static IResult Denied()=>Problem(403,"ScopeDenied");
    private static IResult Problem(int status,string code)=>Results.Problem(statusCode:status,title:code,type:"urn:kafdeck:problem:"+code,extensions:new Dictionary<string,object?>{{"code",code}});
    private static async Task<IResult> Safe(Func<Task<IResult>> action)
    {
        try{return await action().ConfigureAwait(false);}
        catch(GovernanceEffectRejectedException ex){return Problem(ex.Stale?409:403,ex.Code);}
        catch(AttributionConflictException ex){return Problem(ex.Code=="AttributionQueryBudgetExceeded"?429:409,ex.Code);}
        catch(Exception ex)when(ex is ArgumentException or JsonException or FormatException or OverflowException or CryptographicException){return Problem(400,"InvalidAttributionRequest");}
        catch(Exception ex)when(ex is System.Data.Common.DbException or GovernanceGenerationException or OperationCanceledException){return Problem(503,"AttributionUnavailable");}
    }
}
