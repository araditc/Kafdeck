using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.DataProtection;

namespace Kafdeck.Api;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record PolicyDocumentRequest(string Document);
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record PolicyVersionPreviewRequest(string ClusterId,string Document,string? IdempotencyKey=null);
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record PolicyActivationPreviewRequest(string ClusterId,string VersionId,long ExpectedRevision,bool Enabled,string? IdempotencyKey=null);
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record PolicyExplainRequest(string ClusterId,string ResourceKind,string ResourceId,string Action,string Risk);
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record PolicyDryRunRequest(IReadOnlyList<PolicyExplainRequest> Fixtures);
public sealed record PolicyValidationData(bool Valid,string ContentHash,string Document);
public sealed record PolicyVersionListData(IReadOnlyList<GovernancePolicyVersion> Items,string? NextCursor);
public sealed record PolicyDryRunData(IReadOnlyList<GovernanceDecision> Items);
public sealed record PolicyOperationReviewData(Guid OperationId,string PreviewHash,string Kind,string ClusterId,string VersionId,
    string ContentHash,string Document,long ExpectedRevision,bool Enabled,MutationStatusData Operation);
internal sealed record PolicyCursor(string ClusterId,string Actor,string Authority,string AfterId,DateTimeOffset Expires);

public static class KafdeckGovernancePolicyEndpoints
{
    private const string Prefix="/api/v1/governance/policies";
    private static readonly JsonSerializerOptions Json=new(JsonSerializerDefaults.Web);
    public static WebApplication MapKafdeckGovernancePolicies(this WebApplication app)
    {
        app.MapPost(Prefix+"/validate",(PolicyDocumentRequest request,HttpContext h,KafdeckAuthorizationService auth)=>
        {
            if(auth.AuthorizeCollection(h.User,AuthorizationAction.GovernancePolicyRead,null)!=KafdeckAuthorizationOutcome.Allowed)return Denied();
            return Safe(()=>{var p=GovernancePolicyCompiler.Compile(request.Document);return Results.Ok(new PolicyValidationData(true,p.ContentHash,p.CanonicalJson));});
        }).WithName("v09-policy-validate").WithMetadata(new GovernanceRouteContract(AuthorizationAction.GovernancePolicyRead,true,typeof(PolicyDocumentRequest),QueryKeys:"",ResponseType:typeof(PolicyValidationData)));

        app.MapGet(Prefix+"/active",async (string clusterId,HttpContext h,KafdeckAuthorizationService auth,IGovernancePolicyStore store)=>
        {
            if(!Allowed(h,auth,AuthorizationAction.GovernancePolicyRead,clusterId,"active"))return Denied();
            return await SafeAsync(async()=>Results.Ok(await store.GetStateAsync(clusterId,h.RequestAborted).ConfigureAwait(false))).ConfigureAwait(false);
        }).WithName("v09-policy-active").WithMetadata(new GovernanceRouteContract(AuthorizationAction.GovernancePolicyRead,true,QueryKeys:"clusterId",ResponseType:typeof(GovernancePolicyState)));

        app.MapGet(Prefix+"/versions",async (string clusterId,int? pageSize,string? cursor,HttpContext h,KafdeckAuthorizationService auth,IGovernancePolicyStore store,IDataProtectionProvider protection)=>
        {
            if(auth.AuthorizeCollection(h.User,AuthorizationAction.GovernancePolicyRead,clusterId)!=KafdeckAuthorizationOutcome.Allowed)return Denied();
            return await SafeAsync(async()=>
            {
                var take=pageSize??50;if(take is <1 or >200)throw new ArgumentException("Invalid page size.");
                var actor=MutationActor.TryCreate(h.User,out var who)?who!.PrincipalId:"local";
                var protector=protection.CreateProtector("Kafdeck.v09.policy-cursor.v1");
                string? after=null;
                if(cursor is not null)
                {
                    if(cursor.Length>8192)throw new ArgumentException("Invalid cursor.");
                    var parsed=JsonSerializer.Deserialize<PolicyCursor>(protector.Unprotect(cursor),Json)??throw new ArgumentException("Invalid cursor.");
                    if(parsed.ClusterId!=clusterId||parsed.Actor!=actor||parsed.Authority!=auth.PolicyFingerprint||parsed.Expires<=DateTimeOffset.UtcNow)throw new ArgumentException("Invalid cursor.");
                    after=parsed.AfterId;
                }
                var rows=await store.ListVersionsAsync(clusterId,after,take+1,h.RequestAborted).ConfigureAwait(false);
                var visible=rows.Where(r=>Allowed(h,auth,AuthorizationAction.GovernancePolicyRead,clusterId,r.VersionId)).Take(take).ToArray();
                var next=rows.Count>take?protector.Protect(JsonSerializer.Serialize(new PolicyCursor(clusterId,actor,auth.PolicyFingerprint,rows[take-1].VersionId,DateTimeOffset.UtcNow.AddMinutes(10)),Json)):null;
                return Results.Ok(new PolicyVersionListData(visible,next));
            }).ConfigureAwait(false);
        }).WithName("v09-policy-versions").WithMetadata(new GovernanceRouteContract(AuthorizationAction.GovernancePolicyRead,true,ResponseType:typeof(PolicyVersionListData)));

        app.MapGet(Prefix+"/versions/{versionId}",async (string clusterId,string versionId,HttpContext h,KafdeckAuthorizationService auth,IGovernancePolicyStore store)=>
        {
            if(!Allowed(h,auth,AuthorizationAction.GovernancePolicyRead,clusterId,versionId))return Results.NotFound();
            return await SafeAsync(async()=>{var v=await store.GetVersionAsync(clusterId,versionId,false,h.RequestAborted).ConfigureAwait(false);return v is null?Results.NotFound():Results.Ok(v);}).ConfigureAwait(false);
        }).WithName("v09-policy-version").WithMetadata(new GovernanceRouteContract(AuthorizationAction.GovernancePolicyRead,true,QueryKeys:"clusterId",ResponseType:typeof(GovernancePolicyVersion)));

        app.MapGet(Prefix+"/operations/{operationId:guid}",async (Guid operationId,HttpContext h,KafdeckAuthorizationService auth,IMutationOperationRepository operations,IGovernancePolicyStore store)=>
        {
            return await SafeAsync(async()=>
            {
                var op=await operations.GetAsync(operationId,h.RequestAborted).ConfigureAwait(false);
                if(op is null||op.OperationKind is not(MutationOperationKind.GovernancePolicyVersion or MutationOperationKind.GovernancePolicyActivation))return Results.NotFound();
                var material=GovernancePolicyMutationService.Material(op);
                if(!Allowed(h,auth,AuthorizationAction.GovernancePolicyRead,op.ClusterId,material.VersionId))return Results.NotFound();
                var version=await store.GetVersionAsync(op.ClusterId,material.VersionId,true,h.RequestAborted).ConfigureAwait(false);
                if(version is null||version.ContentHash!=material.ContentHash)return Results.NotFound();
                return Results.Ok(new PolicyOperationReviewData(op.OperationId,op.PreviewHash,op.OperationKind.ToString(),op.ClusterId,material.VersionId,material.ContentHash,version.Document,material.ExpectedRevision,material.Enabled,MutationStatusData.From(op)));
            }).ConfigureAwait(false);
        }).WithName("v09-policy-operation").WithMetadata(new GovernanceRouteContract(AuthorizationAction.GovernancePolicyRead,true,QueryKeys:"",ResponseType:typeof(PolicyOperationReviewData)));

        app.MapPost(Prefix+"/explain",async (PolicyExplainRequest request,HttpContext h,KafdeckAuthorizationService auth,IGovernancePolicyStore store,ICatalogStore catalog,ICatalogTopicIdentityPort topics)=>
            await SafeAsync(async()=>Results.Ok(await Explain(request,h,auth,store,catalog,topics).ConfigureAwait(false))).ConfigureAwait(false))
            .WithName("v09-policy-explain").WithMetadata(new GovernanceRouteContract(AuthorizationAction.GovernancePolicyRead,true,typeof(PolicyExplainRequest),QueryKeys:"",ResponseType:typeof(GovernanceDecision)));

        app.MapPost(Prefix+"/dry-run",async (PolicyDryRunRequest request,HttpContext h,KafdeckAuthorizationService auth,IGovernancePolicyStore store,ICatalogStore catalog,ICatalogTopicIdentityPort topics)=>
            await SafeAsync(async()=>
            {
                if(request.Fixtures is not {Count:>0 and <=100})throw new ArgumentException("Dry run requires 1–100 fixtures.");
                var results=new List<GovernanceDecision>();
                foreach(var f in request.Fixtures)results.Add(await Explain(f,h,auth,store,catalog,topics).ConfigureAwait(false));
                return Results.Ok(new PolicyDryRunData(results));
            }).ConfigureAwait(false)).WithName("v09-policy-dry-run").WithMetadata(new GovernanceRouteContract(AuthorizationAction.GovernancePolicyRead,true,typeof(PolicyDryRunRequest),QueryKeys:"",ResponseType:typeof(PolicyDryRunData)));

        app.MapPost(Prefix+"/versions/preview",async (PolicyVersionPreviewRequest request,HttpContext h,KafdeckAuthorizationService auth,IGovernancePolicyStore store,MutationAdmissionService admission)=>
            await SafeAsync(async()=>
            {
                var policy=GovernancePolicyCompiler.Compile(request.Document);
                var id=GovernancePolicyCompiler.Hash(request.ClusterId+"\n"+policy.ContentHash);
                if(!Allowed(h,auth,AuthorizationAction.GovernancePolicyManage,request.ClusterId,id)||!MutationActor.TryCreate(h.User,out var actor))return Denied();
                if(policy.Document.Rules.Any(r=>r.Scope.ClusterId!=request.ClusterId))throw new ArgumentException("Policy scope differs from version scope.");
                _=MutationIdempotency.HashKey(Key(h,request.IdempotencyKey));
                var v=await store.StageAsync(request.ClusterId,policy,actor!.PrincipalId,h.RequestAborted).ConfigureAwait(false);
                var intent=Intent(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,request.ClusterId,v.VersionId,new(v.VersionId,v.ContentHash));
                return Map(await admission.AdmitAsync(h.User,intent,new(MutationRiskClass.Moderate,["governance_policy_version"],MutationConfirmationMode.Explicit,false),Key(h,request.IdempotencyKey),h.RequestAborted).ConfigureAwait(false));
            }).ConfigureAwait(false)).WithName("v09-policy-version-preview").WithMetadata(new GovernanceRouteContract(AuthorizationAction.GovernancePolicyManage,false,typeof(PolicyVersionPreviewRequest),QueryKeys:"",ResponseType:typeof(MutationStatusData))).RequireKafdeckAntiforgery();

        app.MapPost(Prefix+"/activations/preview",async (PolicyActivationPreviewRequest request,HttpContext h,KafdeckAuthorizationService auth,IGovernancePolicyStore store,MutationAdmissionService admission)=>
            await SafeAsync(async()=>
            {
                if(!Allowed(h,auth,AuthorizationAction.GovernancePolicyActivate,request.ClusterId,"active"))return Denied();
                if(request.ExpectedRevision is <0 or long.MaxValue)throw new ArgumentException("Invalid revision.");
                var v=await store.GetVersionAsync(request.ClusterId,request.VersionId,false,h.RequestAborted).ConfigureAwait(false);
                if(v is null)return Results.NotFound();
                var current=await store.GetStateAsync(request.ClusterId,h.RequestAborted).ConfigureAwait(false);
                if(current.Revision!=request.ExpectedRevision)return Problem(409,"PolicyVersionConflict");
                var intent=Intent(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,request.ClusterId,"active",new(v.VersionId,v.ContentHash,request.ExpectedRevision,request.Enabled));
                return Map(await admission.AdmitAsync(h.User,intent,new(MutationRiskClass.Critical,["governance_policy_activation"],MutationConfirmationMode.TypedTarget,true),Key(h,request.IdempotencyKey),h.RequestAborted).ConfigureAwait(false));
            }).ConfigureAwait(false)).WithName("v09-policy-activation-preview").WithMetadata(new GovernanceRouteContract(AuthorizationAction.GovernancePolicyActivate,false,typeof(PolicyActivationPreviewRequest),QueryKeys:"",ResponseType:typeof(MutationStatusData))).RequireKafdeckAntiforgery();

        app.MapPost(Prefix+"/operations/{operationId:guid}/reconcile",async (Guid operationId,HttpContext h,KafdeckAuthorizationService auth,
            IMutationOperationRepository operations,AdoGovernancePolicyStore store,IMutationAuditSink audit)=>await SafeAsync(async()=>
        {
            var source=await operations.GetAsync(operationId,h.RequestAborted).ConfigureAwait(false);
            if(source is null||source.OperationKind is not(MutationOperationKind.GovernancePolicyVersion or MutationOperationKind.GovernancePolicyActivation))return Results.NotFound();
            bool Current()=>MutationActor.TryCreate(h.User,out var actor)&&actor!.PrincipalId==source.RequesterPrincipalId&&
                source.AuthorizationTargets.All(t=>Allowed(h,auth,t.Action,t.ClusterId,t.ResourceName)&&Allowed(h,auth,AuthorizationAction.MutationReconcile,t.ClusterId,t.ResourceName))&&
                (!source.Risk.RequiresIndependentApproval||!string.IsNullOrEmpty(source.ApprovedByPrincipalId)&&source.ApprovedByPrincipalId!=source.RequesterPrincipalId&&
                    source.AuthorizationTargets.All(t=>auth.IsCurrentGovernanceApprovalAllowed(source.ApprovedByPrincipalId,new(t.Action,t.ClusterId,t.ResourceName))));
            if(!Current())return Denied();
            if(source.State==MutationOperationState.AppliedVerified&&source.ResultCode=="GovernanceCommitReceiptRecovered")return Results.Ok(MutationStatusData.From(source));
            if(source.State!=MutationOperationState.ExecutionUnknown||operations is not AdoMutationOperationRepository repository)return Problem(409,"PolicyRecoveryNotAdmitted");
            var receipt=await store.ReadReceiptAsync(operationId,h.RequestAborted).ConfigureAwait(false);
            var material=GovernancePolicyMutationService.Material(source);
            if(!AdoGovernancePolicyStore.Matches(receipt,source)||receipt!.VersionId!=material.VersionId||receipt.ContentHash!=material.ContentHash||
                source.OperationKind==MutationOperationKind.GovernancePolicyActivation&&(receipt.Revision!=material.ExpectedRevision+1||receipt.Enabled!=material.Enabled))return Problem(409,"PolicyCommitEvidenceMissing");
            var aggregate=MutationOperation.Restore(source);var now=DateTimeOffset.UtcNow;
            aggregate.ReconcileVerifiedGovernanceCommit(receipt.OperationId,receipt.OperationKind,receipt.MaterialHash,receipt.PreviewHash,receipt.VersionId,receipt.ContentHash,receipt.Revision,receipt.CommittedAtUtc,now);
            var saved=await repository.TrySaveGuardedAsync(aggregate.Snapshot,source.Version,
                (c,t,ct)=>store.ValidateRecoveryAsync(c,t,source,receipt,Current,ct),h.RequestAborted).ConfigureAwait(false);
            if(saved.Outcome!=MutationSaveOutcome.Saved)return Problem(409,"PolicyRecoveryConflict");
            await audit.WriteAsync(new(now,MutationAuditEventType.Reconciled,source.OperationId,source.RequesterPrincipalId,source.ClusterId,source.OperationKind,source.Risk.RiskClass,aggregate.Snapshot.State,source.ResourceKeys,source.PreviewHash,"GovernanceCommitReceiptRecovered"),CancellationToken.None).ConfigureAwait(false);
            return Results.Ok(MutationStatusData.From(aggregate.Snapshot));
        }).ConfigureAwait(false)).WithName("v09-policy-reconcile").WithMetadata(new GovernanceRouteContract(AuthorizationAction.MutationReconcile,false,QueryKeys:"",ResponseType:typeof(MutationStatusData))).RequireKafdeckAntiforgery();
        return app;
    }

    private static async Task<GovernanceDecision> Explain(PolicyExplainRequest f,HttpContext h,KafdeckAuthorizationService auth,IGovernancePolicyStore store,ICatalogStore catalog,ICatalogTopicIdentityPort topics)
    {
        GovernancePolicyCompiler.Bounded(f.ClusterId,256);GovernancePolicyCompiler.Bounded(f.ResourceId,512);
        if(!Enum.TryParse<AuthorizationAction>(f.Action,true,out var action)||!Enum.IsDefined(action)||!f.Action.Equals(action.ToString(),StringComparison.OrdinalIgnoreCase)||!Enum.TryParse<MutationRiskClass>(f.Risk,true,out var risk)||!Enum.IsDefined(risk)||!f.Risk.Equals(risk.ToString(),StringComparison.OrdinalIgnoreCase)||f.ResourceKind!=AdoGovernancePolicyStore.ResourceKind(action.ToString()))throw new ArgumentException("Invalid explain fixture.");
        if(!Allowed(h,auth,AuthorizationAction.GovernancePolicyRead,f.ClusterId,"active")||!Allowed(h,auth,action,f.ClusterId,f.ResourceId))return new(false,"ScopeDenied",null,[]);
        var state=await store.GetStateAsync(f.ClusterId,h.RequestAborted).ConfigureAwait(false);
        if(!state.Enabled)return new(true,"Allowed",state.Digest,[]);
        if(!Allowed(h,auth,AuthorizationAction.GovernancePolicyRead,f.ClusterId,state.VersionId!))return new(false,"ScopeDenied",null,[]);
        var v=await store.GetVersionAsync(f.ClusterId,state.VersionId!,false,h.RequestAborted).ConfigureAwait(false)??throw new InvalidOperationException("Active policy unavailable.");
        IReadOnlyDictionary<string,string>? facts=null;
        if(f.ResourceKind=="topic"&&Allowed(h,auth,AuthorizationAction.CatalogRead,f.ClusterId,f.ResourceId))
        {
            var row=await catalog.FindTopicAsync(f.ClusterId,f.ResourceId,h.RequestAborted).ConfigureAwait(false);
            var live=await topics.ObserveAsync(f.ClusterId,f.ResourceId,h.RequestAborted).ConfigureAwait(false);
            if(row is not null&&!row.Archived&&live is not null&&row.TopicIdentity==live.Fingerprint)
                facts=new Dictionary<string,string>{{"classification",row.Classification??""},{"domain",row.Domain??""},{"teamId",row.TeamId??""},{"applicationId",row.ApplicationId??""}};
        }
        return GovernancePolicyCompiler.Evaluate(GovernancePolicyCompiler.Compile(v.Document),new(f.ClusterId,f.ResourceKind,f.ResourceId,action.ToString(),risk.ToString()),facts,state.Digest);
    }
    private static MutationIntentDescriptor Intent(MutationOperationKind kind,AuthorizationAction action,string cluster,string resource,PolicyMutationMaterial material)=>
        new(kind,cluster,JsonSerializer.Serialize(material,Json),[$"governance:{cluster}:policy:{resource}"],AuthorizationTargets:[new(action,cluster,resource)]);
    private static bool Allowed(HttpContext h,KafdeckAuthorizationService auth,AuthorizationAction action,string cluster,string resource)=>auth.Authorize(h.User,new(action,cluster,resource))==KafdeckAuthorizationOutcome.Allowed;
    private static string Key(HttpContext h,string? body)
    {
        var header=h.Request.Headers["Idempotency-Key"].FirstOrDefault();
        if(header is not null&&body is not null&&header!=body)throw new ArgumentException("Conflicting idempotency keys.");
        return header??body??"";
    }
    private static IResult Map(MutationAdmissionResult result)=>result.Operation is not null&&result.Outcome is MutationAdmissionOutcome.Created or MutationAdmissionOutcome.ExistingSameIntent?Results.Ok(MutationStatusData.From(result.Operation)):
        Problem(result.Outcome==MutationAdmissionOutcome.Forbidden?403:result.Outcome==MutationAdmissionOutcome.Unauthenticated?401:result.Outcome==MutationAdmissionOutcome.IdempotencyConflict?409:result.Code=="GovernanceUnavailable"?503:400,result.Code??result.Outcome.ToString());
    private static IResult Denied()=>Problem(403,"ScopeDenied");
    private static IResult Problem(int status,string code)=>Results.Problem(statusCode:status,title:code,type:"urn:kafdeck:problem:"+code,extensions:new Dictionary<string,object?>{{"code",code}});
    private static IResult Safe(Func<IResult> f){try{return f();}catch(Exception ex)when(ex is ArgumentException or JsonException or InvalidOperationException or FormatException){return Problem(400,"InvalidPolicy");}}
    private static async Task<IResult> SafeAsync(Func<Task<IResult>> f)
    {
        try{return await f().ConfigureAwait(false);}
        catch(Exception ex)when(ex is ArgumentException or JsonException or FormatException or CryptographicException or OverflowException){return Problem(400,"InvalidPolicy");}
        catch(GovernanceEffectRejectedException ex){return Problem(ex.Stale?409:403,ex.Code);}
        catch(OperationCanceledException){throw;}
        catch(Exception){return Problem(503,"GovernanceUnavailable");}
    }
}
