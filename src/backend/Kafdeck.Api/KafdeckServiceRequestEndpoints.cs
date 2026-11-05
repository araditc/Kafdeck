using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.DataProtection;

namespace Kafdeck.Api;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record ServiceRequestChangePreviewRequest(string ClusterId,string? RequestId,long ExpectedVersion,ServiceRequestCommand Command,
    Guid? OperationId,string Reason,DateTimeOffset? ExpiresAtUtc=null,string? IdempotencyKey=null);
public sealed record ServiceRequestData(string RequestId,string ClusterId,long Revision,long Version,string BusinessState,string RequesterPrincipalId,
    string ClientKind,Guid OperationId,string PreviewHash,string MaterialHash,string PolicyDigest,string Reason,DateTimeOffset CreatedAtUtc,
    DateTimeOffset UpdatedAtUtc,DateTimeOffset ExpiresAtUtc,string? ExecutionState,string? ExecutionCode,IReadOnlyList<ServiceRequestDecision> Decisions,
    ServiceRequestBinding Binding);
public sealed record ServiceRequestPage(IReadOnlyList<ServiceRequestData> Items,string? NextCursor);
public sealed record ServiceRequestHistoryPage(IReadOnlyList<ServiceRequestHistory> Items,string? NextCursor);
public sealed record ServiceRequestPlanData(ServiceRequestPlan Plan,MutationStatusData Operation);

public static class KafdeckServiceRequestEndpoints
{
    private const string Prefix="/api/v1/governance/requests";
    public static WebApplication MapKafdeckServiceRequestEndpoints(this WebApplication app)
    {
        app.MapGet(Prefix,async(HttpContext h,string clusterId,int? pageSize,string? cursor,AdoServiceRequestStore store,
            IMutationOperationRepository operations,KafdeckAuthorizationService auth,IDataProtectionProvider protection,AdoGovernancePolicyStore policies)=>await Safe(async()=>
        {
            ServiceRequestContract.Bound(clusterId,256);var size=Size(pageSize);
            if(auth.AuthorizeCollection(h.User,AuthorizationAction.RequestRead,clusterId)!=KafdeckAuthorizationOutcome.Allowed)return Denied();
            var scope=Scope(h,auth,clusterId+":"+size.ToString(CultureInfo.InvariantCulture));var protector=protection.CreateProtector("Kafdeck.W74.Requests.Cursor.v1");
            var after=ReadCursor(cursor,scope,protector);
            var rows=await store.ListAuthorizedAsync(clusterId,after,size,r=>Readable(h,auth,r),h.RequestAborted).ConfigureAwait(false);
            var result=new List<ServiceRequestData>();foreach(var row in rows.Take(size))result.Add(await CurrentData(row,await operations.GetAsync(row.Binding.OperationId,h.RequestAborted).ConfigureAwait(false),policies,h.RequestAborted).ConfigureAwait(false));
            var next=rows.Count>size?WriteCursor(scope,rows[size-1].RequestId,protector):null;
            return Results.Ok(new ServiceRequestPage(result,next));
        }).ConfigureAwait(false)).WithName("v09-request-list").WithMetadata(new GovernanceRouteContract(AuthorizationAction.RequestRead,true,ResponseType:typeof(ServiceRequestPage)));
        app.MapGet(Prefix+"/{requestId}",async(string requestId,string clusterId,HttpContext h,AdoServiceRequestStore store,
            IMutationOperationRepository operations,KafdeckAuthorizationService auth,AdoGovernancePolicyStore policies)=>await Safe(async()=>
        {
            ServiceRequestContract.Id(requestId);var row=await store.GetAsync(clusterId,requestId,h.RequestAborted).ConfigureAwait(false);
            if(row is null||!Readable(h,auth,row))return Results.NotFound();
            return Results.Ok(await CurrentData(row,await operations.GetAsync(row.Binding.OperationId,h.RequestAborted).ConfigureAwait(false),policies,h.RequestAborted).ConfigureAwait(false));
        }).ConfigureAwait(false)).WithName("v09-request-get").WithMetadata(new GovernanceRouteContract(AuthorizationAction.RequestRead,true,QueryKeys:"clusterId",ResponseType:typeof(ServiceRequestData)));
        app.MapGet(Prefix+"/{requestId}/history",async(string requestId,string clusterId,int? pageSize,string? cursor,HttpContext h,
            AdoServiceRequestStore store,KafdeckAuthorizationService auth,IDataProtectionProvider protection)=>await Safe(async()=>
        {
            ServiceRequestContract.Id(requestId);var size=Size(pageSize);var current=await store.GetAsync(clusterId,requestId,h.RequestAborted).ConfigureAwait(false);
            if(current is null||!Readable(h,auth,current))return Results.NotFound();
            var protector=protection.CreateProtector("Kafdeck.W74.Requests.HistoryCursor.v1");var scope=Scope(h,auth,clusterId+":"+requestId+":"+size.ToString(CultureInfo.InvariantCulture));
            var after=ReadCursor(cursor,scope,protector);var version=after is null?0:long.Parse(after,CultureInfo.InvariantCulture);
            var rows=await store.HistoryAuthorizedAsync(clusterId,requestId,version,size,r=>Readable(h,auth,r),h.RequestAborted).ConfigureAwait(false);
            return Results.Ok(new ServiceRequestHistoryPage(rows.Take(size).ToArray(),rows.Count>size?WriteCursor(scope,rows[size-1].Request.Version.ToString(CultureInfo.InvariantCulture),protector):null));
        }).ConfigureAwait(false)).WithName("v09-request-history").WithMetadata(new GovernanceRouteContract(AuthorizationAction.RequestRead,true,ResponseType:typeof(ServiceRequestHistoryPage)));
        var previewHandler=async(ServiceRequestChangePreviewRequest request,HttpContext h,AdoServiceRequestStore store,
            IMutationOperationRepository operations,KafdeckAuthorizationService auth,MutationAdmissionService admission)=>await Safe(async()=>
        {
            ServiceRequestContract.Bound(request.ClusterId,256);ServiceRequestContract.Bound(request.Reason,2048);
            if(h.Request.Path.Value!.EndsWith("/decisions/preview",StringComparison.Ordinal)!=ServiceRequestContract.IsDecision(request.Command))return Problem(400,"InvalidRequestCommandRoute");
            if(!Enum.IsDefined(request.Command)||request.ExpectedVersion<0||!MutationActor.TryCreate(h.User,out var actor)||actor is null)return Problem(400,"InvalidRequest");
            var action=ServiceRequestMutation.Action(request.Command);var kind=ServiceRequestMutation.Kind(request.Command);
            var key=h.Request.Headers["Idempotency-Key"].FirstOrDefault()??request.IdempotencyKey??"";
            if(request.IdempotencyKey is not null&&request.IdempotencyKey!=key)throw new ArgumentException("Conflicting idempotency key.");
            _=MutationIdempotency.HashKey(key);var planId=MutationIdempotency.DeriveOperationId(actor.PrincipalId,request.ClusterId,kind,key);
            var requestId=request.RequestId??(request.Command==ServiceRequestCommand.Draft?new Guid(Convert.FromHexString(GovernancePolicyCompiler.Hash("request:"+planId.ToString("D")))[..16]).ToString("N"):throw new ArgumentException("Request ID required."));
            ServiceRequestContract.Id(requestId);
            if(!Allowed(h,auth,action,request.ClusterId,requestId)||!Allowed(h,auth,AuthorizationAction.RequestRead,request.ClusterId,requestId))return Denied();
            if(ServiceRequestContract.IsDecision(request.Command)&&!auth.IsLiveRequestDecisionAllowed(h.User,new(action,request.ClusterId,ServiceRequestContract.Resource(requestId))))return Denied();
            var old=request.Command==ServiceRequestCommand.Draft?null:await store.GetAsync(request.ClusterId,requestId,h.RequestAborted).ConfigureAwait(false);
            if(request.Command!=ServiceRequestCommand.Draft&&(old is null||!Readable(h,auth,old)))return Results.NotFound();
            if(old is not null&&old.Version!=request.ExpectedVersion)return Problem(409,"RequestVersionConflict");
            var targetId=request.Command is ServiceRequestCommand.Draft or ServiceRequestCommand.Revise?request.OperationId??throw new ArgumentException("Target operation required."):old!.Binding.OperationId;
            if(request.Command is not(ServiceRequestCommand.Draft or ServiceRequestCommand.Revise)&&request.OperationId is not null&&request.OperationId!=targetId)throw new ArgumentException("Request binding mismatch.");
            var target=await operations.GetAsync(targetId,h.RequestAborted).ConfigureAwait(false);
            if(target is null||target.OperationKind is MutationOperationKind.ServiceRequestChange or MutationOperationKind.ServiceRequestDecision||target.ClusterId!=request.ClusterId)return Results.NotFound();
            var binding=AdoServiceRequestStore.Bind(target);
            if(!binding.AuthorizationTargets.All(t=>auth.Authorize(h.User,ServiceRequestMutationService.Visibility(binding,t))==KafdeckAuthorizationOutcome.Allowed))return Results.NotFound();
            if(!ServiceRequestContract.IsDecision(request.Command)&&target.RequesterPrincipalId!=actor.PrincipalId)return Denied();
            if(ServiceRequestContract.IsDecision(request.Command)&&ServiceRequestContract.SameHuman(target.RequesterPrincipalId,actor.PrincipalId))return Denied();
            var existing=await store.GetPlanAsync(planId,h.RequestAborted).ConfigureAwait(false);var now=existing?.CreatedAtUtc??DateTimeOffset.UtcNow;
            var expires=request.ExpiresAtUtc??(old is not null&&request.Command!=ServiceRequestCommand.Revise?old.ExpiresAtUtc:target.PreviewExpiresAtUtc);
            var plan=new ServiceRequestPlan(planId,request.ClusterId,"",requestId,request.Command,request.ExpectedVersion,actor.PrincipalId,
                h.User.Identity is NonBrowserReadIdentity identity?identity.Kind==NonBrowserPrincipalKind.Human?"CliHuman":"Automation":"Browser",
                auth.PolicyFingerprint,store.DeploymentGeneration,binding,request.Reason,expires,now,existing?.ExpiresAtUtc??new[]{now.AddMinutes(10),target.PreviewExpiresAtUtc}.Min());
            plan=plan with{ContentHash=ServiceRequestContract.PlanHash(plan)};plan=await store.StageAsync(plan,h.RequestAborted).ConfigureAwait(false);
            var admitted=await admission.AdmitAsync(h.User,plan.PlanId,ServiceRequestMutation.Intent(plan),new(MutationRiskClass.Moderate,["governed_request_metadata"],MutationConfirmationMode.Explicit,false),key,h.RequestAborted).ConfigureAwait(false);
            return admitted.Operation is not null&&admitted.Outcome is MutationAdmissionOutcome.Created or MutationAdmissionOutcome.ExistingSameIntent
                ?Results.Ok(MutationStatusData.From(admitted.Operation)):Problem(admitted.Outcome==MutationAdmissionOutcome.Forbidden?403:admitted.Outcome==MutationAdmissionOutcome.IdempotencyConflict?409:admitted.Outcome==MutationAdmissionOutcome.Unavailable?503:400,admitted.Code??admitted.Outcome.ToString());
        }).ConfigureAwait(false);
        app.MapPost(Prefix+"/changes/preview",previewHandler).WithName("v09-request-change-preview").WithMetadata(new GovernanceRouteContract(AuthorizationAction.RequestSubmit,false,typeof(ServiceRequestChangePreviewRequest),QueryKeys:"",ResponseType:typeof(MutationStatusData))).RequireKafdeckAntiforgery();
        app.MapPost(Prefix+"/decisions/preview",previewHandler).WithName("v09-request-decision-preview").WithMetadata(new GovernanceRouteContract(AuthorizationAction.RequestDecide,false,typeof(ServiceRequestChangePreviewRequest),QueryKeys:"",ResponseType:typeof(MutationStatusData))).RequireKafdeckAntiforgery();
        app.MapGet(Prefix+"/plans/{operationId:guid}",async(Guid operationId,HttpContext h,AdoServiceRequestStore store,
            IMutationOperationRepository operations,KafdeckAuthorizationService auth)=>await Safe(async()=>
        {
            var plan=await store.GetPlanAsync(operationId,h.RequestAborted).ConfigureAwait(false);var operation=await operations.GetAsync(operationId,h.RequestAborted).ConfigureAwait(false);
            if(plan is null||operation is null||!Allowed(h,auth,AuthorizationAction.RequestRead,plan.ClusterId,plan.RequestId)||
                !plan.Binding.AuthorizationTargets.All(t=>auth.Authorize(h.User,ServiceRequestMutationService.Visibility(plan.Binding,t))==KafdeckAuthorizationOutcome.Allowed))return Results.NotFound();
            return Results.Ok(new ServiceRequestPlanData(plan,MutationStatusData.From(operation)));
        }).ConfigureAwait(false)).WithName("v09-request-plan").WithMetadata(new GovernanceRouteContract(AuthorizationAction.RequestRead,true,QueryKeys:"",ResponseType:typeof(ServiceRequestPlanData)));
        app.MapPost(Prefix+"/operations/{operationId:guid}/recover",async(Guid operationId,HttpContext h,AdoServiceRequestStore store,
            IMutationOperationRepository operations,ServiceRequestMutationService mutations,KafdeckAuthorizationService auth,IMutationAuditSink audit)=>await Safe(async()=>
        {
            var operation=await operations.GetAsync(operationId,h.RequestAborted).ConfigureAwait(false);
            if(operation is null||operation.OperationKind is not(MutationOperationKind.ServiceRequestChange or MutationOperationKind.ServiceRequestDecision))return Results.NotFound();
            var plan=await mutations.LoadAsync(operation,h.RequestAborted).ConfigureAwait(false);if(plan is null)return Problem(409,"RequestMaterialChanged");
            bool Current()=>mutations.CurrentForPrincipal(plan,operation,h.User,true)&&Allowed(h,auth,AuthorizationAction.MutationReconcile,plan.ClusterId,plan.RequestId);
            if(!Current())return Denied();
            if(operation.State is not(MutationOperationState.ExecutionUnknown or MutationOperationState.AppliedVerified)||
                operation.State==MutationOperationState.AppliedVerified&&operation.ResultCode!="WorkflowCommitReceiptRecovered"||
                operations is not AdoMutationOperationRepository repository)return Problem(409,"RequestRecoveryNotAdmitted");
            var receipt=await store.GetReceiptAsync(operationId,h.RequestAborted).ConfigureAwait(false);if(receipt is null)return Problem(409,"RequestPositiveReceiptRequired");
            if(operation.State==MutationOperationState.AppliedVerified)
            {
                await using var db=await h.RequestServices.GetRequiredService<IMutationDbConnectionFactory>().OpenAsync(h.RequestAborted).ConfigureAwait(false);
                await using var tx=await db.BeginTransactionAsync(h.RequestAborted).ConfigureAwait(false);
                await store.VerifyReceiptOnTransactionAsync(db,tx,plan,receipt,operation,Current,h.RequestAborted).ConfigureAwait(false);
                await tx.CommitAsync(h.RequestAborted).ConfigureAwait(false);return Results.Ok(MutationStatusData.From(operation));
            }
            var recovered=MutationOperation.Restore(operation);recovered.ReconcileVerifiedWorkflowCommit(operationId,(int)operation.OperationKind,
                GovernancePolicyCompiler.Hash(operation.CanonicalIntent),receipt.PreviewHash,receipt.RequestId,receipt.ContentHash,receipt.Version,receipt.CommittedAtUtc,DateTimeOffset.UtcNow);
            var saved=await repository.TrySaveGuardedAsync(recovered.Snapshot,operation.Version,
                (db,tx,token)=>store.VerifyReceiptOnTransactionAsync(db,tx,plan,receipt,operation,Current,token),h.RequestAborted).ConfigureAwait(false);
            if(saved.Outcome!=MutationSaveOutcome.Saved)return Problem(409,"RequestRecoveryVersionConflict");
            if(operation.ExecutionClaimGeneration>0)await operations.ReleaseResourceClaimsAsync(operationId,operation.ExecutionClaimGeneration,CancellationToken.None).ConfigureAwait(false);
            await audit.WriteAsync(new(DateTimeOffset.UtcNow,MutationAuditEventType.Reconciled,operationId,operation.RequesterPrincipalId,operation.ClusterId,operation.OperationKind,
                operation.Risk.RiskClass,recovered.Snapshot.State,operation.ResourceKeys,operation.PreviewHash,"WorkflowCommitReceiptRecovered"),CancellationToken.None).ConfigureAwait(false);
            return Results.Ok(MutationStatusData.From(recovered.Snapshot));
        }).ConfigureAwait(false)).WithName("v09-request-recover").WithMetadata(new GovernanceRouteContract(AuthorizationAction.MutationReconcile,false,QueryKeys:"",ResponseType:typeof(MutationStatusData))).RequireKafdeckAntiforgery();
        return app;
    }
    private static ServiceRequestData Data(ServiceRequestRecord row,MutationOperationSnapshot? operation)=>new(row.RequestId,row.ClusterId,row.Revision,row.Version,row.BusinessState.ToString(),row.RequesterPrincipalId,
        row.ClientKind,row.Binding.OperationId,row.Binding.PreviewHash,row.Binding.MaterialHash,row.Binding.PolicyDigest,row.Reason,row.CreatedAtUtc,row.UpdatedAtUtc,row.ExpiresAtUtc,
        operation?.State.ToString(),operation?.ResultCode,row.Decisions,row.Binding);
    private static async Task<ServiceRequestData> CurrentData(ServiceRequestRecord row,MutationOperationSnapshot? operation,AdoGovernancePolicyStore policies,CancellationToken token)
    {
        if(operation is not null&&operation.DispatchStartedAtUtc is null&&row.BusinessState is not(ServiceRequestState.Rejected or ServiceRequestState.Cancelled))
        {
            try{await policies.ValidateBindingAsync(operation,token).ConfigureAwait(false);}
            catch(GovernanceEffectRejectedException e)when(e.Stale){row=row with{BusinessState=ServiceRequestState.NeedsReplan};}
        }
        return Data(row,operation);
    }
    private static bool Allowed(HttpContext h,KafdeckAuthorizationService auth,AuthorizationAction action,string cluster,string id)=>auth.Authorize(h.User,new(action,cluster,ServiceRequestContract.Resource(id)))==KafdeckAuthorizationOutcome.Allowed;
    private static bool Readable(HttpContext h,KafdeckAuthorizationService auth,ServiceRequestRecord row)=>Allowed(h,auth,AuthorizationAction.RequestRead,row.ClusterId,row.RequestId)&&
        row.Binding.AuthorizationTargets.All(t=>auth.Authorize(h.User,ServiceRequestMutationService.Visibility(row.Binding,t))==KafdeckAuthorizationOutcome.Allowed);
    private static int Size(int? size)=>size is null?50:size is >=1 and <=200?size.Value:throw new ArgumentException("Invalid page size.");
    private static string Scope(HttpContext h,KafdeckAuthorizationService auth,string scope)=>MutationActor.TryCreate(h.User,out var actor)&&actor is not null?
        GovernancePolicyCompiler.Hash(actor.PrincipalId+"\n"+auth.PolicyFingerprint+"\n"+scope):throw new ArgumentException("Missing current actor.");
    private static string WriteCursor(string scope,string after,IDataProtector p)=>p.Protect(scope+"\n"+after);
    private static string? ReadCursor(string? value,string scope,IDataProtector p)
    {if(value is null)return null;if(value.Length>2048)throw new ArgumentException("Invalid cursor.");var parts=p.Unprotect(value).Split('\n');return parts.Length==2&&parts[0]==scope?parts[1]:throw new ArgumentException("Cursor scope mismatch.");}
    private static IResult Denied()=>Problem(403,"ScopeDenied");
    private static IResult Problem(int status,string code)=>Results.Problem(statusCode:status,title:code,type:"urn:kafdeck:problem:"+code,extensions:new Dictionary<string,object?>{{"code",code}});
    private static async Task<IResult> Safe(Func<Task<IResult>> action)
    {
        try{return await action().ConfigureAwait(false);}
        catch(ServiceRequestConflictException e){return Problem(409,e.Code);}
        catch(GovernanceEffectRejectedException e){return Problem(e.Stale?409:403,e.Code);}
        catch(Exception e)when(e is ArgumentException or JsonException or CryptographicException or FormatException or OverflowException){return Problem(400,"InvalidRequest");}
        catch(OperationCanceledException){throw;}
        catch{return Problem(503,"GovernanceUnavailable");}
    }
}
