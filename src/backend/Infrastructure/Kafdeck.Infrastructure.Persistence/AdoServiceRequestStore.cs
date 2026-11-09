using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>The owning request repository. Business decisions never change kernel operation state.
/// Its approval reader runs on the effect's existing transaction, including resumable workers.</summary>
public sealed class AdoServiceRequestStore(IMutationDbConnectionFactory connections,
    IMutationOperationTransactionReader operations,IServiceRequestCurrentAuthority authority,
    IGovernanceGenerationTransactionGate? generationGate=null):IApprovedServiceRequestTransactionReader
{
    public const string SchemaManifest="""
        CREATE TABLE IF NOT EXISTS service_request_schema(version INTEGER PRIMARY KEY,fingerprint TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS service_request_heads(
          request_id TEXT PRIMARY KEY,cluster_id TEXT NOT NULL,operation_id TEXT NOT NULL UNIQUE,
          version BIGINT NOT NULL CHECK(version>0),revision BIGINT NOT NULL CHECK(revision>0),state INTEGER NOT NULL,
          requester TEXT NOT NULL,body TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS service_request_scope_page ON service_request_heads(cluster_id,request_id);
        CREATE TABLE IF NOT EXISTS service_request_revisions(
          request_id TEXT NOT NULL,revision BIGINT NOT NULL,body TEXT NOT NULL,
          PRIMARY KEY(request_id,revision),FOREIGN KEY(request_id) REFERENCES service_request_heads(request_id));
        CREATE TABLE IF NOT EXISTS service_request_decisions(
          request_id TEXT NOT NULL,revision BIGINT NOT NULL,actor TEXT NOT NULL,body TEXT NOT NULL,
          PRIMARY KEY(request_id,revision,actor),FOREIGN KEY(request_id,revision) REFERENCES service_request_revisions(request_id,revision));
        CREATE TABLE IF NOT EXISTS service_request_events(
          request_id TEXT NOT NULL,version BIGINT NOT NULL,operation_id TEXT NOT NULL UNIQUE,body TEXT NOT NULL,
          PRIMARY KEY(request_id,version),FOREIGN KEY(request_id) REFERENCES service_request_heads(request_id));
        CREATE TABLE IF NOT EXISTS service_request_plans(plan_id TEXT PRIMARY KEY,cluster_id TEXT NOT NULL,content_hash TEXT NOT NULL,body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS service_request_receipts(operation_id TEXT PRIMARY KEY,request_id TEXT NOT NULL,content_hash TEXT NOT NULL,body TEXT NOT NULL,
          FOREIGN KEY(request_id) REFERENCES service_request_heads(request_id));
        """;
    public static readonly string SchemaFingerprint=GovernancePolicyCompiler.Hash(SchemaManifest.Replace("\r\n","\n",StringComparison.Ordinal));
    public string DeploymentGeneration=>generationGate?.DeploymentGeneration??"unconfigured";

    public async Task InitializeAsync(CancellationToken token=default)
    {
        await using var db=await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx=await db.BeginTransactionAsync(token).ConfigureAwait(false);
        if(connections.SupportsSelectForUpdate)await Run(db,tx,"SELECT pg_advisory_xact_lock(64964007401)",token).ConfigureAwait(false);
        await Run(db,tx,SchemaManifest,token).ConfigureAwait(false);
        await Run(db,tx,"INSERT INTO service_request_schema(version,fingerprint) VALUES(1,@h) ON CONFLICT(version) DO NOTHING",token,("@h",SchemaFingerprint)).ConfigureAwait(false);
        await using(var cmd=Command(db,tx,"SELECT version,fingerprint FROM service_request_schema"))
        await using(var r=await cmd.ExecuteReaderAsync(token).ConfigureAwait(false))
            if(!await r.ReadAsync(token).ConfigureAwait(false)||r.GetInt32(0)!=1||r.GetString(1)!=SchemaFingerprint||await r.ReadAsync(token).ConfigureAwait(false))
                throw new InvalidOperationException("Unsupported request migration ledger.");
        await tx.CommitAsync(token).ConfigureAwait(false);
    }
    public static string MaterialHash(MutationOperationSnapshot operation)=>GovernancePolicyCompiler.Hash(JsonSerializer.Serialize(new{
        operation.OperationId,operation.RequesterPrincipalId,operation.ClusterId,operation.OperationKind,operation.CanonicalIntentHash,
        operation.ResourceKeys,operation.MaterialDigests,operation.AuthorizationTargets,operation.Risk,operation.PreviewHash},ServiceRequestContract.Json));
    public static string PolicyDigest(MutationOperationSnapshot operation)=>ServiceRequestContract.PolicyDigest(operation.Preconditions
        .Where(p=>p.Key.StartsWith(AdoGovernancePolicyStore.PreconditionKey+":",StringComparison.Ordinal)).Select(p=>KeyValuePair.Create(p.Key,p.Fingerprint)));
    public static ServiceRequestBinding Bind(MutationOperationSnapshot operation)=>new(operation.OperationId,operation.ClusterId,
        operation.RequesterPrincipalId,operation.PreviewHash,MaterialHash(operation),PolicyDigest(operation),operation.OperationKind.ToString(),
        operation.Risk.RiskClass.ToString(),operation.ResourceKeys.ToArray(),operation.AuthorizationTargets.Select(t=>new ServiceRequestTarget(t.Action,t.ClusterId,t.ResourceName)).ToArray(),operation.PreviewExpiresAtUtc);
    public static bool Matches(ServiceRequestBinding binding,MutationOperationSnapshot operation)=>
        binding.OperationId==operation.OperationId&&binding.ClusterId==operation.ClusterId&&binding.OriginalRequesterPrincipalId==operation.RequesterPrincipalId&&
        binding.PreviewHash==operation.PreviewHash&&binding.MaterialHash==MaterialHash(operation)&&binding.PolicyDigest==PolicyDigest(operation);
    public Task<ServiceRequestRecord?> GetAsync(string cluster,string id,CancellationToken token=default)=>Read(async(db,tx)=>
        {var row=await Load(db,tx,id,token).ConfigureAwait(false);return row?.ClusterId==cluster?Project(row,await operations.ReadAsync(db,tx,row.Binding.OperationId,token).ConfigureAwait(false),await Now(db,tx,token).ConfigureAwait(false)):null;},token);
    public Task<ServiceRequestPlan?> GetPlanAsync(Guid id,CancellationToken token=default)=>Read((db,tx)=>LoadJson<ServiceRequestPlan>(db,tx,"SELECT body FROM service_request_plans WHERE plan_id=@id",token,("@id",id.ToString("D"))),token);
    public Task<ServiceRequestReceipt?> GetReceiptAsync(Guid id,CancellationToken token=default)=>Read((db,tx)=>LoadJson<ServiceRequestReceipt>(db,tx,"SELECT body FROM service_request_receipts WHERE operation_id=@id",token,("@id",id.ToString("D"))),token);

    public Task<IReadOnlyList<ServiceRequestRecord>> ListAuthorizedAsync(string cluster,string? after,int size,
        Func<ServiceRequestRecord,bool> visible,CancellationToken token=default)=>Read<IReadOnlyList<ServiceRequestRecord>>(async(db,tx)=>
    {
        ServiceRequestContract.Bound(cluster,256);if(after is not null)ServiceRequestContract.Id(after);if(size is <1 or >200)throw new ArgumentException("Invalid request page.");
        var rows=new List<ServiceRequestRecord>();
        await using(var cmd=Command(db,tx,"SELECT body FROM service_request_heads WHERE cluster_id=@c AND request_id>@after ORDER BY request_id LIMIT 10001",("@c",cluster),("@after",after??"")))
        await using(var reader=await cmd.ExecuteReaderAsync(token).ConfigureAwait(false))
        {while(await reader.ReadAsync(token).ConfigureAwait(false)){var row=Deserialize<ServiceRequestRecord>(reader.GetString(0));if(visible(row)){rows.Add(row);if(rows.Count>size)break;}}}
        var now=await Now(db,tx,token).ConfigureAwait(false);
        for(var i=0;i<rows.Count;i++)rows[i]=Project(rows[i],await operations.ReadAsync(db,tx,rows[i].Binding.OperationId,token).ConfigureAwait(false),now);
        return rows;
    },token);
    public Task<IReadOnlyList<ServiceRequestHistory>> HistoryAuthorizedAsync(string cluster,string id,long after,int size,
        Func<ServiceRequestRecord,bool> visible,CancellationToken token=default)=>Read<IReadOnlyList<ServiceRequestHistory>>(async(db,tx)=>
    {
        ServiceRequestContract.Id(id);if(after<0||size is <1 or >200)throw new ArgumentException("Invalid history page.");
        var result=new List<ServiceRequestHistory>();
        await using var cmd=Command(db,tx,"SELECT body FROM service_request_events WHERE request_id=@id AND version>@v ORDER BY version LIMIT 10001",("@id",id),("@v",after));
        await using var reader=await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        while(await reader.ReadAsync(token).ConfigureAwait(false)){var e=Deserialize<ServiceRequestHistory>(reader.GetString(0));if(e.Request.ClusterId==cluster&&visible(e.Request)){result.Add(e);if(result.Count>size)break;}}
        return result;
    },token);
    public Task<ServiceRequestPlan> StageAsync(ServiceRequestPlan plan,CancellationToken token=default)=>WithTransactionAsync(async(db,tx)=>
    {
        ServiceRequestContract.Validate(plan);
        if(plan.DeploymentGeneration!=DeploymentGeneration)throw new ServiceRequestConflictException("RequestRestoreGenerationChanged");
        var existing=await LoadJson<ServiceRequestPlan>(db,tx,"SELECT body FROM service_request_plans WHERE plan_id=@id",token,("@id",plan.PlanId.ToString("D"))).ConfigureAwait(false);
        if(existing is not null){if(existing.ContentHash!=plan.ContentHash)throw new ServiceRequestConflictException("RequestIdempotencyConflict");return existing;}
        var count=Convert.ToInt64(await Scalar(db,tx,"SELECT COUNT(*) FROM service_request_plans WHERE cluster_id=@c",token,("@c",plan.ClusterId)).ConfigureAwait(false),CultureInfo.InvariantCulture);
        if(count>=ServiceRequestContract.MaximumRequestsPerCluster*10)throw new ServiceRequestConflictException("RequestPlanCapacityExceeded");
        await Run(db,tx,"INSERT INTO service_request_plans(plan_id,cluster_id,content_hash,body) VALUES(@id,@c,@h,@body)",token,
            ("@id",plan.PlanId.ToString("D")),("@c",plan.ClusterId),("@h",plan.ContentHash),("@body",Serialize(plan))).ConfigureAwait(false);return plan;
    },token,readOnly:false);

    public async Task<ServiceRequestReceipt> CommitAsync(ServiceRequestPlan plan,MutationOperationSnapshot changeOperation,
        Func<bool> currentChangeAuthority,Func<DbConnection,DbTransaction,MutationOperationSnapshot,CancellationToken,Task> validateBindingDigests,
        CancellationToken token=default)
    {
        try{return await CommitCoreAsync(plan,changeOperation,currentChangeAuthority,validateBindingDigests,token).ConfigureAwait(false);}
        catch(Microsoft.Data.Sqlite.SqliteException e)when(e.SqliteErrorCode is 5 or 6){throw new ServiceRequestConflictException("RequestVersionConflict");}
        catch(Npgsql.PostgresException e)when(e.SqlState is "40001" or "40P01"){throw new ServiceRequestConflictException("RequestVersionConflict");}
        catch(Microsoft.Data.Sqlite.SqliteException e)when(e.SqliteErrorCode==19){throw new ServiceRequestConflictException("RequestOperationAlreadyBound");}
        catch(Npgsql.PostgresException e)when(e.SqlState=="23505"){throw new ServiceRequestConflictException("RequestOperationAlreadyBound");}
    }
    private async Task<ServiceRequestReceipt> CommitCoreAsync(ServiceRequestPlan plan,MutationOperationSnapshot changeOperation,
        Func<bool> currentChangeAuthority,Func<DbConnection,DbTransaction,MutationOperationSnapshot,CancellationToken,Task> validateBindingDigests,
        CancellationToken token=default)
    {
        await using var db=await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx=await db.BeginTransactionAsync(token).ConfigureAwait(false);
        await Generation(db,tx,token).ConfigureAwait(false);
        ServiceRequestContract.Validate(plan);
        if(plan.DeploymentGeneration!=DeploymentGeneration||!currentChangeAuthority())throw new ServiceRequestConflictException("RequestCurrentAuthorityDenied");
        var saved=await LoadJson<ServiceRequestPlan>(db,tx,"SELECT body FROM service_request_plans WHERE plan_id=@id",token,("@id",plan.PlanId.ToString("D"))).ConfigureAwait(false);
        if(saved?.ContentHash!=plan.ContentHash||changeOperation.OperationId!=plan.PlanId||changeOperation.RequesterPrincipalId!=plan.RequesterPrincipalId)
            throw new ServiceRequestConflictException("RequestMaterialChanged");
        var reference=JsonSerializer.Deserialize<RequestCommitReference>(changeOperation.CanonicalIntent,ServiceRequestContract.Json);
        var isDecision=ServiceRequestContract.IsDecision(plan.Command);
        if(reference is null||reference.Schema!="request-v1"||reference.PlanId!=plan.PlanId||reference.ContentHash!=plan.ContentHash||reference.DeploymentGeneration!=DeploymentGeneration||
            changeOperation.ClusterId!=plan.ClusterId||changeOperation.OperationKind!=(isDecision?MutationOperationKind.ServiceRequestDecision:MutationOperationKind.ServiceRequestChange)||
            changeOperation.AuthorizationTargets.Count!=1||changeOperation.AuthorizationTargets[0]!=new MutationAuthorizationTarget(isDecision?AuthorizationAction.RequestDecide:AuthorizationAction.RequestSubmit,plan.ClusterId,ServiceRequestContract.Resource(plan.RequestId))||
            changeOperation.ResourceKeys.Count!=1||changeOperation.ResourceKeys[0]!=ServiceRequestContract.Resource(plan.RequestId)||changeOperation.MaterialDigests.Count!=0||
            !changeOperation.Preconditions.Contains(new("request.material",plan.ContentHash))||changeOperation.Risk.RiskClass!=MutationRiskClass.Moderate||changeOperation.Risk.RequiresIndependentApproval||changeOperation.Risk.ConfirmationMode!=MutationConfirmationMode.Explicit)
            throw new ServiceRequestConflictException("RequestMaterialChanged");
        var receipt=await LoadJson<ServiceRequestReceipt>(db,tx,"SELECT body FROM service_request_receipts WHERE operation_id=@id",token,("@id",plan.PlanId.ToString("D"))).ConfigureAwait(false);
        if(receipt is not null){if(receipt.ContentHash!=plan.ContentHash||receipt.PreviewHash!=changeOperation.PreviewHash)throw new ServiceRequestConflictException("RequestReceiptConflict");return receipt;}
        var target=await operations.ReadAsync(db,tx,plan.Binding.OperationId,token).ConfigureAwait(false)??throw new ServiceRequestConflictException("RequestNeedsReplan");
        var now=await Now(db,tx,token).ConfigureAwait(false);
        if(plan.ExpiresAtUtc<=now||!Matches(plan.Binding,target)||plan.RequestExpiresAtUtc<=now||target.PreviewExpiresAtUtc<=now)
            throw new ServiceRequestConflictException("RequestNeedsReplan");
        if(plan.Command!=ServiceRequestCommand.Cancel)await validateBindingDigests(db,tx,target,token).ConfigureAwait(false);
        if(connections.SupportsSelectForUpdate)await Run(db,tx,"SELECT pg_advisory_xact_lock(64964007402)",token).ConfigureAwait(false);
        var old=await Load(db,tx,plan.RequestId,token,true).ConfigureAwait(false);
        if((old?.Version??0)!=plan.ExpectedVersion||old is not null&&old.ClusterId!=plan.ClusterId)throw new ServiceRequestConflictException("RequestVersionConflict");
        if(old is not null&&old.Version>=ServiceRequestContract.MaximumEventsPerRequest)throw new ServiceRequestConflictException("RequestHistoryCapacityExceeded");
        if(old is not null&&old.Binding.OperationId!=target.OperationId)
        {
            var previous=await operations.ReadAsync(db,tx,old.Binding.OperationId,token).ConfigureAwait(false);
            if(previous is null||IsEffectStarted(previous))throw new ServiceRequestConflictException("RequestEffectAlreadyDispatched");
        }
        if(IsEffectStarted(target))throw new ServiceRequestConflictException("RequestEffectAlreadyDispatched");
        var principal=plan.RequesterPrincipalId;
        ServiceRequestRecord row;
        if(plan.Command==ServiceRequestCommand.Draft)
        {
            if(old is not null||principal!=target.RequesterPrincipalId)throw new ServiceRequestConflictException("RequestOriginalRequesterRequired");
            var capacity=Convert.ToInt64(await Scalar(db,tx,"SELECT COUNT(*) FROM service_request_heads WHERE cluster_id=@c",token,("@c",plan.ClusterId)).ConfigureAwait(false),CultureInfo.InvariantCulture);
            if(capacity>=ServiceRequestContract.MaximumRequestsPerCluster)throw new ServiceRequestConflictException("RequestCapacityExceeded");
            row=new(plan.RequestId,plan.ClusterId,1,1,ServiceRequestState.Draft,principal,plan.ClientKind,plan.Binding,plan.Reason,now,now,plan.RequestExpiresAtUtc,[]);
            await Run(db,tx,"INSERT INTO service_request_heads(request_id,cluster_id,operation_id,version,revision,state,requester,body) VALUES(@id,@c,@op,1,1,@s,@actor,@body)",token,
                ("@id",row.RequestId),("@c",row.ClusterId),("@op",row.Binding.OperationId.ToString("D")),("@s",(int)row.BusinessState),("@actor",row.RequesterPrincipalId),("@body",Serialize(row))).ConfigureAwait(false);
            await SaveRevision(db,tx,row,token).ConfigureAwait(false);
        }
        else
        {
            if(old is null)throw new ServiceRequestConflictException("RequestNotFound");
            var projected=Project(old,target,now);
            if(ServiceRequestContract.IsDecision(plan.Command))
            {
                if(ServiceRequestContract.SameHuman(principal,old.RequesterPrincipalId)||plan.ClientKind=="Automation"||projected.BusinessState is not(ServiceRequestState.Submitted or ServiceRequestState.Approved)||
                    !Matches(old.Binding,target)||!authority.IsCurrentHumanDecisionAllowed(principal,new(AuthorizationAction.RequestDecide,old.ClusterId,ServiceRequestContract.Resource(old.RequestId)))||
                    target.AuthorizationTargets.Any(t=>!authority.IsCurrentRequesterAllowed(principal,VisibilityRequest(target,t))))
                    throw new ServiceRequestConflictException("RequestEligibleDistinctHumanRequired");
                if(old.Decisions.Any(d=>ServiceRequestContract.SameHuman(d.ActorPrincipalId,principal)))throw new ServiceRequestConflictException("RequestDecisionAlreadyRecorded");
                var decision=new ServiceRequestDecision(principal,plan.Command==ServiceRequestCommand.Approve,plan.Reason,plan.AuthorityFingerprint,now);
                row=old with{Version=checked(old.Version+1),BusinessState=decision.Approved?ServiceRequestState.Approved:ServiceRequestState.Rejected,
                    Decisions=old.Decisions.Append(decision).ToArray(),UpdatedAtUtc=now};
                await Run(db,tx,"INSERT INTO service_request_decisions(request_id,revision,actor,body) VALUES(@id,@r,@actor,@body)",token,
                    ("@id",row.RequestId),("@r",row.Revision),("@actor",principal),("@body",Serialize(decision))).ConfigureAwait(false);
            }
            else
            {
                if(principal!=old.RequesterPrincipalId||target.RequesterPrincipalId!=old.RequesterPrincipalId)throw new ServiceRequestConflictException("RequestOriginalRequesterRequired");
                row=plan.Command switch
                {
                    ServiceRequestCommand.Revise when projected.BusinessState is ServiceRequestState.Draft or ServiceRequestState.NeedsReplan or ServiceRequestState.Rejected or ServiceRequestState.Expired or ServiceRequestState.Cancelled =>
                        old with{Revision=checked(old.Revision+1),Version=checked(old.Version+1),BusinessState=ServiceRequestState.Draft,Binding=plan.Binding,Reason=plan.Reason,
                            ExpiresAtUtc=plan.RequestExpiresAtUtc,Decisions=[],UpdatedAtUtc=now},
                    ServiceRequestCommand.Submit when projected.BusinessState==ServiceRequestState.Draft&&Matches(old.Binding,target)=>old with{Version=checked(old.Version+1),BusinessState=ServiceRequestState.Submitted,UpdatedAtUtc=now},
                    ServiceRequestCommand.Cancel when projected.BusinessState is ServiceRequestState.Draft or ServiceRequestState.Submitted or ServiceRequestState.Approved=>old with{Version=checked(old.Version+1),BusinessState=ServiceRequestState.Cancelled,UpdatedAtUtc=now},
                    _=>throw new ServiceRequestConflictException("RequestTransitionDenied")
                };
                if(plan.Command==ServiceRequestCommand.Revise)await SaveRevision(db,tx,row,token).ConfigureAwait(false);
            }
            if(await Run(db,tx,"UPDATE service_request_heads SET operation_id=@op,version=@v,revision=@r,state=@s,body=@body WHERE request_id=@id AND version=@old",token,
                ("@op",row.Binding.OperationId.ToString("D")),("@v",row.Version),("@r",row.Revision),("@s",(int)row.BusinessState),("@body",Serialize(row)),("@id",row.RequestId),("@old",old.Version)).ConfigureAwait(false)!=1)
                throw new ServiceRequestConflictException("RequestVersionConflict");
        }
        var history=new ServiceRequestHistory(row,principal,plan.PlanId,now);
        await Run(db,tx,"INSERT INTO service_request_events(request_id,version,operation_id,body) VALUES(@id,@v,@op,@body)",token,
            ("@id",row.RequestId),("@v",row.Version),("@op",plan.PlanId.ToString("D")),("@body",Serialize(history))).ConfigureAwait(false);
        receipt=new(plan.PlanId,row.RequestId,plan.ContentHash,changeOperation.PreviewHash,principal,DeploymentGeneration,row.Version,now,row);
        await Run(db,tx,"INSERT INTO service_request_receipts(operation_id,request_id,content_hash,body) VALUES(@op,@id,@h,@body)",token,
            ("@op",plan.PlanId.ToString("D")),("@id",row.RequestId),("@h",plan.ContentHash),("@body",Serialize(receipt))).ConfigureAwait(false);
        if(!currentChangeAuthority())throw new ServiceRequestConflictException("RequestCurrentAuthorityDenied");
        await Generation(db,tx,token).ConfigureAwait(false);
        await tx.CommitAsync(token).ConfigureAwait(false);return receipt;
    }
    public async Task<GovernanceApprovedRequestFact?> ReadApprovedAsync(DbConnection db,DbTransaction tx,MutationOperationSnapshot operation,CancellationToken token)
    {
        var row=await LoadJson<ServiceRequestRecord>(db,tx,"SELECT body FROM service_request_heads WHERE operation_id=@op"+(connections.SupportsSelectForUpdate?" FOR UPDATE":""),token,("@op",operation.OperationId.ToString("D"))).ConfigureAwait(false);
        if(row is null||row.BusinessState!=ServiceRequestState.Approved||!Matches(row.Binding,operation)||row.ExpiresAtUtc<=await Now(db,tx,token).ConfigureAwait(false))return null;
        var request=new AuthorizationRequest(AuthorizationAction.RequestRead,row.ClusterId,ServiceRequestContract.Resource(row.RequestId));
        if(!authority.IsCurrentRequesterAllowed(row.RequesterPrincipalId,request)||
            !authority.IsCurrentRequesterAllowed(row.RequesterPrincipalId,request with{Action=AuthorizationAction.RequestSubmit})||
            operation.AuthorizationTargets.Any(t=>!authority.IsCurrentRequesterAllowed(row.RequesterPrincipalId,EffectRequest(operation,t))))return null;
        var decisions=row.Decisions;
        if(decisions.Any(d=>!d.Approved))return null;
        var approved=decisions.FirstOrDefault(d=>d.Approved&&!ServiceRequestContract.SameHuman(d.ActorPrincipalId,row.RequesterPrincipalId)&&
            authority.IsCurrentHumanDecisionAllowed(d.ActorPrincipalId,request with{Action=AuthorizationAction.RequestDecide})&&
            authority.IsCurrentRequesterAllowed(d.ActorPrincipalId,request)&&
            operation.AuthorizationTargets.All(t=>authority.IsCurrentRequesterAllowed(d.ActorPrincipalId,VisibilityRequest(operation,t))));
        return approved is null?null:new(operation.OperationId,operation.PreviewHash,MaterialHash(operation),PolicyDigest(operation),row.RequestId,row.Revision,approved.ActorPrincipalId);
    }
    public async Task VerifyReceiptOnTransactionAsync(DbConnection db,DbTransaction tx,ServiceRequestPlan plan,ServiceRequestReceipt receipt,
        MutationOperationSnapshot operation,Func<bool> current,CancellationToken token)
    {
        await Generation(db,tx,token).ConfigureAwait(false);
        var stored=await LoadJson<ServiceRequestReceipt>(db,tx,"SELECT body FROM service_request_receipts WHERE operation_id=@op",token,("@op",operation.OperationId.ToString("D"))).ConfigureAwait(false);
        if(!current()||stored is null||Serialize(stored)!=Serialize(receipt)||receipt.ContentHash!=plan.ContentHash||receipt.PreviewHash!=operation.PreviewHash||
            receipt.ActorPrincipalId!=operation.RequesterPrincipalId||receipt.DeploymentGeneration!=DeploymentGeneration)
            throw new ServiceRequestConflictException("RequestPositiveReceiptRequired");
    }
    private static bool IsEffectStarted(MutationOperationSnapshot operation)=>operation.DispatchStartedAtUtc is not null||operation.State is
        MutationOperationState.Executing or MutationOperationState.AppliedVerified or MutationOperationState.AppliedUnverified or MutationOperationState.PartiallyApplied or MutationOperationState.ExecutionUnknown;
    public static AuthorizationRequest EffectRequest(MutationOperationSnapshot operation,MutationAuthorizationTarget target)=>
        operation.OperationKind==MutationOperationKind.DeclarativeNotificationSubscription?DeclarativeMutationAuthorization.Request(target):
        new(target.Action,target.ClusterId=="$deployment"&&target.Action is AuthorizationAction.NotificationRead or AuthorizationAction.NotificationManage?null:target.ClusterId,target.ResourceName);
    public static AuthorizationRequest VisibilityRequest(MutationOperationSnapshot operation,MutationAuthorizationTarget target)
    {
        var request=EffectRequest(operation,target);
        var action=target.Action.ToString();
        return request with{Action=action.StartsWith("Topic",StringComparison.Ordinal)||action.StartsWith("Record",StringComparison.Ordinal)?AuthorizationAction.TopicRead:
            action.StartsWith("Consumer",StringComparison.Ordinal)?AuthorizationAction.ConsumerRead:action.StartsWith("Schema",StringComparison.Ordinal)?AuthorizationAction.SchemaRead:
            action.StartsWith("Connect",StringComparison.Ordinal)?AuthorizationAction.ConnectRead:action.StartsWith("Notification",StringComparison.Ordinal)?AuthorizationAction.NotificationRead:
            action.StartsWith("Declarative",StringComparison.Ordinal)?AuthorizationAction.DeclarativeRead:action.StartsWith("Catalog",StringComparison.Ordinal)?AuthorizationAction.CatalogRead:
            action.StartsWith("GovernancePolicy",StringComparison.Ordinal)?AuthorizationAction.GovernancePolicyRead:
            action.StartsWith("Request",StringComparison.Ordinal)?AuthorizationAction.RequestRead:action.StartsWith("RateCard",StringComparison.Ordinal)?AuthorizationAction.AttributionRead:AuthorizationAction.ClusterRead};
    }
    public static ServiceRequestRecord Project(ServiceRequestRecord row,MutationOperationSnapshot? operation,DateTimeOffset now)
    {
        if(IsTerminal(row.BusinessState))return row;
        if(operation is null||!Matches(row.Binding,operation)||operation.State==MutationOperationState.StalePreview)return row with{BusinessState=ServiceRequestState.NeedsReplan};
        if(!IsEffectStarted(operation)&&(row.ExpiresAtUtc<=now||operation.PreviewExpiresAtUtc<=now||operation.State==MutationOperationState.Expired))return row with{BusinessState=ServiceRequestState.Expired};
        return row;
    }
    private static bool IsTerminal(ServiceRequestState state)=>state is ServiceRequestState.Rejected or ServiceRequestState.Cancelled;
    private Task Generation(DbConnection db,DbTransaction tx,CancellationToken token)=>generationGate?.ValidateAsync(db,tx,token)??Task.CompletedTask;
    private Task<T> Read<T>(Func<DbConnection,DbTransaction,Task<T>> body,CancellationToken token) =>
        WithTransactionAsync(body,token,readOnly:true);

    private async Task<T> WithTransactionAsync<T>(Func<DbConnection,DbTransaction,Task<T>> body,CancellationToken token,bool readOnly)
    {
        await using var db = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx = await db.BeginTransactionAsync(token).ConfigureAwait(false);
        if (generationGate is not null)
        {
            if (readOnly) await generationGate.ValidateReadAsync(db, tx, token).ConfigureAwait(false);
            else await generationGate.ValidateAsync(db, tx, token).ConfigureAwait(false);
        }
        var value = await body(db, tx).ConfigureAwait(false);
        await tx.CommitAsync(token).ConfigureAwait(false);
        return value;
    }
    private static Task<ServiceRequestRecord?> Load(DbConnection db,DbTransaction tx,string id,CancellationToken token,bool locked=false)=>LoadJson<ServiceRequestRecord>(db,tx,"SELECT body FROM service_request_heads WHERE request_id=@id"+(locked&&db is Npgsql.NpgsqlConnection?" FOR UPDATE":""),token,("@id",id));
    private static Task<int> SaveRevision(DbConnection db,DbTransaction tx,ServiceRequestRecord row,CancellationToken token)=>Run(db,tx,"INSERT INTO service_request_revisions(request_id,revision,body) VALUES(@id,@r,@body)",token,("@id",row.RequestId),("@r",row.Revision),("@body",Serialize(row)));
    private async Task<DateTimeOffset> Now(DbConnection db,DbTransaction tx,CancellationToken token)
    {var value=await Scalar(db,tx,connections.SupportsSelectForUpdate?"SELECT CURRENT_TIMESTAMP":"SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')",token).ConfigureAwait(false);return value is DateTime d?new(DateTime.SpecifyKind(d,DateTimeKind.Utc)):value is DateTimeOffset o?o:DateTimeOffset.Parse(Convert.ToString(value,CultureInfo.InvariantCulture)!,CultureInfo.InvariantCulture);}
    private static string Serialize<T>(T value)=>JsonSerializer.Serialize(value,ServiceRequestContract.Json);
    [System.Text.Json.Serialization.JsonUnmappedMemberHandling(System.Text.Json.Serialization.JsonUnmappedMemberHandling.Disallow)]
    private sealed record RequestCommitReference(string Schema,Guid PlanId,string ContentHash,string DeploymentGeneration);
    private static T Deserialize<T>(string text)=>JsonSerializer.Deserialize<T>(text,ServiceRequestContract.Json)??throw new InvalidOperationException("Invalid stored request material.");
    private static async Task<T?> LoadJson<T>(DbConnection db,DbTransaction tx,string sql,CancellationToken token,params(string,object?)[] args)where T:class
    {var value=await Scalar(db,tx,sql,token,args).ConfigureAwait(false);return value is string text?Deserialize<T>(text):null;}
    private static DbCommand Command(DbConnection db,DbTransaction tx,string sql,params(string Name,object? Value)[] args)
    {var cmd=db.CreateCommand();cmd.Transaction=tx;cmd.CommandText=sql;foreach(var(name,value)in args){var p=cmd.CreateParameter();p.ParameterName=name;p.Value=value??DBNull.Value;cmd.Parameters.Add(p);}return cmd;}
    private static async Task<object?> Scalar(DbConnection db,DbTransaction tx,string sql,CancellationToken token,params(string,object?)[] args)
    {await using var cmd=Command(db,tx,sql,args);return await cmd.ExecuteScalarAsync(token).ConfigureAwait(false);}
    private static async Task<int> Run(DbConnection db,DbTransaction tx,string sql,CancellationToken token,params(string,object?)[] args)
    {await using var cmd=Command(db,tx,sql,args);return await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);}
}
