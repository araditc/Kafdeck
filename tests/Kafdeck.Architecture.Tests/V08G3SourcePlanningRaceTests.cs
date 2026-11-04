using System.Data.Common;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08G3SourcePlanningRaceTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task SQLite_plan_only_cannot_overwrite_or_accept_concurrently_advanced_source(bool cycleBack) => Run(false, cycleBack);

    [W68G3PostgresTheory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Real_PostgreSQL_plan_only_cannot_overwrite_or_accept_concurrently_advanced_source(bool cycleBack) => Run(true, cycleBack);

    private static async Task Run(bool postgres, bool cycleBack)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60)); var token = timeout.Token;
        await using var database = await W68G3PersistenceFixture.CreateAsync(postgres, token);
        var inner = database.Store; var wrapped = new PausingStore(inner);
        var repository = new AdoMutationOperationRepository(new MutationConnections(database.Connections));
        await repository.InitializeAsync(token);
        var planner = Human("planner"); var manager = Human("manager");
        MutationActor.TryCreate(planner, out var plannerActor); MutationActor.TryCreate(manager, out var managerActor);
        const string sourceRef = "race-source"; const string resource = "race-subscription";
        var manifestA = Manifest("Active"); var manifestB = Manifest("Paused");
        var hashA = DeclarativeSubscriptionManifest.Parse(Encoding.UTF8.GetBytes(manifestA), "json").Fingerprint();
        var anchor = Path.Combine(Path.GetTempPath(), "kafdeck-g3-planning-race-anchor-" + Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(anchor, JsonSerializer.Serialize(new { version = 1, deploymentId = W68G3PersistenceFixture.Deployment, generation = database.Generation, state = "active" }));
        try
        {
            var values = new Dictionary<string, string?>
            {
                ["Kafdeck:Declarative:Enabled"]="true",["Kafdeck:Declarative:DeploymentId"]=W68G3PersistenceFixture.Deployment,
                ["Kafdeck:Declarative:DeploymentGeneration"]=database.Generation.ToString("D"),["Kafdeck:Declarative:RecoveryAnchorFile"]=anchor,
                ["Kafdeck:Declarative:RecoveryAnchorSha256"]=Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(anchor))).ToLowerInvariant(),
            };
            AddController(0, "race-planner", plannerActor!.PrincipalId); AddController(1, "race-manager", managerActor!.PrincipalId);
            var config = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
            var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new(
                [new("planner", [new(AuthorizationAction.DeclarativeRead,[W68G3PersistenceFixture.Deployment],[resource]),
                    new(AuthorizationAction.DeclarativePlan,[W68G3PersistenceFixture.Deployment],[resource]),
                    new(AuthorizationAction.NotificationRead,null,[resource,"new"])]),
                 new("manager", [new(AuthorizationAction.DeclarativeSourceManage,[W68G3PersistenceFixture.Deployment],[sourceRef]),
                    new(AuthorizationAction.NotificationRead,null,[resource,"new"])])],
                [new("https://idp.example","planner",["planner"]),new("https://idp.example","manager",["manager"])], [])));
            var options = new KafdeckOptions(new("http://127.0.0.1:8080",null,AccessMode.Oidc,null),[],
                Administration:new AdministrationOptions(new MutationOptions(true,null,null,TimeSpan.FromMinutes(2),2)));
            var policy = new KafdeckAuthorizationService(options,evaluator);
            var authority = new DeclarativeRuntimeAuthorizer(config,policy);
            var requestAuthorization = new MutationRequestAuthorizationService(policy);
            Assert.True(authority.Authorize(planner,W68G3PersistenceFixture.Deployment,W68G3PersistenceFixture.Owner,sourceRef,hashA,resource,["new"],DeclarativeRuntimeVerb.Plan));
            Assert.False(authority.Authorize(planner,W68G3PersistenceFixture.Deployment,W68G3PersistenceFixture.Owner,sourceRef,hashA,resource,["new"],DeclarativeRuntimeVerb.SourceManage));
            Assert.False(authority.Authorize(planner,W68G3PersistenceFixture.Deployment,W68G3PersistenceFixture.Owner,sourceRef,hashA,resource,["new"],DeclarativeRuntimeVerb.Apply));
            using var services = new ServiceCollection().AddSingleton<IConfiguration>(config).BuildServiceProvider();
            var managerContext = new DefaultHttpContext { User=manager,RequestServices=services };
            var baseline = Data(await KafdeckDeclarativeSourceEndpoints.AdvanceAsync(sourceRef,
                new(manifestA,"json",Guid.Empty,0,hashA),managerContext,inner,database.Routing,authority,token));
            var plannerContext = new DefaultHttpContext { User=planner,RequestServices=services };
            var idempotency = Guid.NewGuid().ToString("N"); plannerContext.Request.Headers["Idempotency-Key"]=idempotency;
            var operationId = MutationIdempotency.DeriveOperationId(plannerActor.PrincipalId,W68G3PersistenceFixture.Deployment,
                MutationOperationKind.DeclarativeNotificationSubscription,idempotency);
            var service = new DeclarativeEndpointService(wrapped,database.Routing,authority,new(repository,requestAuthorization,options),
                repository,null!,policy,null!,new(wrapped,authority),requestAuthorization);
            var planning = service.HandleAsync("plan",new(manifestA,"json",sourceRef,hashA,0),plannerContext,token);
            try
            {
                // Ownership is read only after HandleAsync captured and authorized A.
                // No database transaction is held by this deterministic barrier.
                var reached = await Task.WhenAny(wrapped.Reached.Task,planning).WaitAsync(token);
                Assert.Same(wrapped.Reached.Task,reached);
                Assert.NotNull(wrapped.CapturedSource);
                Assert.Equal(baseline.Revision,wrapped.CapturedSource!.Revision);
                Assert.Equal(hashA,wrapped.CapturedSource.PinnedRevision);
                var advanced = Data(await KafdeckDeclarativeSourceEndpoints.AdvanceAsync(sourceRef,
                    new(manifestB,"json",baseline.Generation,baseline.Revision,baseline.SourceHash),managerContext,inner,database.Routing,authority,token));
                if(cycleBack) advanced=Data(await KafdeckDeclarativeSourceEndpoints.AdvanceAsync(sourceRef,
                    new(manifestA,"json",advanced.Generation,advanced.Revision,advanced.SourceHash),managerContext,inner,database.Routing,authority,token));
                var sourceAfterManager = await inner.GetRegistrationAsync(W68RuntimeRegistrationKind.Source,W68G3PersistenceFixture.Deployment,sourceRef,token);
                var auditAfterManager = await SourceAdvanceAuditCount(database.Connections,token);
                wrapped.Resume.TrySetResult();
                var response = await planning.WaitAsync(token);
                Assert.Equal(409,((IStatusCodeHttpResult)response).StatusCode);
                var current = await inner.GetRegistrationAsync(W68RuntimeRegistrationKind.Source,W68G3PersistenceFixture.Deployment,sourceRef,token);
                Assert.Equal(sourceAfterManager,current);
                Assert.Equal(advanced.SourceHash,current!.PinnedRevision); Assert.Equal(advanced.Revision,current.Revision);
                Assert.Equal(cycleBack?3:2,current.Revision);
                Assert.Equal(auditAfterManager,await SourceAdvanceAuditCount(database.Connections,token));
                Assert.Equal(0,wrapped.SourceRegistrationWrites); Assert.Equal(0,wrapped.PlanWrites);
                Assert.Equal(0,wrapped.EffectWrites);
                Assert.Null(await inner.GetPlanAsync(operationId,token)); Assert.Null(await repository.GetAsync(operationId,token));
                Assert.Null(await inner.GetCommitReceiptAsync(operationId,token));
                Assert.Null(await database.Routing.GetSubscriptionAsync(resource,token));
                Assert.Null(await inner.GetOwnershipAsync(new(W68G3PersistenceFixture.Deployment,"Deployment",W68G3PersistenceFixture.Deployment,"NotificationSubscription",resource),token));
            }
            finally { wrapped.Resume.TrySetResult(); await planning.WaitAsync(token); }

            void AddController(int index,string id,string principal)
            {
                var p=$"Kafdeck:Declarative:Registrations:{index}:";
                foreach(var pair in new Dictionary<string,string> { ["Id"]=id,["OwnerId"]=W68G3PersistenceFixture.Owner,["SourceRef"]=sourceRef,
                    ["SourceRevision"]=hashA,["PrincipalId"]=principal,["Enabled"]="true",["AllowRevisionAdvance"]="true",
                    ["ExpiresAtUtc"]=DateTimeOffset.UtcNow.AddHours(1).ToString("O"),["ResourceIds:0"]=resource }) values[p+pair.Key]=pair.Value;
            }
        }
        finally { File.Delete(anchor); }
    }

    private static ClaimsPrincipal Human(string subject) => OidcIdentityNormalizer.Normalize(
        new(new ClaimsIdentity([new Claim("sub",subject)],"oidc")),"https://idp.example",null,DateTimeOffset.UtcNow);
    private static string Manifest(string state) => JsonSerializer.Serialize(new { apiVersion="kafdeck.io/v0.8",kind="NotificationSubscription",
        metadata=new {deploymentRef=W68G3PersistenceFixture.Deployment,name="race-subscription",ownerRef=W68G3PersistenceFixture.Owner},
        spec=new {destinationRef="new",state,eventClasses=new[]{"Operational"},eventTypes=Array.Empty<string>()} });
    private static DeclarativeSourceRevisionData Data(IResult result)
    { Assert.Equal(200,((IStatusCodeHttpResult)result).StatusCode);return Assert.IsType<DeclarativeSourceRevisionData>(((IValueHttpResult)result).Value); }
    private static async Task<long> SourceAdvanceAuditCount(INotificationDeliveryDbConnectionFactory factory,CancellationToken token)
    {
        await using var c=await factory.OpenAsync(token);await using var q=c.CreateCommand();
        // All fixture owner/controller registrations remain revision 1; only
        // the source manager creates Registration revisions 2 and later.
        q.CommandText="SELECT COUNT(*) FROM kafdeck_declarative_runtime_audit WHERE event_kind='Registration' AND event_revision>=2";
        return Convert.ToInt64(await q.ExecuteScalarAsync(token));
    }
    private sealed class MutationConnections(INotificationDeliveryDbConnectionFactory inner):IMutationDbConnectionFactory
    { public bool SupportsSelectForUpdate=>inner.SupportsSelectForUpdate;public ValueTask<DbConnection> OpenAsync(CancellationToken token=default)=>inner.OpenAsync(token); }

    private sealed class PausingStore(IW68RuntimeStore inner):IW68RuntimeStore
    {
        public TaskCompletionSource Reached {get;}=new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Resume {get;}=new(TaskCreationOptions.RunContinuationsAsynchronously);
        public W68RuntimeRegistration? CapturedSource {get;private set;}
        public int SourceRegistrationWrites {get;private set;} public int PlanWrites {get;private set;} public int EffectWrites {get;private set;}
        public async Task<W68RuntimeOwnership?> GetOwnershipAsync(W68RuntimeResourceKey key,CancellationToken token=default)
        {var value=await inner.GetOwnershipAsync(key,token);Reached.TrySetResult();await Resume.Task.WaitAsync(token);return value;}
        public async Task<W68RuntimeRegistration?> GetRegistrationAsync(W68RuntimeRegistrationKind kind,string deployment,string id,CancellationToken token=default)
        {var value=await inner.GetRegistrationAsync(kind,deployment,id,token);if(kind==W68RuntimeRegistrationKind.Source&&CapturedSource is null)CapturedSource=value;return value;}
        public Task<W68RuntimeRegistration?> UpsertRegistrationAsync(W68RuntimeRegistration value,long revision,Func<CancellationToken,Task<bool>> authority,CancellationToken token=default)
        {if(value.Kind==W68RuntimeRegistrationKind.Source)SourceRegistrationWrites++;return inner.UpsertRegistrationAsync(value,revision,authority,token);}
        public Task<W68StoredRuntimePlan?> SavePlanAsync(W68StoredRuntimePlan value,Func<W68StoredRuntimePlan,CancellationToken,Task<bool>> authority,CancellationToken token=default)
        {PlanWrites++;return inner.SavePlanAsync(value,authority,token);}
        public Task<W68RuntimeCommitReceipt?> ApplySubscriptionAsync(Guid id,string hash,string principal,Func<W68StoredRuntimePlan,CancellationToken,Task<bool>> authority,CancellationToken token=default)
        {EffectWrites++;return inner.ApplySubscriptionAsync(id,hash,principal,authority,token);}
        public Task InitializeAsync(CancellationToken token=default)=>inner.InitializeAsync(token);
        public Task<DateTimeOffset> ReadDatabaseUtcNowAsync(CancellationToken token=default)=>inner.ReadDatabaseUtcNowAsync(token);
        public Task<Guid?> GetDeploymentGenerationAsync(string deployment,CancellationToken token=default)=>inner.GetDeploymentGenerationAsync(deployment,token);
        public Task<W68StoredRuntimePlan?> GetPlanAsync(Guid id,CancellationToken token=default)=>inner.GetPlanAsync(id,token);
        public Task<W68StoredRuntimePlan?> LoadPlanAsync(Guid id,CancellationToken token=default)=>inner.LoadPlanAsync(id,token);
        public Task<W68RuntimeLease?> AcquireLeaseAsync(string deployment,string scope,string controller,string owner,string principal,string holder,int ttl,Func<CancellationToken,Task<bool>> authority,CancellationToken token=default)=>inner.AcquireLeaseAsync(deployment,scope,controller,owner,principal,holder,ttl,authority,token);
        public Task<W68RuntimeLease?> RenewLeaseAsync(W68RuntimeLease lease,string controller,string owner,string principal,int ttl,Func<CancellationToken,Task<bool>> authority,CancellationToken token=default)=>inner.RenewLeaseAsync(lease,controller,owner,principal,ttl,authority,token);
        public Task<bool> ReleaseLeaseAsync(W68RuntimeLease lease,string controller,string owner,string principal,Func<CancellationToken,Task<bool>> authority,CancellationToken token=default)=>inner.ReleaseLeaseAsync(lease,controller,owner,principal,authority,token);
        public Task<W68RuntimeCommitReceipt?> GetCommitReceiptAsync(Guid id,CancellationToken token=default)=>inner.GetCommitReceiptAsync(id,token);
        public Task<W68RuntimeCommitReceipt?> GetVerifiedCommitReceiptAsync(Guid id,string hash,string principal,Func<W68StoredRuntimePlan,CancellationToken,Task<bool>> authority,CancellationToken token=default)=>inner.GetVerifiedCommitReceiptAsync(id,hash,principal,authority,token);
        public Task<bool> IsPlanCurrentAsync(Guid id,string hash,Func<W68StoredRuntimePlan,CancellationToken,Task<bool>> authority,CancellationToken token=default)=>inner.IsPlanCurrentAsync(id,hash,authority,token);
    }
}
