using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W74RequestRecoveryAuthorityTests
{
    [Fact]
    public void Direct_recovery_uses_verified_HTTP_principal_without_dispatch_context_and_retains_original_actor_checks()
    {
        const string issuer="https://request-recovery.example/realm";var now=DateTimeOffset.UtcNow;
        ClaimsPrincipal Human(string subject)=>OidcIdentityNormalizer.Normalize(new(new ClaimsIdentity([new Claim("sub",subject)],"oidc")),issuer,null,now);
        var owner=Human("owner");var reviewer=Human("reviewer");Assert.True(MutationActor.TryCreate(owner,out var actor));
        var snapshot=AuthorizationPolicyCompiler.Compile(new([
            new("owner",new[]{AuthorizationAction.RequestRead,AuthorizationAction.RequestSubmit,AuthorizationAction.TopicRead}.Select(action=>new AuthorizationPermissionDefinition(action,["alpha"],["*"])).ToArray()),
            new("reviewer",new[]{AuthorizationAction.RequestRead,AuthorizationAction.RequestDecide,AuthorizationAction.TopicRead}.Select(action=>new AuthorizationPermissionDefinition(action,["alpha"],["*"])).ToArray())],
            [new(issuer,"owner",["owner"]),new(issuer,"reviewer",["reviewer"])],[]));
        var auth=new KafdeckAuthorizationService(new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0",null,AccessMode.Oidc,null),[]),new(snapshot));
        var authorization=new MutationRequestAuthorizationService(auth);var accessor=new MutationExecutionRequestContextAccessor();
        var factory=new SqliteMutationDbConnectionFactory(Path.Combine(Path.GetTempPath(),"inert-request-authority-"+Guid.NewGuid().ToString("N")+".db"));
        var store=new AdoServiceRequestStore(factory,new AdoMutationOperationRepository(factory),new ServiceRequestAuthority(auth));
        var service=new ServiceRequestMutationService(store,new AdoGovernancePolicyStore(factory),accessor,authorization,auth);
        var target=MutationOperation.CreatePreview(actor!.PrincipalId,new(MutationOperationKind.TopicCreate,"alpha","{}",["orders"],AuthorizationTargets:[new(AuthorizationAction.TopicCreate,"alpha","orders")]),
            new(MutationRiskClass.Moderate,["unit-fixture"],MutationConfirmationMode.Explicit,false),"fixture",now.AddMinutes(5),now,"target").Snapshot;
        var plan=new ServiceRequestPlan(Guid.NewGuid(),"alpha","",Guid.NewGuid().ToString("N"),ServiceRequestCommand.Draft,0,actor.PrincipalId,"Browser",auth.PolicyFingerprint,
            Guid.NewGuid().ToString("D"),AdoServiceRequestStore.Bind(target),"unit fixture",now.AddMinutes(5),now,now.AddMinutes(5));
        plan=plan with{ContentHash=ServiceRequestContract.PlanHash(plan)};
        var operation=MutationOperation.CreatePreview(plan.PlanId,actor.PrincipalId,ServiceRequestMutation.Intent(plan),new(MutationRiskClass.Moderate,["unit-fixture"],MutationConfirmationMode.Explicit,false),
            authorization.PolicyVersionFor(owner,MutationOperationKind.ServiceRequestChange),now.AddMinutes(5),now,"request").Snapshot;
        Assert.Null(accessor.CurrentPrincipal);Assert.False(service.Current(plan,operation));
        Assert.True(service.CurrentForPrincipal(plan,operation,owner));
        Assert.False(service.CurrentForPrincipal(plan,operation,reviewer));Assert.False(service.CurrentForPrincipal(plan,operation,new ClaimsPrincipal()));
        using(accessor.Push(owner))Assert.True(service.Current(plan,operation));Assert.False(service.Current(plan,operation));
        var expired=plan with{ExpiresAtUtc=now.AddSeconds(-1)};
        Assert.False(service.CurrentForPrincipal(expired,operation,owner));Assert.True(service.CurrentForPrincipal(expired,operation,owner,true));
        Assert.False(service.CurrentForPrincipal(expired,operation,reviewer,true));
    }
}
