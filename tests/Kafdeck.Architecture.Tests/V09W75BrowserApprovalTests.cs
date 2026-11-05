using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W75BrowserApprovalTests
{
    private const string Issuer="https://browser.example/realms/governance";
    [Fact]
    public void Browser_only_activation_uses_current_direct_humans_without_enabling_bearer_approvals()
    {
        var configuration=new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string,string?>{{"Kafdeck:NonBrowserRead:Enabled","false"}}).Build();
        using var configurationLifetime=(IDisposable)configuration;
        var original=Policy();var current=original;
        var evaluator=new AuthorizationPolicyEvaluator(current,()=>current);
        var service=new KafdeckAuthorizationService(new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0",null,AccessMode.Oidc,null),[]),evaluator,new NonBrowserReadRegistry(configuration));
        var authorization=new MutationRequestAuthorizationService(service);
        var requester=Browser("requester");var reviewer=Browser("reviewer");
        var target=new AuthorizationRequest(AuthorizationAction.GovernancePolicyActivate,"alpha","active");
        Assert.True(service.IsLiveGovernanceApprovalAllowed(reviewer,target));
        var reviewerId=SecurityAuditPrincipal.FromOperatorKey(new(Issuer,"reviewer"));
        Assert.True(service.IsCurrentGovernanceApprovalAllowed(reviewerId,target));
        Assert.False(service.IsLiveGovernanceApprovalAllowed(reviewer,target with{ClusterId="hidden"}));
        Assert.False(service.IsLiveGovernanceApprovalAllowed(reviewer,target with{ResourceName="other"}));
        Assert.False(service.IsLiveGovernanceApprovalAllowed(reviewer,target with{Action=AuthorizationAction.TopicDelete}));
        Assert.False(service.IsLiveGovernanceApprovalAllowed(new ClaimsPrincipal(),target));
        Assert.False(service.IsCurrentGovernanceApprovalAllowed("oidc:malformed",target));
        // Legacy families retain the existing registration gate; the new exception is closed to kind 34.
        Assert.False(service.IsLiveApprovalAllowed(reviewer,target));
        Assert.False(service.IsCurrentApprovalAllowed(reviewerId,target));
        Assert.True(MutationActor.TryCreate(requester,out var actor));
        var intent=new MutationIntentDescriptor(MutationOperationKind.GovernancePolicyActivation,"alpha","{}",["policy:alpha:active"],AuthorizationTargets:[new(AuthorizationAction.GovernancePolicyActivate,"alpha","active")]);
        var aggregate=MutationOperation.CreatePreview(actor!.PrincipalId,intent,new(MutationRiskClass.Critical,["policy_activation"],MutationConfirmationMode.TypedTarget,true),
            authorization.PolicyVersionFor(requester,MutationOperationKind.GovernancePolicyActivation),DateTimeOffset.UtcNow.AddMinutes(2),DateTimeOffset.UtcNow,"browser-only");
        aggregate.OpenForConfirmation(DateTimeOffset.UtcNow);aggregate.Confirm(actor.PrincipalId,aggregate.Snapshot.PreviewHash,DateTimeOffset.UtcNow,aggregate.Snapshot.ConfirmationChallenge);
        Assert.True(authorization.IsApprovalAllowed(reviewer,aggregate.Snapshot));
        Assert.False(authorization.IsApprovalAllowed(requester,aggregate.Snapshot));
        var bearer=new ClaimsPrincipal(new NonBrowserReadIdentity("disabled-registration",new(Issuer,"reviewer","cli","api",DateTimeOffset.UtcNow.AddMinutes(1)),NonBrowserPrincipalKind.Human));
        Assert.False(service.IsLiveGovernanceApprovalAllowed(bearer,target));
        Assert.False(service.IsCurrentGovernanceApprovalAllowed(((NonBrowserReadIdentity)bearer.Identity!).AuditId,target));
        Assert.False(authorization.IsApprovalAllowed(bearer,aggregate.Snapshot));
        Assert.True(OperatorSessionContextFactory.TryCreate(reviewer,out var session));
        var evidence=new MutationApprovalAuthorizer(evaluator).Authorize(session!.Identity,aggregate.Snapshot);
        aggregate.Approve(evidence,aggregate.Snapshot.PreviewHash,DateTimeOffset.UtcNow);
        Assert.Equal(KafdeckAuthorizationOutcome.Allowed,authorization.AuthorizeForDispatch(requester,aggregate.Snapshot));
        current=Policy(activate:false);
        Assert.False(service.IsLiveGovernanceApprovalAllowed(reviewer,target));Assert.False(service.IsCurrentGovernanceApprovalAllowed(reviewerId,target));
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden,authorization.AuthorizeForDispatch(requester,aggregate.Snapshot));
        current=Policy(approve:false);
        Assert.False(service.IsLiveGovernanceApprovalAllowed(reviewer,target));Assert.False(service.IsCurrentGovernanceApprovalAllowed(reviewerId,target));
        current=Policy(direct:false);
        Assert.False(service.IsLiveGovernanceApprovalAllowed(reviewer,target));Assert.False(service.IsCurrentGovernanceApprovalAllowed(reviewerId,target));
        current=original;
        Assert.Equal(KafdeckAuthorizationOutcome.Allowed,authorization.AuthorizeForDispatch(requester,aggregate.Snapshot));
    }

    private static ClaimsPrincipal Browser(string subject)=>OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub",subject)],"oidc")),Issuer,null,DateTimeOffset.UtcNow);
    private static AuthorizationPolicySnapshot Policy(bool activate=true,bool approve=true,bool direct=true)
    {
        AuthorizationPermissionDefinition Permission(AuthorizationAction action)=>new(action,["alpha"],["active"]);
        var reviewer=new List<AuthorizationPermissionDefinition>();if(activate)reviewer.Add(Permission(AuthorizationAction.GovernancePolicyActivate));if(approve)reviewer.Add(Permission(AuthorizationAction.MutationApprove));
        var subjects=new List<AuthorizationSubjectBindingDefinition>{new(Issuer,"requester",["owner"])};
        if(direct)subjects.Add(new(Issuer,"reviewer",["reviewer"]));
        return AuthorizationPolicyCompiler.Compile(new([
            new("owner",[Permission(AuthorizationAction.GovernancePolicyActivate),Permission(AuthorizationAction.MutationApprove)]),new("reviewer",reviewer)],subjects,[]));
    }
}
