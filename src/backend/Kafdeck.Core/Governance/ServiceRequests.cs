using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Security;

namespace Kafdeck.Core.Governance;

[JsonConverter(typeof(ServiceRequestStateConverter))]
public enum ServiceRequestState { Draft=1, Submitted=2, Approved=3, Rejected=4, Cancelled=5, Expired=6, NeedsReplan=7 }
[JsonConverter(typeof(ServiceRequestCommandConverter))]
public enum ServiceRequestCommand { Draft=1, Revise=2, Submit=3, Approve=4, Reject=5, Cancel=6 }
public sealed class ServiceRequestStateConverter():JsonStringEnumConverter<ServiceRequestState>(allowIntegerValues:false);
public sealed class ServiceRequestCommandConverter():JsonStringEnumConverter<ServiceRequestCommand>(allowIntegerValues:false);
public sealed record ServiceRequestTarget(AuthorizationAction Action,string ClusterId,string ResourceName);
public sealed record ServiceRequestBinding(Guid OperationId,string ClusterId,string OriginalRequesterPrincipalId,string PreviewHash,
    string MaterialHash,string PolicyDigest,string OperationKind,string Risk,IReadOnlyList<string> ResourceKeys,
    IReadOnlyList<ServiceRequestTarget> AuthorizationTargets,DateTimeOffset OperationExpiresAtUtc);
public sealed record ServiceRequestDecision(string ActorPrincipalId,bool Approved,string Reason,string AuthorityFingerprint,DateTimeOffset AtUtc);
public sealed record ServiceRequestRecord(string RequestId,string ClusterId,long Revision,long Version,ServiceRequestState BusinessState,
    string RequesterPrincipalId,string ClientKind,ServiceRequestBinding Binding,string Reason,DateTimeOffset CreatedAtUtc,
    DateTimeOffset UpdatedAtUtc,DateTimeOffset ExpiresAtUtc,IReadOnlyList<ServiceRequestDecision> Decisions);
public sealed record ServiceRequestHistory(ServiceRequestRecord Request,string ActorPrincipalId,Guid OperationId,DateTimeOffset AtUtc);
public sealed record ServiceRequestPlan(Guid PlanId,string ClusterId,string ContentHash,string RequestId,ServiceRequestCommand Command,
    long ExpectedVersion,string RequesterPrincipalId,string ClientKind,string AuthorityFingerprint,string DeploymentGeneration,
    ServiceRequestBinding Binding,string Reason,DateTimeOffset RequestExpiresAtUtc,DateTimeOffset CreatedAtUtc,DateTimeOffset ExpiresAtUtc);
public sealed record ServiceRequestReceipt(Guid OperationId,string RequestId,string ContentHash,string PreviewHash,string ActorPrincipalId,
    string DeploymentGeneration,long Version,DateTimeOffset CommittedAtUtc,ServiceRequestRecord Request);

/// <summary>Only the owning request repository produces this server-side fact after live validation.
/// It is never accepted by a request/preview/explain wire DTO.</summary>
public sealed record GovernanceApprovedRequestFact(Guid OperationId,string PreviewHash,string MaterialHash,string PolicyDigest,
    string RequestId,long Revision,string DecisionActorPrincipalId);

public sealed class ServiceRequestConflictException(string code):InvalidOperationException(code)
{ public string Code {get;}=code; }

public static class ServiceRequestContract
{
    public const int MaximumRequestsPerCluster=10_000;
    public const int MaximumEventsPerRequest=10_000;
    public static readonly JsonSerializerOptions Json=new(JsonSerializerDefaults.Web){MaxDepth=16,Converters={new JsonStringEnumConverter()}};
    public static string Resource(string id)=>"request:"+Id(id);
    public static string Id(string id)=>Guid.TryParseExact(id,"N",out var value)&&value!=Guid.Empty&&id==id.ToLowerInvariant()
        ?id:throw new ArgumentException("A server-issued request UUID is required.");
    public static string Bound(string value,int maximum)
    { if(string.IsNullOrWhiteSpace(value)||value.Length>maximum||value!=value.Trim()||value.Any(char.IsControl))throw new ArgumentException("Invalid request text.");return value; }
    public static bool IsDecision(ServiceRequestCommand command)=>command is ServiceRequestCommand.Approve or ServiceRequestCommand.Reject;
    public static bool SameHuman(string left,string right)
    {
        static string Canonical(string value)=>NonBrowserHumanPrincipalId.TryParse(value,out var human)?SecurityAuditPrincipal.FromOperatorKey(human!.HumanKey):value;
        return Canonical(left)==Canonical(right);
    }
    public static string PolicyDigest(IEnumerable<KeyValuePair<string,string>> markers)=>GovernancePolicyCompiler.Hash(JsonSerializer.Serialize(
        markers.OrderBy(p=>p.Key,StringComparer.Ordinal).Select(p=>new{p.Key,p.Value}),Json));
    public static string PlanHash(ServiceRequestPlan plan)=>GovernancePolicyCompiler.Hash(JsonSerializer.Serialize(plan with{ContentHash=""},Json));
    public static void Validate(ServiceRequestPlan plan)
    {
        Id(plan.RequestId);Bound(plan.ClusterId,256);Bound(plan.RequesterPrincipalId,2048);Bound(plan.AuthorityFingerprint,64);
        Bound(plan.DeploymentGeneration,128);Bound(plan.Reason,2048);
        if(plan.PlanId==Guid.Empty||!Enum.IsDefined(plan.Command)||plan.ExpectedVersion<0||plan.ExpiresAtUtc<=plan.CreatedAtUtc||
            plan.ExpiresAtUtc-plan.CreatedAtUtc>TimeSpan.FromMinutes(15)||plan.RequestExpiresAtUtc<=plan.CreatedAtUtc||
            plan.RequestExpiresAtUtc>plan.Binding.OperationExpiresAtUtc||plan.Binding.OperationId==Guid.Empty||
            plan.Binding.ClusterId!=plan.ClusterId||plan.Binding.PreviewHash.Length!=64||plan.Binding.MaterialHash.Length!=64||
            plan.Binding.PolicyDigest.Length!=64||plan.ContentHash!=PlanHash(plan)||
            plan.ClientKind is not("Browser" or "CliHuman" or "Automation")||plan.Binding.AuthorizationTargets.Count is <1 or >2048)
            throw new ArgumentException("Invalid immutable request material.");
        if(plan.Command==ServiceRequestCommand.Draft&&plan.ExpectedVersion!=0||plan.Command!=ServiceRequestCommand.Draft&&plan.ExpectedVersion<1)
            throw new ArgumentException("Invalid request expected version.");
    }
}
