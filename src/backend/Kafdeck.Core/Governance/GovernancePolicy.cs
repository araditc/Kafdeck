using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Security;

namespace Kafdeck.Core.Governance;

public sealed record GovernanceCondition(string Field, string Operator, string Value);
public sealed record GovernanceSelector(string ClusterId, string ResourceKind, IReadOnlyList<string> ResourceIds, string Action);
public sealed record GovernanceRule(string Id, int Priority, GovernanceSelector Scope, IReadOnlyList<GovernanceCondition> Conditions, string Outcome);
public sealed record GovernanceDocument(int SchemaVersion, IReadOnlyList<GovernanceRule> Rules);
public sealed record CompiledGovernancePolicy(string ContentHash, string CanonicalJson, GovernanceDocument Document);
public sealed record GovernanceFact(string ClusterId, string ResourceKind, string ResourceId, string Action, string Risk);
public sealed record GovernanceDecision(bool Allowed, string Code, string? Digest, IReadOnlyList<string> MatchedRuleIds);

/// <summary>Closed, finite data; no expression evaluation, regex, network lookup or executable policy.</summary>
public static class GovernancePolicyCompiler
{
    public const int MaximumBytes = 256 * 1024;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private static readonly HashSet<string> Kinds = new(StringComparer.Ordinal)
    { "topic", "consumerGroup", "schema", "connector", "broker", "cluster", "catalog", "policy", "dataJob", "generator", "acl", "scram", "quota", "notification", "request", "rateCard" };
    private static readonly HashSet<string> Fields = new(StringComparer.Ordinal)
    { "classification", "domain", "teamId", "applicationId", "risk", "requestApproved" };

    public static CompiledGovernancePolicy Compile(string document)
    {
        try{return CompileCore(document);}
        catch(Exception ex)when(ex is InvalidOperationException or FormatException or OverflowException)
        {throw new ArgumentException("Invalid closed policy value.",nameof(document),ex);}
    }

    private static CompiledGovernancePolicy CompileCore(string document)
    {
        ArgumentNullException.ThrowIfNull(document);
        if (Encoding.UTF8.GetByteCount(document) > MaximumBytes) throw new ArgumentException("Policy exceeds 256 KiB.");
        using var parsed = JsonDocument.Parse(document, new JsonDocumentOptions { MaxDepth = 12, CommentHandling = JsonCommentHandling.Disallow });
        var root = Object(parsed.RootElement, "schemaVersion", "rules");
        if (root.GetProperty("schemaVersion").GetInt32() != 1) throw new ArgumentException("Unsupported policy schema.");
        var array = root.GetProperty("rules");
        if (array.ValueKind != JsonValueKind.Array || array.GetArrayLength() > 100) throw new ArgumentException("Policy has too many rules.");
        var rules = new List<GovernanceRule>();
        var ids = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in array.EnumerateArray())
        {
            var rule = Object(item, "id", "priority", "scope", "conditions", "outcome");
            var id = Text(rule, "id", 128);
            if (!ids.Add(id)) throw new ArgumentException("Duplicate rule ID.");
            var priority = rule.GetProperty("priority").GetInt32();
            if (priority is < 0 or > 10000) throw new ArgumentException("Invalid priority.");
            var scope = Object(rule.GetProperty("scope"), "clusterId", "resourceKind", "resourceIds", "action");
            var cluster = Text(scope, "clusterId", 256);
            var kind = Text(scope, "resourceKind", 32);
            if (!Kinds.Contains(kind)) throw new ArgumentException("Unregistered resource kind.");
            var action = Text(scope, "action", 64);
            if (!Enum.TryParse<AuthorizationAction>(action, true, out var parsedAction) || !Enum.IsDefined(parsedAction) || !string.Equals(action,parsedAction.ToString(),StringComparison.OrdinalIgnoreCase)) throw new ArgumentException("Unregistered action.");
            if(kind!=ResourceKind(parsedAction.ToString()))throw new ArgumentException("Action does not bind the selected resource kind.");
            if(cluster=="$deployment"&&(kind!="notification"||parsedAction!=AuthorizationAction.NotificationManage))throw new ArgumentException("Deployment scope permits only existing global notification controls.");
            var resources = scope.GetProperty("resourceIds");
            if (resources.ValueKind != JsonValueKind.Array || resources.GetArrayLength() is < 1 or > 20) throw new ArgumentException("Selectors require 1–20 exact resource IDs.");
            var resourceIds = resources.EnumerateArray().Select(x => Bounded(x.GetString(), 512)).ToArray();
            if (resourceIds.Distinct(StringComparer.Ordinal).Count() != resourceIds.Length) throw new ArgumentException("Duplicate selector ID.");
            var conditionsElement = rule.GetProperty("conditions");
            if (conditionsElement.ValueKind != JsonValueKind.Array || conditionsElement.GetArrayLength() > 10) throw new ArgumentException("Too many conditions.");
            var conditions = new List<GovernanceCondition>();
            foreach (var c in conditionsElement.EnumerateArray())
            {
                var condition = Object(c, "field", "operator", "value");
                var field = Text(condition, "field", 32);
                var op = Text(condition, "operator", 16);
                if (!Fields.Contains(field) || op is not ("equals" or "notEquals")) throw new ArgumentException("Unregistered condition.");
                conditions.Add(new(field, op, Text(condition, "value", 256)));
            }
            var outcome = Text(rule, "outcome", 32);
            if (outcome is not ("Deny" or "RequireApprovedRequest")) throw new ArgumentException("Policy cannot grant authority.");
            rules.Add(new(id, priority, new(cluster, kind, resourceIds.Order(StringComparer.Ordinal).ToArray(), parsedAction.ToString()),
                conditions.OrderBy(c => c.Field, StringComparer.Ordinal).ThenBy(c => c.Operator, StringComparer.Ordinal).ThenBy(c => c.Value, StringComparer.Ordinal).ToArray(), outcome));
        }
        var normalized = new GovernanceDocument(1, rules.OrderBy(r => r.Priority).ThenBy(r => r.Id, StringComparer.Ordinal).ToArray());
        var canonical = JsonSerializer.Serialize(normalized, Json);
        if(Encoding.UTF8.GetByteCount(canonical)>MaximumBytes)throw new ArgumentException("Canonical policy exceeds 256 KiB.");
        return new(Hash(canonical), canonical, normalized);
    }

    public static GovernanceDecision Evaluate(CompiledGovernancePolicy policy, GovernanceFact fact,
        IReadOnlyDictionary<string, string>? metadata, string? digest = null,GovernanceApprovedRequestFact? approvedRequest=null)
    {
        var matches = new List<string>();
        var requireRequest = false;
        var denied = false;
        foreach (var rule in policy.Document.Rules)
        {
            if (rule.Scope.ClusterId != fact.ClusterId || rule.Scope.ResourceKind != fact.ResourceKind ||
                !rule.Scope.ResourceIds.Contains(fact.ResourceId, StringComparer.Ordinal) ||
                !string.Equals(rule.Scope.Action, fact.Action, StringComparison.OrdinalIgnoreCase)) continue;
            var matching = true;
            foreach (var condition in rule.Conditions)
            {
                var value = condition.Field == "risk" ? fact.Risk.ToLowerInvariant() :
                    condition.Field == "requestApproved" ? approvedRequest is null?null:"true" : metadata?.GetValueOrDefault(condition.Field);
                if (value is null) return new(false, "GovernanceEvidenceUnavailable", digest, Array.Empty<string>());
                var equal = string.Equals(value, condition.Value, StringComparison.Ordinal);
                if (condition.Operator == "equals" ? !equal : equal) { matching = false; break; }
            }
            if (!matching) continue;
            matches.Add(rule.Id);
            denied |= rule.Outcome == "Deny";
            requireRequest |= rule.Outcome == "RequireApprovedRequest";
        }
        // W74 alone may supply real immutable request-revision authority. No caller fixture can fabricate approval.
        return new(!denied && (!requireRequest||approvedRequest is not null), denied ? "GovernanceDenied" : requireRequest&&approvedRequest is null ? "RequestApprovalRequired" : "Allowed", digest, matches);
    }

    public static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    public static string ResourceKind(string action)=>action.StartsWith("Topic",StringComparison.Ordinal)||action.StartsWith("Record",StringComparison.Ordinal)?"topic":
        action.StartsWith("Consumer",StringComparison.Ordinal)?"consumerGroup":action.StartsWith("Schema",StringComparison.Ordinal)?"schema":
        action.StartsWith("Connect",StringComparison.Ordinal)?"connector":action.StartsWith("GovernancePolicy",StringComparison.Ordinal)?"policy":
        action.StartsWith("Catalog",StringComparison.Ordinal)?"catalog":action.StartsWith("DataJob",StringComparison.Ordinal)?"dataJob":
        action.StartsWith("DataGenerator",StringComparison.Ordinal)?"generator":action.StartsWith("Acl",StringComparison.Ordinal)?"acl":
        action.StartsWith("Scram",StringComparison.Ordinal)?"scram":action.StartsWith("Quota",StringComparison.Ordinal)?"quota":
        action.StartsWith("Notification",StringComparison.Ordinal)?"notification":action.StartsWith("Request",StringComparison.Ordinal)?"request":
        action.StartsWith("RateCard",StringComparison.Ordinal)?"rateCard":action.StartsWith("Broker",StringComparison.Ordinal)?"broker":"cluster";
    private static JsonElement Object(JsonElement value, params string[] required)
    {
        if (value.ValueKind != JsonValueKind.Object) throw new ArgumentException("Policy requires an object.");
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var p in value.EnumerateObject())
            if (!names.Add(p.Name) || !required.Contains(p.Name, StringComparer.Ordinal)) throw new ArgumentException("Duplicate or unknown policy key.");
        if (required.Any(x => !names.Contains(x))) throw new ArgumentException("Missing policy key.");
        return value;
    }
    private static string Text(JsonElement value, string name, int max) => Bounded(value.GetProperty(name).GetString(), max);
    public static string Bounded(string? value, int max)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > max || value != value.Trim() || value.Any(char.IsControl)) throw new ArgumentException("Invalid bounded identifier.");
        return value;
    }
}

public sealed record GovernancePolicyVersion(string VersionId, string ClusterId, string ContentHash, string Document, string CreatedBy, DateTimeOffset CreatedAtUtc);
public sealed record GovernancePolicyState(string ClusterId, long Revision, bool Enabled, string? VersionId, string? ContentHash, string? Digest);
public sealed record GovernanceCommitReceipt(Guid OperationId,int OperationKind,string ClusterId,string VersionId,string ContentHash,
    string Actor,string PreviewHash,string MaterialHash,string? BoundDigest,string? DeploymentGeneration,long Revision,bool Enabled,DateTimeOffset CommittedAtUtc);
public sealed class GovernancePolicyCapacityException():InvalidOperationException("PolicyVersionCapacityExceeded");
public interface IGovernancePolicyStore
{
    Task InitializeAsync(CancellationToken token = default);
    Task<GovernancePolicyState> GetStateAsync(string clusterId, CancellationToken token = default);
    Task<GovernancePolicyVersion?> GetVersionAsync(string clusterId, string versionId, bool includeDraft = false, CancellationToken token = default);
    Task<IReadOnlyList<GovernancePolicyVersion>> ListVersionsAsync(string clusterId, string? afterId, int limit, CancellationToken token = default);
    Task<IReadOnlyList<GovernancePolicyVersion>> ListAuthorizedVersionsAsync(string clusterId,string? afterId,int pageSize,
        Func<string,bool> authorizedVersionId,CancellationToken token=default);
    Task<GovernancePolicyVersion> StageAsync(string clusterId, CompiledGovernancePolicy policy, string actor, CancellationToken token = default);
}

public interface IPolicyMetadataReader
{
    Task<IReadOnlyDictionary<string, string>?> ReadAsync(string clusterId, string resourceId, CancellationToken token = default);
}
