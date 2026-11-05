using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Kafdeck.Cli;

/// <summary>Compiled v0.9 catalog/policy commands; files cannot choose arbitrary routes, actors or execution handlers.</summary>
public static class CliGovernanceCommands
{
    private const string Prefix = "/api/v1/governance/";
    public static bool IsGovernance(CliFamilyInvocation? invocation) => invocation?.Route.StartsWith(Prefix, StringComparison.Ordinal) == true;
    public static int RequestByteLimit(CliFamilyInvocation invocation) => invocation.Payload == CliFamilyPayload.GovernanceCatalogImport ? 1024 * 1024 : IsGovernance(invocation) ? 256 * 1024 : CliFamilyCommands.MaxRequestBytes;
    public static CliFamilyInvocation? TryParse(IReadOnlyList<string> args, string? cursor = null, int? limit = null)
    {
        if (args.Count == 0 || args[0] is not ("catalog" or "policies")) return null;
        var group = args[0]; var command = args.Count > 1 ? args[1] : "";
        string Scope(int index) => Segment(args[index]);
        string Page(string cluster) => "clusterId=" + Uri.EscapeDataString(cluster) + "&pageSize=" + (limit ?? 50).ToString(CultureInfo.InvariantCulture) +
            (cursor is null ? "" : "&cursor=" + Uri.EscapeDataString(cursor));
        CliFamilyInvocation Read(string route) => new(Prefix + route, false, CliFamilyPayload.Empty, Read: true, Method: "get");
        CliFamilyInvocation Post(string route, CliFamilyPayload payload, bool preview, string? scope = null) =>
            new(Prefix + route, preview, payload, Read: !preview, GovernanceScope: scope);
        if (group == "catalog")
        {
            if (args.Count is 3 or 4 && command is "teams" or "applications" or "topics" && (args.Count == 3 || args[3] == "list"))
                return Read("catalog/" + command + "?" + Page(Scope(2)));
            if (command is "get" or "history" && args.Count == 5)
            {
                var cluster = Scope(2); var kind = Family(args[3]); var id = CatalogId(args[4]);
                if (command == "get" && (cursor is not null || limit is not null)) throw new CliUsageException("Only list/history commands accept pagination.");
                return Read("catalog/" + kind + "/" + id + (command == "history" ? "/history?" + Page(cluster) : "?clusterId=" + Uri.EscapeDataString(cluster)));
            }
            if (command == "plan" && args.Count == 3) return NoPage(Read("catalog/operations/" + Id(args[2])), cursor, limit);
            if (command == "recover" && args.Count == 3) return NoPage(new(Prefix + "catalog/operations/" + Id(args[2]) + "/recover",false,CliFamilyPayload.GovernanceCatalogRecovery),cursor,limit);
            if (command == "change-preview" && args.Count == 4)
                return NoPage(Post("catalog/" + Family(args[3]) + "/preview", CliFamilyPayload.GovernanceCatalogChange, true, Scope(2)), cursor, limit);
            if (command == "import-preview" && args.Count == 3)
                return NoPage(Post("catalog/imports/preview", CliFamilyPayload.GovernanceCatalogImport, true, Scope(2)), cursor, limit);
        }
        if (group == "policies")
        {
            if (command == "versions" && args.Count == 3) return Read("policies/versions?" + Page(Scope(2)));
            if (command is "active" or "state" && args.Count == 3) return NoPage(Read("policies/active?clusterId=" + Uri.EscapeDataString(Scope(2))), cursor, limit);
            if (command == "version" && args.Count == 4) return NoPage(Read("policies/versions/" + PolicyId(args[3]) + "?clusterId=" + Uri.EscapeDataString(Scope(2))), cursor, limit);
            if (command == "plan" && args.Count == 3) return NoPage(Read("policies/operations/" + Id(args[2])), cursor, limit);
            if (command is "reconcile" or "recover" && args.Count == 3) return NoPage(new(Prefix+"policies/operations/"+Id(args[2])+"/reconcile",false,CliFamilyPayload.Empty),cursor,limit);
            if (command == "validate" && args.Count == 2) return NoPage(Post("policies/validate", CliFamilyPayload.GovernancePolicyDocument, false), cursor, limit);
            if (command == "validate" && args.Count == 3) return NoPage(Post("policies/validate", CliFamilyPayload.GovernancePolicyDocument, false, Scope(2)), cursor, limit);
            if (command is "explain" or "dry-run" && args.Count == 2) return NoPage(Post("policies/" + command,
                command == "explain" ? CliFamilyPayload.GovernancePolicyFact : CliFamilyPayload.GovernancePolicyDryRun, false), cursor, limit);
            if (command is "explain" or "dry-run" && args.Count == 3) return NoPage(Post("policies/" + command,
                command == "explain" ? CliFamilyPayload.GovernancePolicyFact : CliFamilyPayload.GovernancePolicyDryRun, false, Scope(2)), cursor, limit);
            if (command == "version-preview" && args.Count == 3) return NoPage(Post("policies/versions/preview", CliFamilyPayload.GovernancePolicyDocument, true, Scope(2)), cursor, limit);
            if (command is "activation-preview" or "activate-preview" && args.Count == 3) return NoPage(Post("policies/activations/preview", CliFamilyPayload.GovernancePolicyActivation, true, Scope(2)), cursor, limit);
        }
        throw new CliUsageException("Use a named catalog/policies command and its exact scoped arguments from --help.");
    }
    private static CliFamilyInvocation NoPage(CliFamilyInvocation invocation, string? cursor, int? limit) => cursor is null && limit is null ? invocation : throw new CliUsageException("Only catalog list/history and policy versions accept pagination.");
    private static string Family(string value) => value is "teams" or "applications" or "topics" ? value : throw new CliUsageException("Choose teams, applications or topics.");
    private static string Id(string value) => Guid.TryParseExact(value, "D", out var id) && id != Guid.Empty ? id.ToString("D") : throw new CliUsageException("Use an exact nonempty server-issued UUID.");
    private static string CatalogId(string value) => Guid.TryParseExact(value,"N",out var id)&&id!=Guid.Empty&&value==value.ToLowerInvariant()?value:throw new CliUsageException("Use the exact 32-character lowercase catalog record ID.");
    private static string PolicyId(string value) => value.Length==64&&value.All(c=>c is >= '0' and <= '9' or >= 'a' and <= 'f')?value:throw new CliUsageException("Use the exact immutable policy version SHA256.");
    private static string Segment(string value) => !string.IsNullOrWhiteSpace(value) && value.Length <= 128 && value == value.Trim() && !value.Any(char.IsControl) && value is not ("." or "..") && value.IndexOfAny(['/', '\\', '?', '#']) < 0 ? value : throw new CliUsageException("Use an exact authorized cluster identifier.");

    public static bool Validate(JsonElement value, CliFamilyInvocation invocation)
    {
        if (!Validate(value, invocation.Payload)) return false;
        var scope = invocation.GovernanceScope;
        if (invocation.Route == Prefix + "policies/validate" && !Closed(value,["clusterId","document"],["clusterId","document"])) return false;
        if (scope is null) return true;
        if (invocation.Payload == CliFamilyPayload.GovernancePolicyDryRun)
            return value.GetProperty("fixtures").EnumerateArray().All(item => item.GetProperty("clusterId").GetString() == scope);
        return value.TryGetProperty("clusterId", out var cluster) && cluster.GetString() == scope;
    }
    public static bool Validate(JsonElement value, CliFamilyPayload payload) => payload switch
    {
        CliFamilyPayload.GovernanceCatalogChange or CliFamilyPayload.GovernanceCatalogImport => Closed(value, ["clusterId", "changes"], ["clusterId", "changes"]) && Text(value.GetProperty("clusterId"), 128) &&
            value.GetProperty("changes") is {ValueKind:JsonValueKind.Array} changes && changes.GetArrayLength() is >0 and <=500 && changes.EnumerateArray().All(Change),
        CliFamilyPayload.GovernancePolicyDocument => Closed(value, ["clusterId", "document", "idempotencyKey"], ["clusterId", "document"]) &&
            Text(value.GetProperty("document"), 256 * 1024, controls: true) && Text(value.GetProperty("clusterId"),128) && Optional(value,"idempotencyKey",v=>Text(v,256)),
        CliFamilyPayload.GovernancePolicyActivation => Closed(value,["clusterId","versionId","expectedRevision","enabled","idempotencyKey"],["clusterId","versionId","expectedRevision","enabled"]) &&
            Text(value.GetProperty("clusterId"),128) && value.GetProperty("versionId").ValueKind==JsonValueKind.String && value.GetProperty("versionId").GetString() is {Length:64} version && version.All(c=>c is >= '0' and <= '9' or >= 'a' and <= 'f') && Revision(value.GetProperty("expectedRevision")) && Boolean(value.GetProperty("enabled")) && Optional(value,"idempotencyKey",v=>Text(v,256)),
        CliFamilyPayload.GovernancePolicyFact => Fact(value),
        CliFamilyPayload.GovernancePolicyDryRun => Closed(value,["fixtures"],["fixtures"]) && value.GetProperty("fixtures") is {ValueKind:JsonValueKind.Array} fixtures && fixtures.GetArrayLength() is >0 and <=100 && fixtures.EnumerateArray().All(Fact),
        CliFamilyPayload.GovernanceCatalogRecovery => Closed(value,["previewHash"],["previewHash"]) && value.GetProperty("previewHash").ValueKind==JsonValueKind.String && value.GetProperty("previewHash").GetString() is {Length:64} hash && hash.All(c=>c is >= '0' and <= '9' or >= 'a' and <= 'f'),
        _ => false,
    };
    private static bool Change(JsonElement value) => Closed(value,["kind","id","expectedRevision","name","description","archived","teamId","applicationId","topicName","owner","domain","classification","documentationReference","tags","rebindTopicIdentity"],["kind","expectedRevision","name"]) &&
        value.GetProperty("kind").ValueKind == JsonValueKind.String && new[]{"team","application","topic"}.Contains(value.GetProperty("kind").GetString()!.ToLowerInvariant(),StringComparer.Ordinal) &&
        Revision(value.GetProperty("expectedRevision")) && CatalogText(value.GetProperty("name"),128) &&
        new[]{"id","teamId","applicationId"}.All(name=>Optional(value,name,v=>v.ValueKind==JsonValueKind.Null||Uuid(v))) &&
        Optional(value,"description",v=>v.ValueKind==JsonValueKind.Null||CatalogText(v,2048)) &&
        new[]{"owner","domain","classification"}.All(name=>Optional(value,name,v=>v.ValueKind==JsonValueKind.Null||CatalogText(v,128))) &&
        Optional(value,"documentationReference",v=>v.ValueKind==JsonValueKind.Null||CatalogText(v,2048)) &&
        Optional(value,"topicName",v=>v.ValueKind==JsonValueKind.Null||CatalogText(v,249)) &&
        Optional(value,"archived",Boolean) && Optional(value,"rebindTopicIdentity",Boolean) &&
        Optional(value,"tags",v=>v.ValueKind==JsonValueKind.Null||v.ValueKind==JsonValueKind.Array&&v.GetArrayLength()<=20&&v.EnumerateArray().All(tag=>CatalogText(tag,64)));
    private static bool Fact(JsonElement value) => Closed(value,["clusterId","resourceKind","resourceId","action","risk"],["clusterId","resourceKind","resourceId","action","risk"]) &&
        new[]{"clusterId","resourceKind","resourceId","action","risk"}.All(name=>Text(value.GetProperty(name),512));
    private static bool Closed(JsonElement value,string[] allowed,string[] required)
    {
        if(value.ValueKind!=JsonValueKind.Object)return false;
        var seen=new HashSet<string>(StringComparer.Ordinal);
        return value.EnumerateObject().All(p=>allowed.Contains(p.Name,StringComparer.Ordinal)&&seen.Add(p.Name))&&required.All(seen.Contains);
    }
    private static bool Optional(JsonElement value,string name,Func<JsonElement,bool> predicate)=>!value.TryGetProperty(name,out var item)||predicate(item);
    private static bool Boolean(JsonElement value)=>value.ValueKind is JsonValueKind.True or JsonValueKind.False;
    private static bool Revision(JsonElement value)=>value.ValueKind==JsonValueKind.Number&&value.TryGetInt64(out var number)&&number>=0&&number<long.MaxValue;
    private static bool Uuid(JsonElement value)=>value.ValueKind==JsonValueKind.String&&value.GetString() is {} text&&Guid.TryParseExact(text,"N",out var id)&&id!=Guid.Empty&&text==text.ToLowerInvariant();
    private static bool Text(JsonElement value,int maximum,bool empty=false,bool controls=false)=>value.ValueKind==JsonValueKind.String&&value.GetString() is {} text&&(empty||!string.IsNullOrWhiteSpace(text))&&Encoding.UTF8.GetByteCount(text)<=maximum&&(controls||!text.Any(char.IsControl));
    private static bool CatalogText(JsonElement value,int maximum)=>value.ValueKind==JsonValueKind.String&&value.GetString() is {} text&&!string.IsNullOrWhiteSpace(text)&&text.Length<=maximum&&!text.Any(char.IsControl);
    public static int Outcome(string body,CliFamilyInvocation invocation)
    {
        using var parsed=JsonDocument.Parse(body);
        if(parsed.RootElement.TryGetProperty("operation",out var operation))return CliApplication.MutationOutcomeCategory(operation.GetRawText());
        if(invocation.Preview||parsed.RootElement.TryGetProperty("state",out _))return CliApplication.MutationOutcomeCategory(body);
        return CliApplication.Success;
    }
}
