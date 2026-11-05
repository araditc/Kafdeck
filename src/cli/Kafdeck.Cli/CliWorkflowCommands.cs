using System.Globalization;
using System.Text.Json;

namespace Kafdeck.Cli;

/// <summary>Named request/measurement routes; files never choose actors, authority, metrics URLs or execution handlers.</summary>
public static class CliWorkflowCommands
{
    private const string Prefix="/api/v1/governance/";
    public static bool IsPayload(CliFamilyPayload payload)=>payload is CliFamilyPayload.WorkflowRequestChange or CliFamilyPayload.WorkflowRateCardDocument or CliFamilyPayload.WorkflowRateCardActivation;
    public static CliFamilyInvocation? TryParse(IReadOnlyList<string> args,string? cursor=null,int? limit=null)
    {
        if(args.Count==0||args[0] is not("requests" or "measurements" or "attribution" or "dashboards" or "rate-cards"))return null;
        var family=args[0];var command=args.Count>1?args[1]:"";
        CliFamilyInvocation Read(string route)=>new(Prefix+route,false,CliFamilyPayload.Empty,Read:true,Method:"get");
        CliFamilyInvocation Preview(string route,CliFamilyPayload payload,string cluster)=>new(Prefix+route,true,payload,GovernanceScope:cluster);
        string Page(string cluster)=>"clusterId="+Uri.EscapeDataString(Segment(cluster))+"&pageSize="+(limit??50).ToString(CultureInfo.InvariantCulture)+(cursor is null?"":"&cursor="+Uri.EscapeDataString(cursor));
        CliFamilyInvocation NoPage(CliFamilyInvocation value)=>cursor is null&&limit is null?value:throw new CliUsageException("Only paginated workflow reads accept --cursor/--limit.");
        if(family=="requests")
        {
            if(command=="list"&&args.Count==3)return Read("requests?"+Page(args[2]));
            if(command is "get" or "history"&&args.Count==4)
            {
                var route="requests/"+RequestId(args[3]);
                return command=="history"?Read(route+"/history?"+Page(args[2])):NoPage(Read(route+"?clusterId="+Uri.EscapeDataString(Segment(args[2]))));
            }
            if(command=="change-preview"&&args.Count==3)return NoPage(Preview("requests/changes/preview",CliFamilyPayload.WorkflowRequestChange,Segment(args[2])));
            if(command=="plan"&&args.Count==3)return NoPage(Read("requests/plans/"+OperationId(args[2])));
            if(command=="recover"&&args.Count==3)return NoPage(new(Prefix+"requests/operations/"+OperationId(args[2])+"/recover",false,CliFamilyPayload.Empty));
        }
        if(family=="rate-cards")
        {
            if(command=="versions"&&args.Count==3)return Read("rate-cards/versions?"+Page(args[2]));
            if(command=="state"&&args.Count==3)return NoPage(Read("rate-cards/state?clusterId="+Uri.EscapeDataString(Segment(args[2]))));
            if(command=="version-preview"&&args.Count==3)return NoPage(Preview("rate-cards/versions/preview",CliFamilyPayload.WorkflowRateCardDocument,Segment(args[2])));
            if(command is "activation-preview" or "activate-preview"&&args.Count==3)return NoPage(Preview("rate-cards/activations/preview",CliFamilyPayload.WorkflowRateCardActivation,Segment(args[2])));
            if(command=="plan"&&args.Count==3)return NoPage(Read("rate-cards/operations/"+OperationId(args[2])));
            if(command=="recover"&&args.Count==3)return NoPage(new(Prefix+"rate-cards/operations/"+OperationId(args[2])+"/recover",false,CliFamilyPayload.Empty));
        }
        if(family is "measurements" or "attribution" or "dashboards"&&args.Count is 5 or 6&&
            (family,command) is ("measurements","list") or ("attribution","windows") or ("dashboards","show"))
        {
            if(!Utc(args[3],out var from)||!Utc(args[4],out var to)||to-from<TimeSpan.FromMinutes(1)||to-from>TimeSpan.FromDays(31))throw new CliUsageException("Use an explicit UTC interval from one minute to 31 days.");
            var query=Page(args[2])+"&fromUtc="+Uri.EscapeDataString(args[3])+"&toUtc="+Uri.EscapeDataString(args[4]);
            if(args.Count==6)query+="&resourceId="+Uri.EscapeDataString(TextValue(args[5],512));
            return Read(family+"?"+query);
        }
        throw new CliUsageException("Use an exact named requests/measurements/attribution/dashboards/rate-cards command from --help.");
    }
    public static bool Validate(JsonElement value,CliFamilyInvocation invocation)=>Validate(value,invocation.Payload)&&
        (invocation.GovernanceScope is null||value.TryGetProperty("clusterId",out var scope)&&scope.GetString()==invocation.GovernanceScope);
    public static bool Validate(JsonElement value,CliFamilyPayload payload)
    {
        try
        {
            return payload switch
            {
                CliFamilyPayload.WorkflowRequestChange=>Change(value),
                CliFamilyPayload.WorkflowRateCardDocument=>Closed(value,["clusterId","document","idempotencyKey"],["clusterId","document"])&&Text(value.GetProperty("clusterId"),128)&&Rate(value.GetProperty("document"))&&Optional(value,"idempotencyKey",v=>Text(v,256)),
                CliFamilyPayload.WorkflowRateCardActivation=>Closed(value,["clusterId","versionId","expectedRevision","enabled","idempotencyKey"],["clusterId","versionId","expectedRevision","enabled"])&&Text(value.GetProperty("clusterId"),128)&&Digest(value.GetProperty("versionId"))&&Version(value.GetProperty("expectedRevision"))&&Boolean(value.GetProperty("enabled"))&&Optional(value,"idempotencyKey",v=>Text(v,256)),
                _=>false,
            };
        }
        catch(Exception exception)when(exception is InvalidOperationException or ArgumentException or OverflowException or FormatException){return false;}
    }
    public static bool RequiresHumanDecision(JsonElement value,CliFamilyPayload payload)=>payload==CliFamilyPayload.WorkflowRequestChange&&value.TryGetProperty("command",out var command)&&command.GetString() is "Approve" or "Reject";
    public static CliFamilyInvocation BindCompiledDecisionRoute(CliFamilyInvocation invocation,JsonElement payload)=>RequiresHumanDecision(payload,invocation.Payload)
        ?invocation with{Route=Prefix+"requests/decisions/preview"}:invocation;
    private static bool Change(JsonElement value)
    {
        if(!Closed(value,["clusterId","requestId","expectedVersion","command","operationId","reason","expiresAtUtc","idempotencyKey"],["clusterId","expectedVersion","command","reason"])||
            !Text(value.GetProperty("clusterId"),128)||!Version(value.GetProperty("expectedVersion"))||!Text(value.GetProperty("reason"),2048)||
            value.GetProperty("command").ValueKind!=JsonValueKind.String||value.GetProperty("command").GetString() is not("Draft" or "Revise" or "Submit" or "Approve" or "Reject" or "Cancel")||
            !Optional(value,"idempotencyKey",v=>Text(v,256)))return false;
        var command=value.GetProperty("command").GetString();
        if(command=="Draft")
        {if(value.GetProperty("expectedVersion").GetInt64()!=0||value.TryGetProperty("requestId",out var id)&&id.ValueKind!=JsonValueKind.Null)return false;}
        else if(value.GetProperty("expectedVersion").GetInt64()<1||!value.TryGetProperty("requestId",out var id)||!RequestUuid(id))return false;
        if(command is "Draft" or "Revise")return value.TryGetProperty("operationId",out var operation)&&OperationUuid(operation)&&value.TryGetProperty("expiresAtUtc",out var expiry)&&Utc(expiry);
        return !value.TryGetProperty("operationId",out var unused)||unused.ValueKind==JsonValueKind.Null;
    }
    private static bool Rate(JsonElement value)
    {
        if(!Closed(value,["metricKind","unit","currency","amount","effectiveFromUtc","effectiveToUtc","weights"],["metricKind","unit","currency","amount","effectiveFromUtc","effectiveToUtc","weights"]))return false;
        var kind=value.GetProperty("metricKind").GetString();var unit=value.GetProperty("unit").GetString();
        if((kind,unit) is not(("trafficBytes","bytes") or ("storageByteSeconds","byteSeconds"))||value.GetProperty("currency").GetString() is not{Length:3} currency||currency.Any(c=>c is <'A' or >'Z')||
            !Decimal(value.GetProperty("amount"),out _)||!Utc(value.GetProperty("effectiveFromUtc"))||!Utc(value.GetProperty("effectiveToUtc"))||value.GetProperty("effectiveToUtc").GetDateTimeOffset()<=value.GetProperty("effectiveFromUtc").GetDateTimeOffset())return false;
        var weights=value.GetProperty("weights");if(weights.ValueKind!=JsonValueKind.Array||weights.GetArrayLength()>100)return false;
        decimal sum=0;var owners=new HashSet<string>(StringComparer.Ordinal);
        foreach(var weight in weights.EnumerateArray())
        {
            if(!Closed(weight,["teamId","applicationId","weight"],["teamId","applicationId","weight"])||!OptionalOwner(weight.GetProperty("teamId"))||!OptionalOwner(weight.GetProperty("applicationId"))||
                weight.GetProperty("teamId").ValueKind==JsonValueKind.Null&&weight.GetProperty("applicationId").ValueKind==JsonValueKind.Null||!Decimal(weight.GetProperty("weight"),out var number)||number<=0||number>1)return false;
            if(!owners.Add((weight.GetProperty("teamId").GetString()??"")+"/"+(weight.GetProperty("applicationId").GetString()??"")))return false;
            sum=checked(sum+number);
        }
        return sum<=1;
    }
    private static bool Decimal(JsonElement value,out decimal number){number=0;return value.ValueKind==JsonValueKind.String&&value.GetString() is {} text&&text.Length<=64&&text.Length>0&&text[0]!='+'&&!text.Any(char.IsWhiteSpace)&&decimal.TryParse(text,NumberStyles.AllowDecimalPoint,CultureInfo.InvariantCulture,out number)&&number>=0;}
    private static bool Utc(JsonElement value)=>value.ValueKind==JsonValueKind.String&&Utc(value.GetString()!,out _);
    private static bool Utc(string value,out DateTimeOffset result)
    {
        result=default;
        if(value.Length>35||!(value.EndsWith('Z')||value.EndsWith("+00:00",StringComparison.Ordinal)))return false;
        string[] formats=["yyyy-MM-dd'T'HH:mm:ss'Z'","yyyy-MM-dd'T'HH:mm:ss.FFFFFFF'Z'","yyyy-MM-dd'T'HH:mm:sszzz","yyyy-MM-dd'T'HH:mm:ss.FFFFFFFzzz"];
        return DateTimeOffset.TryParseExact(value,formats,CultureInfo.InvariantCulture,DateTimeStyles.AssumeUniversal,out result)&&result.Offset==TimeSpan.Zero;
    }
    private static bool Closed(JsonElement value,string[] allowed,string[] required){if(value.ValueKind!=JsonValueKind.Object)return false;var names=new HashSet<string>(StringComparer.Ordinal);return value.EnumerateObject().All(p=>allowed.Contains(p.Name,StringComparer.Ordinal)&&names.Add(p.Name))&&required.All(names.Contains);}
    private static bool Optional(JsonElement value,string name,Func<JsonElement,bool> predicate)=>!value.TryGetProperty(name,out var item)||predicate(item);
    private static bool OptionalOwner(JsonElement value)=>value.ValueKind==JsonValueKind.Null||RequestUuid(value);
    private static bool Text(JsonElement value,int maximum)=>value.ValueKind==JsonValueKind.String&&value.GetString() is {} text&&!string.IsNullOrWhiteSpace(text)&&text.Length<=maximum&&text==text.Trim()&&!text.Any(char.IsControl);
    private static bool Version(JsonElement value)=>value.ValueKind==JsonValueKind.Number&&value.TryGetInt64(out var number)&&number>=0&&number<long.MaxValue;
    private static bool Boolean(JsonElement value)=>value.ValueKind is JsonValueKind.True or JsonValueKind.False;
    private static bool Digest(JsonElement value)=>value.ValueKind==JsonValueKind.String&&value.GetString() is{Length:64} text&&text.All(c=>c is >= '0' and <= '9' or >= 'a' and <= 'f');
    private static bool RequestUuid(JsonElement value)=>value.ValueKind==JsonValueKind.String&&Guid.TryParseExact(value.GetString(),"N",out var id)&&id!=Guid.Empty&&value.GetString()==id.ToString("N");
    private static bool OperationUuid(JsonElement value)=>value.ValueKind==JsonValueKind.String&&Guid.TryParseExact(value.GetString(),"D",out var id)&&id!=Guid.Empty;
    private static string RequestId(string value)=>Guid.TryParseExact(value,"N",out var id)&&id!=Guid.Empty&&value==id.ToString("N")?value:throw new CliUsageException("Use the exact server-issued lowercase request ID.");
    private static string OperationId(string value)=>Guid.TryParseExact(value,"D",out var id)&&id!=Guid.Empty?id.ToString("D"):throw new CliUsageException("Use an exact nonempty operation UUID.");
    private static string Segment(string value)=>value is not("." or "..")&&value.IndexOfAny(['/','\\','?','#'])<0?TextValue(value,128):throw new CliUsageException("Use an exact authorized cluster identifier.");
    private static string TextValue(string value,int maximum)=>!string.IsNullOrWhiteSpace(value)&&value.Length<=maximum&&value==value.Trim()&&!value.Any(char.IsControl)?value:throw new CliUsageException("Use bounded exact safe text.");
}
