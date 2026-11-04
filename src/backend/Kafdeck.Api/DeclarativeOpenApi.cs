using System.Text.Json.Nodes;
using Kafdeck.Core.Declarative;

namespace Kafdeck.Api;

/// <summary>Finite deployed W68/W71 routes. Request fields never carry actor, approval or arbitrary provider authority.</summary>
internal static class DeclarativeOpenApi
{
    internal static void Extend(JsonObject paths,JsonObject schemas)
    {
        schemas["DeclarativeManifestRequest"]=Closed(
            ("manifest",Text(DeclarativeSubscriptionManifest.MaxInputBytes)),("format",Enum("json","yaml")),
            ("sourceRef",Identifier()),("sourceRevision",Hash()),("expectedOwnershipRevision",Revision()));
        schemas["DeclarativeApplyRequest"]=Closed(("planFingerprint",Hash()));
        schemas["DeclarativeSourceAdvanceRequest"]=Closed(("manifest",Text(DeclarativeSubscriptionManifest.MaxInputBytes)),
            ("format",Enum("json","yaml")),("expectedGeneration",Uuid()),("expectedRevision",Revision()),("expectedSourceHash",Hash()));
        schemas["DeclarativeSourceRevision"]=Closed(("sourceRef",Identifier()),("generation",Uuid()),("revision",Revision()),("sourceHash",Hash()),("changed",new(){["type"]="boolean"}));
        schemas["DeclarativeSubscriptionProjection"]=new JsonObject { ["type"]="object",["properties"]=new JsonObject
        {
            ["destinationRef"]=Identifier(),["state"]=Enum("Active","Paused","Retired"),["revision"]=Revision(),
            ["eventClasses"]=Array(Text(128),16),["eventTypes"]=Array(Text(256),32)
        }};
        schemas["DeclarativeObservation"]=new JsonObject { ["type"]="object",["properties"]=new JsonObject
        {
            ["valid"]=new JsonObject{["type"]="boolean"},["drift"]=Enum("InSync","OutOfSync","Missing","Unknown","Inaccessible","Conflict","Unsupported"),
            ["desiredFingerprint"]=Hash(),["deploymentId"]=Identifier(),["scopeKind"]=Enum("Deployment"),["scopeId"]=Identifier(),
            ["resourceId"]=Identifier(),["kind"]=Enum("NotificationSubscription"),["schemaVersion"]=new JsonObject{["const"]=1},
            ["ownerRef"]=Nullable(Identifier()),["sourceRef"]=Nullable(Identifier()),["ownershipGeneration"]=Nullable(Uuid()),
            ["sourceGeneration"]=Nullable(Uuid()),["deploymentGeneration"]=Nullable(Uuid()),["sourceRegistrationRevision"]=Nullable(Revision()),
            ["subscriptionRevision"]=Nullable(Revision()),["ownershipRevision"]=Revision(),["ownership"]=Enum("ObservedOnly","Managed","Relinquished"),
            ["sourceInSync"]=new JsonObject{["type"]="boolean"},["observedAtUtc"]=Date(),["observedFingerprint"]=Text(64),
            ["capability"]=Text(128),["authorization"]=Text(128),["current"]=Nullable(Ref("DeclarativeSubscriptionProjection"))
        }};
        schemas["DeclarativeStoredPlan"]=new JsonObject{["type"]="object",["properties"]=new JsonObject
        {
            ["planId"]=Uuid(),["planFingerprint"]=Hash(),["storedProofFingerprint"]=Hash(),["deploymentId"]=Identifier(),
            ["resourceKind"]=Enum("NotificationSubscription"),["resourceId"]=Identifier(),["desiredFingerprint"]=Hash(),
            ["expectedOwnershipRevision"]=Revision(),["expectedSubscriptionRevision"]=Nullable(Revision()),["expiresAtUtc"]=Date(),
            ["ownerId"]=Identifier(),["sourceId"]=Identifier(),["sourceRevisionHash"]=Hash(),["sourceRegistrationRevision"]=Revision(),
            ["policyFingerprint"]=Hash(),["observedSubscriptionFingerprint"]=Text(64),["deploymentGeneration"]=Uuid(),
            ["before"]=Nullable(Ref("DeclarativeSubscriptionProjection")),["desired"]=Ref("DeclarativeSubscriptionProjection"),
            ["after"]=Nullable(Ref("DeclarativeSubscriptionProjection")),["changes"]=Array(Text(128),8),["risk"]=Enum("Critical"),
            ["requiresIndependentApproval"]=new JsonObject{["type"]="boolean"},["state"]=Text(64),["command"]=Enum("Apply","Adopt"),["compensatesPlanId"]=Nullable(Uuid())
        }};
        schemas["CompensationEligibility"]=new JsonObject{["type"]="object",["properties"]=new JsonObject
        {
            ["catalogVersion"]=new JsonObject{["const"]=1},["sourceOperationId"]=Uuid(),
            ["eligibility"]=Enum("NotCompensatable","BlockedByUnknownEffect","BlockedByDrift","RequiresFreshPlan"),["reason"]=Text(256),
            ["inverseManifest"]=Text(DeclarativeSubscriptionManifest.MaxInputBytes),["format"]=Enum("json"),["sourceRef"]=Identifier(),
            ["sourceRevision"]=Hash(),["expectedOwnershipRevision"]=Revision(),["requiresSourceAdvance"]=new JsonObject{["type"]="boolean"}
        }};
        schemas["CompensationCatalog"]=Closed(("catalogVersion",new(){["const"]=1}),("family",Enum("NotificationSubscriptionStateToggle")),
            ("executable",new(){["type"]="boolean"}),("states",Array(Enum("Active","Paused"),2)),
            ("freshGovernedPlanRequired",new(){["type"]="boolean"}),("independentCriticalApproval",new(){["type"]="boolean"}),
            ("notCompensatable",Array(Text(128),16)));
        schemas["DeclarativeCapabilities"]=new JsonObject{["type"]="object",["properties"]=new JsonObject
        {
            ["enabled"]=new JsonObject{["type"]="boolean"},["apiVersion"]=Enum(DeclarativeSubscriptionManifest.ApiVersion),
            ["families"]=Array(new JsonObject{["type"]="object",["properties"]=new JsonObject{["kind"]=Enum("NotificationSubscription","DataQualityPolicy","KafkaTopic","Acl","ConnectProfile"),["apply"]=new JsonObject{["type"]="boolean"},["ownership"]=Text(64)}},5)
        }};
        Add("/api/v1/declarative/capabilities","get","v08-declarative-capabilities",null,"DeclarativeCapabilities","DeclarativeRead");
        foreach(var verb in new[]{"validate","observe","drift","import-check","import-preview","plan"})
            Add("/api/v1/declarative/"+verb,"post","v08-declarative-"+verb,"DeclarativeManifestRequest",
                verb is "plan" or "import-preview"?"MutationStatus":"DeclarativeObservation",
                verb is "import-check" or "import-preview"?"DeclarativeRead,DeclarativePlan,DeclarativeAdopt,NotificationRead":verb=="plan"?"DeclarativeRead,DeclarativePlan,NotificationRead":"DeclarativeRead,NotificationRead",verb is "plan" or "import-preview");
        Add("/api/v1/declarative/plans/{operationId}","get","v08-declarative-plan-detail",null,"DeclarativeStoredPlan","DeclarativeRead,NotificationRead");
        foreach(var verb in new[]{"apply","recover"})Add("/api/v1/declarative/plans/{operationId}/"+verb,"post","v08-declarative-"+verb,"DeclarativeApplyRequest","MutationStatus","DeclarativeRead,DeclarativePlan,DeclarativeApply,NotificationRead,NotificationManage");
        Add("/api/v1/declarative/sources/{sourceRef}","get","v08-declarative-source-detail",null,"DeclarativeSourceRevision","DeclarativeRead,NotificationRead",requiredQuery:"resourceId");
        Add("/api/v1/declarative/sources/{sourceRef}/advance","post","v08-declarative-source-advance","DeclarativeSourceAdvanceRequest","DeclarativeSourceRevision","DeclarativeSourceManage,NotificationRead");
        Add("/api/v1/declarative/compensation/catalog","get","v08-declarative-compensation-catalog",null,"CompensationCatalog","DeclarativeRead");
        Add("/api/v1/declarative/plans/{sourcePlanId}/compensation","get","v08-declarative-compensation-inspect",null,"CompensationEligibility","DeclarativeRead,NotificationRead");
        Add("/api/v1/declarative/plans/{sourcePlanId}/compensation-preview","post","v08-declarative-compensation-preview","DeclarativeManifestRequest","MutationStatus","DeclarativeRead,DeclarativePlan,NotificationRead",true);

        void Add(string path,string method,string name,string? request,string response,string actions,bool idempotency=false,string? requiredQuery=null)
        {
            var parameters=new JsonArray();foreach(var part in path.Split('/').Where(x=>x.StartsWith('{')))
                parameters.Add(new JsonObject{["name"]=part[1..^1],["in"]="path",["required"]=true,["schema"]=part.Contains("sourceRef",StringComparison.Ordinal)?Identifier():Uuid()});
            if(requiredQuery is not null)parameters.Add(new JsonObject{["name"]=requiredQuery,["in"]="query",["required"]=true,["schema"]=Identifier(),["description"]="Exactly one resourceId is required. Duplicate values, extra query keys and invalid identifiers are rejected."});
            if(idempotency)parameters.Add(new JsonObject{["name"]="Idempotency-Key",["in"]="header",["required"]=true,["schema"]=Text(128)});
            var responses=new JsonObject{["200"]=new JsonObject{["description"]="Typed authorized result; inspect durable state. HTTP success does not grant effect/approval authority.",["content"]=new JsonObject{["application/json"]=new JsonObject{["schema"]=Ref(response)}}}};
            foreach(var (code,component) in new[]{("400","Invalid"),("401","Unauthenticated"),("403","Denied"),("404","NotFound"),("409","Conflict"),("413","TooLarge"),("503","Unavailable")})
                responses[code]=new JsonObject{["$ref"]="#/components/responses/"+component};
            var operation=new JsonObject{["operationId"]=name,["parameters"]=parameters,["responses"]=responses,
                ["x-kafdeck-actions"]=new JsonArray(actions.Split(',').Select(x=>(JsonNode?)JsonValue.Create(x)).ToArray()),
                ["description"]="Default-off exact registered principal/source/owner and current backend RBAC. Closed duplicate-free body at most65536bytes; manifest UTF8 at most16384bytes, finite JSON/YAML grammar, no raw secrets, arbitrary provider HTTP, actor or approval fields. Plan is immutable, expires and retains current policy/generation/CAS. CRITICAL effects require separate confirmation and distinct human approval. Source advance grants neither plan nor apply. Recovery requires positive current receipt and never redispatches unknown effects. Compensation is a fresh linked plan for exact verified metadata state toggles only; delivered notifications and irreversible/unresolved effects are never reversed. Browser cookie routes retain CSRF; bearer credential collisions are denied.",
                ["security"]=new JsonArray(new JsonObject{["bearerAuth"]=new JsonArray()})};
            if(request is not null)operation["requestBody"]=new JsonObject{["required"]=true,["content"]=new JsonObject{["application/json"]=new JsonObject{["schema"]=Ref(request)}}};
            paths[path]=new JsonObject{[method]=operation};
        }
    }
    private static JsonObject Closed(params (string Name,JsonObject Schema)[] fields)
    {var properties=new JsonObject();foreach(var field in fields)properties[field.Name]=field.Schema;return new(){["type"]="object",["additionalProperties"]=false,["properties"]=properties,["required"]=new JsonArray(fields.Select(x=>(JsonNode?)JsonValue.Create(x.Name)).ToArray())};}
    private static JsonObject Ref(string name)=>new(){["$ref"]="#/components/schemas/"+name};
    private static JsonObject Text(int max)=>new(){["type"]="string",["minLength"]=1,["maxLength"]=max};
    private static JsonObject Identifier()=>new(){["type"]="string",["minLength"]=1,["maxLength"]=128,["pattern"]="^[A-Za-z0-9._-]+$"};
    private static JsonObject Hash()=>new(){["type"]="string",["minLength"]=64,["maxLength"]=64,["pattern"]="^[a-f0-9]{64}$"};
    private static JsonObject Uuid()=>new(){["type"]="string",["format"]="uuid"};
    private static JsonObject Date()=>new(){["type"]="string",["format"]="date-time"};
    private static JsonObject Revision()=>new(){["type"]="integer",["minimum"]=0,["maximum"]=long.MaxValue};
    private static JsonObject Enum(params string[] values)=>new(){["type"]="string",["enum"]=new JsonArray(values.Select(x=>(JsonNode?)JsonValue.Create(x)).ToArray())};
    private static JsonObject Array(JsonObject item,int max)=>new(){["type"]="array",["items"]=item,["maxItems"]=max};
    private static JsonObject Nullable(JsonObject schema)=>new(){["anyOf"]=new JsonArray(schema,new JsonObject{["type"]="null"})};
}
