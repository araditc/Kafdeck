using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using Kafdeck.Core.Attribution;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Persistence;

public sealed record RateCardCommitReceipt(Guid OperationId,int OperationKind,string ClusterId,string PreviewHash,string MaterialHash,
    string VersionId,string ContentHash,long Revision,bool Enabled,string? DeploymentGeneration,DateTimeOffset CommittedAtUtc);

/// <summary>Trusted measurement ingestion and immutable rate-card authority use the mutation database and generation fence.</summary>
public sealed class AdoAttributionStore(IMutationDbConnectionFactory connections,IGovernanceGenerationTransactionGate? generation=null,
    IGovernancePolicyTransactionGate? policy=null,ICatalogActiveReferenceTransactionReader? catalogReferences=null):IAttributionStore
{
    private const string Schema="""
      CREATE TABLE IF NOT EXISTS attribution_schema(version INTEGER PRIMARY KEY,digest TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS attribution_visibility_scopes(cluster_id TEXT NOT NULL,scope_id TEXT NOT NULL,resource_id TEXT NOT NULL,
        team_id TEXT NULL,application_id TEXT NULL,PRIMARY KEY(cluster_id,scope_id));
      CREATE TABLE IF NOT EXISTS attribution_measurements(window_id TEXT PRIMARY KEY,cluster_id TEXT NOT NULL,resource_id TEXT NOT NULL,
        starts_utc TEXT NOT NULL,ends_utc TEXT NOT NULL,source_id TEXT NOT NULL,content_hash TEXT NOT NULL,body TEXT NOT NULL,scope_id TEXT NOT NULL,
        FOREIGN KEY(cluster_id,scope_id) REFERENCES attribution_visibility_scopes(cluster_id,scope_id));
      CREATE INDEX IF NOT EXISTS attribution_measurements_scope ON attribution_measurements(cluster_id,window_id);
      CREATE INDEX IF NOT EXISTS attribution_measurements_authorized_scope ON attribution_measurements(cluster_id,scope_id,window_id);
      CREATE INDEX IF NOT EXISTS attribution_measurements_time ON attribution_measurements(cluster_id,starts_utc,ends_utc);
      CREATE TABLE IF NOT EXISTS attribution_rate_versions(cluster_id TEXT NOT NULL,version_id TEXT NOT NULL,content_hash TEXT NOT NULL,
        committed INTEGER NOT NULL CHECK(committed IN (0,1)),body TEXT NOT NULL,PRIMARY KEY(cluster_id,version_id));
      CREATE TABLE IF NOT EXISTS attribution_rate_state(cluster_id TEXT PRIMARY KEY,revision BIGINT NOT NULL,version_id TEXT NULL,enabled INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS attribution_rate_bindings(cluster_id TEXT NOT NULL,version_id TEXT NOT NULL,enabled INTEGER NOT NULL,
        PRIMARY KEY(cluster_id,version_id),FOREIGN KEY(cluster_id,version_id) REFERENCES attribution_rate_versions(cluster_id,version_id));
      CREATE TABLE IF NOT EXISTS attribution_rate_audit(operation_id TEXT PRIMARY KEY,cluster_id TEXT NOT NULL,revision BIGINT NOT NULL,body TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS attribution_rate_receipts(operation_id TEXT PRIMARY KEY,body TEXT NOT NULL);
      """;
    public Task InitializeAsync(CancellationToken token=default)=>Transaction(async(c,t)=>
    {
        await Run(c,t,Schema,token);var digest=AttributionContract.Hash(Schema.Replace("\r\n","\n",StringComparison.Ordinal));
        await Run(c,t,"INSERT INTO attribution_schema(version,digest) VALUES(1,@hash) ON CONFLICT(version) DO NOTHING",token,("@hash",digest));
        if(await Scalar(c,t,"SELECT digest FROM attribution_schema WHERE version=1",token) is not string actual||actual!=digest)throw new InvalidOperationException("Attribution migration digest mismatch.");
        return true;
    },token,false);

    public Task AppendAsync(IReadOnlyList<MeasurementWindow> windows,CancellationToken token=default)=>Transaction(async(c,t)=>
    {
        if(windows.Count is <1 or >500)throw new ArgumentException("Measured batch requires 1–500 windows.");
        foreach(var row in windows)
        {
            AttributionContract.Window(row);var body=Serialize(row);var hash=AttributionContract.Hash(body);
            var existing=await Scalar(c,t,"SELECT body FROM attribution_measurements WHERE window_id=@id",token,("@id",row.WindowId));
            if(existing is string previous){var first=Deserialize<MeasurementWindow>(previous);if(first.ProvenanceHash!=row.ProvenanceHash||first.ClusterId!=row.ClusterId||first.ResourceId!=row.ResourceId||first.SourceId!=row.SourceId)throw new AttributionConflictException("MeasurementIdentityConflict");continue;}
            var scope=AttributionContract.AccessScope(row);
            if(await Scalar(c,t,"SELECT scope_id FROM attribution_visibility_scopes WHERE cluster_id=@cluster AND scope_id=@id",token,("@cluster",row.ClusterId),("@id",scope.ScopeId)) is null)
            {
                var count=Convert.ToInt64(await Scalar(c,t,"SELECT count(*) FROM attribution_visibility_scopes WHERE cluster_id=@cluster",token,("@cluster",row.ClusterId)),CultureInfo.InvariantCulture);
                if(count>=AttributionContract.MaximumVisibilityScopesPerCluster)throw new AttributionConflictException("AttributionScopeCapacityExceeded");
                await Run(c,t,"INSERT INTO attribution_visibility_scopes(cluster_id,scope_id,resource_id,team_id,application_id) VALUES(@cluster,@id,@resource,@team,@app)",token,("@cluster",row.ClusterId),("@id",scope.ScopeId),("@resource",scope.ResourceId),("@team",scope.TeamId),("@app",scope.ApplicationId));
            }
            await using(var overlap=Command(c,t,"SELECT body FROM attribution_measurements WHERE cluster_id=@cluster AND resource_id=@resource AND source_id=@source AND starts_utc<@to AND ends_utc>@from LIMIT 50001",("@cluster",row.ClusterId),("@resource",row.ResourceId),("@source",row.SourceId),("@from",Stamp(row.WindowStartUtc)),("@to",Stamp(row.WindowEndUtc))))
            {
                await using var r=await overlap.ExecuteReaderAsync(token);while(await r.ReadAsync(token))if(Deserialize<MeasurementWindow>(r.GetString(0)).MetricKind==row.MetricKind)throw new AttributionConflictException("MeasurementWindowOverlap");
            }
            await Run(c,t,"INSERT INTO attribution_measurements(window_id,cluster_id,resource_id,starts_utc,ends_utc,source_id,content_hash,body,scope_id) VALUES(@id,@cluster,@resource,@from,@to,@source,@hash,@body,@scope)",token,
                ("@id",row.WindowId),("@cluster",row.ClusterId),("@resource",row.ResourceId),("@from",Stamp(row.WindowStartUtc)),("@to",Stamp(row.WindowEndUtc)),("@source",row.SourceId),("@hash",hash),("@body",body),("@scope",scope.ScopeId));
        }
        return true;
    },token);

    public Task<IReadOnlyList<MeasurementWindow>> QueryAuthorizedAsync(MeasurementQuery q,Func<MeasurementAccessScope,bool> allowed,CancellationToken token=default)
    {
        AttributionContract.Query(q);ArgumentNullException.ThrowIfNull(allowed);
        return Transaction<IReadOnlyList<MeasurementWindow>>(async(c,t)=>
        {
            var scopes=new List<string>();
            await using(var inventory=Command(c,t,"SELECT scope_id,resource_id,team_id,application_id FROM attribution_visibility_scopes WHERE cluster_id=@cluster AND (CAST(@resource AS TEXT) IS NULL OR resource_id=@resource) ORDER BY scope_id LIMIT 10001",("@cluster",q.ClusterId),("@resource",q.ResourceId)))
            {
                await using var r=await inventory.ExecuteReaderAsync(token);var count=0;
                while(await r.ReadAsync(token))
                {
                    if(++count>AttributionContract.MaximumVisibilityScopesPerCluster)throw new AttributionConflictException("AttributionScopeCapacityExceeded");
                    var scope=new MeasurementAccessScope(r.GetString(0),q.ClusterId,r.GetString(1),r.IsDBNull(2)?null:r.GetString(2),r.IsDBNull(3)?null:r.GetString(3));
                    if(allowed(scope))scopes.Add(scope.ScopeId);
                }
            }
            if(scopes.Count==0)return Array.Empty<MeasurementWindow>();
            var parameters=scopes.Select((id,i)=>(Name:"@scope"+i.ToString(CultureInfo.InvariantCulture),Value:(object?)id)).ToArray();
            var values=new List<(string Name,object? Value)>{("@cluster",q.ClusterId),("@from",Stamp(q.FromUtc)),("@to",Stamp(q.ToUtc)),("@after",q.AfterId??""),("@take",q.PageSize+1)};values.AddRange(parameters);
            await using var cmd=Command(c,t,"SELECT body FROM attribution_measurements WHERE cluster_id=@cluster AND starts_utc>=@from AND ends_utc<=@to AND window_id>@after AND scope_id IN ("+string.Join(',',parameters.Select(p=>p.Name))+") ORDER BY window_id LIMIT @take",values.ToArray());
            await using var reader=await cmd.ExecuteReaderAsync(token);var rows=new List<MeasurementWindow>(q.PageSize+1);
            while(await reader.ReadAsync(token))
            {
                rows.Add(Deserialize<MeasurementWindow>(reader.GetString(0)));
            }
            return rows;
        },token);
    }
    public Task<RateCardVersion> StageRateAsync(string clusterId,RateCardDocument document,string actor,CancellationToken token=default)
    {
        AttributionContract.Text(clusterId,256);AttributionContract.Text(actor,2048);document=AttributionContract.Rate(document);
        var hash=AttributionContract.Hash(Serialize(document));var id=AttributionContract.Hash(clusterId+"\n"+hash);
        return Transaction(async(c,t)=>
        {
            var found=await ReadRate(c,t,clusterId,id,true,token);if(found is not null)return found;
            if(Convert.ToInt64(await Scalar(c,t,"SELECT count(*) FROM attribution_rate_versions WHERE cluster_id=@cluster",token,("@cluster",clusterId)),CultureInfo.InvariantCulture)>=10_000)throw new AttributionConflictException("RateCardCapacityExceeded");
            var version=new RateCardVersion(id,clusterId,hash,document,actor,DateTimeOffset.UtcNow,false);
            await Run(c,t,"INSERT INTO attribution_rate_versions(cluster_id,version_id,content_hash,committed,body) VALUES(@cluster,@id,@hash,0,@body)",token,("@cluster",clusterId),("@id",id),("@hash",hash),("@body",Serialize(version)));
            return version;
        },token);
    }
    public Task<RateCardVersion?> GetRateAsync(string clusterId,string versionId,bool includeDraft,CancellationToken token=default)
    {AttributionContract.Text(clusterId,256);AttributionContract.Digest(versionId);return Transaction((c,t)=>ReadRate(c,t,clusterId,versionId,includeDraft,token),token);}
    public Task<RateCardState> GetStateAsync(string clusterId,CancellationToken token=default)=>Transaction((c,t)=>State(c,t,clusterId,token),token);
    public Task<IReadOnlyList<RateCardVersion>> ListRatesAuthorizedAsync(string clusterId,string? afterId,int take,Func<RateCardVersion,bool> allowed,CancellationToken token=default)
    {
        AttributionContract.Text(clusterId,256);if(afterId is not null)AttributionContract.Digest(afterId);if(take is <1 or >201)throw new ArgumentException("Invalid rate page.");
        return Transaction<IReadOnlyList<RateCardVersion>>(async(c,t)=>
        {
            await using var cmd=Command(c,t,"SELECT body FROM attribution_rate_versions WHERE cluster_id=@cluster AND committed=1 AND version_id>@after ORDER BY version_id LIMIT 10001",("@cluster",clusterId),("@after",afterId??""));
            await using var reader=await cmd.ExecuteReaderAsync(token);var rows=new List<RateCardVersion>();var scanned=0;
            while(await reader.ReadAsync(token)){if(++scanned>10_000)throw new AttributionConflictException("RateCardCapacityExceeded");var row=Deserialize<RateCardVersion>(reader.GetString(0));if(allowed(row))rows.Add(row);if(rows.Count>=take)break;}
            return rows;
        },token);
    }
    public Task<RateCardVersion?> FindActiveRateAsync(string clusterId,string metricKind,DateTimeOffset fromUtc,DateTimeOffset toUtc,CancellationToken token=default)=>FindActiveRateAuthorizedAsync(clusterId,metricKind,fromUtc,toUtc,_=>true,token);
    public Task<RateCardVersion?> FindActiveRateAuthorizedAsync(string clusterId,string metricKind,DateTimeOffset fromUtc,DateTimeOffset toUtc,Func<RateCardVersion,bool> allowed,CancellationToken token=default)=>Transaction(async(c,t)=>
    {
        ArgumentNullException.ThrowIfNull(allowed);var rows=await ActiveRates(c,t,clusterId,token);var matches=rows.Where(v=>allowed(v)&&v.Document.MetricKind==metricKind&&v.Document.EffectiveFromUtc<=fromUtc&&v.Document.EffectiveToUtc>=toUtc).ToArray();
        if(matches.Length>1)throw new AttributionConflictException("RateCardCurrencyOrIntervalAmbiguous");return matches.SingleOrDefault();
    },token);

    public Task<MutationProviderResult> CommitRateAsync(MutationOperationSnapshot op,string versionId,string contentHash,long expectedRevision,bool enabled,Func<bool> currentAuthority,CancellationToken token=default)=>Transaction(async(c,t)=>
    {
        if(op.OperationKind is not(MutationOperationKind.RateCardVersion or MutationOperationKind.RateCardActivation))throw new ArgumentException("Wrong rate operation.");
        using(var material=JsonDocument.Parse(op.CanonicalIntent))
        {
            var root=material.RootElement;
            if(root.GetProperty("versionId").GetString()!=versionId||root.GetProperty("contentHash").GetString()!=contentHash||root.GetProperty("expectedRevision").GetInt64()!=expectedRevision||root.GetProperty("enabled").GetBoolean()!=enabled)throw new AttributionConflictException("RateCardMaterialMismatch");
        }
        if(!currentAuthority())throw new GovernanceEffectRejectedException("ScopeDenied");
        if(policy is not null)await policy.ValidateDispatchAsync(c,t,op,token);
        var receipt=await Receipt(c,t,op.OperationId,token);
        if(receipt is not null){if(!Matches(receipt,op)||receipt.VersionId!=versionId||receipt.ContentHash!=contentHash)throw new AttributionConflictException("RateCardReceiptConflict");return Result(receipt);}
        var version=await ReadRate(c,t,op.ClusterId,versionId,true,token)??throw new AttributionConflictException("RateCardVersionMissing");
        if(version.ContentHash!=contentHash||AttributionContract.Hash(Serialize(version.Document))!=contentHash)throw new AttributionConflictException("RateCardVersionConflict");
        await ValidateReferences(c,t,version,token);
        var state=await State(c,t,op.ClusterId,token);long revision=state.Revision;
        if(op.OperationKind==MutationOperationKind.RateCardVersion)
        {
            if(!version.Committed){version=version with{Committed=true};await Run(c,t,"UPDATE attribution_rate_versions SET committed=1,body=@body WHERE cluster_id=@cluster AND version_id=@id",token,("@body",Serialize(version)),("@cluster",op.ClusterId),("@id",versionId));}
        }
        else
        {
            if(!version.Committed||state.Revision!=expectedRevision||expectedRevision is <0 or long.MaxValue)throw new AttributionConflictException("RateCardVersionConflict");
            if(enabled)
            {
                var active=await ActiveRates(c,t,op.ClusterId,token);
                if(active.Any(v=>v.VersionId!=versionId&&v.Document.Unit==version.Document.Unit&&v.Document.Currency==version.Document.Currency&&v.Document.EffectiveFromUtc<version.Document.EffectiveToUtc&&v.Document.EffectiveToUtc>version.Document.EffectiveFromUtc))throw new AttributionConflictException("RateCardIntervalOverlap");
            }
            revision=checked(expectedRevision+1);
            await Run(c,t,"INSERT INTO attribution_rate_state(cluster_id,revision,version_id,enabled) VALUES(@cluster,@revision,@id,@enabled) ON CONFLICT(cluster_id) DO UPDATE SET revision=@revision,version_id=@id,enabled=@enabled",token,("@cluster",op.ClusterId),("@revision",revision),("@id",versionId),("@enabled",enabled?1:0));
            await Run(c,t,"INSERT INTO attribution_rate_bindings(cluster_id,version_id,enabled) VALUES(@cluster,@id,@enabled) ON CONFLICT(cluster_id,version_id) DO UPDATE SET enabled=@enabled",token,("@cluster",op.ClusterId),("@id",versionId),("@enabled",enabled?1:0));
        }
        var saved=new RateCardCommitReceipt(op.OperationId,(int)op.OperationKind,op.ClusterId,op.PreviewHash,AttributionContract.Hash(op.CanonicalIntent),versionId,contentHash,revision,enabled,generation?.DeploymentGeneration,DateTimeOffset.UtcNow);
        await Run(c,t,"INSERT INTO attribution_rate_receipts(operation_id,body) VALUES(@id,@body)",token,("@id",op.OperationId.ToString("D")),("@body",Serialize(saved)));
        await Run(c,t,"INSERT INTO attribution_rate_audit(operation_id,cluster_id,revision,body) VALUES(@id,@cluster,@revision,@body)",token,("@id",op.OperationId.ToString("D")),("@cluster",op.ClusterId),("@revision",revision),("@body",Serialize(saved)));
        if(!currentAuthority())throw new GovernanceEffectRejectedException("ScopeDenied");return Result(saved);
    },token);

    public Task<RateCardCommitReceipt?> ReadReceiptAsync(Guid id,CancellationToken token=default)=>Transaction((c,t)=>Receipt(c,t,id,token),token);
    public Task ValidateRateReferencesAsync(RateCardVersion version,CancellationToken token=default)=>Transaction(async(c,t)=>
    {await ValidateReferences(c,t,version,token);return true;},token);
    public Task VerifyPositiveReceiptAsync(MutationOperationSnapshot op,RateCardCommitReceipt receipt,Func<bool> currentAuthority,CancellationToken token=default)=>Transaction(async(c,t)=>
    {await ValidateRecoveryAsync(c,t,op,receipt,currentAuthority,token);return true;},token);
    public async Task ValidateRecoveryAsync(DbConnection c,DbTransaction t,MutationOperationSnapshot op,RateCardCommitReceipt expected,Func<bool> currentAuthority,CancellationToken token)
    {
        if(generation is not null)await generation.ValidateAsync(c,t,token);
        if(!currentAuthority())throw new GovernanceEffectRejectedException("ScopeDenied");var actual=await Receipt(c,t,op.OperationId,token);
        if(!Matches(actual,op)||actual!=expected||actual!.DeploymentGeneration!=generation?.DeploymentGeneration)throw new AttributionConflictException("RateCardPositiveReceiptRequired");
        var version=await ReadRate(c,t,op.ClusterId,actual.VersionId,false,token);if(version?.ContentHash!=actual.ContentHash)throw new AttributionConflictException("RateCardPositiveReceiptRequired");
        if(op.OperationKind==MutationOperationKind.RateCardActivation){var state=await State(c,t,op.ClusterId,token);if(state.Revision!=actual.Revision||state.VersionId!=actual.VersionId||state.Enabled!=actual.Enabled)throw new AttributionConflictException("RateCardRecoverySuperseded");}
        if(policy is not null)await policy.ValidateDispatchAsync(c,t,op,token);
        if(!currentAuthority())throw new GovernanceEffectRejectedException("ScopeDenied");
    }
    public static bool Matches(RateCardCommitReceipt? r,MutationOperationSnapshot op)=>r is not null&&r.OperationId==op.OperationId&&r.OperationKind==(int)op.OperationKind&&r.ClusterId==op.ClusterId&&r.PreviewHash==op.PreviewHash&&r.MaterialHash==AttributionContract.Hash(op.CanonicalIntent);
    public async Task<long> DeleteExpiredMeasurementsAsync(int retentionDays,DateTimeOffset now,CancellationToken token=default)
    {if(retentionDays is <1 or >366)throw new ArgumentException("Invalid measurement retention.");return await Transaction(async(c,t)=>{var deleted=await Run(c,t,"DELETE FROM attribution_measurements WHERE ends_utc<@before",token,("@before",Stamp(now.AddDays(-retentionDays))));await Run(c,t,"DELETE FROM attribution_visibility_scopes WHERE NOT EXISTS(SELECT 1 FROM attribution_measurements w WHERE w.cluster_id=attribution_visibility_scopes.cluster_id AND w.scope_id=attribution_visibility_scopes.scope_id)",token);return(long)deleted;},token);}
    private static MutationProviderResult Result(RateCardCommitReceipt r)=>new(MutationExecutionResultKind.AppliedVerified,"RateCardCommitted",new Dictionary<string,string>{["rate-card.version"]=r.VersionId,["rate-card.hash"]=r.ContentHash,["rate-card.revision"]=r.Revision.ToString(CultureInfo.InvariantCulture),["rate-card.receipt"]=r.OperationId.ToString("D")});
    private async Task ValidateReferences(DbConnection c,DbTransaction t,RateCardVersion version,CancellationToken token)
    {
        if(version.Document.Weights.Count==0)return;
        if(catalogReferences is null)throw new AttributionConflictException("RateCardReferenceAuthorityUnavailable");
        foreach(var weight in version.Document.Weights)
        {
            if(weight.TeamId is not null&&!await catalogReferences.IsActiveReferenceAsync(c,t,version.ClusterId,Kafdeck.Core.Catalog.CatalogEntityKind.Team,weight.TeamId,token))throw new AttributionConflictException("RateCardReferenceChanged");
            if(weight.ApplicationId is not null&&!await catalogReferences.IsActiveReferenceAsync(c,t,version.ClusterId,Kafdeck.Core.Catalog.CatalogEntityKind.Application,weight.ApplicationId,token))throw new AttributionConflictException("RateCardReferenceChanged");
        }
    }
    private async Task<T> Transaction<T>(Func<DbConnection,DbTransaction,Task<T>> action,CancellationToken token,bool fenced=true)
    {
        await using var c=await connections.OpenAsync(token);await using var t=await c.BeginTransactionAsync(token);
        if(fenced&&generation is not null)await generation.ValidateAsync(c,t,token);
        if(connections.SupportsSelectForUpdate)await Run(c,t,"SELECT pg_advisory_xact_lock(64964007601)",token);
        var result=await action(c,t);await t.CommitAsync(token);return result;
    }
    private static async Task<RateCardVersion?> ReadRate(DbConnection c,DbTransaction t,string cluster,string id,bool draft,CancellationToken token)
    {var value=await Scalar(c,t,"SELECT body FROM attribution_rate_versions WHERE cluster_id=@cluster AND version_id=@id AND (@draft=1 OR committed=1)",token,("@cluster",cluster),("@id",id),("@draft",draft?1:0));return value is string body?Deserialize<RateCardVersion>(body):null;}
    private static async Task<RateCardState> State(DbConnection c,DbTransaction t,string cluster,CancellationToken token)
    {
        AttributionContract.Text(cluster,256);await using var cmd=Command(c,t,"SELECT revision,version_id,enabled FROM attribution_rate_state WHERE cluster_id=@cluster",("@cluster",cluster));await using var r=await cmd.ExecuteReaderAsync(token);
        return await r.ReadAsync(token)?new(cluster,r.GetInt64(0),r.IsDBNull(1)?null:r.GetString(1),r.GetInt32(2)==1):new(cluster,0,null,false);
    }
    private static async Task<List<RateCardVersion>> ActiveRates(DbConnection c,DbTransaction t,string cluster,CancellationToken token)
    {
        await using var cmd=Command(c,t,"SELECT v.body FROM attribution_rate_versions v JOIN attribution_rate_bindings b ON b.cluster_id=v.cluster_id AND b.version_id=v.version_id WHERE b.cluster_id=@cluster AND b.enabled=1 AND v.committed=1 LIMIT 10001",("@cluster",cluster));
        await using var r=await cmd.ExecuteReaderAsync(token);var result=new List<RateCardVersion>();while(await r.ReadAsync(token)){if(result.Count==10_000)throw new AttributionConflictException("RateCardCapacityExceeded");result.Add(Deserialize<RateCardVersion>(r.GetString(0)));}return result;
    }
    private static async Task<RateCardCommitReceipt?> Receipt(DbConnection c,DbTransaction t,Guid id,CancellationToken token)
    {var value=await Scalar(c,t,"SELECT body FROM attribution_rate_receipts WHERE operation_id=@id",token,("@id",id.ToString("D")));return value is string body?Deserialize<RateCardCommitReceipt>(body):null;}
    private static string Serialize<T>(T value)=>JsonSerializer.Serialize(value,AttributionContract.Json);
    private static T Deserialize<T>(string value)=>JsonSerializer.Deserialize<T>(value,AttributionContract.Json)??throw new InvalidOperationException("Invalid stored attribution data.");
    private static string Stamp(DateTimeOffset value)=>value.ToUniversalTime().ToString("O",CultureInfo.InvariantCulture);
    private static DbCommand Command(DbConnection c,DbTransaction t,string sql,params(string Name,object? Value)[] args)
    {var cmd=c.CreateCommand();cmd.Transaction=t;cmd.CommandText=sql;foreach(var (name,value)in args){var p=cmd.CreateParameter();p.ParameterName=name;p.Value=value??DBNull.Value;cmd.Parameters.Add(p);}return cmd;}
    private static async Task<object?> Scalar(DbConnection c,DbTransaction t,string sql,CancellationToken token,params(string Name,object? Value)[] args){await using var cmd=Command(c,t,sql,args);return await cmd.ExecuteScalarAsync(token);}
    private static async Task<int> Run(DbConnection c,DbTransaction t,string sql,CancellationToken token,params(string Name,object? Value)[] args){await using var cmd=Command(c,t,sql,args);return await cmd.ExecuteNonQueryAsync(token);}
}
