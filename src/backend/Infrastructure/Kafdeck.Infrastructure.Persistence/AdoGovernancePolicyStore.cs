using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using Kafdeck.Core.Governance;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Npgsql;
using System.Collections.Concurrent;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>Policy versions and monotonic activation epochs share the mutation database and its dispatch transaction.</summary>
public sealed class AdoGovernancePolicyStore(
    IMutationDbConnectionFactory connections,
    IPolicyMetadataTransactionReader? metadata = null,
    IGovernanceGenerationTransactionGate? generationGate = null) : IGovernancePolicyStore, IGovernancePolicyTransactionGate
{
    public const string PreconditionKey = "v09.governance";
    public const string SchemaManifest = """
            CREATE TABLE IF NOT EXISTS governance_policy_schema (version INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS governance_policy_versions (
              cluster_id TEXT NOT NULL, version_id TEXT NOT NULL, content_hash TEXT NOT NULL,
              document_json TEXT NOT NULL, created_by TEXT NOT NULL, created_at_utc TEXT NOT NULL,
              committed INTEGER NOT NULL CHECK(committed IN (0,1)), committed_by TEXT NULL, operation_id TEXT NULL,
              PRIMARY KEY(cluster_id,version_id));
            CREATE TABLE IF NOT EXISTS governance_policy_state (
              cluster_id TEXT PRIMARY KEY, revision BIGINT NOT NULL CHECK(revision>0), enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
              version_id TEXT NOT NULL, content_hash TEXT NOT NULL, digest TEXT NOT NULL,
              FOREIGN KEY(cluster_id,version_id) REFERENCES governance_policy_versions(cluster_id,version_id));
            CREATE TABLE IF NOT EXISTS governance_policy_activations (
              cluster_id TEXT NOT NULL, revision BIGINT NOT NULL, enabled INTEGER NOT NULL,
              version_id TEXT NOT NULL, content_hash TEXT NOT NULL, digest TEXT NOT NULL,
              actor TEXT NOT NULL, operation_id TEXT NOT NULL, activated_at_utc TEXT NOT NULL,
              PRIMARY KEY(cluster_id,revision), UNIQUE(operation_id),
              FOREIGN KEY(cluster_id,version_id) REFERENCES governance_policy_versions(cluster_id,version_id));
            CREATE TABLE IF NOT EXISTS governance_policy_commit_receipts (
              operation_id TEXT PRIMARY KEY, operation_kind INTEGER NOT NULL CHECK(operation_kind IN (33,34)),
              cluster_id TEXT NOT NULL, version_id TEXT NOT NULL, content_hash TEXT NOT NULL,
              actor TEXT NOT NULL, preview_hash TEXT NOT NULL, material_hash TEXT NOT NULL,
              bound_digest TEXT NULL, deployment_generation TEXT NULL, revision BIGINT NOT NULL, enabled INTEGER NOT NULL,
              committed_at_utc TEXT NOT NULL,
              FOREIGN KEY(cluster_id,version_id) REFERENCES governance_policy_versions(cluster_id,version_id));
            """;
    public static readonly string SchemaFingerprint=GovernancePolicyCompiler.Hash(SchemaManifest.Replace("\r\n","\n",StringComparison.Ordinal));

    private string? _databaseIdentity;
    private readonly ConcurrentDictionary<(Guid Operation,string Cluster,string Resource),(string Identity,DateTimeOffset At)> _topicEvidence=new();
    public void CaptureTopicIdentity(Guid operationId,string cluster,string resource,string identity)
    {
        var now=DateTimeOffset.UtcNow;
        if(_topicEvidence.Count>=4096)
            foreach(var key in _topicEvidence.OrderBy(x=>x.Value.At).Take(1024).Select(x=>x.Key))_topicEvidence.TryRemove(key,out _);
        _topicEvidence[(operationId,cluster,resource)]=(identity,now);
    }
    public async Task InitializeAsync(CancellationToken token = default)
    {
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        _databaseIdentity=DatabaseIdentity(c);
        await using var tx = await c.BeginTransactionAsync(token).ConfigureAwait(false);
        if (connections.SupportsSelectForUpdate) await Execute(c, tx, "SELECT pg_advisory_xact_lock(64964007501)", token).ConfigureAwait(false);
        await Execute(c,tx,SchemaManifest,token).ConfigureAwait(false);
        await using(var ledger=Command(c,tx,"INSERT INTO governance_policy_schema(version,fingerprint) VALUES(1,@fingerprint) ON CONFLICT(version) DO NOTHING",("@fingerprint",SchemaFingerprint)))
            await ledger.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        await using var check = Command(c, tx, "SELECT version,fingerprint FROM governance_policy_schema");
        await using (var r = await check.ExecuteReaderAsync(token).ConfigureAwait(false))
        {
            if (!await r.ReadAsync(token).ConfigureAwait(false) || r.GetInt32(0) != 1 || r.GetString(1) != SchemaFingerprint || await r.ReadAsync(token).ConfigureAwait(false))
                throw new InvalidOperationException("Unsupported governance policy migration ledger.");
        }
        await tx.CommitAsync(token).ConfigureAwait(false);
    }

    public async Task<GovernancePolicyState> GetStateAsync(string clusterId, CancellationToken token = default)
    {
        GovernancePolicyCompiler.Bounded(clusterId, 256);
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx = await c.BeginTransactionAsync(token).ConfigureAwait(false);
        var state = await ReadState(c, tx, clusterId, false, token).ConfigureAwait(false);
        if (generationGate is not null) await generationGate.ValidateAsync(c, tx, token).ConfigureAwait(false);
        await tx.CommitAsync(token).ConfigureAwait(false);
        return state;
    }

    public async Task<GovernancePolicyVersion?> GetVersionAsync(string clusterId, string versionId, bool includeDraft = false, CancellationToken token = default)
    {
        GovernancePolicyCompiler.Bounded(clusterId, 256); GovernancePolicyCompiler.Bounded(versionId, 64);
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        return await ReadVersion(c, null, clusterId, versionId, includeDraft, token).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<GovernancePolicyVersion>> ListVersionsAsync(string clusterId, string? afterId, int limit, CancellationToken token = default)
    {
        GovernancePolicyCompiler.Bounded(clusterId, 256);
        if (limit is < 1 or > 201) throw new ArgumentOutOfRangeException(nameof(limit));
        if (afterId is not null) GovernancePolicyCompiler.Bounded(afterId, 64);
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var command = Command(c, null, "SELECT version_id,cluster_id,content_hash,document_json,created_by,created_at_utc FROM governance_policy_versions WHERE cluster_id=@cluster AND committed=1 AND version_id>@after ORDER BY version_id LIMIT @take",
            ("@cluster",clusterId),("@after",afterId??""),("@take",limit));
        await using var r = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        var result = new List<GovernancePolicyVersion>();
        while (await r.ReadAsync(token).ConfigureAwait(false)) result.Add(Version(r));
        return result;
    }

    public async Task<GovernancePolicyVersion> StageAsync(string clusterId, CompiledGovernancePolicy policy, string actor, CancellationToken token = default)
    {
        GovernancePolicyCompiler.Bounded(clusterId, 256); GovernancePolicyCompiler.Bounded(actor, 2048);
        if (policy.Document.Rules.Any(r => r.Scope.ClusterId != clusterId)) throw new ArgumentException("Policy selectors must bind the version cluster.");
        var id = GovernancePolicyCompiler.Hash(clusterId + "\n" + policy.ContentHash);
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx = await c.BeginTransactionAsync(token).ConfigureAwait(false);
        if (generationGate is not null) await generationGate.ValidateAsync(c, tx, token).ConfigureAwait(false);
        // A draft is immutable preview material only. Neither listing nor enforcement sees it before the governed version effect commits.
        await using var command = Command(c, tx, """
            INSERT INTO governance_policy_versions(cluster_id,version_id,content_hash,document_json,created_by,created_at_utc,committed)
            VALUES(@cluster,@id,@hash,@document,@actor,@at,0) ON CONFLICT(cluster_id,version_id) DO NOTHING
            """, ("@cluster",clusterId),("@id",id),("@hash",policy.ContentHash),("@document",policy.CanonicalJson),("@actor",actor),("@at",DateTimeOffset.UtcNow.ToString("O",CultureInfo.InvariantCulture)));
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        var version = await ReadVersion(c, tx, clusterId, id, true, token).ConfigureAwait(false) ?? throw new InvalidOperationException("Draft unavailable.");
        if (version.ContentHash != policy.ContentHash || version.Document != policy.CanonicalJson) throw new InvalidOperationException("Draft fingerprint conflict.");
        await tx.CommitAsync(token).ConfigureAwait(false);
        return version;
    }

    public async Task ValidateDispatchAsync(DbConnection c, DbTransaction tx, MutationOperationSnapshot operation, CancellationToken token)
    {
        try
        {
            if (generationGate is not null) await generationGate.ValidateAsync(c, tx, token).ConfigureAwait(false);
            foreach (var cluster in operation.AuthorizationTargets.Select(t=>t.ClusterId).Append(operation.ClusterId).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal))
            {
                var state = await ReadState(c, tx, cluster, true, token).ConfigureAwait(false);
                var markers = operation.Preconditions.Where(p => p.Key == PreconditionKey+":"+cluster).ToArray();
                if (markers.Length > 1 || markers.SingleOrDefault()?.Fingerprint != state.Digest)
                    throw new GovernanceEffectRejectedException("GovernancePolicyChanged", true);
                if (!state.Enabled) continue;
                var version = await ReadVersion(c, tx, state.ClusterId, state.VersionId!, false, token).ConfigureAwait(false);
                if (version is null || version.ContentHash != state.ContentHash) throw new GovernanceEffectRejectedException("GovernanceUnavailable");
                var policy = GovernancePolicyCompiler.Compile(version.Document);
                if (policy.ContentHash != version.ContentHash) throw new GovernanceEffectRejectedException("GovernanceUnavailable");
                foreach (var target in operation.AuthorizationTargets.Where(t=>t.ClusterId==cluster))
                {
                    var kind = ResourceKind(target.Action.ToString());
                    var facts = kind == "topic" && metadata is not null ? await metadata.ReadAsync(c, tx, target.ClusterId, target.ResourceName, token).ConfigureAwait(false) : null;
                    var requiresMetadata=policy.Document.Rules.Any(r=>r.Scope.ClusterId==cluster&&r.Scope.ResourceKind==kind&&r.Scope.ResourceIds.Contains(target.ResourceName,StringComparer.Ordinal)&&
                        r.Scope.Action.Equals(target.Action.ToString(),StringComparison.OrdinalIgnoreCase)&&r.Conditions.Any(condition=>condition.Field is not("risk" or "requestApproved")));
                    if(requiresMetadata&&(facts is null||!_topicEvidence.TryGetValue((operation.OperationId,cluster,target.ResourceName),out var observed)||
                        observed.At<DateTimeOffset.UtcNow.AddSeconds(-15)||!facts.TryGetValue("topicIdentity",out var boundIdentity)||boundIdentity!=observed.Identity))
                        throw new GovernanceEffectRejectedException("GovernanceEvidenceUnavailable");
                    var result = GovernancePolicyCompiler.Evaluate(policy, new(target.ClusterId,kind,target.ResourceName,target.Action.ToString(),operation.Risk.RiskClass.ToString()), facts, state.Digest);
                    if (!result.Allowed) throw new GovernanceEffectRejectedException(result.Code);
                }
            }
        }
        catch (GovernanceEffectRejectedException) { throw; }
        catch (OperationCanceledException) { throw; }
        catch (Exception) { throw new GovernanceEffectRejectedException("GovernanceUnavailable"); }
    }

    public async Task ValidateOperationAsync(MutationOperationSnapshot operation,CancellationToken token)
    {
        await using var c=await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx=await c.BeginTransactionAsync(token).ConfigureAwait(false);
        await ValidateDispatchAsync(c,tx,operation,token).ConfigureAwait(false);
        await tx.CommitAsync(token).ConfigureAwait(false);
    }

    /// <summary>Acquire before opening another store's metadata transaction. A policy activation cannot
    /// commit until that bounded metadata effect completes; no network effect runs inside this lease.</summary>
    public async Task<IAsyncDisposable> AcquireCommitLeaseAsync(MutationOperationSnapshot operation,CancellationToken token)
    {
        var c=await connections.OpenAsync(token).ConfigureAwait(false);
        DbTransaction? tx=null;
        try
        {
            tx=c is SqliteConnection sqlite ? sqlite.BeginTransaction(deferred:true) : await c.BeginTransactionAsync(token).ConfigureAwait(false);
            await ValidateDispatchAsync(c,tx,operation,token).ConfigureAwait(false);
            return new ReadLease(c,tx);
        }
        catch{if(tx is not null)await tx.DisposeAsync().ConfigureAwait(false);await c.DisposeAsync().ConfigureAwait(false);throw;}
    }

    public async Task<IAsyncDisposable> AcquireGlobalControlLeaseAsync(string resourceId,CancellationToken token)
    {
        GovernancePolicyCompiler.Bounded(resourceId,512);
        var c=await connections.OpenAsync(token).ConfigureAwait(false);
        DbTransaction? tx=null;
        try
        {
            tx=c is SqliteConnection sqlite ? sqlite.BeginTransaction(deferred:true) : await c.BeginTransactionAsync(token).ConfigureAwait(false);
            if(generationGate is not null)await generationGate.ValidateAsync(c,tx,token).ConfigureAwait(false);
            var state=await ReadState(c,tx,"$deployment",true,token).ConfigureAwait(false);
            if(state.Enabled)
            {
                var v=await ReadVersion(c,tx,"$deployment",state.VersionId!,false,token).ConfigureAwait(false)??throw new GovernanceEffectRejectedException("GovernanceUnavailable");
                var p=GovernancePolicyCompiler.Compile(v.Document);
                if(v.ContentHash!=state.ContentHash||p.ContentHash!=v.ContentHash)throw new GovernanceEffectRejectedException("GovernanceUnavailable");
                var decision=GovernancePolicyCompiler.Evaluate(p,new("$deployment","notification",resourceId,"NotificationManage","moderate"),null,state.Digest);
                if(!decision.Allowed)throw new GovernanceEffectRejectedException(decision.Code);
            }
            return new ReadLease(c,tx);
        }
        catch{if(tx is not null)await tx.DisposeAsync().ConfigureAwait(false);await c.DisposeAsync().ConfigureAwait(false);throw;}
    }

    private sealed class ReadLease(DbConnection connection,DbTransaction transaction):IAsyncDisposable
    {
        public async ValueTask DisposeAsync(){await transaction.DisposeAsync().ConfigureAwait(false);await connection.DisposeAsync().ConfigureAwait(false);}
    }

    public async Task<IAsyncDisposable> AcquireMetadataLeaseAsync(INotificationDeliveryDbConnectionFactory target,
        MutationOperationSnapshot? operation,string? globalResource,CancellationToken token)
    {
        await using var connection=await target.OpenAsync(token).ConfigureAwait(false);
        if(IsOwnDatabase(connection))return new NoopLease();
        return operation is null?await AcquireGlobalControlLeaseAsync(globalResource!,token).ConfigureAwait(false):await AcquireCommitLeaseAsync(operation,token).ConfigureAwait(false);
    }

    public async Task ValidateMetadataTransactionAsync(DbConnection connection,DbTransaction transaction,
        MutationOperationSnapshot? operation,string? globalResource,CancellationToken token)
    {
        if(!IsOwnDatabase(connection))return; // Separate authority database is protected by the pre-acquired read lease.
        if(operation is not null){await ValidateDispatchAsync(connection,transaction,operation,token).ConfigureAwait(false);return;}
        if(generationGate is not null)await generationGate.ValidateAsync(connection,transaction,token).ConfigureAwait(false);
        var state=await ReadState(connection,transaction,"$deployment",true,token).ConfigureAwait(false);
        if(!state.Enabled)return;
        var v=await ReadVersion(connection,transaction,"$deployment",state.VersionId!,false,token).ConfigureAwait(false)??throw new GovernanceEffectRejectedException("GovernanceUnavailable");
        var policy=GovernancePolicyCompiler.Compile(v.Document);
        if(policy.ContentHash!=v.ContentHash||v.ContentHash!=state.ContentHash)throw new GovernanceEffectRejectedException("GovernanceUnavailable");
        var result=GovernancePolicyCompiler.Evaluate(policy,new("$deployment","notification",globalResource!,"NotificationManage","moderate"),null,state.Digest);
        if(!result.Allowed)throw new GovernanceEffectRejectedException(result.Code);
    }

    private bool IsOwnDatabase(DbConnection connection)=>_databaseIdentity is not null&&_databaseIdentity==DatabaseIdentity(connection);
    private static string DatabaseIdentity(DbConnection connection)
    {
        if(connection is SqliteConnection)
        {
            var path=Path.GetFullPath(new SqliteConnectionStringBuilder(connection.ConnectionString).DataSource);
            return "sqlite:"+(OperatingSystem.IsWindows()?path.ToUpperInvariant():path);
        }
        if(connection is NpgsqlConnection)
        {
            var b=new NpgsqlConnectionStringBuilder(connection.ConnectionString);
            return $"pg:{b.Host?.ToLowerInvariant()}:{b.Port}:{b.Database}:{b.SearchPath}";
        }
        throw new GovernanceEffectRejectedException("GovernanceDatabaseUnsupported");
    }
    private sealed class NoopLease:IAsyncDisposable {public ValueTask DisposeAsync()=>ValueTask.CompletedTask;}

    public async Task<MutationProviderResult> CommitVersionAsync(MutationOperationSnapshot operation, string versionId, string hash, Func<bool> authority, CancellationToken token)
    {
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx = await c.BeginTransactionAsync(token).ConfigureAwait(false);
        if (generationGate is not null) await generationGate.ValidateAsync(c, tx, token).ConfigureAwait(false);
        await ValidateDispatchAsync(c, tx, operation, token).ConfigureAwait(false);
        if (!authority()) throw new GovernanceEffectRejectedException("CurrentAuthorityDenied");
        var version = await ReadVersion(c, tx, operation.ClusterId, versionId, true, token).ConfigureAwait(false);
        if (version is null || version.ContentHash != hash) return Failure("PolicyVersionConflict");
        await using var update = Command(c, tx, "UPDATE governance_policy_versions SET committed=1,committed_by=@actor,operation_id=@op WHERE cluster_id=@cluster AND version_id=@id AND content_hash=@hash AND committed=0",
            ("@actor",operation.RequesterPrincipalId),("@op",operation.OperationId.ToString("D")),("@cluster",operation.ClusterId),("@id",versionId),("@hash",hash));
        await update.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        if (!authority()) throw new GovernanceEffectRejectedException("CurrentAuthorityDenied");
        await WriteReceipt(c,tx,operation,versionId,hash,0,false,token).ConfigureAwait(false);
        await tx.CommitAsync(token).ConfigureAwait(false);
        return new(MutationExecutionResultKind.AppliedVerified,"PolicyVersionCommitted",new Dictionary<string,string>{{"policy.version",versionId},{"policy.hash",hash}});
    }

    public async Task<MutationProviderResult> ActivateAsync(MutationOperationSnapshot operation, string versionId, string hash, long expectedRevision, bool enabled, Func<bool> authority, CancellationToken token)
    {
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx = await c.BeginTransactionAsync(token).ConfigureAwait(false);
        if (generationGate is not null) await generationGate.ValidateAsync(c, tx, token).ConfigureAwait(false);
        await ValidateDispatchAsync(c, tx, operation, token).ConfigureAwait(false);
        if (!authority()) throw new GovernanceEffectRejectedException("CurrentAuthorityDenied");
        var version = await ReadVersion(c, tx, operation.ClusterId, versionId, false, token).ConfigureAwait(false);
        if (version is null || version.ContentHash != hash) return Failure("PolicyVersionConflict");
        var revision = checked(expectedRevision + 1);
        var digest = GovernancePolicyCompiler.Hash("v1\n"+operation.ClusterId+"\n"+revision.ToString(CultureInfo.InvariantCulture)+"\n"+(enabled?"1":"0")+"\n"+versionId+"\n"+hash);
        await using var command = expectedRevision == 0 ? Command(c, tx, """
            INSERT INTO governance_policy_state(cluster_id,revision,enabled,version_id,content_hash,digest)
            VALUES(@cluster,@revision,@enabled,@id,@hash,@digest) ON CONFLICT(cluster_id) DO NOTHING
            """) : Command(c, tx, """
            UPDATE governance_policy_state SET revision=@revision,enabled=@enabled,version_id=@id,content_hash=@hash,digest=@digest
            WHERE cluster_id=@cluster AND revision=@expected
            """, ("@expected",expectedRevision));
        Add(command,"@cluster",operation.ClusterId);Add(command,"@revision",revision);Add(command,"@enabled",enabled?1:0);Add(command,"@id",versionId);Add(command,"@hash",hash);Add(command,"@digest",digest);
        if (await command.ExecuteNonQueryAsync(token).ConfigureAwait(false) != 1) return Failure("PolicyVersionConflict");
        await using var audit = Command(c, tx, """
            INSERT INTO governance_policy_activations(cluster_id,revision,enabled,version_id,content_hash,digest,actor,operation_id,activated_at_utc)
            VALUES(@cluster,@revision,@enabled,@id,@hash,@digest,@actor,@op,@at)
            """,("@cluster",operation.ClusterId),("@revision",revision),("@enabled",enabled?1:0),("@id",versionId),("@hash",hash),("@digest",digest),
            ("@actor",operation.RequesterPrincipalId),("@op",operation.OperationId.ToString("D")),("@at",DateTimeOffset.UtcNow.ToString("O",CultureInfo.InvariantCulture)));
        await audit.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        if (!authority()) throw new GovernanceEffectRejectedException("CurrentAuthorityDenied");
        await WriteReceipt(c,tx,operation,versionId,hash,revision,enabled,token).ConfigureAwait(false);
        await tx.CommitAsync(token).ConfigureAwait(false);
        return new(MutationExecutionResultKind.AppliedVerified,"PolicyActivated",new Dictionary<string,string>{{"policy.version",versionId},{"policy.hash",hash},{"policy.revision",revision.ToString(CultureInfo.InvariantCulture)},{"policy.digest",digest}});
    }

    public static string ResourceKind(string action)=>GovernancePolicyCompiler.ResourceKind(action);

    public async Task<GovernanceCommitReceipt?> ReadReceiptAsync(Guid operationId,CancellationToken token=default)
    {
        await using var c=await connections.OpenAsync(token).ConfigureAwait(false);
        return await ReadReceiptAsync(c,null,operationId,token).ConfigureAwait(false);
    }

    public async Task<GovernanceCommitReceipt?> ReadReceiptAsync(DbConnection c,DbTransaction? tx,Guid operationId,CancellationToken token)
    {
        await using var cmd=Command(c,tx,"SELECT operation_kind,cluster_id,version_id,content_hash,actor,preview_hash,material_hash,bound_digest,deployment_generation,revision,enabled,committed_at_utc FROM governance_policy_commit_receipts WHERE operation_id=@id",("@id",operationId.ToString("D")));
        await using var r=await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        return await r.ReadAsync(token).ConfigureAwait(false)?new(operationId,r.GetInt32(0),r.GetString(1),r.GetString(2),r.GetString(3),r.GetString(4),r.GetString(5),r.GetString(6),r.IsDBNull(7)?null:r.GetString(7),r.IsDBNull(8)?null:r.GetString(8),r.GetInt64(9),r.GetInt32(10)==1,DateTimeOffset.Parse(r.GetString(11),CultureInfo.InvariantCulture)):null;
    }

    public async Task ValidateRecoveryAsync(DbConnection c,DbTransaction tx,MutationOperationSnapshot source,
        GovernanceCommitReceipt expected,Func<bool> authority,CancellationToken token)
    {
        if(generationGate is not null)await generationGate.ValidateAsync(c,tx,token).ConfigureAwait(false);
        var fresh=await ReadReceiptAsync(c,tx,source.OperationId,token).ConfigureAwait(false);
        if(fresh!=expected||!Matches(fresh,source)||!authority())throw new GovernanceEffectRejectedException("PolicyCommitReceiptConflict");
    }

    public static bool Matches(GovernanceCommitReceipt? receipt,MutationOperationSnapshot operation)=>receipt is not null &&
        receipt.OperationId==operation.OperationId&&receipt.OperationKind==(int)operation.OperationKind&&receipt.ClusterId==operation.ClusterId&&
        receipt.Actor==operation.RequesterPrincipalId&&receipt.PreviewHash==operation.PreviewHash&&
        receipt.MaterialHash==GovernancePolicyCompiler.Hash(operation.CanonicalIntent)&&
        receipt.BoundDigest==operation.Preconditions.SingleOrDefault(p=>p.Key==PreconditionKey+":"+operation.ClusterId)?.Fingerprint;

    private async Task WriteReceipt(DbConnection c,DbTransaction tx,MutationOperationSnapshot operation,string version,string hash,long revision,bool enabled,CancellationToken token)
    {
        var bound=operation.Preconditions.SingleOrDefault(p=>p.Key==PreconditionKey+":"+operation.ClusterId)?.Fingerprint;
        await using var cmd=Command(c,tx,"""
            INSERT INTO governance_policy_commit_receipts(operation_id,operation_kind,cluster_id,version_id,content_hash,actor,
              preview_hash,material_hash,bound_digest,deployment_generation,revision,enabled,committed_at_utc)
            VALUES(@op,@kind,@cluster,@version,@hash,@actor,@preview,@material,@bound,@generation,@revision,@enabled,@at)
            ON CONFLICT(operation_id) DO NOTHING
            """,("@op",operation.OperationId.ToString("D")),("@kind",(int)operation.OperationKind),("@cluster",operation.ClusterId),("@version",version),("@hash",hash),
            ("@actor",operation.RequesterPrincipalId),("@preview",operation.PreviewHash),("@material",GovernancePolicyCompiler.Hash(operation.CanonicalIntent)),
            ("@bound",(object?)bound??DBNull.Value),("@generation",(object?)generationGate?.DeploymentGeneration??DBNull.Value),("@revision",revision),("@enabled",enabled?1:0),("@at",DateTimeOffset.UtcNow.ToString("O",CultureInfo.InvariantCulture)));
        await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        var receipt=await ReadReceiptAsync(c,tx,operation.OperationId,token).ConfigureAwait(false);
        if(!Matches(receipt,operation)||receipt!.VersionId!=version||receipt.ContentHash!=hash||receipt.Revision!=revision||receipt.Enabled!=enabled)
            throw new GovernanceEffectRejectedException("PolicyCommitReceiptConflict");
    }

    private async Task<GovernancePolicyState> ReadState(DbConnection c, DbTransaction tx, string cluster, bool locked, CancellationToken token)
    {
        if (locked && connections.SupportsSelectForUpdate)
        {
            await using var mutex = Command(c,tx,"SELECT pg_advisory_xact_lock(hashtextextended(@key, 7501))",("@key",cluster));
            await mutex.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }
        await using var cmd = Command(c,tx,"SELECT revision,enabled,version_id,content_hash,digest FROM governance_policy_state WHERE cluster_id=@cluster",("@cluster",cluster));
        await using var r = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        var state=await r.ReadAsync(token).ConfigureAwait(false) ? new GovernancePolicyState(cluster,r.GetInt64(0),r.GetInt32(1)==1,r.GetString(2),r.GetString(3),r.GetString(4)) : new(cluster,0,false,null,null,null);
        // The outside deployment identity survives neither an old worker nor an old approval.
        // Never-enrolled stores retain the exact v0.8 absent-marker behavior.
        return generationGate?.DeploymentGeneration is { } generation
            ? state with{Digest=GovernancePolicyCompiler.Hash("deployment:"+generation+"\n"+(state.Digest??"never"))} : state;
    }
    private static async Task<GovernancePolicyVersion?> ReadVersion(DbConnection c, DbTransaction? tx, string cluster, string id, bool draft, CancellationToken token)
    {
        await using var cmd = Command(c,tx,"SELECT version_id,cluster_id,content_hash,document_json,created_by,created_at_utc FROM governance_policy_versions WHERE cluster_id=@cluster AND version_id=@id"+(draft?"":" AND committed=1"),("@cluster",cluster),("@id",id));
        await using var r = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        return await r.ReadAsync(token).ConfigureAwait(false) ? Version(r) : null;
    }
    private static GovernancePolicyVersion Version(DbDataReader r) => new(r.GetString(0),r.GetString(1),r.GetString(2),r.GetString(3),r.GetString(4),DateTimeOffset.Parse(r.GetString(5),CultureInfo.InvariantCulture));
    private static MutationProviderResult Failure(string code) => new(MutationExecutionResultKind.FailedDefinitive,code);
    private static DbCommand Command(DbConnection c, DbTransaction? tx, string sql, params (string Name, object Value)[] args)
    { var cmd=c.CreateCommand();cmd.Transaction=tx;cmd.CommandText=sql;foreach(var a in args)Add(cmd,a.Name,a.Value);return cmd; }
    private static void Add(DbCommand c,string name,object value) { var p=c.CreateParameter();p.ParameterName=name;p.Value=value;c.Parameters.Add(p); }
    private static async Task Execute(DbConnection c,DbTransaction tx,string sql,CancellationToken token) { await using var cmd=Command(c,tx,sql);await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false); }
}
