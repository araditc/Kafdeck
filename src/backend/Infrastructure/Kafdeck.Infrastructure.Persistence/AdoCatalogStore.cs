using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Governance;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>Catalog-owned SQL. Plans, revisions, references and receipts commit atomically on the owning connection.</summary>
public sealed class AdoCatalogStore(IMutationDbConnectionFactory connections, string deploymentGeneration,
    IGovernanceGenerationTransactionGate? generationGate = null) : ICatalogStore, IPolicyMetadataTransactionReader
{
    public string DeploymentGeneration { get; } = CatalogContract.Bound(deploymentGeneration, "generation", 128, false);
    private const string Schema = """
        CREATE TABLE IF NOT EXISTS catalog_owned_schema(version INTEGER PRIMARY KEY,digest TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS catalog_owned_entities(
          cluster_id TEXT NOT NULL,kind INTEGER NOT NULL CHECK(kind IN (1,2,3)),id TEXT NOT NULL,
          revision BIGINT NOT NULL CHECK(revision>0),topic_name TEXT NULL,body TEXT NOT NULL,
          PRIMARY KEY(cluster_id,kind,id),UNIQUE(cluster_id,topic_name));
        CREATE TABLE IF NOT EXISTS catalog_owned_references(
          cluster_id TEXT NOT NULL,source_kind INTEGER NOT NULL,source_id TEXT NOT NULL,
          target_kind INTEGER NOT NULL,target_id TEXT NOT NULL,
          PRIMARY KEY(cluster_id,source_kind,source_id,target_kind,target_id),
          FOREIGN KEY(cluster_id,source_kind,source_id) REFERENCES catalog_owned_entities(cluster_id,kind,id) DEFERRABLE INITIALLY DEFERRED,
          FOREIGN KEY(cluster_id,target_kind,target_id) REFERENCES catalog_owned_entities(cluster_id,kind,id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TABLE IF NOT EXISTS catalog_owned_history(
          cluster_id TEXT NOT NULL,kind INTEGER NOT NULL,id TEXT NOT NULL,revision BIGINT NOT NULL,
          body TEXT NOT NULL,actor_fingerprint TEXT NOT NULL,authority_fingerprint TEXT NOT NULL,
          operation_id TEXT NOT NULL,effective_from_utc TEXT NOT NULL,effective_to_utc TEXT NULL,
          PRIMARY KEY(cluster_id,kind,id,revision),
          FOREIGN KEY(cluster_id,kind,id) REFERENCES catalog_owned_entities(cluster_id,kind,id));
        CREATE TABLE IF NOT EXISTS catalog_owned_plans(
          plan_id TEXT PRIMARY KEY,content_hash TEXT NOT NULL,body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS catalog_owned_receipts(
          operation_id TEXT PRIMARY KEY,content_hash TEXT NOT NULL,body TEXT NOT NULL,
          FOREIGN KEY(operation_id) REFERENCES catalog_owned_plans(plan_id));
        CREATE INDEX IF NOT EXISTS catalog_owned_entities_page ON catalog_owned_entities(cluster_id,kind,id);
        """;

    public async Task InitializeAsync(CancellationToken token)
    {
        await using var db = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx = await db.BeginTransactionAsync(token).ConfigureAwait(false);
        await LockAsync(db, tx, token).ConfigureAwait(false);
        await RunAsync(db, tx, Schema, token).ConfigureAwait(false);
        var digest = CatalogContract.Hash(Schema.Replace("\r\n", "\n", StringComparison.Ordinal));
        await RunAsync(db, tx, "INSERT INTO catalog_owned_schema(version,digest) VALUES(1,@hash) ON CONFLICT(version) DO NOTHING", token, ("@hash", digest)).ConfigureAwait(false);
        var stored = await ScalarAsync(db, tx, "SELECT digest FROM catalog_owned_schema WHERE version=1", token).ConfigureAwait(false);
        if (!string.Equals(stored as string, digest, StringComparison.Ordinal)) throw new InvalidOperationException("Catalog migration digest mismatch.");
        await tx.CommitAsync(token).ConfigureAwait(false);
    }

    public async Task<CatalogRecord?> GetAsync(string clusterId, CatalogEntityKind kind, string id, CancellationToken token)
    {
        CatalogContract.Id(id); ValidateScope(clusterId, kind);
        return await ReadAsync((db, tx) => LoadAsync(db, tx, clusterId, kind, id, token), token).ConfigureAwait(false);
    }
    public async Task<CatalogRecord?> FindTopicAsync(string clusterId, string topicName, CancellationToken token) =>
        await ReadAsync(async (db, tx) =>
        {
            var value = await ScalarAsync(db, tx, "SELECT body FROM catalog_owned_entities WHERE cluster_id=@cluster AND kind=3 AND topic_name=@topic", token,
                ("@cluster", clusterId), ("@topic", topicName)).ConfigureAwait(false);
            return value is string json ? Deserialize<CatalogRecord>(json) : null;
        }, token).ConfigureAwait(false);

    public async Task<IReadOnlyList<CatalogRecord>> ListAsync(string clusterId, CatalogEntityKind kind, string? afterId, int take, CancellationToken token)
    {
        ValidateScope(clusterId, kind); if (take is < 1 or > 201) throw new ArgumentOutOfRangeException(nameof(take));
        if (afterId is not null) CatalogContract.Id(afterId);
        return await ReadAsync<IReadOnlyList<CatalogRecord>>(async (db, tx) =>
        {
            await using var command = Command(db, tx, "SELECT body FROM catalog_owned_entities WHERE cluster_id=@cluster AND kind=@kind AND id>@after ORDER BY id LIMIT @take",
                ("@cluster", clusterId), ("@kind", (int)kind), ("@after", afterId ?? ""), ("@take", take));
            await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
            var rows = new List<CatalogRecord>();
            while (await reader.ReadAsync(token).ConfigureAwait(false)) rows.Add(Deserialize<CatalogRecord>(reader.GetString(0)));
            return rows;
        }, token).ConfigureAwait(false);
    }
    public async Task<IReadOnlyList<CatalogRecord>> ListAuthorizedAsync(string clusterId, CatalogEntityKind kind, string? afterId,
        int take, Func<CatalogRecord, bool> canRead, CancellationToken token)
    {
        ValidateScope(clusterId, kind); ArgumentNullException.ThrowIfNull(canRead);
        if (take is < 1 or > 201) throw new ArgumentOutOfRangeException(nameof(take));
        if (afterId is not null) CatalogContract.Id(afterId);
        return await ReadAsync<IReadOnlyList<CatalogRecord>>(async (db, tx) =>
        {
            await using var command = Command(db, tx, "SELECT body FROM catalog_owned_entities WHERE cluster_id=@cluster AND kind=@kind AND id>@after ORDER BY id LIMIT @scan",
                ("@cluster", clusterId), ("@kind", (int)kind), ("@after", afterId ?? ""), ("@scan", CatalogContract.MaxEntitiesPerScope + 1));
            await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
            var rows = new List<CatalogRecord>(); var scanned = 0;
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                if (++scanned > CatalogContract.MaxEntitiesPerScope) throw new CatalogConflictException("CatalogCapacityInvariantViolated");
                var row = Deserialize<CatalogRecord>(reader.GetString(0));
                if (canRead(row)) rows.Add(row);
                if (rows.Count == take) break;
            }
            return rows;
        }, token).ConfigureAwait(false);
    }
    public async Task<IReadOnlyList<CatalogHistoryItem>> HistoryAsync(string clusterId, CatalogEntityKind kind, string id, long afterRevision, int take, CancellationToken token)
    {
        ValidateScope(clusterId, kind); CatalogContract.Id(id);
        if (afterRevision < 0 || take is < 1 or > 201) throw new ArgumentOutOfRangeException(nameof(take));
        return await ReadAsync<IReadOnlyList<CatalogHistoryItem>>(async (db, tx) =>
        {
            await using var command = Command(db, tx, "SELECT body,actor_fingerprint,authority_fingerprint,operation_id,effective_from_utc,effective_to_utc FROM catalog_owned_history WHERE cluster_id=@cluster AND kind=@kind AND id=@id AND revision>@after ORDER BY revision LIMIT @take",
                ("@cluster", clusterId), ("@kind", (int)kind), ("@id", id), ("@after", afterRevision), ("@take", take));
            await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
            var rows = new List<CatalogHistoryItem>();
            while (await reader.ReadAsync(token).ConfigureAwait(false)) rows.Add(new(Deserialize<CatalogRecord>(reader.GetString(0)), reader.GetString(1), reader.GetString(2),
                Guid.Parse(reader.GetString(3)), DateTimeOffset.Parse(reader.GetString(4), CultureInfo.InvariantCulture),
                reader.IsDBNull(5) ? null : DateTimeOffset.Parse(reader.GetString(5), CultureInfo.InvariantCulture)));
            return rows;
        }, token).ConfigureAwait(false);
    }
    public async Task<IReadOnlyList<CatalogHistoryItem>> HistoryAuthorizedAsync(string clusterId, CatalogEntityKind kind, string id,
        long afterRevision, int take, Func<CatalogRecord, bool> canRead, CancellationToken token)
    {
        ValidateScope(clusterId, kind); CatalogContract.Id(id); ArgumentNullException.ThrowIfNull(canRead);
        if (afterRevision < 0 || take is < 1 or > 201) throw new ArgumentOutOfRangeException(nameof(take));
        return await ReadAsync<IReadOnlyList<CatalogHistoryItem>>(async (db, tx) =>
        {
            await using var command = Command(db, tx, "SELECT body,actor_fingerprint,authority_fingerprint,operation_id,effective_from_utc,effective_to_utc FROM catalog_owned_history WHERE cluster_id=@cluster AND kind=@kind AND id=@id AND revision>@after ORDER BY revision LIMIT @scan",
                ("@cluster", clusterId), ("@kind", (int)kind), ("@id", id), ("@after", afterRevision), ("@scan", CatalogContract.MaxRevisionsPerEntity + 1));
            await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
            var rows = new List<CatalogHistoryItem>(); var scanned = 0;
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                if (++scanned > CatalogContract.MaxRevisionsPerEntity) throw new CatalogConflictException("CatalogHistoryCapacityInvariantViolated");
                var record = Deserialize<CatalogRecord>(reader.GetString(0));
                if (!canRead(record)) continue;
                rows.Add(new(record, reader.GetString(1), reader.GetString(2), Guid.Parse(reader.GetString(3)),
                    DateTimeOffset.Parse(reader.GetString(4), CultureInfo.InvariantCulture), reader.IsDBNull(5) ? null : DateTimeOffset.Parse(reader.GetString(5), CultureInfo.InvariantCulture)));
                if (rows.Count == take) break;
            }
            return rows;
        }, token).ConfigureAwait(false);
    }
    public async Task<CatalogPlan?> GetPlanAsync(Guid planId, CancellationToken token) =>
        await ReadAsync((db, tx) => LoadPlanAsync(db, tx, planId, token), token).ConfigureAwait(false);
    public async Task<CatalogCommitReceipt?> GetReceiptAsync(Guid operationId, CancellationToken token) =>
        await ReadAsync((db, tx) => LoadReceiptAsync(db, tx, operationId, token), token).ConfigureAwait(false);

    public async Task<CatalogPlan> StageAsync(CatalogPlan plan, CancellationToken token)
    {
        CatalogContract.ValidatePlan(plan);
        if (plan.DeploymentGeneration != DeploymentGeneration) throw new CatalogConflictException("CatalogRestoreGenerationChanged");
        await using var db = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx = await db.BeginTransactionAsync(token).ConfigureAwait(false);
        await LockAsync(db, tx, token).ConfigureAwait(false);
        await GenerationAsync(db, tx, token).ConfigureAwait(false);
        var existing = await LoadPlanAsync(db, tx, plan.PlanId, token).ConfigureAwait(false);
        if (existing is not null)
        {
            if (existing.ContentHash != plan.ContentHash || existing.RequesterPrincipalId != plan.RequesterPrincipalId || existing.ClusterId != plan.ClusterId)
                throw new CatalogConflictException("IdempotencyConflict");
            return existing;
        }
        await ValidateRowsAsync(db, tx, plan, token).ConfigureAwait(false);
        await RunAsync(db, tx, "INSERT INTO catalog_owned_plans(plan_id,content_hash,body) VALUES(@id,@hash,@body)", token,
            ("@id", plan.PlanId.ToString("D")), ("@hash", plan.ContentHash), ("@body", Serialize(plan))).ConfigureAwait(false);
        await tx.CommitAsync(token).ConfigureAwait(false);
        return plan;
    }
    public async Task ValidateAsync(CatalogPlan plan, CancellationToken token)
    {
        CatalogContract.ValidatePlan(plan);
        await ReadAsync(async (db, tx) => { await ValidateRowsAsync(db, tx, plan, token).ConfigureAwait(false); return true; }, token).ConfigureAwait(false);
    }
    public Task<CatalogCommitReceipt?> CommitAsync(CatalogPlan plan, Func<bool> currentAuthority, CancellationToken token) =>
        CommitWithPolicyAsync(plan, currentAuthority, null, token);

    public async Task<CatalogCommitReceipt?> CommitWithPolicyAsync(CatalogPlan plan, Func<bool> currentAuthority,
        Func<DbConnection, DbTransaction, CancellationToken, Task>? policyCheck, CancellationToken token)
    {
        CatalogContract.ValidatePlan(plan); ArgumentNullException.ThrowIfNull(currentAuthority);
        await using var db = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx = await db.BeginTransactionAsync(token).ConfigureAwait(false);
        await LockAsync(db, tx, token).ConfigureAwait(false);
        await GenerationAsync(db, tx, token).ConfigureAwait(false);
        if (plan.DeploymentGeneration != DeploymentGeneration || !currentAuthority()) return null;
        var stored = await LoadPlanAsync(db, tx, plan.PlanId, token).ConfigureAwait(false);
        if (stored is null || Serialize(stored) != Serialize(plan)) throw new CatalogConflictException("CatalogPlanMismatch");
        var receipt = await LoadReceiptAsync(db, tx, plan.PlanId, token).ConfigureAwait(false);
        if (receipt is not null) return receipt.ContentHash == plan.ContentHash ? receipt : throw new CatalogConflictException("CatalogReceiptConflict");
        var now = await DatabaseNowAsync(db, tx, token).ConfigureAwait(false);
        if (plan.ExpiresAtUtc <= now) throw new CatalogConflictException("CatalogPlanExpired");
        if (policyCheck is not null) await policyCheck(db, tx, token).ConfigureAwait(false);
        await ValidateRowsAsync(db, tx, plan, token).ConfigureAwait(false);
        var changed = new List<CatalogRecord>();
        foreach (var item in plan.Changes)
        {
            var old = await LoadAsync(db, tx, plan.ClusterId, item.Change.Kind, item.Id, token).ConfigureAwait(false);
            var c = item.Change;
            var record = new CatalogRecord(c.Kind, item.Id, plan.ClusterId, checked(c.ExpectedRevision + 1), c.Name, c.Description, c.Archived,
                c.TeamId, c.ApplicationId, c.TopicName, item.TopicIdentity, c.Owner, c.Domain, c.Classification, c.DocumentationReference, c.Tags ?? [],
                old?.CreatedBy ?? CatalogContract.Hash(plan.RequesterPrincipalId), old?.CreatedAtUtc ?? now, now);
            var parameters = new (string, object?)[] { ("@cluster", plan.ClusterId), ("@kind", (int)c.Kind), ("@id", item.Id), ("@revision", record.Revision),
                ("@expected", c.ExpectedRevision), ("@topic", c.TopicName), ("@body", Serialize(record)) };
            var count = await RunAsync(db, tx, old is null ?
                "INSERT INTO catalog_owned_entities(cluster_id,kind,id,revision,topic_name,body) VALUES(@cluster,@kind,@id,@revision,@topic,@body) ON CONFLICT DO NOTHING" :
                "UPDATE catalog_owned_entities SET revision=@revision,topic_name=@topic,body=@body WHERE cluster_id=@cluster AND kind=@kind AND id=@id AND revision=@expected", token, parameters).ConfigureAwait(false);
            if (count != 1) throw new CatalogConflictException("CatalogRevisionConflict");
            await RunAsync(db, tx, "UPDATE catalog_owned_history SET effective_to_utc=@now WHERE cluster_id=@cluster AND kind=@kind AND id=@id AND effective_to_utc IS NULL", token,
                ("@now", Stamp(now)), ("@cluster", plan.ClusterId), ("@kind", (int)c.Kind), ("@id", item.Id)).ConfigureAwait(false);
            await RunAsync(db, tx, "INSERT INTO catalog_owned_history(cluster_id,kind,id,revision,body,actor_fingerprint,authority_fingerprint,operation_id,effective_from_utc,effective_to_utc) VALUES(@cluster,@kind,@id,@revision,@body,@actor,@authority,@operation,@now,NULL)", token,
                ("@cluster", plan.ClusterId), ("@kind", (int)c.Kind), ("@id", item.Id), ("@revision", record.Revision), ("@body", Serialize(record)),
                ("@actor", CatalogContract.Hash(plan.RequesterPrincipalId)), ("@authority", plan.AuthorityFingerprint), ("@operation", plan.PlanId.ToString("D")), ("@now", Stamp(now))).ConfigureAwait(false);
            changed.Add(record);
        }
        foreach (var record in changed)
        {
            await RunAsync(db, tx, "DELETE FROM catalog_owned_references WHERE cluster_id=@cluster AND source_kind=@kind AND source_id=@id", token,
                ("@cluster", record.ClusterId), ("@kind", (int)record.Kind), ("@id", record.Id)).ConfigureAwait(false);
            foreach (var reference in References(record.TeamId, record.ApplicationId))
                await RunAsync(db, tx, "INSERT INTO catalog_owned_references(cluster_id,source_kind,source_id,target_kind,target_id) VALUES(@cluster,@kind,@id,@targetKind,@targetId)", token,
                    ("@cluster", record.ClusterId), ("@kind", (int)record.Kind), ("@id", record.Id), ("@targetKind", (int)reference.Kind), ("@targetId", reference.Id)).ConfigureAwait(false);
        }
        receipt = new(plan.PlanId, plan.ContentHash, now, changed);
        await RunAsync(db, tx, "INSERT INTO catalog_owned_receipts(operation_id,content_hash,body) VALUES(@id,@hash,@body)", token,
            ("@id", plan.PlanId.ToString("D")), ("@hash", plan.ContentHash), ("@body", Serialize(receipt))).ConfigureAwait(false);
        if (policyCheck is not null) await policyCheck(db, tx, token).ConfigureAwait(false);
        await GenerationAsync(db, tx, token).ConfigureAwait(false);
        if (!currentAuthority()) return null;
        await tx.CommitAsync(token).ConfigureAwait(false);
        return receipt;
    }

    public async Task VerifyReceiptOnTransactionAsync(DbConnection db, DbTransaction tx, CatalogPlan plan,
        CatalogCommitReceipt expected, Func<bool> currentAuthority, CancellationToken token)
    {
        await GenerationAsync(db, tx, token).ConfigureAwait(false);
        if (plan.DeploymentGeneration != DeploymentGeneration || !currentAuthority()) throw new CatalogConflictException("CatalogRecoveryDenied");
        var stored = await LoadPlanAsync(db, tx, plan.PlanId, token).ConfigureAwait(false);
        var receipt = await LoadReceiptAsync(db, tx, plan.PlanId, token).ConfigureAwait(false);
        var now = await DatabaseNowAsync(db, tx, token).ConfigureAwait(false);
        if (stored is null || Serialize(stored) != Serialize(plan) || receipt is null || Serialize(receipt) != Serialize(expected) ||
            receipt.OperationId != plan.PlanId || receipt.ContentHash != plan.ContentHash || receipt.Records.Count != plan.Changes.Count ||
            receipt.AppliedAtUtc < plan.CreatedAtUtc || receipt.AppliedAtUtc > plan.ExpiresAtUtc || receipt.AppliedAtUtc > now)
            throw new CatalogConflictException("CatalogPositiveReceiptRequired");
        foreach (var item in plan.Changes)
        {
            var record = receipt.Records.SingleOrDefault(r => r.Kind == item.Change.Kind && r.Id == item.Id);
            var change = item.Change;
            if (record is null || record.ClusterId != plan.ClusterId || record.Revision != change.ExpectedRevision + 1 ||
                record.Name != change.Name || record.Description != change.Description || record.Archived != change.Archived ||
                record.TeamId != change.TeamId || record.ApplicationId != change.ApplicationId || record.TopicName != change.TopicName ||
                record.TopicIdentity != item.TopicIdentity || record.Owner != change.Owner || record.Domain != change.Domain ||
                record.Classification != change.Classification || record.DocumentationReference != change.DocumentationReference ||
                !record.Tags.SequenceEqual(change.Tags ?? []) || record.UpdatedAtUtc != receipt.AppliedAtUtc)
                throw new CatalogConflictException("CatalogPositiveReceiptRequired");
        }
        if (!currentAuthority()) throw new CatalogConflictException("CatalogRecoveryDenied");
    }

    private async Task ValidateRowsAsync(DbConnection db, DbTransaction tx, CatalogPlan plan, CancellationToken token)
    {
        if (plan.DeploymentGeneration != DeploymentGeneration) throw new CatalogConflictException("CatalogRestoreGenerationChanged");
        var pending = plan.Changes.ToDictionary(i => (i.Change.Kind, i.Id));
        foreach (var group in plan.Changes.GroupBy(item => item.Change.Kind))
        {
            var count = Convert.ToInt64(await ScalarAsync(db, tx, "SELECT COUNT(*) FROM catalog_owned_entities WHERE cluster_id=@cluster AND kind=@kind", token,
                ("@cluster", plan.ClusterId), ("@kind", (int)group.Key)).ConfigureAwait(false), CultureInfo.InvariantCulture);
            if (count + group.Count(item => item.Change.ExpectedRevision == 0) > CatalogContract.MaxEntitiesPerScope)
                throw new CatalogConflictException("CatalogCapacityExceeded");
        }
        foreach (var item in plan.Changes)
        {
            var old = await LoadAsync(db, tx, plan.ClusterId, item.Change.Kind, item.Id, token).ConfigureAwait(false);
            if ((old?.Revision ?? 0) != item.Change.ExpectedRevision) throw new CatalogConflictException("CatalogRevisionConflict");
            var revisions = Convert.ToInt64(await ScalarAsync(db, tx, "SELECT COUNT(*) FROM catalog_owned_history WHERE cluster_id=@cluster AND kind=@kind AND id=@id", token,
                ("@cluster", plan.ClusterId), ("@kind", (int)item.Change.Kind), ("@id", item.Id)).ConfigureAwait(false), CultureInfo.InvariantCulture);
            if (revisions >= CatalogContract.MaxRevisionsPerEntity) throw new CatalogConflictException("CatalogHistoryCapacityExceeded");
            if (old is not null && (old.Kind != item.Change.Kind || old.TopicName != item.Change.TopicName ||
                old.TopicIdentity != item.TopicIdentity && !item.Change.RebindTopicIdentity))
                throw new CatalogConflictException("CatalogTopicIdentityChanged");
            if (item.Change.Kind == CatalogEntityKind.Topic && old is null)
            {
                var taken = await ScalarAsync(db, tx, "SELECT id FROM catalog_owned_entities WHERE cluster_id=@cluster AND topic_name=@topic", token,
                    ("@cluster", plan.ClusterId), ("@topic", item.Change.TopicName)).ConfigureAwait(false);
                if (taken is not null && taken is not DBNull) throw new CatalogConflictException("CatalogTopicAlreadyRegistered");
            }
            foreach (var reference in References(item.Change.TeamId, item.Change.ApplicationId))
            {
                var archived = pending.TryGetValue((reference.Kind, reference.Id), out var proposed) ? proposed.Change.Archived :
                    (await LoadAsync(db, tx, plan.ClusterId, reference.Kind, reference.Id, token).ConfigureAwait(false))?.Archived;
                if (archived is null || archived.Value) throw new CatalogConflictException("CatalogReferenceUnavailable");
            }
            if (item.Change.Archived)
            {
                await using var command = Command(db, tx, "SELECT source_kind,source_id FROM catalog_owned_references WHERE cluster_id=@cluster AND target_kind=@kind AND target_id=@id",
                    ("@cluster", plan.ClusterId), ("@kind", (int)item.Change.Kind), ("@id", item.Id));
                var referenced = new List<(CatalogEntityKind Kind, string Id)>();
                await using (var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false))
                    while (await reader.ReadAsync(token).ConfigureAwait(false)) referenced.Add(((CatalogEntityKind)reader.GetInt32(0), reader.GetString(1)));
                foreach (var reference in referenced)
                {
                    if (pending.TryGetValue(reference, out var proposed))
                    {
                        if (!proposed.Change.Archived && References(proposed.Change.TeamId, proposed.Change.ApplicationId).Contains((item.Change.Kind, item.Id)))
                            throw new CatalogConflictException("CatalogStillReferenced");
                    }
                    else if ((await LoadAsync(db, tx, plan.ClusterId, reference.Kind, reference.Id, token).ConfigureAwait(false))?.Archived == false)
                        throw new CatalogConflictException("CatalogStillReferenced");
                }
            }
        }
        if (plan.Changes.Where(i => i.Change.Kind == CatalogEntityKind.Topic).Select(i => i.Change.TopicName).Distinct(StringComparer.Ordinal).Count() != plan.Changes.Count(i => i.Change.Kind == CatalogEntityKind.Topic))
            throw new CatalogConflictException("CatalogDuplicateTopic");
    }

    /// <summary>Policy infrastructure calls this owning-store port on its existing transaction, never a second connection.</summary>
    public async Task<IReadOnlyDictionary<string, string>?> ReadAsync(DbConnection db, DbTransaction tx, string clusterId, string resourceId, CancellationToken token)
    {
        var json = await ScalarAsync(db, tx, "SELECT body FROM catalog_owned_entities WHERE cluster_id=@cluster AND kind=3 AND topic_name=@topic", token,
            ("@cluster", clusterId), ("@topic", resourceId)).ConfigureAwait(false);
        if (json is not string value) return null;
        var row = Deserialize<CatalogRecord>(value);
        return GovernancePolicyMetadata.FromCatalog(row);
    }
    private async Task<T> ReadAsync<T>(Func<DbConnection, DbTransaction, Task<T>> read, CancellationToken token)
    {
        await using var db = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var tx = await db.BeginTransactionAsync(token).ConfigureAwait(false);
        await GenerationAsync(db, tx, token).ConfigureAwait(false);
        var result = await read(db, tx).ConfigureAwait(false);
        await tx.CommitAsync(token).ConfigureAwait(false); return result;
    }
    private Task GenerationAsync(DbConnection db, DbTransaction tx, CancellationToken token) => generationGate?.ValidateAsync(db, tx, token) ?? Task.CompletedTask;
    private async Task LockAsync(DbConnection db, DbTransaction tx, CancellationToken token)
    { if (connections.SupportsSelectForUpdate) await RunAsync(db, tx, "SELECT pg_advisory_xact_lock(64964007301)", token).ConfigureAwait(false); }
    private static async Task<CatalogRecord?> LoadAsync(DbConnection db, DbTransaction tx, string clusterId, CatalogEntityKind kind, string id, CancellationToken token)
    { var value = await ScalarAsync(db, tx, "SELECT body FROM catalog_owned_entities WHERE cluster_id=@cluster AND kind=@kind AND id=@id", token, ("@cluster", clusterId), ("@kind", (int)kind), ("@id", id)).ConfigureAwait(false); return value is string json ? Deserialize<CatalogRecord>(json) : null; }
    private static async Task<CatalogPlan?> LoadPlanAsync(DbConnection db, DbTransaction tx, Guid id, CancellationToken token)
    { var value = await ScalarAsync(db, tx, "SELECT body FROM catalog_owned_plans WHERE plan_id=@id", token, ("@id", id.ToString("D"))).ConfigureAwait(false); return value is string json ? Deserialize<CatalogPlan>(json) : null; }
    private static async Task<CatalogCommitReceipt?> LoadReceiptAsync(DbConnection db, DbTransaction tx, Guid id, CancellationToken token)
    { var value = await ScalarAsync(db, tx, "SELECT body FROM catalog_owned_receipts WHERE operation_id=@id", token, ("@id", id.ToString("D"))).ConfigureAwait(false); return value is string json ? Deserialize<CatalogCommitReceipt>(json) : null; }
    private async Task<DateTimeOffset> DatabaseNowAsync(DbConnection db, DbTransaction tx, CancellationToken token)
    {
        var value = await ScalarAsync(db, tx, connections.SupportsSelectForUpdate ? "SELECT CURRENT_TIMESTAMP" : "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')", token).ConfigureAwait(false);
        return value is DateTime time ? new DateTimeOffset(DateTime.SpecifyKind(time, DateTimeKind.Utc)) : value is DateTimeOffset offset ? offset.ToUniversalTime() : DateTimeOffset.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture).ToUniversalTime();
    }
    private static IEnumerable<(CatalogEntityKind Kind, string Id)> References(string? teamId, string? applicationId)
    { if (teamId is not null) yield return (CatalogEntityKind.Team, teamId); if (applicationId is not null) yield return (CatalogEntityKind.Application, applicationId); }
    private static void ValidateScope(string clusterId, CatalogEntityKind kind)
    { CatalogContract.Bound(clusterId, "cluster", 256, false); if (!Enum.IsDefined(kind)) throw new ArgumentOutOfRangeException(nameof(kind)); }
    private static string Stamp(DateTimeOffset value) => value.ToString("O", CultureInfo.InvariantCulture);
    private static string Serialize<T>(T value) => JsonSerializer.Serialize(value, CatalogContract.Json);
    private static T Deserialize<T>(string value) => JsonSerializer.Deserialize<T>(value, CatalogContract.Json) ?? throw new InvalidOperationException("Catalog stored data invalid.");
    private static DbCommand Command(DbConnection db, DbTransaction tx, string sql, params (string Name, object? Value)[] values)
    { var command = db.CreateCommand(); command.Transaction = tx; command.CommandText = sql; foreach (var (name, value) in values) { var parameter = command.CreateParameter(); parameter.ParameterName = name; parameter.Value = value ?? DBNull.Value; command.Parameters.Add(parameter); } return command; }
    private static async Task<object?> ScalarAsync(DbConnection db, DbTransaction tx, string sql, CancellationToken token, params (string Name, object? Value)[] values)
    { await using var command = Command(db, tx, sql, values); return await command.ExecuteScalarAsync(token).ConfigureAwait(false); }
    private static async Task<int> RunAsync(DbConnection db, DbTransaction tx, string sql, CancellationToken token, params (string Name, object? Value)[] values)
    { await using var command = Command(db, tx, sql, values); return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false); }
}
