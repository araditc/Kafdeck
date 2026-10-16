using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>Bounded operator plans, immutable effect dispositions and atomic generation/cutoff advancement.</summary>
public sealed class AdoNotificationRecoveryStore(INotificationDeliveryDbConnectionFactory connections,
    INotificationRecoveryAuthority? authority = null, INotificationRecoverySourceInventory? sourceInventory = null)
    : INotificationRecoveryStore, INotificationRecoveryEventGate, INotificationRecoverySourceEvidence
{
    private static readonly JsonSerializerOptions Json = new();
    private const string Unreconciled = """
        FROM kafdeck_notification_deliveries d WHERE d.state IN ('Pending','InFlight','Failed','UnknownExternalEffect')
        AND NOT EXISTS (SELECT 1 FROM notification_recovery_receipts r WHERE r.notification_id=d.notification_id
            AND r.destination_id=d.destination_id AND r.fenced_revision=d.revision AND r.target_generation=@target)
        """;

    public async Task InitializeAsync(CancellationToken token)
    {
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await c.BeginTransactionAsync(IsolationLevel.Serializable, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        await ExecuteAsync(c, t, """
            CREATE TABLE IF NOT EXISTS notification_recovery_plans (
              operation_id TEXT PRIMARY KEY, revision BIGINT NOT NULL, plan_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS notification_recovery_audit (
              operation_id TEXT NOT NULL, revision BIGINT NOT NULL, plan_json TEXT NOT NULL,
              PRIMARY KEY(operation_id,revision));
            CREATE TABLE IF NOT EXISTS notification_recovery_receipts (
              notification_id TEXT NOT NULL, destination_id TEXT NOT NULL, fenced_revision BIGINT NOT NULL,
              target_generation TEXT NOT NULL, operation_id TEXT NOT NULL, disposition TEXT NOT NULL,
              proof_sha256 TEXT NOT NULL, requester TEXT NOT NULL, approver TEXT NOT NULL,
              action TEXT NOT NULL CHECK(action='SuppressRestoredIntent'),
              PRIMARY KEY(notification_id,destination_id,fenced_revision,target_generation));
            CREATE TABLE IF NOT EXISTS notification_recovery_epoch (
              id INTEGER PRIMARY KEY CHECK(id=1), generation TEXT NOT NULL,
              cutoff_utc TEXT NOT NULL, operation_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS notification_recovery_source_suppressions (
              generation TEXT NOT NULL, notification_id TEXT NOT NULL, destination_id TEXT NOT NULL,
              operation_id TEXT NOT NULL, disposition TEXT NOT NULL CHECK(disposition='SuppressedHistoricalSource'),
              PRIMARY KEY(generation,notification_id,destination_id));
            CREATE TABLE IF NOT EXISTS notification_recovery_source_captures (
              notification_id TEXT PRIMARY KEY, captured_at_utc TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS notification_recovery_historical_sources (
              target_generation TEXT NOT NULL,notification_id TEXT NOT NULL,source_identity TEXT NOT NULL,
              operation_id TEXT NOT NULL,disposition TEXT NOT NULL,proof_sha256 TEXT NOT NULL,
              PRIMARY KEY(target_generation,notification_id));
            """, token).ConfigureAwait(false);
        // Deliberately does not create notification_delivery_generation or enroll a database.
        await t.CommitAsync(token).ConfigureAwait(false);
    }

    public async Task<NotificationRecoveryStatus> StatusAsync(CancellationToken token)
    {
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        var generation = await GenerationAsync(c, null, token).ConfigureAwait(false);
        var epoch = await EpochAsync(c, null, token).ConfigureAwait(false);
        var count = await ScalarAsync(c, null,
            "SELECT COUNT(*) FROM kafdeck_notification_deliveries WHERE state IN ('Pending','InFlight','Failed','UnknownExternalEffect')", token).ConfigureAwait(false);
        return new(generation, epoch?.Cutoff, epoch?.Operation, Convert.ToInt64(count, CultureInfo.InvariantCulture));
    }

    public async Task<NotificationRecoveryPlan?> GetAsync(Guid operationId, CancellationToken token)
    {
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        return await ReadPlanAsync(c, null, operationId, token).ConfigureAwait(false);
    }

    public async Task<NotificationRecoveryPlan> PreviewAsync(NotificationRecoveryRequest request, string requester,
        string humanFingerprint, string policyFingerprint, CancellationToken token)
    {
        request.Validate(); ValidateActor(requester, humanFingerprint, policyFingerprint);
        await VerifyAuthorityAsync(request, token).ConfigureAwait(false);
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await c.BeginTransactionAsync(IsolationLevel.Serializable, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        if (Convert.ToInt64(await ScalarAsync(c, t, "SELECT COUNT(*) FROM notification_recovery_plans", token).ConfigureAwait(false), CultureInfo.InvariantCulture) >= 10000)
            throw new InvalidOperationException("Notification recovery plan retention budget is exhausted; archive through a reviewed maintenance procedure.");
        var now = await NowAsync(c, t, token).ConfigureAwait(false);
        if (await GenerationAsync(c, t, token).ConfigureAwait(false) != request.ExpectedDatabaseGeneration)
            throw new InvalidOperationException("Notification recovery generation changed.");
        var source = await ReadSourceAsync(token).ConfigureAwait(false);
        var items = request.Command == NotificationRecoveryCommand.FenceBatch
            ? await ItemsAsync(c, t, request.TargetGeneration, request.MaxItems, token).ConfigureAwait(false) : [];
        if (request.Command == NotificationRecoveryCommand.FenceBatch && items.Count == 0)
            throw new InvalidOperationException("There are no unreconciled delivery intents in this batch.");
        var sourceItems = request.Command == NotificationRecoveryCommand.FenceSourceBatch ?
            await UnfencedSourceAsync(c, t, request.TargetGeneration, source, request.MaxItems, token).ConfigureAwait(false) : [];
        if (request.Command == NotificationRecoveryCommand.FenceSourceBatch && sourceItems.Count == 0)
            throw new InvalidOperationException("There are no unreconciled historical source events in this batch.");
        await VerifyAdvancementAsync(c, t, request, source, token).ConfigureAwait(false);
        var id = Guid.NewGuid();
        var hash = Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(
            new { id, request, requester, humanFingerprint, policyFingerprint, cutoff = now, expires = now.AddMinutes(10), items,
                source.SourceIdentityFingerprint, source.InventoryHash, sourceItems }, Json))).ToLowerInvariant();
        var plan = new NotificationRecoveryPlan(id, 1, hash, NotificationRecoveryState.Previewed, request, now.AddMinutes(10),
            requester, humanFingerprint, null, null, policyFingerprint, now, items,
            await CountAsync(c, t, request.TargetGeneration, token).ConfigureAwait(false), false,
            source.SourceIdentityFingerprint, source.InventoryHash, sourceItems);
        await SaveAsync(c, t, plan, null, token).ConfigureAwait(false);
        await t.CommitAsync(token).ConfigureAwait(false);
        return plan;
    }

    public async Task<NotificationRecoveryPlan?> ApproveAsync(Guid operationId, long revision, string previewHash,
        string approver, string humanFingerprint, string policyFingerprint, CancellationToken token)
    {
        ValidateActor(approver, humanFingerprint, policyFingerprint);
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await c.BeginTransactionAsync(IsolationLevel.Serializable, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        var plan = await ReadPlanAsync(c, t, operationId, token).ConfigureAwait(false);
        if (plan is null) return null;
        // Current server policy and authenticated human separation establish
        // authority independently of the caller's optimistic concurrency token.
        if (plan.PolicyFingerprint != policyFingerprint || plan.RequesterHumanFingerprint == humanFingerprint) return null;
        if (plan.State != NotificationRecoveryState.Previewed ||
            plan.ExpiresAtUtc <= await NowAsync(c, t, token).ConfigureAwait(false)) return null;
        await VerifyAuthorityAsync(plan.Request, token).ConfigureAwait(false);
        if (!MatchesExpectedRevision(plan, revision, previewHash)) return null;
        var approved = plan with { Revision = checked(plan.Revision + 1), State = NotificationRecoveryState.Approved,
            ApprovedByPrincipalId = approver, ApproverHumanFingerprint = humanFingerprint };
        await SaveAsync(c, t, approved, plan.Revision, token).ConfigureAwait(false);
        await t.CommitAsync(token).ConfigureAwait(false);
        return approved;
    }

    public async Task<NotificationRecoveryPlan?> ApplyAsync(Guid operationId, long revision, string previewHash,
        string requesterHumanFingerprint, string policyFingerprint, Func<bool> currentAuthority, CancellationToken token)
    {
        if (!NotificationRecoveryRequest.Fingerprint(requesterHumanFingerprint) || !NotificationRecoveryRequest.Fingerprint(policyFingerprint))
            throw new ArgumentException("Invalid notification recovery actor.");
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await c.BeginTransactionAsync(IsolationLevel.Serializable, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        if (connections.SupportsSelectForUpdate)
            await ExecuteAsync(c, t, "LOCK TABLE kafdeck_notification_deliveries IN SHARE ROW EXCLUSIVE MODE", token).ConfigureAwait(false);
        var plan = await ReadPlanAsync(c, t, operationId, token).ConfigureAwait(false);
        if (plan is null) return null;
        // The server-owned policy and live authorization guard every apply;
        // the request revision/hash below can only reject a stale operation.
        if (plan.PolicyFingerprint != policyFingerprint || plan.RequesterHumanFingerprint != requesterHumanFingerprint) return null;
        if (plan.State != NotificationRecoveryState.Approved ||
            plan.ExpiresAtUtc <= await NowAsync(c, t, token).ConfigureAwait(false)) return null;
        var request = plan.Request;
        await VerifyAuthorityAsync(request, token).ConfigureAwait(false);
        if (!currentAuthority()) return null;
        if (!MatchesExpectedRevision(plan, revision, previewHash)) return null;
        if (await GenerationAsync(c, t, token).ConfigureAwait(false) != request.ExpectedDatabaseGeneration) return null;
        var source = await ReadSourceAsync(token).ConfigureAwait(false);
        if (source.SourceIdentityFingerprint != plan.SourceIdentityFingerprint || source.InventoryHash != plan.SourceInventoryHash) return null;
        var now = await NowAsync(c, t, token).ConfigureAwait(false);
        if (request.Command == NotificationRecoveryCommand.FenceBatch)
        {
            foreach (var item in plan.Items)
            {
                var changed = await ExecuteAsync(c, t, """
                    UPDATE kafdeck_notification_deliveries SET state='UnknownExternalEffect',next_attempt_at_utc=NULL,
                    due_at_utc=@now,outcome_code='unknown-external-effect',provider_receipt_fingerprint=NULL,
                    revision=revision+1,updated_at_utc=CASE
                      WHEN updated_at_utc>@now THEN updated_at_utc
                      WHEN created_at_utc>@now THEN created_at_utc ELSE @now END
                    WHERE notification_id=@id AND destination_id=@destination AND revision=@revision
                    AND state IN ('Pending','InFlight','Failed','UnknownExternalEffect')
                    """, token, ("@now", Stamp(now)), ("@id", item.NotificationId.ToString("D")),
                    ("@destination", item.DestinationId), ("@revision", item.Revision)).ConfigureAwait(false);
                if (changed != 1) return null;
                await ExecuteAsync(c, t, """
                    INSERT INTO notification_recovery_receipts(notification_id,destination_id,fenced_revision,target_generation,
                        operation_id,disposition,proof_sha256,requester,approver,action)
                    VALUES(@id,@destination,@revision,@generation,@operation,@disposition,@proof,@requester,@approver,'SuppressRestoredIntent')
                    """, token, ("@id", item.NotificationId.ToString("D")), ("@destination", item.DestinationId),
                    ("@revision", checked(item.Revision + 1)), ("@generation", request.TargetGeneration.ToString("D")),
                    ("@operation", operationId.ToString("D")), ("@disposition", request.Disposition!.Value.ToString()),
                    ("@proof", request.ProofReferenceSha256), ("@requester", plan.RequesterPrincipalId),
                    ("@approver", plan.ApprovedByPrincipalId!)).ConfigureAwait(false);
            }
        }
        else if (request.Command == NotificationRecoveryCommand.FenceSourceBatch)
        {
            foreach (var id in plan.SourceEventIds)
            {
                var changed = await ExecuteAsync(c, t, """
                    INSERT INTO notification_recovery_historical_sources(target_generation,notification_id,source_identity,operation_id,disposition,proof_sha256)
                    VALUES(@generation,@id,@identity,@operation,@disposition,@proof)
                    ON CONFLICT(target_generation,notification_id) DO NOTHING
                    """, token, ("@generation", request.TargetGeneration.ToString("D")), ("@id", id.ToString("D")),
                    ("@identity", source.SourceIdentityFingerprint), ("@operation", operationId.ToString("D")),
                    ("@disposition", request.Disposition!.Value.ToString()), ("@proof", request.ProofReferenceSha256)).ConfigureAwait(false);
                if (changed != 1) return null;
            }
        }
        else
        {
            await VerifyAdvancementAsync(c, t, request, source, token).ConfigureAwait(false);
            if (request.Command == NotificationRecoveryCommand.EnrollEmpty)
                await ExecuteAsync(c, t, """
                    CREATE TABLE IF NOT EXISTS notification_delivery_generation(id INTEGER PRIMARY KEY CHECK(id=1),generation TEXT NOT NULL);
                    INSERT INTO notification_delivery_generation(id,generation) VALUES(1,@generation)
                    """, token, ("@generation", request.TargetGeneration.ToString("D"))).ConfigureAwait(false);
            else if (await ExecuteAsync(c, t, "UPDATE notification_delivery_generation SET generation=@generation WHERE id=1 AND generation=@old",
                token, ("@generation", request.TargetGeneration.ToString("D")), ("@old", request.ExpectedDatabaseGeneration!.Value.ToString("D"))).ConfigureAwait(false) != 1) return null;
            await ExecuteAsync(c, t, """
                INSERT INTO notification_recovery_epoch(id,generation,cutoff_utc,operation_id) VALUES(1,@generation,@cutoff,@operation)
                ON CONFLICT(id) DO UPDATE SET generation=excluded.generation,cutoff_utc=excluded.cutoff_utc,operation_id=excluded.operation_id
                """, token, ("@generation", request.TargetGeneration.ToString("D")), ("@cutoff", Stamp(plan.HistoricalCutoffUtc)),
                ("@operation", operationId.ToString("D"))).ConfigureAwait(false);
        }
        // Re-read the outside authority immediately before committing; application never activates it.
        await VerifyAuthorityAsync(request, token).ConfigureAwait(false);
        if (!currentAuthority()) return null;
        var checkedSource = await ReadSourceAsync(token).ConfigureAwait(false);
        if (checkedSource.SourceIdentityFingerprint != plan.SourceIdentityFingerprint || checkedSource.InventoryHash != plan.SourceInventoryHash) return null;
        var applied = plan with { Revision = checked(plan.Revision + 1), State = NotificationRecoveryState.Applied,
            RemainingUnreconciled = await CountAsync(c, t, request.TargetGeneration, token).ConfigureAwait(false),
            ReadyForOperatorActivation = request.Command is NotificationRecoveryCommand.AdvanceGeneration or NotificationRecoveryCommand.EnrollEmpty };
        await SaveAsync(c, t, applied, plan.Revision, token).ConfigureAwait(false);
        await t.CommitAsync(token).ConfigureAwait(false);
        return applied;
    }

    public async ValueTask<bool> CheckAsync(Guid eventId, string destinationId, DateTimeOffset occurredAtUtc, CancellationToken token)
    {
        if (eventId == Guid.Empty) throw new ArgumentException("Invalid historical notification identity.");
        NotificationDeliveryIdentity.NormalizeDestinationId(destinationId);
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        // A legacy, never-enrolled deployment has no recovery cutoff. Opting into recovery creates this metadata explicitly.
        if (!await ExistsAsync(c, null, "notification_recovery_epoch", token).ConfigureAwait(false)) return true;
        await using var t = await c.BeginTransactionAsync(IsolationLevel.Serializable, token).ConfigureAwait(false);
        var epoch = await EpochAsync(c, t, token).ConfigureAwait(false);
        if (epoch is null) return true;
        if (await GenerationAsync(c, t, token).ConfigureAwait(false) != epoch.Generation)
            throw new InvalidOperationException("Notification recovery generation metadata is inconsistent.");
        var captured = await ScalarAsync(c, t, "SELECT captured_at_utc FROM notification_recovery_source_captures WHERE notification_id=@id",
            token, ("@id", eventId.ToString("D"))).ConfigureAwait(false);
        var comparable = captured is string captureStamp ? DateTimeOffset.ParseExact(captureStamp, "O", CultureInfo.InvariantCulture) : occurredAtUtc.ToUniversalTime();
        var historical = await ScalarAsync(c, t, "SELECT operation_id FROM notification_recovery_historical_sources WHERE target_generation=@generation AND notification_id=@id",
            token, ("@generation", epoch.Generation.ToString("D")), ("@id", eventId.ToString("D"))).ConfigureAwait(false);
        if (historical is not string && comparable > epoch.Cutoff) return true;
        var count = Convert.ToInt64(await ScalarAsync(c, t, "SELECT COUNT(*) FROM notification_recovery_source_suppressions", token).ConfigureAwait(false), CultureInfo.InvariantCulture);
        if (count >= 1000000) throw new InvalidOperationException("Notification recovery source suppression budget is exhausted.");
        await ExecuteAsync(c, t, """
            INSERT INTO notification_recovery_source_suppressions(generation,notification_id,destination_id,operation_id,disposition)
            VALUES(@generation,@id,@destination,@operation,'SuppressedHistoricalSource')
            ON CONFLICT(generation,notification_id,destination_id) DO NOTHING
            """, token, ("@generation", epoch.Generation.ToString("D")), ("@id", eventId.ToString("D")),
            ("@destination", destinationId), ("@operation", epoch.Operation.ToString("D"))).ConfigureAwait(false);
        await t.CommitAsync(token).ConfigureAwait(false);
        return false;
    }

    public async ValueTask RecordCaptureAsync(Guid eventId, DateTimeOffset databaseCapturedAtUtc, CancellationToken token)
    {
        if (eventId == Guid.Empty || databaseCapturedAtUtc == default) throw new ArgumentException("Invalid notification source capture evidence.");
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        // Legacy deployments have no recovery metadata to maintain. Recovery
        // maintenance creates these tables before any reviewed advancement.
        if (!await ExistsAsync(c, null, "notification_recovery_epoch", token).ConfigureAwait(false)) return;
        await using var t = await c.BeginTransactionAsync(IsolationLevel.Serializable, token).ConfigureAwait(false);
        var stamp = Stamp(databaseCapturedAtUtc);
        var existing = await ScalarAsync(c, t, "SELECT captured_at_utc FROM notification_recovery_source_captures WHERE notification_id=@id",
            token, ("@id", eventId.ToString("D"))).ConfigureAwait(false);
        if (existing is string known)
        {
            if (known != stamp) throw new InvalidOperationException("Notification source capture identity changed.");
            return;
        }
        if (Convert.ToInt64(await ScalarAsync(c, t, "SELECT COUNT(*) FROM notification_recovery_source_captures", token).ConfigureAwait(false), CultureInfo.InvariantCulture) >= 1000000)
            throw new InvalidOperationException("Notification recovery source evidence budget is exhausted.");
        await ExecuteAsync(c, t, "INSERT INTO notification_recovery_source_captures(notification_id,captured_at_utc) VALUES(@id,@stamp)", token,
            ("@id", eventId.ToString("D")), ("@stamp", stamp)).ConfigureAwait(false);
        await t.CommitAsync(token).ConfigureAwait(false);
    }

    private async Task VerifyAdvancementAsync(DbConnection c, DbTransaction t, NotificationRecoveryRequest request, NotificationRecoverySourceInventory source, CancellationToken token)
    {
        if (request.Command is NotificationRecoveryCommand.FenceBatch or NotificationRecoveryCommand.FenceSourceBatch) return;
        var count = request.Command == NotificationRecoveryCommand.EnrollEmpty
            ? Convert.ToInt64(await ScalarAsync(c, t, "SELECT COUNT(*) FROM kafdeck_notification_deliveries", token).ConfigureAwait(false), CultureInfo.InvariantCulture)
            : await CountAsync(c, t, request.TargetGeneration, token).ConfigureAwait(false);
        if (count != 0) throw new InvalidOperationException("Notification recovery cannot advance with unreconciled delivery intents.");
        if (request.Command == NotificationRecoveryCommand.EnrollEmpty ? source.EventIds.Count != 0 :
            (await UnfencedSourceAsync(c, t, request.TargetGeneration, source, 1, token).ConfigureAwait(false)).Count != 0)
            throw new InvalidOperationException("Notification recovery cannot advance with unreconciled historical source inventory.");
    }

    private async Task<NotificationRecoverySourceInventory> ReadSourceAsync(CancellationToken token)
    {
        if (sourceInventory is null) throw new InvalidOperationException("Notification recovery requires an explicit read-only product source inventory.");
        var inventory = await sourceInventory.ReadAsync(token).ConfigureAwait(false);
        if (!NotificationRecoveryRequest.Fingerprint(inventory.SourceIdentityFingerprint) || !NotificationRecoveryRequest.Fingerprint(inventory.InventoryHash) ||
            inventory.EventIds.Count > 10000 || inventory.EventIds.Any(id => id == Guid.Empty) || inventory.EventIds.Distinct().Count() != inventory.EventIds.Count)
            throw new InvalidOperationException("Invalid bounded notification source inventory.");
        return inventory;
    }
    private static async Task<List<Guid>> UnfencedSourceAsync(DbConnection c, DbTransaction t, Guid generation,
        NotificationRecoverySourceInventory source, int take, CancellationToken token)
    {
        var known = new HashSet<Guid>();
        await using (var command = Command(c, t, "SELECT notification_id FROM notification_recovery_historical_sources WHERE target_generation=@generation AND source_identity=@identity",
            ("@generation", generation.ToString("D")), ("@identity", source.SourceIdentityFingerprint)))
        await using (var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false))
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                if (known.Count >= 10000) throw new InvalidOperationException("Historical source inventory retention budget is exhausted.");
                known.Add(Guid.ParseExact(reader.GetString(0), "D"));
            }
        return source.EventIds.Where(id => !known.Contains(id)).Take(take).ToList();
    }

    private ValueTask VerifyAuthorityAsync(NotificationRecoveryRequest request, CancellationToken token) =>
        authority is null ? throw new InvalidOperationException("Notification recovery maintenance authority is unavailable.") :
        authority.VerifyQuarantinedAsync(request.TargetGeneration, request.ExpectedAnchorSha256, token);

    private async Task<long> CountAsync(DbConnection c, DbTransaction t, Guid generation, CancellationToken token) =>
        Convert.ToInt64(await ScalarAsync(c, t, "SELECT COUNT(*) " + Unreconciled, token,
            ("@target", generation.ToString("D"))).ConfigureAwait(false), CultureInfo.InvariantCulture);
    private static async Task<List<NotificationRecoveryItem>> ItemsAsync(DbConnection c, DbTransaction t, Guid generation, int take, CancellationToken token)
    {
        await using var command = Command(c, t, "SELECT d.notification_id,d.destination_id,d.revision " + Unreconciled +
            " ORDER BY d.created_at_utc,d.notification_id,d.destination_id LIMIT @take", ("@target", generation.ToString("D")), ("@take", take));
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        List<NotificationRecoveryItem> items = [];
        while (await reader.ReadAsync(token).ConfigureAwait(false)) items.Add(new(Guid.Parse(reader.GetString(0)), reader.GetString(1), reader.GetInt64(2)));
        return items;
    }
    private async Task<DateTimeOffset> NowAsync(DbConnection c, DbTransaction t, CancellationToken token)
    {
        var value = await ScalarAsync(c, t, connections.DatabaseUtcNowSql, token).ConfigureAwait(false);
        return value is DateTime dt ? new(DateTime.SpecifyKind(dt, DateTimeKind.Utc)) : value is DateTimeOffset dto ? dto.ToUniversalTime() :
            DateTimeOffset.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture).ToUniversalTime();
    }
    private async Task LockAsync(DbConnection c, DbTransaction t, CancellationToken token)
    {
        if (connections.SupportsSelectForUpdate)
            await ExecuteAsync(c, t, "SELECT pg_advisory_xact_lock(64964006603)", token).ConfigureAwait(false);
    }
    private async Task<Guid?> GenerationAsync(DbConnection c, DbTransaction? t, CancellationToken token)
    {
        if (!await ExistsAsync(c, t, "notification_delivery_generation", token).ConfigureAwait(false)) return null;
        var value = await ScalarAsync(c, t, "SELECT generation FROM notification_delivery_generation WHERE id=1", token).ConfigureAwait(false);
        if (value is null or DBNull) return null;
        return Guid.TryParseExact(Convert.ToString(value, CultureInfo.InvariantCulture), "D", out var generation) && generation != Guid.Empty
            ? generation : throw new InvalidOperationException("Invalid notification delivery generation metadata.");
    }
    private async Task<bool> ExistsAsync(DbConnection c, DbTransaction? t, string table, CancellationToken token) =>
        await ScalarAsync(c, t, connections.SupportsSelectForUpdate ? "SELECT to_regclass(@table)::text" :
            "SELECT name FROM sqlite_master WHERE type='table' AND name=@table", token, ("@table", table)).ConfigureAwait(false) is not (null or DBNull);
    private sealed record Epoch(Guid Generation, DateTimeOffset Cutoff, Guid Operation);
    private static async Task<Epoch?> EpochAsync(DbConnection c, DbTransaction? t, CancellationToken token)
    {
        await using var cmd = Command(c, t, "SELECT generation,cutoff_utc,operation_id FROM notification_recovery_epoch WHERE id=1");
        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        return await reader.ReadAsync(token).ConfigureAwait(false) ? new(Guid.ParseExact(reader.GetString(0), "D"),
            DateTimeOffset.ParseExact(reader.GetString(1), "O", CultureInfo.InvariantCulture), Guid.ParseExact(reader.GetString(2), "D")) : null;
    }
    private static async Task<NotificationRecoveryPlan?> ReadPlanAsync(DbConnection c, DbTransaction? t, Guid id, CancellationToken token)
    {
        var value = await ScalarAsync(c, t, "SELECT plan_json FROM notification_recovery_plans WHERE operation_id=@id", token, ("@id", id.ToString("D"))).ConfigureAwait(false);
        return value is string json ? JsonSerializer.Deserialize<NotificationRecoveryPlan>(json, Json)! : null;
    }
    private static async Task SaveAsync(DbConnection c, DbTransaction t, NotificationRecoveryPlan plan, long? expected, CancellationToken token)
    {
        var json = JsonSerializer.Serialize(plan, Json);
        if (json.Length > 131072) throw new InvalidOperationException("Notification recovery preview exceeds its finite budget.");
        var changed = await ExecuteAsync(c, t, expected is null ?
            "INSERT INTO notification_recovery_plans(operation_id,revision,plan_json) VALUES(@id,@revision,@json)" :
            "UPDATE notification_recovery_plans SET revision=@revision,plan_json=@json WHERE operation_id=@id AND revision=@expected",
            token, ("@id", plan.OperationId.ToString("D")), ("@revision", plan.Revision), ("@json", json), ("@expected", expected)).ConfigureAwait(false);
        if (changed != 1) throw new InvalidOperationException("Notification recovery revision conflict.");
        await ExecuteAsync(c, t, "INSERT INTO notification_recovery_audit(operation_id,revision,plan_json) VALUES(@id,@revision,@json)", token,
            ("@id", plan.OperationId.ToString("D")), ("@revision", plan.Revision), ("@json", json)).ConfigureAwait(false);
    }
    private static bool MatchesExpectedRevision(NotificationRecoveryPlan plan, long revision, string hash) =>
        revision > 0 && plan.Revision == revision && NotificationRecoveryRequest.Fingerprint(hash) && plan.PreviewHash == hash;
    private static void ValidateActor(string actor, string human, string policy)
    {
        if (string.IsNullOrWhiteSpace(actor) || actor.Length > 2048 || actor.Any(char.IsControl) ||
            !NotificationRecoveryRequest.Fingerprint(human) || !NotificationRecoveryRequest.Fingerprint(policy))
            throw new ArgumentException("Invalid notification recovery actor.");
    }
    private static string Stamp(DateTimeOffset value) => value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
    private static DbCommand Command(DbConnection c, DbTransaction? t, string sql, params (string Name, object? Value)[] parameters)
    {
        var cmd = c.CreateCommand(); cmd.Transaction = t; cmd.CommandText = sql;
        foreach (var (name, value) in parameters) { var p = cmd.CreateParameter(); p.ParameterName = name; p.Value = value ?? DBNull.Value; cmd.Parameters.Add(p); }
        return cmd;
    }
    private static async Task<object?> ScalarAsync(DbConnection c, DbTransaction? t, string sql, CancellationToken token, params (string Name, object? Value)[] parameters)
    { await using var cmd = Command(c, t, sql, parameters); return await cmd.ExecuteScalarAsync(token).ConfigureAwait(false); }
    private static async Task<int> ExecuteAsync(DbConnection c, DbTransaction? t, string sql, CancellationToken token, params (string Name, object? Value)[] parameters)
    { await using var cmd = Command(c, t, sql, parameters); return await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false); }
}
