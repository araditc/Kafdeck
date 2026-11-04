using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;

namespace Kafdeck.Infrastructure.Persistence;

public static class W68RuntimeSchema
{
    public static Task InstallOrUpgradeAsync(INotificationDeliveryDbConnectionFactory connections, CancellationToken token) =>
        InstallOrUpgradeAsync(connections, false, null, token);
    private const string Manifest = "w68-g3-runtime-v2";
    private static readonly string[] BaselineTables = ["kafdeck_declarative_schema_migrations", "kafdeck_declarative_key_versions",
        "kafdeck_declarative_ownership", "kafdeck_declarative_controller_leases", "kafdeck_declarative_lease_epoch_high_water", "kafdeck_declarative_ownership_audit"];
    private static readonly string[] RuntimeTables = ["kafdeck_declarative_registrations", "kafdeck_declarative_plans",
        "kafdeck_declarative_commit_receipts", "kafdeck_declarative_generation_history", "kafdeck_declarative_lease_bindings",
        "kafdeck_declarative_runtime_audit", "kafdeck_declarative_runtime_schema", "kafdeck_declarative_deployments"];
    private static string BaselineSql
    {
        get
        {
            var assembly = typeof(W68RuntimeSchema).Assembly;
            using var stream = assembly.GetManifestResourceStream(assembly.GetManifestResourceNames().Single(x => x.EndsWith("w68-g2-baseline-v1.sql", StringComparison.Ordinal)))!;
            using var reader = new StreamReader(stream); return reader.ReadToEnd();
        }
    }

    // Explicit offline operator action, never invoked by runtime reads/status.
    // Existing unledgered installations require deliberate reviewed adoption.
    public static async Task InstallOrUpgradeAsync(INotificationDeliveryDbConnectionFactory connections,
        bool adoptExisting = false, string? adoptionEvidenceSha256 = null, CancellationToken token = default)
    {
        if (adoptExisting && !Hash(adoptionEvidenceSha256)) throw new ArgumentException("Existing schema adoption requires a reviewed evidence SHA256.");
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = c is SqliteConnection sqlite ? sqlite.BeginTransaction(IsolationLevel.Serializable, deferred: false) :
            await c.BeginTransactionAsync(IsolationLevel.ReadCommitted, token).ConfigureAwait(false);
        var pg = c is not SqliteConnection;
        if (pg) await Exec(c, t, "SELECT pg_advisory_xact_lock(68080068);SELECT pg_advisory_xact_lock(68080041)", token).ConfigureAwait(false);
        var existing = 0;
        foreach (var table in BaselineTables.Skip(1)) if (await Exists(c, t, table, token).ConfigureAwait(false)) existing++;
        var ledger = await Exists(c, t, BaselineTables[0], token).ConfigureAwait(false);
        if (adoptExisting && existing == 0 && !ledger)
            throw new InvalidOperationException("Explicit existing-installation adoption cannot target an empty schema.");
        if (existing is not (0 or 5) || ledger && existing != 5)
            throw new InvalidOperationException("Partial declarative baseline cannot be upgraded.");
        var adopting = existing == 5 && !ledger;
        if (adopting && !adoptExisting) throw new InvalidOperationException("Existing five-table schema requires explicit reviewed adoption.");
        if (adopting)
        {
            await Exec(c, t, LedgerDdl(pg), token).ConfigureAwait(false);
            // Provisional row is invisible outside this transaction. Full exact
            // structural comparison below must succeed before any commit.
            await Exec(c, t, "INSERT INTO kafdeck_declarative_schema_migrations(version,manifest_id) VALUES(1,'w68-g2-postgresql-baseline-v1')", token).ConfigureAwait(false);
        }
        await Baseline(c, t, pg, token).ConfigureAwait(false);
        var runtimeCount = 0;
        foreach (var table in RuntimeTables) if (await Exists(c, t, table, token).ConfigureAwait(false)) runtimeCount++;
        var version = await Scalar(c, t, "SELECT manifest_id FROM kafdeck_declarative_schema_migrations WHERE version=2", token).ConfigureAwait(false);
        if (runtimeCount != 0 && (runtimeCount != RuntimeTables.Length || version is not string id || id != Manifest) ||
            runtimeCount == 0 && version is not null and not DBNull)
            throw new InvalidOperationException("Partial or unmanaged declarative runtime schema.");
        var expectedBaseline = await ExpectedSignature(c, t, pg, false, token).ConfigureAwait(false);
        if (await Signature(c, t, pg, BaselineTables, token).ConfigureAwait(false) != expectedBaseline)
            throw new InvalidOperationException("Existing declarative baseline structure differs from the verified version.");
        foreach (var statement in RuntimeDdl(pg)) await Exec(c, t, statement, token).ConfigureAwait(false);
        var actual = await Signature(c, t, pg, BaselineTables.Concat(RuntimeTables).ToArray(), token).ConfigureAwait(false);
        if (actual != await ExpectedSignature(c, t, pg, true, token).ConfigureAwait(false))
            throw new InvalidOperationException("Declarative runtime schema structure is invalid.");
        if (Convert.ToInt64(await Scalar(c, t, """
            SELECT COUNT(*) FROM kafdeck_declarative_ownership o
            LEFT JOIN kafdeck_declarative_key_versions v ON v.deployment_id=o.deployment_id AND v.scope_kind=o.scope_kind
                AND v.scope_id=o.scope_id AND v.resource_kind=o.resource_kind AND v.resource_id=o.resource_id
            WHERE v.revision_high_water IS NULL OR v.revision_high_water<o.ownership_revision
            """, token).ConfigureAwait(false), CultureInfo.InvariantCulture) != 0 ||
            Convert.ToInt64(await Scalar(c, t, """
            SELECT COUNT(*) FROM kafdeck_declarative_controller_leases l LEFT JOIN kafdeck_declarative_lease_epoch_high_water h
                ON h.deployment_id=l.deployment_id AND h.lease_scope_id=l.lease_scope_id
            WHERE h.epoch_high_water IS NULL OR h.epoch_high_water<l.epoch
            """, token).ConfigureAwait(false), CultureInfo.InvariantCulture) != 0)
            throw new InvalidOperationException("Declarative durable high-water evidence is inconsistent.");
        await Exec(c, t, """
            INSERT INTO kafdeck_declarative_generation_history(generation_id,deployment_id,resource_key,allocated_revision)
            SELECT ownership_generation_id,deployment_id,resource_id,ownership_revision FROM kafdeck_declarative_ownership WHERE 1=1
            ON CONFLICT(generation_id) DO NOTHING
            """, token).ConfigureAwait(false);
        var manifestHash = CodeManifest(pg);
        if (runtimeCount == 0)
        {
            await Exec(c, t, "INSERT INTO kafdeck_declarative_runtime_schema(id,manifest_hash,structure_hash) VALUES(1,@manifest,@structure)", token,
                ("@manifest", manifestHash), ("@structure", actual)).ConfigureAwait(false);
            await Exec(c, t, "INSERT INTO kafdeck_declarative_schema_migrations(version,manifest_id) VALUES(2,@id)", token, ("@id", Manifest)).ConfigureAwait(false);
        }
        else await VerifyOnConnection(c, t, pg, token).ConfigureAwait(false);
        if (adopting)
            await Exec(c, t, """
                INSERT INTO kafdeck_declarative_runtime_audit(operation_id,event_kind,event_revision,deployment_id,principal_id,event_fingerprint)
                VALUES(@id,'SchemaAdoption',1,'offline-schema','offline-reviewed-operator',@proof)
                """, token, ("@id", Guid.NewGuid().ToString("D")), ("@proof", adoptionEvidenceSha256)).ConfigureAwait(false);
        await t.CommitAsync(token).ConfigureAwait(false);
    }

    public static async Task VerifyAsync(INotificationDeliveryDbConnectionFactory connections, CancellationToken token = default)
    {
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await VerifyOnConnection(c, null, c is not SqliteConnection, token).ConfigureAwait(false);
    }

    // Offline only: the operator has quarantined the outside authority and
    // physically fenced old writers. A proof reference records that reviewed
    // action; neither a restored row nor a runtime status read may invoke it.
    public static async Task AdmitGenerationAsync(INotificationDeliveryDbConnectionFactory connections,
        string deploymentId, Guid? expectedOldGeneration, Guid targetGeneration, string reviewedFenceProofSha256,
        CancellationToken token = default)
    {
        if (string.IsNullOrWhiteSpace(deploymentId) || deploymentId.Length > 128 || deploymentId != deploymentId.Trim() ||
            deploymentId.Any(char.IsControl) || targetGeneration == Guid.Empty || expectedOldGeneration == Guid.Empty ||
            expectedOldGeneration == targetGeneration || !Hash(reviewedFenceProofSha256))
            throw new ArgumentException("Invalid explicit declarative generation admission.");
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = c is SqliteConnection sqlite ? sqlite.BeginTransaction(IsolationLevel.Serializable, deferred: false) :
            await c.BeginTransactionAsync(IsolationLevel.ReadCommitted, token).ConfigureAwait(false);
        if (c is not SqliteConnection) await Exec(c, t, "SELECT pg_advisory_xact_lock(68080068)", token).ConfigureAwait(false);
        await VerifyOnConnection(c, t, c is not SqliteConnection, token).ConfigureAwait(false);
        if (Convert.ToInt64(await Scalar(c, t, "SELECT COUNT(*) FROM kafdeck_declarative_deployments WHERE deployment_id<>@d", token,
            ("@d", deploymentId)).ConfigureAwait(false), CultureInfo.InvariantCulture) != 0)
            throw new InvalidOperationException("Notification persistence admits one exact deployment identity per schema.");
        Guid? old = null; Guid? previous = null; string? proof = null; long revision = 0;
        await using (var command = Cmd(c, t, "SELECT generation,previous_generation,proof_sha256,revision FROM kafdeck_declarative_deployments WHERE deployment_id=@d", ("@d", deploymentId)))
        await using (var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false))
        {
            if (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                old = Guid.Parse(reader.GetString(0)); previous = reader.IsDBNull(1) ? null : Guid.Parse(reader.GetString(1));
                proof = reader.GetString(2); revision = reader.GetInt64(3);
            }
        }
        if (old == targetGeneration && previous == expectedOldGeneration && proof == reviewedFenceProofSha256) return;
        if (old != expectedOldGeneration) throw new InvalidOperationException("Declarative generation admission CAS conflict.");
        var observed = await Scalar(c, t, connections.DatabaseUtcNowSql, token).ConfigureAwait(false);
        object stamp = c is SqliteConnection ?
            DateTimeOffset.Parse((string)observed!, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).ToUniversalTime().ToString("O") : observed!;
        await Exec(c, t, "UPDATE kafdeck_declarative_controller_leases SET expires_at_utc=@now WHERE deployment_id=@d", token,
            ("@now", stamp), ("@d", deploymentId)).ConfigureAwait(false);
        await Exec(c, t, """
            INSERT INTO kafdeck_declarative_deployments(deployment_id,generation,previous_generation,proof_sha256,revision)
            VALUES(@d,@generation,@old,@proof,@revision) ON CONFLICT(deployment_id)
            DO UPDATE SET generation=excluded.generation,previous_generation=excluded.previous_generation,
                proof_sha256=excluded.proof_sha256,revision=excluded.revision
            """, token, ("@d", deploymentId), ("@generation", targetGeneration.ToString("D")),
            ("@old", expectedOldGeneration?.ToString("D")), ("@proof", reviewedFenceProofSha256), ("@revision", checked(revision + 1))).ConfigureAwait(false);
        await Exec(c, t, """
            INSERT INTO kafdeck_declarative_runtime_audit(operation_id,event_kind,event_revision,deployment_id,principal_id,event_fingerprint)
            VALUES(@id,'GenerationAdmission',@revision,@d,'offline-reviewed-operator',@proof)
            """, token, ("@id", targetGeneration.ToString("D")), ("@revision", checked(revision + 1)), ("@d", deploymentId),
            ("@proof", reviewedFenceProofSha256)).ConfigureAwait(false);
        await t.CommitAsync(token).ConfigureAwait(false);
    }
    private static async Task VerifyOnConnection(DbConnection c, DbTransaction? t, bool pg, CancellationToken token)
    {
        if (!await Exists(c, t, "kafdeck_declarative_runtime_schema", token).ConfigureAwait(false) ||
            await Scalar(c, t, "SELECT manifest_id FROM kafdeck_declarative_schema_migrations WHERE version=2", token).ConfigureAwait(false) is not string version || version != Manifest)
            throw new InvalidOperationException("Declarative runtime schema requires explicit operator migration.");
        await using var command = Cmd(c, t, "SELECT manifest_hash,structure_hash FROM kafdeck_declarative_runtime_schema WHERE id=1");
        string manifest; string structure;
        await using (var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false))
        {
            if (!await reader.ReadAsync(token).ConfigureAwait(false)) throw new InvalidOperationException("Declarative schema proof is absent.");
            manifest = reader.GetString(0); structure = reader.GetString(1);
        }
        if (manifest != CodeManifest(pg) || structure != await Signature(c, t, pg, BaselineTables.Concat(RuntimeTables).ToArray(), token).ConfigureAwait(false))
            throw new InvalidOperationException("Declarative versioned schema verification failed.");
    }

    private static async Task<string> ExpectedSignature(DbConnection c, DbTransaction t, bool pg, bool runtime, CancellationToken token)
    {
        if (!pg)
        {
            await using var reference = new SqliteConnection("Data Source=:memory:"); await reference.OpenAsync(token).ConfigureAwait(false);
            await using var tx = reference.BeginTransaction();
            await Baseline(reference, tx, false, token).ConfigureAwait(false);
            if (runtime) foreach (var ddl in RuntimeDdl(false)) await Exec(reference, tx, ddl, token).ConfigureAwait(false);
            return await Signature(reference, tx, false, runtime ? BaselineTables.Concat(RuntimeTables).ToArray() : BaselineTables, token).ConfigureAwait(false);
        }
        var target = (string)(await Scalar(c, t, "SELECT current_schema()", token).ConfigureAwait(false) ?? throw new InvalidOperationException("Explicit target schema required."));
        if (target is "pg_catalog" or "information_schema" || target.Length > 63) throw new InvalidOperationException("Unsafe declarative target schema.");
        var temporary = "w68_verify_" + Guid.NewGuid().ToString("N");
        string Quote(string name) => '"' + name.Replace("\"", "\"\"", StringComparison.Ordinal) + '"';
        await Exec(c, t, "CREATE SCHEMA " + Quote(temporary) + ";SET LOCAL search_path=" + Quote(temporary) + ",pg_catalog", token).ConfigureAwait(false);
        await Baseline(c, t, true, token).ConfigureAwait(false);
        if (runtime) foreach (var ddl in RuntimeDdl(true)) await Exec(c, t, ddl, token).ConfigureAwait(false);
        var signature = await Signature(c, t, true, runtime ? BaselineTables.Concat(RuntimeTables).ToArray() : BaselineTables, token).ConfigureAwait(false);
        await Exec(c, t, "SET LOCAL search_path=" + Quote(target) + ",pg_catalog;DROP SCHEMA " + Quote(temporary) + " CASCADE", token).ConfigureAwait(false);
        return signature;
    }
    private static async Task Baseline(DbConnection c, DbTransaction t, bool pg, CancellationToken token)
    {
        if (pg) { await Exec(c, t, BaselineSql, token).ConfigureAwait(false); return; }
        await Exec(c, t, LedgerDdl(false), token).ConfigureAwait(false);
        foreach (var ddl in W68OwnershipLedgerSchema.SqliteStatements) await Exec(c, t, ddl, token).ConfigureAwait(false);
        await Exec(c, t, W68LeaseSchemaCandidate.Sqlite, token).ConfigureAwait(false);
        await Exec(c, t, W68PermanentLeaseEpochHighWaterSchema.Sqlite, token).ConfigureAwait(false);
        await Exec(c, t, """
            CREATE TABLE IF NOT EXISTS kafdeck_declarative_ownership_audit(
                operation_id TEXT COLLATE BINARY NOT NULL PRIMARY KEY, deployment_id TEXT NOT NULL,scope_kind TEXT NOT NULL,
                scope_id TEXT NOT NULL,resource_kind TEXT NOT NULL,resource_id TEXT NOT NULL,principal_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,source_id TEXT NOT NULL,lease_epoch INTEGER NOT NULL CHECK(lease_epoch>0),
                new_revision INTEGER NOT NULL CHECK(new_revision>0),event_fingerprint TEXT NOT NULL CHECK(length(event_fingerprint)=64),
                recorded_at_utc TEXT NOT NULL DEFAULT(STRFTIME('%Y-%m-%dT%H:%M:%fZ','now')))
            """, token).ConfigureAwait(false);
        await Exec(c, t, "INSERT INTO kafdeck_declarative_schema_migrations(version,manifest_id) VALUES(1,'w68-g2-postgresql-baseline-v1') ON CONFLICT(version) DO NOTHING", token).ConfigureAwait(false);
        if (await Scalar(c, t, "SELECT manifest_id FROM kafdeck_declarative_schema_migrations WHERE version=1", token).ConfigureAwait(false) is not string id || id != "w68-g2-postgresql-baseline-v1")
            throw new InvalidOperationException("Unknown declarative SQLite baseline version.");
    }
    private static string LedgerDdl(bool pg) => "CREATE TABLE IF NOT EXISTS kafdeck_declarative_schema_migrations(version INTEGER PRIMARY KEY CHECK(version>0),manifest_id " +
        (pg ? "VARCHAR(128) COLLATE \"C\"" : "TEXT COLLATE BINARY") + " NOT NULL,applied_at_utc " +
        (pg ? "TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()" : "TEXT NOT NULL DEFAULT(STRFTIME('%Y-%m-%dT%H:%M:%fZ','now'))") + ")";

    private static string[] RuntimeDdl(bool pg)
    {
        var text = pg ? "TEXT COLLATE \"C\"" : "TEXT COLLATE BINARY";
        var guid = pg ? "UUID" : "TEXT";
        var time = pg ? "TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()" : "TEXT NOT NULL DEFAULT(STRFTIME('%Y-%m-%dT%H:%M:%fZ','now'))";
        return
        [
            $"CREATE TABLE IF NOT EXISTS kafdeck_declarative_registrations(deployment_id {text} NOT NULL,kind {text} NOT NULL CHECK(kind IN('Owner','Source','Controller')),registration_id {text} NOT NULL,revision BIGINT NOT NULL CHECK(revision>0),registration_json TEXT NOT NULL CHECK(length(registration_json)<=16384),PRIMARY KEY(deployment_id,kind,registration_id))",
            $"CREATE TABLE IF NOT EXISTS kafdeck_declarative_plans(operation_id {text} NOT NULL PRIMARY KEY,plan_fingerprint {text} NOT NULL CHECK(length(plan_fingerprint)=64),principal_id {text} NOT NULL CHECK(length(principal_id)<=2048),plan_json TEXT NOT NULL CHECK(length(plan_json)<=131072))",
            $"CREATE TABLE IF NOT EXISTS kafdeck_declarative_commit_receipts(operation_id {text} NOT NULL PRIMARY KEY,plan_fingerprint {text} NOT NULL CHECK(length(plan_fingerprint)=64),receipt_json TEXT NOT NULL CHECK(length(receipt_json)<=4096),FOREIGN KEY(operation_id) REFERENCES kafdeck_declarative_plans(operation_id))",
            $"CREATE TABLE IF NOT EXISTS kafdeck_declarative_generation_history(generation_id {guid} NOT NULL PRIMARY KEY,deployment_id {text} NOT NULL,resource_key TEXT NOT NULL,allocated_revision BIGINT NOT NULL CHECK(allocated_revision>0))",
            $"CREATE TABLE IF NOT EXISTS kafdeck_declarative_lease_bindings(deployment_id {text} NOT NULL,lease_scope_id {text} NOT NULL,holder_id {text} NOT NULL,epoch BIGINT NOT NULL CHECK(epoch>0),controller_id {text} NOT NULL,principal_id {text} NOT NULL CHECK(length(principal_id)<=2048),PRIMARY KEY(deployment_id,lease_scope_id),FOREIGN KEY(deployment_id,lease_scope_id) REFERENCES kafdeck_declarative_controller_leases(deployment_id,lease_scope_id))",
            $"CREATE TABLE IF NOT EXISTS kafdeck_declarative_runtime_audit(operation_id {text} NOT NULL,event_kind {text} NOT NULL,event_revision BIGINT NOT NULL CHECK(event_revision>0),deployment_id {text} NOT NULL,principal_id {text} NOT NULL CHECK(length(principal_id)<=2048),event_fingerprint {text} NOT NULL CHECK(length(event_fingerprint)=64),recorded_at_utc {time},PRIMARY KEY(operation_id,event_kind,event_revision))",
            $"CREATE TABLE IF NOT EXISTS kafdeck_declarative_runtime_schema(id INTEGER PRIMARY KEY CHECK(id=1),manifest_hash {text} NOT NULL CHECK(length(manifest_hash)=64),structure_hash {text} NOT NULL CHECK(length(structure_hash)=64))",
            $"CREATE TABLE IF NOT EXISTS kafdeck_declarative_deployments(deployment_id {text} NOT NULL PRIMARY KEY,generation {text} NOT NULL UNIQUE,previous_generation {text},proof_sha256 {text} NOT NULL CHECK(length(proof_sha256)=64),revision BIGINT NOT NULL CHECK(revision>0))",
        ];
    }
    private static string CodeManifest(bool pg) => W68RuntimeFingerprints.Desired(BaselineSql.Replace("\r\n", "\n", StringComparison.Ordinal) + "\n" + string.Join('\n', RuntimeDdl(pg)));
    private static bool Hash(string? value) => value is { Length: 64 } && value.All(x => x is >= '0' and <= '9' or >= 'a' and <= 'f');
    private static async Task<bool> Exists(DbConnection c, DbTransaction? t, string name, CancellationToken token) => c is SqliteConnection ?
        await Scalar(c, t, "SELECT name FROM sqlite_master WHERE type='table' AND name=@name", token, ("@name", name)).ConfigureAwait(false) is string :
        await Scalar(c, t, "SELECT to_regclass(@name)::text", token, ("@name", name)).ConfigureAwait(false) is string;

    private static async Task<string> Signature(DbConnection c, DbTransaction? t, bool pg, string[] tables, CancellationToken token)
    {
        var result = new StringBuilder();
        foreach (var table in tables.Order(StringComparer.Ordinal))
        {
            if (!pg)
            {
                var sql = (string?)await Scalar(c, t, "SELECT sql FROM sqlite_master WHERE type='table' AND name=@table", token, ("@table", table)).ConfigureAwait(false);
                result.Append(table).Append(':').Append(Regex.Replace(sql ?? "MISSING", "\\s+", " ").Replace("IF NOT EXISTS ", "", StringComparison.Ordinal)).Append('\n');
                if (Convert.ToInt64(await Scalar(c, t, "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND tbl_name=@table", token, ("@table", table)).ConfigureAwait(false), CultureInfo.InvariantCulture) != 0)
                    throw new InvalidOperationException("Unexpected declarative schema trigger.");
            }
            else
            {
                var shape = await Scalar(c, t, """
                    SELECT jsonb_build_object('kind',r.relkind,'persistence',r.relpersistence,'rls',r.relrowsecurity,'force',r.relforcerowsecurity,
                      'columns',(SELECT jsonb_agg(jsonb_build_object('name',a.attname,'type',format_type(a.atttypid,a.atttypmod),
                        'required',a.attnotnull,'collation',co.collname,'default',pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum)
                        FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
                        LEFT JOIN pg_collation co ON co.oid=a.attcollation WHERE a.attrelid=r.oid AND a.attnum>0 AND NOT a.attisdropped),
                      'constraints',(SELECT jsonb_agg(x.shape ORDER BY x.shape::text) FROM
                        (SELECT jsonb_build_object('type',con.contype,'definition',pg_get_constraintdef(con.oid),'validated',con.convalidated,
                           'deferred',con.condeferred,'deferrable',con.condeferrable) shape FROM pg_constraint con WHERE con.conrelid=r.oid) x),
                      'uniqueIndexes',(SELECT jsonb_agg(x.shape ORDER BY x.shape::text) FROM
                        (SELECT jsonb_build_object('keys',i.indkey::text,'primary',i.indisprimary,'valid',i.indisvalid,'ready',i.indisready,
                          'predicate',pg_get_expr(i.indpred,i.indrelid),'expressions',pg_get_expr(i.indexprs,i.indrelid)) shape
                         FROM pg_index i WHERE i.indrelid=r.oid AND i.indisunique) x),
                      'triggers',(SELECT COUNT(*) FROM pg_trigger tr WHERE tr.tgrelid=r.oid AND NOT tr.tgisinternal))::text
                    FROM pg_class r JOIN pg_namespace ns ON ns.oid=r.relnamespace
                    WHERE ns.nspname=current_schema() AND r.relname=@table
                    """, token, ("@table", table)).ConfigureAwait(false);
                result.Append(table).Append(':').Append(shape as string ?? "MISSING").Append('\n');
            }
        }
        return W68RuntimeFingerprints.Desired(result.ToString());
    }
    private static DbCommand Cmd(DbConnection c, DbTransaction? t, string sql, params (string, object?)[] values)
    {
        var cmd = c.CreateCommand(); cmd.Transaction = t; cmd.CommandText = sql; cmd.CommandTimeout = 30;
        foreach (var (name, value) in values) { var p = cmd.CreateParameter(); p.ParameterName = name; p.Value = value ?? DBNull.Value; cmd.Parameters.Add(p); }
        return cmd;
    }
    private static async Task<object?> Scalar(DbConnection c, DbTransaction? t, string sql, CancellationToken token, params (string, object?)[] values)
    { await using var cmd = Cmd(c, t, sql, values); return await cmd.ExecuteScalarAsync(token).ConfigureAwait(false); }
    private static async Task Exec(DbConnection c, DbTransaction t, string sql, CancellationToken token, params (string, object?)[] values)
    { await using var cmd = Cmd(c, t, sql, values); await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false); }
}
