using System.Data;
using System.Data.Common;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

// Internal audit candidate. Audit and owner CAS must share the identical
// transaction; no runtime identity, permission or source registration is minted.
internal static class W68PostgreSqlOwnershipAuditCandidate
{
    internal const string Schema = """
        CREATE TABLE IF NOT EXISTS kafdeck_declarative_ownership_audit (
          operation_id VARCHAR(128) COLLATE "C" PRIMARY KEY,
          deployment_id VARCHAR(128) COLLATE "C" NOT NULL,
          scope_kind VARCHAR(64) COLLATE "C" NOT NULL,
          scope_id VARCHAR(128) COLLATE "C" NOT NULL,
          resource_kind VARCHAR(64) COLLATE "C" NOT NULL,
          resource_id VARCHAR(128) COLLATE "C" NOT NULL,
          principal_id VARCHAR(128) COLLATE "C" NOT NULL,
          owner_id VARCHAR(128) COLLATE "C" NOT NULL,
          source_id VARCHAR(128) COLLATE "C" NOT NULL,
          lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
          new_revision BIGINT NOT NULL CHECK (new_revision > 0),
          event_fingerprint CHAR(64) NOT NULL,
          recorded_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        )
        """;

    internal static async Task RecordAsync(
        DbConnection connection, DbTransaction transaction,
        string operationId, string deployment, string scopeKind, string scope,
        string resourceKind, string resource, string principal, string owner,
        string source, long leaseEpoch, long newRevision, string fingerprint,
        CancellationToken ct = default)
    {
        if (connection is not NpgsqlConnection || transaction is not NpgsqlTransaction ||
            !ReferenceEquals(transaction.Connection, connection) ||
            connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Live PostgreSQL transaction required.");
        foreach (var value in new[] { operationId, deployment, scopeKind, scope, resourceKind,
                 resource, principal, owner, source })
            if (string.IsNullOrWhiteSpace(value) || value.Length > 128 || value != value.Trim())
                throw new ArgumentException("Invalid canonical audit identity.");
        if (leaseEpoch <= 0 || newRevision <= 0 || fingerprint is null ||
            fingerprint.Length != 64 || !fingerprint.All(ch =>
                ch is >= '0' and <= '9' or >= 'a' and <= 'f'))
            throw new ArgumentException("Invalid audit integrity evidence.");
        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = """
            INSERT INTO kafdeck_declarative_ownership_audit
            (operation_id,deployment_id,scope_kind,scope_id,resource_kind,resource_id,
             principal_id,owner_id,source_id,lease_epoch,new_revision,event_fingerprint)
            VALUES (@op,@d,@sk,@s,@rk,@r,@principal,@owner,@source,@epoch,@revision,@fp)
            """;
        foreach (var (key, value) in new (string, object)[] {
            ("@op",operationId),("@d",deployment),("@sk",scopeKind),("@s",scope),
            ("@rk",resourceKind),("@r",resource),("@principal",principal),
            ("@owner",owner),("@source",source),("@epoch",leaseEpoch),
            ("@revision",newRevision),("@fp",fingerprint) })
        {
            var p = cmd.CreateParameter(); p.ParameterName = key; p.Value = value;
            cmd.Parameters.Add(p);
        }
        if (await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false) != 1)
            throw new InvalidOperationException("Durable audit insert not confirmed.");
    }
}
