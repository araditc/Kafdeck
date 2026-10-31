using System.Data;
using System.Data.Common;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

// Transaction-scoped candidate only. Caller must authenticate principal, prove
// registered owner/source, prepare durable audit in THIS transaction and commit
// atomically. This helper NEVER registers runtime authority.
internal static class W68PostgreSqlFencedOwnershipCasCandidate
{
    internal const string Sql = """
        UPDATE kafdeck_declarative_ownership AS o
           SET ownership_revision = o.ownership_revision + 1,
               stored_lease_epoch = @epoch,
               evidence_fingerprint = @fingerprint,
               claimant_principal_id = @principal,
               policy_version = @policy,
               audit_operation_id = @audit,
               updated_at_utc = clock_timestamp()
         WHERE o.deployment_id = @deployment
           AND o.scope_kind = @scopeKind AND o.scope_id = @scope
           AND o.resource_kind = @kind AND o.resource_id = @resource
           AND o.ownership_revision = @expectedRevision
           AND o.ownership_revision < 9223372036854775807
           AND o.registered_owner_id = @owner
           AND o.registered_source_id = @source
           AND o.ownership_state = 'Managed'
           AND o.stored_lease_epoch <= @epoch
           AND EXISTS (
               SELECT 1 FROM kafdeck_declarative_controller_leases AS l
                WHERE l.deployment_id = o.deployment_id
                  AND l.lease_scope_id = @leaseScope
                  AND l.holder_id = @holder
                  AND l.epoch = @epoch
                  AND l.expires_at_utc > clock_timestamp()
                FOR UPDATE
           )
        RETURNING o.ownership_revision
        """;

    internal static async Task<long> TryUpdateAsync(
        DbConnection connection, DbTransaction transaction,
        string deployment, string scopeKind, string scope, string resourceKind,
        string resource, string owner, string source, string leaseScope,
        string authenticatedHolder, long epoch, long expectedRevision,
        string evidenceFingerprint, string authenticatedPrincipal,
        string policyVersion, string durableAuditOperationId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        if (!ReferenceEquals(transaction.Connection, connection) ||
            connection is not NpgsqlConnection || transaction is not NpgsqlTransaction ||
            connection.State != ConnectionState.Open)
            throw new InvalidOperationException("PostgreSQL transaction required.");
        foreach (var s in new [] {deployment,scopeKind,scope,resourceKind,resource,owner,source,
                                  leaseScope,authenticatedHolder,authenticatedPrincipal,
                                  policyVersion,durableAuditOperationId})
            if (string.IsNullOrWhiteSpace(s) || s.Length > 128 || s != s.Trim())
                throw new ArgumentException("Noncanonical ownership evidence.");
        if (evidenceFingerprint.Length != 64 || !evidenceFingerprint.All(
            ch => ch is >= '0' and <= '9' or >= 'a' and <= 'f'))
            throw new ArgumentException("Invalid SHA256 fingerprint.",nameof(evidenceFingerprint));
        if (epoch <= 0 || expectedRevision < 1 || expectedRevision == long.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(expectedRevision));
        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = Sql;
        foreach (var (key,value) in new (string,object)[] {
            ("deployment",deployment),("scopeKind",scopeKind),("scope",scope),
            ("kind",resourceKind),("resource",resource),("owner",owner),("source",source),
            ("leaseScope",leaseScope),("holder",authenticatedHolder),
            ("epoch",epoch),("expectedRevision",expectedRevision),
            ("fingerprint",evidenceFingerprint),("principal",authenticatedPrincipal),
            ("policy",policyVersion),("audit",durableAuditOperationId)})
        {
            var p = cmd.CreateParameter();
            p.ParameterName = "@"+key;
            p.Value = value;
            cmd.Parameters.Add(p);
        }
        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is long next && next == expectedRevision + 1 ? next : 0;
        // 0 is denial, NOT proof of absence or authorization to adopt.
    }
}
