using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

// Candidate-only unit of work: immutable audit failure rolls back ownership CAS
// and permanent revision ledger together. All identities are caller-supplied and
// therefore this is NOT an authenticated runtime controller API.
internal static class W68PostgreSqlAuditedOwnershipCasCandidate
{
    internal static async Task<long> TryUpdateAsync(
        NpgsqlConnection connection,
        string deployment, string scopeKind, string scope, string resourceKind,
        string resource, string owner, string source, string leaseScope,
        string holder, long epoch, long expectedRevision, string fingerprint,
        string principal, string policyVersion, string auditOperationId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
        try
        {
            var revision = await W68PostgreSqlFencedOwnershipCasCandidate.TryUpdateAsync(
                connection, transaction, deployment, scopeKind, scope, resourceKind,
                resource, owner, source, leaseScope, holder, epoch, expectedRevision,
                fingerprint, principal, policyVersion, auditOperationId, ct).ConfigureAwait(false);
            if (revision == 0)
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                return 0;
            }
            await W68PostgreSqlOwnershipAuditCandidate.RecordAsync(
                connection, transaction, auditOperationId, deployment, scopeKind,
                scope, resourceKind, resource, principal, owner, source, epoch,
                revision, fingerprint, ct).ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return revision;
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }
}
