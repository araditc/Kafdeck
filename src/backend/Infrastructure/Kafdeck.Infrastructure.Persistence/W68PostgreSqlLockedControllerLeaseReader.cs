using System.Data;
using System.Data.Common;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

// Candidate-only PostgreSQL row-lock observation. The caller owns the transaction.
// This is NOT holder authentication, an acquired/renewed lease, ownership adoption,
// or a permission to apply; a future owner-CAS must use this SAME live transaction
// and separately verify the authenticated holder, acquired epoch and DB time.
internal sealed record W68PostgreSqlLockedControllerLeaseObservation(
    W68StoredControllerLease Lease,
    DateTimeOffset DatabaseObservedAtUtc);

internal static class W68PostgreSqlLockedControllerLeaseReader
{
    // NOWAIT fails instead of waiting behind another writer. Never interpret a
    // lock failure, a missing row or a provider/storage error as permission.
    internal const string LockedReadSql = """
        SELECT holder_id, epoch, expires_at_utc, clock_timestamp()
          FROM kafdeck_declarative_controller_leases
         WHERE deployment_id = @deployment AND lease_scope_id = @scope
         FOR UPDATE NOWAIT
        """;

    internal static async Task<W68PostgreSqlLockedControllerLeaseObservation?> ReadForUpdateAsync(
        DbConnection connection, DbTransaction transaction,
        string deploymentId, string scopeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        if (!ReferenceEquals(transaction.Connection, connection))
            throw new ArgumentException("Transaction must belong to the provided connection.", nameof(transaction));
        Validate(deploymentId, nameof(deploymentId));
        Validate(scopeId, nameof(scopeId));

        // SQLite transactional SELECT does not provide PostgreSQL row-lock
        // semantics. Do not silently downgrade the authoritative proof.
        if (connection is not NpgsqlConnection || transaction is not NpgsqlTransaction)
            throw new NotSupportedException("Only a live PostgreSQL transaction supports this locked observation.");
        if (connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Connection must be open.");

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = LockedReadSql;
        Add(command, "@deployment", deploymentId);
        Add(command, "@scope", scopeId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null; // Unproven absence, never an acquisition or adoption grant.

        var holderId = reader.GetString(0);
        var epoch = reader.GetInt64(1);
        var expiresAtUtc = RequireUtc(reader.GetValue(2));
        var databaseNowUtc = RequireUtc(reader.GetValue(3));
        if (string.IsNullOrWhiteSpace(holderId) || holderId.Length > 128 ||
            epoch < 1 || expiresAtUtc == default || databaseNowUtc == default)
            throw new InvalidOperationException("Invalid authoritative lease row.");

        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidOperationException("Ambiguous authoritative lease rows.");

        return new W68PostgreSqlLockedControllerLeaseObservation(
            new W68StoredControllerLease(deploymentId, scopeId, holderId, epoch, expiresAtUtc),
            databaseNowUtc);
    }

    // Npgsql maps PostgreSQL timestamptz to UTC DateTime. Reject timezone-less
    // values rather than interpreting them using an application host clock.
    internal static DateTimeOffset RequireUtc(object value) => value switch
    {
        DateTime dt when dt.Kind == DateTimeKind.Utc => new DateTimeOffset(dt),
        DateTimeOffset dto when dto.Offset == TimeSpan.Zero => dto,
        _ => throw new InvalidOperationException("Expected UTC PostgreSQL timestamptz.")
    };

    private static void Validate(string value, string name)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 128 ||
            !string.Equals(value, value.Trim(), StringComparison.Ordinal))
            throw new ArgumentException("Invalid canonical lease scope.", name);
    }

    private static void Add(DbCommand command, string name, string value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
