using System.Data;
using System.Data.Common;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

// Internal candidate-only CAS renewal. Caller must already have an authenticated
// controller identity and an acquired epoch from a separately admitted authority.
// This helper does NOT authenticate, acquire, authorize, migrate or run on startup.
internal static class W68PostgreSqlLeaseRenewCandidate
{
    internal const string Sql = """
        UPDATE kafdeck_declarative_controller_leases
           SET expires_at_utc = clock_timestamp() + (@duration_seconds * INTERVAL '1 second')
         WHERE deployment_id = @deployment AND lease_scope_id = @scope
           AND holder_id = @holder AND epoch = @epoch
           AND expires_at_utc > clock_timestamp()
        RETURNING epoch, expires_at_utc
        """;

    internal static async Task<W68StoredControllerLease?> TryRenewAsync(
        DbConnection connection, DbTransaction transaction,
        string deploymentId, string scopeId, string authenticatedHolderId,
        long acquiredEpoch, int durationSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        if (!ReferenceEquals(transaction.Connection, connection))
            throw new ArgumentException("Transaction must belong to the connection.", nameof(transaction));
        Validate(deploymentId, nameof(deploymentId));
        Validate(scopeId, nameof(scopeId));
        Validate(authenticatedHolderId, nameof(authenticatedHolderId));
        if (acquiredEpoch < 1) throw new ArgumentOutOfRangeException(nameof(acquiredEpoch));
        if (durationSeconds is < 1 or > 300) throw new ArgumentOutOfRangeException(nameof(durationSeconds));
        if (connection is not NpgsqlConnection || transaction is not NpgsqlTransaction)
            throw new NotSupportedException("Lease renew candidate requires PostgreSQL.");
        if (connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Connection must be open.");

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = Sql;
        Add(command, "@deployment", deploymentId);
        Add(command, "@scope", scopeId);
        Add(command, "@holder", authenticatedHolderId);
        Add(command, "@epoch", acquiredEpoch);
        Add(command, "@duration_seconds", durationSeconds);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null; // Never renew after expiry, stale epoch or wrong identity.
        var epoch = reader.GetInt64(0);
        var expiry = W68PostgreSqlLockedControllerLeaseReader.RequireUtc(reader.GetValue(1));
        if (epoch != acquiredEpoch)
            throw new InvalidOperationException("Unexpected lease renewal epoch.");
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidOperationException("Ambiguous lease renewal.");
        return new W68StoredControllerLease(deploymentId, scopeId, authenticatedHolderId, epoch, expiry);
    }

    private static void Validate(string value, string name)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 128 ||
            !string.Equals(value, value.Trim(), StringComparison.Ordinal))
            throw new ArgumentException("Invalid canonical lease identity or scope.", name);
    }

    private static void Add(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
