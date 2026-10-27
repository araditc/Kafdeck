using System.Data.Common;
using System.Globalization;

namespace Kafdeck.Infrastructure.Persistence;

// Storage-only projection: a transaction does not prove PostgreSQL row locking,
// authorization, clock freshness or an acquired controller lease.
internal sealed record W68StoredControllerLease(
    string DeploymentId, string ScopeId, string HolderId,
    long Epoch, DateTimeOffset ExpiresAtUtc);

internal static class W68StoredControllerLeaseReader
{
    internal static async Task<W68StoredControllerLease?> ReadAsync(
        DbConnection connection, DbTransaction transaction,
        string deploymentId, string scopeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        if (!ReferenceEquals(transaction.Connection, connection))
            throw new ArgumentException("Transaction must belong to the provided connection.", nameof(transaction));
        Validate(deploymentId, nameof(deploymentId));
        Validate(scopeId, nameof(scopeId));
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT holder_id, epoch, expires_at_utc
              FROM kafdeck_declarative_controller_leases
             WHERE deployment_id = @deployment AND lease_scope_id = @scope
            """;
        Add(command, "@deployment", deploymentId);
        Add(command, "@scope", scopeId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null; // Storage absence is NOT an authorization or acquisition grant.
        var epoch = reader.GetInt64(1);
        var expiry = reader.GetValue(2);
        var expires = expiry is DateTimeOffset dto
            ? dto.ToUniversalTime()
            : DateTimeOffset.Parse(Convert.ToString(expiry, CultureInfo.InvariantCulture)!,
                CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        var result = new W68StoredControllerLease(
            deploymentId, scopeId, reader.GetString(0), epoch, expires);
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidOperationException("Non-unique controller lease observation.");
        return result;
    }

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
