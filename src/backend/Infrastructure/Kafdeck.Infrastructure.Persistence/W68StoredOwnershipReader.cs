using System.Data.Common;
using System.Globalization;

namespace Kafdeck.Infrastructure.Persistence;

// An internal storage projection only. No API, service registration, permission,
// actor identity, registry source verification, or ownership authority is issued.
internal sealed record W68StoredOwnershipObservation(
    string RegisteredOwnerId,
    string RegisteredSourceId,
    string SourceGenerationId,
    string PinnedSourceRevision,
    string OwnershipGenerationId,
    long OwnershipRevision,
    long StoredLeaseEpoch,
    string State);

internal static class W68StoredOwnershipReader
{
    internal static async Task<W68StoredOwnershipObservation?> ReadExactAsync(
        DbConnection connection,
        DbTransaction transaction,
        string deploymentId,
        string scopeKind,
        string scopeId,
        string resourceKind,
        string resourceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        if (!ReferenceEquals(transaction.Connection, connection))
            throw new ArgumentException("The transaction must belong to the connection.", nameof(transaction));
        Validate(deploymentId, 128);
        Validate(scopeKind, 64);
        Validate(scopeId, 128);
        Validate(resourceKind, 64);
        Validate(resourceId, 128);

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT registered_owner_id, registered_source_id,
                   registered_source_generation_id, pinned_source_revision,
                   ownership_generation_id, ownership_revision,
                   stored_lease_epoch, ownership_state
              FROM kafdeck_declarative_ownership
             WHERE deployment_id=@deployment AND scope_kind=@scopeKind
               AND scope_id=@scope AND resource_kind=@kind AND resource_id=@resource
            """;
        Add(command, "@deployment", deploymentId);
        Add(command, "@scopeKind", scopeKind);
        Add(command, "@scope", scopeId);
        Add(command, "@kind", resourceKind);
        Add(command, "@resource", resourceId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null; // Null is unproven storage absence, NEVER authorization to adopt.
        var result = new W68StoredOwnershipObservation(
            reader.GetString(0), reader.GetString(1),
            Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture)!,
            reader.GetString(3),
            Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture)!,
            reader.GetInt64(5), reader.GetInt64(6), reader.GetString(7));
        // The persisted exact key is unique. Never silently choose a row from
        // unexpected duplication or degrade a storage error into "missing".
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidOperationException("Ambiguous ownership rows.");
        return result;
    }

    private static void Validate(string value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > maxLength ||
            !string.Equals(value, value.Trim(), StringComparison.Ordinal))
            throw new ArgumentException("Invalid exact ownership resource scope.");
    }

    private static void Add(DbCommand command, string key, string value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = key;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
