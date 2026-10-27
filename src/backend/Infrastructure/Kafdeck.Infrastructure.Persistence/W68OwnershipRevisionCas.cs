using System.Data.Common;

namespace Kafdeck.Infrastructure.Persistence;

// Candidate-only compare-and-swap primitive. The caller must supply an already
// authorized transaction; this helper does not authenticate a controller,
// acquire a lease, register an owner, or install schemas.
internal static class W68OwnershipRevisionCas
{
    internal static async Task<bool> TryAdvanceAsync(
        DbConnection connection, DbTransaction transaction,
        string deploymentId, string scopeKind, string scopeId,
        string resourceKind, string resourceId,
        long expectedRevision, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        if (transaction.Connection != connection)
            throw new ArgumentException("The transaction must belong to the supplied connection.", nameof(transaction));
        Validate(deploymentId, 128, nameof(deploymentId));
        Validate(scopeKind, 64, nameof(scopeKind));
        Validate(scopeId, 128, nameof(scopeId));
        Validate(resourceKind, 64, nameof(resourceKind));
        Validate(resourceId, 128, nameof(resourceId));
        if (expectedRevision < 0 || expectedRevision == long.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(expectedRevision));

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE kafdeck_declarative_key_versions
               SET revision_high_water = revision_high_water + 1
             WHERE deployment_id = @deployment AND scope_kind = @scope_kind
               AND scope_id = @scope AND resource_kind = @kind
               AND resource_id = @resource AND revision_high_water = @expected
            """;
        Add(command, "@deployment", deploymentId);
        Add(command, "@scope_kind", scopeKind);
        Add(command, "@scope", scopeId);
        Add(command, "@kind", resourceKind);
        Add(command, "@resource", resourceId);
        Add(command, "@expected", expectedRevision);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    private static void Validate(string value, int limit, string name)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > limit ||
            !string.Equals(value, value.Trim(), StringComparison.Ordinal))
            throw new ArgumentException("Invalid canonical ownership scope.", name);
    }

    private static void Add(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
