using System.Data;
using System.Data.Common;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

// Isolated storage primitive, NOT an authenticated lease acquisition.
// Caller must hold a governed transaction; acquiring a lease row and high-water
// advance must be made atomic by a separately admitted orchestration layer.
internal static class W68PostgreSqlEpochAdvanceCandidate
{
    internal const string Sql = """
        INSERT INTO kafdeck_declarative_lease_epoch_high_water
            (deployment_id, lease_scope_id, epoch_high_water)
        VALUES (@deployment, @scope, 1)
        ON CONFLICT (deployment_id, lease_scope_id)
        DO UPDATE SET epoch_high_water =
            kafdeck_declarative_lease_epoch_high_water.epoch_high_water + 1
        WHERE kafdeck_declarative_lease_epoch_high_water.epoch_high_water < 9223372036854775807
        RETURNING epoch_high_water
        """;

    internal static async Task<long> AdvanceAsync(
        DbConnection connection, DbTransaction transaction,
        string deployment, string scope, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        if (!ReferenceEquals(transaction.Connection, connection))
            throw new ArgumentException("Transaction must belong to the connection.", nameof(transaction));
        Validate(deployment, nameof(deployment));
        Validate(scope, nameof(scope));
        if (connection is not NpgsqlConnection || transaction is not NpgsqlTransaction)
            throw new NotSupportedException("The monotonic epoch advance requires PostgreSQL.");
        if (connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = Sql;
        Add(command, "@deployment", deployment);
        Add(command, "@scope", scope);
        var value = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return value is long epoch && epoch > 0
            ? epoch : throw new InvalidOperationException("Epoch advance failed or exhausted.");
    }

    private static void Validate(string value, string parameter)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 128 ||
            !string.Equals(value, value.Trim(), StringComparison.Ordinal))
            throw new ArgumentException("Invalid canonical lease scope.", parameter);
    }
    private static void Add(DbCommand command, string name, object value)
    {
        var p = command.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        command.Parameters.Add(p);
    }
}
