using System.Data.Common;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Kafdeck.Infrastructure.Persistence;

public interface IGovernanceGenerationTransactionGate
{
    string? DeploymentGeneration => null;
    Task ValidateAsync(DbConnection connection, DbTransaction transaction, CancellationToken token);
    Task ValidateReadAsync(DbConnection connection, DbTransaction transaction, CancellationToken token) =>
        ValidateAsync(connection, transaction, token);
}

public sealed class GovernanceGenerationException() : InvalidOperationException("Governance deployment generation is not admitted.");

/// <summary>Outside deployment generation is checked on the owning transaction, including old live workers.</summary>
public sealed class AdoGovernanceGenerationGate(IMutationDbConnectionFactory connections, Guid? configuredGeneration)
    : IGovernanceGenerationTransactionGate
{
    public string? DeploymentGeneration => configuredGeneration?.ToString("D");
    private static readonly string SchemaDigest = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
        "governance-generation-v1:singleton-generation-revision;admissions:next-previous-fenced-workers-sha256-restore-sha256-utc"))).ToLowerInvariant();
    public async Task InitializeAsync(CancellationToken token = default)
    {
        if (configuredGeneration == Guid.Empty) throw new GovernanceGenerationException();
        await using var connection = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        await CreateSchemaAsync(connection, transaction, token).ConfigureAwait(false);
        if (configuredGeneration is { } generation)
        {
            await ExecuteAsync(connection, transaction,
                "INSERT INTO governance_deployment_generation(singleton_id,generation,revision) VALUES(1,@generation,1) ON CONFLICT(singleton_id) DO NOTHING",
                token, ("@generation", generation.ToString("D"))).ConfigureAwait(false);
        }
        await ValidateAsync(connection, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
    }

    public Task ValidateAsync(DbConnection connection, DbTransaction transaction, CancellationToken token) =>
        ValidateAsync(connection, transaction, readOnly: false, token);

    public Task ValidateReadAsync(DbConnection connection, DbTransaction transaction, CancellationToken token) =>
        ValidateAsync(connection, transaction, readOnly: true, token);

    private async Task ValidateAsync(DbConnection connection, DbTransaction transaction, bool readOnly, CancellationToken token)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT generation FROM governance_deployment_generation WHERE singleton_id=1";
        // A PostgreSQL share lock allows concurrent reads and still blocks the restore UPDATE.
        if (connections.SupportsSelectForUpdate)
            command.CommandText += readOnly ? " FOR SHARE" : " FOR UPDATE";
        var value = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
        if (value is null or DBNull)
        {
            if (configuredGeneration is not null) throw new GovernanceGenerationException();
            return; // Never-enrolled legacy store; no governance identity is inferred.
        }
        if (configuredGeneration is null || !string.Equals(Convert.ToString(value, CultureInfo.InvariantCulture),
                configuredGeneration.Value.ToString("D"), StringComparison.Ordinal)) throw new GovernanceGenerationException();
    }

    /// <summary>Offline quarantined operator action. Never called from HTTP or a running hosted worker.</summary>
    public async Task AdmitRestoredGenerationAsync(Guid previous, Guid next, string fencedWorkersEvidenceSha256,
        string restoreEvidenceSha256, CancellationToken token = default)
    {
        if (previous == Guid.Empty || next == Guid.Empty || previous == next || configuredGeneration != next ||
            !IsHash(fencedWorkersEvidenceSha256) || !IsHash(restoreEvidenceSha256))
            throw new ArgumentException("Generation admission requires exact previous/new generation and two evidence SHA256 values.");
        await using var connection = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        await CreateSchemaAsync(connection, transaction, token).ConfigureAwait(false);
        var changed = await ExecuteAsync(connection, transaction,
            "UPDATE governance_deployment_generation SET generation=@next,revision=revision+1 WHERE singleton_id=1 AND generation=@previous",
            token, ("@next", next.ToString("D")), ("@previous", previous.ToString("D"))).ConfigureAwait(false);
        if (changed != 1) throw new GovernanceGenerationException();
        await ExecuteAsync(connection, transaction,
            """
            INSERT INTO governance_generation_admissions(generation,previous_generation,fenced_workers_sha256,restore_sha256,admitted_at_utc)
            VALUES(@next,@previous,@fenced,@restore,@time)
            """, token, ("@next", next.ToString("D")), ("@previous", previous.ToString("D")),
            ("@fenced", fencedWorkersEvidenceSha256), ("@restore", restoreEvidenceSha256),
            ("@time", DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture))).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
    }

    private async Task CreateSchemaAsync(DbConnection connection, DbTransaction transaction, CancellationToken token)
    {
        if (connections.SupportsSelectForUpdate)
            await ExecuteAsync(connection, transaction, "SELECT pg_advisory_xact_lock(64964000901)", token).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction,
            """
            CREATE TABLE IF NOT EXISTS governance_deployment_generation(
                singleton_id INTEGER PRIMARY KEY CHECK(singleton_id=1),generation TEXT NOT NULL,revision BIGINT NOT NULL CHECK(revision>0));
            CREATE TABLE IF NOT EXISTS governance_generation_admissions(
                generation TEXT PRIMARY KEY,previous_generation TEXT NOT NULL,fenced_workers_sha256 TEXT NOT NULL,
                restore_sha256 TEXT NOT NULL,admitted_at_utc TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS governance_generation_schema(version INTEGER PRIMARY KEY,digest TEXT NOT NULL);
            """, token).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction,
            "INSERT INTO governance_generation_schema(version,digest) VALUES(1,@digest) ON CONFLICT(version) DO NOTHING",
            token, ("@digest", SchemaDigest)).ConfigureAwait(false);
        await using var check = connection.CreateCommand(); check.Transaction = transaction;
        check.CommandText = "SELECT version,digest FROM governance_generation_schema";
        await using var reader = await check.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false) || reader.GetInt32(0) != 1 || reader.GetString(1) != SchemaDigest ||
            await reader.ReadAsync(token).ConfigureAwait(false)) throw new GovernanceGenerationException();
    }

    private static bool IsHash(string value) => value is { Length: 64 } && value.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');

    private static async Task<int> ExecuteAsync(DbConnection connection, DbTransaction transaction, string sql,
        CancellationToken token, params (string Name, object Value)[] parameters)
    {
        await using var command = connection.CreateCommand(); command.Transaction = transaction; command.CommandText = sql;
        foreach (var (name, value) in parameters)
        { var parameter = command.CreateParameter(); parameter.ParameterName = name; parameter.Value = value; command.Parameters.Add(parameter); }
        return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }
}
