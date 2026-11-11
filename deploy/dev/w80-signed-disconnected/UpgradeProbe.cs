using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;

var verb = args[0];
var provider = args[1];
var location = args[2];
IMutationDbConnectionFactory factory = provider switch
{
    "sqlite" => new SqliteMutationDbConnectionFactory(location),
    "postgres" => new PostgreSqlMutationDbConnectionFactory(File.ReadAllText(location).Trim()),
    _ => throw new ArgumentException("Unknown fixture provider")
};

if (verb == "backup" && provider == "sqlite")
{
    using var source = new SqliteConnection(new SqliteConnectionStringBuilder
    { DataSource = location, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ConnectionString);
    using var target = new SqliteConnection(new SqliteConnectionStringBuilder
    { DataSource = args[3], Pooling = false }.ConnectionString);
    source.Open();
    target.Open();
    source.BackupDatabase(target);
    using var check = target.CreateCommand();
    check.CommandText = "PRAGMA integrity_check";
    if ((string?)check.ExecuteScalar() != "ok") throw new InvalidDataException("SQLite backup integrity failed.");
    Console.WriteLine("SQLite BackupDatabase and integrity_check succeeded");
    return;
}

if (verb == "seed")
{
    var old = new AdoMutationOperationRepository(factory);
    await old.InitializeAsync();
    var now = DateTimeOffset.UtcNow;
    foreach (var label in new[] {"pending", "approved", "unknown"})
    {
        var risk = new MutationRiskDecision(MutationRiskClass.Moderate, ["w80-upgrade-fixture"],
            MutationConfirmationMode.Explicit, label != "unknown");
        var op = MutationOperation.CreatePreview("oidc:https://w80.fixture|requester",
            new(MutationOperationKind.TopicCreate, "w80-fixture", "{\"name\":\"w80-" + label + "\"}",
                ["topic:w80-fixture:w80-" + label]),
            risk, "w80-fixture-v08", now.AddMinutes(20), now, "w80-" + label);
        op.OpenForConfirmation(now);
        op.Confirm(op.Snapshot.RequesterPrincipalId, op.Snapshot.PreviewHash, now, op.Snapshot.ConfirmationChallenge);
        var snapshot = op.Snapshot;
        if (label == "approved")
        {
            // Synthetic approval fixture: migration must preserve the old version's durable representation.
            snapshot = snapshot with { State = MutationOperationState.Ready, ApprovedByPrincipalId = "oidc:https://w80.fixture|approver",
                ApprovedAtUtc = now, ApprovalAuthorizationEvidenceHash = new string('a', 64), Version = snapshot.Version + 1 };
        }
        else if (label == "unknown")
        {
            op.ClaimExecution(now, now.AddSeconds(30));
            op.MarkDispatchStarted(now);
            op.Complete(MutationExecutionResultKind.ExecutionUnknown, "W80FixtureUnknown", now);
            snapshot = op.Snapshot;
        }
        await old.CreateAsync(snapshot);
    }
}
else if (verb != "observe")
{
    throw new ArgumentException("Unknown fixture operation");
}

await using var connection = await factory.OpenAsync();
var states = new List<object>();
using (var command = connection.CreateCommand())
{
    command.CommandText = "SELECT state, snapshot_json FROM kafdeck_mutation_operations ORDER BY state, operation_id";
    using var reader = await command.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        states.Add(new
        {
            State = (MutationOperationState)reader.GetInt32(0),
            SnapshotSha256 = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(reader.GetString(1)))).ToLowerInvariant()
        });
    }
}
var schema = new List<string>();
using (var command = connection.CreateCommand())
{
    command.CommandText = provider == "sqlite"
        ? "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        : "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname=current_schema() ORDER BY tablename";
    using var reader = await command.ExecuteReaderAsync();
    while (await reader.ReadAsync()) schema.Add(reader.GetString(0));
}
Console.WriteLine(JsonSerializer.Serialize(new { States = states, Tables = schema }));
