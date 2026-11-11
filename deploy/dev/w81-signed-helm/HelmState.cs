using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Infrastructure.Persistence;

var factory = new SqliteMutationDbConnectionFactory(args[1]);
if (args[0] == "admit")
{
    var gate = new AdoGovernanceGenerationGate(factory, Guid.Parse(args[3]));
    await gate.AdmitRestoredGenerationAsync(Guid.Parse(args[2]), Guid.Parse(args[3]), args[4], args[5]);
}
await using var connection = await factory.OpenAsync();
var tables = new List<string>();
await using (var command = connection.CreateCommand())
{
    command.CommandText = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name";
    await using var reader = await command.ExecuteReaderAsync();
    while (await reader.ReadAsync()) tables.Add(reader.GetString(0));
}
var result = new SortedDictionary<string, object>();
foreach (var table in tables.Where(name => name.EndsWith("_schema", StringComparison.Ordinal) ||
    name is "governance_deployment_generation" or "governance_generation_admissions"))
{
    var rows = new List<string>();
    await using var command = connection.CreateCommand();
    command.CommandText = "SELECT * FROM \"" + table.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    await using var reader = await command.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        var values = new object?[reader.FieldCount];
        for (var index = 0; index < values.Length; index++) values[index] = reader.IsDBNull(index) ? null : reader.GetValue(index);
        rows.Add(JsonSerializer.Serialize(values));
    }
    rows.Sort(StringComparer.Ordinal);
    result[table] = new { count = rows.Count, rows, sha256 = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(string.Join("\n", rows)))) };
}
if (!result.ContainsKey("governance_deployment_generation")) throw new InvalidDataException("Missing actual governance state.");
Console.WriteLine(JsonSerializer.Serialize(result));
