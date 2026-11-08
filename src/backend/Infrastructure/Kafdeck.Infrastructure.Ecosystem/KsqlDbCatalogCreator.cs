using System.Globalization;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.RegularExpressions;
using Kafdeck.Core.ReadViews;

namespace Kafdeck.Infrastructure.Ecosystem;

public sealed record KsqlCatalogField(string? Name, string? Type);

public sealed record KsqlCatalogDefinition(
    string? Name,
    string? Topic,
    string? ValueFormat,
    IReadOnlyList<KsqlCatalogField?>? Fields,
    int? Partitions,
    int? Replicas,
    KsqlCatalogField? PrimaryKey = null);

public enum KsqlCatalogKind { Stream, Table }

public enum KsqlCatalogCreation
{
    Created,
    Unconfirmed,
    Rejected,
    Denied,
}

public static partial class KsqlDbCatalogCreator
{
    private const string MediaType = "application/vnd.ksql.v1+json";
    private const long MaxResponseBytes = 128 * 1024;

    public static async Task<string?> GetKafkaClusterIdAsync(HttpClient client, CancellationToken token)
    {
        try
        {
            var runtime = new HttpReadRuntime(client, null, IsRegistered: true);
            var reply = await ReadOnlyHttpSupport.GetJsonAsync(runtime, "info", MaxResponseBytes, token).ConfigureAwait(false);
            return reply.ValueKind == JsonValueKind.Object && reply.TryGetProperty("KsqlServerInfo", out var server)
                ? StringProperty(server, "kafkaClusterId") : null;
        }
        catch (Exception ex) when (IsUnconfirmed(ex))
        {
            return null;
        }
    }

    public static bool TryBuildStatement(
        KsqlCatalogKind kind, KsqlCatalogDefinition definition, out string name, out string statement) =>
        TryBuildStatement(kind, definition, ifNotExists: false, out name, out statement);

    private static bool TryBuildStatement(KsqlCatalogKind kind, KsqlCatalogDefinition definition,
        bool ifNotExists, out string name, out string statement)
    {
        name = "";
        statement = "";
        if (kind is not (KsqlCatalogKind.Stream or KsqlCatalogKind.Table) ||
            definition.Name is not { } objectName || !Identifier().IsMatch(objectName) ||
            definition.Topic is not { } topic || !TopicName().IsMatch(topic) || topic is "." or ".." ||
            definition.ValueFormat is not ("JSON" or "JSON_SR" or "AVRO" or "PROTOBUF" or "DELIMITED") ||
            definition.Fields is not { Count: >= 1 and <= 64 } fields ||
            (definition.Partitions is null) != (definition.Replicas is null) ||
            definition.Partitions is < 1 or > 256 || definition.Replicas is < 1 or > 16)
            return false;

        var columns = new List<string>(fields.Count);
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (kind == KsqlCatalogKind.Table)
        {
            if (definition.PrimaryKey is not { Name: { } keyName, Type: "VARCHAR" or "INTEGER" or "BIGINT" or "DOUBLE" } key ||
                !Identifier().IsMatch(keyName))
                return false;
            names.Add(keyName);
            columns.Add($"`{keyName.ToUpperInvariant()}` {key.Type} PRIMARY KEY");
        }
        else if (definition.PrimaryKey is not null)
        {
            return false;
        }

        foreach (var field in fields)
        {
            if (field?.Name is not { } fieldName || !Identifier().IsMatch(fieldName) ||
                !names.Add(fieldName) || field.Type is not ("VARCHAR" or "INTEGER" or "BIGINT" or "DOUBLE" or "BOOLEAN"))
                return false;
            columns.Add($"`{fieldName.ToUpperInvariant()}` {field.Type}");
        }

        name = objectName.ToUpperInvariant();
        var options = $"KAFKA_TOPIC='{topic}', VALUE_FORMAT='{definition.ValueFormat}'";
        if (kind == KsqlCatalogKind.Table)
            options += ", KEY_FORMAT='KAFKA'";
        if (definition.Partitions is int partitions && definition.Replicas is int replicas)
            options += string.Create(CultureInfo.InvariantCulture, $", PARTITIONS={partitions}, REPLICAS={replicas}");
        var keyword = kind == KsqlCatalogKind.Table ? "TABLE" : "STREAM";
        var guard = ifNotExists ? " IF NOT EXISTS" : "";
        statement = $"CREATE {keyword}{guard} `{name}` ({string.Join(", ", columns)}) WITH ({options});";
        return true;
    }

    public static async Task<KsqlCatalogCreation> CreateAsync(
        HttpClient client, KsqlCatalogKind kind, KsqlCatalogDefinition definition,
        string name, string statement, CancellationToken cancellationToken)
    {
        var runtime = new HttpReadRuntime(client, null, IsRegistered: true);
        var result = await SubmitAsync(runtime, kind, name, statement, cancellationToken).ConfigureAwait(false);
        if (result != KsqlCatalogCreation.Unconfirmed || kind != KsqlCatalogKind.Table ||
            definition.Partitions is not int partitions || definition.Replicas is not int replicas ||
            cancellationToken.IsCancellationRequested ||
            !TryBuildStatement(kind, definition, ifNotExists: true, out _, out var replay))
            return result;

        // ksqlDB can create the topic before it confirms the Table. Verify the topic
        // and its sizing before resuming the DDL in the same request.
        using var recovery = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        recovery.CancelAfter(TimeSpan.FromSeconds(22));
        var token = recovery.Token;
        try
        {
            var assertion = string.Create(CultureInfo.InvariantCulture,
                $"ASSERT TOPIC '{definition.Topic}' WITH (PARTITIONS={partitions}, REPLICAS={replicas}) TIMEOUT 5 SECONDS;");
            var topic = await ExecuteStatementAsync(runtime, assertion, token).ConfigureAwait(false);
            if (StringProperty(topic, "topicName") != definition.Topic ||
                !topic.TryGetProperty("exists", out var exists) || exists.ValueKind != JsonValueKind.True)
                return result;

            var resumed = await SubmitAsync(runtime, kind, name, replay, token).ConfigureAwait(false);
            if (resumed is KsqlCatalogCreation.Denied or KsqlCatalogCreation.Rejected)
                return resumed;

            // IF NOT EXISTS can succeed for an unrelated definition using the same name.
            var description = await ExecuteStatementAsync(runtime, $"DESCRIBE `{name}`;", token).ConfigureAwait(false);
            return description.TryGetProperty("sourceDescription", out var source) &&
                MatchesTable(source, name, definition) ? KsqlCatalogCreation.Created : result;
        }
        catch (Exception ex) when (IsUnconfirmed(ex))
        {
            return result;
        }
    }

    private static async Task<KsqlCatalogCreation> SubmitAsync(HttpReadRuntime runtime, KsqlCatalogKind kind,
        string name, string statement, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        var token = deadline.Token;
        var polling = false;
        try
        {
            var reply = await ExecuteStatementAsync(runtime, statement, token).ConfigureAwait(false);
            if (!reply.TryGetProperty("commandStatus", out var commandStatus))
                return KsqlCatalogCreation.Unconfirmed;

            var status = StringProperty(commandStatus, "status");
            var command = StringProperty(reply, "commandId");
            var resource = kind == KsqlCatalogKind.Table ? "table" : "stream";
            var plainId = $"{resource}/{name}/create";
            var quotedId = $"{resource}/`{name}`/create";
            if (command == plainId || command == quotedId)
            {
                var path = $"status/{resource}/{Uri.EscapeDataString(command == plainId ? name : $"`{name}`")}/create";
                for (var attempt = 0; attempt < 5 && status is ("QUEUED" or "PARSING" or "EXECUTING"); attempt++)
                {
                    polling = true;
                    await Task.Delay(500, token).ConfigureAwait(false);
                    using var poll = new HttpRequestMessage(HttpMethod.Get, path);
                    poll.Headers.Accept.ParseAdd(MediaType);
                    var update = await ReadOnlyHttpSupport.SendJsonAsync(runtime, poll, MaxResponseBytes, token)
                        .ConfigureAwait(false);
                    status = StringProperty(update, "status");
                }
            }

            return status switch
            {
                "SUCCESS" => KsqlCatalogCreation.Created,
                "ERROR" => KsqlCatalogCreation.Rejected,
                _ => KsqlCatalogCreation.Unconfirmed,
            };
        }
        catch (ReadViewHttpException ex) when (!polling && ex.Category == ReadViewFailureCategory.Unauthorized)
        {
            return KsqlCatalogCreation.Denied;
        }
        catch (ReadViewHttpException ex) when (!polling && ex.Code == "upstream_request_failed")
        {
            return KsqlCatalogCreation.Rejected;
        }
        catch (Exception ex) when (IsUnconfirmed(ex))
        {
            return KsqlCatalogCreation.Unconfirmed;
        }
    }

    private static async Task<JsonElement> ExecuteStatementAsync(
        HttpReadRuntime runtime, string statement, CancellationToken token)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "ksql")
        {
            Content = JsonContent.Create(new { ksql = statement, streamsProperties = new Dictionary<string, string>() },
                mediaType: new MediaTypeHeaderValue(MediaType)),
        };
        request.Headers.Accept.ParseAdd(MediaType);
        var reply = await ReadOnlyHttpSupport.SendJsonAsync(runtime, request, MaxResponseBytes, token).ConfigureAwait(false);
        if (reply.ValueKind != JsonValueKind.Array || reply.GetArrayLength() != 1 ||
            reply[0].ValueKind != JsonValueKind.Object)
            throw new JsonException("Expected one ksqlDB result.");
        return reply[0];
    }

    private static bool MatchesTable(JsonElement source, string name, KsqlCatalogDefinition definition)
    {
        if (StringProperty(source, "name") != name || StringProperty(source, "type") != "TABLE" ||
            StringProperty(source, "topic") != definition.Topic || StringProperty(source, "keyFormat") != "KAFKA" ||
            StringProperty(source, "valueFormat") != definition.ValueFormat ||
            !source.TryGetProperty("fields", out var fields) || fields.ValueKind != JsonValueKind.Array ||
            fields.GetArrayLength() != definition.Fields!.Count + 1)
            return false;

        return MatchesField(fields[0], definition.PrimaryKey!, isKey: true) &&
            definition.Fields.Select((field, index) => MatchesField(fields[index + 1], field!, isKey: false)).All(match => match);
    }

    private static bool MatchesField(JsonElement actual, KsqlCatalogField expected, bool isKey) =>
        StringProperty(actual, "name") == expected.Name!.ToUpperInvariant() &&
        (StringProperty(actual, "type") == "KEY") == isKey &&
        actual.TryGetProperty("schema", out var schema) &&
        StringProperty(schema, "type") == (expected.Type == "VARCHAR" ? "STRING" : expected.Type);

    private static bool IsUnconfirmed(Exception exception) =>
        exception is ReadViewHttpException or ResponseBoundExceededException or HttpRequestException or
            JsonException or OperationCanceledException;

    private static string? StringProperty(JsonElement item, string name) =>
        item.ValueKind == JsonValueKind.Object && item.TryGetProperty(name, out var value) &&
        value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    [GeneratedRegex(@"\A[A-Za-z_][A-Za-z0-9_]{0,127}\z", RegexOptions.CultureInvariant)]
    private static partial Regex Identifier();

    [GeneratedRegex(@"\A[A-Za-z0-9._-]{1,249}\z", RegexOptions.CultureInvariant)]
    private static partial Regex TopicName();
}
