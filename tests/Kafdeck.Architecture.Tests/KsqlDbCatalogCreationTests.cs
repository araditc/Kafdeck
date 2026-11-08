using System.Net;
using System.Text;
using System.Text.Json;
using Kafdeck.Infrastructure.Ecosystem;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KsqlDbCatalogCreationTests
{
    private static readonly KsqlCatalogDefinition Definition =
        new("qa_stream", "qa-topic", "JSON", [new("message", "VARCHAR")], 1, 3);

    [Theory]
    [InlineData(KsqlCatalogKind.Stream, "stream", "QA_STREAM",
        "CREATE STREAM `QA_STREAM` (`MESSAGE` VARCHAR) WITH (KAFKA_TOPIC='qa-topic', VALUE_FORMAT='JSON', PARTITIONS=1, REPLICAS=3);")]
    [InlineData(KsqlCatalogKind.Table, "table", "QA_TABLE",
        "CREATE TABLE `QA_TABLE` (`ID` VARCHAR PRIMARY KEY, `MESSAGE` VARCHAR) WITH (KAFKA_TOPIC='qa-topic', VALUE_FORMAT='JSON', KEY_FORMAT='KAFKA', PARTITIONS=1, REPLICAS=3);")]
    public async Task Sends_one_typed_create_and_confirms_a_queued_command(
        KsqlCatalogKind kind, string resource, string expectedName, string expectedSql)
    {
        var input = kind == KsqlCatalogKind.Table
            ? Definition with { Name = "qa_table", PrimaryKey = new("id", "VARCHAR") }
            : Definition;
        Assert.True(KsqlDbCatalogCreator.TryBuildStatement(kind, input, out var name, out var sql));
        Assert.Equal(expectedName, name);
        Assert.Equal(expectedSql, sql);

        var postCount = 0;
        using var client = new HttpClient(new Handler(async (request, token) =>
        {
            if (request.Method == HttpMethod.Post)
            {
                postCount++;
                Assert.Equal("/ksql", request.RequestUri!.AbsolutePath);
                using var body = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(token));
                Assert.Equal(sql, body.RootElement.GetProperty("ksql").GetString());
                return Json($$"""
                    [{"commandId":"{{resource}}/{{expectedName}}/create","commandStatus":{"status":"QUEUED"} }]
                    """);
            }

            Assert.Equal(HttpMethod.Get, request.Method);
            Assert.Equal($"/status/{resource}/{expectedName}/create", request.RequestUri!.AbsolutePath);
            return Json("""{"status":"SUCCESS"}""");
        })) { BaseAddress = new Uri("https://ksql.example/") };

        Assert.Equal(KsqlCatalogCreation.Created,
            await KsqlDbCatalogCreator.CreateAsync(client, kind, input, name, sql, CancellationToken.None));
        Assert.Equal(1, postCount);
    }

    [Fact]
    public void Rejects_statement_injection_and_ambiguous_column_or_topic_definition()
    {
        foreach (var input in new[]
        {
            Definition with { Name = "stream; DROP STREAM x" },
            Definition with { Topic = "topic'--" },
            Definition with { Fields = [new("message", "VARCHAR"), new("MESSAGE", "INTEGER")] },
            Definition with { Fields = [new("message", "VARCHAR) WITH (...)")] },
            Definition with { Replicas = null },
        })
            Assert.False(KsqlDbCatalogCreator.TryBuildStatement(KsqlCatalogKind.Stream, input, out _, out _));

        Assert.True(KsqlDbCatalogCreator.TryBuildStatement(KsqlCatalogKind.Stream,
            Definition with { Partitions = null, Replicas = null }, out _, out var sql));
        Assert.DoesNotContain("PARTITIONS", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true, "qa-topic", KsqlCatalogCreation.Created, 4)]
    [InlineData(true, "other-topic", KsqlCatalogCreation.Unconfirmed, 4)]
    [InlineData(false, "qa-topic", KsqlCatalogCreation.Unconfirmed, 2)]
    public async Task Recovers_an_unconfirmed_table_only_after_verifying_topic_and_table(
        bool topicReady, string describedTopic, KsqlCatalogCreation expected, int expectedCalls)
    {
        var input = Definition with { Name = "qa_table", PrimaryKey = new("id", "VARCHAR") };
        Assert.True(KsqlDbCatalogCreator.TryBuildStatement(KsqlCatalogKind.Table, input, out var name, out var sql));
        var calls = 0;
        using var client = new HttpClient(new Handler(async (request, token) =>
        {
            Assert.Equal(HttpMethod.Post, request.Method);
            using var body = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(token));
            var statement = body.RootElement.GetProperty("ksql").GetString();
            switch (++calls)
            {
                case 1:
                    Assert.Equal(sql, statement);
                    return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
                case 2:
                    Assert.Equal("ASSERT TOPIC 'qa-topic' WITH (PARTITIONS=1, REPLICAS=3) TIMEOUT 5 SECONDS;", statement);
                    return topicReady
                        ? Json("""[{"topicName":"qa-topic","exists":true}]""")
                        : new HttpResponseMessage(HttpStatusCode.BadRequest);
                case 3:
                    Assert.Equal(sql.Replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", StringComparison.Ordinal), statement);
                    return Json("""[{"commandId":"table/QA_TABLE/create","commandStatus":{"status":"SUCCESS"}}]""");
                case 4:
                    Assert.Equal("DESCRIBE `QA_TABLE`;", statement);
                    return Json($$$"""
                        [{"sourceDescription":{"name":"QA_TABLE","type":"TABLE","topic":"{{{describedTopic}}}",
                          "keyFormat":"KAFKA","valueFormat":"JSON","fields":[
                            {"name":"ID","type":"KEY","schema":{"type":"STRING"}},
                            {"name":"MESSAGE","schema":{"type":"STRING"}}
                          ]} }]
                        """);
                default:
                    throw new InvalidOperationException("Unexpected additional ksqlDB request.");
            }
        })) { BaseAddress = new Uri("https://ksql.example/") };

        Assert.Equal(expected,
            await KsqlDbCatalogCreator.CreateAsync(client, KsqlCatalogKind.Table, input, name, sql, CancellationToken.None));
        Assert.Equal(expectedCalls, calls);
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData("message", "VARCHAR")]
    [InlineData("id", "BOOLEAN")]
    public void Rejects_missing_duplicate_or_unsupported_table_key(string? key, string? type)
    {
        var input = Definition with { PrimaryKey = key is null ? null : new(key, type) };
        Assert.False(KsqlDbCatalogCreator.TryBuildStatement(KsqlCatalogKind.Table, input, out _, out _));
    }

    private static HttpResponseMessage Json(string value) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(value, Encoding.UTF8, "application/json"),
    };

    private sealed class Handler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            respond(request, cancellationToken);
    }
}
