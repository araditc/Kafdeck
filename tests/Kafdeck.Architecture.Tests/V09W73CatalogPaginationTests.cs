using System.Globalization;
using System.Net;
using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W73CatalogPaginationTests
{
    [Fact]
    public async Task SQLite_HTTP_pages_and_capacity_filter_before_pagination_and_cannot_move_topic_history()
    {
        var directory = Path.Combine(Path.GetTempPath(), "w73-page-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        try { await ExerciseAsync(new SqliteMutationDbConnectionFactory(Path.Combine(directory, "catalog.db"))); }
        finally { SqliteConnection.ClearAllPools(); Directory.Delete(directory, true); }
    }
    [CatalogPostgreSqlFact]
    public async Task PostgreSQL_HTTP_pages_and_capacity_filter_before_pagination_and_cannot_move_topic_history()
    { await using var database = await G2CliPostgreSqlDatabase.CreateAsync(); await ExerciseAsync(new PostgreSqlMutationDbConnectionFactory(database.ConnectionString)); }

    private static async Task ExerciseAsync(IMutationDbConnectionFactory factory)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2)); var token = deadline.Token;
        var generation = Guid.NewGuid(); var gate = new AdoGovernanceGenerationGate(factory, generation); await gate.InitializeAsync(token);
        var store = new AdoCatalogStore(factory, generation.ToString("D"), gate); await store.InitializeAsync(token);
        var teams = Enumerable.Range(1, 5).Select(n => new CatalogPlannedChange(new(CatalogEntityKind.Team, null, 0, "Team " + n), Id(n), null)).ToArray();
        await Commit(store, "alpha", teams, token);
        var appId = Id(6);
        for (var revision = 0; revision < 4; revision++)
            await Commit(store, "alpha", [new(new(CatalogEntityKind.Application, revision == 0 ? null : appId, revision, "App", TeamId: Id(revision + 1)), appId, null)], token);
        var topic = new CatalogPlannedChange(new(CatalogEntityKind.Topic, null, 0, "Private", TopicName: "private-old"), Id(7), CatalogContract.Hash("old-physical-identity"));
        await Commit(store, "alpha", [topic], token);

        var visible = await store.ListAuthorizedAsync("alpha", CatalogEntityKind.Team, null, 2, row => row.Id == Id(2) || row.Id == Id(4), token);
        Assert.Equal(new[] { Id(2), Id(4) }, visible.Select(row => row.Id));
        Assert.Empty(await store.ListAuthorizedAsync("alpha", CatalogEntityKind.Team, null, 2, _ => false, token));
        var historic = await store.HistoryAuthorizedAsync("alpha", CatalogEntityKind.Application, appId, 0, 2, row => row.TeamId == Id(2) || row.TeamId == Id(4), token);
        Assert.Equal(new long[] { 2, 4 }, historic.Select(row => row.Record.Revision));
        Assert.Empty(await store.HistoryAuthorizedAsync("alpha", CatalogEntityKind.Application, appId, 0, 2, _ => false, token));

        // Real HTTP/socket + real databases, controlled authenticated-operator fixture (not genuine OIDC proof).
        var options = new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0", null, AccessMode.Oidc, null), []);
        var reader = new AuthorizationRoleDefinition("reader", [new(AuthorizationAction.CatalogRead, ["alpha"], ["catalog:team:" + Id(2), "catalog:team:" + Id(4), "catalog:application:" + appId, "allowed-new"])]);
        var single = new AuthorizationRoleDefinition("single", [new(AuthorizationAction.CatalogRead, ["alpha"], ["catalog:team:" + Id(2)])]);
        var none = new AuthorizationRoleDefinition("none", [new(AuthorizationAction.CatalogRead, ["alpha"], ["catalog:team:never-matches"])]);
        const string issuer = "https://pagination-fixture.invalid";
        var policy = new AuthorizationPolicyDefinition([reader, single, none], [new(issuer, "reader", ["reader"]), new(issuer, "other-reader", ["reader"]), new(issuer, "single", ["single"]), new(issuer, "none", ["none"])], []);
        var builder = WebApplication.CreateBuilder(); builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddSingleton(options); builder.Services.AddSingleton<ICatalogStore>(store); builder.Services.AddSingleton(store);
        builder.Services.AddSingleton<IMutationDbConnectionFactory>(factory);
        builder.Services.AddSingleton(new KafdeckAuthorizationService(options, new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(policy))));
        using var digest = new HmacMutationMaterialDigestService(new string('a', 64)); builder.Services.AddSingleton<IMutationMaterialDigestService>(digest);
        builder.Services.AddSingleton<ICatalogTopicIdentityPort>(new NoTopicRead());
        builder.Services.AddSingleton<MutationAdmissionService>(_ => throw new InvalidOperationException("Read-only HTTP fixture cannot admit mutations."));
        builder.Services.AddSingleton<IMutationOperationRepository>(_ => throw new InvalidOperationException("Read-only HTTP fixture cannot access mutation operations."));
        builder.Services.AddSingleton<IMutationAuditSink>(_ => throw new InvalidOperationException("Read-only HTTP fixture cannot mutate audit."));
        builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:0");
        await using (var app = builder.Build())
        {
            app.UseRouting(); app.Use((context, next) =>
            {
                var subject = context.Request.Headers["X-Fixture-Subject"].FirstOrDefault() ?? "reader";
                context.User = OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", subject)], "oidc")), issuer, null, DateTimeOffset.UtcNow);
                return next(context);
            });
            app.UseAntiforgery(); app.MapKafdeckCatalog(); await app.StartAsync(token);
            using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false }) { BaseAddress = new Uri(Assert.Single(app.Urls)) };
            using (var oldHistory = await client.GetAsync("/api/v1/governance/catalog/topics/" + topic.Id + "/history?clusterId=alpha", token))
                Assert.Equal(HttpStatusCode.NotFound, oldHistory.StatusCode);
            const string route = "/api/v1/governance/catalog/teams?clusterId=alpha&pageSize=1";
            using var first = JsonDocument.Parse(await client.GetStringAsync(route, token));
            Assert.Equal(Id(2), first.RootElement.GetProperty("items")[0].GetProperty("id").GetString());
            Assert.Single(first.RootElement.GetProperty("items").EnumerateArray());
            var cursor = first.RootElement.GetProperty("nextCursor").GetString(); Assert.NotNull(cursor);
            using var repeat = JsonDocument.Parse(await client.GetStringAsync(route, token)); Assert.Equal(cursor, repeat.RootElement.GetProperty("nextCursor").GetString());
            using var second = JsonDocument.Parse(await client.GetStringAsync(route + "&cursor=" + Uri.EscapeDataString(cursor), token));
            Assert.Equal(Id(4), second.RootElement.GetProperty("items")[0].GetProperty("id").GetString());
            Assert.Equal(JsonValueKind.Null, second.RootElement.GetProperty("nextCursor").ValueKind);
            foreach (var hidden in new[] { Id(1), Id(3), Id(5) }) { Assert.DoesNotContain(hidden, first.RootElement.GetRawText()); Assert.DoesNotContain(hidden, second.RootElement.GetRawText()); }
            client.DefaultRequestHeaders.Add("X-Fixture-Subject", "other-reader");
            using (var crossActor = await client.GetAsync(route + "&cursor=" + Uri.EscapeDataString(cursor), token)) Assert.Equal(HttpStatusCode.BadRequest, crossActor.StatusCode);
            client.DefaultRequestHeaders.Remove("X-Fixture-Subject"); client.DefaultRequestHeaders.Add("X-Fixture-Subject", "single");
            using (var one = JsonDocument.Parse(await client.GetStringAsync(route, token)))
            { Assert.Single(one.RootElement.GetProperty("items").EnumerateArray()); Assert.Equal(JsonValueKind.Null, one.RootElement.GetProperty("nextCursor").ValueKind); }
            client.DefaultRequestHeaders.Remove("X-Fixture-Subject"); client.DefaultRequestHeaders.Add("X-Fixture-Subject", "none");
            using (var zero = JsonDocument.Parse(await client.GetStringAsync(route, token)))
            { Assert.Empty(zero.RootElement.GetProperty("items").EnumerateArray()); Assert.Equal(JsonValueKind.Null, zero.RootElement.GetProperty("nextCursor").ValueKind); }
            client.DefaultRequestHeaders.Remove("X-Fixture-Subject");
            var historyRoute = "/api/v1/governance/catalog/applications/" + appId + "/history?clusterId=alpha&pageSize=1";
            using var h1 = JsonDocument.Parse(await client.GetStringAsync(historyRoute, token));
            Assert.Equal(2, h1.RootElement.GetProperty("items")[0].GetProperty("record").GetProperty("revision").GetInt64());
            using var h2 = JsonDocument.Parse(await client.GetStringAsync(historyRoute + "&cursor=" + Uri.EscapeDataString(h1.RootElement.GetProperty("nextCursor").GetString()!), token));
            Assert.Equal(4, h2.RootElement.GetProperty("items")[0].GetProperty("record").GetProperty("revision").GetInt64()); Assert.Equal(JsonValueKind.Null, h2.RootElement.GetProperty("nextCursor").ValueKind);
            await app.StopAsync(token);
        }
        var moved = MakePlan(store, "alpha", [topic with { Change = topic.Change with { Id = topic.Id, ExpectedRevision = 1, TopicName = "allowed-new", RebindTopicIdentity = true }, TopicIdentity = CatalogContract.Hash("new-physical-identity") }]);
        Assert.Equal("CatalogTopicIdentityChanged", (await Assert.ThrowsAsync<CatalogConflictException>(() => store.StageAsync(moved, token))).Code);
        Assert.NotNull(await store.FindTopicAsync("alpha", "private-old", token)); Assert.Null(await store.FindTopicAsync("alpha", "allowed-new", token));
        Assert.Single(await store.HistoryAsync("alpha", CatalogEntityKind.Topic, topic.Id, 0, 200, token));
        await VerifyCapacity(factory, store, token);
    }

    private static async Task VerifyCapacity(IMutationDbConnectionFactory factory, AdoCatalogStore store, CancellationToken token)
    {
        // Bulk fixture seed establishes the public boundary; it is not product mutation/approval evidence.
        var stamp = DateTimeOffset.UtcNow; var placeholder = Id(1);
        var template = new CatalogRecord(CatalogEntityKind.Team, placeholder, "quota", 1, "Capacity fixture", null, false, null, null, null, null, null, null, null, null, [], new string('a', 64), stamp, stamp);
        var source = factory.SupportsSelectForUpdate ? "WITH source AS (SELECT generate_series(1,@count) AS n) " : "WITH RECURSIVE source(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM source WHERE n<@count) ";
        var id = factory.SupportsSelectForUpdate ? "lpad(to_hex(n),32,'0')" : "printf('%032x',n)";
        await Execute(factory, source + "INSERT INTO catalog_owned_entities(cluster_id,kind,id,revision,topic_name,body) SELECT 'quota',1," + id + ",1,NULL,replace(@body,@placeholder," + id + ") FROM source", token,
            ("@count", CatalogContract.MaxEntitiesPerScope - 1), ("@body", JsonSerializer.Serialize(template, CatalogContract.Json)), ("@placeholder", placeholder));
        var a = MakePlan(store, "quota", [new(new(CatalogEntityKind.Team, null, 0, "Last slot A"), Id(10_000), null)]);
        var b = MakePlan(store, "quota", [new(new(CatalogEntityKind.Team, null, 0, "Last slot B"), Id(10_001), null)]);
        await store.StageAsync(a, token); await store.StageAsync(b, token);
        async Task<bool> CommitOne(CatalogPlan plan)
        { try { return await store.CommitAsync(plan, () => true, token) is not null; } catch (CatalogConflictException ex) { Assert.Equal("CatalogCapacityExceeded", ex.Code); return false; } }
        Assert.Single(await Task.WhenAll(Task.Run(() => CommitOne(a), token), Task.Run(() => CommitOne(b), token)), result => result);
        var excess = MakePlan(store, "quota", [new(new(CatalogEntityKind.Team, null, 0, "Beyond capacity"), Id(10_002), null)]);
        Assert.Equal("CatalogCapacityExceeded", (await Assert.ThrowsAsync<CatalogConflictException>(() => store.StageAsync(excess, token))).Code);
        Assert.Empty(await store.ListAuthorizedAsync("quota", CatalogEntityKind.Team, null, 2, _ => false, token));
        await Commit(store, "history-quota", [new(new(CatalogEntityKind.Team, null, 0, "History capacity fixture"), Id(1), null)], token);
        var original = (await store.GetAsync("history-quota", CatalogEntityKind.Team, Id(1), token))!;
        var historyStamp = DateTimeOffset.UtcNow;
        var historic = original with { UpdatedAtUtc = historyStamp };
        await Execute(factory, source + "INSERT INTO catalog_owned_history(cluster_id,kind,id,revision,body,actor_fingerprint,authority_fingerprint,operation_id,effective_from_utc,effective_to_utc) " +
            "SELECT 'history-quota',1,@id,n,replace(@body,@marker,'\"revision\":'||CAST(n AS TEXT)),@actor,@authority,@operation,@stamp,CASE WHEN n=@count THEN NULL ELSE @stamp END FROM source WHERE n>1", token,
            ("@count", CatalogContract.MaxRevisionsPerEntity), ("@id", Id(1)), ("@body", JsonSerializer.Serialize(historic, CatalogContract.Json)), ("@marker", "\"revision\":1"),
            ("@actor", new string('a', 64)), ("@authority", new string('b', 64)), ("@operation", Guid.NewGuid().ToString("D")), ("@stamp", historyStamp.ToString("O", CultureInfo.InvariantCulture)));
        await Execute(factory, "UPDATE catalog_owned_history SET effective_to_utc=@stamp WHERE cluster_id='history-quota' AND kind=1 AND id=@id AND revision=1; " +
            "UPDATE catalog_owned_entities SET revision=@revision,body=@body WHERE cluster_id='history-quota' AND kind=1 AND id=@id", token,
            ("@stamp", historyStamp.ToString("O", CultureInfo.InvariantCulture)), ("@id", Id(1)), ("@revision", CatalogContract.MaxRevisionsPerEntity),
            ("@body", JsonSerializer.Serialize(historic with { Revision = CatalogContract.MaxRevisionsPerEntity }, CatalogContract.Json)));
        var beyondHistory = MakePlan(store, "history-quota", [new(new(CatalogEntityKind.Team, Id(1), CatalogContract.MaxRevisionsPerEntity, "Do not truncate immutable history"), Id(1), null)]);
        Assert.Equal("CatalogHistoryCapacityExceeded", (await Assert.ThrowsAsync<CatalogConflictException>(() => store.StageAsync(beyondHistory, token))).Code);
        Assert.Null(await store.GetPlanAsync(beyondHistory.PlanId, token));
        Assert.Equal(CatalogContract.MaxRevisionsPerEntity, (await store.GetAsync("history-quota", CatalogEntityKind.Team, Id(1), token))!.Revision);
    }
    private static string Id(int value) => value.ToString("x32", CultureInfo.InvariantCulture);
    private static async Task Commit(AdoCatalogStore store, string cluster, CatalogPlannedChange[] changes, CancellationToken token)
    { var plan = MakePlan(store, cluster, changes); await store.StageAsync(plan, token); Assert.NotNull(await store.CommitAsync(plan, () => true, token)); }
    private static CatalogPlan MakePlan(ICatalogStore store, string cluster, IReadOnlyList<CatalogPlannedChange> changes)
    { var normalized = changes.Select(item => item with { Change = CatalogContract.Validate(item.Change) }).ToArray(); var now = DateTimeOffset.UtcNow; return new(Guid.NewGuid(), cluster, "oidc:https://pagination-fixture.invalid|owner", new string('a', 64), store.DeploymentGeneration, CatalogContract.MaterialHash(cluster, normalized), normalized, now, now.AddMinutes(5)); }
    private static async Task Execute(IMutationDbConnectionFactory factory, string sql, CancellationToken token, params (string Name, object Value)[] parameters)
    { await using var db = await factory.OpenAsync(token); await using var command = db.CreateCommand(); command.CommandText = sql; foreach (var (name, value) in parameters) { var p = command.CreateParameter(); p.ParameterName = name; p.Value = value; command.Parameters.Add(p); } await command.ExecuteNonQueryAsync(token); }
    private sealed class NoTopicRead : ICatalogTopicIdentityPort
    { public Task<CatalogTopicIdentity?> ObserveAsync(string clusterId, string topicName, CancellationToken token) => throw new InvalidOperationException("Teams/application paging must not query Kafka."); }
}
