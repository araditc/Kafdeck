using Kafdeck.Core.Catalog;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class CatalogPostgreSqlFactAttribute : FactAttribute
{
    public CatalogPostgreSqlFactAttribute()
    { if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES"))) Skip = "Requires actual PostgreSQL for W73 catalog concurrency/restart/restore evidence."; }
}

public sealed class V09W73CatalogPersistenceTests
{
    [Fact]
    public async Task Sqlite_atomic_catalog_history_import_restart_concurrency_and_restore_fence()
    {
        var directory = Path.Combine(Path.GetTempPath(), "kafdeck-w73-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        try { await ExerciseAsync(new SqliteMutationDbConnectionFactory(Path.Combine(directory, "catalog.db"))); }
        finally { SqliteConnection.ClearAllPools(); Directory.Delete(directory, true); }
    }
    [CatalogPostgreSqlFact]
    public async Task PostgreSql_atomic_catalog_history_import_restart_concurrency_and_restore_fence()
    {
        await using var database = await G2CliPostgreSqlDatabase.CreateAsync();
        await ExerciseAsync(new PostgreSqlMutationDbConnectionFactory(database.ConnectionString));
    }

    private static async Task ExerciseAsync(IMutationDbConnectionFactory connections)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2)); var token = deadline.Token;
        var generation = Guid.NewGuid(); var gate = new AdoGovernanceGenerationGate(connections, generation);
        await gate.InitializeAsync(token);
        var a = new AdoCatalogStore(connections, generation.ToString("D"), gate);
        var b = new AdoCatalogStore(connections, generation.ToString("D"), gate);
        await a.InitializeAsync(token); await b.InitializeAsync(token);
        var team = new CatalogPlannedChange(new(CatalogEntityKind.Team, null, 0, "Payments"), Guid.NewGuid().ToString("N"), null);
        var created = Plan(a, [team]); await a.StageAsync(created, token);
        var receipt = await a.CommitAsync(created, () => true, token); Assert.NotNull(receipt); Assert.Single(receipt.Records);
        Assert.Equal(1, (await b.GetAsync("alpha", CatalogEntityKind.Team, team.Id, token))!.Revision);
        Assert.Equal(receipt.ContentHash, (await b.GetReceiptAsync(created.PlanId, token))!.ContentHash);
        var replay = await b.CommitAsync(created, () => true, token); Assert.Equal(receipt.ContentHash, replay!.ContentHash);
        Assert.Single(await b.HistoryAsync("alpha", CatalogEntityKind.Team, team.Id, 0, 200, token));

        var app = new CatalogPlannedChange(new(CatalogEntityKind.Application, null, 0, "Checkout", TeamId: team.Id), Guid.NewGuid().ToString("N"), null);
        var topic = new CatalogPlannedChange(new(CatalogEntityKind.Topic, null, 0, "Orders", TeamId: team.Id, ApplicationId: app.Id, TopicName: "orders", Owner: "descriptive-owner", Classification: "internal", Tags: ["sales"]), Guid.NewGuid().ToString("N"), CatalogContract.Hash("physical-topic-a"));
        var import = Plan(a, [app, topic]); await a.StageAsync(import, token);
        Assert.NotNull(await b.CommitAsync(import, () => true, token));
        var owned = await b.FindTopicAsync("alpha", "orders", token); Assert.NotNull(owned); Assert.Equal(team.Id, owned.TeamId);
        Assert.Null(await b.FindTopicAsync("beta", "orders", token));
        await using (var db = await connections.OpenAsync(token))
        await using (var tx = await db.BeginTransactionAsync(token))
        {
            var metadata = await a.ReadAsync(db, tx, "alpha", "orders", token);
            Assert.Equal("internal", metadata!["classification"]); Assert.Equal(app.Id, metadata["applicationId"]);
        }

        // Same expected revision from independent provider instances has exactly one winner.
        var edit1 = Plan(a, [new(new(CatalogEntityKind.Team, team.Id, 1, "Payments A"), team.Id, null)]);
        var edit2 = Plan(a, [new(new(CatalogEntityKind.Team, team.Id, 1, "Payments B"), team.Id, null)]);
        await a.StageAsync(edit1, token); await b.StageAsync(edit2, token);
        async Task<bool> Commit(AdoCatalogStore store, CatalogPlan plan)
        { try { return await store.CommitAsync(plan, () => true, token) is not null; } catch (CatalogConflictException) { return false; } }
        var race = await Task.WhenAll(Commit(a, edit1), Commit(b, edit2)); Assert.Single(race, v => v);
        var history = await b.HistoryAsync("alpha", CatalogEntityKind.Team, team.Id, 0, 200, token);
        Assert.Equal(2, history.Count); Assert.NotNull(history[0].EffectiveToUtc); Assert.Null(history[1].EffectiveToUtc);
        Assert.Equal(history[0].EffectiveToUtc, history[1].EffectiveFromUtc);
        Assert.All(history, row => Assert.Equal(CatalogContract.Hash("oidc:https://idp.example|requester"), row.ActorFingerprint));

        // A late authority failure rolls back all rows, audit and receipt together.
        var changedApp = new CatalogPlannedChange(app.Change with { Id = app.Id, ExpectedRevision = 1, Name = "Updated" }, app.Id, null);
        var changedTopic = new CatalogPlannedChange(topic.Change with { Id = topic.Id, ExpectedRevision = 1, Description = "updated" }, topic.Id, topic.TopicIdentity);
        var late = Plan(a, [changedApp, changedTopic]); await a.StageAsync(late, token); var calls = 0;
        Assert.Null(await a.CommitAsync(late, () => ++calls == 1, token));
        Assert.Equal(1, (await b.GetAsync("alpha", CatalogEntityKind.Application, app.Id, token))!.Revision);
        Assert.Equal(1, (await b.GetAsync("alpha", CatalogEntityKind.Topic, topic.Id, token))!.Revision);
        Assert.Null(await b.GetReceiptAsync(late.PlanId, token));
        Assert.Single(await b.HistoryAsync("alpha", CatalogEntityKind.Application, app.Id, 0, 200, token));

        var invalidReference = Plan(a, [new(new(CatalogEntityKind.Application, null, 0, "No owner", TeamId: Guid.NewGuid().ToString("N")), Guid.NewGuid().ToString("N"), null)]);
        await Assert.ThrowsAsync<CatalogConflictException>(() => a.StageAsync(invalidReference, token));
        var archiveReferenced = Plan(a, [new(new(CatalogEntityKind.Team, team.Id, 2, "Payments", Archived: true), team.Id, null)]);
        await Assert.ThrowsAsync<CatalogConflictException>(() => a.StageAsync(archiveReferenced, token));

        var recreated = Plan(a, [changedTopic with { TopicIdentity = CatalogContract.Hash("physical-topic-b") }]);
        await Assert.ThrowsAsync<CatalogConflictException>(() => a.StageAsync(recreated, token));
        var explicitRebind = Plan(a, [changedTopic with { Change = changedTopic.Change with { RebindTopicIdentity = true, Owner = null, TeamId = null, ApplicationId = null, Classification = "restricted" }, TopicIdentity = CatalogContract.Hash("physical-topic-b") }]);
        await a.StageAsync(explicitRebind, token); Assert.NotNull(await b.CommitAsync(explicitRebind, () => true, token));
        Assert.Null((await a.FindTopicAsync("alpha", "orders", token))!.Owner);

        // A new independent store sees committed evidence and the immutable page order.
        var restarted = new AdoCatalogStore(connections, generation.ToString("D"), gate); await restarted.InitializeAsync(token);
        var page = await restarted.ListAsync("alpha", CatalogEntityKind.Team, null, 1, token); Assert.Single(page);
        Assert.Empty(await restarted.ListAsync("alpha", CatalogEntityKind.Team, page[0].Id, 1, token));
        Assert.Equal(receipt.ContentHash, (await restarted.GetReceiptAsync(created.PlanId, token))!.ContentHash);

        var pending = Plan(a, [new(new(CatalogEntityKind.Team, null, 0, "Fenced"), Guid.NewGuid().ToString("N"), null)]); await a.StageAsync(pending, token);
        // Simulate the externally admitted post-restore generation transition on the actual durable row.
        await using (var db = await connections.OpenAsync(token))
        await using (var command = db.CreateCommand())
        { command.CommandText = "UPDATE governance_deployment_generation SET generation=@next,revision=revision+1 WHERE singleton_id=1"; var parameter = command.CreateParameter(); parameter.ParameterName = "@next"; parameter.Value = Guid.NewGuid().ToString("D"); command.Parameters.Add(parameter); Assert.Equal(1, await command.ExecuteNonQueryAsync(token)); }
        await Assert.ThrowsAsync<GovernanceGenerationException>(() => a.CommitAsync(pending, () => true, token));
    }
    private static CatalogPlan Plan(ICatalogStore store, IReadOnlyList<CatalogPlannedChange> changes)
    {
        var normalized = changes.Select(i => i with { Change = CatalogContract.Validate(i.Change) }).ToArray(); var now = DateTimeOffset.UtcNow;
        return new(Guid.NewGuid(), "alpha", "oidc:https://idp.example|requester", new string('a', 64), store.DeploymentGeneration,
            CatalogContract.MaterialHash("alpha", normalized), normalized, now, now.AddMinutes(5));
    }

    [Theory]
    [InlineData("javascript:alert(1)")]
    [InlineData("http://insecure.example")]
    [InlineData("https://user:password@example.org")]
    public void Unsafe_documentation_is_never_fetched_or_accepted(string reference) =>
        Assert.Throws<ArgumentException>(() => CatalogContract.Validate(new(CatalogEntityKind.Team, null, 0, "Team", DocumentationReference: reference)));

    [Fact]
    public void Catalog_rejects_invalid_ids_scopes_duplicate_tags_and_executable_material()
    {
        Assert.Throws<ArgumentException>(() => CatalogContract.Validate(new(CatalogEntityKind.Team, "client-invented-id", 1, "Team")));
        Assert.Throws<ArgumentException>(() => CatalogContract.Validate(new(CatalogEntityKind.Team, null, 1, "Team")));
        Assert.Throws<ArgumentException>(() => CatalogContract.Validate(new(CatalogEntityKind.Team, null, 0, "Team\nrole=admin")));
        Assert.Throws<ArgumentException>(() => CatalogContract.Validate(new(CatalogEntityKind.Team, null, 0, "Team", Tags: ["same", "same"])));
        Assert.Throws<ArgumentException>(() => CatalogContract.Validate(new(CatalogEntityKind.Topic, null, 0, "Topic", TopicName: "../secret")));
        Assert.Throws<System.Text.Json.JsonException>(() => System.Text.Json.JsonSerializer.Deserialize<CatalogChange>("{\"kind\":\"Team\",\"id\":null,\"expectedRevision\":0,\"name\":\"Team\",\"role\":\"admin\"}", CatalogContract.Json));
    }
}
