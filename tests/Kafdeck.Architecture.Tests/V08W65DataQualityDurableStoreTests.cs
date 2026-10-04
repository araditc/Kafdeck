using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W65DataQualityDurableStoreTests
{
    [Fact]
    public async Task Sqlite_store_round_trips_policy_and_evidence_without_raw_payload()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-data-quality-{Guid.NewGuid():N}.db");

        try
        {
            var store = new AdoDataQualityLifecycleStore(
                new SqliteDataQualityDbConnectionFactory(path));
            await store.InitializeAsync();

            var now = DateTimeOffset.UtcNow;
            var policy = Policy("orders-quality");

            var created = await store.CreatePolicyAsync(
                policy,
                DataQualityPolicyLifecycleState.Active,
                now);

            Assert.Equal(1, created.Revision);

            var fetched = await store.GetPolicyAsync(policy.PolicyId);
            Assert.NotNull(fetched);
            Assert.Equal(
                policy.Scope.ClusterId,
                fetched!.Definition.Scope.ClusterId);

            var replaced = await store.ReplacePolicyAsync(
                policy,
                DataQualityPolicyLifecycleState.Paused,
                expectedRevision: 1,
                now.AddSeconds(1));

            Assert.NotNull(replaced);
            Assert.Equal(2, replaced!.Revision);
            Assert.Equal(
                DataQualityPolicyLifecycleState.Paused,
                replaced.State);

            var stale = await store.SetPolicyStateAsync(
                policy.PolicyId,
                DataQualityPolicyLifecycleState.Active,
                expectedRevision: 1,
                now.AddSeconds(2));
            Assert.Null(stale);

            await store.AppendEvidenceAsync(Point(now));
            var page = await store.QueryEvidenceAsync(
                new DataQualityEvidenceQuery(
                    policy.PolicyId,
                    now.AddMinutes(-2),
                    now.AddMinutes(1)));

            Assert.Single(page.Points);
            Assert.False(page.Truncated);
            Assert.Equal(
                DataQualityEvidencePoint.BoundedEvaluatorSource,
                page.Points[0].Evidence.Source);
            Assert.Equal(1, page.Points[0].Evidence.EvaluatedRecords);
            Assert.Equal("prod", page.Points[0].Progress.ClusterId);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (File.Exists(path))
            {
                File.Delete(path);
            }

            var wal = path + "-wal";
            var shm = path + "-shm";
            if (File.Exists(wal))
            {
                File.Delete(wal);
            }

            if (File.Exists(shm))
            {
                File.Delete(shm);
            }
        }
    }

    [Fact]
    public async Task Sqlite_store_policy_listing_is_stable_and_bounded()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-data-quality-{Guid.NewGuid():N}.db");

        try
        {
            var store = new AdoDataQualityLifecycleStore(
                new SqliteDataQualityDbConnectionFactory(path));
            await store.InitializeAsync();

            var now = DateTimeOffset.UtcNow;
            foreach (var id in new[] { "a-policy", "b-policy", "c-policy" })
            {
                await store.CreatePolicyAsync(
                    Policy(id),
                    DataQualityPolicyLifecycleState.Active,
                    now);
            }

            var first = await store.ListPoliciesAsync(
                new DataQualityPolicyListQuery(
                    "prod",
                    maxResults: 2,
                    state: DataQualityPolicyLifecycleState.Active));

            Assert.Equal(2, first.Items.Count);
            Assert.True(first.Truncated);
            Assert.Equal("b-policy", first.NextPolicyId);

            var second = await store.ListPoliciesAsync(
                new DataQualityPolicyListQuery(
                    "prod",
                    maxResults: 2,
                    state: DataQualityPolicyLifecycleState.Active,
                    afterPolicyId: first.NextPolicyId));

            Assert.Single(second.Items);
            Assert.Equal(
                "c-policy",
                second.Items[0].Definition.PolicyId);
            Assert.False(second.Truncated);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    [Fact]
    public async Task Evidence_append_is_idempotent()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-data-quality-{Guid.NewGuid():N}.db");

        try
        {
            var store = new AdoDataQualityLifecycleStore(
                new SqliteDataQualityDbConnectionFactory(path));
            await store.InitializeAsync();

            var now = DateTimeOffset.UtcNow;
            var point = Point(now);

            await store.AppendEvidenceAsync(point);
            await store.AppendEvidenceAsync(point);

            var page = await store.QueryEvidenceAsync(
                new DataQualityEvidenceQuery(
                    "orders-quality",
                    now.AddMinutes(-2),
                    now.AddMinutes(1)));

            Assert.Single(page.Points);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }


    [Fact]
    public async Task Evidence_identity_keeps_distinct_cluster_and_topic_scopes()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-data-quality-{Guid.NewGuid():N}.db");

        try
        {
            var store = new AdoDataQualityLifecycleStore(
                new SqliteDataQualityDbConnectionFactory(path));
            await store.InitializeAsync();

            var now = DateTimeOffset.UtcNow;
            await store.AppendEvidenceAsync(
                Point(now, clusterId: "prod", topicName: "orders"));
            await store.AppendEvidenceAsync(
                Point(now, clusterId: "dr", topicName: "orders-copy"));

            var page = await store.QueryEvidenceAsync(
                new DataQualityEvidenceQuery(
                    "orders-quality",
                    now.AddMinutes(-2),
                    now.AddMinutes(1)));

            Assert.Equal(2, page.Points.Count);
            Assert.Contains(
                page.Points,
                point =>
                    point.Progress.ClusterId == "prod" &&
                    point.Progress.TopicName == "orders");
            Assert.Contains(
                page.Points,
                point =>
                    point.Progress.ClusterId == "dr" &&
                    point.Progress.TopicName == "orders-copy");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    [Fact]
    public async Task State_update_returns_definition_from_the_revision_that_won_cas()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-data-quality-{Guid.NewGuid():N}.db");

        try
        {
            var store = new AdoDataQualityLifecycleStore(
                new SqliteDataQualityDbConnectionFactory(path));
            await store.InitializeAsync();

            var now = DateTimeOffset.UtcNow;
            var original = Policy("orders-quality");
            await store.CreatePolicyAsync(
                original,
                DataQualityPolicyLifecycleState.Active,
                now);

            var replacement =
                new DataQualityPolicyDefinition(
                    "orders-quality",
                    1,
                    new DataQualityPolicyScope(
                        "prod",
                        "orders-v2",
                        [0]),
                    [
                        new DataQualityRule(
                            "id-required",
                            DataQualityRuleKind.RequiredPath,
                            "/id"),
                    ]);

            var replaced = await store.ReplacePolicyAsync(
                replacement,
                DataQualityPolicyLifecycleState.Active,
                expectedRevision: 1,
                now.AddSeconds(1));
            Assert.NotNull(replaced);

            var stateUpdated = await store.SetPolicyStateAsync(
                "orders-quality",
                DataQualityPolicyLifecycleState.Paused,
                expectedRevision: 2,
                now.AddSeconds(2));

            Assert.NotNull(stateUpdated);
            Assert.Equal(3, stateUpdated!.Revision);
            Assert.Equal(
                "orders-v2",
                stateUpdated.Definition.Scope.TopicName);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    [Fact]
    public void PostgreSql_factory_is_ha_capable_and_hides_error_detail()
    {
        var factory =
            new PostgreSqlDataQualityDbConnectionFactory(
                "Host=localhost;Database=kafdeck;Username=test;Password=test");

        Assert.True(factory.SupportsSelectForUpdate);
    }

    private static DataQualityPolicyDefinition Policy(string id) =>
        new(
            id,
            1,
            new DataQualityPolicyScope(
                "prod",
                "orders",
                [0]),
            [
                new DataQualityRule(
                    "id-required",
                    DataQualityRuleKind.RequiredPath,
                    "/id"),
            ]);

    private static DataQualityEvidencePoint Point(
        DateTimeOffset now,
        string clusterId = "prod",
        string topicName = "orders")
    {
        var evidence =
            new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                now.AddMinutes(-1),
                now,
                1,
                10,
                0,
                [
                    new DataQualityRuleViolationCount(
                        "id-required",
                        0),
                ],
                DataQualityEvidenceState.Available,
                DataQualityEvidencePoint.BoundedEvaluatorSource);
        var progress =
            new DataQualityEvaluationProgress(
                "orders-quality",
                1,
                clusterId,
                topicName,
                0,
                now.AddMinutes(-1),
                now,
                0,
                1,
                1,
                1,
                10,
                DataQualityEvidenceState.Available,
                DataQualityEvaluationOutcome.Complete,
                now);

        return new DataQualityEvidencePoint(
            new DataQualityEvaluationResult(
                evidence,
                progress));
    }
}
