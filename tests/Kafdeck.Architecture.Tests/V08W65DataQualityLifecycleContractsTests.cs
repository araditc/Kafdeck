using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W65DataQualityLifecycleContractsTests
{
    [Fact]
    public void Lifecycle_snapshot_requires_positive_revision_and_utc_time()
    {
        var now = DateTimeOffset.UtcNow;
        var snapshot = new DataQualityPolicyLifecycleSnapshot(
            Policy(),
            DataQualityPolicyLifecycleState.Active,
            7,
            now);

        Assert.Equal(7, snapshot.Revision);
        Assert.Equal(DataQualityPolicyLifecycleState.Active, snapshot.State);
        Assert.Equal(TimeSpan.Zero, snapshot.UpdatedAtUtc.Offset);

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityPolicyLifecycleSnapshot(
                Policy(),
                DataQualityPolicyLifecycleState.Active,
                0,
                now));
    }

    [Fact]
    public void Policy_list_query_is_bounded_and_cursor_based()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityPolicyListQuery(
                "prod",
                DataQualityPolicyListQuery.HardMaxResults + 1));

        var query = new DataQualityPolicyListQuery(
            "prod",
            state: DataQualityPolicyLifecycleState.Active,
            afterPolicyId: "orders-a");

        Assert.Equal(
            DataQualityPolicyListQuery.DefaultMaxResults,
            query.MaxResults);
        Assert.Equal("orders-a", query.AfterPolicyId);
    }

    [Fact]
    public void Policy_page_exposes_truncation_and_continuation()
    {
        var now = DateTimeOffset.UtcNow;
        var page = new DataQualityPolicyPage(
            [
                new DataQualityPolicyLifecycleSnapshot(
                    Policy(),
                    DataQualityPolicyLifecycleState.Active,
                    1,
                    now),
            ],
            truncated: true,
            nextPolicyId: "orders-quality");

        Assert.True(page.Truncated);
        Assert.Equal("orders-quality", page.NextPolicyId);

        Assert.Throws<ArgumentException>(
            () => new DataQualityPolicyPage(
                page.Items,
                truncated: false,
                nextPolicyId: "unexpected"));

        Assert.Throws<ArgumentException>(
            () => new DataQualityPolicyPage(
                page.Items,
                truncated: true,
                nextPolicyId: " orders-quality "));

        Assert.Throws<ArgumentException>(
            () => new DataQualityPolicyPage(
                page.Items,
                truncated: true,
                nextPolicyId: "orders-quality\n"));
    }

    [Fact]
    public void Evidence_query_is_time_and_point_bounded()
    {
        var now = DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvidenceQuery(
                "orders-quality",
                now.AddDays(-32),
                now));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityEvidenceQuery(
                "orders-quality",
                now.AddHours(-1),
                now,
                DataQualityEvidenceQuery.HardMaxPoints + 1));
    }

    [Fact]
    public void Durable_evidence_requires_a_validated_evaluation_pair()
    {
        var now = DateTimeOffset.UtcNow;
        var evidence = Evidence(
            now,
            DataQualityEvidencePoint.BoundedEvaluatorSource);
        var progress = Progress(now);

        var point = new DataQualityEvidencePoint(
            new DataQualityEvaluationResult(
                evidence,
                progress));

        Assert.Same(evidence, point.Evidence);
        Assert.Same(progress, point.Progress);

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvaluationResult(
                evidence,
                new DataQualityEvaluationProgress(
                    "other-policy",
                    1,
                    "prod",
                    "orders",
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
                    now)));
    }

    [Fact]
    public void Durable_evidence_rejects_arbitrary_string_sources()
    {
        var now = DateTimeOffset.UtcNow;
        var result = new DataQualityEvaluationResult(
            Evidence(
                now,
                "short-raw-material"),
            Progress(now));

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvidencePoint(result));
    }

    [Fact]
    public void Lifecycle_store_separates_create_from_revision_guarded_replace()
    {
        var methods = typeof(IDataQualityLifecycleStore)
            .GetMethods()
            .ToDictionary(method => method.Name, StringComparer.Ordinal);

        Assert.True(methods.ContainsKey("CreatePolicyAsync"));
        Assert.True(methods.ContainsKey("ReplacePolicyAsync"));
        Assert.False(methods.ContainsKey("PutPolicyAsync"));

        var replace = methods["ReplacePolicyAsync"];
        Assert.Contains(
            replace.GetParameters(),
            parameter =>
                parameter.Name == "expectedRevision" &&
                parameter.ParameterType == typeof(long));
    }

    [Fact]
    public void Lifecycle_contracts_expose_no_raw_record_or_payload_durability()
    {
        var contractTypes = new[]
        {
            typeof(DataQualityPolicyLifecycleSnapshot),
            typeof(DataQualityPolicyPage),
            typeof(DataQualityEvidencePoint),
            typeof(DataQualityEvidencePage),
            typeof(IDataQualityLifecycleStore),
        };

        Assert.All(
            contractTypes.SelectMany(type => type.GetProperties()),
            property =>
            {
                Assert.NotEqual(
                    typeof(KafkaRawRecord),
                    property.PropertyType);
                Assert.DoesNotContain(
                    "Payload",
                    property.Name,
                    StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain(
                    "KeyBytes",
                    property.Name,
                    StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain(
                    "ValueBytes",
                    property.Name,
                    StringComparison.OrdinalIgnoreCase);
            });
    }

    [Fact]
    public void Evidence_page_cannot_exceed_hard_query_ceiling()
    {
        var point = Point();
        var tooMany = Enumerable
            .Repeat(
                point,
                DataQualityEvidenceQuery.HardMaxPoints + 1)
            .ToArray();

        Assert.Throws<ArgumentException>(
            () => new DataQualityEvidencePage(
                tooMany,
                truncated: true));
    }

    private static DataQualityPolicyDefinition Policy() =>
        new(
            "orders-quality",
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

    private static DataQualityAggregateEvidence Evidence(
        DateTimeOffset now,
        string source) =>
        new(
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
            source);

    private static DataQualityEvaluationProgress Progress(
        DateTimeOffset now) =>
        new(
            "orders-quality",
            1,
            "prod",
            "orders",
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

    private static DataQualityEvidencePoint Point()
    {
        var now = DateTimeOffset.UtcNow;
        return new DataQualityEvidencePoint(
            new DataQualityEvaluationResult(
                Evidence(
                    now,
                    DataQualityEvidencePoint.BoundedEvaluatorSource),
                Progress(now)));
    }
}
