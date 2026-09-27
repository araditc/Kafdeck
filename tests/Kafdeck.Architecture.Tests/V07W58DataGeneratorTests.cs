using System.Text;
using System.Text.Json;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Generator;
using Kafdeck.Modules.Schemas;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W58DataGeneratorTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 27, 10, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Plan_fingerprint_seed_and_authorization_are_deterministic()
    {
        var source = BuiltInSource();
        var destination = Destination();
        var budget = new DataGeneratorBudget();

        var first = DataGeneratorPolicy.CreatePlan(
            destination,
            source,
            recordCount: 100,
            seed: 42,
            budget);
        var second = DataGeneratorPolicy.CreatePlan(
            destination,
            source,
            recordCount: 100,
            seed: 42,
            budget);
        var changedSeed = DataGeneratorPolicy.CreatePlan(
            destination,
            source,
            recordCount: 100,
            seed: 43,
            budget);

        Assert.Equal(
            first.PlanFingerprint,
            second.PlanFingerprint);
        Assert.NotEqual(
            first.PlanFingerprint,
            changedSeed.PlanFingerprint);

        var intent =
            DataGeneratorPolicy.BuildIntent(first);

        Assert.Equal(
            MutationOperationKind.DataGenerator,
            intent.Kind);
        Assert.Equal("prod", intent.ClusterId);
        Assert.Equal(
            new[]
            {
                AuthorizationAction.ClusterRead,
                AuthorizationAction.TopicRead,
                AuthorizationAction.RecordProduce,
                AuthorizationAction.DataGeneratorPlan,
                AuthorizationAction.DataGeneratorExecute,
            },
            intent.AuthorizationTargets!
                .Select(item => item.Action)
                .ToArray());
        Assert.DoesNotContain(
            intent.AuthorizationTargets!,
            item =>
                item.Action ==
                AuthorizationAction.SchemaRead);

        Assert.Equal(
            MutationRiskClass.High,
            DataGeneratorPolicy
                .ClassifyRisk(first)
                .RiskClass);
    }

    [Fact]
    public void Schema_source_binds_schema_read_and_fingerprint()
    {
        var source =
            new DataGeneratorSource(
                DataGeneratorSourceKind.Schema,
                Schema:
                    new DataGeneratorSchemaSource(
                        "orders-value",
                        3,
                        27,
                        RecordSchemaFormat.JsonSchema,
                        new string('b', 64)));

        var plan =
            DataGeneratorPolicy.CreatePlan(
                Destination(),
                source,
                recordCount: 10,
                seed: 7,
                new DataGeneratorBudget());

        var intent =
            DataGeneratorPolicy.BuildIntent(plan);

        Assert.Contains(
            intent.AuthorizationTargets!,
            item =>
                item.Action ==
                    AuthorizationAction.SchemaRead &&
                item.ClusterId == "prod" &&
                item.ResourceName ==
                    "orders-value");
        Assert.Contains(
            intent.Preconditions!,
            item =>
                item.Key ==
                    "generator.source" &&
                item.Fingerprint ==
                    new string('b', 64));
    }

    [Fact]
    public void Budget_hard_caps_and_cap_plus_one_fail_closed()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataGeneratorBudget(
                maxRecordsPerSecond:
                    DataGeneratorBudget
                        .HardMaxRecordsPerSecond + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataGeneratorBudget(
                maxBytesPerSecond:
                    DataGeneratorBudget
                        .HardMaxBytesPerSecond + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataGeneratorBudget(
                maxTotalRecords:
                    DataGeneratorBudget
                        .HardMaxTotalRecords + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataGeneratorBudget(
                maxTotalBytes:
                    DataGeneratorBudget
                        .HardMaxTotalBytes + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataGeneratorBudget(
                maxDuration:
                    DataGeneratorBudget
                        .HardMaxDuration +
                    TimeSpan.FromTicks(1)));
    }

    [Fact]
    public void Built_in_template_materialization_is_seeded_and_index_deterministic()
    {
        var plan =
            DataGeneratorPolicy.CreatePlan(
                Destination(),
                BuiltInSource(),
                recordCount: 10,
                seed: 12345,
                new DataGeneratorBudget());

        var tooling =
            new SchemaDeveloperService(
                new EmptySchemaCatalog());
        var materializer =
            new DataGeneratorMaterializer(
                tooling);

        var first =
            materializer.Materialize(
                plan,
                2);
        var second =
            materializer.Materialize(
                plan,
                2);
        var other =
            materializer.Materialize(
                plan,
                3);

        Assert.Equal(
            first.Value.ToArray(),
            second.Value.ToArray());
        Assert.NotEqual(
            Convert.ToBase64String(
                first.Value.Span),
            Convert.ToBase64String(
                other.Value.Span));

        using var json =
            JsonDocument.Parse(first.Value);
        Assert.Equal(
            2,
            json.RootElement
                .GetProperty("sequence")
                .GetInt64());
        Assert.Empty(first.Headers);
        Assert.Null(first.Key);
    }

    [Fact]
    public void Durable_generator_progress_contains_no_payload_and_ambiguous_batch_blocks_replay()
    {
        const string payloadSentinel =
            "VERY-SECRET-GENERATED-PAYLOAD";

        var plan =
            DataGeneratorPolicy.CreatePlan(
                Destination(),
                BuiltInSource(),
                recordCount: 10,
                seed: 1,
                new DataGeneratorBudget(
                    maxRecordsPerSecond: 10,
                    maxBytesPerSecond:
                        1024 * 1024));

        var snapshot =
            DataGeneratorProgress.CreateInitial(
                Guid.NewGuid(),
                workerGeneration: 1,
                plan,
                Now);

        snapshot =
            DataGeneratorProgress.ChargeRuntime(
                snapshot,
                plan,
                TimeSpan.FromSeconds(1),
                Now.AddSeconds(1));

        snapshot =
            DataGeneratorProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                recordIndex: 0,
                rawBytes:
                    Encoding.UTF8.GetByteCount(
                        payloadSentinel),
                Now.AddSeconds(1));

        var batchId =
            snapshot.Generator!
                .PendingBatch!.BatchId;

        snapshot =
            DataGeneratorProgress.MarkDispatchStarted(
                snapshot,
                batchId,
                Now.AddSeconds(1));

        snapshot =
            DataGeneratorProgress.MarkAmbiguous(
                snapshot,
                batchId,
                Now.AddSeconds(1));

        Assert.Equal(
            FleetProgressPhase.WaitingForExternalAction,
            snapshot.Phase);
        Assert.Equal(
            FleetGeneratorBatchState.DispatchStarted,
            snapshot.Generator!
                .PendingBatch!.State);

        Assert.Throws<MutationStateException>(
            () => DataGeneratorProgress
                .EnsureCanDispatch(
                    snapshot,
                    plan));

        var persisted =
            JsonSerializer.Serialize(snapshot);

        Assert.DoesNotContain(
            payloadSentinel,
            persisted,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "value",
            persisted,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Rate_budget_does_not_reset_after_acknowledgement()
    {
        var plan =
            DataGeneratorPolicy.CreatePlan(
                Destination(),
                BuiltInSource(),
                recordCount: 2,
                seed: 5,
                new DataGeneratorBudget(
                    maxBatchRecords: 2,
                    maxTotalRecords: 2,
                    maxRecordsPerSecond: 1,
                    maxBytesPerSecond:
                        1024 * 1024));

        var snapshot =
            DataGeneratorProgress.CreateInitial(
                Guid.NewGuid(),
                1,
                plan,
                Now);

        snapshot =
            DataGeneratorProgress.ChargeRuntime(
                snapshot,
                plan,
                TimeSpan.FromSeconds(1),
                Now.AddSeconds(1));

        snapshot =
            DataGeneratorProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                0,
                10,
                Now.AddSeconds(1));
        var batch =
            snapshot.Generator!
                .PendingBatch!.BatchId;
        snapshot =
            DataGeneratorProgress.MarkDispatchStarted(
                snapshot,
                batch,
                Now.AddSeconds(1));
        snapshot =
            DataGeneratorProgress.CompleteAcknowledged(
                snapshot,
                plan,
                batch,
                Now.AddSeconds(1));

        Assert.Throws<DataGeneratorRateLimitException>(
            () => DataGeneratorProgress
                .ReserveBeforeDispatch(
                    snapshot,
                    plan,
                    1,
                    10,
                    Now.AddSeconds(1)));

        snapshot =
            DataGeneratorProgress.ChargeRuntime(
                snapshot,
                plan,
                TimeSpan.FromSeconds(1),
                Now.AddSeconds(2));

        var allowed =
            DataGeneratorProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                1,
                10,
                Now.AddSeconds(2));

        Assert.NotNull(
            allowed.Generator?
                .PendingBatch);
    }

    private static DataGeneratorDestination Destination() =>
        new(
            "prod",
            "profile-v1",
            "physical-kafka-a",
            "generated.events",
            2,
            new string('a', 64));

    private static DataGeneratorSource BuiltInSource() =>
        new(
            DataGeneratorSourceKind.BuiltInTemplate,
            Template:
                new DataGeneratorTemplateSource(
                    DataGeneratorBuiltInTemplate
                        .BasicJsonV1,
                    1,
                    DataGeneratorPolicy
                        .BuiltInTemplateFingerprint(
                            DataGeneratorBuiltInTemplate
                                .BasicJsonV1,
                            1)));

    private sealed class EmptySchemaCatalog :
        ISchemaCatalogReadPort
    {
        private static ReadViewResult<T> Unsupported<T>() =>
            ReadViewResult<T>.Failed(
                new ReadViewFailure(
                    ReadViewFailureCategory.Unsupported,
                    "not_used",
                    "Not used by this test.",
                    false));

        public Task<ReadViewResult<IReadOnlyList<SchemaSubjectSummary>>>
            ListSubjectsAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                Unsupported<
                    IReadOnlyList<
                        SchemaSubjectSummary>>());

        public Task<ReadViewResult<IReadOnlyList<SchemaVersionSummary>>>
            ListVersionsAsync(
                string clusterId,
                string subject,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                Unsupported<
                    IReadOnlyList<
                        SchemaVersionSummary>>());

        public Task<ReadViewResult<SchemaVersionDetail>>
            GetVersionAsync(
                string clusterId,
                string subject,
                int version,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                Unsupported<SchemaVersionDetail>());

        public Task<
            ReadViewResult<
                SchemaCompatibilityObservation>>
            GetCompatibilityAsync(
                string clusterId,
                string subject,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                Unsupported<
                    SchemaCompatibilityObservation>());

        public Task<
            ReadViewResult<
                SchemaGlobalCompatibilityObservation>>
            GetGlobalCompatibilityAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                Unsupported<
                    SchemaGlobalCompatibilityObservation>());
    }
}
