using System.Text;
using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05RecordProductionTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 22, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Planner_binds_payload_only_by_hmac_digest_and_safe_metadata()
    {
        const string sentinel = "payload-secret-sentinel";
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var planner = new RecordProductionPlanner(
            new FakeKafkaAdministrationPort(),
            digest,
            policy: new RecordProductionPolicy(maxRecords: 4));

        using var result = await planner.PlanAsync(
            Request(
                Encoding.UTF8.GetBytes(sentinel),
                key: Encoding.UTF8.GetBytes("private-key"),
                headers: new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal)
                {
                    ["trace"] = Encoding.UTF8.GetBytes("private-header"),
                }));

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotNull(result.Plan);
        Assert.NotNull(result.ExecutionMaterial);
        Assert.Equal(MutationRiskClass.Moderate, result.Plan!.Risk.RiskClass);
        var target = Assert.Single(result.Plan.Intent.AuthorizationTargets!);
        Assert.Equal(AuthorizationAction.RecordProduce, target.Action);
        Assert.Equal("orders", target.ResourceName);
        Assert.Single(result.Plan.Intent.MaterialDigests!);

        var durable = JsonSerializer.Serialize(result.Plan);
        Assert.DoesNotContain(sentinel, durable, StringComparison.Ordinal);
        Assert.DoesNotContain("private-key", durable, StringComparison.Ordinal);
        Assert.DoesNotContain("private-header", durable, StringComparison.Ordinal);
        Assert.Contains("record/0000", durable, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Planner_enforces_record_and_total_byte_limits_before_preview()
    {
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var planner = new RecordProductionPlanner(
            new FakeKafkaAdministrationPort(),
            digest,
            policy: new RecordProductionPolicy(
                maxRecords: 2,
                maxValueBytes: 8,
                maxTotalBytes: 12));

        using var tooMany = await planner.PlanAsync(
            new RecordProductionRequest(
                "prod",
                "orders",
                new[]
                {
                    Record("a"),
                    Record("b"),
                    Record("c"),
                }));

        Assert.False(tooMany.IsSuccess);
        Assert.Equal(RecordProductionPlanningFailureCode.LimitExceeded, tooMany.Failure!.Code);

        using var tooLarge = await planner.PlanAsync(
            Request(Encoding.UTF8.GetBytes("123456789")));

        Assert.False(tooLarge.IsSuccess);
        Assert.Equal(RecordProductionPlanningFailureCode.LimitExceeded, tooLarge.Failure!.Code);
    }

    [Fact]
    public async Task Schema_validation_is_fail_closed_and_safe()
    {
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var unavailable = new FakeSchemaValidator(
            new RecordProductionSchemaValidationResult(
                RecordProductionSchemaValidationState.Unavailable,
                "validator_unavailable"));
        var planner = new RecordProductionPlanner(
            new FakeKafkaAdministrationPort(),
            digest,
            unavailable);

        using var failed = await planner.PlanAsync(
            Request(
                Encoding.UTF8.GetBytes("payload"),
                schema: new RecordProductionSchemaRequest("registry", "orders-value")));

        Assert.False(failed.IsSuccess);
        Assert.Equal(
            RecordProductionPlanningFailureCode.SchemaValidationUnavailable,
            failed.Failure!.Code);

        var valid = new FakeSchemaValidator(
            new RecordProductionSchemaValidationResult(
                RecordProductionSchemaValidationState.Valid,
                "valid",
                new string('a', 64)));
        var validPlanner = new RecordProductionPlanner(
            new FakeKafkaAdministrationPort(),
            digest,
            valid);

        using var accepted = await validPlanner.PlanAsync(
            Request(
                Encoding.UTF8.GetBytes("payload"),
                schema: new RecordProductionSchemaRequest("registry", "orders-value")));

        Assert.True(accepted.IsSuccess);
        Assert.Equal("valid", accepted.Plan!.Canonical.Records[0].SchemaValidationCode);
        Assert.Equal(new string('a', 64), accepted.Plan.Canonical.Records[0].SchemaFingerprint);
    }

    [Fact]
    public void Template_materializer_is_bounded_typed_and_has_no_execution_engine()
    {
        var request = RecordProductionTemplateMaterializer.Materialize(
            "prod",
            "orders",
            new RecordProductionTemplateDefinition(
                "receipt",
                3,
                "{\"id\":\"{{id}}\"}",
                new[] { "id" }),
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["id"] = "42",
            });

        Assert.Equal(
            "{\"id\":\"42\"}",
            Encoding.UTF8.GetString(request.Records.Single().Value.Span));
        Assert.Equal("receipt", request.Template!.TemplateId);
        Assert.Equal(3, request.Template.Version);

        Assert.Throws<ArgumentException>(() =>
            RecordProductionTemplateMaterializer.Materialize(
                "prod",
                "orders",
                new RecordProductionTemplateDefinition(
                    "unsafe",
                    1,
                    "{{unknown}}",
                    Array.Empty<string>()),
                new Dictionary<string, string>(StringComparer.Ordinal)));
    }

    [Fact]
    public async Task Batch_execution_reports_acknowledgements_partial_and_unknown_without_payload_evidence()
    {
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var planner = new RecordProductionPlanner(
            new FakeKafkaAdministrationPort(),
            digest);

        using var preparation = await planner.PlanAsync(
            new RecordProductionRequest(
                "prod",
                "orders",
                new[] { Record("one"), Record("two") }));
        Assert.True(preparation.IsSuccess);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            preparation.Plan!.Intent,
            preparation.Plan.Risk,
            "w34-test",
            Now.AddMinutes(5),
            Now,
            "record-batch");
        operation.OpenForConfirmation(Now);
        operation.Confirm(
            operation.Snapshot.RequesterPrincipalId,
            operation.Snapshot.PreviewHash,
            Now);

        var allAck = new RecordProductionExecutionService(
            new SequenceProducer(
                Ack(0, 10),
                Ack(0, 11)));
        using var material = new MutationExecutionMaterial(preparation.ExecutionMaterial!.Items);
        var acknowledged = await allAck.ExecuteAsync(
            new MutationExecutionContext(operation.Snapshot, material));

        Assert.Equal(MutationExecutionResultKind.AppliedVerified, acknowledged.ResultKind);
        Assert.Equal("2", acknowledged.SafeEvidence!["record.count"]);
        Assert.Equal("2", acknowledged.SafeEvidence["acknowledged.count"]);

        var partialService = new RecordProductionExecutionService(
            new SequenceProducer(
                Ack(0, 10),
                new MutationProviderResult(
                    MutationExecutionResultKind.FailedDefinitive,
                    "broker_rejected")));
        using var partialMaterial = new MutationExecutionMaterial(preparation.ExecutionMaterial.Items);
        var partial = await partialService.ExecuteAsync(
            new MutationExecutionContext(operation.Snapshot, partialMaterial));

        Assert.Equal(MutationExecutionResultKind.PartiallyApplied, partial.ResultKind);
        Assert.Equal("1", partial.SafeEvidence!["acknowledged.count"]);
        Assert.Equal("1", partial.SafeEvidence["failure.ordinal"]);

        var unknownService = new RecordProductionExecutionService(
            new SequenceProducer(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "timeout")));
        using var unknownMaterial = new MutationExecutionMaterial(preparation.ExecutionMaterial.Items);
        var unknown = await unknownService.ExecuteAsync(
            new MutationExecutionContext(operation.Snapshot, unknownMaterial));

        Assert.Equal(MutationExecutionResultKind.ExecutionUnknown, unknown.ResultKind);
        Assert.Equal("0", unknown.SafeEvidence!["acknowledged.count"]);
        Assert.DoesNotContain("one", JsonSerializer.Serialize(unknown), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Precondition_validator_detects_topic_drift()
    {
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var reads = new FakeKafkaAdministrationPort();
        var planner = new RecordProductionPlanner(reads, digest);

        using var preparation = await planner.PlanAsync(Request(Encoding.UTF8.GetBytes("payload")));
        Assert.True(preparation.IsSuccess);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            preparation.Plan!.Intent,
            preparation.Plan.Risk,
            "w34-test",
            Now.AddMinutes(5),
            Now,
            "record-precondition");

        var changedReads = new FakeKafkaAdministrationPort(partitions: 2);
        var validator = new RecordProductionPreconditionValidator(changedReads);

        var result = await validator.ValidateAsync(operation.Snapshot);

        Assert.Equal(MutationPreDispatchGuardOutcome.StalePreview, result.Outcome);
    }

    [Fact]
    public void Producer_contract_is_typed_and_does_not_expose_generic_configuration()
    {
        var methods = typeof(IRecordProduceMutationPort).GetMethods();
        Assert.Single(methods);
        Assert.Equal(nameof(IRecordProduceMutationPort.ProduceAsync), methods[0].Name);

        var forbidden = new[] { "config", "command", "script", "sql", "url", "method", "payload" };
        foreach (var property in typeof(RecordProduceMutation).GetProperties())
        {
            Assert.DoesNotContain(
                forbidden,
                term => property.Name.Contains(term, StringComparison.OrdinalIgnoreCase));
        }
    }

    private static RecordProductionRequest Request(
        byte[] value,
        byte[]? key = null,
        IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? headers = null,
        RecordProductionSchemaRequest? schema = null) =>
        new(
            "prod",
            "orders",
            new[]
            {
                new RecordProductionRecordInput(
                    key,
                    value,
                    headers ?? new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal)),
            },
            schema);

    private static RecordProductionRecordInput Record(string value) =>
        new(
            null,
            Encoding.UTF8.GetBytes(value),
            new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal));

    private static MutationProviderResult Ack(int partition, long offset) =>
        new(
            MutationExecutionResultKind.AppliedVerified,
            "record_produce_acknowledged",
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["provider.accepted"] = "true",
                ["partition"] = partition.ToString(),
                ["offset"] = offset.ToString(),
                ["verification.state"] = "observed",
            });

    private sealed class SequenceProducer : IRecordProduceMutationPort
    {
        private readonly Queue<MutationProviderResult> _results;

        public SequenceProducer(params MutationProviderResult[] results)
        {
            _results = new Queue<MutationProviderResult>(results);
        }

        public Task<MutationProviderResult> ProduceAsync(
            RecordProduceMutation request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(_results.Dequeue());
    }

    private sealed class FakeSchemaValidator : IRecordProductionSchemaValidator
    {
        private readonly RecordProductionSchemaValidationResult _result;

        public FakeSchemaValidator(RecordProductionSchemaValidationResult result)
        {
            _result = result;
        }

        public Task<RecordProductionSchemaValidationResult> ValidateAsync(
            RecordProductionSchemaValidationContext context,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(_result);
    }

    private sealed class FakeKafkaAdministrationPort : IKafkaAdministrationPort
    {
        private readonly int _partitions;

        public FakeKafkaAdministrationPort(int partitions = 1)
        {
            _partitions = partitions;
        }

        public Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(
                KafkaResult<TopicMetadata>.Success(
                    new TopicMetadata(
                        topicName,
                        false,
                        Enumerable.Range(0, _partitions)
                            .Select(index => new PartitionMetadata(
                                index,
                                1,
                                new[] { 1 },
                                new[] { 1 }))
                            .ToArray()),
                    Observation()));

        public Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetBrokerConfigurationAsync(
            string clusterId,
            int brokerId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<KafkaCapabilities>> GetCapabilitiesAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        private static ObservationMetadata Observation()
        {
            var now = DateTimeOffset.UtcNow;
            return new ObservationMetadata(now, now, now, ObservationSource.Live);
        }
    }
}
