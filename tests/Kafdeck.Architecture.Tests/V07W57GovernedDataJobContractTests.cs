using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W57GovernedDataJobContractTests
{
    [Fact]
    public void Byte_preserving_job_reuses_frozen_transfer_contract_deterministically()
    {
        var transfer = TransferPlan();
        var first = GovernedDataJobPolicy.FromTransfer(
            GovernedDataJobKind.Forward,
            transfer);
        var second = GovernedDataJobPolicy.FromTransfer(
            GovernedDataJobKind.Forward,
            transfer);

        Assert.Equal(first, second);
        Assert.Equal(
            GovernedDataTransformKind.BytePreserving,
            first.Transform.Kind);
        Assert.Equal(64, first.PlanFingerprint.Length);

        GovernedDataJobPolicy.ValidatePlan(first);
    }

    [Fact]
    public void Data_job_intent_requires_independent_read_export_produce_and_job_permissions()
    {
        var plan = GovernedDataJobPolicy.FromTransfer(
            GovernedDataJobKind.Replay,
            TransferPlan());

        var intent = GovernedDataJobPolicy.BuildIntent(plan);

        Assert.Equal(
            MutationOperationKind.DataJob,
            intent.Kind);

        Assert.Contains(
            intent.AuthorizationTargets!,
            target =>
                target.Action ==
                    AuthorizationAction.RecordRead &&
                target.ClusterId == "source");
        Assert.Contains(
            intent.AuthorizationTargets!,
            target =>
                target.Action ==
                    AuthorizationAction.RecordExport &&
                target.ClusterId == "source");
        Assert.Contains(
            intent.AuthorizationTargets!,
            target =>
                target.Action ==
                    AuthorizationAction.RecordProduce &&
                target.ClusterId == "destination");
        Assert.Contains(
            intent.AuthorizationTargets!,
            target =>
                target.Action ==
                    AuthorizationAction.DataJobPlan);
        Assert.Contains(
            intent.AuthorizationTargets!,
            target =>
                target.Action ==
                    AuthorizationAction.DataJobExecute);

        Assert.Contains(
            intent.Preconditions!,
            item =>
                item.Key == "data-job.plan" &&
                item.Fingerprint ==
                    plan.PlanFingerprint);
        Assert.Contains(
            intent.ResourceKeys,
            key =>
                key ==
                $"data-job/{plan.PlanFingerprint}");
    }

    [Fact]
    public void Reprocess_transform_is_closed_bounded_and_raises_risk_floor()
    {
        var transform = new GovernedDataTransform(
            GovernedDataTransformKind
                .MaskedStructuredProjection,
            "json",
            new[] { "customerId", "status" });

        var plan = GovernedDataJobPolicy.FromTransfer(
            GovernedDataJobKind.Reprocess,
            TransferPlan(),
            transform);

        GovernedDataJobPolicy.ValidatePlan(plan);
        var risk =
            GovernedDataJobPolicy.ClassifyRisk(plan);

        Assert.True(
            (int)risk.RiskClass >=
            (int)MutationRiskClass.High);
        Assert.Equal(
            MutationConfirmationMode.TypedTarget,
            risk.ConfirmationMode);
        Assert.Contains(
            "data_job_reprocess",
            risk.Reasons);
    }

    [Theory]
    [InlineData("javascript")]
    [InlineData("python")]
    [InlineData("custom-plugin")]
    public void Arbitrary_transform_families_are_not_admitted(
        string format)
    {
        var transform = new GovernedDataTransform(
            GovernedDataTransformKind
                .MaskedStructuredProjection,
            format,
            new[] { "customerId" });

        Assert.Throws<MutationStateException>(
            () => GovernedDataJobPolicy.FromTransfer(
                GovernedDataJobKind.Reprocess,
                TransferPlan(),
                transform));
    }

    [Fact]
    public void Byte_preserving_transform_rejects_structured_configuration()
    {
        var transform = new GovernedDataTransform(
            GovernedDataTransformKind
                .BytePreserving,
            "json",
            new[] { "customerId" });

        Assert.Throws<MutationStateException>(
            () => GovernedDataJobPolicy.FromTransfer(
                GovernedDataJobKind.Forward,
                TransferPlan(),
                transform));
    }

    [Fact]
    public void Structured_projection_field_count_is_hard_bounded()
    {
        var fields = Enumerable
            .Range(
                0,
                GovernedDataJobPolicy
                    .MaxProjectedFields + 1)
            .Select(index => $"field{index}")
            .ToArray();

        var transform = new GovernedDataTransform(
            GovernedDataTransformKind
                .MaskedStructuredProjection,
            "messagepack",
            fields);

        Assert.Throws<MutationStateException>(
            () => GovernedDataJobPolicy.FromTransfer(
                GovernedDataJobKind.Reprocess,
                TransferPlan(),
                transform));
    }

    [Fact]
    public void Tampered_plan_fingerprint_fails_closed()
    {
        var plan = GovernedDataJobPolicy.FromTransfer(
            GovernedDataJobKind.Forward,
            TransferPlan());

        var tampered = plan with
        {
            PlanFingerprint =
                new string('0', 64),
        };

        Assert.Throws<MutationStateException>(
            () =>
                GovernedDataJobPolicy.ValidatePlan(
                    tampered));
    }

    [Fact]
    public void Same_physical_cluster_alias_fails_closed()
    {
        var transfer = TransferPlan();
        var aliased = transfer with
        {
            Destination = transfer.Destination with
            {
                KafkaClusterId =
                    transfer.Source.KafkaClusterId,
            },
        };

        var plan = GovernedDataJobPolicy.FromTransfer(
            GovernedDataJobKind.Forward,
            aliased);

        Assert.Throws<MutationStateException>(
            () =>
                GovernedDataJobPolicy.ValidatePlan(
                    plan));
    }

    [Fact]
    public void Data_job_has_high_builtin_mutation_risk_floor()
    {
        var risk =
            MutationRiskClassifier.Classify(
                new MutationRiskInput(
                    MutationOperationKind.DataJob));

        Assert.Equal(
            MutationRiskClass.High,
            risk.RiskClass);
        Assert.Equal(
            MutationConfirmationMode.TypedTarget,
            risk.ConfirmationMode);
    }

    private static ClusterTransferPlan TransferPlan()
    {
        var source =
            new ClusterTransferEndpoint(
                "source",
                "profile-v1",
                "physical-source");
        var destination =
            new ClusterTransferEndpoint(
                "destination",
                "profile-v2",
                "physical-destination");
        var mappings =
            new[]
            {
                new ClusterTransferMapping(
                    "orders.dlq",
                    0,
                    "orders.replay",
                    0,
                    100,
                    250,
                    Sha('a'),
                    Sha('b')),
            };
        var budget =
            new ClusterTransferBudget(
                maxBatchRecords: 100,
                maxBatchBytes: 1024 * 1024,
                maxTotalRecords: 150,
                maxTotalBytes: 10 * 1024 * 1024,
                maxDuration:
                    TimeSpan.FromMinutes(10),
                maxRecordsPerSecond: 100,
                maxBytesPerSecond: 1024 * 1024);
        var policy =
            new ClusterTransferDataPolicy(
                "default",
                1,
                Sha('c'));

        return new ClusterTransferPlan(
            source,
            destination,
            mappings,
            budget,
            policy,
            ClusterTransferPolicy.PlanFingerprint(
                source,
                destination,
                mappings,
                budget,
                policy));
    }

    private static string Sha(char value) =>
        new(value, 64);
}
