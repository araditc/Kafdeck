using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W41FleetContractTests
{
    [Fact]
    public void V05_operation_kind_values_and_idempotency_scope_remain_stable()
    {
        Assert.Equal(1, (int)MutationOperationKind.TopicCreate);
        Assert.Equal(2, (int)MutationOperationKind.TopicAlter);
        Assert.Equal(3, (int)MutationOperationKind.TopicIncreasePartitions);
        Assert.Equal(4, (int)MutationOperationKind.TopicDelete);
        Assert.Equal(5, (int)MutationOperationKind.RecordProduce);
        Assert.Equal(6, (int)MutationOperationKind.ConsumerOffsetAlter);
        Assert.Equal(7, (int)MutationOperationKind.ConsumerDelete);
        Assert.Equal(8, (int)MutationOperationKind.SchemaCreate);
        Assert.Equal(9, (int)MutationOperationKind.SchemaAlter);
        Assert.Equal(10, (int)MutationOperationKind.SchemaDelete);
        Assert.Equal(11, (int)MutationOperationKind.ConnectCreate);
        Assert.Equal(12, (int)MutationOperationKind.ConnectAlter);
        Assert.Equal(13, (int)MutationOperationKind.ConnectDelete);
        Assert.Equal(14, (int)MutationOperationKind.RecordsPurge);

        Assert.Equal(
            "dd4774953eda2d1076ff5544c613fcbb65d2c4dd7cc0ed6f1092852958e25710",
            MutationIdempotency.BuildScope(
                "oidc:https://idp.example|alice",
                "prod",
                MutationOperationKind.TopicCreate));
    }

    [Fact]
    public void V05_authorization_action_values_remain_stable()
    {
        Assert.Equal(1, (int)AuthorizationAction.SystemRead);
        Assert.Equal(14, (int)AuthorizationAction.CatalogRead);
        Assert.Equal(15, (int)AuthorizationAction.TopicCreate);
        Assert.Equal(27, (int)AuthorizationAction.RecordsPurge);
        Assert.Equal(28, (int)AuthorizationAction.AclRead);
        Assert.Equal(45, (int)AuthorizationAction.MutationReconcile);
    }

    [Fact]
    public void Legacy_normalizer_does_not_accept_new_fleet_kinds()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            MutationAuthorization.ExpectedAction(MutationOperationKind.AclAlter));
    }

    [Fact]
    public void Quota_change_requires_closed_read_and_alter_conjunction()
    {
        var requirements = FleetMutationAuthorization.NormalizeRequirements(
            MutationOperationKind.QuotaAlter,
            new[]
            {
                Target(AuthorizationAction.QuotaRead, "prod", "quota/user/alice/producer_byte_rate"),
                Target(AuthorizationAction.QuotaAlter, "prod", "quota/user/alice/producer_byte_rate"),
            });

        Assert.Equal(2, requirements.Count);
        Assert.Throws<ArgumentException>(() =>
            FleetMutationAuthorization.NormalizeRequirements(
                MutationOperationKind.QuotaAlter,
                new[]
                {
                    Target(AuthorizationAction.QuotaAlter, "prod", "quota/user/alice/producer_byte_rate"),
                }));
        Assert.Throws<ArgumentException>(() =>
            FleetMutationAuthorization.NormalizeRequirements(
                MutationOperationKind.QuotaAlter,
                new[]
                {
                    Target(AuthorizationAction.QuotaRead, "prod", "quota/user/alice/producer_byte_rate"),
                    Target(AuthorizationAction.QuotaAlter, "prod", "quota/user/alice/producer_byte_rate"),
                    Target(AuthorizationAction.TopicDelete, "prod", "payments"),
                }));
    }

    [Fact]
    public void Single_cluster_fleet_kind_rejects_cross_cluster_substitution()
    {
        Assert.Throws<ArgumentException>(() =>
            FleetMutationAuthorization.NormalizeRequirements(
                MutationOperationKind.AclAlter,
                new[]
                {
                    Target(AuthorizationAction.AclRead, "prod", "acl/topic/payments/alice/read"),
                    Target(AuthorizationAction.AclAlter, "dr", "acl/topic/payments/alice/read"),
                }));
    }

    [Fact]
    public void Finite_transfer_binds_two_physical_clusters_and_export_produce_boundary()
    {
        var requirements = FleetMutationAuthorization.NormalizeRequirements(
            MutationOperationKind.ClusterTransfer,
            TransferRequirements());

        Assert.Equal(9, requirements.Count);

        var withoutExport = TransferRequirements()
            .Where(target => target.Action != AuthorizationAction.RecordExport)
            .ToArray();
        Assert.Throws<ArgumentException>(() =>
            FleetMutationAuthorization.NormalizeRequirements(
                MutationOperationKind.ClusterTransfer,
                withoutExport));

        var sameDestination = TransferRequirements()
            .Select(target =>
                target.Action == AuthorizationAction.RecordProduce
                    ? target with { ClusterId = "prod" }
                    : target)
            .ToArray();
        Assert.Throws<ArgumentException>(() =>
            FleetMutationAuthorization.NormalizeRequirements(
                MutationOperationKind.ClusterTransfer,
                sameDestination));
    }

    [Fact]
    public void Replication_integration_requires_exactly_one_connect_lifecycle_mutation_action()
    {
        var baseline = new[]
        {
            Target(AuthorizationAction.ReplicationIntegrationAlter, "prod", "replication/prod-to-dr"),
            Target(AuthorizationAction.ClusterRead, "prod", "cluster/prod"),
            Target(AuthorizationAction.ClusterRead, "dr", "cluster/dr"),
            Target(AuthorizationAction.TopicRead, "prod", "topic/payments"),
            Target(AuthorizationAction.TopicRead, "dr", "topic/payments"),
            Target(AuthorizationAction.ConnectRead, "dr", "connector/mm2-payments"),
            Target(AuthorizationAction.ConnectAlter, "dr", "connector/mm2-payments"),
            Target(AuthorizationAction.RecordRead, "prod", "topic/payments"),
            Target(AuthorizationAction.RecordExport, "prod", "topic/payments"),
            Target(AuthorizationAction.RecordProduce, "dr", "topic/payments"),
        };

        Assert.NotEmpty(FleetMutationAuthorization.NormalizeRequirements(
            MutationOperationKind.ReplicationIntegration,
            baseline));

        Assert.Throws<ArgumentException>(() =>
            FleetMutationAuthorization.NormalizeRequirements(
                MutationOperationKind.ReplicationIntegration,
                baseline
                    .Append(Target(
                        AuthorizationAction.ConnectCreate,
                        "dr",
                        "connector/mm2-payments"))
                    .ToArray()));
    }

    [Fact]
    public void Uncertainty_disposition_requires_reconcile_and_original_effect_conjunction()
    {
        Assert.NotEmpty(FleetMutationAuthorization.NormalizeRequirements(
            MutationOperationKind.FleetUncertaintyDisposition,
            new[]
            {
                Target(AuthorizationAction.MutationReconcile, "prod", "operation/11111111-1111-1111-1111-111111111111"),
                Target(AuthorizationAction.TopicDelete, "prod", "payments"),
            }));

        Assert.Throws<ArgumentException>(() =>
            FleetMutationAuthorization.NormalizeRequirements(
                MutationOperationKind.FleetUncertaintyDisposition,
                new[]
                {
                    Target(AuthorizationAction.MutationReconcile, "prod", "operation/11111111-1111-1111-1111-111111111111"),
                }));
    }

    private static MutationAuthorizationTarget[] TransferRequirements() =>
        new[]
        {
            Target(AuthorizationAction.ClusterTransferPlan, "prod", "pair/prod-to-dr"),
            Target(AuthorizationAction.ClusterTransferExecute, "prod", "pair/prod-to-dr"),
            Target(AuthorizationAction.ClusterRead, "prod", "cluster/prod"),
            Target(AuthorizationAction.ClusterRead, "dr", "cluster/dr"),
            Target(AuthorizationAction.TopicRead, "prod", "topic/payments"),
            Target(AuthorizationAction.TopicRead, "dr", "topic/payments"),
            Target(AuthorizationAction.RecordRead, "prod", "topic/payments"),
            Target(AuthorizationAction.RecordExport, "prod", "topic/payments"),
            Target(AuthorizationAction.RecordProduce, "dr", "topic/payments"),
        };

    private static MutationAuthorizationTarget Target(
        AuthorizationAction action,
        string clusterId,
        string resourceName) =>
        new(action, clusterId, resourceName);
}
