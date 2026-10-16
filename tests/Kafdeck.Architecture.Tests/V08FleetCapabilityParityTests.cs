using Kafdeck.Api;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08FleetCapabilityParityTests
{
    [Fact]
    public void Inventory_is_closed_complete_bounded_and_grants_no_execution_authority()
    {
        var inventory=KafdeckBearerCapabilities.CreateInventory();
        Assert.Equal(1,inventory.Version);Assert.Equal("supported-ui",inventory.InventoryBasis);
        Assert.Equal(Enum.GetValues<MutationOperationKind>().Length,inventory.Families.Count);
        Assert.Equal(inventory.Families.Count,inventory.Families.Select(f=>f.Kind).Distinct(StringComparer.Ordinal).Count());
        Assert.All(inventory.Families,f=>
        {
            Assert.False(f.AuthorityFromCapability);Assert.InRange(f.ReasonCode.Length,1,96);
            Assert.NotEmpty(f.SourceEvidence);
            Assert.All(f.NamedPreviewRoutes.Concat(f.MaterialExecuteRoutes),route=>
            {Assert.StartsWith("v08-bearer-",route);Assert.InRange(route.Length,1,128);Assert.DoesNotContain("{",route);});
            if(!f.HandlerAvailable) { Assert.Empty(f.NamedPreviewRoutes);Assert.Empty(f.MaterialExecuteRoutes); }
        });
    }
    [Theory]
    [InlineData("quotaAlter","inheritedUnsupported","inheritedUnsupported")]
    [InlineData("preferredLeaderElection","notHosted","notAdmitted")]
    [InlineData("partitionReassign","inheritedUnsupported","inheritedUnsupported")]
    [InlineData("replicationFactorAlter","inheritedUnsupported","inheritedUnsupported")]
    [InlineData("reassignmentThrottle","notHosted","notAdmitted")]
    [InlineData("brokerMaintenance","notHosted","notAdmitted")]
    [InlineData("logDirectoryMaintenance","inheritedUnsupported","inheritedUnsupported")]
    [InlineData("clusterTransfer","notHosted","notAdmitted")]
    [InlineData("replicationIntegration","notHosted","notAdmitted")]
    public void Inherited_fleet_limits_never_become_an_alternate_execution_tunnel(string kind,string api,string provider)
    {
        var family=Assert.Single(KafdeckBearerCapabilities.CreateInventory().Families,f=>f.Kind==kind);
        Assert.False(family.HandlerAvailable);Assert.Equal(api,family.ApiContractStatus);
        Assert.Equal(provider,family.ProviderCapability);Assert.NotEqual("supported",family.UiStatus);
    }
    [Fact]
    public void Named_routes_do_not_claim_provider_support_or_completed_reconciliation()
    {
        var inventory=KafdeckBearerCapabilities.CreateInventory();
        var topic=Assert.Single(inventory.Families,f=>f.Kind=="topicCreate");
        Assert.True(topic.HandlerAvailable);Assert.Equal("probeRequired",topic.ProviderCapability);
        Assert.Contains("v08-bearer-topic-create-preview",topic.NamedPreviewRoutes);
        Assert.Contains("v08-bearer-mutation-execute",topic.MaterialExecuteRoutes);
        Assert.All(inventory.Families.SelectMany(f=>f.MaterialExecuteRoutes),r=>Assert.False(r.EndsWith("-reconcile",StringComparison.Ordinal)));
        var reconciliation=Assert.Single(inventory.Families,f=>f.Kind=="fleetUncertaintyDisposition");
        Assert.True(reconciliation.HandlerAvailable);Assert.Equal("probeRequired",reconciliation.ProviderCapability);
        Assert.Contains("v08-bearer-v08-data-job-reconcile-preview",reconciliation.NamedPreviewRoutes);
        Assert.Contains("v08-bearer-v08-data-generator-reconcile-preview",reconciliation.NamedPreviewRoutes);
        Assert.Contains("v08-bearer-mutation-execute",reconciliation.MaterialExecuteRoutes);
    }
}
