using Confluent.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W41PinnedClientCapabilityTests
{
    [Fact]
    public void Pinned_client_compile_probe_exposes_acl_scram_config_and_preferred_election_primitives()
    {
        // These nameof expressions are deliberate compile probes against the exact
        // Confluent.Kafka package restored by this repository. A package that drops
        // or renames one of these APIs must fail compilation rather than silently
        // degrading a governed v0.6 capability.
        var requiredSymbols = new[]
        {
            nameof(IAdminClient.DescribeAclsAsync),
            nameof(IAdminClient.CreateAclsAsync),
            nameof(IAdminClient.DeleteAclsAsync),
            nameof(IAdminClient.DescribeUserScramCredentialsAsync),
            nameof(IAdminClient.AlterUserScramCredentialsAsync),
            nameof(IAdminClient.DescribeConfigsAsync),
            nameof(IAdminClient.IncrementalAlterConfigsAsync),
            nameof(IAdminClientExtensions.ElectLeadersAsync),
        };

        Assert.Equal(8, requiredSymbols.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void Pinned_client_probe_keeps_quota_and_partition_reassignment_gaps_explicit()
    {
        var publicMethodNames = typeof(IAdminClient)
            .GetMethods()
            .Select(method => method.Name)
            .Concat(typeof(IAdminClientExtensions).GetMethods().Select(method => method.Name))
            .ToHashSet(StringComparer.Ordinal);

        // W41 must not manufacture provider support through CLI/REST/reflection
        // tunnels when the pinned .NET surface does not expose these operations.
        Assert.DoesNotContain("DescribeClientQuotasAsync", publicMethodNames);
        Assert.DoesNotContain("AlterClientQuotasAsync", publicMethodNames);
        Assert.DoesNotContain("AlterPartitionReassignmentsAsync", publicMethodNames);
        Assert.DoesNotContain("ListPartitionReassignmentsAsync", publicMethodNames);
    }

    [Fact]
    public void Capability_probe_is_metadata_only_and_performs_no_provider_io()
    {
        var adminMethods = typeof(IAdminClient)
            .GetMethods()
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Contains(nameof(IAdminClient.DescribeAclsAsync), adminMethods);
        Assert.Contains(nameof(IAdminClient.AlterUserScramCredentialsAsync), adminMethods);
    }
}
