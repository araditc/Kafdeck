using Kafdeck.Core.Consumers;
using Kafdeck.Core.ReadViews;
using Kafdeck.Modules.Consumers;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class ConsumerExplorerServiceTests
{
    [Fact]
    public void Aggregate_lag_excludes_out_of_range_and_marks_projection_partial()
    {
        ConsumerOffsetProjection[] partitions =
        [
            new("payments", 0, 90, 100, 10, ConsumerOffsetState.Observed),
            new("payments", 1, 120, 100, null, ConsumerOffsetState.CommittedOffsetOutOfRange),
        ];

        var projection = ConsumerExplorerService.BuildLagProjection("payments-worker", partitions);

        Assert.Equal(10, projection.TotalLag);
        Assert.True(projection.IsPartial);
        Assert.Single(projection.Limitations);
        Assert.Contains("out of the observed log range", projection.Limitations[0].SafeMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Aggregate_lag_is_unknown_when_no_partition_has_valid_lag()
    {
        ConsumerOffsetProjection[] partitions =
        [
            new("payments", 0, null, 100, null, ConsumerOffsetState.MissingCommittedOffset),
            new("payments", 1, 120, 100, null, ConsumerOffsetState.CommittedOffsetOutOfRange),
        ];

        var projection = ConsumerExplorerService.BuildLagProjection("payments-worker", partitions);

        Assert.Null(projection.TotalLag);
        Assert.True(projection.IsPartial);
        Assert.Equal(2, projection.Limitations.Count);
    }

    [Fact]
    public void Empty_group_offset_set_has_zero_observed_lag_without_partial_flag()
    {
        var projection = ConsumerExplorerService.BuildLagProjection(
            "empty",
            Array.Empty<ConsumerOffsetProjection>());

        Assert.Equal(0, projection.TotalLag);
        Assert.False(projection.IsPartial);
        Assert.Empty(projection.Limitations);
    }

    [Fact]
    public void Read_view_limitations_do_not_contain_payload_or_secret_fields()
    {
        var properties = typeof(ReadViewLimitation).GetProperties().Select(property => property.Name).ToArray();

        Assert.DoesNotContain(properties, name => name.Contains("Payload", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(properties, name => name.Contains("Secret", StringComparison.OrdinalIgnoreCase));
    }
}
