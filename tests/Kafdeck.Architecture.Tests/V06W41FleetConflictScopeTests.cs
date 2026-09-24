using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W41FleetConflictScopeTests
{
    [Fact]
    public void Topic_partition_and_configuration_share_the_exact_physical_topic_scope()
    {
        var topic = FleetConflictScope.FromFleetConflictKey(
            FleetConflictKeyCodec.Topic("prod", "payments.events"));
        var partition = FleetConflictScope.FromFleetConflictKey(
            FleetConflictKeyCodec.TopicPartition("prod", "payments.events", 7));
        var config = FleetConflictScope.FromFleetConflictKey(
            FleetConflictKeyCodec.TopicConfiguration(
                "prod",
                "payments.events",
                "min.insync.replicas"));

        Assert.Equal(topic, partition);
        Assert.Equal(topic, config);
        Assert.Equal(
            FleetConflictKeyCodec.Topic("prod", "payments.events"),
            topic);
    }

    [Fact]
    public void Legacy_v05_topic_claim_maps_to_the_same_scope_as_fleet_children()
    {
        var legacy = FleetConflictScope.FromLegacyTopicResourceKey(
            "cluster/prod/topic/payments.events");
        var fleetPartition = FleetConflictScope.FromFleetConflictKey(
            FleetConflictKeyCodec.TopicPartition("prod", "payments.events", 2));
        var fleetConfig = FleetConflictScope.FromFleetConflictKey(
            FleetConflictKeyCodec.TopicConfiguration(
                "prod",
                "payments.events",
                "retention.ms"));

        Assert.Equal(legacy, fleetPartition);
        Assert.Equal(legacy, fleetConfig);
    }

    [Fact]
    public void Scope_does_not_cross_physical_cluster_or_resource_identity()
    {
        var prod = FleetConflictScope.FromFleetConflictKey(
            FleetConflictKeyCodec.TopicPartition("prod", "payments.events", 0));
        var dr = FleetConflictScope.FromFleetConflictKey(
            FleetConflictKeyCodec.TopicPartition("dr", "payments.events", 0));
        var otherTopic = FleetConflictScope.FromFleetConflictKey(
            FleetConflictKeyCodec.TopicPartition("prod", "audit.events", 0));

        Assert.NotEqual(prod, dr);
        Assert.NotEqual(prod, otherTopic);
    }

    [Fact]
    public void Broker_and_broker_configuration_share_one_physical_broker_scope()
    {
        var broker = FleetConflictScope.FromFleetConflictKey(
            FleetConflictKeyCodec.Broker("prod", "broker-7"));
        var config = FleetConflictScope.FromFleetConflictKey(
            FleetConflictKeyCodec.BrokerConfiguration(
                "prod",
                "broker-7",
                "leader.replication.throttled.rate"));

        Assert.Equal(broker, config);
        Assert.Equal(FleetConflictKeyCodec.Broker("prod", "broker-7"), broker);
    }

    [Fact]
    public void Non_hierarchical_target_keeps_its_complete_typed_identity()
    {
        var firstKey = FleetConflictKeyCodec.Encode(
            new FleetConflictTarget(
                FleetConflictTargetKind.TransferPair,
                "prod",
                "prod-to-dr",
                "mapping-v1"));
        var secondKey = FleetConflictKeyCodec.Encode(
            new FleetConflictTarget(
                FleetConflictTargetKind.TransferPair,
                "prod",
                "prod-to-dr",
                "mapping-v2"));

        Assert.Equal(firstKey, FleetConflictScope.FromFleetConflictKey(firstKey));
        Assert.Equal(secondKey, FleetConflictScope.FromFleetConflictKey(secondKey));
        Assert.NotEqual(
            FleetConflictScope.FromFleetConflictKey(firstKey),
            FleetConflictScope.FromFleetConflictKey(secondKey));
    }

    [Fact]
    public void Unsupported_or_ambiguous_legacy_scope_is_rejected_fail_closed()
    {
        Assert.Throws<MutationStateException>(() =>
            FleetConflictScope.FromLegacyTopicResourceKey(
                "cluster/prod/consumer/group-a"));
        Assert.Throws<MutationStateException>(() =>
            FleetConflictScope.FromLegacyTopicResourceKey(
                "cluster/prod/topic/payments/events"));
    }
}