using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W41FleetConflictRelationTests
{
    [Fact]
    public void Conflict_key_round_trips_versioned_length_checked_identity()
    {
        var encoded = FleetConflictKeyCodec.TopicConfiguration(
            "prod-α",
            "payments.events",
            "min.insync.replicas");

        var decoded = FleetConflictKeyCodec.Decode(encoded);

        Assert.Equal(FleetConflictTargetKind.TopicConfiguration, decoded.Kind);
        Assert.Equal("prod-α", decoded.PhysicalClusterId);
        Assert.Equal("payments.events", decoded.ResourceId);
        Assert.Equal("min.insync.replicas", decoded.SubresourceId);
        Assert.Equal(encoded, FleetConflictKeyCodec.Encode(decoded));
    }

    [Fact]
    public void Tampered_length_prefix_is_rejected_fail_closed()
    {
        var encoded = FleetConflictKeyCodec.Topic("prod", "payments.events");
        var marker = "4:cHJvZA";
        Assert.Contains(marker, encoded, StringComparison.Ordinal);

        var tampered = encoded.Replace(marker, "5:cHJvZA", StringComparison.Ordinal);

        Assert.Throws<MutationStateException>(() =>
            FleetConflictKeyCodec.Decode(tampered));
    }

    [Fact]
    public void Topic_parent_blocks_partition_and_configuration_children()
    {
        var topic = FleetConflictKeyCodec.Decode(
            FleetConflictKeyCodec.Topic("prod", "payments.events"));
        var partition = FleetConflictKeyCodec.Decode(
            FleetConflictKeyCodec.TopicPartition("prod", "payments.events", 3));
        var config = FleetConflictKeyCodec.Decode(
            FleetConflictKeyCodec.TopicConfiguration(
                "prod",
                "payments.events",
                "min.insync.replicas"));

        Assert.True(FleetConflictRelation.Conflicts(topic, partition));
        Assert.True(FleetConflictRelation.Conflicts(partition, config));
        Assert.True(FleetConflictRelation.Conflicts(topic, config));
    }

    [Fact]
    public void Topic_conflict_relation_does_not_cross_topic_or_physical_cluster()
    {
        var partition = FleetConflictKeyCodec.Decode(
            FleetConflictKeyCodec.TopicPartition("prod", "payments.events", 1));
        var otherTopic = FleetConflictKeyCodec.Decode(
            FleetConflictKeyCodec.Topic("prod", "audit.events"));
        var otherCluster = FleetConflictKeyCodec.Decode(
            FleetConflictKeyCodec.Topic("dr", "payments.events"));

        Assert.False(FleetConflictRelation.Conflicts(partition, otherTopic));
        Assert.False(FleetConflictRelation.Conflicts(partition, otherCluster));
    }

    [Fact]
    public void Legacy_v05_topic_resource_key_conflicts_with_fleet_partition_or_config_child()
    {
        const string legacy = "cluster/prod/topic/payments.events";

        Assert.True(FleetConflictRelation.ConflictsWithLegacyResourceKey(
            FleetConflictKeyCodec.TopicPartition("prod", "payments.events", 7),
            legacy));
        Assert.True(FleetConflictRelation.ConflictsWithLegacyResourceKey(
            FleetConflictKeyCodec.TopicConfiguration(
                "prod",
                "payments.events",
                "retention.ms"),
            legacy));
        Assert.False(FleetConflictRelation.ConflictsWithLegacyResourceKey(
            FleetConflictKeyCodec.TopicPartition("prod", "audit.events", 7),
            legacy));
    }

    [Fact]
    public void Legacy_bridge_rejects_non_topic_or_ambiguous_resource_keys()
    {
        Assert.Throws<MutationStateException>(() =>
            FleetConflictRelation.ParseLegacyTopicResourceKey(
                "cluster/prod/consumer/group-a"));
        Assert.Throws<MutationStateException>(() =>
            FleetConflictRelation.ParseLegacyTopicResourceKey(
                "cluster/prod/topic/payments/events"));
    }

    [Fact]
    public void Broker_parent_blocks_its_config_but_not_another_broker()
    {
        var broker = FleetConflictKeyCodec.Decode(
            FleetConflictKeyCodec.Broker("prod", "broker-7"));
        var config = FleetConflictKeyCodec.Decode(
            FleetConflictKeyCodec.BrokerConfiguration(
                "prod",
                "broker-7",
                "leader.replication.throttled.rate"));
        var other = FleetConflictKeyCodec.Decode(
            FleetConflictKeyCodec.Broker("prod", "broker-8"));

        Assert.True(FleetConflictRelation.Conflicts(broker, config));
        Assert.False(FleetConflictRelation.Conflicts(config, other));
    }
}
