namespace Kafdeck.Modules.Administration;

/// <summary>
/// Canonical shared serialization scope for the W41 bridge between legacy
/// v0.5 resource claims and durable v0.6 fleet conflict obligations.
///
/// A scope is deliberately coarser than a fleet conflict key only where the
/// admitted conflict relation is already conservative: topic/partition/topic-
/// configuration targets collapse to the physical topic, and broker/broker-
/// configuration targets collapse to the physical broker. Other target kinds
/// keep their complete typed identity. The returned value is itself a canonical
/// <see cref="FleetConflictKeyCodec"/> key and is safe to hash for a durable
/// database guard row.
/// </summary>
public static class FleetConflictScope
{
    public static string FromFleetConflictKey(string conflictKey) =>
        FromTarget(FleetConflictKeyCodec.Decode(conflictKey));

    /// <summary>
    /// Maps only the already-admitted v0.5 topic resource-key shape into the
    /// same physical-topic scope used by fleet topic effects. Unsupported or
    /// ambiguous legacy shapes fail closed instead of being guessed.
    /// </summary>
    public static string FromLegacyTopicResourceKey(string resourceKey) =>
        FromTarget(FleetConflictRelation.ParseLegacyTopicResourceKey(resourceKey));

    public static string FromTarget(FleetConflictTarget target)
    {
        ArgumentNullException.ThrowIfNull(target);

        return target.Kind switch
        {
            FleetConflictTargetKind.Topic or
            FleetConflictTargetKind.TopicPartition or
            FleetConflictTargetKind.TopicConfiguration =>
                FleetConflictKeyCodec.Topic(
                    target.PhysicalClusterId,
                    target.ResourceId),

            FleetConflictTargetKind.Broker or
            FleetConflictTargetKind.BrokerConfiguration =>
                FleetConflictKeyCodec.Broker(
                    target.PhysicalClusterId,
                    target.ResourceId),

            _ => FleetConflictKeyCodec.Encode(target),
        };
    }
}