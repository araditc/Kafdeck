using Kafdeck.Core.Notifications;

namespace Kafdeck.Core.Declarative;

public enum DeclarativeDriftState { InSync, OutOfSync, Missing, Unknown, Inaccessible, Conflict, Unsupported }

// Pure comparison input: the backend must establish current read authority before
// constructing evidence. This object is never accepted as caller authority.
public sealed record DeclarativeSubscriptionObservation(
    string DeploymentRef,
    string SubscriptionId,
    DeclarativeDriftState ReadState,
    DateTimeOffset ObservedAtUtc,
    NotificationSubscriptionSnapshot? Snapshot = null,
    string? ManagedOwnerRef = null);

public static class DeclarativeSubscriptionDrift
{
    public static DeclarativeDriftState Compare(DeclarativeSubscriptionManifest desired,
        DeclarativeSubscriptionObservation observation, DateTimeOffset nowUtc, TimeSpan maxAge)
    {
        ArgumentNullException.ThrowIfNull(desired); ArgumentNullException.ThrowIfNull(observation);
        if (maxAge <= TimeSpan.Zero || maxAge > TimeSpan.FromMinutes(5)) throw new ArgumentOutOfRangeException(nameof(maxAge));
        if (!Enum.IsDefined(observation.ReadState)) return DeclarativeDriftState.Unknown;
        if (observation.ReadState is DeclarativeDriftState.Inaccessible or DeclarativeDriftState.Unsupported)
            return observation.ReadState;
        if (desired.DeploymentRef != observation.DeploymentRef || desired.Definition.SubscriptionId != observation.SubscriptionId)
            return DeclarativeDriftState.Conflict;
        if (observation.ObservedAtUtc == default || observation.ObservedAtUtc > nowUtc || nowUtc - observation.ObservedAtUtc > maxAge)
            return DeclarativeDriftState.Unknown;
        if (observation.ManagedOwnerRef is { } owner && owner != desired.OwnerRef) return DeclarativeDriftState.Conflict;
        if (observation.ReadState is not (DeclarativeDriftState.InSync or DeclarativeDriftState.OutOfSync))
            return observation.ReadState == DeclarativeDriftState.Missing && observation.Snapshot is not null
                ? DeclarativeDriftState.Unknown : observation.ReadState;
        if (observation.Snapshot is not { } current) return DeclarativeDriftState.Unknown;
        if (current.Definition.SubscriptionId != observation.SubscriptionId || current.State == NotificationSubscriptionState.Retired)
            return DeclarativeDriftState.Conflict;
        return current.State == desired.State && current.Definition.DestinationId == desired.Definition.DestinationId &&
               current.Definition.EventClasses.SequenceEqual(desired.Definition.EventClasses) &&
               current.Definition.EventTypes.SequenceEqual(desired.Definition.EventTypes)
            ? DeclarativeDriftState.InSync : DeclarativeDriftState.OutOfSync;
    }
}
