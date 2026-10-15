using Kafdeck.Core.Notifications;

namespace Kafdeck.Core.Declarative;

public enum CandidateDriftState
{
    InSync, OutOfSync, Missing, Unknown, Inaccessible, Conflict, Unsupported
}

// Evidence must originate from an authorized backend read. This helper is not an authority boundary.
public sealed record CandidateSubscriptionObservation(
    string DeploymentRef,
    string SubscriptionId,
    CandidateDriftState ReadState,
    DateTimeOffset ObservedAtUtc,
    NotificationSubscriptionSnapshot? Snapshot = null);

public static class CandidateSubscriptionDrift
{
    public static CandidateDriftState Compare(CandidateSubscriptionManifest desired,
        CandidateSubscriptionObservation observation, DateTimeOffset nowUtc, TimeSpan maxAge)
    {
        ArgumentNullException.ThrowIfNull(desired);
        ArgumentNullException.ThrowIfNull(observation);
        if (maxAge <= TimeSpan.Zero || maxAge > TimeSpan.FromMinutes(5))
            throw new ArgumentOutOfRangeException(nameof(maxAge));
        if (!Enum.IsDefined(observation.ReadState)) return CandidateDriftState.Unknown;
        // Authorization/capability truth dominates missing/stale/mismatched metadata.
        if (observation.ReadState is CandidateDriftState.Inaccessible or CandidateDriftState.Unsupported)
            return observation.ReadState;
        if (desired.DeploymentRef != observation.DeploymentRef ||
            desired.Definition.SubscriptionId != observation.SubscriptionId)
            return CandidateDriftState.Conflict;
        if (observation.ObservedAtUtc == default || observation.ObservedAtUtc > nowUtc ||
            nowUtc - observation.ObservedAtUtc > maxAge)
            return CandidateDriftState.Unknown;
        if (observation.ReadState is not (CandidateDriftState.InSync or CandidateDriftState.OutOfSync))
            return observation.ReadState == CandidateDriftState.Missing && observation.Snapshot is not null
                ? CandidateDriftState.Unknown : observation.ReadState;
        if (observation.Snapshot is not { } current) return CandidateDriftState.Unknown;
        if (current.Definition.SubscriptionId != observation.SubscriptionId ||
            current.State == NotificationSubscriptionState.Retired)
            return CandidateDriftState.Conflict;
        return desired.Fingerprint() == CandidateSubscriptionManifest.Fingerprint(
            observation.DeploymentRef, current.Definition, current.State)
            ? CandidateDriftState.InSync : CandidateDriftState.OutOfSync;
    }
}
