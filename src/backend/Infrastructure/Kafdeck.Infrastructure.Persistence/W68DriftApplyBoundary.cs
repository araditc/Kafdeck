namespace Kafdeck.Infrastructure.Persistence;

// Pure internal safety predicate. Drift classifications are observational,
// never an ownership grant or authorization to mutate Kafka.
internal enum W68DriftObservation { InSync, OutOfSync, Missing, Unknown, Inaccessible, Conflict, Unsupported }
internal sealed record W68DriftAuthorityEvidence(
    W68DriftObservation Status, bool AuthorizedCurrentRead,
    bool RegisteredOwner, bool FreshRevision, bool ImmutablePlan,
    bool GovernedApplyPermission, bool BackendRiskReviewSatisfied);

internal static class W68DriftApplyBoundary
{
    internal static bool CanEnterGovernedApply(W68DriftAuthorityEvidence? e) =>
        e is not null && e.AuthorizedCurrentRead && e.RegisteredOwner &&
        e.FreshRevision && e.ImmutablePlan && e.GovernedApplyPermission &&
        e.BackendRiskReviewSatisfied &&
        e.Status is W68DriftObservation.OutOfSync or W68DriftObservation.Missing;
}
