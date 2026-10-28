namespace Kafdeck.Infrastructure.Persistence;

// Internal pure candidate. Does not authenticate, lock storage or grant runtime mutation.
internal enum W68ReconciliationStatus { InSync, OutOfSync, Missing, Unknown, Inaccessible, Conflict, Unsupported }
internal sealed record W68ReconciliationEvidence(W68ReconciliationStatus Status,bool AuthorizedRead,bool RegisteredOwner,bool FreshLease,bool ImmutablePlan,bool GoverningApproval,bool StorageAvailable);
internal static class W68ReconciliationGate {
 internal static bool CanSchedule(W68ReconciliationEvidence? e) =>
 e is not null && e.AuthorizedRead && e.RegisteredOwner && e.FreshLease &&
 e.ImmutablePlan && e.GoverningApproval && e.StorageAvailable &&
 (e.Status==W68ReconciliationStatus.OutOfSync||e.Status==W68ReconciliationStatus.Missing);
}
