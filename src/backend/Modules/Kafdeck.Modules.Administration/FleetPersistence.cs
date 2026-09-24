namespace Kafdeck.Modules.Administration;

public enum FleetProgressCreateOutcome
{
    Created = 1,
    Existing = 2,
    ParentOperationNotFound = 3,
}

public sealed record FleetProgressCreateResult(
    FleetProgressCreateOutcome Outcome,
    FleetOperationProgressSnapshot? Progress);

public enum FleetProgressSaveOutcome
{
    Saved = 1,
    NotFound = 2,
    VersionConflict = 3,
}

public sealed record FleetProgressSaveResult(
    FleetProgressSaveOutcome Outcome,
    FleetOperationProgressSnapshot? Progress);

public enum FleetConflictObligationCreateOutcome
{
    Created = 1,
    ExistingSameEffect = 2,
    ExistingDifferentEffect = 3,
    ParentOperationNotFound = 4,
    LegacyResourceClaimConflict = 5,
}

public sealed record FleetConflictObligationCreateResult(
    FleetConflictObligationCreateOutcome Outcome,
    FleetConflictObligationSnapshot? Obligation);

public enum FleetConflictObligationSaveOutcome
{
    Saved = 1,
    NotFound = 2,
    VersionConflict = 3,
    ImmutableIdentityConflict = 4,
}

public sealed record FleetConflictObligationSaveResult(
    FleetConflictObligationSaveOutcome Outcome,
    FleetConflictObligationSnapshot? Obligation);

/// <summary>
/// Durable subordinate state for v0.6 fleet operations. The parent
/// <see cref="MutationOperationSnapshot"/> remains the sole approval,
/// idempotency and terminal-outcome authority; this store is not a second
/// executor or operation aggregate.
/// </summary>
public interface IFleetMutationStateStore
{
    Task InitializeAsync(CancellationToken cancellationToken = default);

    Task<FleetProgressCreateResult> CreateProgressAsync(
        FleetOperationProgressSnapshot progress,
        CancellationToken cancellationToken = default);

    Task<FleetOperationProgressSnapshot?> GetProgressAsync(
        Guid operationId,
        CancellationToken cancellationToken = default);

    Task<FleetProgressSaveResult> TrySaveProgressAsync(
        FleetOperationProgressSnapshot progress,
        long expectedVersion,
        CancellationToken cancellationToken = default);

    Task<FleetConflictObligationCreateResult> CreateConflictObligationAsync(
        FleetConflictObligationSnapshot obligation,
        CancellationToken cancellationToken = default);

    Task<FleetConflictObligationSnapshot?> GetConflictObligationAsync(
        Guid obligationId,
        CancellationToken cancellationToken = default);

    Task<FleetConflictObligationSnapshot?> FindBlockingConflictObligationAsync(
        string conflictKey,
        CancellationToken cancellationToken = default);

    Task<FleetConflictObligationSaveResult> TrySaveConflictObligationAsync(
        FleetConflictObligationSnapshot obligation,
        long expectedVersion,
        CancellationToken cancellationToken = default);
}
