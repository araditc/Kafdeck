namespace Kafdeck.Modules.Administration;

public enum MutationCreateOutcome
{
    Created = 1,
    ExistingSameIntent = 2,
    IdempotencyConflict = 3,
}

public sealed record MutationCreateResult(
    MutationCreateOutcome Outcome,
    MutationOperationSnapshot Operation);

public enum MutationSaveOutcome
{
    Saved = 1,
    NotFound = 2,
    VersionConflict = 3,
}

public sealed record MutationSaveResult(
    MutationSaveOutcome Outcome,
    MutationOperationSnapshot? Operation);

public enum MutationResourceClaimOutcome
{
    Acquired = 1,
    Conflict = 2,
    InvalidExecutionClaim = 3,
}

public sealed record MutationResourceClaimResult(
    MutationResourceClaimOutcome Outcome,
    string? ConflictingResourceKey = null);

public enum MutationLeaseRenewOutcome
{
    Renewed = 1,
    NotFound = 2,
    VersionConflict = 3,
    InvalidExecutionClaim = 4,
    Expired = 5,
}

public sealed record MutationLeaseRenewResult(
    MutationLeaseRenewOutcome Outcome,
    MutationOperationSnapshot? Operation);

public interface IMutationOperationRepository
{
    Task InitializeAsync(CancellationToken cancellationToken = default);

    Task<MutationCreateResult> CreateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default);

    Task<MutationOperationSnapshot?> GetAsync(
        Guid operationId,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyList<MutationOperationSnapshot>> ListByStateAsync(
        MutationOperationState state,
        int limit,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyList<MutationOperationSnapshot>> ListRecoverableExecutionsAsync(
        DateTimeOffset nowUtc,
        bool includeActiveLeases,
        int limit,
        CancellationToken cancellationToken = default);

    Task<MutationSaveResult> TrySaveAsync(
        MutationOperationSnapshot operation,
        long expectedVersion,
        CancellationToken cancellationToken = default);

    Task<MutationResourceClaimResult> TryAcquireResourceClaimsAsync(
        Guid operationId,
        long executionClaimGeneration,
        IReadOnlyList<string> resourceKeys,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken = default);

    Task<MutationLeaseRenewResult> TryRenewExecutionLeaseAsync(
        MutationOperationSnapshot operation,
        long expectedVersion,
        CancellationToken cancellationToken = default);

    Task ReleaseResourceClaimsAsync(
        Guid operationId,
        long executionClaimGeneration,
        CancellationToken cancellationToken = default);
}
