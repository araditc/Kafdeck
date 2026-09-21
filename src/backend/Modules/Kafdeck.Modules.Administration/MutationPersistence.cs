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
}

public sealed record MutationResourceClaimResult(
    MutationResourceClaimOutcome Outcome,
    string? ConflictingResourceKey = null);

public interface IMutationOperationRepository
{
    Task InitializeAsync(CancellationToken cancellationToken = default);

    Task<MutationCreateResult> CreateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default);

    Task<MutationOperationSnapshot?> GetAsync(
        Guid operationId,
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

    Task ReleaseResourceClaimsAsync(
        Guid operationId,
        long executionClaimGeneration,
        CancellationToken cancellationToken = default);
}
