using System.Collections.Concurrent;

namespace Kafdeck.Modules.Administration;

public enum MutationPreDispatchGuardOutcome
{
    Allowed = 1,
    AuthorizationDenied = 2,
    CapabilityUnsupported = 3,
    StalePreview = 4,
}

public sealed record MutationPreDispatchGuardResult(
    MutationPreDispatchGuardOutcome Outcome,
    string ResultCode)
{
    public static MutationPreDispatchGuardResult Allowed { get; } =
        new(MutationPreDispatchGuardOutcome.Allowed, "allowed");
}

public interface IMutationPreDispatchGuard
{
    Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default);
}

public sealed class FailClosedMutationPreDispatchGuard : IMutationPreDispatchGuard
{
    public Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(new MutationPreDispatchGuardResult(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            "mutation_handler_not_admitted"));
    }
}

public interface IMutationExecutionHandler
{
    MutationOperationKind OperationKind { get; }

    Task<MutationProviderResult> ExecuteAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default);
}

public sealed class MutationExecutionHandlerRegistry
{
    private readonly IReadOnlyDictionary<MutationOperationKind, IMutationExecutionHandler> _handlers;

    public MutationExecutionHandlerRegistry(IEnumerable<IMutationExecutionHandler> handlers)
    {
        ArgumentNullException.ThrowIfNull(handlers);

        var byKind = new Dictionary<MutationOperationKind, IMutationExecutionHandler>();
        foreach (var handler in handlers)
        {
            ArgumentNullException.ThrowIfNull(handler);
            if (!byKind.TryAdd(handler.OperationKind, handler))
            {
                throw new InvalidOperationException(
                    $"More than one mutation handler is registered for '{handler.OperationKind}'.");
            }
        }

        _handlers = byKind;
    }

    public bool TryGet(
        MutationOperationKind operationKind,
        out IMutationExecutionHandler? handler) =>
        _handlers.TryGetValue(operationKind, out handler);
}

public sealed record MutationExecutorPolicy
{
    public MutationExecutorPolicy(
        int maxConcurrentPerCluster,
        TimeSpan operationTimeout,
        TimeSpan resourceClaimTtl,
        int maxTrackedClusters = 256)
    {
        if (maxConcurrentPerCluster is < 1 or > 16)
        {
            throw new ArgumentOutOfRangeException(nameof(maxConcurrentPerCluster));
        }

        if (operationTimeout < TimeSpan.FromSeconds(1) || operationTimeout > TimeSpan.FromMinutes(2))
        {
            throw new ArgumentOutOfRangeException(nameof(operationTimeout));
        }

        if (resourceClaimTtl <= operationTimeout + TimeSpan.FromSeconds(10) ||
            resourceClaimTtl > TimeSpan.FromMinutes(10))
        {
            throw new ArgumentOutOfRangeException(
                nameof(resourceClaimTtl),
                "Resource-claim TTL must exceed operation timeout by at least ten seconds and be at most ten minutes.");
        }

        if (maxTrackedClusters is < 1 or > 4096)
        {
            throw new ArgumentOutOfRangeException(nameof(maxTrackedClusters));
        }

        MaxConcurrentPerCluster = maxConcurrentPerCluster;
        OperationTimeout = operationTimeout;
        ResourceClaimTtl = resourceClaimTtl;
        MaxTrackedClusters = maxTrackedClusters;
    }

    public int MaxConcurrentPerCluster { get; }
    public TimeSpan OperationTimeout { get; }
    public TimeSpan ResourceClaimTtl { get; }
    public int MaxTrackedClusters { get; }
}

public sealed class MutationExecutor
{
    private readonly IMutationOperationRepository _repository;
    private readonly IMutationAuditSink _audit;
    private readonly IMutationPreDispatchGuard _guard;
    private readonly MutationExecutionHandlerRegistry _handlers;
    private readonly MutationExecutorPolicy _policy;
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _clusterSemaphores =
        new(StringComparer.Ordinal);

    public MutationExecutor(
        IMutationOperationRepository repository,
        IMutationAuditSink audit,
        IMutationPreDispatchGuard guard,
        MutationExecutionHandlerRegistry handlers,
        MutationExecutorPolicy policy,
        TimeProvider? timeProvider = null)
    {
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _audit = audit ?? throw new ArgumentNullException(nameof(audit));
        _guard = guard ?? throw new ArgumentNullException(nameof(guard));
        _handlers = handlers ?? throw new ArgumentNullException(nameof(handlers));
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationOperationSnapshot> ExecuteAsync(
        Guid operationId,
        CancellationToken cancellationToken = default)
    {
        var initial = await _repository.GetAsync(operationId, cancellationToken).ConfigureAwait(false) ??
                      throw new KeyNotFoundException($"Mutation operation '{operationId:D}' was not found.");

        if (initial.State != MutationOperationState.Ready)
        {
            return initial;
        }

        var semaphore = GetClusterSemaphore(initial.ClusterId);
        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await ExecuteUnderClusterLimitAsync(operationId, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task<MutationOperationSnapshot> ExecuteUnderClusterLimitAsync(
        Guid operationId,
        CancellationToken cancellationToken)
    {
        var current = await _repository.GetAsync(operationId, cancellationToken).ConfigureAwait(false) ??
                      throw new KeyNotFoundException($"Mutation operation '{operationId:D}' was not found.");

        if (current.State != MutationOperationState.Ready)
        {
            return current;
        }

        var operation = MutationOperation.Restore(current);
        var expectedVersion = current.Version;
        var claimNowUtc = _timeProvider.GetUtcNow();
        var claimExpiresAtUtc = claimNowUtc.Add(_policy.ResourceClaimTtl);
        var generation = operation.ClaimExecution(claimNowUtc, claimExpiresAtUtc);

        var claimed = await _repository.TrySaveAsync(
            operation.Snapshot,
            expectedVersion,
            cancellationToken).ConfigureAwait(false);

        if (claimed.Outcome != MutationSaveOutcome.Saved)
        {
            return claimed.Operation ??
                   throw new MutationStateException("Mutation execution claim could not be persisted.");
        }

        await WriteAuditAsync(
            operation.Snapshot,
            MutationAuditEventType.ExecutionClaimed,
            "execution_claimed",
            cancellationToken).ConfigureAwait(false);

        var resourceClaimsAcquired = false;
        var releaseResourceClaims = false;
        try
        {
            var claimResult = await _repository.TryAcquireResourceClaimsAsync(
                operation.Snapshot.OperationId,
                generation,
                operation.Snapshot.ResourceKeys,
                claimExpiresAtUtc,
                cancellationToken).ConfigureAwait(false);

            if (claimResult.Outcome != MutationResourceClaimOutcome.Acquired)
            {
                var outcomeCode = claimResult.Outcome == MutationResourceClaimOutcome.Conflict
                    ? "resource_conflict"
                    : "invalid_execution_claim";

                operation.Complete(
                    MutationExecutionResultKind.FailedBeforeDispatch,
                    outcomeCode,
                    _timeProvider.GetUtcNow());
                await PersistNextAsync(operation, CancellationToken.None).ConfigureAwait(false);
                await WriteAuditAsync(
                    operation.Snapshot,
                    claimResult.Outcome == MutationResourceClaimOutcome.Conflict
                        ? MutationAuditEventType.ResourceConflict
                        : MutationAuditEventType.Completed,
                    outcomeCode,
                    CancellationToken.None).ConfigureAwait(false);
                return operation.Snapshot;
            }

            resourceClaimsAcquired = true;

            MutationPreDispatchGuardResult guard;
            try
            {
                guard = await _guard.ValidateAsync(
                    operation.Snapshot,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                operation.Complete(
                    MutationExecutionResultKind.FailedBeforeDispatch,
                    "pre_dispatch_cancelled",
                    _timeProvider.GetUtcNow());
                await PersistNextAsync(operation, CancellationToken.None).ConfigureAwait(false);
                await WriteAuditAsync(
                    operation.Snapshot,
                    MutationAuditEventType.Completed,
                    "pre_dispatch_cancelled",
                    CancellationToken.None).ConfigureAwait(false);
                releaseResourceClaims = true;
                return operation.Snapshot;
            }
            catch (Exception)
            {
                operation.Complete(
                    MutationExecutionResultKind.FailedBeforeDispatch,
                    "pre_dispatch_guard_failed",
                    _timeProvider.GetUtcNow());
                await PersistNextAsync(operation, CancellationToken.None).ConfigureAwait(false);
                await WriteAuditAsync(
                    operation.Snapshot,
                    MutationAuditEventType.Completed,
                    "pre_dispatch_guard_failed",
                    CancellationToken.None).ConfigureAwait(false);
                releaseResourceClaims = true;
                return operation.Snapshot;
            }

            if (guard.Outcome == MutationPreDispatchGuardOutcome.StalePreview)
            {
                operation.MarkStaleBeforeDispatch(_timeProvider.GetUtcNow());
                await PersistNextAsync(operation, cancellationToken).ConfigureAwait(false);
                await WriteAuditAsync(
                    operation.Snapshot,
                    MutationAuditEventType.StalePreview,
                    guard.ResultCode,
                    cancellationToken).ConfigureAwait(false);
                releaseResourceClaims = true;
                return operation.Snapshot;
            }

            if (guard.Outcome != MutationPreDispatchGuardOutcome.Allowed)
            {
                operation.Complete(
                    MutationExecutionResultKind.FailedBeforeDispatch,
                    guard.ResultCode,
                    _timeProvider.GetUtcNow());
                await PersistNextAsync(operation, cancellationToken).ConfigureAwait(false);
                await WriteAuditAsync(
                    operation.Snapshot,
                    MutationAuditEventType.Completed,
                    guard.ResultCode,
                    cancellationToken).ConfigureAwait(false);
                releaseResourceClaims = true;
                return operation.Snapshot;
            }

            if (!_handlers.TryGet(operation.Snapshot.OperationKind, out var handler) || handler is null)
            {
                operation.Complete(
                    MutationExecutionResultKind.FailedBeforeDispatch,
                    "mutation_handler_not_admitted",
                    _timeProvider.GetUtcNow());
                await PersistNextAsync(operation, cancellationToken).ConfigureAwait(false);
                await WriteAuditAsync(
                    operation.Snapshot,
                    MutationAuditEventType.Completed,
                    "mutation_handler_not_admitted",
                    cancellationToken).ConfigureAwait(false);
                releaseResourceClaims = true;
                return operation.Snapshot;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                operation.Complete(
                    MutationExecutionResultKind.FailedBeforeDispatch,
                    "cancelled_before_dispatch",
                    _timeProvider.GetUtcNow());
                await PersistNextAsync(operation, CancellationToken.None).ConfigureAwait(false);
                await WriteAuditAsync(
                    operation.Snapshot,
                    MutationAuditEventType.Completed,
                    "cancelled_before_dispatch",
                    CancellationToken.None).ConfigureAwait(false);
                releaseResourceClaims = true;
                return operation.Snapshot;
            }

            operation.MarkDispatchStarted(_timeProvider.GetUtcNow());
            await PersistNextAsync(operation, CancellationToken.None).ConfigureAwait(false);
            await WriteAuditAsync(
                operation.Snapshot,
                MutationAuditEventType.DispatchStarted,
                "dispatch_started",
                CancellationToken.None).ConfigureAwait(false);

            var providerResult = await ExecuteProviderAsync(
                handler,
                operation.Snapshot).ConfigureAwait(false);

            if (providerResult.ResultKind == MutationExecutionResultKind.FailedBeforeDispatch)
            {
                providerResult = new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "invalid_pre_dispatch_result_after_dispatch");
            }

            operation.Complete(
                providerResult.ResultKind,
                providerResult.ResultCode,
                _timeProvider.GetUtcNow());

            // Once external dispatch may have occurred, caller cancellation must not erase
            // the durable outcome classification. Persist/audit under an internal token.
            await PersistNextAsync(operation, CancellationToken.None).ConfigureAwait(false);
            await WriteAuditAsync(
                operation.Snapshot,
                MutationAuditEventType.Completed,
                providerResult.ResultCode,
                CancellationToken.None).ConfigureAwait(false);

            releaseResourceClaims =
                operation.Snapshot.State != MutationOperationState.ExecutionUnknown;
            return operation.Snapshot;
        }
        finally
        {
            if (resourceClaimsAcquired && releaseResourceClaims)
            {
                await _repository.ReleaseResourceClaimsAsync(
                    operation.Snapshot.OperationId,
                    generation,
                    CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    private async Task<MutationProviderResult> ExecuteProviderAsync(
        IMutationExecutionHandler handler,
        MutationOperationSnapshot operation)
    {
        // After DispatchStarted is durable, execution is server-owned. A caller disconnect
        // must not cancel an external mutation and create avoidable outcome ambiguity.
        using var timeout = new CancellationTokenSource(_policy.OperationTimeout);
        Task<MutationProviderResult>? execution = null;

        try
        {
            execution = handler.ExecuteAsync(operation, timeout.Token);
            return await execution
                .WaitAsync(_policy.OperationTimeout)
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            timeout.Cancel();
            if (execution is not null)
            {
                _ = ObserveLateCompletionAsync(execution);
            }

            return new MutationProviderResult(
                MutationExecutionResultKind.ExecutionUnknown,
                "execution_timeout");
        }
        catch (OperationCanceledException)
        {
            if (execution is not null && !execution.IsCompleted)
            {
                _ = ObserveLateCompletionAsync(execution);
            }

            return new MutationProviderResult(
                MutationExecutionResultKind.ExecutionUnknown,
                "execution_cancelled_or_timeout");
        }
        catch (Exception)
        {
            return new MutationProviderResult(
                MutationExecutionResultKind.ExecutionUnknown,
                "provider_exception_after_dispatch");
        }
    }

    private static async Task ObserveLateCompletionAsync(Task execution)
    {
        try
        {
            await execution.ConfigureAwait(false);
        }
        catch
        {
            // The durable operation is already ExecutionUnknown. This observation exists
            // only to consume a late task fault without changing the persisted outcome.
        }
    }

    private async Task PersistNextAsync(
        MutationOperation operation,
        CancellationToken cancellationToken)
    {
        var expectedVersion = checked(operation.Snapshot.Version - 1);
        var saved = await _repository.TrySaveAsync(
            operation.Snapshot,
            expectedVersion,
            cancellationToken).ConfigureAwait(false);

        if (saved.Outcome != MutationSaveOutcome.Saved)
        {
            throw new MutationStateException(
                $"Mutation state transition could not be persisted: {saved.Outcome}.");
        }
    }

    private SemaphoreSlim GetClusterSemaphore(string clusterId)
    {
        if (_clusterSemaphores.TryGetValue(clusterId, out var existing))
        {
            return existing;
        }

        if (_clusterSemaphores.Count >= _policy.MaxTrackedClusters)
        {
            throw new InvalidOperationException("Mutation executor cluster limit has been reached.");
        }

        return _clusterSemaphores.GetOrAdd(
            clusterId,
            _ => new SemaphoreSlim(
                _policy.MaxConcurrentPerCluster,
                _policy.MaxConcurrentPerCluster));
    }

    private ValueTask WriteAuditAsync(
        MutationOperationSnapshot snapshot,
        MutationAuditEventType eventType,
        string outcomeCode,
        CancellationToken cancellationToken) =>
        _audit.WriteAsync(
            new MutationAuditEvent(
                _timeProvider.GetUtcNow(),
                eventType,
                snapshot.OperationId,
                snapshot.RequesterPrincipalId,
                snapshot.ClusterId,
                snapshot.OperationKind,
                snapshot.Risk.RiskClass,
                snapshot.State,
                snapshot.ResourceKeys,
                snapshot.PreviewHash,
                outcomeCode),
            cancellationToken);
}
