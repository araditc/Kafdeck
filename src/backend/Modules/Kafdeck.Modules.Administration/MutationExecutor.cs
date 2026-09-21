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
        MutationExecutionContext context,
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
    private sealed record ProviderExecutionOutcome(
        MutationProviderResult Result,
        Task? LateExecution = null);

    private sealed class LateExecutionPermitState
    {
        public Task? Execution { get; set; }
        public MutationExecutionMaterial? Material { get; set; }
        public Guid OperationId { get; set; }
        public long ExecutionGeneration { get; set; }
        public string? ClusterId { get; set; }
        public bool DurableClusterSlotHeld { get; set; }
    }

    private readonly IMutationOperationRepository _repository;
    private readonly IMutationAuditSink _audit;
    private readonly IMutationPreDispatchGuard _guard;
    private readonly MutationExecutionHandlerRegistry _handlers;
    private readonly IMutationMaterialDigestService _materialDigestService;
    private readonly MutationExecutorPolicy _policy;
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _clusterSemaphores =
        new(StringComparer.Ordinal);

    public MutationExecutor(
        IMutationOperationRepository repository,
        IMutationAuditSink audit,
        IMutationPreDispatchGuard guard,
        MutationExecutionHandlerRegistry handlers,
        IMutationMaterialDigestService materialDigestService,
        MutationExecutorPolicy policy,
        TimeProvider? timeProvider = null)
    {
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _audit = audit ?? throw new ArgumentNullException(nameof(audit));
        _guard = guard ?? throw new ArgumentNullException(nameof(guard));
        _handlers = handlers ?? throw new ArgumentNullException(nameof(handlers));
        _materialDigestService = materialDigestService ??
                                 throw new ArgumentNullException(nameof(materialDigestService));
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Task<MutationOperationSnapshot> ExecuteAsync(
        Guid operationId,
        CancellationToken cancellationToken = default) =>
        ExecuteAsync(operationId, executionMaterial: null, cancellationToken);

    public async Task<MutationOperationSnapshot> ExecuteAsync(
        Guid operationId,
        IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? executionMaterial,
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
        var lateExecution = new LateExecutionPermitState();
        try
        {
            return await ExecuteUnderClusterLimitAsync(
                    operationId,
                    executionMaterial,
                    lateExecution,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            if (lateExecution.Execution is null)
            {
                semaphore.Release();
            }
            else
            {
                _ = ObserveLateCompletionReleasePermitAndDisposeAsync(
                    lateExecution,
                    semaphore);
            }
        }
    }

    private async Task<MutationOperationSnapshot> ExecuteUnderClusterLimitAsync(
        Guid operationId,
        IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? executionMaterialInput,
        LateExecutionPermitState lateExecution,
        CancellationToken cancellationToken)
    {
        var current = await _repository.GetAsync(operationId, cancellationToken).ConfigureAwait(false) ??
                      throw new KeyNotFoundException($"Mutation operation '{operationId:D}' was not found.");

        if (current.State != MutationOperationState.Ready)
        {
            return current;
        }

        MutationExecutionMaterial? executionMaterial =
            new(executionMaterialInput);

        var operation = MutationOperation.Restore(current);
        var expectedVersion = current.Version;
        var claimNowUtc = _timeProvider.GetUtcNow();
        var claimExpiresAtUtc = claimNowUtc.Add(_policy.ResourceClaimTtl);
        var generation = operation.ClaimExecution(claimNowUtc, claimExpiresAtUtc);

        MutationSaveResult claimed;
        try
        {
            claimed = await _repository.TrySaveAsync(
                operation.Snapshot,
                expectedVersion,
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            executionMaterial.Dispose();
            throw;
        }

        if (claimed.Outcome != MutationSaveOutcome.Saved)
        {
            executionMaterial.Dispose();
            return claimed.Operation ??
                   throw new MutationStateException("Mutation execution claim could not be persisted.");
        }

        try
        {
            await WriteAuditAsync(
                operation.Snapshot,
                MutationAuditEventType.ExecutionClaimed,
                "execution_claimed",
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            executionMaterial.Dispose();
            throw;
        }

        var clusterSlotAcquired = false;
        var resourceClaimsAcquired = false;
        var releaseResourceClaims = false;
        try
        {
            var clusterSlot = await _repository.TryAcquireClusterExecutionSlotAsync(
                operation.Snapshot.OperationId,
                generation,
                operation.Snapshot.ClusterId,
                _policy.MaxConcurrentPerCluster,
                claimExpiresAtUtc,
                cancellationToken).ConfigureAwait(false);

            if (clusterSlot.Outcome != MutationClusterSlotOutcome.Acquired)
            {
                var outcomeCode = clusterSlot.Outcome == MutationClusterSlotOutcome.Saturated
                    ? "cluster_concurrency_saturated"
                    : "invalid_execution_claim";

                operation.Complete(
                    MutationExecutionResultKind.FailedBeforeDispatch,
                    outcomeCode,
                    _timeProvider.GetUtcNow());
                await PersistNextAsync(operation, CancellationToken.None).ConfigureAwait(false);
                await WriteAuditAsync(
                    operation.Snapshot,
                    MutationAuditEventType.Completed,
                    outcomeCode,
                    CancellationToken.None).ConfigureAwait(false);
                return operation.Snapshot;
            }

            clusterSlotAcquired = true;
            lateExecution.OperationId = operation.Snapshot.OperationId;
            lateExecution.ExecutionGeneration = generation;
            lateExecution.ClusterId = operation.Snapshot.ClusterId;
            lateExecution.DurableClusterSlotHeld = true;

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

            if (!ExecutionMaterialMatches(operation.Snapshot, executionMaterial))
            {
                operation.Complete(
                    MutationExecutionResultKind.FailedBeforeDispatch,
                    "execution_material_mismatch",
                    _timeProvider.GetUtcNow());
                await PersistNextAsync(operation, CancellationToken.None).ConfigureAwait(false);
                await WriteAuditAsync(
                    operation.Snapshot,
                    MutationAuditEventType.Completed,
                    "execution_material_mismatch",
                    CancellationToken.None).ConfigureAwait(false);
                releaseResourceClaims = true;
                return operation.Snapshot;
            }

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

            var renewalNowUtc = _timeProvider.GetUtcNow();
            var renewalExpectedVersion = operation.Snapshot.Version;
            try
            {
                operation.RenewExecutionLease(
                    renewalNowUtc,
                    renewalNowUtc.Add(_policy.ResourceClaimTtl));
            }
            catch (MutationStateException)
            {
                operation.Complete(
                    MutationExecutionResultKind.FailedBeforeDispatch,
                    "execution_lease_expired_before_dispatch",
                    renewalNowUtc);
                var expiredSave = await _repository.TrySaveAsync(
                    operation.Snapshot,
                    renewalExpectedVersion,
                    CancellationToken.None).ConfigureAwait(false);

                if (expiredSave.Outcome == MutationSaveOutcome.Saved)
                {
                    await WriteAuditAsync(
                        operation.Snapshot,
                        MutationAuditEventType.Completed,
                        "execution_lease_expired_before_dispatch",
                        CancellationToken.None).ConfigureAwait(false);
                    releaseResourceClaims = true;
                    return operation.Snapshot;
                }

                return expiredSave.Operation ??
                       throw new MutationStateException(
                           "Expired execution lease could not be reconciled before dispatch.");
            }

            var renewedLease = await _repository.TryRenewExecutionLeaseAsync(
                operation.Snapshot,
                renewalExpectedVersion,
                CancellationToken.None).ConfigureAwait(false);

            if (renewedLease.Outcome != MutationLeaseRenewOutcome.Renewed)
            {
                return renewedLease.Operation ??
                       throw new MutationStateException(
                           $"Execution lease renewal failed before dispatch: {renewedLease.Outcome}.");
            }

            operation.MarkDispatchStarted(_timeProvider.GetUtcNow());
            await PersistNextAsync(operation, CancellationToken.None).ConfigureAwait(false);
            await TryWritePostDispatchAuditAsync(
                operation.Snapshot,
                MutationAuditEventType.DispatchStarted,
                "dispatch_started").ConfigureAwait(false);

            var executionContext = new MutationExecutionContext(
                operation.Snapshot,
                executionMaterial);
            var providerOutcome = await ExecuteProviderAsync(
                    handler,
                    executionContext)
                .ConfigureAwait(false);
            var providerResult = providerOutcome.Result;

            if (providerOutcome.LateExecution is not null)
            {
                lateExecution.Execution = providerOutcome.LateExecution;
                lateExecution.Material = executionMaterial;
                executionMaterial = null; // Outer permit observer owns material and durable slot from here.
            }

            if (providerResult.ResultKind == MutationExecutionResultKind.FailedBeforeDispatch)
            {
                providerResult = new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "invalid_pre_dispatch_result_after_dispatch");
            }

            operation.Complete(
                providerResult.ResultKind,
                providerResult.ResultCode,
                _timeProvider.GetUtcNow(),
                providerResult.SafeEvidence);

            // Once external dispatch may have occurred, caller cancellation or audit
            // failure must not erase the durable outcome classification or retain a
            // resource claim for a terminal known outcome.
            await PersistNextAsync(operation, CancellationToken.None).ConfigureAwait(false);

            releaseResourceClaims =
                operation.Snapshot.State != MutationOperationState.ExecutionUnknown;

            await TryWritePostDispatchAuditAsync(
                operation.Snapshot,
                MutationAuditEventType.Completed,
                providerResult.ResultCode).ConfigureAwait(false);

            return operation.Snapshot;
        }
        finally
        {
            executionMaterial?.Dispose();

            if (resourceClaimsAcquired && releaseResourceClaims)
            {
                await _repository.ReleaseResourceClaimsAsync(
                    operation.Snapshot.OperationId,
                    generation,
                    CancellationToken.None).ConfigureAwait(false);
            }

            if (clusterSlotAcquired && lateExecution.Execution is null)
            {
                await _repository.ReleaseClusterExecutionSlotAsync(
                    operation.Snapshot.OperationId,
                    generation,
                    CancellationToken.None).ConfigureAwait(false);
                lateExecution.DurableClusterSlotHeld = false;
            }
        }
    }

    private async Task<ProviderExecutionOutcome> ExecuteProviderAsync(
        IMutationExecutionHandler handler,
        MutationExecutionContext context)
    {
        // After DispatchStarted is durable, execution is server-owned. A caller disconnect
        // must not cancel an external mutation and create avoidable outcome ambiguity.
        using var timeout = new CancellationTokenSource(_policy.OperationTimeout);
        Task<MutationProviderResult>? execution = null;

        try
        {
            execution = Task.Run(
                async () => await handler
                    .ExecuteAsync(context, timeout.Token)
                    .ConfigureAwait(false),
                CancellationToken.None);

            var result = await execution
                .WaitAsync(_policy.OperationTimeout)
                .ConfigureAwait(false);
            return new ProviderExecutionOutcome(result);
        }
        catch (TimeoutException)
        {
            timeout.Cancel();
            return new ProviderExecutionOutcome(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "execution_timeout"),
                execution is { IsCompleted: false } ? execution : null);
        }
        catch (OperationCanceledException)
        {
            return new ProviderExecutionOutcome(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "execution_cancelled_or_timeout"),
                execution is { IsCompleted: false } ? execution : null);
        }
        catch (Exception)
        {
            return new ProviderExecutionOutcome(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "provider_exception_after_dispatch"));
        }
    }

    private async Task ObserveLateCompletionReleasePermitAndDisposeAsync(
        LateExecutionPermitState lateExecution,
        SemaphoreSlim semaphore)
    {
        var execution = lateExecution.Execution!;
        var heartbeatInterval = TimeSpan.FromTicks(
            Math.Max(
                TimeSpan.FromSeconds(1).Ticks,
                _policy.ResourceClaimTtl.Ticks / 4));

        try
        {
            while (!execution.IsCompleted)
            {
                var heartbeatDelay = Task.Delay(heartbeatInterval);
                var completed = await Task
                    .WhenAny(execution, heartbeatDelay)
                    .ConfigureAwait(false);

                if (ReferenceEquals(completed, execution))
                {
                    break;
                }

                if (!lateExecution.DurableClusterSlotHeld ||
                    string.IsNullOrWhiteSpace(lateExecution.ClusterId))
                {
                    continue;
                }

                try
                {
                    var renewOutcome = await _repository.TryRenewClusterExecutionSlotAsync(
                        lateExecution.OperationId,
                        lateExecution.ExecutionGeneration,
                        lateExecution.ClusterId,
                        _timeProvider.GetUtcNow().Add(_policy.ResourceClaimTtl),
                        CancellationToken.None).ConfigureAwait(false);

                    if (renewOutcome != MutationClusterSlotRenewOutcome.Renewed)
                    {
                        // The durable lease no longer belongs to this execution. Keep the
                        // local process permit until the provider task finishes, but do not
                        // pretend the shared lease still exists.
                        lateExecution.DurableClusterSlotHeld = false;
                    }
                }
                catch
                {
                    // A transient store outage also prevents healthy peers from acquiring
                    // new durable slots. Keep the local permit and retry on the next heartbeat.
                }
            }

            await execution.ConfigureAwait(false);
        }
        catch
        {
            // The durable operation is already ExecutionUnknown. This observation exists
            // only to consume a late task fault and retain concurrency capacity while the
            // underlying provider call is truly still live.
        }
        finally
        {
            if (lateExecution.DurableClusterSlotHeld)
            {
                try
                {
                    await _repository.ReleaseClusterExecutionSlotAsync(
                        lateExecution.OperationId,
                        lateExecution.ExecutionGeneration,
                        CancellationToken.None).ConfigureAwait(false);
                    lateExecution.DurableClusterSlotHeld = false;
                }
                catch
                {
                    // The lease has an expiry and is reclaimable after heartbeat stops.
                }
            }

            lateExecution.Material!.Dispose();
            semaphore.Release();
        }
    }

    private bool ExecutionMaterialMatches(
        MutationOperationSnapshot operation,
        MutationExecutionMaterial material)
    {
        if (operation.MaterialDigests.Count != material.Count)
        {
            return false;
        }

        foreach (var expected in operation.MaterialDigests)
        {
            if (!material.TryGet(expected.Name, out var value))
            {
                return false;
            }

            var actualDigest = _materialDigestService.ComputeDigest(value.Span);
            if (!string.Equals(actualDigest, expected.Digest, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
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

    private async ValueTask TryWritePostDispatchAuditAsync(
        MutationOperationSnapshot snapshot,
        MutationAuditEventType eventType,
        string outcomeCode)
    {
        try
        {
            await WriteAuditAsync(
                snapshot,
                eventType,
                outcomeCode,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // Dispatch/outcome state is already durable. Audit sink availability must not
            // convert a known execution path into ExecutionUnknown or retain claims.
        }
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
