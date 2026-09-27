namespace Kafdeck.Modules.Connect;

public enum ConnectAutoRestartRunOutcome
{
    NotFound = 1,
    NotEligible = 2,
    Waiting = 3,
    LeaseUnavailable = 4,
    PersistenceConflict = 5,
    Recovered = 6,
    Blocked = 7,
    DispatchAccepted = 8,
    DispatchFailedDefinitive = 9,
    DispatchAmbiguous = 10,
    StatePersistenceFailedAfterDispatch = 11,
    AuditFailedBeforeDispatch = 12,
    AuditFailedAfterDispatch = 13,
}

public sealed record ConnectAutoRestartRunResult(
    ConnectAutoRestartRunOutcome Outcome,
    string Code,
    ConnectAutoRestartActivation? Activation);

public sealed record ConnectAutoRestartControllerPolicy
{
    public static ConnectAutoRestartControllerPolicy Default { get; } =
        new(TimeSpan.FromSeconds(30));

    public ConnectAutoRestartControllerPolicy(
        TimeSpan leaseTtl)
    {
        if (leaseTtl < TimeSpan.FromSeconds(5) ||
            leaseTtl > TimeSpan.FromMinutes(5))
        {
            throw new ArgumentOutOfRangeException(nameof(leaseTtl));
        }

        LeaseTtl = leaseTtl;
    }

    public TimeSpan LeaseTtl { get; }
}

public sealed class ConnectAutoRestartController
{
    private readonly IConnectAutoRestartStateStore _store;
    private readonly IConnectAutoRestartAttemptRevalidator _revalidator;
    private readonly IConnectAutoRestartDispatchPort _dispatcher;
    private readonly IConnectAutoRestartAuditSink _audit;
    private readonly ConnectAutoRestartControllerPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public ConnectAutoRestartController(
        IConnectAutoRestartStateStore store,
        IConnectAutoRestartAttemptRevalidator revalidator,
        IConnectAutoRestartDispatchPort dispatcher,
        IConnectAutoRestartAuditSink audit,
        ConnectAutoRestartControllerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _store =
            store ?? throw new ArgumentNullException(nameof(store));
        _revalidator =
            revalidator ??
            throw new ArgumentNullException(nameof(revalidator));
        _dispatcher =
            dispatcher ??
            throw new ArgumentNullException(nameof(dispatcher));
        _audit =
            audit ?? throw new ArgumentNullException(nameof(audit));
        _policy =
            policy ?? ConnectAutoRestartControllerPolicy.Default;
        _timeProvider =
            timeProvider ?? TimeProvider.System;
    }

    public async Task<ConnectAutoRestartRunResult> RunOnceAsync(
        Guid activationId,
        string workerId,
        CancellationToken cancellationToken = default)
    {
        if (activationId == Guid.Empty)
        {
            throw new ArgumentOutOfRangeException(nameof(activationId));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(workerId);

        var initial =
            await _store
                .GetAsync(
                    activationId,
                    cancellationToken)
                .ConfigureAwait(false);

        if (initial is null)
        {
            return Result(
                ConnectAutoRestartRunOutcome.NotFound,
                "auto_restart_activation_not_found",
                null);
        }

        var now = _timeProvider.GetUtcNow();
        var initialEligibility =
            initial.Evaluate(now);

        if (initialEligibility ==
            ConnectAutoRestartEligibility.Waiting)
        {
            return Result(
                ConnectAutoRestartRunOutcome.Waiting,
                "auto_restart_backoff_active",
                initial);
        }

        if (initialEligibility is
            ConnectAutoRestartEligibility.Disabled or
            ConnectAutoRestartEligibility.CircuitOpen or
            ConnectAutoRestartEligibility.DispatchUnresolved or
            ConnectAutoRestartEligibility.Terminal)
        {
            return Result(
                ConnectAutoRestartRunOutcome.NotEligible,
                EligibilityCode(initialEligibility),
                initial);
        }

        var lease =
            await _store
                .TryAcquireLeaseAsync(
                    activationId,
                    workerId,
                    now,
                    _policy.LeaseTtl,
                    cancellationToken)
                .ConfigureAwait(false);

        if (lease is null)
        {
            return Result(
                ConnectAutoRestartRunOutcome.LeaseUnavailable,
                "auto_restart_lease_unavailable",
                initial);
        }

        var current =
            await _store
                .GetAsync(
                    activationId,
                    cancellationToken)
                .ConfigureAwait(false);

        if (current is null)
        {
            await ReleaseLeaseBestEffortAsync(
                    lease,
                    cancellationToken)
                .ConfigureAwait(false);

            return Result(
                ConnectAutoRestartRunOutcome.NotFound,
                "auto_restart_activation_not_found",
                null);
        }

        now = _timeProvider.GetUtcNow();
        var eligibility =
            current.Evaluate(now);

        if (eligibility ==
            ConnectAutoRestartEligibility.LifetimeExpired)
        {
            return await PersistTerminalAsync(
                    current,
                    current.Exhaust(
                        "auto_restart_lifetime_exhausted"),
                    lease,
                    now,
                    "auto_restart_lifetime_exhausted",
                    cancellationToken)
                .ConfigureAwait(false);
        }

        if (eligibility ==
            ConnectAutoRestartEligibility.AttemptsExhausted)
        {
            return await PersistTerminalAsync(
                    current,
                    current.Exhaust(
                        "auto_restart_attempts_exhausted"),
                    lease,
                    now,
                    "auto_restart_attempts_exhausted",
                    cancellationToken)
                .ConfigureAwait(false);
        }

        if (eligibility !=
            ConnectAutoRestartEligibility.Eligible)
        {
            await ReleaseLeaseBestEffortAsync(
                    lease,
                    cancellationToken)
                .ConfigureAwait(false);

            return Result(
                eligibility ==
                ConnectAutoRestartEligibility.Waiting
                    ? ConnectAutoRestartRunOutcome.Waiting
                    : ConnectAutoRestartRunOutcome.NotEligible,
                EligibilityCode(eligibility),
                current);
        }

        ConnectAutoRestartRevalidationResult revalidation;
        try
        {
            revalidation =
                await _revalidator
                    .RevalidateAsync(
                        current,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            await ReleaseLeaseBestEffortAsync(
                    lease,
                    CancellationToken.None)
                .ConfigureAwait(false);
            throw;
        }
        catch
        {
            var blocked =
                current.Block(
                    "auto_restart_revalidation_failed");

            if (!await _store
                    .TryUpdateAsync(
                        blocked,
                        current.Version,
                        lease,
                        _timeProvider.GetUtcNow(),
                        CancellationToken.None)
                    .ConfigureAwait(false))
            {
                return Result(
                    ConnectAutoRestartRunOutcome.PersistenceConflict,
                    "auto_restart_revalidation_failure_persistence_conflict",
                    current);
            }

            await ReleaseLeaseBestEffortAsync(
                    lease,
                    CancellationToken.None)
                .ConfigureAwait(false);

            return Result(
                ConnectAutoRestartRunOutcome.Blocked,
                "auto_restart_revalidation_failed",
                blocked);
        }

        if (revalidation.IsRecovered)
        {
            var recovered =
                current.MarkRecovered(
                    revalidation.Code);

            if (!await _store
                    .TryUpdateAsync(
                        recovered,
                        current.Version,
                        lease,
                        _timeProvider.GetUtcNow(),
                        cancellationToken)
                    .ConfigureAwait(false))
            {
                return Result(
                    ConnectAutoRestartRunOutcome.PersistenceConflict,
                    "auto_restart_recovered_persistence_conflict",
                    current);
            }

            await ReleaseLeaseBestEffortAsync(
                    lease,
                    cancellationToken)
                .ConfigureAwait(false);

            return Result(
                ConnectAutoRestartRunOutcome.Recovered,
                revalidation.Code,
                recovered);
        }

        if (!revalidation.IsAllowed)
        {
            var blocked =
                current.Block(
                    revalidation.Code);

            if (!await _store
                    .TryUpdateAsync(
                        blocked,
                        current.Version,
                        lease,
                        _timeProvider.GetUtcNow(),
                        cancellationToken)
                    .ConfigureAwait(false))
            {
                return Result(
                    ConnectAutoRestartRunOutcome.PersistenceConflict,
                    "auto_restart_block_persistence_conflict",
                    current);
            }

            await ReleaseLeaseBestEffortAsync(
                    lease,
                    cancellationToken)
                .ConfigureAwait(false);

            return Result(
                ConnectAutoRestartRunOutcome.Blocked,
                revalidation.Code,
                blocked);
        }

        var dispatchId = Guid.NewGuid();
        var reserved =
            current.ReserveAttempt(
                _timeProvider.GetUtcNow(),
                dispatchId);

        if (!await _store
                .TryUpdateAsync(
                    reserved,
                    current.Version,
                    lease,
                    _timeProvider.GetUtcNow(),
                    cancellationToken)
                .ConfigureAwait(false))
        {
            await ReleaseLeaseBestEffortAsync(
                    lease,
                    cancellationToken)
                .ConfigureAwait(false);

            return Result(
                ConnectAutoRestartRunOutcome.PersistenceConflict,
                "auto_restart_attempt_reservation_conflict",
                current);
        }

        try
        {
            await _audit
                .WriteAsync(
                    AuditEvent(
                        reserved,
                        dispatchId,
                        ConnectAutoRestartAuditEventType.DispatchStarted,
                        "auto_restart_dispatch_started",
                        _timeProvider.GetUtcNow()),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            var blocked =
                reserved.Block(
                    "auto_restart_audit_unavailable");

            bool blockedPersisted;
            try
            {
                blockedPersisted =
                    await _store
                        .TryUpdateAsync(
                            blocked,
                            reserved.Version,
                            lease,
                            _timeProvider.GetUtcNow(),
                            CancellationToken.None)
                        .ConfigureAwait(false);
            }
            catch
            {
                blockedPersisted = false;
            }

            if (blockedPersisted)
            {
                await ReleaseLeaseBestEffortAsync(
                        lease,
                        CancellationToken.None)
                    .ConfigureAwait(false);

                return Result(
                    ConnectAutoRestartRunOutcome.AuditFailedBeforeDispatch,
                    "auto_restart_audit_unavailable",
                    blocked);
            }

            return Result(
                ConnectAutoRestartRunOutcome.PersistenceConflict,
                "auto_restart_audit_failure_persistence_conflict",
                reserved);
        }

        ConnectAutoRestartDispatchResult dispatch;
        try
        {
            dispatch =
                await _dispatcher
                    .RestartAsync(
                        new ConnectAutoRestartDispatchRequest(
                            reserved.ActivationId,
                            dispatchId,
                            reserved.Target,
                            reserved.AutomationPrincipalId),
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            dispatch =
                new ConnectAutoRestartDispatchResult(
                    ConnectAutoRestartDispatchOutcome.Ambiguous,
                    "auto_restart_dispatch_cancelled_or_unknown");
        }
        catch
        {
            dispatch =
                new ConnectAutoRestartDispatchResult(
                    ConnectAutoRestartDispatchOutcome.Ambiguous,
                    "auto_restart_dispatch_exception_unknown");
        }

        try
        {
            await _audit
                .WriteAsync(
                    AuditEvent(
                        reserved,
                        dispatchId,
                        ConnectAutoRestartAuditEventType.DispatchOutcome,
                        dispatch.Code,
                        _timeProvider.GetUtcNow()),
                    CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch
        {
            // Provider dispatch has already occurred. Preserve the durable
            // Dispatching + unresolved marker and require reconciliation.
            return Result(
                ConnectAutoRestartRunOutcome.AuditFailedAfterDispatch,
                "auto_restart_post_dispatch_audit_failed",
                reserved);
        }

        var next = dispatch.Outcome switch
        {
            ConnectAutoRestartDispatchOutcome.Accepted =>
                reserved.RecordAccepted(
                    dispatchId,
                    dispatch.Code),

            ConnectAutoRestartDispatchOutcome.FailedDefinitive =>
                reserved.RecordDefinitiveFailure(
                    _timeProvider.GetUtcNow(),
                    dispatchId,
                    dispatch.Code),

            ConnectAutoRestartDispatchOutcome.Blocked =>
                reserved.Block(
                    dispatch.Code),

            _ =>
                reserved.RecordAmbiguous(
                    dispatchId,
                    dispatch.Code),
        };

        bool persisted;
        try
        {
            persisted =
                await _store
                    .TryUpdateAsync(
                        next,
                        reserved.Version,
                        lease,
                        _timeProvider.GetUtcNow(),
                        CancellationToken.None)
                    .ConfigureAwait(false);
        }
        catch
        {
            persisted = false;
        }

        if (!persisted)
        {
            // The pre-dispatch durable reservation is intentionally left in
            // Dispatching + unresolved state. A later worker must reconcile;
            // it must never redispatch blindly.
            return Result(
                ConnectAutoRestartRunOutcome
                    .StatePersistenceFailedAfterDispatch,
                "auto_restart_post_dispatch_persistence_failed",
                reserved);
        }

        await ReleaseLeaseBestEffortAsync(
                lease,
                CancellationToken.None)
            .ConfigureAwait(false);

        return dispatch.Outcome switch
        {
            ConnectAutoRestartDispatchOutcome.Accepted =>
                Result(
                    ConnectAutoRestartRunOutcome.DispatchAccepted,
                    dispatch.Code,
                    next),

            ConnectAutoRestartDispatchOutcome.FailedDefinitive =>
                Result(
                    ConnectAutoRestartRunOutcome.DispatchFailedDefinitive,
                    dispatch.Code,
                    next),

            ConnectAutoRestartDispatchOutcome.Blocked =>
                Result(
                    ConnectAutoRestartRunOutcome.Blocked,
                    dispatch.Code,
                    next),

            _ =>
                Result(
                    ConnectAutoRestartRunOutcome.DispatchAmbiguous,
                    dispatch.Code,
                    next),
        };
    }

    private async Task<ConnectAutoRestartRunResult> PersistTerminalAsync(
        ConnectAutoRestartActivation current,
        ConnectAutoRestartActivation terminal,
        ConnectAutoRestartLease lease,
        DateTimeOffset now,
        string code,
        CancellationToken cancellationToken)
    {
        if (!await _store
                .TryUpdateAsync(
                    terminal,
                    current.Version,
                    lease,
                    now,
                    cancellationToken)
                .ConfigureAwait(false))
        {
            return Result(
                ConnectAutoRestartRunOutcome.PersistenceConflict,
                $"{code}_persistence_conflict",
                current);
        }

        await ReleaseLeaseBestEffortAsync(
                lease,
                cancellationToken)
            .ConfigureAwait(false);

        return Result(
            ConnectAutoRestartRunOutcome.NotEligible,
            code,
            terminal);
    }

    private async Task ReleaseLeaseBestEffortAsync(
        ConnectAutoRestartLease lease,
        CancellationToken cancellationToken)
    {
        try
        {
            _ = await _store
                .ReleaseLeaseAsync(
                    lease,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            // Lease expiry/takeover is already fenced by generation.
        }
    }

    private static ConnectAutoRestartAuditEvent AuditEvent(
        ConnectAutoRestartActivation activation,
        Guid dispatchId,
        ConnectAutoRestartAuditEventType eventType,
        string code,
        DateTimeOffset timestampUtc) =>
        new(
            timestampUtc,
            eventType,
            activation.ActivationId,
            dispatchId,
            activation.Target.CanonicalKey,
            activation.AutomationPrincipalId,
            activation.AttemptsUsed,
            code);

    private static string EligibilityCode(
        ConnectAutoRestartEligibility eligibility) =>
        eligibility switch
        {
            ConnectAutoRestartEligibility.Disabled =>
                "auto_restart_disabled",
            ConnectAutoRestartEligibility.Waiting =>
                "auto_restart_backoff_active",
            ConnectAutoRestartEligibility.LifetimeExpired =>
                "auto_restart_lifetime_exhausted",
            ConnectAutoRestartEligibility.AttemptsExhausted =>
                "auto_restart_attempts_exhausted",
            ConnectAutoRestartEligibility.CircuitOpen =>
                "auto_restart_circuit_open",
            ConnectAutoRestartEligibility.DispatchUnresolved =>
                "auto_restart_dispatch_unresolved",
            ConnectAutoRestartEligibility.Terminal =>
                "auto_restart_terminal",
            _ =>
                "auto_restart_eligible",
        };

    private static ConnectAutoRestartRunResult Result(
        ConnectAutoRestartRunOutcome outcome,
        string code,
        ConnectAutoRestartActivation? activation) =>
        new(
            outcome,
            code,
            activation);
}
