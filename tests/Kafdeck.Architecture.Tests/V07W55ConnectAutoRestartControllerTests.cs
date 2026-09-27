using System.Security.Cryptography;
using System.Text;
using Kafdeck.Modules.Connect;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W55ConnectAutoRestartControllerTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 27, 8, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Revalidation_denial_blocks_before_dispatch()
    {
        var activation = Activation();
        var store = new InMemoryStore(activation);
        var dispatcher = new FakeDispatcher(
            new ConnectAutoRestartDispatchResult(
                ConnectAutoRestartDispatchOutcome.Accepted,
                "accepted"));
        var controller = Controller(
            store,
            new FakeRevalidator(
                ConnectAutoRestartRevalidationOutcome.AuthorizationDenied,
                "auto_restart_authorization_revoked"),
            dispatcher);

        var result = await controller.RunOnceAsync(
            activation.ActivationId,
            "worker-a");

        Assert.Equal(
            ConnectAutoRestartRunOutcome.Blocked,
            result.Outcome);
        Assert.Equal(0, dispatcher.Calls);
        Assert.Equal(
            ConnectAutoRestartCircuitState.Blocked,
            store.Snapshot.CircuitState);
        Assert.Equal(
            "auto_restart_authorization_revoked",
            store.Snapshot.TerminalReason);
    }

    [Fact]
    public async Task Recovered_target_completes_without_restart_dispatch()
    {
        var activation = Activation();
        var store = new InMemoryStore(activation);
        var dispatcher = new FakeDispatcher(
            new ConnectAutoRestartDispatchResult(
                ConnectAutoRestartDispatchOutcome.Accepted,
                "accepted"));
        var controller = Controller(
            store,
            new FakeRevalidator(
                ConnectAutoRestartRevalidationOutcome.Recovered,
                "auto_restart_target_recovered"),
            dispatcher);

        var result = await controller.RunOnceAsync(
            activation.ActivationId,
            "worker-a");

        Assert.Equal(
            ConnectAutoRestartRunOutcome.Recovered,
            result.Outcome);
        Assert.Equal(0, dispatcher.Calls);
        Assert.Equal(
            ConnectAutoRestartCircuitState.Recovered,
            store.Snapshot.CircuitState);
    }

    [Fact]
    public async Task Accepted_restart_is_reserved_before_dispatch_and_waits_for_reobservation()
    {
        var activation = Activation();
        var store = new InMemoryStore(activation);
        var dispatcher = new FakeDispatcher(
            new ConnectAutoRestartDispatchResult(
                ConnectAutoRestartDispatchOutcome.Accepted,
                "connect_restart_accepted"),
            beforeReturn: () =>
            {
                Assert.Equal(
                    ConnectAutoRestartCircuitState.Dispatching,
                    store.Snapshot.CircuitState);
                Assert.True(
                    store.Snapshot.HasUnresolvedDispatch);
                Assert.Equal(
                    1,
                    store.Snapshot.AttemptsUsed);
            });

        var controller = Controller(
            store,
            Allowed(),
            dispatcher);

        var result = await controller.RunOnceAsync(
            activation.ActivationId,
            "worker-a");

        Assert.Equal(
            ConnectAutoRestartRunOutcome.DispatchAccepted,
            result.Outcome);
        Assert.Equal(1, dispatcher.Calls);
        Assert.Equal(
            ConnectAutoRestartCircuitState.Waiting,
            store.Snapshot.CircuitState);
        Assert.False(
            store.Snapshot.HasUnresolvedDispatch);
        Assert.Equal(1, store.Snapshot.AttemptsUsed);
        Assert.Equal(
            "connect_restart_accepted",
            store.Snapshot.TerminalReason);
    }

    [Fact]
    public async Task Ambiguous_restart_opens_circuit_and_keeps_unresolved_dispatch()
    {
        var activation = Activation();
        var store = new InMemoryStore(activation);
        var controller = Controller(
            store,
            Allowed(),
            new FakeDispatcher(
                new ConnectAutoRestartDispatchResult(
                    ConnectAutoRestartDispatchOutcome.Ambiguous,
                    "connect_restart_outcome_unknown")));

        var result = await controller.RunOnceAsync(
            activation.ActivationId,
            "worker-a");

        Assert.Equal(
            ConnectAutoRestartRunOutcome.DispatchAmbiguous,
            result.Outcome);
        Assert.Equal(
            ConnectAutoRestartCircuitState.Ambiguous,
            store.Snapshot.CircuitState);
        Assert.True(
            store.Snapshot.HasUnresolvedDispatch);
        Assert.Equal(1, store.Snapshot.AttemptsUsed);

        var second = await controller.RunOnceAsync(
            activation.ActivationId,
            "worker-b");

        Assert.Equal(
            ConnectAutoRestartRunOutcome.NotEligible,
            second.Outcome);
    }

    [Fact]
    public async Task Post_dispatch_persistence_failure_leaves_durable_unresolved_marker()
    {
        var activation = Activation();
        var store = new InMemoryStore(activation)
        {
            FailUpdateWhenExpectedVersion = 2,
        };
        var controller = Controller(
            store,
            Allowed(),
            new FakeDispatcher(
                new ConnectAutoRestartDispatchResult(
                    ConnectAutoRestartDispatchOutcome.Accepted,
                    "connect_restart_accepted")));

        var result = await controller.RunOnceAsync(
            activation.ActivationId,
            "worker-a");

        Assert.Equal(
            ConnectAutoRestartRunOutcome
                .StatePersistenceFailedAfterDispatch,
            result.Outcome);

        Assert.Equal(
            ConnectAutoRestartCircuitState.Dispatching,
            store.Snapshot.CircuitState);
        Assert.True(
            store.Snapshot.HasUnresolvedDispatch);
        Assert.Equal(1, store.Snapshot.AttemptsUsed);
    }

    [Fact]
    public async Task Lifetime_expiry_is_persisted_terminal_without_dispatch()
    {
        var activation = Activation(
            new ConnectAutoRestartPolicy(
                enabled: true,
                activationLifetime:
                    TimeSpan.FromSeconds(30)));
        var time = new FixedTimeProvider(
            Now.AddSeconds(31));
        var store = new InMemoryStore(activation);
        var dispatcher = new FakeDispatcher(
            new ConnectAutoRestartDispatchResult(
                ConnectAutoRestartDispatchOutcome.Accepted,
                "accepted"));

        var controller = new ConnectAutoRestartController(
            store,
            Allowed(),
            dispatcher,
            timeProvider: time);

        var result = await controller.RunOnceAsync(
            activation.ActivationId,
            "worker-a");

        Assert.Equal(
            ConnectAutoRestartRunOutcome.NotEligible,
            result.Outcome);
        Assert.Equal(
            ConnectAutoRestartCircuitState.Exhausted,
            store.Snapshot.CircuitState);
        Assert.Equal(0, dispatcher.Calls);
    }

    private static ConnectAutoRestartController Controller(
        InMemoryStore store,
        IConnectAutoRestartAttemptRevalidator revalidator,
        IConnectAutoRestartDispatchPort dispatcher) =>
        new(
            store,
            revalidator,
            dispatcher,
            timeProvider: new FixedTimeProvider(Now));

    private static IConnectAutoRestartAttemptRevalidator Allowed() =>
        new FakeRevalidator(
            ConnectAutoRestartRevalidationOutcome.Allowed,
            "auto_restart_revalidation_allowed");

    private static ConnectAutoRestartActivation Activation(
        ConnectAutoRestartPolicy? policy = null) =>
        ConnectAutoRestartActivation.Create(
            new ConnectAutoRestartTarget(
                "prod",
                "default",
                "sink-a",
                null),
            policy ?? new ConnectAutoRestartPolicy(
                enabled: true,
                jitterBasisPoints: 0),
            Now,
            Fingerprint("provider"),
            Fingerprint("config"),
            Fingerprint("policy"),
            "oidc:https://idp.example|kafdeck-auto-restart",
            Guid.Parse(
                "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"));

    private static string Fingerprint(string value) =>
        Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();

    private sealed class FakeRevalidator :
        IConnectAutoRestartAttemptRevalidator
    {
        private readonly ConnectAutoRestartRevalidationResult _result;

        public FakeRevalidator(
            ConnectAutoRestartRevalidationOutcome outcome,
            string code)
        {
            _result = new(
                outcome,
                code);
        }

        public Task<ConnectAutoRestartRevalidationResult> RevalidateAsync(
            ConnectAutoRestartActivation activation,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(_result);
        }
    }

    private sealed class FakeDispatcher :
        IConnectAutoRestartDispatchPort
    {
        private readonly ConnectAutoRestartDispatchResult _result;
        private readonly Action? _beforeReturn;

        public FakeDispatcher(
            ConnectAutoRestartDispatchResult result,
            Action? beforeReturn = null)
        {
            _result = result;
            _beforeReturn = beforeReturn;
        }

        public int Calls { get; private set; }

        public Task<ConnectAutoRestartDispatchResult> RestartAsync(
            ConnectAutoRestartDispatchRequest request,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Calls++;
            _beforeReturn?.Invoke();
            return Task.FromResult(_result);
        }
    }

    private sealed class InMemoryStore :
        IConnectAutoRestartStateStore
    {
        private ConnectAutoRestartLease? _lease;
        private long _leaseGeneration;

        public InMemoryStore(
            ConnectAutoRestartActivation snapshot)
        {
            Snapshot = snapshot;
        }

        public ConnectAutoRestartActivation Snapshot { get; private set; }

        public long? FailUpdateWhenExpectedVersion { get; init; }

        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<ConnectAutoRestartActivation?> GetAsync(
            Guid activationId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<ConnectAutoRestartActivation?>(
                activationId == Snapshot.ActivationId
                    ? Snapshot
                    : null);

        public Task<ConnectAutoRestartActivation?> GetActiveByTargetAsync(
            ConnectAutoRestartTarget target,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<ConnectAutoRestartActivation?>(
                Snapshot.ReleasesActiveClaim
                    ? null
                    : Snapshot.Target == target
                        ? Snapshot
                        : null);

        public Task<bool> TryCreateAsync(
            ConnectAutoRestartActivation activation,
            int aggregateProfileLimit,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(false);

        public Task<bool> TryUpdateAsync(
            ConnectAutoRestartActivation activation,
            long expectedVersion,
            ConnectAutoRestartLease lease,
            DateTimeOffset now,
            CancellationToken cancellationToken = default)
        {
            if (FailUpdateWhenExpectedVersion ==
                    expectedVersion ||
                Snapshot.Version != expectedVersion ||
                _lease is null ||
                _lease.Generation != lease.Generation ||
                _lease.OwnerId != lease.OwnerId ||
                _lease.ExpiresAtUtc != lease.ExpiresAtUtc ||
                now >= lease.ExpiresAtUtc)
            {
                return Task.FromResult(false);
            }

            Snapshot = activation;
            return Task.FromResult(true);
        }

        public Task<ConnectAutoRestartLease?> TryAcquireLeaseAsync(
            Guid activationId,
            string ownerId,
            DateTimeOffset now,
            TimeSpan leaseTtl,
            CancellationToken cancellationToken = default)
        {
            if (activationId != Snapshot.ActivationId ||
                (_lease is not null &&
                 _lease.ExpiresAtUtc > now &&
                 !string.Equals(
                     _lease.OwnerId,
                     ownerId,
                     StringComparison.Ordinal)))
            {
                return Task.FromResult<ConnectAutoRestartLease?>(null);
            }

            _lease = new ConnectAutoRestartLease(
                activationId,
                ownerId,
                ++_leaseGeneration,
                now.Add(leaseTtl));

            return Task.FromResult<ConnectAutoRestartLease?>(_lease);
        }

        public Task<bool> ReleaseLeaseAsync(
            ConnectAutoRestartLease lease,
            CancellationToken cancellationToken = default)
        {
            if (_lease is null ||
                _lease.Generation != lease.Generation ||
                _lease.OwnerId != lease.OwnerId)
            {
                return Task.FromResult(false);
            }

            _lease = null;
            return Task.FromResult(true);
        }

        public Task<IReadOnlyList<ConnectAutoRestartActivation>>
            ListActiveAsync(
                string clusterId,
                string connectProfileId,
                int limit,
                CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<ConnectAutoRestartActivation>>(
                new[] { Snapshot });
    }

    private sealed class FixedTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _now;

        public FixedTimeProvider(DateTimeOffset now)
        {
            _now = now;
        }

        public override DateTimeOffset GetUtcNow() => _now;
    }
}
