using Kafdeck.Modules.Connect;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W55ConnectAutoRestartContractTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 27, 6, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Default_policy_is_disabled_and_bounded()
    {
        var policy = ConnectAutoRestartPolicy.Disabled;

        Assert.False(policy.Enabled);
        Assert.Equal(
            ConnectAutoRestartPolicy.DefaultMaxAttempts,
            policy.MaxAttempts);
        Assert.Equal(
            ConnectAutoRestartPolicy.DefaultInitialBackoff,
            policy.InitialBackoff);
        Assert.Equal(
            ConnectAutoRestartPolicy.DefaultMaxBackoff,
            policy.MaxBackoff);
        Assert.Equal(
            ConnectAutoRestartPolicy.DefaultActivationLifetime,
            policy.ActivationLifetime);
        Assert.Equal(
            ConnectAutoRestartPolicy.DefaultMaxActivePoliciesPerProfile,
            policy.MaxActivePoliciesPerProfile);
    }

    [Fact]
    public void Policy_accepts_hard_caps_and_rejects_cap_plus_one()
    {
        var policy = new ConnectAutoRestartPolicy(
            enabled: true,
            maxAttempts: ConnectAutoRestartPolicy.HardMaxAttempts,
            initialBackoff:
                ConnectAutoRestartPolicy.HardMinInitialBackoff,
            maxBackoff:
                ConnectAutoRestartPolicy.HardMaxBackoff,
            activationLifetime:
                ConnectAutoRestartPolicy.HardMaxActivationLifetime,
            maxActivePoliciesPerProfile:
                ConnectAutoRestartPolicy.HardMaxActivePoliciesPerProfile,
            jitterBasisPoints:
                ConnectAutoRestartPolicy.HardMaxJitterBasisPoints);

        Assert.True(policy.Enabled);

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new ConnectAutoRestartPolicy(
                enabled: true,
                maxAttempts:
                    ConnectAutoRestartPolicy.HardMaxAttempts + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new ConnectAutoRestartPolicy(
                enabled: true,
                initialBackoff:
                    ConnectAutoRestartPolicy.HardMinInitialBackoff -
                    TimeSpan.FromTicks(1)));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new ConnectAutoRestartPolicy(
                enabled: true,
                maxBackoff:
                    ConnectAutoRestartPolicy.HardMaxBackoff +
                    TimeSpan.FromTicks(1)));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new ConnectAutoRestartPolicy(
                enabled: true,
                activationLifetime:
                    ConnectAutoRestartPolicy.HardMaxActivationLifetime +
                    TimeSpan.FromTicks(1)));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new ConnectAutoRestartPolicy(
                enabled: true,
                maxActivePoliciesPerProfile:
                    ConnectAutoRestartPolicy.HardMaxActivePoliciesPerProfile +
                    1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new ConnectAutoRestartPolicy(
                enabled: true,
                jitterBasisPoints:
                    ConnectAutoRestartPolicy.HardMaxJitterBasisPoints +
                    1));
    }

    [Fact]
    public void Backoff_is_deterministic_bounded_and_exponential()
    {
        var policy = new ConnectAutoRestartPolicy(
            enabled: true,
            jitterBasisPoints: 0);
        var activationId =
            Guid.Parse("01234567-89ab-cdef-0123-456789abcdef");

        Assert.Equal(
            TimeSpan.FromSeconds(10),
            ConnectAutoRestartBackoff.ComputeDelay(
                policy,
                activationId,
                1));
        Assert.Equal(
            TimeSpan.FromSeconds(20),
            ConnectAutoRestartBackoff.ComputeDelay(
                policy,
                activationId,
                2));
        Assert.Equal(
            TimeSpan.FromSeconds(40),
            ConnectAutoRestartBackoff.ComputeDelay(
                policy,
                activationId,
                3));

        var jittered = new ConnectAutoRestartPolicy(
            enabled: true,
            jitterBasisPoints: 2_000);

        var first =
            ConnectAutoRestartBackoff.ComputeDelay(
                jittered,
                activationId,
                1);
        var second =
            ConnectAutoRestartBackoff.ComputeDelay(
                jittered,
                activationId,
                1);

        Assert.Equal(first, second);
        Assert.InRange(
            first,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromSeconds(12));
    }

    [Fact]
    public void Activation_reserves_attempt_before_external_dispatch()
    {
        var activation = Activation(
            new ConnectAutoRestartPolicy(
                enabled: true,
                jitterBasisPoints: 0));

        Assert.Equal(
            ConnectAutoRestartEligibility.Eligible,
            activation.Evaluate(Now));

        var dispatchId = Guid.NewGuid();
        var reserved = activation.ReserveAttempt(
            Now,
            dispatchId);

        Assert.Equal(1, reserved.AttemptsUsed);
        Assert.Equal(
            ConnectAutoRestartCircuitState.Dispatching,
            reserved.CircuitState);
        Assert.True(reserved.HasUnresolvedDispatch);
        Assert.Equal(
            dispatchId,
            reserved.UnresolvedDispatchId);
        Assert.Equal(
            Now.AddSeconds(10),
            reserved.NextAttemptUtc);
        Assert.Equal(
            ConnectAutoRestartEligibility.DispatchUnresolved,
            reserved.Evaluate(Now.AddSeconds(20)));
    }

    [Fact]
    public void Ambiguous_dispatch_consumes_attempt_and_opens_circuit()
    {
        var activation = Activation(
            new ConnectAutoRestartPolicy(
                enabled: true));
        var dispatchId = Guid.NewGuid();

        var reserved = activation.ReserveAttempt(
            Now,
            dispatchId);
        var ambiguous = reserved.RecordAmbiguous(
            dispatchId,
            "provider_outcome_unknown");

        Assert.Equal(1, ambiguous.AttemptsUsed);
        Assert.Equal(
            ConnectAutoRestartCircuitState.Ambiguous,
            ambiguous.CircuitState);
        Assert.True(ambiguous.HasUnresolvedDispatch);
        Assert.Equal(
            ConnectAutoRestartEligibility.DispatchUnresolved,
            ambiguous.Evaluate(Now.AddMinutes(1)));

        Assert.Throws<InvalidOperationException>(
            () => ambiguous.ReserveAttempt(
                Now.AddMinutes(1),
                Guid.NewGuid()));
    }

    [Fact]
    public void Definitive_failures_do_not_reset_attempts_and_exhaust_at_limit()
    {
        var policy = new ConnectAutoRestartPolicy(
            enabled: true,
            maxAttempts: 2,
            jitterBasisPoints: 0);
        var activation = Activation(policy);

        var firstDispatch = Guid.NewGuid();
        var first = activation
            .ReserveAttempt(Now, firstDispatch)
            .RecordDefinitiveFailure(
                Now,
                firstDispatch,
                "restart_rejected");

        Assert.Equal(1, first.AttemptsUsed);
        Assert.Equal(
            ConnectAutoRestartCircuitState.Waiting,
            first.CircuitState);
        Assert.Equal(
            ConnectAutoRestartEligibility.Waiting,
            first.Evaluate(Now));

        var secondDispatch = Guid.NewGuid();
        var second = first
            .ReserveAttempt(
                first.NextAttemptUtc,
                secondDispatch)
            .RecordDefinitiveFailure(
                first.NextAttemptUtc,
                secondDispatch,
                "restart_rejected");

        Assert.Equal(2, second.AttemptsUsed);
        Assert.Equal(
            ConnectAutoRestartCircuitState.Exhausted,
            second.CircuitState);
        Assert.Equal(
            "auto_restart_attempts_exhausted",
            second.TerminalReason);
        Assert.True(second.IsTerminal);
    }

    [Fact]
    public void Activation_lifetime_is_finite_and_does_not_follow_wall_clock_indefinitely()
    {
        var policy = new ConnectAutoRestartPolicy(
            enabled: true,
            activationLifetime: TimeSpan.FromMinutes(1));
        var activation = Activation(policy);

        Assert.Equal(
            Now.AddMinutes(1),
            activation.DeadlineUtc);
        Assert.Equal(
            ConnectAutoRestartEligibility.LifetimeExpired,
            activation.Evaluate(Now.AddMinutes(1)));
    }

    [Fact]
    public void Target_identity_includes_profile_and_optional_task()
    {
        var connector = new ConnectAutoRestartTarget(
            "prod",
            "analytics",
            "sink-a",
            null);
        var task = connector with { TaskId = 3 };

        Assert.Equal(
            "cluster/prod/connect-profile/analytics/connector/sink-a",
            connector.CanonicalKey);
        Assert.Equal(
            "cluster/prod/connect-profile/analytics/connector/sink-a/task/3",
            task.CanonicalKey);
        Assert.Equal(
            ConnectAutoRestartTargetKind.Connector,
            connector.Kind);
        Assert.Equal(
            ConnectAutoRestartTargetKind.Task,
            task.Kind);
    }

    [Fact]
    public void Disabled_activation_never_becomes_eligible()
    {
        var activation = Activation(
            ConnectAutoRestartPolicy.Disabled);

        Assert.Equal(
            ConnectAutoRestartCircuitState.Disabled,
            activation.CircuitState);
        Assert.Equal(
            ConnectAutoRestartEligibility.Disabled,
            activation.Evaluate(Now));
        Assert.True(activation.IsTerminal);
    }

    private static ConnectAutoRestartActivation Activation(
        ConnectAutoRestartPolicy policy) =>
        ConnectAutoRestartActivation.Create(
            new ConnectAutoRestartTarget(
                "prod",
                "default",
                "sink-a",
                null),
            policy,
            Now,
            Fingerprint("provider"),
            Fingerprint("config"),
            Fingerprint("policy"),
            "oidc:https://idp.example|kafdeck-auto-restart",
            Guid.Parse(
                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));

    private static string Fingerprint(string value) =>
        Convert.ToHexString(
                System.Security.Cryptography.SHA256.HashData(
                    System.Text.Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();
}
