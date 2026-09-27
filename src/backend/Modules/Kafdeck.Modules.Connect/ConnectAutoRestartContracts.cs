using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;

namespace Kafdeck.Modules.Connect;

public enum ConnectAutoRestartTargetKind
{
    Connector = 1,
    Task = 2,
}

public enum ConnectAutoRestartCircuitState
{
    Disabled = 1,
    Armed = 2,
    Waiting = 3,
    Dispatching = 4,
    Recovered = 5,
    Exhausted = 6,
    Blocked = 7,
    Ambiguous = 8,
}

public enum ConnectAutoRestartEligibility
{
    Eligible = 1,
    Disabled = 2,
    Waiting = 3,
    LifetimeExpired = 4,
    AttemptsExhausted = 5,
    CircuitOpen = 6,
    DispatchUnresolved = 7,
    Terminal = 8,
}

public sealed record ConnectAutoRestartTarget(
    string ClusterId,
    string ConnectProfileId,
    string ConnectorName,
    int? TaskId)
{
    public ConnectAutoRestartTargetKind Kind =>
        TaskId.HasValue
            ? ConnectAutoRestartTargetKind.Task
            : ConnectAutoRestartTargetKind.Connector;

    public string CanonicalKey =>
        TaskId.HasValue
            ? $"cluster/{ClusterId}/connect-profile/{ConnectProfileId}/connector/{ConnectorName}/task/{TaskId.Value}"
            : $"cluster/{ClusterId}/connect-profile/{ConnectProfileId}/connector/{ConnectorName}";

    public string AuthorizationResource =>
        string.Equals(
            ConnectProfileId,
            "default",
            StringComparison.Ordinal)
            ? $"connector/{ConnectorName}"
            : $"connect-profile/{ConnectProfileId}/connector/{ConnectorName}";
}

public sealed record ConnectAutoRestartPolicy
{
    public const int DefaultMaxAttempts = 3;
    public const int HardMaxAttempts = 10;

    public static readonly TimeSpan DefaultInitialBackoff =
        TimeSpan.FromSeconds(10);
    public static readonly TimeSpan HardMinInitialBackoff =
        TimeSpan.FromSeconds(5);

    public static readonly TimeSpan DefaultMaxBackoff =
        TimeSpan.FromMinutes(5);
    public static readonly TimeSpan HardMaxBackoff =
        TimeSpan.FromMinutes(30);

    public static readonly TimeSpan DefaultActivationLifetime =
        TimeSpan.FromMinutes(30);
    public static readonly TimeSpan HardMaxActivationLifetime =
        TimeSpan.FromHours(24);

    public const int DefaultMaxActivePoliciesPerProfile = 10;
    public const int HardMaxActivePoliciesPerProfile = 100;

    public const int DefaultJitterBasisPoints = 2_000;
    public const int HardMaxJitterBasisPoints = 5_000;

    public static ConnectAutoRestartPolicy Disabled { get; } =
        new(enabled: false);

    [JsonConstructor]
    public ConnectAutoRestartPolicy(
        bool enabled,
        int maxAttempts,
        TimeSpan initialBackoff,
        TimeSpan maxBackoff,
        TimeSpan activationLifetime,
        int maxActivePoliciesPerProfile,
        int jitterBasisPoints)
        : this(
            enabled,
            maxAttempts,
            (TimeSpan?)initialBackoff,
            (TimeSpan?)maxBackoff,
            (TimeSpan?)activationLifetime,
            maxActivePoliciesPerProfile,
            jitterBasisPoints)
    {
    }

    public ConnectAutoRestartPolicy(
        bool enabled,
        int maxAttempts = DefaultMaxAttempts,
        TimeSpan? initialBackoff = null,
        TimeSpan? maxBackoff = null,
        TimeSpan? activationLifetime = null,
        int maxActivePoliciesPerProfile =
            DefaultMaxActivePoliciesPerProfile,
        int jitterBasisPoints = DefaultJitterBasisPoints)
    {
        var initial = initialBackoff ?? DefaultInitialBackoff;
        var maximum = maxBackoff ?? DefaultMaxBackoff;
        var lifetime = activationLifetime ?? DefaultActivationLifetime;

        if (maxAttempts is < 1 or > HardMaxAttempts)
        {
            throw new ArgumentOutOfRangeException(nameof(maxAttempts));
        }

        if (initial < HardMinInitialBackoff ||
            initial > HardMaxBackoff)
        {
            throw new ArgumentOutOfRangeException(nameof(initialBackoff));
        }

        if (maximum < initial ||
            maximum > HardMaxBackoff)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBackoff));
        }

        if (lifetime <= TimeSpan.Zero ||
            lifetime > HardMaxActivationLifetime)
        {
            throw new ArgumentOutOfRangeException(nameof(activationLifetime));
        }

        if (maxActivePoliciesPerProfile is < 1 or
            > HardMaxActivePoliciesPerProfile)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxActivePoliciesPerProfile));
        }

        if (jitterBasisPoints is < 0 or
            > HardMaxJitterBasisPoints)
        {
            throw new ArgumentOutOfRangeException(
                nameof(jitterBasisPoints));
        }

        Enabled = enabled;
        MaxAttempts = maxAttempts;
        InitialBackoff = initial;
        MaxBackoff = maximum;
        ActivationLifetime = lifetime;
        MaxActivePoliciesPerProfile =
            maxActivePoliciesPerProfile;
        JitterBasisPoints = jitterBasisPoints;
    }

    public bool Enabled { get; }
    public int MaxAttempts { get; }
    public TimeSpan InitialBackoff { get; }
    public TimeSpan MaxBackoff { get; }
    public TimeSpan ActivationLifetime { get; }
    public int MaxActivePoliciesPerProfile { get; }
    public int JitterBasisPoints { get; }
}

public sealed record ConnectAutoRestartActivation(
    Guid ActivationId,
    ConnectAutoRestartTarget Target,
    ConnectAutoRestartPolicy Policy,
    DateTimeOffset ActivatedAtUtc,
    DateTimeOffset DeadlineUtc,
    int AttemptsUsed,
    int ConsecutiveFailures,
    DateTimeOffset NextAttemptUtc,
    ConnectAutoRestartCircuitState CircuitState,
    bool HasUnresolvedDispatch,
    Guid? UnresolvedDispatchId,
    string? TerminalReason,
    string ProviderIdentityFingerprint,
    string ConnectorConfigurationFingerprint,
    string PolicyFingerprint,
    string AutomationPrincipalId,
    long Version)
{
    public bool IsTerminal =>
        CircuitState is
            ConnectAutoRestartCircuitState.Disabled or
            ConnectAutoRestartCircuitState.Recovered or
            ConnectAutoRestartCircuitState.Exhausted or
            ConnectAutoRestartCircuitState.Blocked or
            ConnectAutoRestartCircuitState.Ambiguous;

    public bool ReleasesActiveClaim =>
        CircuitState is
            ConnectAutoRestartCircuitState.Disabled or
            ConnectAutoRestartCircuitState.Recovered or
            ConnectAutoRestartCircuitState.Exhausted or
            ConnectAutoRestartCircuitState.Blocked;

    public static ConnectAutoRestartActivation Create(
        ConnectAutoRestartTarget target,
        ConnectAutoRestartPolicy policy,
        DateTimeOffset now,
        string providerIdentityFingerprint,
        string connectorConfigurationFingerprint,
        string policyFingerprint,
        string automationPrincipalId,
        Guid? activationId = null)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(policy);

        ValidateTarget(target);
        ValidateFingerprint(
            providerIdentityFingerprint,
            nameof(providerIdentityFingerprint));
        ValidateFingerprint(
            connectorConfigurationFingerprint,
            nameof(connectorConfigurationFingerprint));
        ValidateFingerprint(
            policyFingerprint,
            nameof(policyFingerprint));
        var normalizedAutomationPrincipal =
            RequireIdentifier(
                automationPrincipalId,
                nameof(automationPrincipalId),
                1024);

        var state = policy.Enabled
            ? ConnectAutoRestartCircuitState.Armed
            : ConnectAutoRestartCircuitState.Disabled;

        return new ConnectAutoRestartActivation(
            activationId ?? Guid.NewGuid(),
            target,
            policy,
            now,
            now.Add(policy.ActivationLifetime),
            AttemptsUsed: 0,
            ConsecutiveFailures: 0,
            NextAttemptUtc: now,
            state,
            HasUnresolvedDispatch: false,
            UnresolvedDispatchId: null,
            TerminalReason: policy.Enabled
                ? null
                : "auto_restart_disabled",
            providerIdentityFingerprint,
            connectorConfigurationFingerprint,
            policyFingerprint,
            normalizedAutomationPrincipal,
            Version: 1);
    }

    public ConnectAutoRestartEligibility Evaluate(
        DateTimeOffset now)
    {
        if (!Policy.Enabled ||
            CircuitState ==
            ConnectAutoRestartCircuitState.Disabled)
        {
            return ConnectAutoRestartEligibility.Disabled;
        }

        if (HasUnresolvedDispatch)
        {
            return ConnectAutoRestartEligibility.DispatchUnresolved;
        }

        if (CircuitState ==
            ConnectAutoRestartCircuitState.Ambiguous)
        {
            return ConnectAutoRestartEligibility.CircuitOpen;
        }

        if (IsTerminal)
        {
            return ConnectAutoRestartEligibility.Terminal;
        }

        if (now >= DeadlineUtc)
        {
            return ConnectAutoRestartEligibility.LifetimeExpired;
        }

        if (AttemptsUsed >= Policy.MaxAttempts)
        {
            return ConnectAutoRestartEligibility.AttemptsExhausted;
        }

        return now < NextAttemptUtc
            ? ConnectAutoRestartEligibility.Waiting
            : ConnectAutoRestartEligibility.Eligible;
    }

    public ConnectAutoRestartActivation ReserveAttempt(
        DateTimeOffset now,
        Guid dispatchId)
    {
        if (dispatchId == Guid.Empty)
        {
            throw new ArgumentOutOfRangeException(nameof(dispatchId));
        }

        var eligibility = Evaluate(now);
        if (eligibility != ConnectAutoRestartEligibility.Eligible)
        {
            throw new InvalidOperationException(
                $"Auto-restart activation is not eligible: {eligibility}.");
        }

        var attemptNumber = checked(AttemptsUsed + 1);
        return this with
        {
            AttemptsUsed = attemptNumber,
            NextAttemptUtc = now.Add(
                ConnectAutoRestartBackoff.ComputeDelay(
                    Policy,
                    ActivationId,
                    attemptNumber)),
            CircuitState =
                ConnectAutoRestartCircuitState.Dispatching,
            HasUnresolvedDispatch = true,
            UnresolvedDispatchId = dispatchId,
            Version = checked(Version + 1),
        };
    }

    public ConnectAutoRestartActivation RecordAccepted(
        Guid dispatchId,
        string resultCode)
    {
        RequireMatchingDispatch(dispatchId);

        return this with
        {
            ConsecutiveFailures = 0,
            CircuitState =
                ConnectAutoRestartCircuitState.Waiting,
            HasUnresolvedDispatch = false,
            UnresolvedDispatchId = null,
            TerminalReason = RequireReason(resultCode),
            Version = checked(Version + 1),
        };
    }

    public ConnectAutoRestartActivation RecordDefinitiveFailure(
        DateTimeOffset now,
        Guid dispatchId,
        string reason)
    {
        RequireMatchingDispatch(dispatchId);
        var normalizedReason = RequireReason(reason);

        if (AttemptsUsed >= Policy.MaxAttempts)
        {
            return this with
            {
                ConsecutiveFailures =
                    checked(ConsecutiveFailures + 1),
                CircuitState =
                    ConnectAutoRestartCircuitState.Exhausted,
                HasUnresolvedDispatch = false,
                UnresolvedDispatchId = null,
                TerminalReason = "auto_restart_attempts_exhausted",
                Version = checked(Version + 1),
            };
        }

        if (now >= DeadlineUtc)
        {
            return this with
            {
                ConsecutiveFailures =
                    checked(ConsecutiveFailures + 1),
                CircuitState =
                    ConnectAutoRestartCircuitState.Exhausted,
                HasUnresolvedDispatch = false,
                UnresolvedDispatchId = null,
                TerminalReason = "auto_restart_lifetime_exhausted",
                Version = checked(Version + 1),
            };
        }

        return this with
        {
            ConsecutiveFailures =
                checked(ConsecutiveFailures + 1),
            CircuitState =
                ConnectAutoRestartCircuitState.Waiting,
            HasUnresolvedDispatch = false,
            UnresolvedDispatchId = null,
            TerminalReason = normalizedReason,
            Version = checked(Version + 1),
        };
    }

    public ConnectAutoRestartActivation RecordAmbiguous(
        Guid dispatchId,
        string reason)
    {
        RequireMatchingDispatch(dispatchId);

        return this with
        {
            ConsecutiveFailures =
                checked(ConsecutiveFailures + 1),
            CircuitState =
                ConnectAutoRestartCircuitState.Ambiguous,
            HasUnresolvedDispatch = true,
            UnresolvedDispatchId = dispatchId,
            TerminalReason = RequireReason(reason),
            Version = checked(Version + 1),
        };
    }

    public ConnectAutoRestartActivation RecordRecovered(
        Guid dispatchId)
    {
        RequireMatchingDispatch(dispatchId);

        return this with
        {
            ConsecutiveFailures = 0,
            CircuitState =
                ConnectAutoRestartCircuitState.Recovered,
            HasUnresolvedDispatch = false,
            UnresolvedDispatchId = null,
            TerminalReason = "auto_restart_recovered",
            Version = checked(Version + 1),
        };
    }

    public ConnectAutoRestartActivation Disable(
        string reason = "auto_restart_policy_disabled") =>
        this with
        {
            CircuitState =
                ConnectAutoRestartCircuitState.Disabled,
            HasUnresolvedDispatch = false,
            UnresolvedDispatchId = null,
            TerminalReason = RequireReason(reason),
            Version = checked(Version + 1),
        };

    public ConnectAutoRestartActivation MarkRecovered(
        string reason = "auto_restart_target_recovered") =>
        this with
        {
            ConsecutiveFailures = 0,
            CircuitState =
                ConnectAutoRestartCircuitState.Recovered,
            HasUnresolvedDispatch = false,
            UnresolvedDispatchId = null,
            TerminalReason = RequireReason(reason),
            Version = checked(Version + 1),
        };

    public ConnectAutoRestartActivation Exhaust(
        string reason) =>
        this with
        {
            CircuitState =
                ConnectAutoRestartCircuitState.Exhausted,
            HasUnresolvedDispatch = false,
            UnresolvedDispatchId = null,
            TerminalReason = RequireReason(reason),
            Version = checked(Version + 1),
        };

    public ConnectAutoRestartActivation Block(
        string reason) =>
        this with
        {
            CircuitState =
                ConnectAutoRestartCircuitState.Blocked,
            HasUnresolvedDispatch = false,
            UnresolvedDispatchId = null,
            TerminalReason = RequireReason(reason),
            Version = checked(Version + 1),
        };

    private void RequireMatchingDispatch(Guid dispatchId)
    {
        if (!HasUnresolvedDispatch ||
            !UnresolvedDispatchId.HasValue ||
            UnresolvedDispatchId.Value != dispatchId)
        {
            throw new InvalidOperationException(
                "Auto-restart dispatch identity does not match the durable unresolved dispatch.");
        }
    }

    private static void ValidateTarget(
        ConnectAutoRestartTarget target)
    {
        RequireIdentifier(target.ClusterId, nameof(target.ClusterId), 256);
        RequireIdentifier(
            target.ConnectProfileId,
            nameof(target.ConnectProfileId),
            128);
        RequireIdentifier(
            target.ConnectorName,
            nameof(target.ConnectorName),
            512);

        if (target.TaskId is < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(target.TaskId));
        }
    }

    private static string RequireIdentifier(
        string value,
        string parameterName,
        int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, parameterName);
        var normalized = value.Trim();
        if (!string.Equals(value, normalized, StringComparison.Ordinal) ||
            normalized.Length > maxLength ||
            normalized.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Auto-restart target identifier is invalid.",
                parameterName);
        }

        return normalized;
    }

    private static string RequireReason(string reason)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        var normalized = reason.Trim();
        if (normalized.Length > 256 ||
            normalized.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Auto-restart reason is invalid.",
                nameof(reason));
        }

        return normalized;
    }

    private static void ValidateFingerprint(
        string value,
        string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, parameterName);
        if (value.Length != 64 ||
            !value.All(char.IsAsciiHexDigit))
        {
            throw new ArgumentException(
                "Auto-restart fingerprint is invalid.",
                parameterName);
        }
    }
}

public static class ConnectAutoRestartBackoff
{
    public static TimeSpan ComputeDelay(
        ConnectAutoRestartPolicy policy,
        Guid activationId,
        int attemptNumber)
    {
        ArgumentNullException.ThrowIfNull(policy);
        if (attemptNumber < 1 ||
            attemptNumber > ConnectAutoRestartPolicy.HardMaxAttempts)
        {
            throw new ArgumentOutOfRangeException(nameof(attemptNumber));
        }

        var exponent = Math.Min(attemptNumber - 1, 30);
        var factor = 1L << exponent;
        long baseTicks;
        try
        {
            baseTicks = checked(policy.InitialBackoff.Ticks * factor);
        }
        catch (OverflowException)
        {
            baseTicks = long.MaxValue;
        }

        baseTicks = Math.Min(
            baseTicks,
            policy.MaxBackoff.Ticks);

        if (policy.JitterBasisPoints == 0)
        {
            return TimeSpan.FromTicks(baseTicks);
        }

        Span<byte> input = stackalloc byte[20];
        activationId.TryWriteBytes(input[..16]);
        BitConverter.TryWriteBytes(
            input[16..],
            attemptNumber);

        var hash = SHA256.HashData(input);
        var sample = BitConverter.ToUInt32(hash, 0);
        var unit = sample / (double)uint.MaxValue;
        var signed = (unit * 2d) - 1d;
        var jitterFraction =
            policy.JitterBasisPoints / 10_000d;
        var multiplier = 1d + (signed * jitterFraction);

        var jitteredTicks = Math.Clamp(
            (long)Math.Round(
                baseTicks * multiplier,
                MidpointRounding.AwayFromZero),
            1L,
            policy.MaxBackoff.Ticks);

        return TimeSpan.FromTicks(jitteredTicks);
    }
}

public sealed record ConnectAutoRestartLease(
    Guid ActivationId,
    string OwnerId,
    long Generation,
    DateTimeOffset ExpiresAtUtc);

public enum ConnectAutoRestartRevalidationOutcome
{
    Allowed = 1,
    Recovered = 2,
    AuthorizationDenied = 3,
    ProviderIdentityDrift = 4,
    ConfigurationDrift = 5,
    PolicyDrift = 6,
    CapabilityBlocked = 7,
    ReplicationGuardBlocked = 8,
    TargetUnavailable = 9,
}

public sealed record ConnectAutoRestartRevalidationResult(
    ConnectAutoRestartRevalidationOutcome Outcome,
    string Code)
{
    public bool IsAllowed =>
        Outcome == ConnectAutoRestartRevalidationOutcome.Allowed;

    public bool IsRecovered =>
        Outcome == ConnectAutoRestartRevalidationOutcome.Recovered;
}

public sealed record ConnectAutoRestartRuntimePolicySnapshot(
    bool Enabled,
    string PolicyVersion,
    ConnectAutoRestartPolicy Policy)
{
    public string Fingerprint =>
        ConnectAutoRestartPolicyFingerprint.Compute(
            Enabled,
            PolicyVersion,
            Policy);
}

public interface IConnectAutoRestartRuntimePolicyProvider
{
    Task<ConnectAutoRestartRuntimePolicySnapshot> GetCurrentAsync(
        ConnectAutoRestartTarget target,
        CancellationToken cancellationToken = default);
}

public static class ConnectAutoRestartPolicyFingerprint
{
    public static string ComputeEffective(
        ConnectAutoRestartRuntimePolicySnapshot runtime,
        ConnectAutoRestartPolicy persistedPolicy)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        ArgumentNullException.ThrowIfNull(persistedPolicy);

        var canonical = string.Join(
            "\n",
            runtime.Fingerprint,
            Compute(
                persistedPolicy.Enabled,
                "persisted",
                persistedPolicy));

        return Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(canonical)))
            .ToLowerInvariant();
    }

    public static string Compute(
        bool enabled,
        string policyVersion,
        ConnectAutoRestartPolicy policy)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(policyVersion);
        ArgumentNullException.ThrowIfNull(policy);

        var canonical = string.Join(
            "\n",
            enabled ? "1" : "0",
            policyVersion.Trim(),
            policy.MaxAttempts.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            policy.InitialBackoff.Ticks.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            policy.MaxBackoff.Ticks.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            policy.ActivationLifetime.Ticks.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            policy.MaxActivePoliciesPerProfile.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            policy.JitterBasisPoints.ToString(
                System.Globalization.CultureInfo.InvariantCulture));

        return Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(canonical)))
            .ToLowerInvariant();
    }
}

public sealed record ConnectAutoRestartGovernanceSnapshot(
    bool AuthorizationAllowed,
    string ProviderIdentityFingerprint,
    string PolicyFingerprint,
    string? DenialCode = null);

public interface IConnectAutoRestartGovernancePort
{
    Task<ConnectAutoRestartGovernanceSnapshot> GetCurrentAsync(
        ConnectAutoRestartActivation activation,
        CancellationToken cancellationToken = default);
}

public interface IConnectAutoRestartAttemptRevalidator
{
    Task<ConnectAutoRestartRevalidationResult> RevalidateAsync(
        ConnectAutoRestartActivation activation,
        CancellationToken cancellationToken = default);
}

public enum ConnectAutoRestartAuditEventType
{
    DispatchStarted = 1,
    DispatchOutcome = 2,
}

public sealed record ConnectAutoRestartAuditEvent(
    DateTimeOffset TimestampUtc,
    ConnectAutoRestartAuditEventType EventType,
    Guid ActivationId,
    Guid DispatchId,
    string TargetKey,
    string AutomationPrincipalId,
    int AttemptNumber,
    string Code);

public interface IConnectAutoRestartAuditSink
{
    ValueTask WriteAsync(
        ConnectAutoRestartAuditEvent auditEvent,
        CancellationToken cancellationToken = default);
}

public enum ConnectAutoRestartDispatchOutcome
{
    Accepted = 1,
    FailedDefinitive = 2,
    Ambiguous = 3,
    Blocked = 4,
}

public sealed record ConnectAutoRestartDispatchResult(
    ConnectAutoRestartDispatchOutcome Outcome,
    string Code);

public sealed record ConnectAutoRestartDispatchRequest(
    Guid ActivationId,
    Guid DispatchId,
    ConnectAutoRestartTarget Target,
    string AutomationPrincipalId);

public interface IConnectAutoRestartDispatchPort
{
    Task<ConnectAutoRestartDispatchResult> RestartAsync(
        ConnectAutoRestartDispatchRequest request,
        CancellationToken cancellationToken = default);
}

public interface IConnectAutoRestartStateStore
{
    Task InitializeAsync(
        CancellationToken cancellationToken = default);

    Task<ConnectAutoRestartActivation?> GetAsync(
        Guid activationId,
        CancellationToken cancellationToken = default);

    Task<ConnectAutoRestartActivation?> GetActiveByTargetAsync(
        ConnectAutoRestartTarget target,
        CancellationToken cancellationToken = default);

    Task<bool> TryCreateAsync(
        ConnectAutoRestartActivation activation,
        int aggregateProfileLimit,
        CancellationToken cancellationToken = default);

    Task<bool> TryUpdateAsync(
        ConnectAutoRestartActivation activation,
        long expectedVersion,
        ConnectAutoRestartLease lease,
        DateTimeOffset now,
        CancellationToken cancellationToken = default);

    Task<ConnectAutoRestartLease?> TryAcquireLeaseAsync(
        Guid activationId,
        string ownerId,
        DateTimeOffset now,
        TimeSpan leaseTtl,
        CancellationToken cancellationToken = default);

    Task<bool> ReleaseLeaseAsync(
        ConnectAutoRestartLease lease,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyList<ConnectAutoRestartActivation>> ListActiveAsync(
        string clusterId,
        string connectProfileId,
        int limit,
        CancellationToken cancellationToken = default);
}
