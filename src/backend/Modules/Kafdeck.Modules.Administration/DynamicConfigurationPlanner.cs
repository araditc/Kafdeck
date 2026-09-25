using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

public enum DynamicConfigurationPlanningFailureCode
{
    InvalidInput = 1,
    KeyNotAllowlisted = 2,
    ValueRejected = 3,
    SensitiveValue = 4,
    ReadOnlyValue = 5,
    ResetSourceUnavailable = 6,
    ProviderUnauthorized = 7,
    ProviderUnsupported = 8,
    ProviderUnavailable = 9,
    ObservationFailed = 10,
}

public sealed record DynamicConfigurationPlanningFailure(
    DynamicConfigurationPlanningFailureCode Code,
    string SafeMessage);

public sealed record DynamicConfigurationPlanningResult(
    DynamicConfigurationPlan? Plan,
    MutationIntentDescriptor? Intent,
    MutationRiskDecision? Risk,
    DynamicConfigurationPlanningFailure? Failure)
{
    public bool IsSuccess =>
        Plan is not null &&
        Intent is not null &&
        Risk is not null &&
        Failure is null;

    public static DynamicConfigurationPlanningResult Success(
        DynamicConfigurationPlan plan,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk) =>
        new(plan, intent, risk, null);

    public static DynamicConfigurationPlanningResult Failed(
        DynamicConfigurationPlanningFailure failure) =>
        new(null, null, null, failure);
}

public sealed record DynamicConfigurationPlannerPolicy(
    TimeSpan ObservationTimeout)
{
    public static DynamicConfigurationPlannerPolicy Default { get; } =
        new(TimeSpan.FromSeconds(10));
}

public sealed class DynamicConfigurationPlanner
{
    private readonly IDynamicConfigurationPort _provider;
    private readonly DynamicConfigurationRegistry _registry;
    private readonly DynamicConfigurationPlannerPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public DynamicConfigurationPlanner(
        IDynamicConfigurationPort provider,
        DynamicConfigurationRegistry registry,
        DynamicConfigurationPlannerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _policy = policy ?? DynamicConfigurationPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;

        if (_policy.ObservationTimeout < TimeSpan.FromSeconds(1) ||
            _policy.ObservationTimeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(nameof(policy));
        }
    }

    public Task<DynamicConfigurationPlanningResult> PlanSetAsync(
        KafkaCompatibilityLine compatibility,
        DynamicConfigurationTarget target,
        string value,
        CancellationToken cancellationToken = default) =>
        PlanAsync(
            compatibility,
            target,
            value,
            reset: false,
            cancellationToken);

    public Task<DynamicConfigurationPlanningResult> PlanResetAsync(
        KafkaCompatibilityLine compatibility,
        DynamicConfigurationTarget target,
        CancellationToken cancellationToken = default) =>
        PlanAsync(
            compatibility,
            target,
            value: null,
            reset: true,
            cancellationToken);

    private async Task<DynamicConfigurationPlanningResult> PlanAsync(
        KafkaCompatibilityLine compatibility,
        DynamicConfigurationTarget target,
        string? value,
        bool reset,
        CancellationToken cancellationToken)
    {
        DynamicConfigurationTarget normalizedTarget;
        DynamicConfigurationKeyPolicy keyPolicy;
        string? normalizedValue = null;

        try
        {
            normalizedTarget =
                DynamicConfigurationPolicy.NormalizeTarget(target);
            keyPolicy = _registry.Require(
                compatibility,
                normalizedTarget.Key);
            if (!reset)
            {
                normalizedValue = keyPolicy.NormalizeValue(value!);
            }
        }
        catch (DynamicConfigurationPolicyException exception)
        {
            return DynamicConfigurationPlanningResult.Failed(
                new DynamicConfigurationPlanningFailure(
                    exception.Message.Contains("allowlist", StringComparison.OrdinalIgnoreCase)
                        ? DynamicConfigurationPlanningFailureCode.KeyNotAllowlisted
                        : DynamicConfigurationPlanningFailureCode.InvalidInput,
                    "Dynamic configuration request is outside the server-owned W44 policy."));
        }
        catch (ArgumentException)
        {
            return DynamicConfigurationPlanningResult.Failed(
                new DynamicConfigurationPlanningFailure(
                    DynamicConfigurationPlanningFailureCode.InvalidInput,
                    "Dynamic configuration request is invalid."));
        }

        var observed = await _provider.DescribeAsync(
                normalizedTarget,
                new KafkaOperationContext(
                    _timeProvider.GetUtcNow().Add(
                        _policy.ObservationTimeout)),
                cancellationToken)
            .ConfigureAwait(false);

        if (!observed.IsSuccess || observed.Value is null)
        {
            return Failed(observed.Failure);
        }

        DynamicConfigurationObservation safeObservation;
        try
        {
            safeObservation =
                DynamicConfigurationPolicy.NormalizeObservation(
                    observed.Value);
        }
        catch (DynamicConfigurationPolicyException)
        {
            return DynamicConfigurationPlanningResult.Failed(
                new DynamicConfigurationPlanningFailure(
                    DynamicConfigurationPlanningFailureCode.ObservationFailed,
                    "Kafka dynamic configuration evidence is invalid."));
        }

        if (safeObservation.IsSensitive)
        {
            return DynamicConfigurationPlanningResult.Failed(
                new DynamicConfigurationPlanningFailure(
                    DynamicConfigurationPlanningFailureCode.SensitiveValue,
                    "Sensitive Kafka configuration is unavailable through W44."));
        }

        if (safeObservation.IsReadOnly)
        {
            return DynamicConfigurationPlanningResult.Failed(
                new DynamicConfigurationPlanningFailure(
                    DynamicConfigurationPlanningFailureCode.ReadOnlyValue,
                    "Kafka reports the requested configuration as read-only."));
        }

        string? inheritedValue = null;
        string? inheritedSource = null;
        if (reset)
        {
            try
            {
                var inherited =
                    DynamicConfigurationPolicy.RequireInheritedValue(
                        safeObservation);
                inheritedValue = inherited.Value;
                inheritedSource = inherited.Source;
            }
            catch (DynamicConfigurationPolicyException)
            {
                return DynamicConfigurationPlanningResult.Failed(
                    new DynamicConfigurationPlanningFailure(
                        DynamicConfigurationPlanningFailureCode.ResetSourceUnavailable,
                        "Reset is unavailable because an exact dynamic override and inherited value/source were not safely observed."));
            }
        }

        var plan = new DynamicConfigurationPlan(
            compatibility,
            new DynamicConfigurationMutation(
                normalizedTarget,
                normalizedValue),
            DynamicConfigurationPolicy.Fingerprint(
                safeObservation),
            inheritedValue,
            inheritedSource);

        try
        {
            var intent = DynamicConfigurationPolicy.BuildIntent(
                plan,
                keyPolicy);
            return DynamicConfigurationPlanningResult.Success(
                plan,
                intent,
                DynamicConfigurationPolicy.ClassifyRisk(
                    keyPolicy));
        }
        catch (DynamicConfigurationPolicyException)
        {
            return DynamicConfigurationPlanningResult.Failed(
                new DynamicConfigurationPlanningFailure(
                    DynamicConfigurationPlanningFailureCode.InvalidInput,
                    "Dynamic configuration request could not be bound to a safe immutable preview."));
        }
    }

    private static DynamicConfigurationPlanningResult Failed(
        KafkaFailure? failure) =>
        DynamicConfigurationPlanningResult.Failed(
            failure?.Category switch
            {
                KafkaFailureCategory.Unauthorized =>
                    new(
                        DynamicConfigurationPlanningFailureCode.ProviderUnauthorized,
                        "Kafka denied dynamic configuration observation."),
                KafkaFailureCategory.NotSupported =>
                    new(
                        DynamicConfigurationPlanningFailureCode.ProviderUnsupported,
                        "Kafka or the pinned client does not support the required dynamic configuration observation."),
                KafkaFailureCategory.Unavailable or
                KafkaFailureCategory.Timeout or
                KafkaFailureCategory.AuthenticationFailed or
                KafkaFailureCategory.TlsFailure =>
                    new(
                        DynamicConfigurationPlanningFailureCode.ProviderUnavailable,
                        "Kafka dynamic configuration is not currently observable for safe planning."),
                _ =>
                    new(
                        DynamicConfigurationPlanningFailureCode.ObservationFailed,
                        "Kafka dynamic configuration could not be safely observed."),
            });
}
