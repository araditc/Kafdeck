using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

public enum AclMutationPlanningFailureCode
{
    InvalidInput = 1,
    NoChange = 2,
    NoMatchingBindings = 3,
    TooManyBindings = 4,
    ProtectedPrincipal = 5,
    GrantCeilingExceeded = 6,
    ProviderUnauthorized = 7,
    ProviderUnsupported = 8,
    ProviderUnavailable = 9,
    ObservationFailed = 10,
}

public sealed record AclMutationPlanningFailure(
    AclMutationPlanningFailureCode Code,
    string SafeMessage);

public sealed record AclMutationPlanningResult(
    AclMutationPlan? Plan,
    MutationIntentDescriptor? Intent,
    MutationRiskDecision? Risk,
    AclMutationPlanningFailure? Failure)
{
    public bool IsSuccess =>
        Plan is not null &&
        Intent is not null &&
        Risk is not null &&
        Failure is null;

    public static AclMutationPlanningResult Success(
        AclMutationPlan plan,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk) =>
        new(plan, intent, risk, null);

    public static AclMutationPlanningResult Failed(
        AclMutationPlanningFailure failure) =>
        new(null, null, null, failure);
}

public sealed record AclMutationPlannerPolicy
{
    public AclMutationPlannerPolicy(TimeSpan observationTimeout)
    {
        if (observationTimeout < TimeSpan.FromSeconds(1) ||
            observationTimeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(nameof(observationTimeout));
        }

        ObservationTimeout = observationTimeout;
    }

    public TimeSpan ObservationTimeout { get; }

    public static AclMutationPlannerPolicy Default { get; } =
        new(TimeSpan.FromSeconds(10));
}

public sealed class AclMutationPlanner
{
    private readonly IAclObservationPort _observations;
    private readonly AclServerPolicy _policy;
    private readonly AclMutationPlannerPolicy _plannerPolicy;
    private readonly TimeProvider _timeProvider;

    public AclMutationPlanner(
        IAclObservationPort observations,
        AclServerPolicy policy,
        AclMutationPlannerPolicy? plannerPolicy = null,
        TimeProvider? timeProvider = null)
    {
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        _plannerPolicy = plannerPolicy ?? AclMutationPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<AclMutationPlanningResult> PlanCreateAsync(
        string clusterId,
        IReadOnlyList<KafkaAclBinding> requestedBindings,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var cluster = RequireCluster(clusterId);
            var requested = AclMutationPolicy.ValidateCreates(requestedBindings, _policy);
            var observed = await ObserveExactAsync(cluster, requested, cancellationToken)
                .ConfigureAwait(false);
            if (!observed.IsSuccess)
            {
                return Failed(observed.Failure!);
            }

            var existing = observed.Value!
                .ToHashSet();
            var creates = requested
                .Where(binding => !existing.Contains(binding))
                .ToArray();
            if (creates.Length == 0)
            {
                return Failed(
                    AclMutationPlanningFailureCode.NoChange,
                    "All requested ACL bindings already exist.");
            }

            var plan = new AclMutationPlan(
                AclMutationMode.Create,
                cluster,
                Array.AsReadOnly(creates),
                Array.Empty<KafkaAclBinding>(),
                null,
                AclMutationPolicy.FingerprintBindings(observed.Value!));
            return Success(plan);
        }
        catch (AclPolicyException exception)
        {
            return Failed(MapPolicy(exception), exception.Message);
        }
        catch (ArgumentException)
        {
            return Failed(
                AclMutationPlanningFailureCode.InvalidInput,
                "ACL create request is invalid.");
        }
    }

    public async Task<AclMutationPlanningResult> PlanRemoveAsync(
        string clusterId,
        KafkaAclBindingFilter filter,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var cluster = RequireCluster(clusterId);
            var normalizedFilter = AclMutationPolicy.NormalizeMutationFilter(filter);
            var observed = await _observations.DescribeAsync(
                    cluster,
                    normalizedFilter,
                    Operation(),
                    cancellationToken)
                .ConfigureAwait(false);
            if (!observed.IsSuccess || observed.Value is null)
            {
                return Failed(observed.Failure!);
            }

            if (observed.Value.Count == 0)
            {
                return Failed(
                    AclMutationPlanningFailureCode.NoMatchingBindings,
                    "ACL removal filter currently matches no binding.");
            }

            var removals = AclMutationPolicy.ValidateRemovals(
                observed.Value,
                _policy);
            var plan = new AclMutationPlan(
                AclMutationMode.Remove,
                cluster,
                Array.Empty<KafkaAclBinding>(),
                removals,
                normalizedFilter,
                AclMutationPolicy.FingerprintBindings(observed.Value));
            return Success(plan);
        }
        catch (AclPolicyException exception)
        {
            return Failed(MapPolicy(exception), exception.Message);
        }
        catch (ArgumentException)
        {
            return Failed(
                AclMutationPlanningFailureCode.InvalidInput,
                "ACL removal request is invalid.");
        }
    }

    public async Task<AclMutationPlanningResult> PlanReplaceAsync(
        string clusterId,
        KafkaAclBindingFilter sourceFilter,
        IReadOnlyList<KafkaAclBinding> desiredBindings,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var cluster = RequireCluster(clusterId);
            var normalizedFilter =
                AclMutationPolicy.NormalizeMutationFilter(sourceFilter);
            var desired =
                AclMutationPolicy.ValidateCreates(desiredBindings, _policy);

            if (desired.Any(binding =>
                    !AclMutationPolicy.MatchesFilter(binding, normalizedFilter)))
            {
                return Failed(
                    AclMutationPlanningFailureCode.InvalidInput,
                    "Every replacement ACL binding must remain inside the exact source filter.");
            }

            var observed = await _observations.DescribeAsync(
                    cluster,
                    normalizedFilter,
                    Operation(),
                    cancellationToken)
                .ConfigureAwait(false);
            if (!observed.IsSuccess || observed.Value is null)
            {
                return Failed(observed.Failure!);
            }

            var current = AclMutationPolicy.ValidateRemovals(
                observed.Value,
                _policy);
            var currentSet = current.ToHashSet();
            var desiredSet = desired.ToHashSet();

            var removals = current
                .Where(binding => !desiredSet.Contains(binding))
                .ToArray();
            var creates = desired
                .Where(binding => !currentSet.Contains(binding))
                .ToArray();

            if (removals.Length == 0 && creates.Length == 0)
            {
                return Failed(
                    AclMutationPlanningFailureCode.NoChange,
                    "Replacement ACL set already matches the observed provider state.");
            }

            var plan = new AclMutationPlan(
                AclMutationMode.Replace,
                cluster,
                Array.AsReadOnly(creates),
                Array.AsReadOnly(removals),
                normalizedFilter,
                AclMutationPolicy.FingerprintBindings(observed.Value));
            return Success(plan);
        }
        catch (AclPolicyException exception)
        {
            return Failed(MapPolicy(exception), exception.Message);
        }
        catch (ArgumentException)
        {
            return Failed(
                AclMutationPlanningFailureCode.InvalidInput,
                "ACL replacement request is invalid.");
        }
    }

    private async Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> ObserveExactAsync(
        string clusterId,
        IReadOnlyList<KafkaAclBinding> bindings,
        CancellationToken cancellationToken)
    {
        var observed = new HashSet<KafkaAclBinding>();
        ObservationMetadata? metadata = null;

        foreach (var binding in bindings)
        {
            var result = await _observations.DescribeAsync(
                    clusterId,
                    AclMutationPolicy.ExactFilter(binding),
                    Operation(),
                    cancellationToken)
                .ConfigureAwait(false);
            metadata = result.Observation;
            if (!result.IsSuccess || result.Value is null)
            {
                return KafkaResult<IReadOnlyList<KafkaAclBinding>>.Failed(
                    result.Failure!,
                    result.Observation);
            }

            foreach (var item in result.Value)
            {
                observed.Add(item);
            }
        }

        var ordered = observed
            .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
            .ToArray();
        return KafkaResult<IReadOnlyList<KafkaAclBinding>>.Success(
            Array.AsReadOnly(ordered),
            metadata ?? LiveObservation());
    }

    private AclMutationPlanningResult Success(AclMutationPlan plan)
    {
        var intent = AclMutationPolicy.BuildIntent(plan);
        var risk = AclMutationPolicy.ClassifyRisk(
            plan.CreateBindings,
            plan.RemoveBindings);
        return AclMutationPlanningResult.Success(plan, intent, risk);
    }

    private AclMutationPlanningResult Failed(KafkaFailure failure) =>
        failure.Category switch
        {
            KafkaFailureCategory.Unauthorized =>
                Failed(
                    AclMutationPlanningFailureCode.ProviderUnauthorized,
                    "Kafka denied ACL inspection required for safe mutation planning."),
            KafkaFailureCategory.NotSupported =>
                Failed(
                    AclMutationPlanningFailureCode.ProviderUnsupported,
                    "Kafka or the pinned client does not support the required ACL observation capability."),
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.AuthenticationFailed or
            KafkaFailureCategory.TlsFailure =>
                Failed(
                    AclMutationPlanningFailureCode.ProviderUnavailable,
                    "Kafka ACL state is not currently observable for safe mutation planning."),
            _ =>
                Failed(
                    AclMutationPlanningFailureCode.ObservationFailed,
                    "Kafka ACL state could not be safely observed."),
        };

    private static AclMutationPlanningFailureCode MapPolicy(
        AclPolicyException exception) =>
        exception.Code switch
        {
            AclPolicyFailureCode.TooManyBindings =>
                AclMutationPlanningFailureCode.TooManyBindings,
            AclPolicyFailureCode.ProtectedPrincipal =>
                AclMutationPlanningFailureCode.ProtectedPrincipal,
            AclPolicyFailureCode.GrantCeilingExceeded =>
                AclMutationPlanningFailureCode.GrantCeilingExceeded,
            _ => AclMutationPlanningFailureCode.InvalidInput,
        };

    private KafkaOperationContext Operation() =>
        new(_timeProvider.GetUtcNow().Add(_plannerPolicy.ObservationTimeout));

    private ObservationMetadata LiveObservation()
    {
        var now = _timeProvider.GetUtcNow();
        return new ObservationMetadata(now, now, now, ObservationSource.Live);
    }

    private static string RequireCluster(string clusterId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        if (!string.Equals(clusterId, clusterId.Trim(), StringComparison.Ordinal) ||
            clusterId.Length > 256 ||
            clusterId.Any(char.IsControl))
        {
            throw new ArgumentException("ACL cluster ID is invalid.", nameof(clusterId));
        }

        return clusterId;
    }

    private static AclMutationPlanningResult Failed(
        AclMutationPlanningFailureCode code,
        string message) =>
        AclMutationPlanningResult.Failed(
            new AclMutationPlanningFailure(code, message));
}

public sealed class AclAccessAnalysisService
{
    private readonly IAclObservationPort _observations;
    private readonly AclMutationPlannerPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public AclAccessAnalysisService(
        IAclObservationPort observations,
        AclMutationPlannerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
        _policy = policy ?? AclMutationPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<KafkaResult<AclAccessAnalysis>> AnalyzeAsync(
        string clusterId,
        AclAccessQuery query,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        var filter = AclMutationPolicy.NormalizeMutationFilter(
            new KafkaAclBindingFilter(
                query.ResourceType,
                query.ResourceName,
                KafkaAclFilterPatternMode.Match,
                query.Principal,
                Host: null,
                query.Operation,
                PermissionType: null));

        var observed = await _observations.DescribeAsync(
                clusterId,
                filter,
                new KafkaOperationContext(
                    _timeProvider.GetUtcNow().Add(_policy.ObservationTimeout)),
                cancellationToken)
            .ConfigureAwait(false);
        if (!observed.IsSuccess || observed.Value is null)
        {
            return KafkaResult<AclAccessAnalysis>.Failed(
                observed.Failure!,
                observed.Observation);
        }

        return KafkaResult<AclAccessAnalysis>.Success(
            AclMutationPolicy.AnalyzeObservedAccess(query, observed.Value),
            observed.Observation);
    }
}
