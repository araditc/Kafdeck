using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Connect;

public enum ConnectAutoRestartPolicyMutationMode
{
    EnableOrReplace = 1,
    Disable = 2,
}

public enum ConnectAutoRestartPolicyPlanningFailureCode
{
    InvalidInput = 1,
    NoChange = 2,
    TargetNotFound = 3,
    TaskNotFound = 4,
    ObservationUnavailable = 5,
    DeploymentPolicyDisabled = 6,
    AutomationAuthorizationDenied = 7,
    ReplicationBlocked = 8,
    PersistenceUnavailable = 9,
}

public sealed record ConnectAutoRestartPolicyRequest(
    string ClusterId,
    string ConnectProfileId,
    string ConnectorName,
    bool Enabled,
    int? TaskId = null,
    int? MaxAttempts = null,
    int? InitialBackoffSeconds = null,
    int? MaxBackoffSeconds = null,
    int? ActivationLifetimeSeconds = null,
    int? MaxActivePoliciesPerProfile = null,
    int? JitterBasisPoints = null);

public sealed record ConnectAutoRestartPolicyCanonicalIntent(
    ConnectAutoRestartPolicyMutationMode Mode,
    ConnectAutoRestartTarget Target,
    ConnectAutoRestartPolicy? DesiredPolicy,
    string AutomationPrincipalId,
    string ProviderIdentityFingerprint,
    string ConnectorConfigurationFingerprint,
    string EffectivePolicyFingerprint,
    Guid? ExistingActivationId,
    long? ExistingActivationVersion);

public sealed record ConnectAutoRestartPolicyPlan(
    ConnectAutoRestartPolicyCanonicalIntent Canonical,
    MutationIntentDescriptor Intent,
    MutationRiskDecision Risk);

public sealed record ConnectAutoRestartPolicyPlanningFailure(
    ConnectAutoRestartPolicyPlanningFailureCode Code,
    string SafeMessage);

public sealed record ConnectAutoRestartPolicyPlanningResult
{
    private ConnectAutoRestartPolicyPlanningResult(
        ConnectAutoRestartPolicyPlan? plan,
        ConnectAutoRestartPolicyPlanningFailure? failure)
    {
        Plan = plan;
        Failure = failure;
    }

    public ConnectAutoRestartPolicyPlan? Plan { get; }
    public ConnectAutoRestartPolicyPlanningFailure? Failure { get; }
    public bool IsSuccess => Plan is not null && Failure is null;

    public static ConnectAutoRestartPolicyPlanningResult Success(
        ConnectAutoRestartPolicyPlan plan) =>
        new(
            plan ?? throw new ArgumentNullException(nameof(plan)),
            null);

    public static ConnectAutoRestartPolicyPlanningResult Failed(
        ConnectAutoRestartPolicyPlanningFailure failure) =>
        new(
            null,
            failure ?? throw new ArgumentNullException(nameof(failure)));
}

public sealed class ConnectAutoRestartPolicyPlanner
{
    private static readonly string EmptyFingerprint =
        new('0', 64);

    private readonly ConnectMutationPlanner _connect;
    private readonly IConnectAutoRestartStateStore _store;
    private readonly IConnectAutoRestartGovernancePort _governance;
    private readonly IConnectAutoRestartRuntimePolicyProvider _runtimePolicy;
    private readonly TimeProvider _timeProvider;

    public ConnectAutoRestartPolicyPlanner(
        ConnectMutationPlanner connect,
        IConnectAutoRestartStateStore store,
        IConnectAutoRestartGovernancePort governance,
        IConnectAutoRestartRuntimePolicyProvider runtimePolicy,
        TimeProvider? timeProvider = null)
    {
        _connect =
            connect ?? throw new ArgumentNullException(nameof(connect));
        _store =
            store ?? throw new ArgumentNullException(nameof(store));
        _governance =
            governance ?? throw new ArgumentNullException(nameof(governance));
        _runtimePolicy =
            runtimePolicy ??
            throw new ArgumentNullException(nameof(runtimePolicy));
        _timeProvider =
            timeProvider ?? TimeProvider.System;
    }

    public async Task<ConnectAutoRestartPolicyPlanningResult> PlanAsync(
        ConnectAutoRestartPolicyRequest request,
        string automationPrincipalId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            automationPrincipalId);

        ConnectAutoRestartTarget target;
        try
        {
            target = new ConnectAutoRestartTarget(
                ConnectMutationCanonicalization.RequireIdentifier(
                    request.ClusterId,
                    "Cluster ID",
                    256),
                ConnectMutationCanonicalization.RequireConnectProfileId(
                    request.ConnectProfileId),
                ConnectMutationCanonicalization.RequireConnectorName(
                    request.ConnectorName),
                request.TaskId);

            if (request.TaskId is < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(request.TaskId));
            }
        }
        catch (ArgumentException exception)
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.InvalidInput,
                exception.Message);
        }

        ConnectAutoRestartActivation? existing;
        try
        {
            existing =
                await _store
                    .GetActiveByTargetAsync(
                        target,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.PersistenceUnavailable,
                "Connect auto-restart policy state is currently unavailable.");
        }

        if (!request.Enabled)
        {
            if (existing is null)
            {
                return Failed(
                    ConnectAutoRestartPolicyPlanningFailureCode.NoChange,
                    "Connect auto-restart policy is already disabled.");
            }

            var canonical =
                new ConnectAutoRestartPolicyCanonicalIntent(
                    ConnectAutoRestartPolicyMutationMode.Disable,
                    target,
                    null,
                    automationPrincipalId.Trim(),
                    existing.ProviderIdentityFingerprint,
                    existing.ConnectorConfigurationFingerprint,
                    existing.PolicyFingerprint,
                    existing.ActivationId,
                    existing.Version);

            return Success(
                canonical,
                durabilitySensitive: false,
                requireRead: false);
        }

        ConnectAutoRestartRuntimePolicySnapshot runtime;
        try
        {
            runtime =
                await _runtimePolicy
                    .GetCurrentAsync(
                        target,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.InvalidInput,
                "Connect auto-restart deployment policy is invalid.");
        }

        if (!runtime.Enabled)
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.DeploymentPolicyDisabled,
                "Connect auto-restart is disabled by deployment policy.");
        }

        ConnectAutoRestartPolicy desired;
        try
        {
            desired = BuildDesiredPolicy(
                request,
                runtime.Policy);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.InvalidInput,
                exception.Message);
        }

        var observed =
            await _connect
                .ObserveAsync(
                    target.ClusterId,
                    target.ConnectProfileId,
                    target.ConnectorName,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!observed.IsSuccess ||
            observed.Value is null)
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.ObservationUnavailable,
                "Kafka Connect target could not be safely observed.");
        }

        if (!observed.Value.Exists)
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.TargetNotFound,
                "Kafka Connect connector does not exist.");
        }

        if (target.TaskId.HasValue &&
            observed.Value.Tasks.All(item =>
                item.Id != target.TaskId.Value))
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.TaskNotFound,
                "Kafka Connect task does not exist.");
        }

        var replication =
            ConnectReplicationActivationGuard.ValidateControl(
                ConnectControlAction.Restart,
                observed.Value.Configuration);
        if (replication.Outcome !=
            MutationPreDispatchGuardOutcome.Allowed)
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.ReplicationBlocked,
                replication.ResultCode);
        }

        ConnectAutoRestartGovernanceSnapshot governance;
        try
        {
            var probe =
                ConnectAutoRestartActivation.Create(
                    target,
                    desired,
                    _timeProvider.GetUtcNow(),
                    EmptyFingerprint,
                    observed.Value.ConfigurationFingerprint,
                    EmptyFingerprint,
                    automationPrincipalId.Trim(),
                    Guid.NewGuid());

            governance =
                await _governance
                    .GetCurrentAsync(
                        probe,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.ObservationUnavailable,
                "Connect auto-restart governance evidence is unavailable.");
        }

        if (!governance.AuthorizationAllowed)
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.AutomationAuthorizationDenied,
                "Automation principal is not currently authorized to restart the target.");
        }

        if (existing is not null &&
            existing.Policy == desired &&
            string.Equals(
                existing.AutomationPrincipalId,
                automationPrincipalId.Trim(),
                StringComparison.Ordinal) &&
            string.Equals(
                existing.ProviderIdentityFingerprint,
                governance.ProviderIdentityFingerprint,
                StringComparison.Ordinal) &&
            string.Equals(
                existing.ConnectorConfigurationFingerprint,
                observed.Value.ConfigurationFingerprint,
                StringComparison.Ordinal) &&
            string.Equals(
                existing.PolicyFingerprint,
                governance.PolicyFingerprint,
                StringComparison.Ordinal))
        {
            return Failed(
                ConnectAutoRestartPolicyPlanningFailureCode.NoChange,
                "Requested Connect auto-restart policy already matches active policy.");
        }

        var enabledCanonical =
            new ConnectAutoRestartPolicyCanonicalIntent(
                ConnectAutoRestartPolicyMutationMode.EnableOrReplace,
                target,
                desired,
                automationPrincipalId.Trim(),
                governance.ProviderIdentityFingerprint,
                observed.Value.ConfigurationFingerprint,
                governance.PolicyFingerprint,
                existing?.ActivationId,
                existing?.Version);

        return Success(
            enabledCanonical,
            durabilitySensitive: true,
            requireRead: true);
    }

    private static ConnectAutoRestartPolicy BuildDesiredPolicy(
        ConnectAutoRestartPolicyRequest request,
        ConnectAutoRestartPolicy defaults) =>
        new(
            enabled: true,
            request.MaxAttempts ??
                defaults.MaxAttempts,
            request.InitialBackoffSeconds.HasValue
                ? TimeSpan.FromSeconds(
                    request.InitialBackoffSeconds.Value)
                : defaults.InitialBackoff,
            request.MaxBackoffSeconds.HasValue
                ? TimeSpan.FromSeconds(
                    request.MaxBackoffSeconds.Value)
                : defaults.MaxBackoff,
            request.ActivationLifetimeSeconds.HasValue
                ? TimeSpan.FromSeconds(
                    request.ActivationLifetimeSeconds.Value)
                : defaults.ActivationLifetime,
            request.MaxActivePoliciesPerProfile ??
                defaults.MaxActivePoliciesPerProfile,
            request.JitterBasisPoints ??
                defaults.JitterBasisPoints);

    private static ConnectAutoRestartPolicyPlanningResult Success(
        ConnectAutoRestartPolicyCanonicalIntent canonical,
        bool durabilitySensitive,
        bool requireRead)
    {
        var resource =
            canonical.Target.AuthorizationResource;

        var requirements =
            new List<MutationAuthorizationTarget>
            {
                new(
                    AuthorizationAction.ConnectAutoRestartManage,
                    canonical.Target.ClusterId,
                    resource),
            };

        if (requireRead)
        {
            requirements.Add(
                new MutationAuthorizationTarget(
                    AuthorizationAction.ConnectRead,
                    canonical.Target.ClusterId,
                    resource));
        }

        var intent =
            new MutationIntentDescriptor(
                MutationOperationKind.ConnectAutoRestartPolicy,
                canonical.Target.ClusterId,
                ConnectMutationCanonicalization.Serialize(
                    canonical),
                new[]
                {
                    canonical.Target.CanonicalKey,
                },
                new[]
                {
                    new MutationPrecondition(
                        "connect.autorestart.active",
                        ExistingFingerprint(canonical)),
                },
                AuthorizationTargets:
                    Array.AsReadOnly(requirements.ToArray()));

        var risk =
            MutationRiskClassifier.Classify(
                new MutationRiskInput(
                    MutationOperationKind.ConnectAutoRestartPolicy,
                    DurabilitySensitiveChange:
                        durabilitySensitive));

        return ConnectAutoRestartPolicyPlanningResult.Success(
            new ConnectAutoRestartPolicyPlan(
                canonical,
                intent,
                risk));
    }

    private static string ExistingFingerprint(
        ConnectAutoRestartPolicyCanonicalIntent canonical)
    {
        var value = canonical.ExistingActivationId.HasValue
            ? $"{canonical.ExistingActivationId.Value:D}|{canonical.ExistingActivationVersion}"
            : "absent";

        return ConnectMutationCanonicalization.Sha256(
            System.Text.Encoding.UTF8.GetBytes(value));
    }

    private static ConnectAutoRestartPolicyPlanningResult Failed(
        ConnectAutoRestartPolicyPlanningFailureCode code,
        string message) =>
        ConnectAutoRestartPolicyPlanningResult.Failed(
            new ConnectAutoRestartPolicyPlanningFailure(
                code,
                message));
}

public sealed class ConnectAutoRestartPolicyPreconditionValidator
{
    private readonly IConnectAutoRestartStateStore _store;
    private readonly ConnectMutationPlanner _connect;
    private readonly IConnectAutoRestartGovernancePort _governance;
    private readonly IConnectAutoRestartRuntimePolicyProvider _runtimePolicy;
    private readonly TimeProvider _timeProvider;

    public ConnectAutoRestartPolicyPreconditionValidator(
        IConnectAutoRestartStateStore store,
        ConnectMutationPlanner connect,
        IConnectAutoRestartGovernancePort governance,
        IConnectAutoRestartRuntimePolicyProvider runtimePolicy,
        TimeProvider? timeProvider = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _connect = connect ?? throw new ArgumentNullException(nameof(connect));
        _governance = governance ?? throw new ArgumentNullException(nameof(governance));
        _runtimePolicy = runtimePolicy ?? throw new ArgumentNullException(nameof(runtimePolicy));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        if (operation.OperationKind !=
            MutationOperationKind.ConnectAutoRestartPolicy)
        {
            return Unsupported(
                "auto_restart_policy_operation_not_supported");
        }

        ConnectAutoRestartPolicyCanonicalIntent canonical;
        try
        {
            canonical =
                ConnectMutationCanonicalization.Deserialize<
                    ConnectAutoRestartPolicyCanonicalIntent>(
                    operation.CanonicalIntent);
        }
        catch
        {
            return Stale(
                "auto_restart_policy_intent_invalid");
        }

        if (operation.ResourceKeys.Count != 1 ||
            !string.Equals(
                operation.ResourceKeys[0],
                canonical.Target.CanonicalKey,
                StringComparison.Ordinal))
        {
            return Stale(
                "auto_restart_policy_target_binding_changed");
        }

        var active =
            await _store
                .GetActiveByTargetAsync(
                    canonical.Target,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!ExistingMatches(
                canonical,
                active))
        {
            return Stale(
                "auto_restart_policy_active_state_changed");
        }

        if (canonical.Mode ==
            ConnectAutoRestartPolicyMutationMode.Disable)
        {
            return MutationPreDispatchGuardResult.Allowed;
        }

        if (canonical.DesiredPolicy is null)
        {
            return Stale(
                "auto_restart_policy_desired_policy_missing");
        }

        var runtime =
            await _runtimePolicy
                .GetCurrentAsync(
                    canonical.Target,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!runtime.Enabled)
        {
            return Unsupported(
                "auto_restart_deployment_policy_disabled");
        }

        var observed =
            await _connect
                .ObserveAsync(
                    canonical.Target.ClusterId,
                    canonical.Target.ConnectProfileId,
                    canonical.Target.ConnectorName,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!observed.IsSuccess ||
            observed.Value is null ||
            !observed.Value.Exists)
        {
            return Unsupported(
                "auto_restart_policy_target_unavailable");
        }

        if (!string.Equals(
                observed.Value.ConfigurationFingerprint,
                canonical.ConnectorConfigurationFingerprint,
                StringComparison.Ordinal))
        {
            return Stale(
                "auto_restart_policy_configuration_changed");
        }

        var replication =
            ConnectReplicationActivationGuard.ValidateControl(
                ConnectControlAction.Restart,
                observed.Value.Configuration);

        if (replication.Outcome !=
            MutationPreDispatchGuardOutcome.Allowed)
        {
            return replication;
        }

        var probe =
            ConnectAutoRestartActivation.Create(
                canonical.Target,
                canonical.DesiredPolicy,
                _timeProvider.GetUtcNow(),
                canonical.ProviderIdentityFingerprint,
                canonical.ConnectorConfigurationFingerprint,
                canonical.EffectivePolicyFingerprint,
                canonical.AutomationPrincipalId,
                Guid.NewGuid());

        var governance =
            await _governance
                .GetCurrentAsync(
                    probe,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!governance.AuthorizationAllowed)
        {
            return Denied(
                governance.DenialCode ??
                "auto_restart_automation_authorization_revoked");
        }

        if (!string.Equals(
                governance.ProviderIdentityFingerprint,
                canonical.ProviderIdentityFingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                governance.PolicyFingerprint,
                canonical.EffectivePolicyFingerprint,
                StringComparison.Ordinal))
        {
            return Stale(
                "auto_restart_policy_governance_changed");
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    internal static bool ExistingMatches(
        ConnectAutoRestartPolicyCanonicalIntent canonical,
        ConnectAutoRestartActivation? active)
    {
        if (!canonical.ExistingActivationId.HasValue)
        {
            return active is null;
        }

        return active is not null &&
               active.ActivationId ==
               canonical.ExistingActivationId.Value &&
               active.Version ==
               canonical.ExistingActivationVersion;
    }

    private static MutationPreDispatchGuardResult Stale(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome.StalePreview,
            code);

    private static MutationPreDispatchGuardResult Unsupported(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            code);

    private static MutationPreDispatchGuardResult Denied(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome.AuthorizationDenied,
            code);
}

public sealed class ConnectAutoRestartPolicyExecutionHandler :
    IMutationExecutionHandler
{
    private readonly IConnectAutoRestartStateStore _store;
    private readonly TimeProvider _timeProvider;

    public ConnectAutoRestartPolicyExecutionHandler(
        IConnectAutoRestartStateStore store,
        TimeProvider? timeProvider = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.ConnectAutoRestartPolicy;

    public async Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        ConnectAutoRestartPolicyCanonicalIntent canonical;
        try
        {
            canonical =
                ConnectMutationCanonicalization.Deserialize<
                    ConnectAutoRestartPolicyCanonicalIntent>(
                    context.Operation.CanonicalIntent);
        }
        catch
        {
            return FailedBeforeDispatch(
                "auto_restart_policy_canonical_invalid");
        }

        var active =
            await _store
                .GetActiveByTargetAsync(
                    canonical.Target,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!ConnectAutoRestartPolicyPreconditionValidator.ExistingMatches(
                canonical,
                active))
        {
            return FailedBeforeDispatch(
                "auto_restart_policy_active_state_changed");
        }

        if (canonical.Mode ==
            ConnectAutoRestartPolicyMutationMode.Disable)
        {
            if (active is null)
            {
                return FailedBeforeDispatch(
                    "auto_restart_policy_already_disabled");
            }

            return await DisableExistingAsync(
                    active,
                    "auto_restart_policy_disabled",
                    cancellationToken)
                .ConfigureAwait(false)
                ? Verified(
                    "auto_restart_policy_disabled")
                : FailedBeforeDispatch(
                    "auto_restart_policy_disable_conflict");
        }

        if (canonical.DesiredPolicy is null)
        {
            return FailedBeforeDispatch(
                "auto_restart_policy_desired_policy_missing");
        }

        if (active is not null)
        {
            if (!await DisableExistingAsync(
                    active,
                    "auto_restart_policy_superseded",
                    cancellationToken)
                .ConfigureAwait(false))
            {
                return FailedBeforeDispatch(
                    "auto_restart_policy_replace_conflict");
            }
        }

        var now = _timeProvider.GetUtcNow();
        ConnectAutoRestartActivation replacement;
        try
        {
            replacement =
                ConnectAutoRestartActivation.Create(
                    canonical.Target,
                    canonical.DesiredPolicy,
                    now,
                    canonical.ProviderIdentityFingerprint,
                    canonical.ConnectorConfigurationFingerprint,
                    canonical.EffectivePolicyFingerprint,
                    canonical.AutomationPrincipalId,
                    context.Operation.OperationId);
        }
        catch
        {
            return FailedBeforeDispatch(
                "auto_restart_policy_activation_invalid");
        }

        bool created;
        try
        {
            created =
                await _store
                    .TryCreateAsync(
                        replacement,
                        canonical.DesiredPolicy
                            .MaxActivePoliciesPerProfile,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch
        {
            return FailedBeforeDispatch(
                "auto_restart_policy_persistence_unavailable");
        }

        return created
            ? Verified(
                active is null
                    ? "auto_restart_policy_enabled"
                    : "auto_restart_policy_replaced")
            : FailedBeforeDispatch(
                "auto_restart_policy_activation_conflict");
    }

    private async Task<bool> DisableExistingAsync(
        ConnectAutoRestartActivation active,
        string reason,
        CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();
        var lease =
            await _store
                .TryAcquireLeaseAsync(
                    active.ActivationId,
                    $"policy-{active.ActivationId:N}",
                    now,
                    TimeSpan.FromSeconds(30),
                    cancellationToken)
                .ConfigureAwait(false);

        if (lease is null)
        {
            return false;
        }

        var disabled =
            active.Disable(reason);

        return await _store
            .TryUpdateAsync(
                disabled,
                active.Version,
                lease,
                now,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static MutationProviderResult Verified(
        string code) =>
        new(
            MutationExecutionResultKind.AppliedVerified,
            code);

    private static MutationProviderResult FailedBeforeDispatch(
        string code) =>
        new(
            MutationExecutionResultKind.FailedBeforeDispatch,
            code);
}
