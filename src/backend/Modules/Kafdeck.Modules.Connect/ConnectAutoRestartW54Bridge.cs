using Kafdeck.Core.Ecosystem;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Connect;

public sealed class ConnectAutoRestartAttemptRevalidator :
    IConnectAutoRestartAttemptRevalidator
{
    private readonly ConnectMutationPlanner _planner;
    private readonly ConnectMutationPreconditionValidator _preconditions;
    private readonly IConnectAutoRestartGovernancePort _governance;
    private readonly TimeProvider _timeProvider;

    public ConnectAutoRestartAttemptRevalidator(
        ConnectMutationPlanner planner,
        ConnectMutationPreconditionValidator preconditions,
        IConnectAutoRestartGovernancePort governance,
        TimeProvider? timeProvider = null)
    {
        _planner =
            planner ?? throw new ArgumentNullException(nameof(planner));
        _preconditions =
            preconditions ??
            throw new ArgumentNullException(nameof(preconditions));
        _governance =
            governance ??
            throw new ArgumentNullException(nameof(governance));
        _timeProvider =
            timeProvider ?? TimeProvider.System;
    }

    public async Task<ConnectAutoRestartRevalidationResult> RevalidateAsync(
        ConnectAutoRestartActivation activation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(activation);

        var governance =
            await _governance
                .GetCurrentAsync(
                    activation,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!governance.AuthorizationAllowed)
        {
            return Result(
                ConnectAutoRestartRevalidationOutcome.AuthorizationDenied,
                governance.DenialCode ??
                "auto_restart_authorization_revoked");
        }

        if (!string.Equals(
                governance.ProviderIdentityFingerprint,
                activation.ProviderIdentityFingerprint,
                StringComparison.Ordinal))
        {
            return Result(
                ConnectAutoRestartRevalidationOutcome.ProviderIdentityDrift,
                "auto_restart_provider_identity_drift");
        }

        if (!string.Equals(
                governance.PolicyFingerprint,
                activation.PolicyFingerprint,
                StringComparison.Ordinal))
        {
            return Result(
                ConnectAutoRestartRevalidationOutcome.PolicyDrift,
                "auto_restart_policy_drift");
        }

        var observed =
            await _planner
                .ObserveAsync(
                    activation.Target.ClusterId,
                    activation.Target.ConnectProfileId,
                    activation.Target.ConnectorName,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!observed.IsSuccess ||
            observed.Value is null)
        {
            return Result(
                ConnectAutoRestartRevalidationOutcome.TargetUnavailable,
                "auto_restart_target_observation_unavailable");
        }

        if (!observed.Value.Exists)
        {
            return Result(
                ConnectAutoRestartRevalidationOutcome.TargetUnavailable,
                "auto_restart_target_missing");
        }

        if (!string.Equals(
                observed.Value.ConfigurationFingerprint,
                activation.ConnectorConfigurationFingerprint,
                StringComparison.Ordinal))
        {
            return Result(
                ConnectAutoRestartRevalidationOutcome.ConfigurationDrift,
                "auto_restart_connector_configuration_drift");
        }

        if (activation.Target.TaskId.HasValue)
        {
            var task = observed.Value.Tasks
                .SingleOrDefault(item =>
                    item.Id ==
                    activation.Target.TaskId.Value);

            if (task is null)
            {
                return Result(
                    ConnectAutoRestartRevalidationOutcome.TargetUnavailable,
                    "auto_restart_task_missing");
            }

            if (!string.Equals(
                    task.State,
                    "FAILED",
                    StringComparison.Ordinal))
            {
                return Result(
                    ConnectAutoRestartRevalidationOutcome.Recovered,
                    "auto_restart_task_recovered");
            }
        }
        else if (!string.Equals(
                     observed.Value.State,
                     "FAILED",
                     StringComparison.Ordinal))
        {
            return Result(
                ConnectAutoRestartRevalidationOutcome.Recovered,
                "auto_restart_connector_recovered");
        }

        var planning =
            await _planner
                .PlanControlAsync(
                    new ConnectControlRequest(
                        activation.Target.ClusterId,
                        activation.Target.ConnectorName,
                        ConnectControlAction.Restart,
                        activation.Target.TaskId,
                        activation.Target.ConnectProfileId),
                    cancellationToken)
                .ConfigureAwait(false);

        if (!planning.IsSuccess ||
            planning.Plan is null)
        {
            return planning.Failure?.Code switch
            {
                ConnectMutationPlanningFailureCode.ProviderUnsupported or
                ConnectMutationPlanningFailureCode.ProviderNotConfigured =>
                    Result(
                        ConnectAutoRestartRevalidationOutcome.CapabilityBlocked,
                        "auto_restart_restart_capability_unavailable"),

                ConnectMutationPlanningFailureCode.ConnectorNotFound or
                ConnectMutationPlanningFailureCode.TaskNotFound =>
                    Result(
                        ConnectAutoRestartRevalidationOutcome.TargetUnavailable,
                        "auto_restart_target_missing"),

                _ =>
                    Result(
                        ConnectAutoRestartRevalidationOutcome.TargetUnavailable,
                        "auto_restart_restart_planning_unavailable"),
            };
        }

        var snapshot = BuildValidationSnapshot(
            activation,
            planning.Plan);

        var guarded =
            await _preconditions
                .ValidateAsync(
                    snapshot,
                    cancellationToken)
                .ConfigureAwait(false);

        if (guarded.Outcome ==
            MutationPreDispatchGuardOutcome.Allowed)
        {
            return Result(
                ConnectAutoRestartRevalidationOutcome.Allowed,
                "auto_restart_revalidation_allowed");
        }

        if (guarded.ResultCode.Contains(
                "replication",
                StringComparison.OrdinalIgnoreCase))
        {
            return Result(
                ConnectAutoRestartRevalidationOutcome.ReplicationGuardBlocked,
                guarded.ResultCode);
        }

        return guarded.Outcome switch
        {
            MutationPreDispatchGuardOutcome.AuthorizationDenied =>
                Result(
                    ConnectAutoRestartRevalidationOutcome.AuthorizationDenied,
                    guarded.ResultCode),

            MutationPreDispatchGuardOutcome.StalePreview =>
                Result(
                    ConnectAutoRestartRevalidationOutcome.ConfigurationDrift,
                    guarded.ResultCode),

            _ =>
                Result(
                    ConnectAutoRestartRevalidationOutcome.CapabilityBlocked,
                    guarded.ResultCode),
        };
    }

    private MutationOperationSnapshot BuildValidationSnapshot(
        ConnectAutoRestartActivation activation,
        ConnectMutationPlan<ConnectControlCanonicalIntent> plan)
    {
        var now = _timeProvider.GetUtcNow();
        var operation = MutationOperation.CreatePreview(
            activation.AutomationPrincipalId,
            plan.Intent,
            plan.Risk,
            "v0.7-w55-auto-restart",
            now.AddMinutes(2),
            now,
            $"w55-revalidate-{activation.ActivationId:N}-{activation.AttemptsUsed + 1}");

        return operation.Snapshot;
    }

    private static ConnectAutoRestartRevalidationResult Result(
        ConnectAutoRestartRevalidationOutcome outcome,
        string code) =>
        new(outcome, code);
}

public sealed class ConnectAutoRestartTypedDispatchAdapter :
    IConnectAutoRestartDispatchPort
{
    private readonly ConnectMutationPlanner _planner;
    private readonly ConnectMutationPreconditionValidator _preconditions;
    private readonly ConnectMutationExecutionService _execution;
    private readonly TimeProvider _timeProvider;

    public ConnectAutoRestartTypedDispatchAdapter(
        ConnectMutationPlanner planner,
        ConnectMutationPreconditionValidator preconditions,
        ConnectMutationExecutionService execution,
        TimeProvider? timeProvider = null)
    {
        _planner =
            planner ?? throw new ArgumentNullException(nameof(planner));
        _preconditions =
            preconditions ??
            throw new ArgumentNullException(nameof(preconditions));
        _execution =
            execution ?? throw new ArgumentNullException(nameof(execution));
        _timeProvider =
            timeProvider ?? TimeProvider.System;
    }

    public async Task<ConnectAutoRestartDispatchResult> RestartAsync(
        ConnectAutoRestartDispatchRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var planning =
            await _planner
                .PlanControlAsync(
                    new ConnectControlRequest(
                        request.Target.ClusterId,
                        request.Target.ConnectorName,
                        ConnectControlAction.Restart,
                        request.Target.TaskId,
                        request.Target.ConnectProfileId),
                    cancellationToken)
                .ConfigureAwait(false);

        if (!planning.IsSuccess ||
            planning.Plan is null)
        {
            return planning.Failure?.Code switch
            {
                ConnectMutationPlanningFailureCode.ProviderUnsupported or
                ConnectMutationPlanningFailureCode.ProviderNotConfigured =>
                    Blocked(
                        "auto_restart_restart_capability_unavailable"),

                ConnectMutationPlanningFailureCode.ConnectorNotFound or
                ConnectMutationPlanningFailureCode.TaskNotFound =>
                    Blocked(
                        "auto_restart_target_missing"),

                _ =>
                    Definitive(
                        "auto_restart_restart_planning_failed"),
            };
        }

        var now = _timeProvider.GetUtcNow();
        var validationOperation = MutationOperation.CreatePreview(
            request.AutomationPrincipalId,
            planning.Plan.Intent,
            planning.Plan.Risk,
            "v0.7-w55-auto-restart-dispatch",
            now.AddMinutes(2),
            now,
            $"w55-dispatch-{request.ActivationId:N}-{request.DispatchId:N}");

        var guarded =
            await _preconditions
                .ValidateAsync(
                    validationOperation.Snapshot,
                    cancellationToken)
                .ConfigureAwait(false);

        if (guarded.Outcome !=
            MutationPreDispatchGuardOutcome.Allowed)
        {
            return Blocked(
                guarded.ResultCode);
        }

        MutationProviderResult result;
        try
        {
            result =
                await _execution
                    .ControlAsync(
                        planning.Plan.Canonical,
                        cancellationToken,
                        now.AddMinutes(2))
                    .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return Ambiguous(
                "auto_restart_dispatch_cancelled_or_unknown");
        }
        catch
        {
            return Ambiguous(
                "auto_restart_dispatch_exception_unknown");
        }

        return result.ResultKind switch
        {
            MutationExecutionResultKind.AppliedVerified or
            MutationExecutionResultKind.AppliedUnverified =>
                Accepted(result.ResultCode),

            MutationExecutionResultKind.FailedBeforeDispatch or
            MutationExecutionResultKind.FailedDefinitive =>
                Definitive(result.ResultCode),

            _ =>
                Ambiguous(result.ResultCode),
        };
    }

    private static ConnectAutoRestartDispatchResult Accepted(
        string code) =>
        new(
            ConnectAutoRestartDispatchOutcome.Accepted,
            code);

    private static ConnectAutoRestartDispatchResult Definitive(
        string code) =>
        new(
            ConnectAutoRestartDispatchOutcome.FailedDefinitive,
            code);

    private static ConnectAutoRestartDispatchResult Ambiguous(
        string code) =>
        new(
            ConnectAutoRestartDispatchOutcome.Ambiguous,
            code);

    private static ConnectAutoRestartDispatchResult Blocked(
        string code) =>
        new(
            ConnectAutoRestartDispatchOutcome.Blocked,
            code);
}
