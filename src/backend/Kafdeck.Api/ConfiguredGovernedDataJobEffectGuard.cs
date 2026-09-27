using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;

namespace Kafdeck.Api;

public sealed class ConfiguredGovernedDataJobEffectGuard :
    IGovernedDataJobEffectGuard
{
    private readonly AuthorizationPolicyEvaluator _authorization;
    private readonly MutationApprovalAuthorizer _approval;
    private readonly GovernedDataJobPreconditionValidator _preconditions;

    public ConfiguredGovernedDataJobEffectGuard(
        AuthorizationPolicyEvaluator authorization,
        MutationApprovalAuthorizer approval,
        GovernedDataJobPreconditionValidator preconditions)
    {
        _authorization =
            authorization ??
            throw new ArgumentNullException(
                nameof(authorization));
        _approval =
            approval ??
            throw new ArgumentNullException(
                nameof(approval));
        _preconditions =
            preconditions ??
            throw new ArgumentNullException(
                nameof(preconditions));
    }

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        GovernedDataJobPlan plan,
        int rangeIndex,
        long sourceOffset,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(plan);

        if (!OperationAndRangeMatch(
                operation,
                plan,
                rangeIndex,
                sourceOffset))
        {
            return Stale(
                "data_job_effect_binding_invalid");
        }

        if (!RequesterAllowed(operation))
        {
            return Denied(
                "data_job_requester_authorization_revoked");
        }

        if (!ApproverAllowed(operation))
        {
            return Denied(
                "data_job_approver_authorization_revoked");
        }

        var preconditions =
            await _preconditions.ValidateAsync(
                    operation,
                    cancellationToken)
                .ConfigureAwait(false);

        if (preconditions.Outcome !=
            MutationPreDispatchGuardOutcome.Allowed)
        {
            return preconditions;
        }

        // Re-evaluate after all current provider/topic/masking reads so a
        // revocation that races those observations cannot authorize the
        // subsequent Kafka effect.
        if (!RequesterAllowed(operation))
        {
            return Denied(
                "data_job_requester_authorization_revoked");
        }

        if (!ApproverAllowed(operation))
        {
            return Denied(
                "data_job_approver_authorization_revoked");
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private bool RequesterAllowed(
        MutationOperationSnapshot operation)
    {
        foreach (var target in
                 operation.AuthorizationTargets)
        {
            var decision =
                _authorization
                    .EvaluateCanonicalDirectSubject(
                        operation.RequesterPrincipalId,
                        new AuthorizationRequest(
                            target.Action,
                            target.ClusterId,
                            target.ResourceName));

            if (!decision.IsAllowed)
            {
                return false;
            }
        }

        return true;
    }

    private bool ApproverAllowed(
        MutationOperationSnapshot operation)
    {
        if (!operation.Risk
                .RequiresIndependentApproval)
        {
            return true;
        }

        return !string.IsNullOrWhiteSpace(
                   operation.ApprovedByPrincipalId) &&
               !string.Equals(
                   operation.ApprovedByPrincipalId,
                   operation.RequesterPrincipalId,
                   StringComparison.Ordinal) &&
               _approval
                   .IsCurrentlyEligibleDirectSubject(
                       operation.ApprovedByPrincipalId!,
                       operation);
    }

    private static bool OperationAndRangeMatch(
        MutationOperationSnapshot operation,
        GovernedDataJobPlan plan,
        int rangeIndex,
        long sourceOffset)
    {
        if (operation.OperationKind !=
                MutationOperationKind.DataJob ||
            operation.State !=
                MutationOperationState.AppliedVerified ||
            !string.Equals(
                operation.ResultCode,
                "data_job_activated",
                StringComparison.Ordinal) ||
            !string.Equals(
                operation.ClusterId,
                plan.Source.ClusterId,
                StringComparison.Ordinal) ||
            rangeIndex < 0 ||
            rangeIndex >= plan.Ranges.Count)
        {
            return false;
        }

        var range = plan.Ranges[rangeIndex];
        return sourceOffset >= range.StartInclusive &&
               sourceOffset < range.EndExclusive;
    }

    private static MutationPreDispatchGuardResult Denied(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome
                .AuthorizationDenied,
            code);

    private static MutationPreDispatchGuardResult Stale(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome.StalePreview,
            code);
}
