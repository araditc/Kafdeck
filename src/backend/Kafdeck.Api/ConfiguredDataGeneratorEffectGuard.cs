using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Generator;

namespace Kafdeck.Api;

public sealed class ConfiguredDataGeneratorEffectGuard :
    IDataGeneratorEffectGuard
{
    private readonly AuthorizationPolicyEvaluator _authorization;
    private readonly MutationApprovalAuthorizer _approval;
    private readonly DataGeneratorPreconditionValidator _preconditions;

    public ConfiguredDataGeneratorEffectGuard(
        AuthorizationPolicyEvaluator authorization,
        MutationApprovalAuthorizer approval,
        DataGeneratorPreconditionValidator preconditions)
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
        DataGeneratorPlan plan,
        long recordIndex,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(plan);

        if (!OperationMatches(
                operation,
                plan,
                recordIndex))
        {
            return Stale(
                "data_generator_effect_binding_invalid");
        }

        if (!RequesterAllowed(operation))
        {
            return Denied(
                "data_generator_requester_authorization_revoked");
        }

        if (!ApproverAllowed(operation))
        {
            return Denied(
                "data_generator_approver_authorization_revoked");
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

        if (!RequesterAllowed(operation))
        {
            return Denied(
                "data_generator_requester_authorization_revoked");
        }

        if (!ApproverAllowed(operation))
        {
            return Denied(
                "data_generator_approver_authorization_revoked");
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

    private static bool OperationMatches(
        MutationOperationSnapshot operation,
        DataGeneratorPlan plan,
        long recordIndex) =>
        operation.OperationKind ==
            MutationOperationKind.DataGenerator &&
        operation.State ==
            MutationOperationState.AppliedVerified &&
        string.Equals(
            operation.ResultCode,
            "data_generator_activated",
            StringComparison.Ordinal) &&
        string.Equals(
            operation.ClusterId,
            plan.Destination.ClusterId,
            StringComparison.Ordinal) &&
        recordIndex >= 0 &&
        recordIndex < plan.RecordCount;

    private static MutationPreDispatchGuardResult Denied(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome
                .AuthorizationDenied,
            code);

    private static MutationPreDispatchGuardResult Stale(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome
                .StalePreview,
            code);
}
