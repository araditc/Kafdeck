using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;

namespace Kafdeck.Api;

public sealed class W57GovernedDataJobEffectGuard :
    IGovernedDataJobEffectGuard
{
    private readonly MutationExecutionRequestContextAccessor _requestContext;
    private readonly MutationRequestAuthorizationService _authorization;
    private readonly GovernedDataJobPreconditionValidator _validator;

    public W57GovernedDataJobEffectGuard(
        MutationExecutionRequestContextAccessor requestContext,
        MutationRequestAuthorizationService authorization,
        GovernedDataJobPreconditionValidator validator)
    {
        _requestContext = requestContext ??
            throw new ArgumentNullException(nameof(requestContext));
        _authorization = authorization ??
            throw new ArgumentNullException(nameof(authorization));
        _validator = validator ??
            throw new ArgumentNullException(nameof(validator));
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
        cancellationToken.ThrowIfCancellationRequested();

        var requester =
            MutationCurrentRequesterAuthorization.Evaluate(
                _requestContext,
                _authorization,
                operation);

        if (requester.Outcome !=
            MutationPreDispatchGuardOutcome.Allowed)
        {
            return requester;
        }

        if (rangeIndex < 0 ||
            rangeIndex >= plan.Ranges.Count)
        {
            return new(
                MutationPreDispatchGuardOutcome.StalePreview,
                "data_job_range_index_invalid");
        }

        var range = plan.Ranges[rangeIndex];
        if (sourceOffset < range.StartInclusive ||
            sourceOffset >= range.EndExclusive)
        {
            return new(
                MutationPreDispatchGuardOutcome.StalePreview,
                "data_job_source_offset_outside_plan");
        }

        var preconditions =
            await _validator.ValidateAsync(
                    operation,
                    cancellationToken)
                .ConfigureAwait(false);

        if (preconditions.Outcome !=
            MutationPreDispatchGuardOutcome.Allowed)
        {
            return preconditions;
        }

        return MutationCurrentRequesterAuthorization.Evaluate(
            _requestContext,
            _authorization,
            operation);
    }
}
