using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public static class KafdeckMutationApprovalEndpoints
{
    private const int DefaultLimit = 50;
    private const int MaxLimit = 100;

    public static WebApplication MapKafdeckMutationApprovalEndpoints(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapGet("/api/v1/mutations/approvals", async (
                int? limit,
                HttpContext context,
                IMutationOperationRepository repository,
                MutationRequestAuthorizationService authorization,
                CancellationToken cancellationToken) =>
            {
                var requestedLimit = limit ?? DefaultLimit;
                if (requestedLimit is < 1 or > MaxLimit)
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        type: "urn:kafdeck:problem:mutation-approval-limit-invalid",
                        title: "Approval inbox limit is invalid",
                        detail: $"The approval inbox limit must be between 1 and {MaxLimit}.");
                }

                if (!OperatorSessionContextFactory.TryCreate(
                        context.User,
                        out var session) ||
                    session is null)
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status401Unauthorized,
                        type: "urn:kafdeck:problem:operator-authentication-required",
                        title: "Operator authentication required");
                }

                var currentPrincipalId =
                    SecurityAuditPrincipal.FromOperator(session.Identity);
                var candidates = await repository
                    .ListByStateAsync(
                        MutationOperationState.AwaitingApproval,
                        MaxLimit,
                        cancellationToken)
                    .ConfigureAwait(false);

                var visible = new List<MutationStatusData>(requestedLimit);
                foreach (var operation in candidates)
                {
                    if (visible.Count >= requestedLimit)
                    {
                        break;
                    }

                    // Independent approval is never offered back to the requester.
                    if (string.Equals(
                            currentPrincipalId,
                            operation.RequesterPrincipalId,
                            StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (authorization.AuthorizeForDispatch(
                            context.User,
                            operation) != KafdeckAuthorizationOutcome.Allowed)
                    {
                        continue;
                    }

                    visible.Add(MutationStatusData.From(operation));
                }

                return Results.Ok(new MutationApprovalInboxData(visible));
            })
            .WithName("v05-mutation-approval-inbox");

        return app;
    }
}

public sealed record MutationApprovalInboxData(
    IReadOnlyList<MutationStatusData> Items);
