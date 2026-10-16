using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public static class KafdeckMutationApprovalEndpoints
{
    private const int DefaultLimit = 50;
    private const int MaxLimit = 100;
    private const int InitialScanLimit = 100;
    private const int MaxScanLimit = 10_001;

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

                var nowUtc = DateTimeOffset.UtcNow;
                var visible = new List<MutationStatusData>(requestedLimit);
                var scanLimit = InitialScanLimit;

                while (visible.Count < requestedLimit)
                {
                    var candidates = await repository
                        .ListByStateAsync(
                            MutationOperationState.AwaitingApproval,
                            scanLimit,
                            cancellationToken)
                        .ConfigureAwait(false);

                    visible.Clear();
                    foreach (var operation in candidates)
                    {
                        if (visible.Count >= requestedLimit)
                        {
                            break;
                        }

                        // Expired previews are not actionable approval candidates even
                        // if persistence has not yet reconciled their terminal state.
                        if (!IsIndependentCandidate(operation, session.Identity.Key, nowUtc))
                        {
                            continue;
                        }

                        if (operation.PolicyVersion.StartsWith("v0.8-bearer:", StringComparison.Ordinal)
                                ? !authorization.IsApprovalAllowed(context.User, operation)
                                : authorization.AuthorizeForDispatch(context.User, operation) != KafdeckAuthorizationOutcome.Allowed)
                        {
                            continue;
                        }

                        visible.Add(MutationStatusData.From(operation));
                    }

                    if (visible.Count >= requestedLimit ||
                        candidates.Count < scanLimit ||
                        scanLimit >= MaxScanLimit)
                    {
                        break;
                    }

                    scanLimit = Math.Min(
                        checked(scanLimit * 2),
                        MaxScanLimit);
                }

                return Results.Ok(new MutationApprovalInboxData(visible));
            })
            .WithName("v05-mutation-approval-inbox");

        return app;
    }

    internal static bool IsIndependentCandidate(MutationOperationSnapshot operation, OperatorIdentityKey human, DateTimeOffset nowUtc) =>
        operation.PreviewExpiresAtUtc > nowUtc && !MutationActor.IsSameHuman(operation.RequesterPrincipalId, human);
}

public sealed record MutationApprovalInboxData(
    IReadOnlyList<MutationStatusData> Items);
