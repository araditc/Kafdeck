using System.Data.Common;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

public static class KafdeckNotificationRecoveryEndpoints
{
    private static readonly AuthorizationRequest Manage = new(AuthorizationAction.NotificationManage);

    public static WebApplication MapKafdeckNotificationRecovery(this WebApplication app)
    {
        Map(app, "/api/v1/automation/notifications/recovery", "v08-bearer-notification-recovery-", true);
        Map(app, "/api/v1/notifications/recovery", "v08-notification-recovery-", false);
        return app;
    }

    private static void Map(WebApplication app, string prefix, string name, bool bearer)
    {
        app.MapGet(prefix + "/status", async (HttpContext context, KafdeckAuthorizationService auth, INotificationRecoveryStore store) =>
        {
            if (Actor(context, auth, bearer) is null) return Denied();
            return await SafeAsync(async () => Results.Ok(await store.StatusAsync(context.RequestAborted).ConfigureAwait(false))).ConfigureAwait(false);
        }).WithName(name + "status");
        app.MapGet(prefix + "/{operationId:guid}", async (Guid operationId, HttpContext context, KafdeckAuthorizationService auth, INotificationRecoveryStore store) =>
        {
            if (Actor(context, auth, bearer) is null) return Denied();
            return await SafeAsync(async () =>
            {
                var plan = await store.GetAsync(operationId, context.RequestAborted).ConfigureAwait(false);
                return plan is null ? Results.NotFound() : Results.Ok(NotificationRecoveryData.From(plan));
            }).ConfigureAwait(false);
        }).WithName(name + "detail");
        var preview = app.MapPost(prefix + "/preview", async (NotificationRecoveryPreviewRequest request, HttpContext context,
            KafdeckAuthorizationService auth, NotificationRecoveryOptions options, INotificationRecoveryStore store) =>
        {
            var actor = Actor(context, auth, bearer);
            if (actor is null) return Denied();
            return await SafeAsync(async () =>
            {
                var closed = request.ToCore();
                if (closed.TargetGeneration != options.TargetGeneration || closed.ExpectedAnchorSha256 != options.AnchorSha256) return Invalid();
                var plan = await store.PreviewAsync(closed, actor.PrincipalId, Human(actor), auth.PolicyFingerprint, context.RequestAborted).ConfigureAwait(false);
                return Results.Ok(NotificationRecoveryData.From(plan));
            }).ConfigureAwait(false);
        }).WithName(name + "preview");
        var approve = app.MapPost(prefix + "/{operationId:guid}/approve", async (Guid operationId, NotificationRecoveryRevisionRequest request,
            HttpContext context, KafdeckAuthorizationService auth, INotificationRecoveryStore store) =>
        {
            var actor = Actor(context, auth, bearer, approval: true);
            if (actor is null) return Denied();
            return await SafeAsync(async () =>
            {
                request.Validate();
                var plan = await store.ApproveAsync(operationId, request.ExpectedRevision, request.PreviewHash, actor.PrincipalId,
                    Human(actor), auth.PolicyFingerprint, context.RequestAborted).ConfigureAwait(false);
                return plan is null ? Conflict() : Results.Ok(NotificationRecoveryData.From(plan));
            }).ConfigureAwait(false);
        }).WithName(name + "approve");
        var apply = app.MapPost(prefix + "/{operationId:guid}/apply", async (Guid operationId, NotificationRecoveryRevisionRequest request,
            HttpContext context, KafdeckAuthorizationService auth, INotificationRecoveryStore store) =>
        {
            var actor = Actor(context, auth, bearer);
            if (actor is null) return Denied();
            return await SafeAsync(async () =>
            {
                request.Validate();
                var saved = await store.GetAsync(operationId, context.RequestAborted).ConfigureAwait(false);
                if (saved is null) return Results.NotFound();
                // Exact originating principal, distinct human and current policy/approval authority are all required.
                if (saved.RequesterPrincipalId != actor.PrincipalId || saved.RequesterHumanFingerprint != Human(actor) ||
                    saved.PolicyFingerprint != auth.PolicyFingerprint || Actor(context, auth, bearer) is null) return Denied();
                if (saved.State != NotificationRecoveryState.Approved) return Conflict();
                if (saved.ApproverHumanFingerprint == Human(actor) || saved.ApprovedByPrincipalId is null ||
                    !auth.IsCurrentApprovalAllowed(saved.ApprovedByPrincipalId, Manage)) return Denied();
                var plan = await store.ApplyAsync(operationId, request.ExpectedRevision, request.PreviewHash,
                    Human(actor), auth.PolicyFingerprint, () => Actor(context, auth, bearer)?.PrincipalId == saved.RequesterPrincipalId &&
                        auth.PolicyFingerprint == saved.PolicyFingerprint && auth.IsCurrentApprovalAllowed(saved.ApprovedByPrincipalId!, Manage),
                    context.RequestAborted).ConfigureAwait(false);
                return plan is null ? Conflict() : Results.Ok(NotificationRecoveryData.From(plan));
            }).ConfigureAwait(false);
        }).WithName(name + "apply");
        if (!bearer) { preview.RequireKafdeckAntiforgery(); approve.RequireKafdeckAntiforgery(); apply.RequireKafdeckAntiforgery(); }
    }

    private static MutationActor? Actor(HttpContext context, KafdeckAuthorizationService auth, bool bearer, bool approval = false)
    {
        if (bearer && context.User.Identity is not NonBrowserReadIdentity { Kind: NonBrowserPrincipalKind.Human } ||
            !bearer && context.User.Identity is NonBrowserReadIdentity ||
            !MutationActor.TryCreate(context.User, out var actor) || actor?.HumanKey is null) return null;
        var direct = SecurityAuditPrincipal.FromOperatorKey(actor.HumanKey);
        if (!auth.IsDirectSubjectAllowed(direct, Manage)) return null;
        if (approval ? !auth.IsLiveApprovalAllowed(context.User, Manage) : auth.Authorize(context.User, Manage) != KafdeckAuthorizationOutcome.Allowed) return null;
        return actor;
    }
    private static string Human(MutationActor actor) => Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(
        new { actor.HumanKey!.Issuer, actor.HumanKey.Subject }))).ToLowerInvariant();
    private static async Task<IResult> SafeAsync(Func<Task<IResult>> operation)
    {
        try { return await operation().ConfigureAwait(false); }
        catch (ArgumentException) { return Invalid(); }
        catch (Exception e) when (e is InvalidOperationException or KafdeckConfigurationException) { return Conflict(); }
        catch (DbException) { return Results.Problem(statusCode: 503, title: "Notification recovery store is unavailable."); }
    }
    private static IResult Denied() => Results.Problem(statusCode: 403, title: "Current unconstrained human notification recovery authority is required.");
    private static IResult Invalid() => Results.Problem(statusCode: 400, title: "Invalid closed notification recovery request.");
    private static IResult Conflict() => Results.Problem(statusCode: 409, title: "Notification recovery preview, authority or durable state changed.");
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record NotificationRecoveryPreviewRequest(string Command, string? Disposition, string ProofReferenceSha256,
    string FencedSendersEvidenceSha256, Guid? ExpectedDatabaseGeneration, Guid TargetGeneration, string ExpectedAnchorSha256, int MaxItems = 100)
{
    public NotificationRecoveryRequest ToCore()
    {
        if (!Enum.TryParse<NotificationRecoveryCommand>(Command, false, out var command) || command.ToString() != Command ||
            Disposition is not null && (!Enum.TryParse<NotificationRecoveryDisposition>(Disposition, false, out var disposition) || disposition.ToString() != Disposition))
            throw new ArgumentException("Invalid closed notification recovery command.");
        var request = new NotificationRecoveryRequest(command, Disposition is null ? null : Enum.Parse<NotificationRecoveryDisposition>(Disposition),
            ProofReferenceSha256, FencedSendersEvidenceSha256, ExpectedDatabaseGeneration, TargetGeneration, ExpectedAnchorSha256, MaxItems);
        request.Validate(); return request;
    }
}
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record NotificationRecoveryRevisionRequest(long ExpectedRevision, string PreviewHash)
{
    public void Validate()
    { if (ExpectedRevision < 1 || !NotificationRecoveryRequest.Fingerprint(PreviewHash)) throw new ArgumentException("Invalid recovery revision or preview hash."); }
}
public sealed record NotificationRecoveryData(Guid OperationId, long Revision, string PreviewHash, string State, string Command,
    Guid? OldGeneration, Guid TargetGeneration, int ItemCount, DateTimeOffset ExpiresAtUtc, string RequesterPrincipalId,
    string? ApprovedByPrincipalId, string? Disposition, string ProofReferenceSha256, string FencedSendersEvidenceSha256,
    DateTimeOffset HistoricalCutoffUtc, long RemainingUnreconciled, bool ReadyForOperatorActivation,
    string SourceIdentityFingerprint, string SourceInventoryHash, int SourceItemCount,
    IReadOnlyList<NotificationRecoveryItem> Items, IReadOnlyList<Guid> SourceEventIds)
{
    public static NotificationRecoveryData From(NotificationRecoveryPlan plan) => new(plan.OperationId, plan.Revision, plan.PreviewHash,
        plan.State.ToString(), plan.Request.Command.ToString(), plan.Request.ExpectedDatabaseGeneration, plan.Request.TargetGeneration,
        plan.Items.Count, plan.ExpiresAtUtc, plan.RequesterPrincipalId, plan.ApprovedByPrincipalId, plan.Request.Disposition?.ToString(),
        plan.Request.ProofReferenceSha256, plan.Request.FencedSendersEvidenceSha256, plan.HistoricalCutoffUtc, plan.RemainingUnreconciled,
        plan.ReadyForOperatorActivation, plan.SourceIdentityFingerprint, plan.SourceInventoryHash, plan.SourceEventIds.Count,
        plan.Items, plan.SourceEventIds);
}
