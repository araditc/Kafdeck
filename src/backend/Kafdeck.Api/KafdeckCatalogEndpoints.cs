using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record CatalogPreviewRequest(string ClusterId, IReadOnlyList<CatalogChange> Changes);
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record CatalogRecoverRequest(string PreviewHash);
public sealed record CatalogPreviewData(MutationStatusData Operation, Guid PlanId, string ContentHash,
    IReadOnlyList<CatalogPlannedChange> Changes, DateTimeOffset ExpiresAtUtc);
public sealed record CatalogPage<T>(IReadOnlyList<T> Items, string? NextCursor);

public static class KafdeckCatalogEndpoints
{
    public static WebApplication MapKafdeckCatalog(this WebApplication app)
    {
        foreach (var (family, kind) in new[] { ("teams", CatalogEntityKind.Team), ("applications", CatalogEntityKind.Application), ("topics", CatalogEntityKind.Topic) })
        {
            app.MapGet("/api/v1/governance/catalog/" + family, async (HttpContext context, string clusterId, int? pageSize, string? cursor,
                ICatalogStore store, ICatalogTopicIdentityPort topics, KafdeckAuthorizationService auth, IMutationMaterialDigestService digests) =>
                await Safe(async () =>
                {
                    if (!Collection(context, auth, clusterId, AuthorizationAction.CatalogRead)) return Denied(context);
                    var size = PageSize(pageSize); var after = Decode(cursor, context, auth, digests, clusterId, family);
                    var rows = await store.ListAuthorizedAsync(clusterId, kind, after, size + 1, row => Readable(context, auth, row), context.RequestAborted).ConfigureAwait(false);
                    var output = new List<CatalogRecord>();
                    foreach (var row in rows.Take(size))
                        output.Add(row with { TopicIdentityCurrent = await CurrentTopic(row, topics, context.RequestAborted).ConfigureAwait(false) });
                    if (rows.Any(row => !Readable(context, auth, row))) return Denied(context);
                    return Results.Ok(new CatalogPage<CatalogRecord>(output, rows.Count > size ? Encode(rows[size - 1].Id, context, auth, digests, clusterId, family) : null));
                })).WithName("v09-catalog-" + family + "-list").WithMetadata(new GovernanceRouteContract(AuthorizationAction.CatalogRead, true, ResponseType: typeof(CatalogPage<CatalogRecord>)));

            app.MapGet("/api/v1/governance/catalog/" + family + "/{id}", async (HttpContext context, string clusterId, string id,
                ICatalogStore store, ICatalogTopicIdentityPort topics, KafdeckAuthorizationService auth) => await Safe(async () =>
                {
                    var row = await store.GetAsync(clusterId, kind, id, context.RequestAborted).ConfigureAwait(false);
                    if (row is null || !Readable(context, auth, row)) return Results.NotFound();
                    return Results.Ok(row with { TopicIdentityCurrent = await CurrentTopic(row, topics, context.RequestAborted).ConfigureAwait(false) });
                })).WithName("v09-catalog-" + family + "-get").WithMetadata(new GovernanceRouteContract(AuthorizationAction.CatalogRead, true, QueryKeys: "clusterId", ResponseType: typeof(CatalogRecord)));

            app.MapGet("/api/v1/governance/catalog/" + family + "/{id}/history", async (HttpContext context, string clusterId, string id, int? pageSize, string? cursor,
                ICatalogStore store, KafdeckAuthorizationService auth, IMutationMaterialDigestService digests) => await Safe(async () =>
                {
                    var row = await store.GetAsync(clusterId, kind, id, context.RequestAborted).ConfigureAwait(false);
                    if (row is null || !Readable(context, auth, row)) return Results.NotFound();
                    var page = PageSize(pageSize); var scope = family + "/" + id + "/history";
                    var decoded = Decode(cursor, context, auth, digests, clusterId, scope);
                    var after = decoded is null ? 0 : long.Parse(decoded, CultureInfo.InvariantCulture);
                    var rows = await store.HistoryAuthorizedAsync(clusterId, kind, id, after, page + 1, snapshot => Readable(context, auth, snapshot), context.RequestAborted).ConfigureAwait(false);
                    if (rows.Any(snapshot => !Readable(context, auth, snapshot.Record))) return Denied(context);
                    return Results.Ok(new CatalogPage<CatalogHistoryItem>(rows.Take(page).ToArray(), rows.Count > page ? Encode(rows[page - 1].Record.Revision.ToString(CultureInfo.InvariantCulture), context, auth, digests, clusterId, scope) : null));
                })).WithName("v09-catalog-" + family + "-history").WithMetadata(new GovernanceRouteContract(AuthorizationAction.CatalogRead, true, ResponseType: typeof(CatalogPage<CatalogHistoryItem>)));

            app.MapPost("/api/v1/governance/catalog/" + family + "/preview", async (CatalogPreviewRequest request, HttpContext context,
                ICatalogStore store, ICatalogTopicIdentityPort topics, KafdeckAuthorizationService auth, MutationAdmissionService admission, KafdeckOptions options) =>
                await Preview(request, kind, false, context, store, topics, auth, admission, options)).WithName("v09-catalog-" + family + "-preview")
                .WithMetadata(new GovernanceRouteContract(AuthorizationAction.CatalogManage, false, typeof(CatalogPreviewRequest), QueryKeys: "", ResponseType: typeof(CatalogPreviewData))).RequireKafdeckAntiforgery();
        }
        app.MapPost("/api/v1/governance/catalog/imports/preview", async (CatalogPreviewRequest request, HttpContext context,
            ICatalogStore store, ICatalogTopicIdentityPort topics, KafdeckAuthorizationService auth, MutationAdmissionService admission, KafdeckOptions options) =>
            await Preview(request, null, true, context, store, topics, auth, admission, options)).WithName("v09-catalog-import-preview")
            .WithMetadata(new GovernanceRouteContract(AuthorizationAction.CatalogManage, false, typeof(CatalogPreviewRequest), 1024 * 1024, QueryKeys: "", ResponseType: typeof(CatalogPreviewData))).RequireKafdeckAntiforgery();

        app.MapGet("/api/v1/governance/catalog/operations/{operationId:guid}", async (Guid operationId, HttpContext context,
            ICatalogStore store, IMutationOperationRepository operations, KafdeckAuthorizationService auth) => await Safe(async () =>
            {
                var plan = await store.GetPlanAsync(operationId, context.RequestAborted).ConfigureAwait(false);
                if (plan is null || !plan.Changes.All(i => Readable(context, auth, plan.ClusterId, CatalogContract.Resource(i), i.Change.TeamId, i.Change.ApplicationId))) return Results.NotFound();
                var operation = await operations.GetAsync(operationId, context.RequestAborted).ConfigureAwait(false);
                if (operation is null || operation.OperationKind != MutationOperationKind.CatalogMutation) return Results.NotFound();
                return Results.Ok(Data(operation, plan));
            })).WithName("v09-catalog-operation").WithMetadata(new GovernanceRouteContract(AuthorizationAction.CatalogRead, true, QueryKeys: "", ResponseType: typeof(CatalogPreviewData)));
        app.MapPost("/api/v1/governance/catalog/operations/{operationId:guid}/recover", async (Guid operationId, CatalogRecoverRequest request,
            HttpContext context, AdoCatalogStore store, IMutationOperationRepository operations, KafdeckAuthorizationService auth, IMutationAuditSink audit) => await Safe(async () =>
            {
                var plan = await store.GetPlanAsync(operationId, context.RequestAborted).ConfigureAwait(false);
                var operation = await operations.GetAsync(operationId, context.RequestAborted).ConfigureAwait(false);
                if (plan is null || operation is null || operation.OperationKind != MutationOperationKind.CatalogMutation) return Results.NotFound();
                bool Current() => MutationActor.TryCreate(context.User, out var actor) && actor?.PrincipalId == operation.RequesterPrincipalId &&
                    plan.RequesterPrincipalId == operation.RequesterPrincipalId &&
                    plan.Changes.All(i => Readable(context, auth, plan.ClusterId, CatalogContract.Resource(i), i.Change.TeamId, i.Change.ApplicationId) &&
                        Allowed(context, auth, AuthorizationAction.CatalogManage, plan.ClusterId, CatalogContract.Resource(i)) &&
                        Allowed(context, auth, AuthorizationAction.MutationReconcile, plan.ClusterId, CatalogContract.Resource(i)));
                if (!Current()) return Denied(context);
                if (request.PreviewHash != operation.PreviewHash || operation.CanonicalIntent != CatalogMutation.Intent(plan).CanonicalIntent ||
                    operation.State is not (MutationOperationState.ExecutionUnknown or MutationOperationState.AppliedVerified)) return Conflict("CatalogRecoveryPlanMismatch");
                var receipt = await store.GetReceiptAsync(operationId, context.RequestAborted).ConfigureAwait(false);
                if (receipt is null || operations is not AdoMutationOperationRepository writer) return Conflict("CatalogPositiveReceiptRequired");
                var recovered = MutationOperation.Restore(operation);
                if (operation.State == MutationOperationState.ExecutionUnknown)
                    recovered.ReconcileVerifiedCatalogCommit(receipt.OperationId, receipt.ContentHash, receipt.Records.Count, receipt.AppliedAtUtc, DateTimeOffset.UtcNow);
                async Task Guard(System.Data.Common.DbConnection db, System.Data.Common.DbTransaction tx, CancellationToken token) =>
                    await store.VerifyReceiptOnTransactionAsync(db, tx, plan, receipt, Current, token).ConfigureAwait(false);
                if (operation.State == MutationOperationState.AppliedVerified)
                {
                    // Repeat calls do not mutate the already terminal operation; still verify current authority and positive proof.
                    await using var db = await context.RequestServices.GetRequiredService<IMutationDbConnectionFactory>().OpenAsync(context.RequestAborted).ConfigureAwait(false);
                    await using var tx = await db.BeginTransactionAsync(context.RequestAborted).ConfigureAwait(false);
                    await Guard(db, tx, context.RequestAborted).ConfigureAwait(false); return Results.Ok(MutationStatusData.From(operation));
                }
                var saved = await writer.TrySaveGuardedAsync(recovered.Snapshot, operation.Version, Guard, context.RequestAborted).ConfigureAwait(false);
                if (saved.Outcome != MutationSaveOutcome.Saved) return Conflict("CatalogRecoveryVersionConflict");
                if (operation.ExecutionClaimGeneration > 0) await operations.ReleaseResourceClaimsAsync(operationId, operation.ExecutionClaimGeneration, CancellationToken.None).ConfigureAwait(false);
                await audit.WriteAsync(new MutationAuditEvent(DateTimeOffset.UtcNow, MutationAuditEventType.Completed, operationId,
                    operation.RequesterPrincipalId, operation.ClusterId, operation.OperationKind, operation.Risk.RiskClass,
                    recovered.Snapshot.State, operation.ResourceKeys, operation.PreviewHash, "catalog_commit_receipt_recovered"), context.RequestAborted).ConfigureAwait(false);
                return Results.Ok(MutationStatusData.From(recovered.Snapshot));
            })).WithName("v09-catalog-recover").WithMetadata(new GovernanceRouteContract(AuthorizationAction.MutationReconcile, false, typeof(CatalogRecoverRequest), QueryKeys: "", ResponseType: typeof(MutationStatusData))).RequireKafdeckAntiforgery();
        return app;
    }

    private static Task<IResult> Preview(CatalogPreviewRequest request, CatalogEntityKind? familyKind, bool import, HttpContext context,
        ICatalogStore store, ICatalogTopicIdentityPort topics, KafdeckAuthorizationService auth, MutationAdmissionService admission, KafdeckOptions options) => Safe(async () =>
    {
        CatalogContract.Bound(request.ClusterId, "cluster", 256, false);
        if (!options.Clusters.Any(c => c.Id == request.ClusterId) || !Collection(context, auth, request.ClusterId, AuthorizationAction.CatalogManage)) return Denied(context);
        if (!MutationActor.TryCreate(context.User, out var actor) || actor is null) return Denied(context);
        var key = context.Request.Headers["Idempotency-Key"].ToString(); _ = MutationIdempotency.HashKey(key);
        if (request.Changes is null || request.Changes.Count is < 1 or > 500 || !import && request.Changes.Count != 1) throw new ArgumentException("Invalid change count.");
        var operationId = MutationIdempotency.DeriveOperationId(actor.PrincipalId, request.ClusterId, MutationOperationKind.CatalogMutation, key);
        var changes = new List<CatalogPlannedChange>();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted); deadline.CancelAfter(TimeSpan.FromSeconds(30));
        foreach (var (raw, index) in request.Changes.Select((item, index) => (item, index)))
        {
            var change = CatalogContract.Validate(raw);
            if (familyKind is not null && change.Kind != familyKind) throw new ArgumentException("Catalog family mismatch.");
            var id = change.Id ?? new Guid(Convert.FromHexString(CatalogContract.Hash(operationId.ToString("D") + ":" + index.ToString(CultureInfo.InvariantCulture)))[..16]).ToString("N");
            var preliminary = new CatalogPlannedChange(change, id, null);
            if (!Allowed(context, auth, AuthorizationAction.CatalogManage, request.ClusterId, CatalogContract.Resource(preliminary)) ||
                !Allowed(context, auth, AuthorizationAction.CatalogRead, request.ClusterId, CatalogContract.Resource(preliminary))) return Denied(context);
            CatalogRecord? previous = null;
            if (change.Id is not null)
            {
                previous = await store.GetAsync(request.ClusterId, change.Kind, change.Id, deadline.Token).ConfigureAwait(false);
                if (previous is null || !Readable(context, auth, previous) ||
                    !Allowed(context, auth, AuthorizationAction.CatalogManage, request.ClusterId, CatalogContract.Resource(previous))) return Results.NotFound();
                if (previous.TopicName != change.TopicName) return Conflict("CatalogTopicIdentityChanged");
            }
            foreach (var (refKind, refId) in new[] { (CatalogEntityKind.Team, change.TeamId), (CatalogEntityKind.Application, change.ApplicationId) })
                if (refId is not null && !Allowed(context, auth, AuthorizationAction.CatalogRead, request.ClusterId, "catalog:" + refKind.ToString().ToLowerInvariant() + ":" + refId)) return Denied(context);
            string? identityFingerprint = null;
            if (change.Kind == CatalogEntityKind.Topic && change.Archived)
            {
                // Archiving an existing descriptive assignment must remain possible after its physical topic disappeared.
                // It preserves the old identity/revision and cannot create or transfer current ownership.
                if (change.Id is null || change.RebindTopicIdentity) return Problem(400, "CatalogArchiveRequiresExistingAssignment");
                if (previous is null || previous.TopicName != change.TopicName) return Conflict("CatalogArchiveAssignmentChanged");
                identityFingerprint = previous.TopicIdentity;
            }
            else if (change.Kind == CatalogEntityKind.Topic)
            {
                identityFingerprint = (await topics.ObserveAsync(request.ClusterId, change.TopicName!, deadline.Token).ConfigureAwait(false))?.Fingerprint;
                if (identityFingerprint is null) return Problem(503, "CatalogTopicIdentityUnavailable");
            }
            changes.Add(preliminary with { TopicIdentity = identityFingerprint });
        }
        var now = DateTimeOffset.UtcNow;
        var plan = new CatalogPlan(operationId, request.ClusterId, actor.PrincipalId, auth.PolicyFingerprint, store.DeploymentGeneration,
            CatalogContract.MaterialHash(request.ClusterId, changes), changes, now, now.AddMinutes(5));
        plan = await store.StageAsync(plan, context.RequestAborted).ConfigureAwait(false);
        var result = await admission.AdmitAsync(context.User, operationId, CatalogMutation.Intent(plan), CatalogMutation.RiskFor(plan), key, context.RequestAborted).ConfigureAwait(false);
        return result.Outcome switch
        {
            MutationAdmissionOutcome.Created when result.Operation is not null => Results.Created("/api/v1/mutations/" + operationId.ToString("D"), Data(result.Operation, plan)),
            MutationAdmissionOutcome.ExistingSameIntent when result.Operation is not null => Results.Ok(Data(result.Operation, plan)),
            MutationAdmissionOutcome.IdempotencyConflict => Conflict("IdempotencyConflict"),
            MutationAdmissionOutcome.Unavailable => Problem(503, "GovernanceUnavailable"),
            MutationAdmissionOutcome.Unauthenticated or MutationAdmissionOutcome.Forbidden => Denied(context),
            _ => Problem(400, result.Code ?? "CatalogAdmissionInvalid"),
        };
    });
    private static CatalogPreviewData Data(MutationOperationSnapshot operation, CatalogPlan plan) => new(MutationStatusData.From(operation), plan.PlanId, plan.ContentHash, plan.Changes, plan.ExpiresAtUtc);
    private static bool Allowed(HttpContext context, KafdeckAuthorizationService auth, AuthorizationAction action, string cluster, string resource) =>
        auth.Authorize(context.User, new(action, cluster, resource)) == KafdeckAuthorizationOutcome.Allowed;
    private static bool Readable(HttpContext context, KafdeckAuthorizationService auth, CatalogRecord row) => Readable(context, auth, row.ClusterId, CatalogContract.Resource(row), row.TeamId, row.ApplicationId);
    private static bool Readable(HttpContext context, KafdeckAuthorizationService auth, string cluster, string resource, string? teamId, string? applicationId) =>
        Allowed(context, auth, AuthorizationAction.CatalogRead, cluster, resource) &&
        (teamId is null || Allowed(context, auth, AuthorizationAction.CatalogRead, cluster, "catalog:team:" + teamId)) &&
        (applicationId is null || Allowed(context, auth, AuthorizationAction.CatalogRead, cluster, "catalog:application:" + applicationId));
    private static bool Collection(HttpContext context, KafdeckAuthorizationService auth, string cluster, AuthorizationAction action) =>
        auth.AuthorizeCollection(context.User, action, cluster) == KafdeckAuthorizationOutcome.Allowed;
    private static int PageSize(int? value) => value is null ? 50 : value is >= 1 and <= 200 ? value.Value : throw new ArgumentException("Invalid page size.");
    private static async Task<bool?> CurrentTopic(CatalogRecord row, ICatalogTopicIdentityPort topics, CancellationToken token)
    {
        if (row.Kind != CatalogEntityKind.Topic || row.Archived) return null;
        var identity = await topics.ObserveAsync(row.ClusterId, row.TopicName!, token).ConfigureAwait(false);
        return identity is null ? null : identity.Fingerprint == row.TopicIdentity;
    }
    private static string CursorScope(HttpContext context, KafdeckAuthorizationService auth, string cluster, string family) =>
        MutationActor.TryCreate(context.User, out var actor) && actor is not null ? actor.PrincipalId + "\n" + auth.PolicyFingerprint + "\n" + cluster + "\n" + family : throw new ArgumentException("Invalid cursor actor.");
    private static string Encode(string value, HttpContext context, KafdeckAuthorizationService auth, IMutationMaterialDigestService digests, string cluster, string family)
    { var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(value)).TrimEnd('=').Replace('+', '-').Replace('/', '_'); return encoded + "." + digests.ComputeDigest(Encoding.UTF8.GetBytes(CursorScope(context, auth, cluster, family) + "\n" + encoded)); }
    private static string? Decode(string? cursor, HttpContext context, KafdeckAuthorizationService auth, IMutationMaterialDigestService digests, string cluster, string family)
    {
        if (cursor is null) return null;
        if (cursor.Length > 512) throw new ArgumentException("Invalid cursor.");
        var parts = cursor.Split('.'); if (parts.Length != 2) throw new ArgumentException("Invalid cursor.");
        var expected = digests.ComputeDigest(Encoding.UTF8.GetBytes(CursorScope(context, auth, cluster, family) + "\n" + parts[0]));
        if (!CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(expected), Encoding.UTF8.GetBytes(parts[1]))) throw new ArgumentException("Invalid cursor.");
        var base64 = parts[0].Replace('-', '+').Replace('_', '/'); base64 = base64.PadRight((base64.Length + 3) / 4 * 4, '=');
        return new UTF8Encoding(false, true).GetString(Convert.FromBase64String(base64));
    }
    private static async Task<IResult> Safe(Func<Task<IResult>> action)
    {
        try { return await action().ConfigureAwait(false); }
        catch (CatalogConflictException ex) { return Conflict(ex.Code); }
        catch (MutationStateException) { return Conflict("CatalogPositiveReceiptRequired"); }
        catch (Exception ex) when (ex is ArgumentException or JsonException or FormatException or OverflowException) { return Problem(400, "InvalidCatalogRequest"); }
        catch (OperationCanceledException) { return Problem(503, "CatalogUnavailable"); }
        catch (System.Data.Common.DbException) { return Problem(503, "CatalogUnavailable"); }
        catch (GovernanceGenerationException) { return Problem(503, "CatalogRestoreGenerationChanged"); }
    }
    private static IResult Conflict(string code) => Problem(409, code);
    private static IResult Denied(HttpContext context) => Problem(context.User.Identity?.IsAuthenticated == true ? 403 : 401, "ScopeDenied");
    private static IResult Problem(int status, string code) => Results.Problem(statusCode: status, title: "Catalog request failed", extensions: new Dictionary<string, object?> { ["code"] = code });
}
