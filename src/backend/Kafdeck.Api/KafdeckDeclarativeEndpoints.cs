using System.Text;
using System.Text.Json.Serialization;
using System.Data.Common;
using System.Security.Cryptography;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record DeclarativeManifestRequest(string Manifest, string Format, string SourceRef,
    string SourceRevision, long ExpectedOwnershipRevision);
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record DeclarativeApplyRequest(string PlanFingerprint);

public static class KafdeckDeclarativeEndpoints
{
    public static WebApplication MapKafdeckDeclarative(this WebApplication app, bool enabled)
    {
        app.MapGet("/api/v1/declarative/capabilities", Capabilities)
            .WithName("v08-declarative-capabilities")
            .WithMetadata(new DeclarativeRouteContract(AuthorizationAction.DeclarativeRead));
        IResult Capabilities(HttpContext context, KafdeckAuthorizationService authorization, IConfiguration configuration)
        {
            if (!MutationActor.TryCreate(context.User, out _)) return Results.Problem(statusCode: 401, title: "Authentication required");
            if (authorization.AuthorizeCollection(context.User, AuthorizationAction.DeclarativeRead,
                configuration["Kafdeck:Declarative:DeploymentId"]) != KafdeckAuthorizationOutcome.Allowed)
                return Results.Problem(statusCode: 403, title: "Declarative capability access denied");
            return Results.Ok(new
        {
            enabled, apiVersion = DeclarativeSubscriptionManifest.ApiVersion,
            families = new object[]
            {
                new { kind = "NotificationSubscription", validate = true, observe = true, drift = true,
                    import = true, plan = true, create = true, apply = true, delete = false, relinquish = false },
                new { kind = "DataQualityPolicy", ownership = "Unadmitted", apply = false },
                new { kind = "KafkaTopic", ownership = "ObservedOnly", apply = false },
                new { kind = "Acl", ownership = "Unadmitted", apply = false },
                new { kind = "ConnectProfile", ownership = "Unadmitted", apply = false }
            }
            });
        }
        if (!enabled) return app;
        app.MapKafdeckDeclarativeSources();
        foreach (var verb in new[] { "validate", "observe", "drift", "import-preview", "plan" })
        {
            var command = verb;
            var action = command == "import-preview" ? AuthorizationAction.DeclarativeAdopt
                : command == "plan" ? AuthorizationAction.DeclarativePlan : AuthorizationAction.DeclarativeRead;
            app.MapPost("/api/v1/declarative/" + command,
                (DeclarativeManifestRequest request, HttpContext context, DeclarativeEndpointService service,
                    CancellationToken token) => service.HandleAsync(command, request, context, token))
                .WithName("v08-declarative-" + command)
                .WithMetadata(new DeclarativeRouteContract(action, typeof(DeclarativeManifestRequest)))
                .RequireKafdeckAntiforgery();
        }
        app.MapGet("/api/v1/declarative/plans/{operationId:guid}",
            (Guid operationId, HttpContext context, DeclarativeEndpointService service, CancellationToken token)
                => service.ReadPlanAsync(operationId, context, token))
            .WithName("v08-declarative-plan-detail")
            .WithMetadata(new DeclarativeRouteContract(AuthorizationAction.DeclarativeRead));
        app.MapPost("/api/v1/declarative/plans/{operationId:guid}/apply",
            (Guid operationId, DeclarativeApplyRequest request, HttpContext context,
                DeclarativeEndpointService service, CancellationToken token) => service.ApplyAsync(operationId, request, context, token))
            .WithName("v08-declarative-apply")
            .WithMetadata(new DeclarativeRouteContract(AuthorizationAction.DeclarativeApply, typeof(DeclarativeApplyRequest)))
            .RequireKafdeckAntiforgery();
        app.MapPost("/api/v1/declarative/plans/{operationId:guid}/recover",
            (Guid operationId, DeclarativeApplyRequest request, HttpContext context,
                DeclarativeEndpointService service, CancellationToken token) => service.RecoverAsync(operationId, request, context, token))
            .WithName("v08-declarative-recover")
            .WithMetadata(new DeclarativeRouteContract(AuthorizationAction.DeclarativeApply, typeof(DeclarativeApplyRequest)))
            .RequireKafdeckAntiforgery();
        return app;
    }
}

public sealed class DeclarativeEndpointService(IW68RuntimeStore store, INotificationRoutingStore routing,
    DeclarativeRuntimeAuthorizer authorization, MutationAdmissionService admission,
    IMutationOperationRepository operations, MutationDispatchService dispatch,
    KafdeckAuthorizationService policy, IMutationAuditSink audit, DeclarativeLifecycleGuard lifecycle,
    MutationRequestAuthorizationService requestAuthorization)
{
    public async Task<IResult> HandleAsync(string verb, DeclarativeManifestRequest request,
        HttpContext context, CancellationToken token)
    {
        try
        {
            if (!MutationActor.TryCreate(context.User, out var actor) || actor is null) return Problem(401, "declarative_authentication_required");
            if (request.Manifest is null || request.Manifest.Length > DeclarativeSubscriptionManifest.MaxInputBytes ||
                request.ExpectedOwnershipRevision < 0) return Problem(400, "declarative_request_invalid");
            var desired = DeclarativeSubscriptionManifest.Parse(Encoding.UTF8.GetBytes(request.Manifest), request.Format);
            if (request.SourceRevision != desired.Fingerprint()) return Problem(403, "declarative_source_content_mismatch");
            var definition = desired.Definition;
            var registration = authorization.Registration(context.User, desired.DeploymentRef, desired.OwnerRef,
                request.SourceRef, request.SourceRevision, definition.SubscriptionId);
            if (registration is null || !Authorized(DeclarativeRuntimeVerb.Read, [definition.DestinationId]))
                return Problem(403, "declarative_source_or_read_denied");
            var registeredSource = await store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source,
                desired.DeploymentRef, request.SourceRef, token).ConfigureAwait(false);
            if (registration.AllowRevisionAdvance)
            {
                if (registeredSource is not null ? registeredSource.PinnedRevision != request.SourceRevision : registration.SourceRevision != request.SourceRevision)
                    return Problem(409, "declarative_source_revision_advance_required");
            }
            var current = await routing.GetSubscriptionAsync(definition.SubscriptionId, token).ConfigureAwait(false);
            var destinations = current is null ? new[] { definition.DestinationId }
                : new[] { definition.DestinationId, current.Definition.DestinationId };
            if (!Authorized(DeclarativeRuntimeVerb.Read, destinations)) return Problem(403, "declarative_current_read_denied");
            var key = new W68RuntimeResourceKey(desired.DeploymentRef, "Deployment", desired.DeploymentRef,
                "NotificationSubscription", definition.SubscriptionId);
            var ownership = await store.GetOwnershipAsync(key, token).ConfigureAwait(false);
            var conflict = current?.State == NotificationSubscriptionState.Retired || ownership is not null &&
                (ownership.OwnerId != desired.OwnerRef || ownership.SourceId != request.SourceRef || ownership.State != "Managed");
            var drift = conflict ? "Conflict" : current is null ? "Missing"
                : SameDesired(desired, current) ? "InSync" : "OutOfSync";
            if (verb is "validate" or "observe" or "drift") return Results.Ok(new
            {
                valid = true, drift, desiredFingerprint = desired.Fingerprint(),
                observedAtUtc = await store.ReadDatabaseUtcNowAsync(token).ConfigureAwait(false),
                capability = "Supported", authorization = "Allowed",
                subscriptionRevision = current?.Revision, ownershipRevision = ownership?.Revision ?? 0,
                ownership = ownership is null ? "ObservedOnly" : ownership.State,
                sourceInSync = ownership is not null && registeredSource is { Enabled: true } &&
                    registeredSource.OwnerId == desired.OwnerRef && registeredSource.PinnedRevision == request.SourceRevision &&
                    ownership.SourceRevisionHash == request.SourceRevision && ownership.SourceGeneration == registeredSource.GenerationId,
                observedFingerprint = current is null ? "missing" : W68RuntimeFingerprints.Subscription(current)
            });
            var command = verb == "import-preview" ? W68RuntimePlanCommand.Adopt : W68RuntimePlanCommand.Apply;
            var required = command == W68RuntimePlanCommand.Adopt ? DeclarativeRuntimeVerb.Adopt : DeclarativeRuntimeVerb.Plan;
            if (!Authorized(required, destinations)) return Problem(403, "declarative_plan_denied");
            if (conflict || (ownership?.Revision ?? 0) != request.ExpectedOwnershipRevision)
                return Problem(409, "declarative_ownership_conflict");
            if (command == W68RuntimePlanCommand.Adopt && (current is null || ownership is not null))
                return Problem(409, "declarative_adoption_requires_observed_unowned_resource");
            if (command == W68RuntimePlanCommand.Apply && current is not null && ownership is null)
                return Problem(409, "declarative_explicit_import_required");
            var idempotency = context.Request.Headers["Idempotency-Key"].ToString();
            _ = MutationIdempotency.HashKey(idempotency);
            var id = MutationIdempotency.DeriveOperationId(actor.PrincipalId, desired.DeploymentRef,
                MutationOperationKind.DeclarativeNotificationSubscription, idempotency);
            var existing = await store.GetPlanAsync(id, token).ConfigureAwait(false);
            if (existing is not null)
            {
                if (existing.Command != command || existing.DesiredFingerprint != desired.Fingerprint() ||
                    existing.SourceRevisionHash != request.SourceRevision || existing.ExpectedOwnershipRevision != request.ExpectedOwnershipRevision ||
                    existing.RequesterPrincipalId != actor.PrincipalId || existing.PolicyFingerprint != authorization.PolicyFingerprint)
                    return Problem(409, "declarative_idempotency_conflict");
                return await AdmitAsync(existing).ConfigureAwait(false);
            }
            async Task<bool> Authority(CancellationToken _) => await Task.FromResult(Authorized(required, destinations));
            await RegisterAsync(W68RuntimeRegistrationKind.Owner, desired.OwnerRef, "", "").ConfigureAwait(false);
            var source = await RegisterAsync(W68RuntimeRegistrationKind.Source, request.SourceRef,
                request.SourceRef, request.SourceRevision).ConfigureAwait(false);
            await RegisterAsync(W68RuntimeRegistrationKind.Controller, registration.Id, "", "").ConfigureAwait(false);
            if (source is null) return Problem(409, "declarative_source_registration_conflict");
            var scope = "subscription-" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(definition.SubscriptionId))).ToLowerInvariant();
            var lease = await store.AcquireLeaseAsync(desired.DeploymentRef, scope, registration.Id,
                desired.OwnerRef, actor.PrincipalId, id.ToString("N"), 120, Authority, token).ConfigureAwait(false);
            if (lease is null) return Problem(409, "declarative_resource_busy");
            var now = await store.ReadDatabaseUtcNowAsync(token).ConfigureAwait(false);
            var plan = new W68StoredRuntimePlan(id, command, key, desired.OwnerRef, request.SourceRef, source.GenerationId,
                request.SourceRevision, actor.PrincipalId, ownership?.Revision ?? 0, ownership?.GenerationId,
                current?.Revision, current is null ? "missing" : W68RuntimeFingerprints.Subscription(current),
                current?.Definition.DestinationId, definition, desired.State, Encoding.UTF8.GetString(desired.ToCanonicalUtf8()),
                desired.Fingerprint(), authorization.PolicyFingerprint, registration.Id, scope, lease.HolderId, lease.Epoch,
                now.AddSeconds(110), "", ownership?.SourceRevisionHash,
                context.RequestServices.GetRequiredService<IConfiguration>().GetValue<Guid>("Kafdeck:Declarative:DeploymentGeneration"));
            plan = plan with { SourceRegistrationRevision = source.Revision, ObservedSubscription = current };
            plan = plan with { PlanFingerprint = W68RuntimeFingerprints.Plan(plan) };
            var saved = await store.SavePlanAsync(plan, (_, ct) => Authority(ct), token).ConfigureAwait(false);
            return saved is null ? Problem(409, "declarative_plan_conflict") : await AdmitAsync(saved).ConfigureAwait(false);

            bool Authorized(DeclarativeRuntimeVerb action, IEnumerable<string> destinationsToCheck) => authorization.Authorize(
                context.User, desired.DeploymentRef, desired.OwnerRef, request.SourceRef, request.SourceRevision,
                definition.SubscriptionId, destinationsToCheck, action);
            async Task<W68RuntimeRegistration?> RegisterAsync(W68RuntimeRegistrationKind kind, string registrationId,
                string repository, string revision)
            {
                var old = await store.GetRegistrationAsync(kind, desired.DeploymentRef, registrationId, token).ConfigureAwait(false);
                // Provisioning from configuration cannot undo a durable revoke.
                if (old is { Enabled: false }) return null;
                var row = new W68RuntimeRegistration(kind, desired.DeploymentRef, registrationId, old?.GenerationId ?? Guid.NewGuid(),
                    old?.Revision ?? 0, desired.OwnerRef, kind == W68RuntimeRegistrationKind.Controller ? actor.PrincipalId : "", repository,
                    repository.Length == 0 ? "" : definition.SubscriptionId, revision, true);
                if (row == old) return await Authority(token).ConfigureAwait(false) ? old : null;
                return await store.UpsertRegistrationAsync(row, old?.Revision ?? 0, Authority, token).ConfigureAwait(false);
            }
            async Task<IResult> AdmitAsync(W68StoredRuntimePlan saved)
            {
                var result = await admission.AdmitAsync(context.User, saved.PlanId, DeclarativeSubscriptionMutation.Intent(saved),
                    MutationRiskClassifier.Classify(new(MutationOperationKind.DeclarativeNotificationSubscription)),
                    idempotency, token).ConfigureAwait(false);
                return result.Operation is not null && result.Outcome is MutationAdmissionOutcome.Created or MutationAdmissionOutcome.ExistingSameIntent
                    ? Results.Ok(MutationStatusData.From(result.Operation))
                    : Problem(result.Outcome == MutationAdmissionOutcome.Forbidden ? 403 : 409, "declarative_admission_denied");
            }
        }
        catch (Exception exception) when (exception is ArgumentException or MutationStateException or InvalidOperationException)
        { return Problem(400, "declarative_request_invalid"); }
        catch (Exception exception) when (exception is DbException or TimeoutException)
        {
            return verb is "observe" or "drift" ? Results.Ok(new { drift = "Unknown", code = "declarative_observation_unavailable" })
                : Problem(503, "declarative_store_unavailable");
        }
    }

    public async Task<IResult> ReadPlanAsync(Guid id, HttpContext context, CancellationToken token)
    {
        var plan = await store.GetPlanAsync(id, token).ConfigureAwait(false);
        if (plan is null) return Problem(404, "declarative_plan_not_found");
        var operation = await operations.GetAsync(id, token).ConfigureAwait(false);
        if (operation is null) return Problem(404, "declarative_plan_not_found");
        var approver = operation.State == MutationOperationState.AwaitingApproval && operation.Risk.RequiresIndependentApproval &&
            MutationActor.TryCreate(context.User, out var reviewer) && reviewer?.HumanKey is { } reviewerHuman &&
            !MutationActor.IsSameHuman(operation.RequesterPrincipalId, reviewerHuman) &&
            requestAuthorization.IsApprovalAllowed(context.User, operation);
        if (!CanRead(plan, context) && !(approver && await lifecycle.IsCurrentAsync(operation, token).ConfigureAwait(false)))
            return Problem(403, "declarative_plan_read_denied");
        var desiredProjection = new { destinationRef = plan.DesiredSubscription.DestinationId, state = plan.DesiredState.ToString(),
            eventClasses = plan.DesiredSubscription.EventClasses.Select(value => value.ToString()).ToArray(), plan.DesiredSubscription.EventTypes };
        return Results.Ok(new
        {
            planId = plan.PlanId, planFingerprint = operation.PreviewHash, storedProofFingerprint = plan.PlanFingerprint,
            deploymentId = plan.Key.DeploymentId, resourceKind = plan.Key.ResourceKind, resourceId = plan.Key.ResourceId,
            plan.DesiredFingerprint, plan.ExpectedOwnershipRevision, plan.ExpectedSubscriptionRevision,
            plan.ExpiresAtUtc, plan.OwnerId, plan.SourceId, plan.SourceRevisionHash, plan.SourceRegistrationRevision,
            plan.PolicyFingerprint, plan.ObservedSubscriptionFingerprint, plan.DeploymentGeneration,
            before = Projection(plan.ObservedSubscription),
            desired = desiredProjection,
            after = plan.Command == W68RuntimePlanCommand.Adopt ? Projection(plan.ObservedSubscription) : desiredProjection,
            changes = plan.Command == W68RuntimePlanCommand.Adopt ? new[] { "AcquireOwnership" }
                : plan.ExpectedSubscriptionRevision is null ? new[] { "CreateSubscription", "AcquireOwnership" } : new[] { "UpdateSubscription" },
            risk = operation.Risk.RiskClass.ToString(), operation.Risk.RequiresIndependentApproval,
            state = operation.State.ToString(), command = plan.Command.ToString()
        });
    }

    public async Task<IResult> ApplyAsync(Guid id, DeclarativeApplyRequest request, HttpContext context, CancellationToken token)
    {
        var operation = await operations.GetAsync(id, token).ConfigureAwait(false);
        if (operation is null) return Problem(404, "declarative_plan_not_found");
        if (!MutationActor.TryCreate(context.User, out var actor) || actor?.PrincipalId != operation.RequesterPrincipalId)
            return Problem(403, "declarative_requester_denied");
        if (operation.OperationKind != MutationOperationKind.DeclarativeNotificationSubscription || operation.PreviewHash != request.PlanFingerprint)
            return Problem(409, "declarative_plan_fingerprint_mismatch");
        if (operation.State == MutationOperationState.AppliedVerified)
            return await RecoverAsync(id, request, context, token).ConfigureAwait(false);
        return KafdeckMutationEndpoints.MapDispatchResult(await dispatch.ExecuteTopicAsync(context.User, id, token).ConfigureAwait(false));
    }

    public async Task<IResult> RecoverAsync(Guid id, DeclarativeApplyRequest request, HttpContext context, CancellationToken token)
    {
        var plan = await store.GetPlanAsync(id, token).ConfigureAwait(false);
        var saved = await operations.GetAsync(id, token).ConfigureAwait(false);
        if (plan is null || saved is null) return Problem(404, "declarative_plan_not_found");
        if (!RecoveryAllowed()) return Problem(403, "declarative_recovery_denied");
        if (saved.PreviewHash != request.PlanFingerprint || saved.OperationKind != MutationOperationKind.DeclarativeNotificationSubscription ||
            saved.CanonicalIntent != DeclarativeSubscriptionMutation.Intent(plan).CanonicalIntent)
            return Problem(409, "declarative_plan_fingerprint_mismatch");
        var receipt = await store.GetVerifiedCommitReceiptAsync(id, plan.PlanFingerprint, saved.RequesterPrincipalId,
            (_, _) => Task.FromResult(RecoveryAllowed()), token).ConfigureAwait(false);
        if (saved.State is not (MutationOperationState.ExecutionUnknown or MutationOperationState.AppliedVerified) || receipt is null || receipt.OperationId != id ||
            receipt.PlanFingerprint != plan.PlanFingerprint || receipt.CommittedAtUtc > plan.ExpiresAtUtc)
            return Problem(409, "declarative_unknown_requires_positive_commit_receipt");
        if (saved.State == MutationOperationState.AppliedVerified) return Results.Ok(MutationStatusData.From(saved));
        var recovered = MutationOperation.Restore(saved);
        recovered.ReconcileVerifiedDeclarativeCommit(receipt.OperationId, receipt.PlanFingerprint, receipt.OwnershipRevision,
            receipt.SubscriptionRevision, receipt.CommittedAtUtc, DateTimeOffset.UtcNow);
        if (!RecoveryAllowed() || await store.GetVerifiedCommitReceiptAsync(id, plan.PlanFingerprint, saved.RequesterPrincipalId,
            (_, _) => Task.FromResult(RecoveryAllowed()), token).ConfigureAwait(false) != receipt)
            return Problem(403, "declarative_recovery_denied");
        var result = await operations.TrySaveAsync(recovered.Snapshot, saved.Version, token).ConfigureAwait(false);
        if (result.Outcome != MutationSaveOutcome.Saved) return Problem(409, "declarative_recovery_version_conflict");
        // Terminal positive proof releases the original operation's scheduling
        // claims; it does not apply the subscription a second time.
        if (saved.ExecutionClaimGeneration > 0)
            await operations.ReleaseResourceClaimsAsync(id, saved.ExecutionClaimGeneration, CancellationToken.None).ConfigureAwait(false);
        await audit.WriteAsync(new MutationAuditEvent(DateTimeOffset.UtcNow, MutationAuditEventType.Completed,
            id, saved.RequesterPrincipalId, saved.ClusterId, saved.OperationKind, saved.Risk.RiskClass,
            recovered.Snapshot.State, saved.ResourceKeys, saved.PreviewHash, "declarative_commit_receipt_recovered"), token).ConfigureAwait(false);
        return Results.Ok(MutationStatusData.From(recovered.Snapshot));

        bool RecoveryAllowed() => CanRead(plan, context) && plan.PolicyFingerprint == authorization.PolicyFingerprint &&
            authorization.Authorize(context.User, plan.Key.DeploymentId, plan.OwnerId, plan.SourceId, plan.SourceRevisionHash,
                plan.Key.ResourceId, DeclarativeSubscriptionMutation.Destinations(plan),
                plan.Command == W68RuntimePlanCommand.Adopt ? DeclarativeRuntimeVerb.Adopt : DeclarativeRuntimeVerb.Apply) &&
            saved.ApprovedByPrincipalId is not null && saved.AuthorizationTargets.All(target =>
                policy.IsCurrentApprovalAllowed(saved.ApprovedByPrincipalId, DeclarativeMutationAuthorization.Request(target)));
    }

    private bool CanRead(W68StoredRuntimePlan plan, HttpContext context) =>
        Guid.TryParse(context.RequestServices.GetRequiredService<IConfiguration>()["Kafdeck:Declarative:DeploymentGeneration"], out var generation) &&
        generation != Guid.Empty && plan.DeploymentGeneration == generation &&
        MutationActor.TryCreate(context.User, out var actor) && actor?.PrincipalId == plan.RequesterPrincipalId &&
        authorization.Authorize(context.User, plan.Key.DeploymentId, plan.OwnerId, plan.SourceId, plan.SourceRevisionHash,
            plan.Key.ResourceId, new[] { plan.DesiredSubscription.DestinationId }.Concat(
                plan.ObservedDestinationId is null ? Array.Empty<string>() : new[] { plan.ObservedDestinationId }), DeclarativeRuntimeVerb.Read);
    private static bool SameDesired(DeclarativeSubscriptionManifest desired, NotificationSubscriptionSnapshot current) =>
        desired.State == current.State && desired.Definition.DestinationId == current.Definition.DestinationId &&
        desired.Definition.EventClasses.SequenceEqual(current.Definition.EventClasses) && desired.Definition.EventTypes.SequenceEqual(current.Definition.EventTypes);
    private static object? Projection(NotificationSubscriptionSnapshot? snapshot) => snapshot is null ? null : new
    {
        destinationRef = snapshot.Definition.DestinationId, state = snapshot.State.ToString(), snapshot.Revision,
        eventClasses = snapshot.Definition.EventClasses.Select(value => value.ToString()).ToArray(), snapshot.Definition.EventTypes
    };
    private static IResult Problem(int status, string code) => Results.Problem(statusCode: status, title: code,
        extensions: status is 403 or 409 ? new Dictionary<string, object?> { ["drift"] = status == 403 ? "Inaccessible" : "Conflict" } : null);
}
