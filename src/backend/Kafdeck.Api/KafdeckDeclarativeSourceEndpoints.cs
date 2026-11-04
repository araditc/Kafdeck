using System.Data.Common;
using System.Text;
using System.Text.Json.Serialization;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record DeclarativeSourceAdvanceRequest(string Manifest, string Format, Guid ExpectedGeneration,
    long ExpectedRevision, string ExpectedSourceHash);

public sealed record DeclarativeSourceRevisionData(string SourceRef, Guid Generation, long Revision, string SourceHash, bool Changed = false);

/// <summary>Advances only a registered source hash. It grants neither plan nor subscription effect authority.</summary>
public static class KafdeckDeclarativeSourceEndpoints
{
    public static WebApplication MapKafdeckDeclarativeSources(this WebApplication app)
    {
        app.MapGet("/api/v1/declarative/sources/{sourceRef}", ReadAsync)
            .WithName("v08-declarative-source-detail")
            .WithMetadata(new DeclarativeRouteContract(AuthorizationAction.DeclarativeRead));
        app.MapPost("/api/v1/declarative/sources/{sourceRef}/advance", AdvanceAsync)
            .WithName("v08-declarative-source-advance")
            .WithMetadata(new DeclarativeRouteContract(AuthorizationAction.DeclarativeSourceManage, typeof(DeclarativeSourceAdvanceRequest)))
            .RequireKafdeckAntiforgery();
        return app;
    }

    internal static async Task<IResult> ReadAsync(string sourceRef, HttpContext context, IW68RuntimeStore store,
        INotificationRoutingStore routing, DeclarativeRuntimeAuthorizer authorization, CancellationToken token)
    {
        if (context.Request.Query.Count != 1 || !context.Request.Query.TryGetValue("resourceId", out var values) ||
            values.Count != 1 || values[0] is not { } resource || !DeclarativeRuntimeAuthorizer.Identifier(resource))
            return Problem(400, "declarative_source_query_invalid");
        try
        {
            var registration = authorization.SourceRegistration(context.User, sourceRef, resource);
            if (registration is null) return Problem(403, "declarative_source_read_denied");
            if (!authorization.Authorize(context.User, authorization.DeploymentId, registration.OwnerId, sourceRef,
                    registration.SourceRevision, resource, [], DeclarativeRuntimeVerb.Read))
                return Problem(403, "declarative_source_read_denied");
            var subscription = await routing.GetSubscriptionAsync(resource, token).ConfigureAwait(false);
            if (!authorization.Authorize(context.User, authorization.DeploymentId, registration.OwnerId, sourceRef,
                    registration.SourceRevision, resource, subscription is null ? [] : [subscription.Definition.DestinationId], DeclarativeRuntimeVerb.Read))
                return Problem(403, "declarative_source_read_denied");
            var source = await store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source, authorization.DeploymentId, sourceRef, token).ConfigureAwait(false);
            if (source is null) return Problem(404, "declarative_source_not_registered");
            if (!Matches(source, registration, resource)) return Problem(403, "declarative_source_read_denied");
            return Results.Ok(Data(source));
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException) { return Problem(400, "declarative_source_request_invalid"); }
        catch (Exception ex) when (ex is DbException or TimeoutException) { return Problem(503, "declarative_source_store_unavailable"); }
    }

    internal static async Task<IResult> AdvanceAsync(string sourceRef, DeclarativeSourceAdvanceRequest request,
        HttpContext context, IW68RuntimeStore store, INotificationRoutingStore routing,
        DeclarativeRuntimeAuthorizer authorization, CancellationToken token)
    {
        try
        {
            if (request.Manifest is null || request.Manifest.Length > DeclarativeSubscriptionManifest.MaxInputBytes ||
                request.ExpectedRevision < 0 || !DeclarativeRuntimeAuthorizer.Hash(request.ExpectedSourceHash) ||
                request.ExpectedGeneration == Guid.Empty != (request.ExpectedRevision == 0))
                return Problem(400, "declarative_source_request_invalid");
            var desired = DeclarativeSubscriptionManifest.Parse(Encoding.UTF8.GetBytes(request.Manifest), request.Format);
            var resource = desired.Definition.SubscriptionId; var nextHash = desired.Fingerprint();
            var registration = authorization.Registration(context.User, desired.DeploymentRef, desired.OwnerRef, sourceRef, nextHash, resource);
            if (registration is null || !registration.AllowRevisionAdvance || !MutationActor.TryCreate(context.User, out var actor) || actor is null)
                return Problem(403, "declarative_source_advance_denied");
            var destinations = new[] { desired.Definition.DestinationId };
            bool Allowed() => authorization.Registration(context.User, desired.DeploymentRef, desired.OwnerRef, sourceRef, nextHash, resource)?.Id == registration.Id &&
                authorization.Authorize(context.User, desired.DeploymentRef, desired.OwnerRef, sourceRef, nextHash, resource,
                    destinations, DeclarativeRuntimeVerb.SourceManage);
            Task<bool> Authority(CancellationToken _) => Task.FromResult(Allowed());
            if (!Allowed()) return Problem(403, "declarative_source_advance_denied");
            var currentSubscription = await routing.GetSubscriptionAsync(resource, token).ConfigureAwait(false);
            if (currentSubscription is not null) destinations = [desired.Definition.DestinationId, currentSubscription.Definition.DestinationId];
            if (!Allowed()) return Problem(403, "declarative_source_advance_denied");
            var source = await store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source, desired.DeploymentRef, sourceRef, token).ConfigureAwait(false);
            if (source is null)
            {
                // Initial identity is issued by the backend, and its starting
                // hash is the deployment's trusted baseline, never the new caller hash.
                if (request.ExpectedGeneration != Guid.Empty || request.ExpectedRevision != 0 || request.ExpectedSourceHash != registration.SourceRevision)
                    return Problem(409, "declarative_source_revision_conflict");
                if (!await EnsureAsync(W68RuntimeRegistrationKind.Owner, registration.OwnerId, "").ConfigureAwait(false) ||
                    !await EnsureAsync(W68RuntimeRegistrationKind.Controller, registration.Id, actor.PrincipalId).ConfigureAwait(false))
                    return Problem(409, "declarative_source_registration_conflict");
                var baseline = new W68RuntimeRegistration(W68RuntimeRegistrationKind.Source, desired.DeploymentRef, sourceRef,
                    Guid.NewGuid(), 0, desired.OwnerRef, "", sourceRef, resource, registration.SourceRevision, true);
                source = await store.UpsertRegistrationAsync(baseline, 0, Authority, token).ConfigureAwait(false);
                if (source is null) return Problem(409, "declarative_source_revision_conflict");
            }
            else if (source.GenerationId != request.ExpectedGeneration || source.Revision != request.ExpectedRevision ||
                     source.PinnedRevision != request.ExpectedSourceHash)
                return Problem(409, "declarative_source_revision_conflict");
            if (!Matches(source, registration, resource)) return Problem(403, "declarative_source_advance_denied");
            if (!await EnsureAsync(W68RuntimeRegistrationKind.Owner, registration.OwnerId, "").ConfigureAwait(false) ||
                !await EnsureAsync(W68RuntimeRegistrationKind.Controller, registration.Id, actor.PrincipalId).ConfigureAwait(false))
                return Problem(409, "declarative_source_registration_conflict");
            if (source.PinnedRevision == nextHash)
                return Allowed() ? Results.Ok(Data(source)) : Problem(403, "declarative_source_advance_denied");
            var advanced = await store.UpsertRegistrationAsync(source with { PinnedRevision = nextHash }, source.Revision, Authority, token).ConfigureAwait(false);
            return advanced is null ? Problem(409, "declarative_source_revision_conflict") : Results.Ok(Data(advanced, true));

            async Task<bool> EnsureAsync(W68RuntimeRegistrationKind kind, string id, string principal)
            {
                var old = await store.GetRegistrationAsync(kind, desired.DeploymentRef, id, token).ConfigureAwait(false);
                if (old is not null) return old.Enabled && old.OwnerId == registration.OwnerId && old.PrincipalId == principal && Allowed();
                var initial = new W68RuntimeRegistration(kind, desired.DeploymentRef, id, Guid.NewGuid(), 0,
                    registration.OwnerId, principal, "", "", "", true);
                return await store.UpsertRegistrationAsync(initial, 0, Authority, token).ConfigureAwait(false) is not null;
            }
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException) { return Problem(400, "declarative_source_request_invalid"); }
        catch (Exception ex) when (ex is DbException or TimeoutException) { return Problem(503, "declarative_source_store_unavailable"); }
    }

    private static bool Matches(W68RuntimeRegistration source, DeclarativeRuntimeRegistration registration, string resource) =>
        source.Enabled && source.OwnerId == registration.OwnerId && source.RegistrationId == registration.SourceRef &&
        source.RepositoryId == registration.SourceRef && source.Path == resource &&
        (registration.AllowRevisionAdvance || source.PinnedRevision == registration.SourceRevision);
    private static DeclarativeSourceRevisionData Data(W68RuntimeRegistration source, bool changed = false) =>
        new(source.RegistrationId, source.GenerationId, source.Revision, source.PinnedRevision, changed);
    private static IResult Problem(int status, string code) => Results.Problem(statusCode: status, title: code);
}
