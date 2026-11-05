using System.Security.Claims;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

/// <summary>The mutation carries only an immutable lookup reference. Desired effect fields are loaded from server storage.</summary>
public static class DeclarativeSubscriptionMutation
{
    public static MutationIntentDescriptor Intent(W68StoredRuntimePlan plan)
    {
        Validate(plan);
        var targets = Targets(plan);
        return new(MutationOperationKind.DeclarativeNotificationSubscription, plan.Key.DeploymentId,
            JsonSerializer.Serialize(new Reference(1, plan.PlanId, plan.PlanFingerprint)),
            [$"declarative/{plan.Key.DeploymentId}/{plan.DeploymentGeneration:D}/NotificationSubscription/{plan.Key.ResourceId}"],
            [new("declarative.plan", plan.PlanFingerprint), new("declarative.desired", plan.DesiredFingerprint),
                new("declarative.observed", plan.ObservedSubscriptionFingerprint)],
            AuthorizationTargets: targets);
    }

    public static IReadOnlyList<MutationAuthorizationTarget> Targets(W68StoredRuntimePlan plan)
    {
        var deployment = plan.Key.DeploymentId; var resource = plan.Key.ResourceId;
        var targets = new List<MutationAuthorizationTarget>
        {
            new(AuthorizationAction.DeclarativeRead, deployment, resource),
            new(AuthorizationAction.DeclarativePlan, deployment, resource),
        };
        if (plan.Command != W68RuntimePlanCommand.Adopt) targets.Add(new(AuthorizationAction.DeclarativeApply, deployment, resource));
        if (plan.Command != W68RuntimePlanCommand.Apply) targets.Add(new(AuthorizationAction.DeclarativeAdopt, deployment, resource));
        foreach (var destination in Destinations(plan).Append(resource).Distinct(StringComparer.Ordinal))
        {
            targets.Add(new(AuthorizationAction.NotificationRead, deployment, destination));
            if (plan.Command == W68RuntimePlanCommand.Apply) targets.Add(new(AuthorizationAction.NotificationManage, deployment, destination));
        }
        return DeclarativeMutationAuthorization.Normalize(deployment, targets);
    }

    public static IEnumerable<string> Destinations(W68StoredRuntimePlan plan) => plan.ObservedDestinationId is null
        ? [plan.DesiredSubscription.DestinationId] : new[] { plan.ObservedDestinationId, plan.DesiredSubscription.DestinationId }.Distinct(StringComparer.Ordinal);

    public static void Validate(W68StoredRuntimePlan plan)
    {
        if (plan.ObservedSubscription is { } observed &&
            (observed.Definition.SubscriptionId != plan.Key.ResourceId || observed.Revision != plan.ExpectedSubscriptionRevision ||
             observed.Definition.DestinationId != plan.ObservedDestinationId || W68RuntimeFingerprints.Subscription(observed) != plan.ObservedSubscriptionFingerprint))
            throw Invalid();
        if (plan.PlanId == Guid.Empty || !Enum.IsDefined(plan.Command) || plan.Key.ResourceKind != "NotificationSubscription" ||
            plan.Key.ScopeKind != "Deployment" || plan.Key.ScopeId != plan.Key.DeploymentId ||
            !DeclarativeRuntimeAuthorizer.Identifier(plan.Key.DeploymentId) || !DeclarativeRuntimeAuthorizer.Identifier(plan.Key.ResourceId) ||
            !DeclarativeRuntimeAuthorizer.Identifier(plan.OwnerId) || !DeclarativeRuntimeAuthorizer.Identifier(plan.SourceId) ||
            !DeclarativeRuntimeAuthorizer.Identifier(plan.ControllerId) || !DeclarativeRuntimeAuthorizer.Identifier(plan.LeaseScopeId) ||
            string.IsNullOrWhiteSpace(plan.LeaseHolderId) || plan.LeaseHolderId.Length > 128 || plan.LeaseHolderId.Any(char.IsControl) ||
            plan.SourceGeneration == Guid.Empty || plan.ExpectedOwnershipRevision < 0 || plan.LeaseEpoch < 1 ||
            plan.ExpectedSubscriptionRevision is < 1 || plan.DesiredSubscription.SubscriptionId != plan.Key.ResourceId ||
            plan.DesiredState is not (Kafdeck.Core.Notifications.NotificationSubscriptionState.Active or Kafdeck.Core.Notifications.NotificationSubscriptionState.Paused) ||
            !DeclarativeRuntimeAuthorizer.Hash(plan.SourceRevisionHash) || !DeclarativeRuntimeAuthorizer.Hash(plan.PlanFingerprint) ||
            (plan.ExpectedSubscriptionRevision is null
                ? plan.ObservedSubscriptionFingerprint != "missing" || plan.ObservedDestinationId is not null
                : !DeclarativeRuntimeAuthorizer.Hash(plan.ObservedSubscriptionFingerprint) || plan.ObservedDestinationId is null) ||
            !DeclarativeRuntimeAuthorizer.Hash(plan.PolicyFingerprint) ||
            W68RuntimeFingerprints.Plan(plan) != plan.PlanFingerprint || W68RuntimeFingerprints.Desired(plan.DesiredCanonicalJson) != plan.DesiredFingerprint ||
            plan.ExpiresAtUtc == default || string.IsNullOrWhiteSpace(plan.RequesterPrincipalId)) throw Invalid();
        var manifest = DeclarativeSubscriptionManifest.Parse(Encoding.UTF8.GetBytes(plan.DesiredCanonicalJson), "json");
        if (manifest.DeploymentRef != plan.Key.DeploymentId || manifest.OwnerRef != plan.OwnerId || manifest.State != plan.DesiredState ||
            manifest.Definition.SubscriptionId != plan.Key.ResourceId || manifest.Definition.DestinationId != plan.DesiredSubscription.DestinationId ||
            !manifest.Definition.EventClasses.SequenceEqual(plan.DesiredSubscription.EventClasses) ||
            !manifest.Definition.EventTypes.SequenceEqual(plan.DesiredSubscription.EventTypes) ||
            !manifest.ToCanonicalUtf8().SequenceEqual(Encoding.UTF8.GetBytes(plan.DesiredCanonicalJson))) throw Invalid();
        _ = Targets(plan);
    }

    internal static (Guid Id, string Hash) ReadReference(string canonical)
    {
        if (canonical.Length > 512) throw Invalid();
        using var document = JsonDocument.Parse(canonical, new JsonDocumentOptions { MaxDepth = 2 });
        var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object || root.EnumerateObject().Count() != 3 ||
            root.GetProperty("SchemaVersion").GetInt32() != 1 || !root.GetProperty("PlanId").TryGetGuid(out var id) || id == Guid.Empty ||
            root.GetProperty("PlanFingerprint").GetString() is not { } hash || !DeclarativeRuntimeAuthorizer.Hash(hash)) throw Invalid();
        if (canonical != JsonSerializer.Serialize(new Reference(1, id, hash))) throw Invalid();
        return (id, hash);
    }

    private sealed record Reference(int SchemaVersion, Guid PlanId, string PlanFingerprint);
    private static MutationStateException Invalid() => new("The stored declarative plan binding is invalid.");
}

public sealed class DeclarativeSubscriptionMutationHandler(IW68RuntimeStore store, DeclarativeRuntimeAuthorizer authority,
    MutationRequestAuthorizationService authorization, MutationExecutionRequestContextAccessor requestContext, IMutationOperationRepository? operations = null) : IMutationExecutionHandler
{
    public MutationOperationKind OperationKind => MutationOperationKind.DeclarativeNotificationSubscription;

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(MutationOperationSnapshot operation, CancellationToken token = default) =>
        await LoadAuthorizedAsync(operation, token).ConfigureAwait(false) is not null
            ? MutationPreDispatchGuardResult.Allowed : new(MutationPreDispatchGuardOutcome.StalePreview, "declarative_plan_or_authority_stale");

    public async Task<MutationProviderResult> ExecuteAsync(MutationExecutionContext context, CancellationToken cancellationToken = default)
    {
        var plan = await LoadAuthorizedAsync(context.Operation, cancellationToken).ConfigureAwait(false);
        if (plan is null) return new(MutationExecutionResultKind.FailedBeforeDispatch, "declarative_plan_or_authority_stale");
        var compensationSource = plan.CompensatesPlanId is { } sourceId ? await store.GetPlanAsync(sourceId, cancellationToken).ConfigureAwait(false) : null;
        W68RuntimeCommitReceipt? receipt;
        try
        {
            receipt = await store.ApplySubscriptionAsync(plan.PlanId, plan.PlanFingerprint, context.Operation.RequesterPrincipalId,
                async (stored, token) => CurrentAllowed(stored, context.Operation) && (stored.CompensatesPlanId is null || compensationSource is not null && await DeclarativeCompensationAuthority.IsVerifiedPlanAsync(compensationSource, operations, token).ConfigureAwait(false)), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Commit acknowledgement loss cannot be reported as non-application. The immutable receipt is the recovery authority.
            return new(MutationExecutionResultKind.ExecutionUnknown, "declarative_commit_unknown");
        }
        return receipt is null ? new(MutationExecutionResultKind.FailedBeforeDispatch, "declarative_fenced_state_changed")
            : new(MutationExecutionResultKind.AppliedVerified, "declarative_subscription_applied", new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["declarative.plan"] = receipt.OperationId.ToString("D"), ["declarative.plan-fingerprint"] = receipt.PlanFingerprint,
                ["declarative.ownership-revision"] = receipt.OwnershipRevision.ToString(System.Globalization.CultureInfo.InvariantCulture),
                ["declarative.subscription-revision"] = receipt.SubscriptionRevision.ToString(System.Globalization.CultureInfo.InvariantCulture),
            });
    }

    private async Task<W68StoredRuntimePlan?> LoadAuthorizedAsync(MutationOperationSnapshot operation, CancellationToken token)
    {
        try
        {
            if (operation.OperationKind != OperationKind) return null;
            var reference = DeclarativeSubscriptionMutation.ReadReference(operation.CanonicalIntent);
            if (reference.Id != operation.OperationId) return null;
            var plan = await store.LoadPlanAsync(reference.Id, token).ConfigureAwait(false);
            if (plan is null || plan.PlanFingerprint != reference.Hash || plan.CompensatesPlanId is { } sourceId && !await DeclarativeCompensationAuthority.IsVerifiedAsync(sourceId, operations, store, token).ConfigureAwait(false)) return null;
            DeclarativeSubscriptionMutation.Validate(plan);
            var intent = DeclarativeSubscriptionMutation.Intent(plan);
            if (operation.CanonicalIntent != intent.CanonicalIntent || !operation.ResourceKeys.SequenceEqual(intent.ResourceKeys) ||
                !operation.AuthorizationTargets.SequenceEqual(intent.AuthorizationTargets!) ||
                !operation.Preconditions.SequenceEqual(intent.Preconditions!.OrderBy(p => p.Key, StringComparer.Ordinal)) ||
                operation.MaterialDigests.Count != 0 || operation.ClusterId != plan.Key.DeploymentId ||
                !operation.Risk.RequiresIndependentApproval || operation.Risk.RiskClass != MutationRiskClass.Critical ||
                !CurrentAllowed(plan, operation)) return null;
            return plan;
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or JsonException or KeyNotFoundException or FormatException)
        { return null; }
    }

    private bool CurrentAllowed(W68StoredRuntimePlan plan, MutationOperationSnapshot operation)
    {
        var principal = requestContext.CurrentPrincipal;
        if (!MutationActor.TryCreate(principal, out var actor) || actor is null || actor.PrincipalId != operation.RequesterPrincipalId ||
            plan.RequesterPrincipalId != actor.PrincipalId || plan.PolicyFingerprint != authority.PolicyFingerprint ||
            plan.ExpiresAtUtc <= DateTimeOffset.UtcNow || authorization.AuthorizeForDispatch(principal, operation) != KafdeckAuthorizationOutcome.Allowed)
            return false;
        if (authority.Registration(principal, plan.Key.DeploymentId, plan.OwnerId, plan.SourceId, plan.SourceRevisionHash,
                plan.Key.ResourceId)?.Id != plan.ControllerId) return false;
        var verb = plan.Command == W68RuntimePlanCommand.Adopt ? DeclarativeRuntimeVerb.Adopt : DeclarativeRuntimeVerb.Apply;
        return authority.Authorize(principal, plan.Key.DeploymentId, plan.OwnerId, plan.SourceId, plan.SourceRevisionHash,
            plan.Key.ResourceId, DeclarativeSubscriptionMutation.Destinations(plan), verb);
    }
}
