using System.Text.Json;
using System.Data.Common;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

/// <summary>Read-only freshness at confirm/approval. Current caller authorization remains in MutationCommandService.</summary>
public sealed class DeclarativeLifecycleGuard(IW68RuntimeStore store, DeclarativeRuntimeAuthorizer authorization, IMutationOperationRepository? operations = null)
{
    public async Task<bool> IsCurrentAsync(MutationOperationSnapshot operation, CancellationToken token = default)
    {
        try
        {
            var reference = DeclarativeSubscriptionMutation.ReadReference(operation.CanonicalIntent);
            if (reference.Id != operation.OperationId || operation.OperationKind != MutationOperationKind.DeclarativeNotificationSubscription) return false;
            var target = await store.GetPlanAsync(reference.Id, token).ConfigureAwait(false);
            var source = target?.CompensatesPlanId is { } sourceId ? await store.GetPlanAsync(sourceId, token).ConfigureAwait(false) : null;
            return await store.IsPlanCurrentAsync(reference.Id, reference.Hash,
                async (plan, ct) => Current(plan, operation) && (plan.CompensatesPlanId is null || source is not null && await DeclarativeCompensationAuthority.IsVerifiedPlanAsync(source, operations, ct).ConfigureAwait(false)), token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or JsonException or KeyNotFoundException or FormatException or DbException or TimeoutException)
        { return false; }
    }

    private bool Current(W68StoredRuntimePlan plan, MutationOperationSnapshot operation)
    {
        var intent = DeclarativeSubscriptionMutation.Intent(plan);
        return plan.PlanId == operation.OperationId && plan.RequesterPrincipalId == operation.RequesterPrincipalId &&
            plan.PolicyFingerprint == authorization.PolicyFingerprint && operation.PolicyVersion == "v0.8-bearer:" + authorization.PolicyFingerprint &&
            plan.ExpiresAtUtc > DateTimeOffset.UtcNow && operation.CanonicalIntent == intent.CanonicalIntent &&
            operation.ClusterId == plan.Key.DeploymentId && operation.AuthorizationTargets.SequenceEqual(intent.AuthorizationTargets!) &&
            operation.ResourceKeys.SequenceEqual(intent.ResourceKeys) &&
            MutationGovernancePreconditions.ProviderOnly(operation).SequenceEqual(intent.Preconditions!.OrderBy(p => p.Key, StringComparer.Ordinal)) && operation.MaterialDigests.Count == 0 &&
            authorization.IsStoredRegistrationCurrent(plan.RequesterPrincipalId, plan.Key.DeploymentId, plan.OwnerId, plan.SourceId,
                plan.SourceRevisionHash, plan.Key.ResourceId, plan.ControllerId, operation.AuthorizationTargets.Select(t => t.Action).Distinct().ToArray());
    }
}
