using System.Data.Common;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>Version 1 admits only reversal of a verified metadata state toggle. Delivered events are never reversed.</summary>
public static class W71CompensationProof
{
    public const int CatalogVersion = 1;
    public static bool IsToggle(W68StoredRuntimePlan source) => W68RuntimeFingerprints.Plan(source) == source.PlanFingerprint && source.CompensatesPlanId is null &&
        source.Command == W68RuntimePlanCommand.Apply && source.ObservedSubscription is { } before &&
        before.State is NotificationSubscriptionState.Active or NotificationSubscriptionState.Paused &&
        source.DesiredState is NotificationSubscriptionState.Active or NotificationSubscriptionState.Paused &&
        before.State != source.DesiredState && SameDefinition(before.Definition, source.DesiredSubscription);

    public static bool SameDefinition(NotificationSubscriptionDefinition a, NotificationSubscriptionDefinition b) =>
        a.SubscriptionId == b.SubscriptionId && a.DestinationId == b.DestinationId &&
        a.EventClasses.SequenceEqual(b.EventClasses) && a.EventTypes.SequenceEqual(b.EventTypes);

    public static bool CurrentMatches(W68StoredRuntimePlan source, W68RuntimeCommitReceipt receipt,
        W68RuntimeOwnership? owner, NotificationSubscriptionSnapshot? current) => IsToggle(source) &&
        receipt.OperationId == source.PlanId && receipt.PlanFingerprint == source.PlanFingerprint &&
        receipt.CommittedAtUtc != default && receipt.CommittedAtUtc <= source.ExpiresAtUtc &&
        owner is { State: "Managed" } && owner.Key == source.Key && owner.OwnerId == source.OwnerId &&
        owner.SourceId == source.SourceId && owner.SourceGeneration == source.SourceGeneration &&
        owner.SourceRevisionHash == source.SourceRevisionHash && owner.Revision == receipt.OwnershipRevision &&
        owner.GenerationId == receipt.OwnershipGeneration && current is not null &&
        current.Revision == receipt.SubscriptionRevision && current.State == source.DesiredState &&
        SameDefinition(current.Definition, source.DesiredSubscription);

    public static bool IsInverse(W68StoredRuntimePlan inverse, W68StoredRuntimePlan source) =>
        IsToggle(source) && inverse.CompensatesPlanId == source.PlanId && inverse.PlanId != source.PlanId &&
        inverse.Command == W68RuntimePlanCommand.Apply && inverse.Key == source.Key && inverse.OwnerId == source.OwnerId &&
        inverse.SourceId == source.SourceId && inverse.SourceGeneration == source.SourceGeneration &&
        inverse.RequesterPrincipalId == source.RequesterPrincipalId && inverse.DeploymentGeneration == source.DeploymentGeneration &&
        inverse.DesiredState == source.ObservedSubscription!.State && SameDefinition(inverse.DesiredSubscription, source.ObservedSubscription.Definition);
}

public sealed partial class W68RuntimeStore
{
    private async Task<bool> CompensationAllowsAsync(DbConnection connection, DbTransaction transaction, W68StoredRuntimePlan inverse,
        W68RuntimeOwnership? owner, NotificationSubscriptionSnapshot? current, CancellationToken token)
    {
        if (inverse.CompensatesPlanId is null) return true;
        var source = await PlanAsync(connection, transaction, inverse.CompensatesPlanId.Value, token).ConfigureAwait(false);
        var receipt = await ReceiptAsync(connection, transaction, inverse.CompensatesPlanId.Value, token).ConfigureAwait(false);
        var relation = source is not null && W71CompensationProof.IsInverse(inverse, source);
        var matches = source is not null && receipt is not null && W71CompensationProof.CurrentMatches(source, receipt, owner, current);
        return relation && matches;
    }
}
