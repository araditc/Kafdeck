using Kafdeck.Core.Notifications;

namespace Kafdeck.Core.Declarative;

// Not a route or a self-service GitOps interface. Callers MUST bind every
// delegate to authenticated backend authority. No client-controlled grants.
internal sealed class W68CandidateSubscriptionObservationBridge(
    INotificationRoutingStore store,
    Func<string, bool> currentSubscriptionReadAllowed,
    Func<string, bool> currentDestinationReadAllowed,
    Func<string, bool> currentDeploymentAllowed,
    Func<string, string, CancellationToken, ValueTask<bool>> currentOwnershipConsistent,
    string serverDeploymentId)
{
    public async Task<CandidateDriftStatus> InspectAsync(
        CandidateSubscriptionManifest desired,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(desired);
        if (!string.Equals(desired.DeploymentRef, serverDeploymentId, StringComparison.Ordinal) ||
            !currentDeploymentAllowed(desired.DeploymentRef) ||
            !currentSubscriptionReadAllowed(desired.Definition.SubscriptionId))
            return CandidateDriftStatus.Inaccessible;

        NotificationSubscriptionSnapshot? current;
        try
        {
            current = await store.GetSubscriptionAsync(desired.Definition.SubscriptionId, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return CandidateDriftStatus.Unknown;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // A failed observation must never be interpreted as absence.
            return CandidateDriftStatus.Unknown;
        }

        // Recheck authorization after observation to fence read-right revocation.
        if (!currentDeploymentAllowed(desired.DeploymentRef) ||
            !currentSubscriptionReadAllowed(desired.Definition.SubscriptionId) ||
            !currentDestinationReadAllowed(desired.Definition.DestinationId) ||
            (current is not null && !currentDestinationReadAllowed(current.Definition.DestinationId)))
            return CandidateDriftStatus.Inaccessible;

        // No owner registry is currently admitted. A delegate bound to the
        // default deny-all policy prevents observed or absent data from
        // becoming managed merely because a Git manifest names an owner.
        bool ownershipConsistent;
        try
        {
            ownershipConsistent = await currentOwnershipConsistent(
                desired.DeploymentRef, desired.Definition.SubscriptionId, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return CandidateDriftStatus.Unknown;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return CandidateDriftStatus.Unknown;
        }

        if (!ownershipConsistent)
            return CandidateDriftStatus.Conflict;

        // The snapshot's own server-managed revision is the only current
        // revision evidence used; no raw provider, manifest revision or
        // client fingerprint is trusted as observation authority.
        var fingerprint = current is null
            ? null
            : CandidateSubscriptionManifest.Fingerprint(
                serverDeploymentId, current.Definition, current.State);

        return CandidateSubscriptionDriftClassifier.Classify(desired.Fingerprint(),
            new CandidateDriftEvidence(current is null
                ? CandidateObservationTruth.AuthorizedMissing
                : CandidateObservationTruth.AuthorizedCurrent,
                fingerprint, IsRevisionCurrent: true, IsOwnershipConsistent: true));
    }
}
