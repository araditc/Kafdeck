using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>No cache or config fallback: every node reads the shared authoritative revision.</summary>
public sealed class GovernedNotificationCatalog(NotificationGovernanceOptions options,INotificationGovernanceStore store,
    SecretResolver secrets) : INotificationDestinationProfileCatalog,INotificationCredentialResolver,INotificationDeliveryClaimGate,
    INotificationPhysicalRevisionGate
{
    public async ValueTask<NotificationDestinationProfile?> GetAsync(string destinationId,CancellationToken token)
    {
        _ = new NotificationCredentialBindingId(destinationId);
        var state=await store.GetAsync(destinationId,token).ConfigureAwait(false);
        if(state is null) return null;
        var candidate=Current(state);
        return candidate.Clone(state.Revision,candidate.Source.Enabled && state.ProfileEnabled &&
            (candidate.Source.CredentialBindingId is null || state.CredentialEnabled && candidate.Credential?.Enabled==true));
    }
    public async ValueTask<bool> CanClaimAsync(string destinationId,CancellationToken token) =>
        await GetAsync(destinationId,token).ConfigureAwait(false) is { Enabled:true };
    public async ValueTask VerifyAsync(string destinationId,string profileRevisionFingerprint,CancellationToken token)
    {
        var profile=await GetAsync(destinationId,token).ConfigureAwait(false);
        if(profile is not { Enabled:true } || !string.Equals(profile.RevisionFingerprint,profileRevisionFingerprint,StringComparison.Ordinal))
            throw new KafdeckConfigurationException("Current governed destination revision does not admit a physical write.");
    }
    public async ValueTask<NotificationCredentialValue> ResolveAsync(NotificationCredentialResolutionRequest request,CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);
        var state=await store.GetAsync(request.DestinationId,token).ConfigureAwait(false)
            ?? throw new KafdeckConfigurationException("Governed destination is not enrolled.");
        var candidate=Current(state);
        var profile=await GetAsync(request.DestinationId,token).ConfigureAwait(false);
        if(profile is not { Enabled:true } || !state.CredentialEnabled || candidate.Credential is not { Enabled:true } reference ||
            request.Provider!=profile.Provider || request.BindingId.Value!=profile.CredentialBindingId?.Value ||
            request.ProfileRevisionFingerprint!=profile.RevisionFingerprint)
            throw new KafdeckConfigurationException("Governed credential is revoked or its current revision changed.");
        var material=secrets.ResolveBounded(reference.Secret,ConfiguredNotificationCredentialResolver.MaxCredentialBytes);
        // Secret file I/O does not hold the governance authority stationary.
        var after=await store.GetAsync(request.DestinationId,token).ConfigureAwait(false);
        if(after!=state) throw new KafdeckConfigurationException("Governance changed during credential resolution.");
        return new NotificationCredentialValue(material.Reveal());
    }
    private NotificationGovernanceCandidate Current(NotificationGovernanceState state)
    {
        var candidate=options.Find(state.DestinationId,state.CandidateId);
        if(candidate is null || candidate.Fingerprint!=state.CandidateFingerprint || state.Revision<1)
            throw new KafdeckConfigurationException("Shared governance is not identical to the provisioned candidate revision.");
        return candidate;
    }
}
