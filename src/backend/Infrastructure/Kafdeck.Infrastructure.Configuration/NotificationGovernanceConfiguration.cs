using System.Collections.Frozen;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Microsoft.Extensions.Configuration;

namespace Kafdeck.Infrastructure.Configuration;

public sealed class NotificationGovernanceCandidate
{
    public NotificationGovernanceCandidate(string candidateId,string destinationId,NotificationDestinationProfile source,
        NotificationCredentialReferenceOptions? credential)
    {
        _ = new NotificationCredentialBindingId(candidateId);
        _ = new NotificationCredentialBindingId(destinationId);
        if(destinationId is "." or "..") throw new ArgumentException("Managed destination cannot be a path segment alias.",nameof(destinationId));
        ArgumentNullException.ThrowIfNull(source);
        if(source.CredentialBindingId is not null && (credential is null || credential.DestinationId!=source.DestinationId ||
            credential.BindingId.Value!=source.CredentialBindingId.Value))
            throw new KafdeckConfigurationException("Governance candidate credential is not provisioned for its exact source profile.");
        CandidateId=candidateId; DestinationId=destinationId; Source=source; Credential=credential;
        Profile=Clone(null,source.Enabled);
        // Locator identity is bound privately, never exposed as read metadata.
        Fingerprint=Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(Profile.RevisionFingerprint+"\n"+
            (credential is null ? "anonymous" : ((int)credential.Secret.Kind).ToString(CultureInfo.InvariantCulture)+":"+credential.Secret.Locator+":"+credential.Enabled))))
            .ToLowerInvariant();
    }
    public string CandidateId { get; }
    public string DestinationId { get; }
    public string Fingerprint { get; }
    public NotificationDestinationProfile Source { get; }
    public NotificationDestinationProfile Profile { get; }
    public NotificationCredentialReferenceOptions? Credential { get; }
    public NotificationDestinationProfile Clone(long? generation,bool enabled) => new(DestinationId,Source.Provider,Source.DisplayName,
        Source.EnabledEvents,Source.ConfiguredEndpoint,Source.CredentialBindingId,Source.EmailRecipientAddress,
        Source.WebhookAuthentication,enabled,Source.ProviderResourceId,Source.ProviderSecondaryResourceId,Source.Smtp,generation);
}

public sealed class NotificationGovernanceOptions
{
    private readonly FrozenDictionary<string,NotificationGovernanceCandidate> _byKey;
    public NotificationGovernanceOptions(IEnumerable<NotificationGovernanceCandidate> candidates)
    {
        var values=candidates.Take(501).ToArray();
        if(values.Length is <1 or >500) throw new KafdeckConfigurationException("Governance requires one to 500 provisioned candidates.");
        try { _byKey=values.ToFrozenDictionary(c=>c.DestinationId+"\n"+c.CandidateId,StringComparer.Ordinal); }
        catch(ArgumentException) { throw new KafdeckConfigurationException("Duplicate governance candidate identity."); }
        Candidates=Array.AsReadOnly(values);
    }
    public IReadOnlyList<NotificationGovernanceCandidate> Candidates { get; }
    public NotificationGovernanceCandidate? Find(string destinationId,string candidateId) => _byKey.GetValueOrDefault(destinationId+"\n"+candidateId);
    public static NotificationGovernanceOptions? Read(IConfiguration configuration,KafdeckOptions options)
    {
        var section=configuration.GetSection("Kafdeck:Notifications:Governance");
        if(!section.GetChildren().Any()) return null;
        if(section.Value is not null || section.GetChildren().Any(c=>c.Key is not ("Enabled" or "Candidates")))
            throw new KafdeckConfigurationException("Unknown notification governance configuration.");
        var enabled=false;
        if(section["Enabled"] is { } raw && !bool.TryParse(raw,out enabled))
            throw new KafdeckConfigurationException("Governance Enabled must be boolean.");
        var list=section.GetSection("Candidates");
        if(list.Value is not null) throw new KafdeckConfigurationException("Governance candidates must be a bounded array.");
        var children=list.GetChildren().ToArray();
        if(children.Length>500) throw new KafdeckConfigurationException("Governance candidate cap exceeded.");
        var candidates=new List<NotificationGovernanceCandidate>();
        var profiles=options.Notifications?.DestinationProfiles??[];
        var bindings=options.Notifications?.CredentialBindings??[];
        for(var i=0;i<children.Length;i++)
        {
            var child=list.GetSection(i.ToString(CultureInfo.InvariantCulture));
            if(!children.Any(c=>c.Key==child.Key) || child.Value is not null ||
                child.GetChildren().Count()!=3 || child.GetChildren().Any(c=>c.Key is not ("CandidateId" or "DestinationId" or "ProfileId") || c.GetChildren().Any()))
                throw new KafdeckConfigurationException("Governance candidate fields or canonical array indices are invalid.");
            var source=profiles.SingleOrDefault(p=>p.DestinationId==child["ProfileId"])
                ?? throw new KafdeckConfigurationException("Governance source profile is not provisioned.");
            try { candidates.Add(new(child["CandidateId"]!,child["DestinationId"]!,source,
                bindings.SingleOrDefault(b=>b.DestinationId==source.DestinationId))); }
            catch(ArgumentException) { throw new KafdeckConfigurationException("Governance candidate identity is invalid."); }
        }
        if(!enabled) return null;
        if(options.Deployment.Mode!=AccessMode.Oidc || options.Notifications is not { Enabled:true,ManagementEnabled:true })
            throw new KafdeckConfigurationException("Governance requires OIDC, notification observation and explicit management admission.");
        return new(candidates);
    }
}
