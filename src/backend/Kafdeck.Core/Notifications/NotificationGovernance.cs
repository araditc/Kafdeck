namespace Kafdeck.Core.Notifications;

/// <summary>Metadata only. Candidates are provisioned by the server owner.</summary>
public sealed record NotificationGovernanceState(
    string DestinationId, string CandidateId, string CandidateFingerprint,
    long Revision, bool ProfileEnabled, bool CredentialEnabled,
    DateTimeOffset UpdatedAtUtc);

public sealed record NotificationGovernanceChange
{
    public NotificationGovernanceChange(string destinationId, string candidateId, string candidateFingerprint,
        long expectedRevision, bool profileEnabled, bool credentialEnabled, string actor, string policyFingerprint)
    {
        NotificationDeliveryIdentity.NormalizeDestinationId(destinationId);
        _ = new NotificationCredentialBindingId(candidateId);
        if (expectedRevision is < 0 or long.MaxValue || !Fingerprint(candidateFingerprint) || !Fingerprint(policyFingerprint))
            throw new ArgumentException("Governance revision or fingerprint is invalid.");
        if (string.IsNullOrWhiteSpace(actor) || actor.Length > 2048 || actor.Any(char.IsControl))
            throw new ArgumentException("Governance actor is invalid.", nameof(actor));
        DestinationId=destinationId; CandidateId=candidateId; CandidateFingerprint=candidateFingerprint;
        ExpectedRevision=expectedRevision; ProfileEnabled=profileEnabled; CredentialEnabled=credentialEnabled;
        Actor=actor; PolicyFingerprint=policyFingerprint;
    }
    public string DestinationId { get; }
    public string CandidateId { get; }
    public string CandidateFingerprint { get; }
    public long ExpectedRevision { get; }
    public bool ProfileEnabled { get; }
    public bool CredentialEnabled { get; }
    public string Actor { get; }
    public string PolicyFingerprint { get; }
    private static bool Fingerprint(string value) => value is { Length:64 } && value.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');
}

public sealed record NotificationGovernanceAudit(
    string DestinationId, long Revision, string CandidateId, bool ProfileEnabled,
    bool CredentialEnabled, string Actor, string PolicyFingerprint, DateTimeOffset UpdatedAtUtc);

public interface INotificationGovernanceStore
{
    Task InitializeAsync(CancellationToken token);
    Task<NotificationGovernanceState?> GetAsync(string destinationId, CancellationToken token);
    /// <summary>State CAS and immutable actor/policy audit are one transaction.</summary>
    Task<NotificationGovernanceState?> ChangeAsync(NotificationGovernanceChange change, CancellationToken token);
    Task<IReadOnlyList<NotificationGovernanceAudit>> ReadAuditAsync(string destinationId, long afterRevision,
        int maxResults, CancellationToken token);
}

public interface INotificationDeliveryClaimGate
{
    ValueTask<bool> CanClaimAsync(string destinationId, CancellationToken token);
}

/// <summary>Current shared destination authority at the last physical write boundary.
/// A previously resolved credential or profile snapshot does not grant this authority.</summary>
public interface INotificationPhysicalRevisionGate
{
    ValueTask VerifyAsync(string destinationId, string profileRevisionFingerprint, CancellationToken token);
}
