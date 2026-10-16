namespace Kafdeck.Core.Notifications;

/// <summary>Checked after matching the durable safe event, before resolving any endpoint or credential.</summary>
public interface INotificationRecoveryEventGate
{
    ValueTask<bool> CheckAsync(Guid eventId, string destinationId, DateTimeOffset occurredAtUtc, CancellationToken token);
}

/// <summary>Durable source database capture time, kept separate from payload identity and application clock time.</summary>
public interface INotificationRecoverySourceEvidence
{
    ValueTask RecordCaptureAsync(Guid eventId, DateTimeOffset databaseCapturedAtUtc, CancellationToken token);
}

public enum NotificationRecoveryCommand { FenceBatch = 1, AdvanceGeneration = 2, EnrollEmpty = 3, FenceSourceBatch = 4 }
public enum NotificationRecoveryDisposition
{
    ExternalAcceptanceObserved = 1, ExternalNonAcceptanceObserved = 2, UnresolvedRiskAccepted = 3
}
public enum NotificationRecoveryState { Previewed = 1, Approved = 2, Applied = 3 }

public sealed record NotificationRecoveryRequest(NotificationRecoveryCommand Command,
    NotificationRecoveryDisposition? Disposition, string ProofReferenceSha256,
    string FencedSendersEvidenceSha256, Guid? ExpectedDatabaseGeneration, Guid TargetGeneration,
    string ExpectedAnchorSha256, int MaxItems = 100)
{
    public void Validate()
    {
        if (!Enum.IsDefined(Command) || TargetGeneration == Guid.Empty || ExpectedDatabaseGeneration == Guid.Empty ||
            ExpectedDatabaseGeneration == TargetGeneration || MaxItems is < 1 or > 500 ||
            !Fingerprint(ProofReferenceSha256) || !Fingerprint(FencedSendersEvidenceSha256) || !Fingerprint(ExpectedAnchorSha256) ||
            (Command is NotificationRecoveryCommand.FenceBatch or NotificationRecoveryCommand.FenceSourceBatch ? Disposition is null || !Enum.IsDefined(Disposition.Value) : Disposition is not null) ||
            (Command == NotificationRecoveryCommand.EnrollEmpty ? ExpectedDatabaseGeneration is not null : ExpectedDatabaseGeneration is null))
            throw new ArgumentException("Invalid notification recovery request.");
    }
    public static bool Fingerprint(string? value) => value is { Length: 64 } && value.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');
}

public sealed record NotificationRecoveryItem(Guid NotificationId, string DestinationId, long Revision);
public sealed record NotificationRecoveryPlan(Guid OperationId, long Revision, string PreviewHash,
    NotificationRecoveryState State, NotificationRecoveryRequest Request, DateTimeOffset ExpiresAtUtc,
    string RequesterPrincipalId, string RequesterHumanFingerprint, string? ApprovedByPrincipalId,
    string? ApproverHumanFingerprint, string PolicyFingerprint, DateTimeOffset HistoricalCutoffUtc,
    IReadOnlyList<NotificationRecoveryItem> Items, long RemainingUnreconciled, bool ReadyForOperatorActivation,
    string SourceIdentityFingerprint, string SourceInventoryHash, IReadOnlyList<Guid> SourceEventIds);
public sealed record NotificationRecoveryStatus(Guid? DatabaseGeneration, DateTimeOffset? HistoricalCutoffUtc,
    Guid? AdvanceOperationId, long UnresolvedItems);

/// <summary>All writes are explicit operator actions. Startup and status never enroll or advance a generation.</summary>
public interface INotificationRecoveryStore
{
    Task InitializeAsync(CancellationToken token);
    Task<NotificationRecoveryStatus> StatusAsync(CancellationToken token);
    Task<NotificationRecoveryPlan?> GetAsync(Guid operationId, CancellationToken token);
    Task<NotificationRecoveryPlan> PreviewAsync(NotificationRecoveryRequest request, string requester,
        string humanFingerprint, string policyFingerprint, CancellationToken token);
    Task<NotificationRecoveryPlan?> ApproveAsync(Guid operationId, long revision, string previewHash,
        string approver, string humanFingerprint, string policyFingerprint, CancellationToken token);
    Task<NotificationRecoveryPlan?> ApplyAsync(Guid operationId, long revision, string previewHash,
        string requesterHumanFingerprint, string policyFingerprint, Func<bool> currentAuthority, CancellationToken token);
}

/// <summary>Authority is outside the restored database; application code can only read it.</summary>
public interface INotificationRecoveryAuthority
{
    ValueTask VerifyQuarantinedAsync(Guid generation, string anchorSha256, CancellationToken token);
}

public sealed record NotificationRecoverySourceInventory(string SourceIdentityFingerprint, string InventoryHash, IReadOnlyList<Guid> EventIds);
public interface INotificationRecoverySourceInventory
{
    /// <summary>Read-only bounded inventory from the configured source database. Source writers must be fenced during maintenance.</summary>
    Task<NotificationRecoverySourceInventory> ReadAsync(CancellationToken token);
}
