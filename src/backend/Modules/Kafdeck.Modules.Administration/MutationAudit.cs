namespace Kafdeck.Modules.Administration;

public enum MutationAuditEventType
{
    PreviewCreated = 1,
    Confirmed = 2,
    ApprovalGranted = 3,
    ApprovalRejected = 4,
    ExecutionClaimed = 5,
    DispatchStarted = 6,
    Completed = 7,
    Cancelled = 8,
    Expired = 9,
    StalePreview = 10,
    IdempotencyConflict = 11,
    ResourceConflict = 12,
}

public sealed record MutationAuditEvent(
    DateTimeOffset TimestampUtc,
    MutationAuditEventType EventType,
    Guid OperationId,
    string PrincipalId,
    string ClusterId,
    MutationOperationKind OperationKind,
    MutationRiskClass RiskClass,
    MutationOperationState State,
    IReadOnlyList<string> ResourceKeys,
    string PreviewHash,
    string OutcomeCode);

public interface IMutationAuditSink
{
    ValueTask WriteAsync(
        MutationAuditEvent auditEvent,
        CancellationToken cancellationToken = default);
}
