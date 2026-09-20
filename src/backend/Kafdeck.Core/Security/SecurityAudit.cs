namespace Kafdeck.Core.Security;

public enum SecurityAuditEventType
{
    LoginSucceeded = 1,
    LoginFailed = 2,
    Logout = 3,
    AuthorizationDenied = 4,
    SensitiveRead = 5,
    LegacyTokenRequest = 6,
}

public enum SecurityAuditOutcome
{
    Succeeded = 1,
    Denied = 2,
    Failed = 3,
}

public sealed record SecurityAuditEvent
{
    public SecurityAuditEvent(
        DateTimeOffset timestampUtc,
        SecurityAuditEventType eventType,
        string principal,
        string? sessionCorrelationId,
        string? clusterId,
        string? resourceName,
        SecurityAuditOutcome outcome,
        string reasonCategory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(principal);
        ArgumentException.ThrowIfNullOrWhiteSpace(reasonCategory);
        TimestampUtc = timestampUtc;
        EventType = eventType;
        Principal = principal;
        SessionCorrelationId = sessionCorrelationId;
        ClusterId = clusterId;
        ResourceName = resourceName;
        Outcome = outcome;
        ReasonCategory = reasonCategory;
    }

    public DateTimeOffset TimestampUtc { get; }
    public SecurityAuditEventType EventType { get; }
    public string Principal { get; }
    public string? SessionCorrelationId { get; }
    public string? ClusterId { get; }
    public string? ResourceName { get; }
    public SecurityAuditOutcome Outcome { get; }
    public string ReasonCategory { get; }
}

public interface ISecurityAuditSink
{
    ValueTask WriteAsync(SecurityAuditEvent auditEvent, CancellationToken cancellationToken = default);
}

public static class SecurityAuditPrincipal
{
    public const string Anonymous = "anonymous";
    public const string LegacyDeployment = "legacy-deployment";

    public static string FromOperator(OperatorIdentity identity) =>
        $"oidc:{identity.Key.Issuer}|{identity.Key.Subject}";
}
