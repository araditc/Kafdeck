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

public sealed record SecurityAuditEvent(
    DateTimeOffset TimestampUtc,
    SecurityAuditEventType EventType,
    string Principal,
    string? SessionCorrelationId,
    string? ClusterId,
    string? ResourceName,
    SecurityAuditOutcome Outcome,
    string ReasonCategory)
{
    public SecurityAuditEvent
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(Principal);
        ArgumentException.ThrowIfNullOrWhiteSpace(ReasonCategory);
    }
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
