using Kafdeck.Core.Security;
using Microsoft.Extensions.Logging;

namespace Kafdeck.Infrastructure.Security;

public sealed class LoggingSecurityAuditSink : ISecurityAuditSink
{
    private readonly ILogger<LoggingSecurityAuditSink> _logger;

    public LoggingSecurityAuditSink(ILogger<LoggingSecurityAuditSink> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public ValueTask WriteAsync(SecurityAuditEvent auditEvent, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(auditEvent);
        cancellationToken.ThrowIfCancellationRequested();

        _logger.LogInformation(
            "SecurityAudit EventType={EventType} Principal={Principal} SessionCorrelationId={SessionCorrelationId} ClusterId={ClusterId} ResourceName={ResourceName} Outcome={Outcome} ReasonCategory={ReasonCategory} TimestampUtc={TimestampUtc}",
            auditEvent.EventType,
            auditEvent.Principal,
            auditEvent.SessionCorrelationId,
            auditEvent.ClusterId,
            auditEvent.ResourceName,
            auditEvent.Outcome,
            auditEvent.ReasonCategory,
            auditEvent.TimestampUtc);

        return ValueTask.CompletedTask;
    }
}
