using Kafdeck.Core.Security;
using Microsoft.Extensions.Logging;

namespace Kafdeck.Infrastructure.Security;

public sealed class LoggingSecurityAuditSink : ISecurityAuditSink
{
    private const int MaxAuditValueLength = 256;
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
            Sanitize(auditEvent.Principal),
            Sanitize(auditEvent.SessionCorrelationId),
            Sanitize(auditEvent.ClusterId),
            Sanitize(auditEvent.ResourceName),
            auditEvent.Outcome,
            Sanitize(auditEvent.ReasonCategory),
            auditEvent.TimestampUtc);

        return ValueTask.CompletedTask;
    }

    internal static string? Sanitize(string? value)
    {
        if (value is null)
        {
            return null;
        }

        var bounded = value.Length <= MaxAuditValueLength
            ? value
            : value[..MaxAuditValueLength];

        return bounded
            .Replace("\r", "\\r", StringComparison.Ordinal)
            .Replace("\n", "\\n", StringComparison.Ordinal)
            .Replace("\t", "\\t", StringComparison.Ordinal);
    }
}
