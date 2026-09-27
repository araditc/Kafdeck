using Kafdeck.Modules.Connect;

namespace Kafdeck.Api;

public sealed class LoggingConnectAutoRestartAuditSink :
    IConnectAutoRestartAuditSink
{
    private readonly ILogger<LoggingConnectAutoRestartAuditSink> _logger;

    public LoggingConnectAutoRestartAuditSink(
        ILogger<LoggingConnectAutoRestartAuditSink> logger)
    {
        _logger =
            logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public ValueTask WriteAsync(
        ConnectAutoRestartAuditEvent auditEvent,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(auditEvent);
        cancellationToken.ThrowIfCancellationRequested();

        _logger.LogInformation(
            "Connect auto-restart audit: EventType={EventType} ActivationId={ActivationId} DispatchId={DispatchId} Target={Target} AutomationPrincipal={AutomationPrincipal} Attempt={Attempt} Code={Code} TimestampUtc={TimestampUtc}",
            auditEvent.EventType,
            auditEvent.ActivationId,
            auditEvent.DispatchId,
            auditEvent.TargetKey,
            auditEvent.AutomationPrincipalId,
            auditEvent.AttemptNumber,
            auditEvent.Code,
            auditEvent.TimestampUtc);

        return ValueTask.CompletedTask;
    }
}
