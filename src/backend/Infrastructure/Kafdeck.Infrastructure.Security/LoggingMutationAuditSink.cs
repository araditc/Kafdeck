using Kafdeck.Modules.Administration;
using Microsoft.Extensions.Logging;

namespace Kafdeck.Infrastructure.Security;

public sealed class LoggingMutationAuditSink : IMutationAuditSink
{
    private const int MaxLoggedResources = 8;
    private readonly ILogger<LoggingMutationAuditSink> _logger;

    public LoggingMutationAuditSink(ILogger<LoggingMutationAuditSink> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public ValueTask WriteAsync(
        MutationAuditEvent auditEvent,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(auditEvent);
        cancellationToken.ThrowIfCancellationRequested();

        var resources = string.Join(
            ",",
            auditEvent.ResourceKeys
                .Take(MaxLoggedResources)
                .Select(resource => LoggingSecurityAuditSink.Sanitize(resource) ?? string.Empty));

        _logger.LogInformation(
            "MutationAudit EventType={EventType} OperationId={OperationId} Principal={Principal} ClusterId={ClusterId} OperationKind={OperationKind} RiskClass={RiskClass} State={State} ResourceCount={ResourceCount} Resources={Resources} PreviewHash={PreviewHash} OutcomeCode={OutcomeCode} TimestampUtc={TimestampUtc}",
            auditEvent.EventType,
            auditEvent.OperationId,
            LoggingSecurityAuditSink.Sanitize(auditEvent.PrincipalId),
            LoggingSecurityAuditSink.Sanitize(auditEvent.ClusterId),
            auditEvent.OperationKind,
            auditEvent.RiskClass,
            auditEvent.State,
            auditEvent.ResourceKeys.Count,
            resources,
            LoggingSecurityAuditSink.Sanitize(auditEvent.PreviewHash),
            LoggingSecurityAuditSink.Sanitize(auditEvent.OutcomeCode),
            auditEvent.TimestampUtc);

        return ValueTask.CompletedTask;
    }
}
