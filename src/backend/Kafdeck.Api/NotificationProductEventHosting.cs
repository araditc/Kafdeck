using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Kafdeck.Api;

public static class NotificationProductEventHosting
{
    public static bool ReadActivation(IConfiguration configuration, KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(options);
        var raw = configuration["Kafdeck:Notifications:ProductEventsEnabled"];
        if (raw is null) return false;
        if (!bool.TryParse(raw, out var enabled))
            throw new KafdeckConfigurationException("Notification ProductEventsEnabled must be an explicit boolean.");
        if (!enabled) return false;
        if (options.Notifications?.Enabled != true || options.Administration?.Mutations.Enabled != true ||
            options.Notifications.ExecutionMode != NotificationExecutionMode.Standalone ||
            options.Administration.Mutations.Persistence?.ExecutionMode != MutationExecutionMode.Standalone)
            throw new KafdeckConfigurationException("Product notification capture requires enabled notification observation and mutations with standalone persistence.");
        return true;
    }

    public static IServiceCollection AddKafdeckNotificationProductEvents(this IServiceCollection services, bool enabled)
    {
        if (!enabled) return services;
        services.TryAddSingleton(new NotificationProductEventPolicy());
        services.TryAddSingleton<INotificationProductEventOutbox>(provider => new MutationNotificationOutbox(
            provider.GetRequiredService<IMutationDbConnectionFactory>(), provider.GetRequiredService<NotificationProductEventPolicy>()));
        services.TryAddSingleton<INotificationProductEventBackpressure, NotificationProductEventBackpressure>();
        services.TryAddSingleton<NotificationRoutingCoordinator>();
        services.TryAddSingleton<INotificationRecoverySourceEvidence>(sp => new AdoNotificationRecoveryStore(
            sp.GetRequiredService<INotificationDeliveryDbConnectionFactory>()));
        services.TryAddSingleton<NotificationProductEventPump>();
        services.AddHostedService<NotificationProductEventHostedService>();
        return services;
    }

    public static WebApplication MapKafdeckNotificationProductEventStatus(this WebApplication app)
    {
        app.MapGet("/api/v1/notifications/product-events/status", async (HttpContext context,
            KafdeckAuthorizationService authorization, INotificationProductEventOutbox outbox, CancellationToken cancellationToken) =>
        {
            // This is deployment-wide aggregate status. A subscription-scoped
            // reader or manage-only resource grant must not obtain global counts.
            var outcome = authorization.Authorize(context.User, new AuthorizationRequest(AuthorizationAction.NotificationManage));
            if (outcome != KafdeckAuthorizationOutcome.Allowed)
                return Results.Problem(statusCode: outcome == KafdeckAuthorizationOutcome.Unauthenticated ? 401 : 403,
                    title: "Product notification source status is not authorized.");
            return Results.Ok(await outbox.GetStatusAsync(cancellationToken).ConfigureAwait(false));
        }).WithName("v08-notification-product-event-status");
        return app;
    }
}

public sealed class NotificationProductEventHostedService(NotificationProductEventPump pump,
    IMutationOperationRepository repository, INotificationProductEventOutbox outbox,
    ILogger<NotificationProductEventHostedService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await repository.InitializeAsync(stoppingToken).ConfigureAwait(false);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        var lastExpired = 0L;
        var lastOverflow = 0L;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await pump.RunCycleAsync(stoppingToken).ConfigureAwait(false);
                var status = await outbox.GetStatusAsync(stoppingToken).ConfigureAwait(false);
                if (status.Expired != lastExpired || status.Overflowed != lastOverflow)
                    logger.LogWarning("Product notification source reports {Expired} expired and {Overflowed} overflowed events; inspect authorized source status.",
                        status.Expired, status.Overflowed);
                lastExpired = status.Expired;
                lastOverflow = status.Overflowed;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception)
            {
                // No product intent, principal, secret, raw source identity or
                // provider error appears in notification-source diagnostics.
                logger.LogWarning("Product notification source cycle failed; durable unacknowledged events remain for bounded recovery.");
            }
            if (!await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false)) break;
        }
    }
}
