using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Kafdeck.Api;

/// <summary>Explicit, default-off admission of standalone typed Webhook delivery.</summary>
public static class NotificationWebhookDeliveryHosting
{
    public static IServiceCollection AddKafdeckWebhookDelivery(
        this IServiceCollection services, NotificationReadOptions options)
    {
        if (!options.WebhookDeliveryEnabled && !options.HttpProviderDeliveryEnabled && !options.EmailDeliveryEnabled) return services;
        NotificationPhysicalDeliveryAdmission.Validate(options);

        services.TryAddSingleton(NotificationAddressPolicy.NoConfiguredNat64);
        services.TryAddSingleton<INotificationHostAddressLookup, SystemNotificationHostAddressLookup>();
        services.TryAddSingleton<INotificationPinnedTcpDialer, SocketNotificationPinnedTcpDialer>();
        services.TryAddSingleton<INotificationEndpointResolutionPort, NotificationDnsEndpointResolver>();
        services.TryAddSingleton<NotificationPinnedHttpHandlerFactory>();
        services.TryAddSingleton<INotificationPinnedWebhookTransport, ConfiguredWebhookTransport>();
        services.TryAddSingleton<WebhookNotificationAdapter>();
        services.TryAddSingleton<WebhookNotificationDeliveryDispatcher>();
        if (options.HttpProviderDeliveryEnabled || options.EmailDeliveryEnabled)
        {
            services.TryAddSingleton<ConfiguredHttpProviderTransports>();
            services.TryAddSingleton<INotificationSlackTransport>(sp => sp.GetRequiredService<ConfiguredHttpProviderTransports>());
            services.TryAddSingleton<INotificationTeamsTransport>(sp => sp.GetRequiredService<ConfiguredHttpProviderTransports>());
            services.TryAddSingleton<INotificationTelegramTransport>(sp => sp.GetRequiredService<ConfiguredHttpProviderTransports>());
            services.TryAddSingleton<INotificationPagerDutyTransport>(sp => sp.GetRequiredService<ConfiguredHttpProviderTransports>());
            services.TryAddSingleton<INotificationEmailTransport, ConfiguredEmailTransport>();
            services.TryAddSingleton<EmailNotificationAdapter>();
            services.TryAddSingleton<SlackNotificationAdapter>();
            services.TryAddSingleton<TeamsNotificationAdapter>();
            services.TryAddSingleton<TelegramNotificationAdapter>();
            services.TryAddSingleton<PagerDutyNotificationAdapter>();
            services.TryAddSingleton<INotificationDeliveryDispatcher, NotificationProviderDeliveryDispatcher>();
        }
        else services.TryAddSingleton<INotificationDeliveryDispatcher>(sp => sp.GetRequiredService<WebhookNotificationDeliveryDispatcher>());
        services.TryAddSingleton<NotificationDeliveryWorker>();
        services.TryAddSingleton<NotificationRoutingCoordinator>();
        services.AddHostedService<NotificationWebhookDeliveryHostedService>();
        return services;
    }

    private sealed class ConfiguredHttpProviderTransports(INotificationDestinationProfileCatalog profiles,
        INotificationEndpointResolutionPort resolver, NotificationPinnedHttpHandlerFactory handlers) :
        INotificationSlackTransport, INotificationTeamsTransport, INotificationTelegramTransport, INotificationPagerDutyTransport
    {
        Task<NotificationProviderTransportResult> INotificationSlackTransport.SendAsync(NotificationBoundTransportRequest request, CancellationToken token) => SendAsync(NotificationProviderKind.Slack, request, token);
        Task<NotificationProviderTransportResult> INotificationTeamsTransport.SendAsync(NotificationBoundTransportRequest request, CancellationToken token) => SendAsync(NotificationProviderKind.MicrosoftTeams, request, token);
        Task<NotificationProviderTransportResult> INotificationTelegramTransport.SendAsync(NotificationBoundTransportRequest request, CancellationToken token) => SendAsync(NotificationProviderKind.Telegram, request, token);
        Task<NotificationProviderTransportResult> INotificationPagerDutyTransport.SendAsync(NotificationBoundTransportRequest request, CancellationToken token) => SendAsync(NotificationProviderKind.PagerDuty, request, token);
        private async Task<NotificationProviderTransportResult> SendAsync(NotificationProviderKind provider,
            NotificationBoundTransportRequest request, CancellationToken token)
        {
            if (request.Provider != provider) throw new KafdeckConfigurationException("Typed provider transport family mismatch.");
            var profile = await profiles.GetAsync(request.DestinationId, token).ConfigureAwait(false)
                ?? throw new KafdeckConfigurationException("Provider destination was removed.");
            return await new NotificationPinnedProviderHttpTransport(profile, resolver, handlers).SendAsync(request, token).ConfigureAwait(false);
        }
    }

    private sealed class ConfiguredEmailTransport(INotificationDestinationProfileCatalog profiles,
        INotificationEndpointResolutionPort resolver, NotificationPinnedHttpHandlerFactory connections) : INotificationEmailTransport
    {
        public async Task<NotificationProviderTransportResult> SendAsync(NotificationEmailTransportRequest request, CancellationToken token)
        {
            var profile = await profiles.GetAsync(request.DestinationId, token).ConfigureAwait(false)
                ?? throw new KafdeckConfigurationException("Email destination was removed.");
            return await new NotificationPinnedSmtpTransport(profile, resolver, connections).SendAsync(request, token).ConfigureAwait(false);
        }
    }

    private sealed class ConfiguredWebhookTransport(
        INotificationDestinationProfileCatalog profiles,
        NotificationPinnedHttpHandlerFactory handlers) : INotificationPinnedWebhookTransport
    {
        public async Task<NotificationWebhookTransportResult> SendAsync(
            NotificationPinnedWebhookRequest request, CancellationToken cancellationToken)
        {
            var profile = await profiles.GetAsync(request.DestinationId, cancellationToken)
                .ConfigureAwait(false) ?? throw new KafdeckConfigurationException("Notification destination was removed.");
            return await new NotificationPinnedWebhookHttpTransport(profile, handlers)
                .SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
    }
}

public sealed class NotificationWebhookDeliveryHostedService(
    NotificationDeliveryWorker worker,
    INotificationDeliveryStore deliveries,
    INotificationRoutingStore routing,
    ILogger<NotificationWebhookDeliveryHostedService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Also initialize here so this hosted service is safe in a laboratory
        // Generic Host. The API's fail-closed schema check still runs first.
        await deliveries.InitializeAsync(stoppingToken).ConfigureAwait(false);
        await routing.InitializeAsync(stoppingToken).ConfigureAwait(false);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await worker.RecoverStaleInFlightAsync(stoppingToken).ConfigureAwait(false);
                await worker.RunDueCycleAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception)
            {
                // Exception text may include provider addresses or secret locators.
                // Preserve durable InFlight rows for conservative recovery.
                logger.LogError("Notification delivery cycle failed; durable recovery will classify unfinished sends.");
            }
            try
            {
                if (!await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false)) break;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }
}
