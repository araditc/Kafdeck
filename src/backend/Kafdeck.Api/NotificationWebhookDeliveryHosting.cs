using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Kafdeck.Api;

/// <summary>Explicit, default-off admission of standalone anonymous Webhook delivery.</summary>
public static class NotificationWebhookDeliveryHosting
{
    public static IServiceCollection AddKafdeckWebhookDelivery(
        this IServiceCollection services, NotificationReadOptions options)
    {
        if (!options.WebhookDeliveryEnabled) return services;
        if (!options.Enabled || options.ExecutionMode != NotificationExecutionMode.Standalone ||
            options.DestinationProfiles is not { Count: > 0 } ||
            options.DestinationProfiles.Any(profile => profile is null || profile.Provider != NotificationProviderKind.Webhook ||
                profile.CredentialBindingId is not null))
            throw new KafdeckConfigurationException("Physical Webhook admission requires standalone persistence and anonymous Webhook profiles.");

        services.TryAddSingleton(NotificationAddressPolicy.NoConfiguredNat64);
        services.TryAddSingleton<INotificationHostAddressLookup, SystemNotificationHostAddressLookup>();
        services.TryAddSingleton<INotificationPinnedTcpDialer, SocketNotificationPinnedTcpDialer>();
        services.TryAddSingleton<INotificationEndpointResolutionPort, NotificationDnsEndpointResolver>();
        services.TryAddSingleton<NotificationPinnedHttpHandlerFactory>();
        services.TryAddSingleton<INotificationPinnedWebhookTransport, ConfiguredWebhookTransport>();
        services.TryAddSingleton<WebhookNotificationAdapter>();
        services.TryAddSingleton<INotificationDeliveryDispatcher, WebhookNotificationDeliveryDispatcher>();
        services.TryAddSingleton<NotificationDeliveryWorker>();
        services.TryAddSingleton<NotificationRoutingCoordinator>();
        services.AddHostedService<NotificationWebhookDeliveryHostedService>();
        return services;
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
                logger.LogError("Notification Webhook cycle failed; durable recovery will classify unfinished sends.");
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
