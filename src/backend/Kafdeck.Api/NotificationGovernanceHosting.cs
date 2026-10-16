using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;

namespace Kafdeck.Api;

public static class NotificationGovernanceHosting
{
    /// <summary>Call after base notification services, before Build. Does not enable any sender.</summary>
    public static IServiceCollection AddKafdeckNotificationGovernance(this IServiceCollection services,NotificationGovernanceOptions options)
    {
        services.AddSingleton(options);
        services.AddSingleton<INotificationGovernanceStore,AdoNotificationGovernanceStore>();
        services.AddSingleton<GovernedNotificationCatalog>();
        // The final registrations replace the static catalog/resolver at their
        // existing injection points. The restore activation gate stays intact.
        services.AddSingleton<INotificationDestinationProfileCatalog>(sp=>sp.GetRequiredService<GovernedNotificationCatalog>());
        services.AddSingleton<INotificationCredentialResolver>(sp=>sp.GetRequiredService<GovernedNotificationCatalog>());
        services.AddSingleton<INotificationDeliveryClaimGate>(sp=>sp.GetRequiredService<GovernedNotificationCatalog>());
        services.AddSingleton<INotificationPhysicalRevisionGate>(sp=>sp.GetRequiredService<GovernedNotificationCatalog>());
        return services;
    }
    public static Task InitializeKafdeckNotificationGovernanceAsync(this IServiceProvider services,CancellationToken token=default) =>
        services.GetRequiredService<INotificationGovernanceStore>().InitializeAsync(token);
}
