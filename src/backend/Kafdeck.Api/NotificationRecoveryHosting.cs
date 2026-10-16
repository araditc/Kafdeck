using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;

namespace Kafdeck.Api;

public static class NotificationRecoveryHosting
{
    public static IServiceCollection AddKafdeckNotificationRecovery(this IServiceCollection services, NotificationRecoveryOptions options)
    {
        services.AddSingleton(options);
        services.AddSingleton<INotificationRecoveryAuthority, NotificationRecoveryAuthority>();
        services.AddSingleton<INotificationRecoveryStore, AdoNotificationRecoveryStore>();
        return services;
    }

    public static Task InitializeKafdeckNotificationRecoveryAsync(this IServiceProvider services, CancellationToken token = default) =>
        services.GetRequiredService<INotificationRecoveryStore>().InitializeAsync(token);
}
