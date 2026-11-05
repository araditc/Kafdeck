using Kafdeck.Core.Catalog;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public static class CatalogHosting
{
    public static IServiceCollection AddKafdeckCatalog(this IServiceCollection services, string deploymentGeneration)
    {
        services.AddSingleton<AdoCatalogStore>(provider => new(provider.GetRequiredService<IMutationDbConnectionFactory>(), deploymentGeneration,
            provider.GetRequiredService<IGovernanceGenerationTransactionGate>()));
        services.AddSingleton<ICatalogStore>(provider => provider.GetRequiredService<AdoCatalogStore>());
        services.AddSingleton<IPolicyMetadataTransactionReader>(provider => provider.GetRequiredService<AdoCatalogStore>());
        services.AddSingleton<ICatalogTopicIdentityPort>(provider => new ConfluentCatalogTopicIdentityPort(
            provider.GetRequiredService<KafdeckOptions>().Clusters, provider.GetRequiredService<SecretResolver>()));
        services.AddSingleton<CatalogMutationHandler>();
        services.AddSingleton<IMutationExecutionHandler>(provider => provider.GetRequiredService<CatalogMutationHandler>());
        return services;
    }
}
