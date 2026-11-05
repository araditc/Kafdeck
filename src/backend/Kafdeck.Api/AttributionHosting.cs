using Kafdeck.Core.Attribution;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public static class AttributionHosting
{
    public static IServiceCollection AddKafdeckAttribution(this IServiceCollection services,AttributionRuntimeOptions options,bool collect=true)
    {
        options.Validate();services.AddSingleton(options);
        services.AddSingleton<AdoAttributionStore>(p=>new(p.GetRequiredService<IMutationDbConnectionFactory>(),p.GetRequiredService<IGovernanceGenerationTransactionGate>(),p.GetRequiredService<IGovernancePolicyTransactionGate>(),p.GetRequiredService<AdoCatalogStore>()));
        services.AddSingleton<IAttributionStore>(p=>p.GetRequiredService<AdoAttributionStore>());
        services.AddSingleton<AttributionMutationService>();
        foreach(var kind in new[]{MutationOperationKind.RateCardVersion,MutationOperationKind.RateCardActivation})
            services.AddSingleton<IMutationExecutionHandler>(p=>new AttributionExecutionHandler(kind,p.GetRequiredService<AttributionMutationService>()));
        foreach(var source in options.Sources)
            services.AddSingleton<IAttributionMeasurementSource>(p=>new ConfiguredAttributionMeasurementSource(source,
                new NotificationDnsEndpointResolver(new SystemNotificationHostAddressLookup(),NotificationAddressPolicy.NoConfiguredNat64),
                new NotificationPinnedHttpHandlerFactory(new SocketNotificationPinnedTcpDialer(),NotificationAddressPolicy.NoConfiguredNat64),p.GetRequiredService<SecretResolver>()));
        services.AddSingleton<AttributionCollectionService>();
        if(collect&&options.Sources.Count>0)services.AddHostedService<AttributionCollectionHostedService>();
        return services;
    }
}

public sealed class AttributionCollectionService(IEnumerable<IAttributionMeasurementSource> sources,IAttributionStore store,ICatalogStore catalog)
{
    public async Task<int> CollectAsync(CancellationToken token)
    {
        var count=0;
        foreach(var source in sources)
        {
            var acquired=await source.AcquireAsync(token).ConfigureAwait(false);var resolved=new List<MeasurementWindow>(acquired.Count);
            foreach(var window in acquired)resolved.Add(window with{Ownership=await OwnershipAsync(window,token).ConfigureAwait(false)});
            await store.AppendAsync(resolved,token).ConfigureAwait(false);count=checked(count+resolved.Count);
        }
        return count;
    }
    public async Task<MeasurementOwnership> OwnershipAsync(MeasurementWindow window,CancellationToken token)
    {
        var current=await catalog.FindTopicAsync(window.ClusterId,window.ResourceId,token).ConfigureAwait(false);
        if(current is null||window.ResourceIdentity is null)return new(null,null,null,window.ResourceIdentity,"Missing");
        var rows=new List<CatalogHistoryItem>();long after=0;
        do
        {
            var page=await catalog.HistoryAsync(window.ClusterId,CatalogEntityKind.Topic,current.Id,after,200,token).ConfigureAwait(false);
            rows.AddRange(page);if(page.Count<200)break;after=page[^1].Record.Revision;
            if(rows.Count>=CatalogContract.MaxRevisionsPerEntity)break;
        }while(true);
        var overlapping=rows.Where(h=>h.EffectiveFromUtc<window.WindowEndUtc&&(h.EffectiveToUtc is null||h.EffectiveToUtc>window.WindowStartUtc)).ToArray();
        var full=overlapping.Where(h=>h.EffectiveFromUtc<=window.WindowStartUtc&&(h.EffectiveToUtc is null||h.EffectiveToUtc>=window.WindowEndUtc)&&!h.Record.Archived&&h.Record.TopicIdentity==window.ResourceIdentity).ToArray();
        if(full.Length!=1||overlapping.Length!=1)return new(null,null,null,window.ResourceIdentity,"Ambiguous");
        var row=full[0].Record;return new(row.Revision,row.TeamId,row.ApplicationId,row.TopicIdentity,"Observed");
    }
}
public sealed class AttributionCollectionHostedService(AttributionCollectionService collection,AdoAttributionStore store,
    AttributionRuntimeOptions options,ILogger<AttributionCollectionHostedService> logger):BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer=new PeriodicTimer(TimeSpan.FromSeconds(options.PollSeconds));
        do
        {
            try{await collection.CollectAsync(stoppingToken).ConfigureAwait(false);await store.DeleteExpiredMeasurementsAsync(options.RetentionDays,DateTimeOffset.UtcNow,stoppingToken).ConfigureAwait(false);}
            catch(OperationCanceledException)when(stoppingToken.IsCancellationRequested){break;}
            catch(Exception){logger.LogWarning("Attribution measurement collection is unavailable; no synthetic measurements were inserted.");}
        }while(await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }
}
