using Kafdeck.Api;
using Kafdeck.Core.Attribution;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kafdeck.Architecture.Tests;

// Controlled source faults exercise orchestration; real HTTPS acquisition has separate live evidence.
public sealed class V09W76AttributionCollectionTests
{
    private const string PrivateSentinel="private-source-credential-and-response-sentinel";
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Failed_first_source_or_internal_deadline_cannot_starve_later_healthy_source(bool internalDeadline)
    {
        await using var fixture=await Fixture.CreateAsync();var logger=new CapturingLogger<AttributionCollectionService>();
        var failed=new FailingSource(internalDeadline?new OperationCanceledException(PrivateSentinel):new HttpRequestException(PrivateSentinel));
        var window=V09W76AttributionTests.Window("17") with{SourceId="healthy"};var healthy=new HealthySource(window);
        var collector=new AttributionCollectionService([failed,healthy],fixture.Store,fixture.Catalog,logger);
        Assert.Equal(1,await collector.CollectAsync(CancellationToken.None));Assert.Equal(1,failed.Calls);Assert.Equal(1,healthy.Calls);
        var stored=Assert.Single(await fixture.Store.QueryAuthorizedAsync(new("permitted",window.WindowStartUtc,window.WindowEndUtc,window.ResourceId,50),_=>true));
        Assert.Equal("17",stored.Quantity);Assert.Equal("healthy",stored.SourceId);
        var diagnostic=Assert.Single(logger.Entries);Assert.Equal(7601,diagnostic.Id);Assert.Null(diagnostic.Error);Assert.DoesNotContain(PrivateSentinel,diagnostic.Message,StringComparison.Ordinal);
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Retention_runs_even_when_every_source_or_collection_enumeration_fails(bool enumerationFailure)
    {
        await using var fixture=await Fixture.CreateAsync();var expired=Expired();await fixture.Store.AppendAsync([expired]);
        var sourceLogger=new CapturingLogger<AttributionCollectionService>();var hostedLogger=new CapturingLogger<AttributionCollectionHostedService>();
        var failed=new FailingSource(new HttpRequestException(PrivateSentinel));
        IEnumerable<IAttributionMeasurementSource> sequence=enumerationFailure?BrokenSequence(failed):[failed];
        var collection=new AttributionCollectionService(sequence,fixture.Store,fixture.Catalog,sourceLogger);
        using var service=new AttributionCollectionHostedService(collection,fixture.Store,new AttributionRuntimeOptions{RetentionDays=90},hostedLogger);
        await service.RunCycleAsync(CancellationToken.None);
        Assert.Equal(1,failed.Calls);Assert.Empty(await fixture.Store.QueryAuthorizedAsync(new("permitted",expired.WindowStartUtc,expired.WindowEndUtc,expired.ResourceId,50),_=>true));
        Assert.Equal(enumerationFailure?1:0,hostedLogger.Entries.Count);
        foreach(var diagnostic in sourceLogger.Entries.Concat(hostedLogger.Entries)){Assert.Null(diagnostic.Error);Assert.DoesNotContain(PrivateSentinel,diagnostic.Message,StringComparison.Ordinal);}
    }
    [Fact]
    public async Task Caller_cancellation_propagates_without_collecting_later_sources_or_running_retention()
    {
        await using var fixture=await Fixture.CreateAsync();var expired=Expired();await fixture.Store.AppendAsync([expired]);
        using var caller=new CancellationTokenSource();var cancels=new CancellingSource(caller);var healthy=new HealthySource(V09W76AttributionTests.Window("19"));
        var sourceLogger=new CapturingLogger<AttributionCollectionService>();var hostedLogger=new CapturingLogger<AttributionCollectionHostedService>();
        var collection=new AttributionCollectionService([cancels,healthy],fixture.Store,fixture.Catalog,sourceLogger);
        using var service=new AttributionCollectionHostedService(collection,fixture.Store,new AttributionRuntimeOptions{RetentionDays=90},hostedLogger);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>service.RunCycleAsync(caller.Token));
        Assert.Equal(1,cancels.Calls);Assert.Equal(0,healthy.Calls);Assert.Empty(sourceLogger.Entries);Assert.Empty(hostedLogger.Entries);
        Assert.Single(await fixture.Store.QueryAuthorizedAsync(new("permitted",expired.WindowStartUtc,expired.WindowEndUtc,expired.ResourceId,50),_=>true));
        var unused=new HealthySource(V09W76AttributionTests.Window("23"));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>new AttributionCollectionService([unused],fixture.Store,fixture.Catalog).CollectAsync(caller.Token));Assert.Equal(0,unused.Calls);
    }
    private static IEnumerable<IAttributionMeasurementSource> BrokenSequence(IAttributionMeasurementSource first)
    {yield return first;throw new InvalidOperationException(PrivateSentinel);}
    private static MeasurementWindow Expired()
    {
        var old=DateTimeOffset.UtcNow.AddDays(-100);return V09W76AttributionTests.Window("7") with{SourceId="retention-fixture",ResourceId="expired-orders",WindowStartUtc=old.AddMinutes(-2),WindowEndUtc=old.AddMinutes(-1),CollectedAtUtc=old};
    }
    private sealed class HealthySource(MeasurementWindow window):IAttributionMeasurementSource
    {
        public int Calls{get;private set;}
        public AttributionSourceIdentity Identity{get;}=new(window.SourceId,window.ClusterId,window.Provider);
        public Task<IReadOnlyList<MeasurementWindow>> AcquireAsync(CancellationToken token){token.ThrowIfCancellationRequested();Calls++;return Task.FromResult<IReadOnlyList<MeasurementWindow>>([window]);}
    }
    private sealed class FailingSource(Exception error):IAttributionMeasurementSource
    {
        public int Calls{get;private set;}
        public AttributionSourceIdentity Identity{get;}=new("failed","permitted","KafdeckMeasuredCountersV1");
        public Task<IReadOnlyList<MeasurementWindow>> AcquireAsync(CancellationToken token){Calls++;return Task.FromException<IReadOnlyList<MeasurementWindow>>(error);}
    }
    private sealed class CancellingSource(CancellationTokenSource caller):IAttributionMeasurementSource
    {
        public int Calls{get;private set;}
        public AttributionSourceIdentity Identity{get;}=new("cancelled","permitted","KafdeckMeasuredCountersV1");
        public Task<IReadOnlyList<MeasurementWindow>> AcquireAsync(CancellationToken token){Calls++;caller.Cancel();return Task.FromCanceled<IReadOnlyList<MeasurementWindow>>(token);}
    }
    private sealed class CapturingLogger<T>:ILogger<T>
    {
        public List<(int Id,string Message,Exception? Error)> Entries{get;}=[];
        public IDisposable? BeginScope<TState>(TState state)where TState:notnull=>null;
        public bool IsEnabled(LogLevel level)=>true;
        public void Log<TState>(LogLevel level,EventId id,TState state,Exception? exception,Func<TState,Exception?,string> formatter)=>Entries.Add((id.Id,formatter(state,exception),exception));
    }
    private sealed class Fixture(string path,AdoAttributionStore store,AdoCatalogStore catalog):IAsyncDisposable
    {
        public AdoAttributionStore Store{get;}=store;
        public AdoCatalogStore Catalog{get;}=catalog;
        public static async Task<Fixture> CreateAsync()
        {
            var path=Path.Combine(Path.GetTempPath(),"w76-collection-"+Guid.NewGuid().ToString("N")+".db");var factory=new SqliteMutationDbConnectionFactory(path);
            var store=new AdoAttributionStore(factory);await store.InitializeAsync();var catalog=new AdoCatalogStore(factory,"trusted-collection-fixture");await catalog.InitializeAsync(CancellationToken.None);
            return new(path,store,catalog);
        }
        public ValueTask DisposeAsync(){SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");return ValueTask.CompletedTask;}
    }
}
