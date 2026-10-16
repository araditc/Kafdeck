using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08NotificationHaAdmissionTests
{
    [Theory]
    [InlineData(NotificationPersistenceProvider.PostgreSql,true,true,true)]
    [InlineData(NotificationPersistenceProvider.PostgreSql,false,true,false)]
    [InlineData(NotificationPersistenceProvider.PostgreSql,true,false,false)]
    [InlineData(NotificationPersistenceProvider.Sqlite,true,true,false)]
    public void HA_requires_PostgreSQL_connection_and_explicit_external_interlock(NotificationPersistenceProvider provider,bool anchor,bool connection,bool accepted)
    {
        var options=new NotificationReadOptions(true,provider,NotificationExecutionMode.HighAvailability,
            provider==NotificationPersistenceProvider.Sqlite?Path.GetFullPath("fixture.db"):null,
            connection?SecretReference.Parse("env:KAFDECK_TEST_POSTGRES"):null,
            DestinationProfiles:[new("ops",NotificationProviderKind.Webhook,"Ops",[NotificationEventClass.Operational],new Uri("https://hooks.example.org/notify"))],
            WebhookDeliveryEnabled:true,DeliveryInterlockPath:anchor?Path.GetFullPath("operator-anchor.json"):null,DeliveryInterlockSha256:anchor?new string('a',64):null);
        if(accepted) NotificationPhysicalDeliveryAdmission.Validate(options);
        else Assert.Throws<KafdeckConfigurationException>(()=>NotificationPhysicalDeliveryAdmission.Validate(options));
    }
    [G2PostgreSqlFact]
    public async Task PostgreSql_bounded_due_cursor_admits_later_destination_despite_revoked_front_page_across_independent_workers()
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(25));var token=deadline.Token;
        await using var database=await G2CliPostgreSqlDatabase.CreateAsync();
        var left=new AdoNotificationDeliveryStore(new PostgreSqlNotificationDeliveryDbConnectionFactory(database.ConnectionString));
        var right=new AdoNotificationDeliveryStore(new PostgreSqlNotificationDeliveryDbConnectionFactory(database.ConnectionString));
        await left.InitializeAsync(token);await right.InitializeAsync(token);
        var now=(await left.GetCoordinationUtcNowAsync(DateTimeOffset.UtcNow,token)).AddSeconds(-1);
        for(var i=1;i<=6;i++)
            await left.CreateOrGetAsync(new(Guid.Parse("00000000-0000-0000-0000-"+i.ToString("D12")),"blocked",new string('a',64),
                NotificationDeliveryState.Pending,0,now,routedProfileRevisionFingerprint:new string('b',64)),now,token);
        var active=Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff");
        await left.CreateOrGetAsync(new(active,"active",new string('a',64),NotificationDeliveryState.Pending,0,now,routedProfileRevisionFingerprint:new string('b',64)),now,token);
        var dispatcher=new Recorder();var gate=new ClaimGate();
        var a=new NotificationDeliveryWorker(left,dispatcher,workerPolicy:new(maxDuePerCycle:2),claimGate:gate);
        var b=new NotificationDeliveryWorker(right,dispatcher,workerPolicy:new(maxDuePerCycle:2),claimGate:gate);
        for(var i=0;i<4;i++) { await a.RunDueCycleAsync(token);await b.RunDueCycleAsync(token); }
        var row=await right.GetAsync(active,"active",token);Assert.NotNull(row);
        Assert.Equal(NotificationDeliveryState.Delivered,row.Snapshot.State);Assert.Equal(1,row.Snapshot.AttemptCount);Assert.Equal(1,dispatcher.Calls);
        var blocked=await left.GetAsync(Guid.Parse("00000000-0000-0000-0000-000000000001"),"blocked",token);
        Assert.NotNull(blocked);Assert.Equal(0,blocked.Snapshot.AttemptCount);
        var directory=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY");
        if(directory is not null)
        {
            Directory.CreateDirectory(directory);
            await File.WriteAllTextAsync(Path.Combine(directory,"postgres-governance-fairness-evidence.json"),
                "{\"actualPostgreSql\":true,\"independentWorkers\":2,\"frontPendingIntents\":6,\"pageLimit\":2,\"laterDestinationDelivered\":true,\"blockedAttempts\":0,\"activeAttempts\":1,\"physicalProviderProof\":false}",token);
        }
    }
    private sealed class ClaimGate:INotificationDeliveryClaimGate
    { public ValueTask<bool> CanClaimAsync(string destinationId,CancellationToken token)=>ValueTask.FromResult(destinationId=="active"); }
    private sealed class Recorder:INotificationDeliveryDispatcher
    {
        private int _calls;internal int Calls=>Volatile.Read(ref _calls);
        public Task<NotificationDeliveryDispatchResult> DispatchAsync(NotificationDeliveryRecord row,CancellationToken token)
        { Interlocked.Increment(ref _calls);return Task.FromResult(new NotificationDeliveryDispatchResult(NotificationDeliveryDispatchOutcome.Delivered)); }
    }
}
