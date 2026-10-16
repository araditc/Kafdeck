using System.Net.Security;
using System.Text.Json;
using Kafdeck.Core.Notifications;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using TlsProvider=Kafdeck.Architecture.Tests.V08W66HostedWebhookTlsTests.TlsProvider;

namespace Kafdeck.Architecture.Tests;

public sealed partial class V08NotificationGovernancePostgreSqlTests
{
    [PostgreSqlGovernanceTheory]
    [InlineData(NotificationProviderKind.Webhook,false)]
    [InlineData(NotificationProviderKind.Email,false)]
    [InlineData(NotificationProviderKind.Slack,false)]
    [InlineData(NotificationProviderKind.MicrosoftTeams,false)]
    [InlineData(NotificationProviderKind.Telegram,false)]
    [InlineData(NotificationProviderKind.PagerDuty,false)]
    [InlineData(NotificationProviderKind.Webhook,true)]
    [InlineData(NotificationProviderKind.Email,true)]
    [InlineData(NotificationProviderKind.Slack,true)]
    [InlineData(NotificationProviderKind.MicrosoftTeams,true)]
    [InlineData(NotificationProviderKind.Telegram,true)]
    [InlineData(NotificationProviderKind.PagerDuty,true)]
    public async Task Shared_PostgreSQL_revocation_during_DNS_or_after_real_TLS_prevents_credential_and_message_bytes(
        NotificationProviderKind provider,bool afterTls)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(45));var token=deadline.Token;
        await using var database=await G2CliPostgreSqlDatabase.CreateAsync();
        var directory=Path.Combine(Path.GetTempPath(),"governance-boundary-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        var file=Path.Combine(directory,"credential");await File.WriteAllTextAsync(file,Credential(provider),token);
        var hostname=provider==NotificationProviderKind.Webhook?"hooks.example.org":provider==NotificationProviderKind.Email?"smtp.example.org":V08W66ProviderPhysicalTlsTests.Hostname(provider);
        var bytes=0;var sessionEnded=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task Observe(SslStream stream,CancellationToken ct)
        {
            try { var buffer=new byte[4096];Interlocked.Add(ref bytes,await stream.ReadAsync(buffer,ct)); }
            catch(IOException) { }
            finally { sessionEnded.TrySetResult(); }
        }
        await using var tls=new TlsProvider(certificateName:hostname,session:Observe);
        var network=new Network(hostname,tls.Port,provider==NotificationProviderKind.Email?465:443) {HoldDns=!afterTls};
        var boundary=new LateBoundary();var candidate=Candidate(provider,"v1",file);
        var options=new Kafdeck.Infrastructure.Configuration.NotificationGovernanceOptions([candidate]);
        using var a=CreateHost(database.ConnectionString,options,tls,network,provider,afterTls?boundary:null);
        using var b=CreateHost(database.ConnectionString,options,tls,network,provider);
        var left=a.Services.GetRequiredService<INotificationGovernanceStore>();var right=b.Services.GetRequiredService<INotificationGovernanceStore>();
        await left.InitializeAsync(token);await right.InitializeAsync(token);
        NotificationGovernanceChange Change(long revision,bool enabled)=>new("ops",candidate.CandidateId,candidate.Fingerprint,
            revision,true,enabled,"oidc:https://idp.example|operator",new string('c',64));
        try
        {
            Assert.NotNull(await left.ChangeAsync(Change(0,true),token));
            var profile=await a.Services.GetRequiredService<INotificationDestinationProfileCatalog>().GetAsync("ops",token);Assert.NotNull(profile);
            var id=Guid.NewGuid();await SeedAsync(a.Services,profile,id,token);await a.StartAsync(token);
            await (afterTls?boundary.Entered.Task:network.Entered.Task).WaitAsync(token);
            var claimed=await a.Services.GetRequiredService<INotificationDeliveryStore>().GetAsync(id,"ops",token);
            Assert.NotNull(claimed);Assert.Equal(NotificationDeliveryState.InFlight,claimed.Snapshot.State);Assert.Equal(1,claimed.Snapshot.AttemptCount);
            Assert.NotNull(await right.ChangeAsync(Change(1,false),token));
            if(afterTls) boundary.Released.TrySetResult();else network.Released.TrySetResult();
            await WaitAsync(a.Services,id,NotificationDeliveryState.UnknownExternalEffect,token);
            if(afterTls) await sessionEnded.Task.WaitAsync(token);
            Assert.Equal(afterTls?1:0,network.Dials);Assert.Equal(0,Volatile.Read(ref bytes));Assert.Equal(0,tls.RequestCount);
            await a.Services.GetRequiredService<NotificationDeliveryWorker>().RunDueCycleAsync(token);
            Assert.Equal(afterTls?1:0,network.Dials);
            var evidence=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY")??Path.Combine(directory,"evidence");Directory.CreateDirectory(evidence);
            await File.WriteAllTextAsync(Path.Combine(evidence,$"notification-revocation-{provider.ToString().ToLowerInvariant()}-{(afterTls?"tls":"dns")}-evidence.json"),
                JsonSerializer.Serialize(new { actualPostgreSql=true,independentGovernanceStores=2,provider=provider.ToString(),
                    revocationBoundary=afterTls?"after-real-TLS-before-application-write":"blocked-DNS-before-TCP",physicalDials=network.Dials,
                    credentialAndApplicationBytes=bytes,physicalPosts=0,durableState="UnknownExternalEffect",attempts=1,automaticReplay=0 }),token);
            output.WriteLine($"REAL_LATE_REVOCATION provider={provider}; boundary={(afterTls?"TLS":"DNS")}; PostgreSQL=true; applicationBytes=0; attempts=1; terminal=Unknown");
        }
        finally
        {
            boundary.Released.TrySetResult();network.Released.TrySetResult();
            await a.StopAsync(CancellationToken.None);await b.StopAsync(CancellationToken.None);Directory.Delete(directory,true);
        }
    }

    private sealed class LateBoundary
    {
        internal TaskCompletionSource Entered {get;}=new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Released {get;}=new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    private sealed class LateRevisionGate(INotificationPhysicalRevisionGate inner,LateBoundary boundary):INotificationPhysicalRevisionGate
    {
        private int _checks;
        public async ValueTask VerifyAsync(string destinationId,string fingerprint,CancellationToken token)
        {
            if(Interlocked.Increment(ref _checks)==2)
            {boundary.Entered.TrySetResult();await boundary.Released.Task.WaitAsync(token);}
            await inner.VerifyAsync(destinationId,fingerprint,token);
        }
    }
}
