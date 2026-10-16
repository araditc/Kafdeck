using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using Xunit.Abstractions;
using TlsProvider=Kafdeck.Architecture.Tests.V08W66HostedWebhookTlsTests.TlsProvider;
using SmtpScript=Kafdeck.Architecture.Tests.V08W66SmtpPhysicalTlsTests.SmtpScript;

namespace Kafdeck.Architecture.Tests;

public sealed class PostgreSqlGovernanceTheoryAttribute:TheoryAttribute
{
    public PostgreSqlGovernanceTheoryAttribute()
    { if(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES"))) Skip="Requires actual PostgreSQL for hot cross-host governance and physical TLS proofs."; }
}

public sealed partial class V08NotificationGovernancePostgreSqlTests(ITestOutputHelper output)
{
    [PostgreSqlGovernanceTheory]
    [InlineData(NotificationProviderKind.Webhook)]
    [InlineData(NotificationProviderKind.Email)]
    [InlineData(NotificationProviderKind.Slack)]
    [InlineData(NotificationProviderKind.MicrosoftTeams)]
    [InlineData(NotificationProviderKind.Telegram)]
    [InlineData(NotificationProviderKind.PagerDuty)]
    public async Task Two_actual_PostgreSQL_hosts_observe_hot_rotation_revocation_generation_and_one_TLS_send_each(NotificationProviderKind provider)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(45));var token=deadline.Token;
        await using var database=await G2CliPostgreSqlDatabase.CreateAsync();
        var directory=Path.Combine(Path.GetTempPath(),"governance-physical-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        var oldSecret=Credential(provider);var newSecret=Credential(provider);
        var oldFile=Path.Combine(directory,"v1");var newFile=Path.Combine(directory,"v2");
        await File.WriteAllTextAsync(oldFile,oldSecret,token);await File.WriteAllTextAsync(newFile,newSecret,token);
        var eventId=Guid.NewGuid();
        var firstSmtp=new SmtpScript("accepted",oldSecret);var secondSmtp=new SmtpScript("accepted",newSecret);var smtp=firstSmtp;
        var host=provider==NotificationProviderKind.Webhook?"hooks.example.org":provider==NotificationProviderKind.Email?"smtp.example.org":V08W66ProviderPhysicalTlsTests.Hostname(provider);
        var status=provider==NotificationProviderKind.Webhook?204:provider==NotificationProviderKind.MicrosoftTeams?201:provider==NotificationProviderKind.PagerDuty?202:200;
        await using var tls=new TlsProvider(certificateName:host,statusCode:status,responseBody:_=>V08W66ProviderPhysicalTlsTests.Response(provider,"accepted",eventId),
            session:provider==NotificationProviderKind.Email?(stream,ct)=>smtp.RunAsync(stream,ct):null);
        var network=new Network(host,tls.Port,provider==NotificationProviderKind.Email?465:443);
        var first=Candidate(provider,"v1",oldFile);var second=Candidate(provider,"v2",newFile);
        var options=new NotificationGovernanceOptions([first,second]);
        using var a=CreateHost(database.ConnectionString,options,tls,network,provider);
        using var b=CreateHost(database.ConnectionString,options,tls,network,provider);
        var governanceA=a.Services.GetRequiredService<INotificationGovernanceStore>();
        var governanceB=b.Services.GetRequiredService<INotificationGovernanceStore>();
        await governanceA.InitializeAsync(token);await governanceB.InitializeAsync(token);
        NotificationGovernanceChange Change(NotificationGovernanceCandidate candidate,long revision,bool credential)=>
            new("ops",candidate.CandidateId,candidate.Fingerprint,revision,true,credential,"oidc:https://idp.example|operator",new string('c',64));
        try
        {
            Assert.NotNull(await governanceA.ChangeAsync(Change(first,0,true),token));
            var profile=(await b.Services.GetRequiredService<INotificationDestinationProfileCatalog>().GetAsync("ops",token))!;
            Assert.True(await a.Services.GetRequiredService<INotificationRecoveryEventGate>().CheckAsync(eventId,"ops",DateTimeOffset.UtcNow,token));
            await SeedAsync(a.Services,profile,eventId,token);
            await Task.WhenAll(a.StartAsync(token),b.StartAsync(token));
            await WaitAsync(a.Services,eventId,NotificationDeliveryState.Delivered,token);
            Assert.Equal(1,network.Dials);
            Assert.NotNull(await governanceB.ChangeAsync(Change(first,1,false),token));
            Assert.False(await a.Services.GetRequiredService<INotificationDeliveryClaimGate>().CanClaimAsync("ops",token));
            await Assert.ThrowsAsync<KafdeckConfigurationException>(()=>a.Services.GetRequiredService<INotificationCredentialResolver>()
                .ResolveAsync(new(profile),token).AsTask());
            var revokedId=Guid.NewGuid();await SeedAsync(a.Services,profile,revokedId,token);
            await Task.Delay(1200,token);
            var revoked=await a.Services.GetRequiredService<INotificationDeliveryStore>().GetAsync(revokedId,"ops",token);
            Assert.NotNull(revoked);Assert.Equal(0,revoked.Snapshot.AttemptCount);Assert.Equal(NotificationDeliveryState.Pending,revoked.Snapshot.State);
            Assert.Equal(1,network.Dials);
            smtp=secondSmtp;
            Assert.NotNull(await governanceB.ChangeAsync(Change(second,2,true),token));
            await WaitAsync(a.Services,revokedId,NotificationDeliveryState.UnknownExternalEffect,token);
            var rotated=(await a.Services.GetRequiredService<INotificationDestinationProfileCatalog>().GetAsync("ops",token))!;
            Assert.NotEqual(profile.RevisionFingerprint,rotated.RevisionFingerprint);
            eventId=Guid.NewGuid();await SeedAsync(b.Services,rotated,eventId,token);
            await WaitAsync(b.Services,eventId,NotificationDeliveryState.Delivered,token);
            Assert.Equal(2,network.Dials);
            if(provider==NotificationProviderKind.Email)
            { Assert.True(firstSmtp.AuthMatched&&secondSmtp.AuthMatched,"Both SMTP sends must use their exact current secret version.");Assert.Equal(2,firstSmtp.DataCount+secondSmtp.DataCount); }
            else
            {
                Assert.Equal(2,tls.RequestCount);
                if(provider==NotificationProviderKind.Webhook)
                {
                    Assert.True(tls.Requests.First().Contains("Authorization: Bearer "+oldSecret,StringComparison.Ordinal),"Original admitted credential required.");
                    Assert.True(tls.Requests.Last().Contains("Authorization: Bearer "+newSecret,StringComparison.Ordinal),"Rotated admitted credential required.");
                }
                else { V08W66ProviderPhysicalTlsTests.VerifyWire(provider,tls.Requests.Last(),rotated,eventId,newSecret); }
            }
            var rows=await governanceA.ReadAuditAsync("ops",0,100,token);Assert.Equal(3,rows.Count);
            var audit=JsonSerializer.Serialize(rows);
            Assert.False(audit.Contains(oldSecret,StringComparison.Ordinal)||audit.Contains(newSecret,StringComparison.Ordinal),"Governance audit must contain no credentials.");
            var evidence=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY")??Path.Combine(directory,"evidence");Directory.CreateDirectory(evidence);
            await File.WriteAllTextAsync(Path.Combine(evidence,"notification-governance-"+provider.ToString().ToLowerInvariant()+"-evidence.json"),
                JsonSerializer.Serialize(new {actualPostgreSql=true,independentHosts=2,physicalAuthenticatedSends=2,hotCrossHostRevocation=true,
                    revokedClaimAttempts=0,oldRevisionTerminal="UnknownExternalEffect",atomicAuditRows=3,provider=provider.ToString(),
                    externalSaasTenantProven=false}),token);
            output.WriteLine($"REAL_GOVERNANCE_TLS provider={provider}; PostgreSQL=true; hosts=2; physicalSends=2; hotRevoke=true; revokedAttempts=0; oldRevision=Unknown; auditRows=3");
        }
        finally { await a.StopAsync(CancellationToken.None);await b.StopAsync(CancellationToken.None);Directory.Delete(directory,true); }
    }
    private static NotificationGovernanceCandidate Candidate(NotificationProviderKind provider,string version,string file)
    {
        var id="source-"+version;var binding=new NotificationCredentialBindingId("binding-"+version);
        NotificationDestinationProfile source;
        if(provider==NotificationProviderKind.Webhook) source=new(id,provider,"Fixture",[NotificationEventClass.Operational],new Uri("https://hooks.example.org/notify"),binding,webhookAuthentication:NotificationWebhookAuthentication.Bearer);
        else if(provider==NotificationProviderKind.Email) source=new(id,provider,"Fixture",[NotificationEventClass.Operational],credentialBindingId:binding,
            emailRecipientAddress:"ops@example.org",smtp:new("smtp.example.org","kafdeck@example.org","kafdeck"));
        else { var p=V08W66ProviderPhysicalTlsTests.Profile(provider);source=new(id,provider,"Fixture",p.EnabledEvents,credentialBindingId:binding,
            providerResourceId:p.ProviderResourceId,providerSecondaryResourceId:p.ProviderSecondaryResourceId); }
        return new(version,"ops",source,new(id,binding,SecretReference.Parse("file:"+file)));
    }
    private static IHost CreateHost(string connection,NotificationGovernanceOptions governance,TlsProvider tls,Network network,NotificationProviderKind provider,
        LateBoundary? lateBoundary=null)
    {
        var builder=Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<SecretResolver>();
        var factory=new PostgreSqlNotificationDeliveryDbConnectionFactory(connection);
        builder.Services.AddSingleton<INotificationDeliveryDbConnectionFactory>(factory);
        builder.Services.AddSingleton<INotificationRoutingStore>(new AdoNotificationRoutingStore(factory));
        builder.Services.AddSingleton<INotificationDeliveryStore>(new AdoNotificationDeliveryStore(factory));
        builder.Services.AddSingleton<INotificationHostAddressLookup>(network);builder.Services.AddSingleton<INotificationPinnedTcpDialer>(network);
        builder.Services.AddSingleton(sp=>new NotificationPinnedHttpHandlerFactory(network,NotificationAddressPolicy.NoConfiguredNat64,
            trustRoots:new X509Certificate2Collection(tls.Root),revisionGate:lateBoundary is null?sp.GetRequiredService<INotificationPhysicalRevisionGate>():
                new LateRevisionGate(sp.GetRequiredService<INotificationPhysicalRevisionGate>(),lateBoundary)));
        builder.Services.AddKafdeckWebhookDelivery(new(true,NotificationPersistenceProvider.PostgreSql,NotificationExecutionMode.Standalone,null,
            SecretReference.Parse("env:KAFDECK_TEST_POSTGRES"),DestinationProfiles:governance.Candidates.Select(c=>c.Source).ToArray(),
            CredentialBindings:governance.Candidates.Select(c=>c.Credential!).ToArray(),
            WebhookDeliveryEnabled:provider==NotificationProviderKind.Webhook,HttpProviderDeliveryEnabled:provider is not (NotificationProviderKind.Webhook or NotificationProviderKind.Email),EmailDeliveryEnabled:provider==NotificationProviderKind.Email));
        builder.Services.AddKafdeckNotificationGovernance(governance);
        return builder.Build();
    }
    private static async Task SeedAsync(IServiceProvider services,NotificationDestinationProfile profile,Guid id,CancellationToken token)
    {
        var routing=services.GetRequiredService<INotificationRoutingStore>();var deliveries=services.GetRequiredService<INotificationDeliveryStore>();
        await routing.InitializeAsync(token);await deliveries.InitializeAsync(token);
        var now=await deliveries.GetCoordinationUtcNowAsync(DateTimeOffset.UtcNow,token);
        var item=NotificationSafeEvent.Approved(id,NotificationApprovedEventKind.ConsumerLagAlert,now);
        await routing.CreateOrGetEventAsync(item,now,token);
        await deliveries.CreateOrGetAsync(new(id,"ops",item.PayloadFingerprint,NotificationDeliveryState.Pending,0,now,routedProfileRevisionFingerprint:profile.RevisionFingerprint),now,token);
    }
    private static async Task WaitAsync(IServiceProvider services,Guid id,NotificationDeliveryState state,CancellationToken token)
    {
        while(true)
        {
            var row=await services.GetRequiredService<INotificationDeliveryStore>().GetAsync(id,"ops",token);
            if(row?.Snapshot.State==state) return;
            if(row?.Snapshot.State is NotificationDeliveryState.UnknownExternalEffect or NotificationDeliveryState.Exhausted)
                throw new InvalidOperationException($"Expected {state}; durable terminal state={row.Snapshot.State}; attempts={row.Snapshot.AttemptCount}.");
            await Task.Delay(25,token);
        }
    }
    private static string Credential(NotificationProviderKind provider)=>provider switch
    { NotificationProviderKind.Telegram=>"123456:"+Convert.ToHexString(RandomNumberGenerator.GetBytes(24)).ToLowerInvariant(),NotificationProviderKind.PagerDuty=>Convert.ToHexString(RandomNumberGenerator.GetBytes(16)).ToLowerInvariant(),_=>Convert.ToBase64String(RandomNumberGenerator.GetBytes(24)) };
    private sealed class Network(string host,int physical,int logical):INotificationHostAddressLookup,INotificationPinnedTcpDialer
    {
        private int _dials;internal int Dials=>Volatile.Read(ref _dials);
        internal bool HoldDns {get;init;}
        internal TaskCompletionSource Entered {get;}=new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Released {get;}=new(TaskCreationOptions.RunContinuationsAsynchronously);
        public async Task<IReadOnlyList<IPAddress>> GetHostAddressesAsync(string hostname,CancellationToken token)
        {
            if(hostname!=host) throw new InvalidOperationException("Fixture identity mismatch.");
            if(HoldDns) { Entered.TrySetResult();await Released.Task.WaitAsync(token); }
            return [IPAddress.Parse("1.1.1.1")];
        }
        public ValueTask<Stream> DialAsync(IPAddress address,int port,CancellationToken token)
        { if(!address.Equals(IPAddress.Parse("1.1.1.1"))||port!=logical) throw new InvalidOperationException("Fixture pin mismatch.");Interlocked.Increment(ref _dials);return new SocketNotificationPinnedTcpDialer().DialAsync(IPAddress.Loopback,physical,token); }
    }
}
