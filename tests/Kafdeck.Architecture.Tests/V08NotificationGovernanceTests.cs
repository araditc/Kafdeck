using System.Text.Json;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08NotificationGovernanceTests
{
    [Fact]
    public async Task Atomic_state_and_actor_policy_audit_CAS_rollback_and_restart_are_real_Sqlite()
    {
        var directory=Path.Combine(Path.GetTempPath(),"notification-governance-"+Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var file=Path.Combine(directory,"governance.db");
        try
        {
            var factory=new SqliteNotificationDeliveryDbConnectionFactory(file);
            var first=new AdoNotificationGovernanceStore(factory);
            var second=new AdoNotificationGovernanceStore(new SqliteNotificationDeliveryDbConnectionFactory(file));
            await first.InitializeAsync(default);
            Assert.Null(await first.GetAsync("ops",default));
            var created=await first.ChangeAsync(Change(0),default); Assert.NotNull(created);
            Assert.Null(await second.ChangeAsync(Change(0),default));
            Assert.Equal(created,await second.GetAsync("ops",default));
            await using(var connection=await factory.OpenAsync())
            {
                await using var command=connection.CreateCommand();
                command.CommandText="CREATE TRIGGER fail_governance_audit BEFORE INSERT ON notification_governance_audit WHEN NEW.revision=2 BEGIN SELECT RAISE(ABORT,'fixture'); END";
                await command.ExecuteNonQueryAsync();
            }
            await Assert.ThrowsAsync<SqliteException>(()=>second.ChangeAsync(Change(1),default));
            Assert.Equal(1,(await first.GetAsync("ops",default))!.Revision);
            var audit=await second.ReadAuditAsync("ops",0,50,default);
            Assert.Single(audit); Assert.Equal("oidc:https://identity.example|operator",audit[0].Actor);
            Assert.Equal(new string('b',64),audit[0].PolicyFingerprint);
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(()=>first.ReadAuditAsync("ops",0,101,default));
        }
        finally { SqliteConnection.ClearAllPools();Directory.Delete(directory,true); }
    }

    [Fact]
    public void Generation_is_monotonic_authority_not_secret_or_endpoint_mutation()
    {
        var profile=Source();
        var candidate=new NotificationGovernanceCandidate("candidate-v1","ops",profile,new("source",profile.CredentialBindingId!,SecretReference.Parse("env:GOVERNANCE_FIXTURE")));
        var one=candidate.Clone(1,true);var two=candidate.Clone(2,true);
        Assert.NotEqual(one.RevisionFingerprint,two.RevisionFingerprint);
        Assert.Equal(one.ConfiguredEndpoint,two.ConfiguredEndpoint);
        Assert.Equal(one.CredentialBindingId,two.CredentialBindingId);
        Assert.Throws<ArgumentOutOfRangeException>(()=>candidate.Clone(0,true));
        Assert.Throws<ArgumentException>(()=>new NotificationGovernanceCandidate("candidate-v1","..",profile,null));
        var request=new Kafdeck.Api.NotificationManagedDestinationChangeRequest("candidate-v1",1,true,false);
        var json=JsonSerializer.Serialize(request);
        foreach(var forbidden in new[]{"https:","SecretReference","ConfiguredEndpoint","Password","Headers"})
            Assert.DoesNotContain(forbidden,json,StringComparison.OrdinalIgnoreCase);
        Assert.Throws<JsonException>(()=>JsonSerializer.Deserialize<Kafdeck.Api.NotificationManagedDestinationChangeRequest>(
            "{\"CandidateId\":\"candidate-v1\",\"ExpectedRevision\":1,\"ProfileEnabled\":true,\"CredentialEnabled\":true,\"SecretReference\":\"env:PATH\"}"));
    }

    [Theory]
    [InlineData("Enabled","sometimes")]
    [InlineData("SecretReference","env:OTHER")]
    [InlineData("Candidates:0:Url","https://unreviewed.example")]
    [InlineData("Candidates:0:CandidateId","raw/secret")]
    public void Strict_configuration_rejects_unknown_material_and_bad_activation(string suffix,string value)
    {
        var settings=Settings();settings["Kafdeck:Notifications:Governance:"+suffix]=value;
        var config=new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        Assert.Throws<KafdeckConfigurationException>(()=>NotificationGovernanceOptions.Read(config,Options()));
    }
    [Fact]
    public void Disabled_governance_does_not_activate_and_enabled_requires_OIDC_management()
    {
        Assert.Null(NotificationGovernanceOptions.Read(new ConfigurationBuilder().Build(),Options()));
        var config=new ConfigurationBuilder().AddInMemoryCollection(Settings()).Build();
        Assert.Single(NotificationGovernanceOptions.Read(config,Options())!.Candidates);
        Assert.Throws<KafdeckConfigurationException>(()=>NotificationGovernanceOptions.Read(config,
            Options() with { Deployment=new DeploymentOptions("http://127.0.0.1:0",null,AccessMode.Local,null) }));
    }
    internal static NotificationGovernanceChange Change(long expected)=>new("ops","candidate-v1",new string('a',64),expected,true,true,
        "oidc:https://identity.example|operator",new string('b',64));
    private static NotificationDestinationProfile Source()=>new("source",NotificationProviderKind.Webhook,"Source",[NotificationEventClass.Operational],
        new Uri("https://hooks.example.org/notify"),new("binding-v1"),webhookAuthentication:NotificationWebhookAuthentication.Bearer);
    private static KafdeckOptions Options()=>new(new DeploymentOptions("http://127.0.0.1:0",null,AccessMode.Oidc,null),[],
        Notifications:new(true,NotificationPersistenceProvider.Sqlite,NotificationExecutionMode.Standalone,"fixture.db",null,true,[Source()],
            [new("source",new("binding-v1"),SecretReference.Parse("env:GOVERNANCE_FIXTURE"))]));
    private static Dictionary<string,string?> Settings()=>new()
    {
        ["Kafdeck:Notifications:Governance:Enabled"]="true",["Kafdeck:Notifications:Governance:Candidates:0:CandidateId"]="candidate-v1",
        ["Kafdeck:Notifications:Governance:Candidates:0:DestinationId"]="ops",["Kafdeck:Notifications:Governance:Candidates:0:ProfileId"]="source",
    };
}
