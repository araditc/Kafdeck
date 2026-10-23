using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Cli;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08G2HumanRecoveryCliLiveTests
{
    [KeycloakG2Fact]
    public Task Real_Keycloak_two_humans_review_and_apply_SQLite_quarantined_recovery_through_shipped_CLI()=>RunAsync(false);

    [KeycloakG2PostgreSqlKafkaFact]
    public Task Real_Keycloak_two_humans_review_and_apply_PostgreSQL_quarantined_recovery_through_shipped_CLI()=>RunAsync(true);

    private static async Task RunAsync(bool postgres)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromMinutes(4)); var token=deadline.Token;
        var evidence=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY")??Path.GetTempPath();
        var directory=Path.Combine(evidence,"human-recovery-"+Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        await using var database=postgres?await G2CliPostgreSqlDatabase.CreateAsync():null;
        var notificationPath=Path.Combine(directory,"notification.db"); var sourcePath=Path.Combine(directory,"source.db");
        INotificationDeliveryDbConnectionFactory delivery=database is null?new SqliteNotificationDeliveryDbConnectionFactory(notificationPath):new PostgreSqlNotificationDeliveryDbConnectionFactory(database.ConnectionString);
        IMutationDbConnectionFactory source=database is null?new SqliteMutationDbConnectionFactory(sourcePath):new PostgreSqlMutationDbConnectionFactory(database.ConnectionString);
        var routing=new AdoNotificationRoutingStore(delivery); await routing.InitializeAsync(token);
        var ledger=new AdoNotificationDeliveryStore(delivery); await ledger.InitializeAsync(token);
        var e1=Guid.NewGuid();var e2=Guid.NewGuid(); var old=Guid.NewGuid();var now=DateTimeOffset.UtcNow;
        await using(var connection=await delivery.OpenAsync(token))
        await using(var command=connection.CreateCommand())
        {command.CommandText="CREATE TABLE notification_delivery_generation(id INTEGER PRIMARY KEY,generation TEXT NOT NULL); INSERT INTO notification_delivery_generation VALUES(1,'"+e1.ToString("D")+"')";await command.ExecuteNonQueryAsync(token);}
        await ledger.CreateOrGetAsync(new(old,"reviewed-recovery",new string('a',64),NotificationDeliveryState.Pending,0,now,
            routedProfileRevisionFingerprint:new string('b',64)),now,token);
        // Genuine production outbox capture; maintenance starts only after all source writers have stopped.
        var historicalSource=await V08G2PostgreSqlSafeResumeTests.CaptureProductAsync(source,token);
        var anchor=Path.Combine(directory,"outside-quarantined-anchor.json");
        var bytes=JsonSerializer.SerializeToUtf8Bytes(new{version=1,generation=e2,state="quarantined"});
        await File.WriteAllBytesAsync(anchor,bytes,token);var hash=Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
        await using var lab=new G2HumanCliLaboratory(Path.Combine(directory,"api"));
        lab.RegisterHumans(["NotificationManage"],["SystemRead","NotificationRead"],["NotificationManage"]);
        var settings=lab.Settings;
        settings["Kafdeck:Notifications:Enabled"]="true";settings["Kafdeck:Notifications:ManagementEnabled"]="true";
        settings["Kafdeck:Notifications:Provider"]=postgres?"PostgreSql":"Sqlite";
        settings["Kafdeck:Notifications:ExecutionMode"]="Standalone";
        settings["Kafdeck:Notifications:DeliveryInterlockPath"]=anchor;settings["Kafdeck:Notifications:DeliveryInterlockSha256"]=hash;
        settings["Kafdeck:Notifications:Recovery:Enabled"]="true";settings["Kafdeck:Notifications:Recovery:TargetGeneration"]=e2.ToString("D");
        settings["Kafdeck:Notifications:ProductEventsEnabled"]="false";
        settings["Kafdeck:Notifications:WebhookDeliveryEnabled"]="false";settings["Kafdeck:Notifications:HttpProviderDeliveryEnabled"]="false";settings["Kafdeck:Notifications:EmailDeliveryEnabled"]="false";
        settings["Kafdeck:Administration:Mutations:Enabled"]="false";
        settings["Kafdeck:Administration:Mutations:Persistence:Provider"]=postgres?"PostgreSql":"Sqlite";
        settings["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"]="Standalone";
        if(database is null)
        {settings["Kafdeck:Notifications:SqliteDatabasePath"]=notificationPath;settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"]=sourcePath;}
        else
        {lab.Secret("KAFDECK_G2_RECOVERY_CLI_CONNECTION",database.ConnectionString);settings["Kafdeck:Notifications:ConnectionString"]="env:KAFDECK_G2_RECOVERY_CLI_CONNECTION";settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"]="env:KAFDECK_G2_RECOVERY_CLI_CONNECTION";}
        await lab.StartAsync(token);
        Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"notifications","recovery","status"));
        var ids=new List<Guid>();
        foreach(var command in new[]{"FenceBatch","FenceSourceBatch","AdvanceGeneration"})
        {
            var request=await lab.RequestFileAsync(new {command,disposition=command=="AdvanceGeneration"?null:"UnresolvedRiskAccepted",
                proofReferenceSha256=new string('c',64),fencedSendersEvidenceSha256=new string('d',64),expectedDatabaseGeneration=e1,
                targetGeneration=e2,expectedAnchorSha256=hash,maxItems=100},token);
            Assert.Equal(CliApplication.PendingApproval,await lab.RunAsync(token,"notifications","recovery","preview","--request-file",request));
            using var preview=JsonDocument.Parse(lab.Output);var id=preview.RootElement.GetProperty("operationId").GetGuid();ids.Add(id);
            var previewHash=preview.RootElement.GetProperty("previewHash").GetString()!;
            var review=await lab.RequestFileAsync(new{expectedRevision=preview.RootElement.GetProperty("revision").GetInt64(),previewHash},token);
            Assert.Equal(CliApplication.Conflict,await lab.RunAsync(token,"notifications","recovery","approve",id.ToString("D"),"--request-file",review));
            Assert.Equal(CliApplication.Conflict,await lab.RunAsync(token,"notifications","recovery","apply",id.ToString("D"),"--request-file",review));
            lab.Approver=true;
            Assert.Equal(CliApplication.PendingApproval,await lab.RunAsync(token,"notifications","recovery","get",id.ToString("D")));
            Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"notifications","recovery","approve",id.ToString("D"),"--request-file",review));
            using var approved=JsonDocument.Parse(lab.Output);
            var apply=await lab.RequestFileAsync(new{expectedRevision=approved.RootElement.GetProperty("revision").GetInt64(),previewHash},token);
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,await lab.RunAsync(token,"notifications","recovery","apply",id.ToString("D"),"--request-file",apply));
            lab.Approver=false;
            Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"notifications","recovery","apply",id.ToString("D"),"--request-file",apply));
            using var applied=JsonDocument.Parse(lab.Output);Assert.Equal("Applied",applied.RootElement.GetProperty("state").GetString());
        }
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect,(await ledger.GetAsync(old,"reviewed-recovery",token))!.Snapshot.State);
        Assert.Equal(0,(await ledger.GetAsync(old,"reviewed-recovery",token))!.Snapshot.AttemptCount);
        var persisted=new AdoNotificationRecoveryStore(delivery);
        var status=await persisted.StatusAsync(token);Assert.Equal(e2,status.DatabaseGeneration);
        Assert.False(await persisted.CheckAsync(historicalSource,"reviewed-recovery",DateTimeOffset.UtcNow,token));
        Assert.Equal(bytes,await File.ReadAllBytesAsync(anchor,token));
        foreach(var id in ids)
        {
            var plan=await persisted.GetAsync(id,token);Assert.NotNull(plan);
            Assert.NotEqual(plan.RequesterHumanFingerprint,plan.ApproverHumanFingerprint);
            Assert.StartsWith("oidc-cli-human:",plan.RequesterPrincipalId,StringComparison.Ordinal);
            Assert.StartsWith("oidc-cli-human:",plan.ApprovedByPrincipalId!,StringComparison.Ordinal);
        }
        await File.WriteAllBytesAsync(Path.Combine(evidence,(postgres?"postgres":"sqlite")+"-keycloak-human-cli-recovery-evidence.json"),JsonSerializer.SerializeToUtf8Bytes(new {
            realKeycloak=true,shippedApi=true,shippedCli=true,persistence=postgres?"PostgreSQL":"SQLite",approvedCommands=3,
            distinctRealHumans=true,selfApprovalRefused=true,wrongRequesterApplyRefused=true,pendingApplyRefused=true,
            ledgerTerminalUnknown=true,historicalSourceSuppressed=true,automaticSends=0,outsideAnchorStillQuarantined=true,
            completedPkceLogins=lab.CompletedLogins,physicalRestoreCoveredBySeparateScenario=true}),token);
    }
}
