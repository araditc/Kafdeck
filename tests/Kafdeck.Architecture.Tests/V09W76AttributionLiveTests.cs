using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Api;
using Kafdeck.Cli;
using Kafdeck.Core.Attribution;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W76AttributionLiveTests
{
    [KeycloakG2KafkaFact]
    public Task SQLite_actual_TLS_counter_acquisition_genuine_CLI_rate_governance_dashboard_and_history()=>Exercise(false);
    [KeycloakG2PostgreSqlKafkaFact]
    public Task PostgreSQL_actual_TLS_counter_acquisition_genuine_CLI_rate_governance_dashboard_and_history()=>Exercise(true);
    private static async Task Exercise(bool postgres)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromMinutes(15));var token=deadline.Token;
        var evidence=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY")??Path.GetTempPath();Directory.CreateDirectory(evidence);
        var directory=Path.Combine(evidence,"w76-private-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        await using var database=postgres?await G2CliPostgreSqlDatabase.CreateAsync():null;
        await using var lab=new G2HumanCliLaboratory(directory);
        // Cluster/topic collection reads have no resource name. The fixture grants their exact cluster scope;
        // a literal resource wildcard only authorizes named resources and cannot bootstrap the real UI.
        lab.RegisterHumans(["CatalogManage","RateCardManage","MutationReconcile"],["SystemRead","ClusterRead","TopicList","TopicRead","CatalogRead","AttributionRead"],[],"permitted");
        var generation=Guid.NewGuid();var settings=lab.Settings;var bootstrap=Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!;
        settings["Kafdeck:Governance:CatalogEnabled"]="true";settings["Kafdeck:Governance:AttributionEnabled"]="true";settings["Kafdeck:Governance:DeploymentGeneration"]=generation.ToString("D");
        settings["Kafdeck:Administration:Mutations:Enabled"]="true";settings["Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled"]="false";
        settings["Kafdeck:Administration:Mutations:Persistence:Provider"]=postgres?"PostgreSql":"Sqlite";settings["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"]="Standalone";
        settings["Kafdeck:Administration:Mutations:MaterialDigestKey"]="env:KAFDECK_W76_DIGEST";lab.Secret("KAFDECK_W76_DIGEST",Convert.ToHexString(RandomNumberGenerator.GetBytes(32)));
        settings["Kafdeck:Clusters:0:Id"]="permitted";settings["Kafdeck:Clusters:0:BootstrapServers:0"]=bootstrap;settings["Kafdeck:Clusters:0:SecurityProtocol"]="Plaintext";
        IMutationDbConnectionFactory connections;
        if(database is null){var path=Path.Combine(directory,"mutations.db");settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"]=path;connections=new SqliteMutationDbConnectionFactory(path);}
        else{settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"]="env:KAFDECK_W76_PG";lab.Secret("KAFDECK_W76_PG",database.ConnectionString);connections=new PostgreSqlMutationDbConnectionFactory(database.ConnectionString);}
        var browser=!postgres&&Environment.GetEnvironmentVariable("KAFDECK_V09_REAL_BROWSER")=="1";
        if(browser)
        {
            settings["Kafdeck:Deployment:Oidc:ClientId"]=lab.Field("browserClientId");var dist=Path.Combine(Root(),"src/frontend/dist");Assert.True(File.Exists(Path.Combine(dist,"index.html")));
            foreach(var file in Directory.EnumerateFiles(dist,"*",SearchOption.AllDirectories)){var target=Path.Combine(directory,"wwwroot",Path.GetRelativePath(dist,file));Directory.CreateDirectory(Path.GetDirectoryName(target)!);File.Copy(file,target);}
        }
        var topic="w76-"+Guid.NewGuid().ToString("N");using var admin=new AdminClientBuilder(new AdminClientConfig{BootstrapServers=bootstrap,SocketTimeoutMs=10000}).Build();
        await admin.CreateTopicsAsync([new TopicSpecification{Name=topic,NumPartitions=1,ReplicationFactor=1}],new CreateTopicsOptions{RequestTimeout=TimeSpan.FromSeconds(15)});
        try
        {
            await lab.StartAsync(token);
            var phase="start";
            async Task<JsonElement> Success(params string[] args)
            {var code=await lab.RunAsync(token,args);Assert.True(code==CliApplication.Success,"Actual W76 CLI failed; phase="+phase+"; exit="+code+"; outcome="+lab.SafeOutcome);using var parsed=JsonDocument.Parse(lab.Output);return parsed.RootElement.Clone();}
            async Task Commit(JsonElement op)
            {
                var args=new List<string>{"operations","confirm",op.GetProperty("operationId").GetGuid().ToString("D"),op.GetProperty("previewHash").GetString()!};
                if(op.TryGetProperty("confirmationChallenge",out var challenge)&&challenge.ValueKind==JsonValueKind.String)args.AddRange(["--challenge",challenge.GetString()!]);
                await Success(args.ToArray());var result=await Success("operations","execute",op.GetProperty("operationId").GetGuid().ToString("D"));Assert.Equal("appliedVerified",result.GetProperty("state").GetString());
            }
            async Task<JsonElement> Catalog(string family,object[] changes)
            {
                phase="catalog-"+family;
                var file=await lab.RequestFileAsync(new{clusterId="permitted",changes},token);
                var preview=await Success("catalog","change-preview","permitted",family,"--request-file",file,"--idempotency-key",Guid.NewGuid().ToString("N"));
                await Commit(preview.GetProperty("operation"));return preview;
            }
            async Task<JsonElement> RatePreview(string command,object body)
            {phase="rate-"+command;var file=await lab.RequestFileAsync(JsonSerializer.SerializeToElement(body,AttributionContract.Json),token);return await Success("rate-cards",command,"permitted","--request-file",file,"--idempotency-key",Guid.NewGuid().ToString("N"));}
            var team=await Catalog("teams",[new{kind="Team",expectedRevision=0,name="Metered resource owner"}]);var teamId=team.GetProperty("changes")[0].GetProperty("id").GetString()!;
            var assigned=await Catalog("topics",[new{kind="Topic",expectedRevision=0,name="Controlled resource",topicName=topic,teamId}]);
            var topicId=assigned.GetProperty("changes")[0].GetProperty("id").GetString()!;var physicalId=assigned.GetProperty("changes")[0].GetProperty("topicIdentity").GetString()!;
            var gate=new AdoGovernanceGenerationGate(connections,generation);var catalogStore=new AdoCatalogStore(connections,generation.ToString("D"),gate);var measuredStore=new AdoAttributionStore(connections,gate);
            await using var meter=new V09W76MeasuredTrafficFixture();meter.Begin(topic,physicalId);await meter.SendTrafficAsync(65536,token);await meter.SendTrafficAsync(32768,token);Assert.Equal(98304,meter.PhysicalBytes);
            // The physical >=one-minute meter interval overlaps useful genuine operator work, never a fabricated timestamp.
            var rateDocument=new RateCardDocument("trafficBytes","bytes","USD","0.00001",meter.Started.AddMinutes(-1),meter.Started.AddMinutes(10),[]);
            var versionOp=await RatePreview("version-preview",new{clusterId="permitted",document=rateDocument});await Commit(versionOp);
            var material=await Success("rate-cards","plan",versionOp.GetProperty("operationId").GetGuid().ToString("D"));var versionId=material.GetProperty("versionId").GetString()!;
            var active=await RatePreview("activate-preview",new{clusterId="permitted",versionId,expectedRevision=0,enabled=true});await Commit(active);
            await meter.CloseAsync(token);Assert.True(meter.Window.EndUtc-meter.Window.StartUtc>=TimeSpan.FromMinutes(1));
            var source=meter.Source("permitted");var collector=new AttributionCollectionService([source],measuredStore,catalogStore);Assert.Equal(1,await collector.CollectAsync(token));Assert.Equal(1,await collector.CollectAsync(token));
            var from=Utc(meter.Window.StartUtc);var to=Utc(meter.Window.EndUtc);
            var measured=await Success("measurements","list","permitted",from,to,topic);var first=Assert.Single(measured.GetProperty("items").EnumerateArray());
            Assert.Equal("98304",first.GetProperty("quantity").GetString());Assert.Equal(teamId,first.GetProperty("ownership").GetProperty("teamId").GetString());Assert.Equal("Observed",first.GetProperty("quality").GetString());
            var windowId=first.GetProperty("windowId").GetString()!;
            var attributed=await Success("attribution","windows","permitted",from,to,topic);var allocation=Assert.Single(attributed.GetProperty("items").EnumerateArray());
            Assert.Equal("0.98304",allocation.GetProperty("unroundedAmount").GetString());Assert.Equal("0.98",allocation.GetProperty("roundedAmount").GetString());Assert.Equal("0",allocation.GetProperty("residualQuantity").GetString());
            var dashboard=await Success("dashboards","show","permitted",from,to,topic);Assert.Equal("0.98",dashboard.GetProperty("totals").GetProperty("roundedAmount").GetString());
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,await lab.RunAsync(token,"measurements","list","hidden",from,to,topic));
            var secondTeam=await Catalog("teams",[new{kind="Team",expectedRevision=0,name="Next resource owner"}]);var secondTeamId=secondTeam.GetProperty("changes")[0].GetProperty("id").GetString()!;
            await Catalog("topics",[new{kind="Topic",id=topicId,expectedRevision=1,name="Reassigned controlled resource",topicName=topic,teamId=secondTeamId}]);
            var oldWindow=(await measuredStore.QueryAuthorizedAsync(new("permitted",meter.Window.StartUtc,meter.Window.EndUtc,topic,50),_=>true,token)).Single();
            Assert.Equal(teamId,oldWindow.Ownership.TeamId);Assert.Equal(teamId,(await collector.OwnershipAsync(oldWindow,token)).TeamId);
            var crossing=oldWindow with{WindowEndUtc=DateTimeOffset.UtcNow,Quantity=null};Assert.Equal("Ambiguous",(await collector.OwnershipAsync(crossing,token)).Quality);
            // Persisted metadata receipt acknowledgement loss is explicitly injected after the real governed effect.
            var operations=new AdoMutationOperationRepository(connections);var stored=await operations.GetAsync(versionOp.GetProperty("operationId").GetGuid(),token);Assert.NotNull(stored);
            Assert.Equal(MutationSaveOutcome.Saved,(await operations.TrySaveAsync(stored with{State=MutationOperationState.ExecutionUnknown,Version=stored.Version+1,ResultCode="explicit_storage_ack_loss_fixture",SafeProviderEvidence=new Dictionary<string,string>()},stored.Version,token)).Outcome);
            phase="rate-positive-receipt-recovery";
            lab.Approver=true;try{Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,await lab.RunAsync(token,"rate-cards","recover",stored.OperationId.ToString("D")));}finally{lab.Approver=false;}
            await Success("rate-cards","recover",stored.OperationId.ToString("D"));
            var recovered=await operations.GetAsync(stored.OperationId,token);Assert.NotNull(recovered);
            await Success("rate-cards","recover",stored.OperationId.ToString("D"));Assert.Equal(recovered.Version,(await operations.GetAsync(stored.OperationId,token))!.Version);
            Assert.Equal(versionId,(await measuredStore.GetRateAsync("permitted",versionId,false,token))!.VersionId);
            meter.Begin(topic,physicalId);meter.ResetCounter();await meter.SendTrafficAsync(8192,token);
            if(browser)
            {
                var scenario=Path.Combine(directory,"browser-scenario.json");var future=new RateCardDocument("trafficBytes","bytes","USD","0.00002",oldWindow.WindowEndUtc.AddDays(1),oldWindow.WindowEndUtc.AddDays(2),[]);
                await File.WriteAllBytesAsync(scenario,JsonSerializer.SerializeToUtf8Bytes(new{kind="attribution",clusterId="permitted",window=new{fromUtc=from,toUtc=to,resourceId=topic,quantity="98304",unit="bytes",currency="USD",expectedRoundedAmount="0.98"},newRate=new{metricKind=future.MetricKind,unit=future.Unit,currency=future.Currency,amount=future.Amount,effectiveFromUtc=Utc(future.EffectiveFromUtc),effectiveToUtc=Utc(future.EffectiveToUtc),weights=future.Weights}},AttributionContract.Json),token);
                await Browser(lab.Api,scenario,Path.Combine(evidence,"sqlite-w76-real-browser-evidence.json"),token);
            }
            await meter.CloseAsync(token);Assert.Equal(1,await collector.CollectAsync(token));
            var reset=(await measuredStore.QueryAuthorizedAsync(new("permitted",meter.Window.StartUtc,meter.Window.EndUtc,topic,50),_=>true,token)).Single();Assert.Equal("CounterReset",reset.Quality);Assert.Null(reset.Quantity);Assert.Equal(secondTeamId,reset.Ownership.TeamId);
            var resetPage=await Success("dashboards","show","permitted",Utc(meter.Window.StartUtc),Utc(meter.Window.EndUtc),topic);Assert.Equal(JsonValueKind.Null,resetPage.GetProperty("totals").GetProperty("unroundedAmount").ValueKind);
            await lab.RestartAsync(token);var restarted=await Success("attribution","windows","permitted",from,to,topic);Assert.Equal("0.98",restarted.GetProperty("items")[0].GetProperty("roundedAmount").GetString());
            await File.WriteAllBytesAsync(Path.Combine(evidence,(postgres?"postgres":"sqlite")+"-w76-attribution-evidence.json"),JsonSerializer.SerializeToUtf8Bytes(new{
                provider=postgres?"PostgreSQL":"SQLite",sourceProfile="KafdeckMeasuredCountersV1",actualTlsHttp=true,fixtureDnsAndPublicIpDialSeam=true,certificateAndHostnameValidated=true,
                actualControlledBodyBytes=98304,actualWallClockWindowAtLeastOneMinute=true,metricsAcquiredFromPhysicalCounter=true,metricsReads=meter.MetricsReads,
                kafkaMetadataIdentityFromActualBroker=true,kafkaBillableTrafficClaimed=false,genuineKeycloakCli=true,governedHighRateVersionAndActivation=true,
                sourceWindowId=windowId,observedQuantity="98304",expectedUnrounded="0.98304",expectedRounded="0.98",ownershipAtMeasurementPreserved=true,ownershipCrossingAmbiguous=true,
                actualCounterResetNullEstimate=true,scopedReadDenied=true,persistedRestart=true,explicitReceiptAckLossFixture=true,positiveReceiptRecoveryNoRedispatch=true,repeatedReceiptRecoveryRevalidatesAuthority=true,foreignActorReceiptRecoveryDenied=true,
                browser=browser,humanLogins=lab.CompletedLogins}),token);
        }
        finally
        {
            await lab.StopAsync(CancellationToken.None);
            // The native Windows broker owns cleanup by process teardown; deleting its open log directory is not an admitted fixture capability.
            if(Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_DELETE_CAPABLE")=="1")
                try{await admin.DeleteTopicsAsync([topic]);}catch(KafkaException){}catch(TimeoutException){}
        }
    }
    private static string Utc(DateTimeOffset value)=>value.UtcDateTime.ToString("O",CultureInfo.InvariantCulture);
    private static string Root(){var d=new DirectoryInfo(AppContext.BaseDirectory);while(!File.Exists(Path.Combine(d.FullName,"Kafdeck.slnx")))d=d.Parent!;return d.FullName;}
    private static async Task Browser(Uri api,string scenario,string evidence,CancellationToken token)
    {
        var start=new ProcessStartInfo("node"){WorkingDirectory=Root(),UseShellExecute=false,CreateNoWindow=true,RedirectStandardOutput=true,RedirectStandardError=true};
        foreach(var arg in new[]{Path.Combine(Root(),"src/frontend/scripts/v09-workflows-live-browser.cjs"),api.AbsoluteUri,Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")!,scenario,evidence})start.ArgumentList.Add(arg);
        using var process=Process.Start(start)??throw new InvalidOperationException("Actual browser did not start.");var stdout=process.StandardOutput.ReadToEndAsync(token);var stderr=process.StandardError.ReadToEndAsync(token);
        try
        {
            await process.WaitForExitAsync(token);var output=await stdout;var error=await stderr;
            var phase=System.Text.RegularExpressions.Regex.Match(error,@"phase=([a-z-]{1,96})",System.Text.RegularExpressions.RegexOptions.CultureInvariant,TimeSpan.FromSeconds(1));
            Assert.True(process.ExitCode==0&&output.Contains("V09_GENUINE_WORKFLOW_BROWSER_PASSED",StringComparison.Ordinal),"Genuine W76 browser failed; phase="+(phase.Success?phase.Groups[1].Value:"withheld"));Assert.True(File.Exists(evidence));
        }
        finally{if(!process.HasExited){process.Kill(true);await process.WaitForExitAsync(CancellationToken.None);}}
    }
}
