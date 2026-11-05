using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Cli;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W74RequestLiveTests
{
    [KeycloakG2KafkaFact] public Task Genuine_Keycloak_SQLite_requester_reviewer_effect_cancel_revoke_and_restart()=>Exercise(false);
    [KeycloakG2KafkaFact] public Task Genuine_Keycloak_PostgreSQL_requester_reviewer_effect_cancel_revoke_and_restart()=>Exercise(true);
    private static async Task Exercise(bool postgres)
    {
        using var timeout=new CancellationTokenSource(TimeSpan.FromMinutes(20));var token=timeout.Token;
        var directory=Path.Combine(Path.GetTempPath(),"kafdeck-w74-live-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        await using var database=postgres?await G2CliPostgreSqlDatabase.CreateAsync():null;
        await using var lab=new G2HumanCliLaboratory(directory);var settings=lab.Settings;
        var bootstrap=Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!;
        using var broker=new AdminClientBuilder(new AdminClientConfig{BootstrapServers=bootstrap,SocketTimeoutMs=10000}).Build();
        var suffix=Guid.NewGuid().ToString("N")[..12];var approved="w74-approved-"+suffix;var cancelled="w74-cancelled-"+suffix;
        var absent="w74-absent-"+suffix;var revoked="w74-revoked-"+suffix;var browserTopic="w74-browser-"+suffix;
        var names=new[]{approved,cancelled,absent,revoked,browserTopic};
        settings["Kafdeck:Governance:CatalogEnabled"]="true";settings["Kafdeck:Governance:PoliciesEnabled"]="true";settings["Kafdeck:Governance:RequestsEnabled"]="true";
        settings["Kafdeck:Governance:DeploymentGeneration"]=Guid.NewGuid().ToString("D");settings["Kafdeck:Administration:Mutations:Enabled"]="true";
        settings["Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled"]="false";
        settings["Kafdeck:Administration:Mutations:Persistence:Provider"]=postgres?"PostgreSql":"Sqlite";
        settings["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"]="Standalone";
        var digest=Path.Combine(directory,"digest-private");await File.WriteAllTextAsync(digest,Convert.ToHexString(RandomNumberGenerator.GetBytes(32)),token);
        settings["Kafdeck:Administration:Mutations:MaterialDigestKey"]="file:"+digest;
        IMutationDbConnectionFactory connections;
        if(database is null){var path=Path.Combine(directory,"mutations.db");settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"]=path;connections=new SqliteMutationDbConnectionFactory(path);}
        else{var path=Path.Combine(directory,"connection-private");await File.WriteAllTextAsync(path,database.ConnectionString,token);settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"]="file:"+path;connections=new PostgreSqlMutationDbConnectionFactory(database.ConnectionString);}
        settings["Kafdeck:Clusters:0:Id"]="permitted";settings["Kafdeck:Clusters:0:BootstrapServers:0"]=bootstrap;settings["Kafdeck:Clusters:0:SecurityProtocol"]="Plaintext";
        var browser=!postgres&&Environment.GetEnvironmentVariable("KAFDECK_V09_REAL_BROWSER")=="1";
        if(browser)
        {
            settings["Kafdeck:Deployment:Oidc:ClientId"]=lab.Field("browserClientId");var root=Root();var dist=Path.Combine(root,"src/frontend/dist");
            Assert.True(File.Exists(Path.Combine(dist,"index.html")),"Actual compiled UI required.");
            foreach(var file in Directory.EnumerateFiles(dist,"*",SearchOption.AllDirectories)){var target=Path.Combine(directory,"wwwroot",Path.GetRelativePath(dist,file));Directory.CreateDirectory(Path.GetDirectoryName(target)!);File.Copy(file,target);}
        }
        lab.RegisterHumans(["TopicCreate","RequestSubmit","RequestDecide","GovernancePolicyManage","GovernancePolicyActivate","MutationReconcile"],
            ["SystemRead","ClusterRead","TopicList","TopicRead","CatalogRead","RequestRead","GovernancePolicyRead"],["GovernancePolicyActivate"],"permitted");
        var operations=new AdoMutationOperationRepository(connections);
        try
        {
            await lab.StartAsync(token);
            async Task<JsonElement> Success(params string[] args)
            {var exit=await lab.RunAsync(token,args);var phase=string.Join('.',args.Take(2).Select(value=>value.Length<=40&&value.All(c=>char.IsAsciiLetterLower(c)||c=='-')?value:"argument"));
                Assert.True(exit==CliApplication.Success,"W74 genuine CLI failed; phase="+phase+"; exit="+exit+"; "+lab.SafeOutcome);using var document=JsonDocument.Parse(lab.Output);return document.RootElement.Clone();}
            static Guid Id(JsonElement op)=>op.GetProperty("operationId").GetGuid();
            static string Hash(JsonElement op)=>op.GetProperty("previewHash").GetString()!;
            var metadataRecoveryExercised=false;
            async Task Commit(JsonElement op,bool critical=false)
            {
                var id=Id(op).ToString("D");var confirmation=new List<string>{"operations","confirm",id,Hash(op)};
                if(op.TryGetProperty("confirmationChallenge",out var challenge)&&challenge.ValueKind==JsonValueKind.String)confirmation.AddRange(["--challenge",challenge.GetString()!]);
                Assert.Equal(critical?CliApplication.PendingApproval:CliApplication.Success,await lab.RunAsync(token,confirmation.ToArray()));
                if(critical){lab.Approver=true;try{await Success("operations","approve",id,Hash(op));}finally{lab.Approver=false;}}
                await Success("operations","execute",id);
            }
            async Task<JsonElement> RequestPreview(string command,string? requestId,long version,Guid? target,string reason="bounded genuine request")
            {
                var expiresAtUtc=command is "Draft" or "Revise"&&target is not null?
                    (await operations.GetAsync(target.Value,token))!.PreviewExpiresAtUtc.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ",System.Globalization.CultureInfo.InvariantCulture):null;
                var file=await lab.RequestFileAsync(new{clusterId="permitted",requestId,expectedVersion=version,command,operationId=target,reason,expiresAtUtc},token);
                return await Success("requests","change-preview","permitted","--request-file",file,"--idempotency-key",Guid.NewGuid().ToString("N"));
            }
            async Task<string> DraftSubmitApprove(JsonElement target,bool approve=true)
            {
                var draft=await RequestPreview("Draft",null,0,Id(target));await Commit(draft);
                if(!metadataRecoveryExercised)
                {
                    // Explicit repository acknowledgement-loss fixture: the owning SQL receipt really committed.
                    var committed=await operations.GetAsync(Id(draft),token);Assert.NotNull(committed);Assert.Equal(MutationOperationState.AppliedVerified,committed.State);
                    Assert.Equal(MutationSaveOutcome.Saved,(await operations.TrySaveAsync(committed with{State=MutationOperationState.ExecutionUnknown,Version=committed.Version+1,
                        ResultCode="controlled_request_commit_acknowledgement_loss",SafeProviderEvidence=new Dictionary<string,string>()},committed.Version,token)).Outcome);
                    lab.Approver=true;
                    try{Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,await lab.RunAsync(token,"requests","recover",Id(draft).ToString("D")));}
                    finally{lab.Approver=false;}
                    await Success("requests","recover",Id(draft).ToString("D"));
                    await Success("requests","recover",Id(draft).ToString("D"));
                    var recovered=await operations.GetAsync(Id(draft),token);Assert.NotNull(recovered);Assert.Equal(MutationOperationState.AppliedVerified,recovered.State);
                    metadataRecoveryExercised=true;
                }
                var material=await Success("requests","plan",Id(draft).ToString("D"));var requestId=material.GetProperty("plan").GetProperty("requestId").GetString()!;
                var onlyDraft=await Success("requests","history","permitted",requestId);Assert.Single(onlyDraft.GetProperty("items").EnumerateArray());
                var submit=await RequestPreview("Submit",requestId,1,null);await Commit(submit);
                var selfFile=await lab.RequestFileAsync(new{clusterId="permitted",requestId,expectedVersion=2,command="Approve",reason="self approval must be denied"},token);
                Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,await lab.RunAsync(token,"requests","change-preview","permitted","--request-file",selfFile,"--idempotency-key",Guid.NewGuid().ToString("N")));
                if(approve){lab.Approver=true;try{var decision=await RequestPreview("Approve",requestId,2,null);await Commit(decision);}finally{lab.Approver=false;}}
                return requestId;
            }
            var policy=JsonSerializer.Serialize(new{schemaVersion=1,rules=new[]{new{id="require-genuine-business-approval",priority=1,
                scope=new{clusterId="permitted",resourceKind="topic",resourceIds=names,action="TopicCreate"},conditions=Array.Empty<object>(),outcome="RequireApprovedRequest"}}});
            var versionFile=await lab.RequestFileAsync(new{clusterId="permitted",document=policy},token);
            var version=await Success("policies","version-preview","permitted","--request-file",versionFile,"--idempotency-key",Guid.NewGuid().ToString("N"));await Commit(version);
            var plan=await Success("policies","plan",Id(version).ToString("D"));var versionId=plan.GetProperty("versionId").GetString()!;
            var activationFile=await lab.RequestFileAsync(new{clusterId="permitted",versionId,expectedRevision=0,enabled=true},token);
            var activation=await Success("policies","activate-preview","permitted","--request-file",activationFile,"--idempotency-key",Guid.NewGuid().ToString("N"));await Commit(activation,true);
            async Task<JsonElement> Target(string topic)
            {var op=await Success("topics","create-preview","permitted",topic,"1","1","--idempotency-key",Guid.NewGuid().ToString("N"));await Success("operations","confirm",Id(op).ToString("D"),Hash(op));return op;}
            var missing=await Target(absent);
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,await lab.RunAsync(token,"operations","execute",Id(missing).ToString("D")));
            Assert.Equal("RequestApprovalRequired",(await operations.GetAsync(Id(missing),token))!.ResultCode);
            Assert.DoesNotContain(broker.GetMetadata(TimeSpan.FromSeconds(10)).Topics,t=>t.Topic==absent&&t.Error.Code==ErrorCode.NoError);
            var actual=await Target(approved);var actualRequest=await DraftSubmitApprove(actual);
            await lab.StopAsync(token);await lab.StartAsync(token);
            var approvedData=await Success("requests","get","permitted",actualRequest);Assert.Equal("Approved",approvedData.GetProperty("businessState").GetString());
            await Success("operations","execute",Id(actual).ToString("D"));
            Assert.Contains(broker.GetMetadata(TimeSpan.FromSeconds(10)).Topics,t=>t.Topic==approved&&t.Error.Code==ErrorCode.NoError);
            var lateCancel=await RequestPreview("Cancel",actualRequest,3,null);
            await Success("operations","confirm",Id(lateCancel).ToString("D"),Hash(lateCancel));
            Assert.NotEqual(CliApplication.Success,await lab.RunAsync(token,"operations","execute",Id(lateCancel).ToString("D")));
            var after=await Success("requests","get","permitted",actualRequest);Assert.Equal("Approved",after.GetProperty("businessState").GetString());Assert.Equal("AppliedVerified",after.GetProperty("executionState").GetString());
            var cancellationTarget=await Target(cancelled);var cancellationRequest=await DraftSubmitApprove(cancellationTarget);
            await Commit(await RequestPreview("Cancel",cancellationRequest,3,null));
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,await lab.RunAsync(token,"operations","execute",Id(cancellationTarget).ToString("D")));
            Assert.DoesNotContain(broker.GetMetadata(TimeSpan.FromSeconds(10)).Topics,t=>t.Topic==cancelled&&t.Error.Code==ErrorCode.NoError);
            var revokeTarget=await Target(revoked);var revokeRequest=await DraftSubmitApprove(revokeTarget);
            await lab.StopAsync(token);
            var decisionPermissions=settings.Where(p=>p.Key.StartsWith("Kafdeck:Authorization:Roles:1:Permissions:",StringComparison.Ordinal)&&p.Key.EndsWith(":Action",StringComparison.Ordinal)&&p.Value=="RequestDecide").Select(p=>p.Key).ToArray();Assert.NotEmpty(decisionPermissions);
            foreach(var permission in decisionPermissions)settings[permission]="RequestRead";
            await lab.StartAsync(token);
            Assert.NotEqual(CliApplication.Success,await lab.RunAsync(token,"operations","execute",Id(revokeTarget).ToString("D")));
            Assert.DoesNotContain(broker.GetMetadata(TimeSpan.FromSeconds(10)).Topics,t=>t.Topic==revoked&&t.Error.Code==ErrorCode.NoError);
            await lab.StopAsync(token);foreach(var permission in decisionPermissions)settings[permission]="RequestDecide";await lab.StartAsync(token);
            var history=await Success("requests","history","permitted",actualRequest);Assert.Equal(3,history.GetProperty("items").GetArrayLength());
            if(browser)
            {
                var scenario=Path.Combine(directory,"browser-request-scenario-private.json");
                await File.WriteAllBytesAsync(scenario,JsonSerializer.SerializeToUtf8Bytes(new{kind="request",clusterId="permitted",topicName=browserTopic}),token);
                var evidence=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY")??directory;Directory.CreateDirectory(evidence);
                var start=new ProcessStartInfo("node"){UseShellExecute=false,CreateNoWindow=true,RedirectStandardOutput=true,RedirectStandardError=true,WorkingDirectory=Root()};
                foreach(var arg in new[]{Path.Combine(Root(),"src/frontend/scripts/v09-workflows-live-browser.cjs"),lab.Api.AbsoluteUri,Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")!,scenario,Path.Combine(evidence,"sqlite-w74-real-browser-evidence.json")})start.ArgumentList.Add(arg);
                using var process=Process.Start(start)??throw new InvalidOperationException("Actual request browser did not start.");var stdout=process.StandardOutput.ReadToEndAsync(token);var stderr=process.StandardError.ReadToEndAsync(token);
                await process.WaitForExitAsync(token);_=await stdout;_=await stderr;Assert.Equal(0,process.ExitCode);
                Assert.Contains(broker.GetMetadata(TimeSpan.FromSeconds(10)).Topics,t=>t.Topic==browserTopic&&t.Error.Code==ErrorCode.NoError);
            }
            var output=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY")??directory;Directory.CreateDirectory(output);
            await File.WriteAllBytesAsync(Path.Combine(output,(postgres?"postgres":"sqlite")+"-w74-request-evidence.json"),JsonSerializer.SerializeToUtf8Bytes(new{
                provider=postgres?"PostgreSQL":"SQLite",realKeycloak=true,actualShippedCliAndApi=true,actualKafka=true,realDistinctHumanBusinessApproval=true,selfApprovalDenied=true,
                missingRequestNoBrokerEffect=true,approvedRequestBrokerEffect=true,restartPreservesApproval=true,businessAndExecutionStatesSeparate=true,
                lateCancellationCannotClearEffect=true,cancelBeforeDispatchNoBrokerEffect=true,currentReviewerRevocationNoBrokerEffect=true,
                explicitMetadataCommitAckLossFixture=true,positiveRequestReceiptRecoveredThroughGenuineCli=true,wrongActorRecoveryDenied=true,noRecoveryRedispatch=true,
                genuineBrowser=browser,actualPkceLogins=lab.CompletedLogins,fixturePolicySeeding=false}),token);
        }
        finally
        {
            // Linux CI explicitly advertises native topic deletion. Windows fixtures
            // retain their isolated log directories until the whole owned broker stops.
            if(Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_DELETE_CAPABLE")=="1")
                try{await broker.DeleteTopicsAsync(names,new DeleteTopicsOptions{OperationTimeout=TimeSpan.FromSeconds(10),RequestTimeout=TimeSpan.FromSeconds(10)});}catch(DeleteTopicsException){}
        }
    }
    private static string Root(){var d=new DirectoryInfo(AppContext.BaseDirectory);while(!File.Exists(Path.Combine(d.FullName,"Kafdeck.slnx")))d=d.Parent!;return d.FullName;}
}
