using System.Diagnostics;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Cli;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class G2DataJobTheoryAttribute:TheoryAttribute
{
    public G2DataJobTheoryAttribute()
    {
        if(new[]{"KAFDECK_KEYCLOAK_G2_LAB","KAFDECK_G2_KAFKA_BOOTSTRAP","KAFDECK_G2_KAFKA_DESTINATION_BOOTSTRAP"}
            .Any(name=>string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name))))
            Skip="Requires genuine Keycloak and two physical Kafka clusters; no synthetic worker/provider acceptance.";
    }
}

public sealed class V08G2DataJobUncertaintyCliLiveTests
{
    [G2DataJobTheory]
    [InlineData(false,false)] [InlineData(false,true)] [InlineData(true,false)] [InlineData(true,true)]
    public async Task Real_CLI_two_human_reconciliation_resumes_physically_fenced_worker_once_and_rejects_actual_append(bool generator,bool postgres)
    {
        if(postgres&&string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")))
            throw new InvalidOperationException("Enabled data-worker acceptance requires real PostgreSQL.");
        using var deadline=new CancellationTokenSource(TimeSpan.FromMinutes(8));var token=deadline.Token;
        var evidence=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY")??Path.GetTempPath();
        var directory=Path.Combine(evidence,"data-worker-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        await using var database=postgres?await G2CliPostgreSqlDatabase.CreateAsync():null;
        await using var lab=new G2HumanCliLaboratory(Path.Combine(directory,"api"));
        var actions=new[]{"DataJobPlan","DataJobExecute","DataGeneratorPlan","DataGeneratorExecute","RecordProduce","MutationReconcile"};
        lab.RegisterHumans(actions,["SystemRead","ClusterRead","TopicRead","RecordRead","RecordExport"],actions,resource:"*");
        var source=Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!;
        var destination=Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_DESTINATION_BOOTSTRAP")!;
        for(var i=0;i<2;i++)
        {
            lab.Settings[$"Kafdeck:Clusters:{i}:Id"]=i==0?"source":"destination";
            lab.Settings[$"Kafdeck:Clusters:{i}:BootstrapServers:0"]=i==0?source:destination;
            lab.Settings[$"Kafdeck:Clusters:{i}:SecurityProtocol"]="Plaintext";
        }
        lab.Settings["Kafdeck:Generator:EnabledClusterIds:0"]="destination";
        lab.Settings["Kafdeck:Administration:Mutations:Enabled"]="true";
        lab.Settings["Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled"]="false";
        var digestFile=Path.Combine(directory,"digest-private");await File.WriteAllTextAsync(digestFile,Convert.ToHexString(RandomNumberGenerator.GetBytes(32)),token);
        lab.Settings["Kafdeck:Administration:Mutations:MaterialDigestKey"]="file:"+digestFile;
        lab.Settings["Kafdeck:Administration:Mutations:Persistence:Provider"]=postgres?"PostgreSql":"Sqlite";
        lab.Settings["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"]="Standalone";
        IMutationDbConnectionFactory connections;
        if(database is null)
        {
            var path=Path.Combine(directory,"mutations.db");lab.Settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"]=path;
            connections=new SqliteMutationDbConnectionFactory(path);
        }
        else
        {
            var file=Path.Combine(directory,"connection-private");await File.WriteAllTextAsync(file,database.ConnectionString,token);
            lab.Settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"]="file:"+file;
            connections=new PostgreSqlMutationDbConnectionFactory(database.ConnectionString);
        }
        var operations=new AdoMutationOperationRepository(connections);var fleet=new AdoFleetMutationStateStore(connections);
        var suffix=Guid.NewGuid().ToString("N");var sourceTopic="g2-worker-source-"+suffix;
        var targets=new[]{"g2-worker-positive-"+suffix,"g2-worker-negative-"+suffix};
        using var sourceAdmin=Admin(source);using var destinationAdmin=Admin(destination);
        await sourceAdmin.CreateTopicsAsync([Topic(sourceTopic)]);
        await destinationAdmin.CreateTopicsAsync(targets.Select(Topic));
        var payload=new byte[]{0,255,17,42};var key=new byte[]{7,0,9};
        using(var writer=new ProducerBuilder<byte[],byte[]>(new ProducerConfig{BootstrapServers=source,Acks=Acks.All}).Build())
        {
            var headers=new Headers();headers.Add("duplicate",[1]);headers.Add("duplicate",[2]);
            await writer.ProduceAsync(new TopicPartition(sourceTopic,0),new Message<byte[],byte[]>{Key=key,Value=payload,Headers=headers},token);
        }
        await lab.StartAsync(token);
        var completed=0;var appendDenied=0;var exactActors=0;var workerPids=new List<int>();
        for(var scenario=0;scenario<2;scenario++)
        {
            object body=generator?new {destinationClusterId="destination",destinationProfileVersion="g2-owned-v1",destinationTopic=targets[scenario],destinationPartition=0,
                recordCount=1,seed=123,source=new{kind="BuiltInTemplate",template="BasicJsonV1"},budget=new{maxBatchRecords=1,maxTotalRecords=1,maxDurationSeconds=120}}:
                new {sourceClusterId="source",sourceProfileVersion="g2-owned-v1",destinationClusterId="destination",destinationProfileVersion="g2-owned-v1",
                    ranges=new[]{new{sourceTopic,sourcePartition=0,destinationTopic=targets[scenario],destinationPartition=0,startInclusive=0,endExclusive=1}},
                    budget=new{maxBatchRecords=1,maxTotalRecords=1,maxDurationSeconds=120}};
            var family=generator?"generator":"data-jobs";var planCommand=generator?"preview":"forward-preview";
            var parent=await PreviewAsync(lab,[family,planCommand],body,token);
            Assert.Equal(CliApplication.Success,await lab.RunAsync(token,family,"start",parent.Id.ToString("D")));
            Assert.Equal(MutationOperationState.AppliedVerified,(await operations.GetAsync(parent.Id,token))!.State);
            await Task.Delay(300,token);Assert.Equal(0,Watermarks(destination,targets[scenario]).High.Value);
            await using(var child=await WorkerAsync(directory,lab.ConfigurationFile,parent.Id,generator,true,token))
            {
                await WaitAsync(()=>Task.FromResult(File.Exists(child.SignalFile)),child.Process,token);
                var progress=await fleet.GetProgressAsync(parent.Id,token);Assert.NotNull(progress);
                var boundary=generator?progress.Generator?.PendingBatch?.DestinationBoundary:progress.Transfer?.PendingBatch?.DestinationBoundary;
                Assert.NotNull(boundary);Assert.Equal(0,boundary.HighWatermark);Assert.Equal(0,Watermarks(destination,targets[scenario]).High.Value);
                Assert.Equal("DispatchStarted",generator?progress.Generator!.PendingBatch!.State.ToString():progress.Transfer!.PendingBatch!.State.ToString());
                workerPids.Add(child.Process.Id);await child.KillAsync(token);Assert.True(child.Process.HasExited);
            }
            await lab.RestartAsync(token);
            var interrupted=await fleet.GetProgressAsync(parent.Id,token);Assert.NotNull(interrupted);
            while(interrupted.WorkerLeaseExpiresAtUtc>DateTimeOffset.UtcNow) await Task.Delay(100,token);
            var batch=generator?interrupted.Generator!.PendingBatch!.BatchId:interrupted.Transfer!.PendingBatch!.BatchId;
            if(scenario==1)
            {
                using var writer=new ProducerBuilder<byte[],byte[]>(new ProducerConfig{BootstrapServers=destination,Acks=Acks.All}).Build();
                await writer.ProduceAsync(new TopicPartition(targets[scenario],0),new Message<byte[],byte[]>{Value=[5,6,7]},token);
                Assert.Equal(1,Watermarks(destination,targets[scenario]).High.Value);
            }
            var proof=Hash(JsonSerializer.Serialize(new {parent=parent.Id,batch,physicalBeforeEnqueueBarrier=true,
                physicallyExitedWorkerPid=workerPids[^1],observedDestinationHigh=Watermarks(destination,targets[scenario]).High.Value}));
            var fence=Hash("Observed OS process exited before independent provider observation: "+workerPids[^1]);
            var request=await lab.RequestFileAsync(new{batchId=batch,expectedProgressVersion=interrupted.Version,proofReferenceSha256=proof,fencedWorkerEvidenceSha256=fence},token);
            var args=new[]{family,"reconcile-preview",parent.Id.ToString("D"),"--request-file",request,"--idempotency-key","g2-reconcile-"+Guid.NewGuid().ToString("N")};
            if(scenario==1)
            {
                Assert.Equal(CliApplication.Conflict,await lab.RunAsync(token,args));
                var denied=await fleet.GetProgressAsync(parent.Id,token);Assert.NotNull(denied);
                Assert.Equal(interrupted.Version,denied.Version);
                Assert.Equal(batch,generator?denied.Generator!.PendingBatch!.BatchId:denied.Transfer!.PendingBatch!.BatchId);
                Assert.Equal(1,Watermarks(destination,targets[scenario]).High.Value);appendDenied++;continue;
            }
            Assert.Equal(CliApplication.Success,await lab.RunAsync(token,args));
            using var planned=JsonDocument.Parse(lab.Output);var reviewId=planned.RootElement.GetProperty("operationId").GetGuid();
            var reviewHash=planned.RootElement.GetProperty("previewHash").GetString()!;
            Assert.Equal("fleetUncertaintyDisposition",planned.RootElement.GetProperty("operationKind").GetString());
            Assert.Equal("critical",planned.RootElement.GetProperty("riskClass").GetString());
            var confirm=new List<string>{"operations","confirm",reviewId.ToString("D"),reviewHash};
            if(planned.RootElement.TryGetProperty("confirmationChallenge",out var challenge)&&challenge.ValueKind==JsonValueKind.String) confirm.AddRange(["--challenge",challenge.GetString()!]);
            Assert.Equal(CliApplication.PendingApproval,await lab.RunAsync(token,confirm.ToArray()));
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,await lab.RunAsync(token,"operations","approve",reviewId.ToString("D"),reviewHash));
            lab.Approver=true;Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"operations","approve",reviewId.ToString("D"),reviewHash));lab.Approver=false;
            Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"operations","execute",reviewId.ToString("D")));
            var reviewed=await operations.GetAsync(reviewId,token);Assert.NotNull(reviewed);
            Assert.Equal(MutationOperationState.AppliedVerified,reviewed.State);Assert.NotEqual(reviewed.RequesterPrincipalId,reviewed.ApprovedByPrincipalId);
            Assert.Contains(lab.Field("humanSubject"),Uri.UnescapeDataString(reviewed.RequesterPrincipalId),StringComparison.Ordinal);
            Assert.Contains(lab.Field("approverSubject"),Uri.UnescapeDataString(reviewed.ApprovedByPrincipalId!),StringComparison.Ordinal);exactActors++;
            var reconciled=await fleet.GetProgressAsync(parent.Id,token);Assert.NotNull(reconciled);
            Assert.Null(generator?reconciled.Generator!.PendingBatch:(object?)reconciled.Transfer!.PendingBatch);
            Assert.True(reconciled.WorkerGeneration>interrupted.WorkerGeneration);
            await using(var resumed=await WorkerAsync(directory,lab.ConfigurationFile,parent.Id,generator,false,token))
            {
                await WaitAsync(async()=>(await fleet.GetProgressAsync(parent.Id,token))?.Phase==FleetProgressPhase.Completed,resumed.Process,token);
            }
            var durable=await new AdoFleetMutationStateStore(connections).GetProgressAsync(parent.Id,token);Assert.NotNull(durable);
            Assert.Equal(1,generator?durable.Generator!.AcknowledgedRecords:durable.Transfer!.AcknowledgedRecords);
            using(var reader=new ConsumerBuilder<byte[],byte[]>(new ConsumerConfig{BootstrapServers=destination,GroupId="g2-proof-"+Guid.NewGuid().ToString("N"),EnableAutoCommit=false,AutoOffsetReset=AutoOffsetReset.Earliest}).Build())
            {
                reader.Assign(new TopicPartitionOffset(targets[scenario],0,0));var result=reader.Consume(TimeSpan.FromSeconds(10));Assert.NotNull(result);
                if(generator)
                {
                    using var json=JsonDocument.Parse(result.Message.Value);Assert.Equal(0,json.RootElement.GetProperty("sequence").GetInt64());
                    Assert.Equal(unchecked(123*397*397),json.RootElement.GetProperty("seed").GetInt32());
                }
                else
                {
                    Assert.Equal(key,result.Message.Key);Assert.Equal(payload,result.Message.Value);Assert.Equal(2,result.Message.Headers.Count);
                    Assert.Equal(new byte[]{1},result.Message.Headers[0].GetValueBytes());Assert.Equal(new byte[]{2},result.Message.Headers[1].GetValueBytes());
                    Assert.Equal(1,Assert.Single(durable.Transfer!.Checkpoints).NextSourceOffset);
                }
                reader.Close();
            }
            await lab.RestartAsync(token);Assert.Equal(1,Watermarks(destination,targets[scenario]).High.Value);completed++;
        }
        await File.WriteAllTextAsync(Path.Combine(evidence,$"{(postgres?"postgres":"sqlite")}-{(generator?"generator":"data-job")}-uncertainty-evidence.json"),
            JsonSerializer.Serialize(new {actualKeycloak=true,actualKafkaClusters=2,actualApiAndCli=true,persistence=postgres?"PostgreSQL":"SQLite",
                producer=generator?"DataGenerator":"DataJob",physicallyKilledBeforeActualEnqueue=workerPids.Count,reviewedResumeExactlyOnce=completed,
                actualAppendDenials=appendDenied,exactDistinctHumanActors=exactActors,apiProcessRestarts=3,realPKCELogins=lab.CompletedLogins,
                rawByteAndOrderedHeaderReadback=!generator,deterministicGeneratedReadback=generator,
                limitation="Test-only typed producer barrier before real enqueue; all production worker state/guards/observations are real. This does not prove Kafka broker HA."}),token);
    }

    private static async Task<(Guid Id,string Hash)> PreviewAsync(G2HumanCliLaboratory lab,string[] command,object request,CancellationToken token)
    {
        var file=await lab.RequestFileAsync(request,token);var args=command.Concat(new[]{"--request-file",file,"--idempotency-key","g2-job-"+Guid.NewGuid().ToString("N")}).ToArray();
        var exit=await lab.RunAsync(token,args);Assert.True(exit is CliApplication.Success or CliApplication.PendingApproval,"Real data-job preview failed.");
        using var json=JsonDocument.Parse(lab.Output);var id=json.RootElement.GetProperty("operationId").GetGuid();var hash=json.RootElement.GetProperty("previewHash").GetString()!;
        var confirmation=new List<string>{"operations","confirm",id.ToString("D"),hash};
        if(json.RootElement.TryGetProperty("confirmationChallenge",out var challenge)&&challenge.ValueKind==JsonValueKind.String)confirmation.AddRange(["--challenge",challenge.GetString()!]);
        var independent=json.RootElement.GetProperty("requiresIndependentApproval").GetBoolean();
        Assert.Equal(independent?CliApplication.PendingApproval:CliApplication.Success,await lab.RunAsync(token,confirmation.ToArray()));
        if(independent)
        {
            lab.Approver=true;
            try {Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"operations","approve",id.ToString("D"),hash));}
            finally {lab.Approver=false;}
        }
        return(id,hash);
    }
    private static TopicSpecification Topic(string name)=>new(){Name=name,NumPartitions=1,ReplicationFactor=1};
    private static IAdminClient Admin(string bootstrap)=>new AdminClientBuilder(new AdminClientConfig{BootstrapServers=bootstrap,SocketTimeoutMs=10000}).Build();
    private static WatermarkOffsets Watermarks(string bootstrap,string topic)
    {using var reader=new ConsumerBuilder<byte[],byte[]>(new ConsumerConfig{BootstrapServers=bootstrap,GroupId="g2-watermark-"+Guid.NewGuid().ToString("N"),EnableAutoCommit=false}).Build();return reader.QueryWatermarkOffsets(new(topic,0),TimeSpan.FromSeconds(10));}
    private static string Hash(string value)=>Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    private static async Task WaitAsync(Func<Task<bool>> ready,Process process,CancellationToken token)
    {while(!await ready()){if(process.HasExited)throw new InvalidOperationException("Owned worker process exited before its expected durable stage.");await Task.Delay(25,token);}}
    private static async Task<WorkerChild> WorkerAsync(string directory,string settings,Guid operationId,bool generator,bool hold,CancellationToken token)
    {
        var root=new DirectoryInfo(AppContext.BaseDirectory);while(!File.Exists(Path.Combine(root.FullName,"Kafdeck.slnx")))root=root.Parent!;
        var build=typeof(V08G2DataJobUncertaintyCliLiveTests).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration??"Debug";
        var signal=Path.Combine(directory,"before-enqueue-"+Guid.NewGuid().ToString("N")+".json");var config=Path.Combine(directory,"worker-"+Guid.NewGuid().ToString("N")+".json");
        await File.WriteAllBytesAsync(config,JsonSerializer.SerializeToUtf8Bytes(new{SettingsFile=settings,OperationId=operationId,Generator=generator,HoldBeforeEnqueue=hold,SignalFile=signal}),token);
        var start=new ProcessStartInfo("dotnet"){UseShellExecute=false,CreateNoWindow=true,RedirectStandardOutput=true,RedirectStandardError=true};
        start.ArgumentList.Add(Path.Combine(root.FullName,"tests/Kafdeck.NotificationSender.Lab/bin",build,"net10.0/Kafdeck.NotificationSender.Lab.dll"));
        start.ArgumentList.Add("data-worker");start.ArgumentList.Add(config);
        return new(Process.Start(start)??throw new InvalidOperationException("Owned worker failed to start."),signal);
    }
    private sealed class WorkerChild(Process process,string signal):IAsyncDisposable
    {
        internal Process Process {get;}=process;internal string SignalFile {get;}=signal;
        private readonly Task<string> _output=process.StandardOutput.ReadToEndAsync();private readonly Task<string> _error=process.StandardError.ReadToEndAsync();
        internal async Task KillAsync(CancellationToken token){if(!Process.HasExited){Process.Kill(true);await Process.WaitForExitAsync(token);}}
        public async ValueTask DisposeAsync(){await KillAsync(CancellationToken.None);Assert.DoesNotContain("Bearer eyJ",await _output+await _error,StringComparison.Ordinal);Process.Dispose();}
    }
}
