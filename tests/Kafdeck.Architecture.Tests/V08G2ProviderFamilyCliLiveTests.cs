using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Cli;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class G2ProviderFamilyFactAttribute : FactAttribute
{
    public G2ProviderFamilyFactAttribute()
    {
        if(new[]{"KAFDECK_KEYCLOAK_G2_LAB","KAFDECK_G2_KAFKA_BOOTSTRAP","KAFDECK_G2_SCHEMA_REGISTRY_URL","KAFDECK_G2_CONNECT_URL"}
            .Any(name=>string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name))))
            Skip="Requires genuine Keycloak, Kafka, Schema Registry and Kafka Connect laboratory processes; synthetic provider effects are not acceptance.";
    }
}

public sealed class V08G2ProviderFamilyCliLiveTests
{
    [G2ProviderFamilyFact]
    public Task Real_Keycloak_CLI_records_consumer_schema_and_connect_effects_survive_SQLite_repository_reopen()=>RunAsync(false);

    [G2ProviderFamilyFact]
    public Task Real_Keycloak_CLI_records_consumer_schema_and_connect_effects_survive_PostgreSQL_repository_reopen()=>RunAsync(true);

    private static async Task RunAsync(bool postgres)
    {
        if(postgres&&string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")))
            throw new InvalidOperationException("PostgreSQL is mandatory for the enabled provider-family laboratory.");
        using var deadline=new CancellationTokenSource(TimeSpan.FromMinutes(12));var token=deadline.Token;
        var evidence=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY")??Path.GetTempPath();
        var directory=Path.Combine(evidence,"provider-families-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        await using var database=postgres?await G2CliPostgreSqlDatabase.CreateAsync():null;
        await using var lab=new G2HumanCliLaboratory(Path.Combine(directory,"api"));
        var actions=new[]{"RecordProduce","RecordsPurge","ConsumerOffsetAlter","ConsumerDelete","SchemaCreate","SchemaAlter","SchemaDelete","ConnectCreate","ConnectAlter","ConnectDelete","ConnectRestart","ConnectAutoRestartManage","DataJobPlan","DataJobExecute","DataGeneratorPlan","DataGeneratorExecute","MutationReconcile"};
        lab.RegisterHumans(actions,["SystemRead","ClusterRead","TopicRead","RecordRead","RecordExport","ConsumerRead","SchemaRead","ConnectRead"],actions,"permitted","*");
        foreach(var grant in lab.Settings.Where(item=>item.Key.EndsWith(":Action",StringComparison.Ordinal)&&item.Value is "ClusterRead" or "SchemaRead" or "ConnectRead").ToArray())
            lab.Settings.Remove(grant.Key[..^"Action".Length]+"ResourcePatterns:0");
        var settings=lab.Settings; var broker=Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!;
        var registry=Environment.GetEnvironmentVariable("KAFDECK_G2_SCHEMA_REGISTRY_URL")!;
        var connect=Environment.GetEnvironmentVariable("KAFDECK_G2_CONNECT_URL")!;
        settings["Kafdeck:Clusters:0:Id"]="permitted";settings["Kafdeck:Clusters:0:BootstrapServers:0"]=broker;
        settings["Kafdeck:Clusters:0:SecurityProtocol"]="Plaintext";
        settings["Kafdeck:Clusters:0:SchemaRegistry:Url"]=registry;
        settings["Kafdeck:Clusters:0:Connect:Url"]=connect;
        settings["Kafdeck:Clusters:0:Connect:MutationProviderProfile"]="ConfluentCompatibleV1";
        settings["Kafdeck:Administration:Mutations:Enabled"]="true";
        settings["Kafdeck:Administration:ConnectAutoRestart:Enabled"]="true";
        settings["Kafdeck:Administration:ConnectAutoRestart:MaxAttempts"]="1";
        settings["Kafdeck:Administration:ConnectAutoRestart:InitialBackoffSeconds"]="5";
        settings["Kafdeck:Administration:ConnectAutoRestart:MaxBackoffSeconds"]="5";
        settings["Kafdeck:Administration:ConnectAutoRestart:ActivationLifetimeSeconds"]="60";
        settings["Kafdeck:Administration:ConnectAutoRestart:JitterBasisPoints"]="0";
        settings["Kafdeck:Administration:Mutations:MaterialDigestKey"]="env:KAFDECK_G2_FAMILY_DIGEST";
        lab.Secret("KAFDECK_G2_FAMILY_DIGEST",Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32)));
        settings["Kafdeck:Administration:Mutations:Persistence:Provider"]=postgres?"PostgreSql":"Sqlite";
        settings["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"]="Standalone";
        var mutationPath=Path.Combine(directory,"mutations.db");
        if(database is null)settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"]=mutationPath;
        else {settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"]="env:KAFDECK_G2_FAMILY_CONNECTION";lab.Secret("KAFDECK_G2_FAMILY_CONNECTION",database.ConnectionString);}
        var suffix=Guid.NewGuid().ToString("N");var topic="g2-family-"+suffix;var group="g2-family-group-"+suffix;
        using var admin=new AdminClientBuilder(new AdminClientConfig{BootstrapServers=broker,SocketTimeoutMs=10000}).Build();
        await admin.CreateTopicsAsync([new TopicSpecification{Name=topic,NumPartitions=1,ReplicationFactor=1}],new CreateTopicsOptions{RequestTimeout=TimeSpan.FromSeconds(15)});
        await lab.StartAsync(token);
        var operations=new List<Guid>();
        async Task<Guid> PreviewAsync(string[] command,object body,bool useRequestFile=true)
        {
            // Empty-payload preview contracts (for example connector delete) must
            // not receive --request-file; the shipped CLI intentionally rejects it.
            var payload=useRequestFile?new[]{"--request-file",await lab.RequestFileAsync(body,token)}:Array.Empty<string>();
            var args=command.Concat(payload).Concat(new[]{"--idempotency-key","g2-family-"+Guid.NewGuid().ToString("N")}).ToArray();
            var exit=await lab.RunAsync(token,args);
            Assert.True(exit is CliApplication.Success or CliApplication.PendingApproval,"Actual family preview failed; exit="+exit+"; "+lab.SafeOutcome);
            using var preview=JsonDocument.Parse(lab.Output);var id=preview.RootElement.GetProperty("operationId").GetGuid();
            var hash=preview.RootElement.GetProperty("previewHash").GetString()!;
            var confirmation=new List<string>{"operations","confirm",id.ToString("D"),hash};
            if(preview.RootElement.TryGetProperty("confirmationChallenge",out var challenge)&&challenge.ValueKind==JsonValueKind.String)
                confirmation.AddRange(["--challenge",challenge.GetString()!]);
            var needsApproval=preview.RootElement.GetProperty("requiresIndependentApproval").GetBoolean();
            Assert.Equal(needsApproval?CliApplication.PendingApproval:CliApplication.Success,await lab.RunAsync(token,confirmation.ToArray()));
            if(needsApproval)
            {
                Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,await lab.RunAsync(token,"operations","approve",id.ToString("D"),hash));
                lab.Approver=true;
                Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"operations","approve",id.ToString("D"),hash));
                lab.Approver=false;
            }
            operations.Add(id);return id;
        }
        async Task ExecuteAsync(Guid id,string[]? materialCommand=null,object? material=null)
        {
            var command=materialCommand??["operations","execute",id.ToString("D")];
            if(material is not null)command=command.Concat(new[]{"--request-file",await lab.RequestFileAsync(material,token)}).ToArray();
            var executionExit=await lab.RunAsync(token,command);
            Assert.True(executionExit==CliApplication.Success,
                "Physical family execution failed: exit="+executionExit+
                "; "+lab.SafeOutcome);
            using var result=JsonDocument.Parse(lab.Output);Assert.Equal("appliedVerified",result.RootElement.GetProperty("state").GetString());
        }
        var records=new {records=new[]{new{key=Convert.ToBase64String(Encoding.UTF8.GetBytes("g2-family-key")),value=Convert.ToBase64String(Encoding.UTF8.GetBytes("actual-family-record"))}}};
        var produce=await PreviewAsync(["records","produce-preview","permitted",topic],records);
        await ExecuteAsync(produce,["records","produce-execute",produce.ToString("D")],records);
        var readQuery=await lab.RequestFileAsync(new{},token);
        Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"views","v03-records-browse","permitted",topic,"0","--request-file",readQuery));
        Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"views","v03-records-export","permitted",topic,"0","--request-file",readQuery));
        Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"views","v07-schema-registry-capabilities","permitted"));
        using(var consumer=new ConsumerBuilder<string,string>(new ConsumerConfig{BootstrapServers=broker,GroupId=group,EnableAutoCommit=false,AutoOffsetReset=AutoOffsetReset.Earliest,SocketTimeoutMs=10000}).Build())
        {
            consumer.Assign(new TopicPartitionOffset(topic,0,Offset.Beginning));
            var read=consumer.Consume(TimeSpan.FromSeconds(15));Assert.NotNull(read);Assert.Equal("actual-family-record",read.Message.Value);
            consumer.Commit([new TopicPartitionOffset(topic,0,read.Offset+1)]);consumer.Close();
        }
        var reset=await PreviewAsync(["consumer-groups","offsets-preview","permitted",group],new{targets=new[]{new{topicName=topic,partition=0,selector=new{kind="earliest"}}}});
        await ExecuteAsync(reset);
        using(var observer=new ConsumerBuilder<string,string>(new ConsumerConfig{BootstrapServers=broker,GroupId=group,EnableAutoCommit=false}).Build())
            Assert.Equal(0,Assert.Single(observer.Committed([new TopicPartition(topic,0)],TimeSpan.FromSeconds(10))).Offset.Value);
        var purge=await PreviewAsync(["records","purge-preview","permitted"],new{targets=new[]{new{topicName=topic,partition=0,selector=new{kind="absolute",beforeOffset=1}}}});
        await ExecuteAsync(purge);
        using(var observer=new ConsumerBuilder<string,string>(new ConsumerConfig{BootstrapServers=broker,GroupId=group+"-watermark",EnableAutoCommit=false}).Build())
            Assert.Equal(1,observer.QueryWatermarkOffsets(new(topic,0),TimeSpan.FromSeconds(10)).Low.Value);
        var deleteGroup=await PreviewAsync(["consumer-groups","delete-preview","permitted",group],new{mode="group"});await ExecuteAsync(deleteGroup);
        var groups=await admin.ListConsumerGroupsAsync(new ListConsumerGroupsOptions{RequestTimeout=TimeSpan.FromSeconds(10)});
        Assert.DoesNotContain(groups.Valid,g=>g.GroupId==group);

        var subject="g2-family-schema-"+suffix;const string schema="{\"type\":\"record\",\"name\":\"G2Family\",\"fields\":[{\"name\":\"value\",\"type\":\"string\"}]}";
        var register=await PreviewAsync(["schemas","register-preview","permitted",subject],new{format="avro",schema});
        await ExecuteAsync(register,["schemas","register-execute",register.ToString("D")],new{schema});
        using var registryClient=new HttpClient{BaseAddress=new Uri(registry),Timeout=TimeSpan.FromSeconds(10)};
        using(var actual=JsonDocument.Parse(await registryClient.GetStringAsync("subjects/"+Uri.EscapeDataString(subject)+"/versions/latest",token)))
            Assert.Equal(1,actual.RootElement.GetProperty("version").GetInt32());
        var alterSchema=await PreviewAsync(["schemas","compatibility-preview","permitted",subject],new{requestedMode="full"});await ExecuteAsync(alterSchema);
        using(var actual=JsonDocument.Parse(await registryClient.GetStringAsync("config/"+Uri.EscapeDataString(subject),token)))
            Assert.Equal("FULL",actual.RootElement.GetProperty("compatibilityLevel").GetString());
        var deleteSchema=await PreviewAsync(["schemas","delete-preview","permitted",subject],new{version=(int?)null,permanent=false});await ExecuteAsync(deleteSchema);
        using(var deleted=await registryClient.GetAsync("subjects/"+Uri.EscapeDataString(subject)+"/versions/latest",token))Assert.Equal(System.Net.HttpStatusCode.NotFound,deleted.StatusCode);

        var connector="g2-family-sink-"+suffix;var outputPath=Path.Combine(directory,"actual-connect-sink.txt");
        var config=new Dictionary<string,string>{{"name",connector},{"connector.class","org.apache.kafka.connect.file.FileStreamSinkConnector"},{"tasks.max","1"},{"topics",topic},{"file",outputPath}};
        var create=await PreviewAsync(["connect","create-preview","permitted",connector],new{configuration=config});
        await ExecuteAsync(create,["connect","configuration-execute",create.ToString("D")],new{configuration=config});
        using var connectClient=new HttpClient{BaseAddress=new Uri(connect),Timeout=TimeSpan.FromSeconds(10)};
        using(var actual=await connectClient.GetAsync("connectors/"+Uri.EscapeDataString(connector),token))Assert.True(actual.IsSuccessStatusCode);
        Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"views","v07-connect-profile-connectors-detail","permitted","default",connector));
        Assert.Equal(CliApplication.Success,await lab.RunAsync(token,"views","v07-connect-profile-plugins-list","permitted","default"));
        config["tasks.max"]="2";
        var update=await PreviewAsync(["connect","update-preview","permitted",connector],new{configuration=config});
        await ExecuteAsync(update,["connect","configuration-execute",update.ToString("D")],new{configuration=config});
        using(var actual=JsonDocument.Parse(await connectClient.GetStringAsync("connectors/"+Uri.EscapeDataString(connector)+"/config",token)))
            Assert.Equal("2",actual.RootElement.GetProperty("tasks.max").GetString());
        var pause=await PreviewAsync(["connect","control-preview","permitted",connector],new{action="pause"});
        await ExecuteAsync(pause,["connect","control-execute",pause.ToString("D")]);
        var resume=await PreviewAsync(["connect","control-preview","permitted",connector],new{action="resume"});
        await ExecuteAsync(resume,["connect","control-execute",resume.ToString("D")]);
        // This is an owned disposable sink. Opening a directory as an output file
        // gives the actual FileStream task a deterministic physical failure.
        config["file"]=directory;
        var failTask=await PreviewAsync(["connect","update-preview","permitted",connector],new{configuration=config});
        await ExecuteAsync(failTask,["connect","configuration-execute",failTask.ToString("D")],new{configuration=config});
        var failed=false;
        for(var attempt=0;attempt<100;attempt++)
        {
            using var status=JsonDocument.Parse(await connectClient.GetStringAsync("connectors/"+Uri.EscapeDataString(connector)+"/status",token));
            failed=status.RootElement.GetProperty("tasks").EnumerateArray().Any(task=>task.GetProperty("id").GetInt32()==0&&task.GetProperty("state").GetString()=="FAILED");
            if(failed)break;await Task.Delay(100,token);
        }
        Assert.True(failed,"The real Connect task must fail before automatic restart admission.");
        var auto=await PreviewAsync(["connect","auto-restart-preview","permitted","default",connector],new {
            enabled=true,taskId=0,maxAttempts=1,initialBackoffSeconds=5,maxBackoffSeconds=5,activationLifetimeSeconds=60,jitterBasisPoints=0});
        await ExecuteAsync(auto,["connect","auto-restart-apply","permitted","default",connector],new{operationId=auto,taskId=0});
        var attempted=false;
        Kafdeck.Infrastructure.Persistence.IMutationDbConnectionFactory stateConnections=database is null?
            new Kafdeck.Infrastructure.Persistence.SqliteMutationDbConnectionFactory(mutationPath):new Kafdeck.Infrastructure.Persistence.PostgreSqlMutationDbConnectionFactory(database.ConnectionString);
        var restartState=new Kafdeck.Infrastructure.Persistence.AdoConnectAutoRestartStateStore(stateConnections);
        Guid activationId;
        await using(var connection=await stateConnections.OpenAsync(token))
        await using(var command=connection.CreateCommand())
        {
            command.CommandText="SELECT activation_id FROM kafdeck_connect_auto_restart_activations";
            await using var rows=await command.ExecuteReaderAsync(token);
            Assert.True(await rows.ReadAsync(token));activationId=Guid.Parse(rows.GetString(0));
            Assert.False(await rows.ReadAsync(token));
        }
        for(var attempt=0;attempt<30;attempt++)
        {
            var activation=await restartState.GetAsync(activationId,token);
            if(activation is {AttemptsUsed:1}&&activation.Target.ConnectorName==connector&&activation.Target.TaskId==0)
            {attempted=true;break;}
            await Task.Delay(200,token);
        }
        Assert.True(attempted,"The shipped durable controller must observe the actual failed task and consume its single admitted restart attempt.");
        var deleteConnector=await PreviewAsync(["connect","delete-preview","permitted",connector],new{},useRequestFile:false);
        await ExecuteAsync(deleteConnector,["connect","control-execute",deleteConnector.ToString("D")]);
        using(var actual=await connectClient.GetAsync("connectors/"+Uri.EscapeDataString(connector),token))Assert.Equal(System.Net.HttpStatusCode.NotFound,actual.StatusCode);
        var repository=new Kafdeck.Infrastructure.Persistence.AdoMutationOperationRepository(database is null?
            new Kafdeck.Infrastructure.Persistence.SqliteMutationDbConnectionFactory(mutationPath):new Kafdeck.Infrastructure.Persistence.PostgreSqlMutationDbConnectionFactory(database.ConnectionString));
        foreach(var id in operations)
        {var saved=await repository.GetAsync(id,token);Assert.NotNull(saved);Assert.Equal(Kafdeck.Modules.Administration.MutationOperationState.AppliedVerified,saved.State);Assert.StartsWith("oidc-cli-human:",saved.RequesterPrincipalId,StringComparison.Ordinal);}
        await File.WriteAllBytesAsync(Path.Combine(evidence,(postgres?"postgres":"sqlite")+"-real-provider-family-cli-evidence.json"),JsonSerializer.SerializeToUtf8Bytes(new {
            realKeycloak=true,shippedCli=true,shippedApi=true,realKafka=true,realSchemaRegistry=true,realKafkaConnect=true,
            recordBytesReadBack=true,consumerOffsetReadBack=true,groupDeletionReadBack=true,purgeLowWatermarkReadBack=true,
            schemaVersionAndCompatibilityReadBack=true,connectorConfigAndDeletionReadBack=true,independentCriticalHumanApproval=true,
            actualFailedTaskObserved=true,automaticRestartAttempts=1,automaticRestartBudget=1,
            durableAppliedOperations=operations.Count,persistence=postgres?"PostgreSQL":"SQLite",completedPkceLogins=lab.CompletedLogins}),token);
    }
}
