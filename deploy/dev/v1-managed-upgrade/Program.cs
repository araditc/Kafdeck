// Bounded compatibility fixture, not a product command or provider-effect proof.
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;

if (args[0] == "kafka")
{
    var bootstrap=args[1];
    using var admin=new AdminClientBuilder(new AdminClientConfig {BootstrapServers=bootstrap}).Build();
    var names=Enumerable.Range(0,20).Select(i=>$"g06-reference-{i:00}").ToArray();
    var existing=admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Select(t=>t.Topic).ToHashSet(StringComparer.Ordinal);
    var missing=names.Where(n=>!existing.Contains(n)).Select(n=>new TopicSpecification {Name=n,NumPartitions=3,ReplicationFactor=1}).ToArray();
    if(missing.Length>0)await admin.CreateTopicsAsync(missing);
    using var producer=new ProducerBuilder<string,string>(new ProducerConfig {BootstrapServers=bootstrap,Acks=Acks.All}).Build();
    for(var i=0;i<100;i++)await producer.ProduceAsync(names[i%names.Length],new Message<string,string> {Key=i.ToString(System.Globalization.CultureInfo.InvariantCulture),Value=JsonSerializer.Serialize(new{id=i,message="g06-e01-reference"})});
    for(var i=0;i<5;i++)
    {
        using var consumer=new ConsumerBuilder<string,string>(new ConsumerConfig {BootstrapServers=bootstrap,GroupId=$"g06-reference-group-{i}",AutoOffsetReset=AutoOffsetReset.Earliest,EnableAutoCommit=false}).Build();
        consumer.Subscribe(names[i]);var item=consumer.Consume(TimeSpan.FromSeconds(10));if(item is null)throw new InvalidOperationException("Reference group did not consume its seeded record.");consumer.Commit(item);consumer.Close();
    }
    Console.WriteLine(JsonSerializer.Serialize(new{topics=20,partitionsPerTopic=3,records=100,groups=5,actualKafka=true}));return;
}

var verb=args[0];var provider=args[1];var location=args[2];var report=args[3];
IMutationDbConnectionFactory factory=provider switch
{
    "sqlite"=>new SqliteMutationDbConnectionFactory(location),
    "postgres"=>new PostgreSqlMutationDbConnectionFactory(File.ReadAllText(location).Trim()),
    _=>throw new ArgumentException("Explicit fixture database is required.")
};
if(verb=="backup")
{
    if(provider!="sqlite")throw new ArgumentException("Use pg_dump/pg_restore for the PostgreSQL fixture.");
    using var source=new SqliteConnection(new SqliteConnectionStringBuilder {DataSource=location,Mode=SqliteOpenMode.ReadOnly,Pooling=false}.ConnectionString);
    using var target=new SqliteConnection(new SqliteConnectionStringBuilder {DataSource=report,Pooling=false}.ConnectionString);
    source.Open();target.Open();source.BackupDatabase(target);using var check=target.CreateCommand();check.CommandText="PRAGMA integrity_check";
    if((string?)check.ExecuteScalar()!="ok")throw new InvalidDataException("Backup integrity failed.");return;
}
if(verb=="seed")
{
    var repository=new AdoMutationOperationRepository(factory);await repository.InitializeAsync();var now=DateTimeOffset.UtcNow;
    foreach(var label in new[]{"pending","approved","unknown"})
    {
        var operation=MutationOperation.CreatePreview(args[4],
            new(MutationOperationKind.TopicCreate,args[5],"{\"name\":\"g06-"+label+"\"}",["topic:"+args[5]+":g06-"+label]),
            new(MutationRiskClass.Moderate,["g06-synthetic-compatibility-fixture"],MutationConfirmationMode.Explicit,label!="unknown"),
            "g06-format-freeze",now.AddMinutes(20),now,"g06-"+label);
        operation.OpenForConfirmation(now);operation.Confirm(operation.Snapshot.RequesterPrincipalId,operation.Snapshot.PreviewHash,now,operation.Snapshot.ConfirmationChallenge);
        var snapshot=operation.Snapshot;
        if(label=="approved")snapshot=snapshot with {State=MutationOperationState.Ready,ApprovedByPrincipalId=args[6],ApprovedAtUtc=now,ApprovalAuthorizationEvidenceHash=new string('a',64),Version=snapshot.Version+1};
        if(label=="unknown")
        { operation.ClaimExecution(now,now.AddSeconds(30));operation.MarkDispatchStarted(now);operation.Complete(MutationExecutionResultKind.ExecutionUnknown,"G06FixtureUnknown",now);snapshot=operation.Snapshot; }
        await repository.CreateAsync(snapshot);
    }
}
else if(verb!="observe")throw new ArgumentException("Unknown fixture phase.");

await using var connection=await factory.OpenAsync();var rows=new List<object>();
using(var command=connection.CreateCommand())
{
    command.CommandText="SELECT operation_id,state,snapshot_json FROM kafdeck_mutation_operations ORDER BY operation_id";
    using var reader=await command.ExecuteReaderAsync();while(await reader.ReadAsync())rows.Add(new{id=reader.GetString(0),state=((MutationOperationState)reader.GetInt32(1)).ToString(),snapshotSha256=Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(reader.GetString(2))))});
}
var versions=new List<object>();using(var command=connection.CreateCommand())
{
    command.CommandText="SELECT component,schema_version FROM kafdeck_schema_info ORDER BY component";
    using var reader=await command.ExecuteReaderAsync();while(await reader.ReadAsync())versions.Add(new{component=reader.GetString(0),version=reader.GetInt32(1)});
}
var result=JsonSerializer.Serialize(new{provider,syntheticCompatibilityStates=true,providerEffectsClaimed=false,rows,versions},new JsonSerializerOptions {WriteIndented=true});
await File.WriteAllTextAsync(report,result);Console.WriteLine("Observed "+rows.Count+" compatibility snapshots for "+provider+".");
