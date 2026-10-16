using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Infrastructure.SchemaRegistry;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Generator;
using Kafdeck.Modules.Records;
using Kafdeck.Modules.Schemas;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kafdeck.NotificationSender.Lab;

// Test-only OS process. The sole fault seam blocks BEFORE calling the real
// typed Kafka producer. It never returns a fabricated provider acknowledgement.
internal static class DataJobWorkerMode
{
    internal sealed record Input(string SettingsFile,Guid OperationId,bool Generator,bool HoldBeforeEnqueue,string SignalFile);

    internal static async Task<int> RunAsync(string inputFile)
    {
        try
        {
            var bytes=await File.ReadAllBytesAsync(inputFile);
            if(bytes.Length>16384) return 2;
            var input=JsonSerializer.Deserialize<Input>(bytes,new JsonSerializerOptions {UnmappedMemberHandling=JsonUnmappedMemberHandling.Disallow})!;
            if(input.OperationId==Guid.Empty || !Path.IsPathFullyQualified(input.SettingsFile) || !Path.IsPathFullyQualified(input.SignalFile)) return 2;
            var configuration=new ConfigurationBuilder().AddJsonFile(input.SettingsFile,false,false).AddEnvironmentVariables().Build();
            using var configurationLifetime=(IDisposable)configuration;
            var options=KafdeckConfigurationLoader.Load(configuration);KafdeckConfigurationValidator.ValidateAndThrow(options);
            var secrets=new SecretResolver();var persistence=options.Administration!.Mutations!.Persistence!;
            IMutationDbConnectionFactory connections=persistence.Provider==MutationPersistenceProvider.Sqlite
                ?new SqliteMutationDbConnectionFactory(persistence.SqliteDatabasePath!)
                :new PostgreSqlMutationDbConnectionFactory(secrets.Resolve(persistence.ConnectionString!).Reveal());
            var services=new ServiceCollection();services.AddSingleton<IConfiguration>(configuration);
            services.AddSingleton(options);services.AddSingleton(secrets);services.AddKafdeckNonBrowserReads(configuration);
            using var policies=new ReloadingAuthorizationPolicy(configuration);
            services.AddSingleton(new AuthorizationPolicyEvaluator(policies.Current,()=>policies.Current));
            services.AddSingleton<KafdeckAuthorizationService>();services.AddSingleton<MutationApprovalAuthorizer>();
            services.AddSingleton<IMutationOperationRepository>(new AdoMutationOperationRepository(connections));
            services.AddSingleton<IFleetMutationStateStore>(new AdoFleetMutationStateStore(connections));
            services.AddSingleton<IKafkaAdministrationPort>(new ConfluentKafkaAdministrationAdapter(options.Clusters,secrets));
            services.AddSingleton<IKafkaRecordReadPort>(new ConfluentKafkaRecordReadAdapter(options.Clusters,secrets));
            services.AddSingleton<IRecordMaskingPolicyProvider>(new StaticRecordMaskingPolicyProvider(
                RecordMaskingPolicyCompiler.Compile(options.Records?.MaskingPolicy??new RecordMaskingPolicyDefinition("default",1))));
            services.AddSingleton<ISchemaCatalogReadPort>(new ConfluentSchemaCatalogReadAdapter(options.Clusters,secrets));
            services.AddSingleton<SchemaDiffService>();services.AddSingleton<SchemaExplorerService>();services.AddSingleton<SchemaDeveloperService>();
            services.AddSingleton<IDestinationDispatchObserver>(new ConfluentDestinationDispatchObserver(options.Clusters,secrets));
            services.AddSingleton<IClusterTransferProducePort>(new BeforeEnqueueProducer(
                new ConfluentKafkaClusterTransferProduceAdapter(options.Clusters,secrets),input));
            services.AddSingleton<ClusterTransferPlanner>();services.AddSingleton<ClusterTransferSourceReader>();
            services.AddSingleton<GovernedDataJobPlanner>();services.AddSingleton<GovernedDataJobPreconditionValidator>();
            services.AddSingleton<GovernedDataJobStateCoordinator>();services.AddSingleton<GovernedDataJobSourceReader>();
            services.AddSingleton<IGovernedDataJobEffectGuard,ConfiguredGovernedDataJobEffectGuard>();
            services.AddSingleton<GovernedDataJobDispatchCoordinator>();
            services.AddSingleton(new GovernedDataJobWorkerPolicy(leaseTtl:TimeSpan.FromSeconds(6)));
            services.AddSingleton<GovernedDataJobWorker>();
            services.AddSingleton(new DataGeneratorDeploymentPolicy((options.Generator?.EnabledClusterIds??[]).ToHashSet(StringComparer.Ordinal)));
            services.AddSingleton<DataGeneratorPlanner>();services.AddSingleton<DataGeneratorPreconditionValidator>();
            services.AddSingleton<DataGeneratorStateCoordinator>();services.AddSingleton<DataGeneratorMaterializer>();
            services.AddSingleton<IDataGeneratorEffectGuard,ConfiguredDataGeneratorEffectGuard>();
            services.AddSingleton<DataGeneratorDispatchCoordinator>();
            services.AddSingleton(new DataGeneratorWorkerPolicy(leaseTtl:TimeSpan.FromSeconds(6)));services.AddSingleton<DataGeneratorWorker>();
            await using var provider=services.BuildServiceProvider();
            using var deadline=new CancellationTokenSource(TimeSpan.FromMinutes(2));var token=deadline.Token;
            var operations=provider.GetRequiredService<IMutationOperationRepository>();
            var operation=await operations.GetAsync(input.OperationId,token)??throw new InvalidOperationException();
            if(operation.State!=MutationOperationState.AppliedVerified || operation.OperationKind!=(input.Generator?MutationOperationKind.DataGenerator:MutationOperationKind.DataJob)) return 3;
            for(var cycle=0;cycle<1000;cycle++)
            {
                if(input.Generator) await provider.GetRequiredService<DataGeneratorWorker>().ProcessOperationOnceAsync(operation,token);
                else await provider.GetRequiredService<GovernedDataJobWorker>().ProcessOperationOnceAsync(operation,token);
                var progress=await provider.GetRequiredService<IFleetMutationStateStore>().GetProgressAsync(input.OperationId,token);
                if(progress?.Phase==FleetProgressPhase.Completed) return 0;
                await Task.Delay(100,token);
            }
            return 4;
        }
        catch(Exception error)
        {
            // Closed diagnostic only: no exception text, request material, configuration or secret paths.
            Console.Error.WriteLine("DATA_WORKER_FAILURE type="+error.GetType().Name);return 1;
        }
    }

    private sealed class BeforeEnqueueProducer(IClusterTransferProducePort real,Input input):IClusterTransferProducePort
    {
        public async Task<MutationProviderResult> ProduceAsync(ClusterTransferProduceMutation request,CancellationToken token=default)
        {
            if(input.HoldBeforeEnqueue)
            {
                await File.WriteAllBytesAsync(input.SignalFile,JsonSerializer.SerializeToUtf8Bytes(new {
                    operationId=input.OperationId,processId=Environment.ProcessId,beforeActualEnqueue=true,observedAtUtc=DateTimeOffset.UtcNow }),token);
                await Task.Delay(Timeout.InfiniteTimeSpan,token);
                throw new InvalidOperationException("Unreachable before-enqueue laboratory barrier.");
            }
            return await real.ProduceAsync(request,token);
        }
    }
}
