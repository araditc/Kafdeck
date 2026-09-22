using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Api;
using Kafdeck.Core;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Ecosystem;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Infrastructure.SchemaRegistry;
using Kafdeck.Infrastructure.Security;
using Kafdeck.Modules.Clusters;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;
using Kafdeck.Modules.Consumers;
using Kafdeck.Modules.Records;
using Kafdeck.Modules.Schemas;
using Kafdeck.Modules.Topics;

Activity.DefaultIdFormat = ActivityIdFormat.W3C;
Activity.ForceDefaultIdFormat = true;

var builder = WebApplication.CreateBuilder(args);

var kafdeckOptions = KafdeckConfigurationLoader.Load(builder.Configuration);
KafdeckConfigurationValidator.ValidateAndThrow(kafdeckOptions);
var mutationOptions = kafdeckOptions.Administration?.Mutations;
var maskingPolicy = RecordMaskingPolicyCompiler.Compile(
    kafdeckOptions.Records?.MaskingPolicy ??
    new RecordMaskingPolicyDefinition("default", 1));

builder.WebHost.UseUrls(kafdeckOptions.Deployment.ListenUrl);

var secretResolver = new SecretResolver();

var deploymentAccessToken =
    DeploymentAccessModePolicy.UsesDeploymentToken(kafdeckOptions.Deployment.Mode) &&
    kafdeckOptions.Deployment.AccessToken is not null
        ? secretResolver.Resolve(kafdeckOptions.Deployment.AccessToken).Reveal()
        : null;

if (kafdeckOptions.Deployment.Mode == AccessMode.Oidc)
{
    builder.Services.AddKafdeckOidc(kafdeckOptions.Deployment, secretResolver);
    builder.Services.AddKafdeckAntiforgery(kafdeckOptions.Deployment.ListenUrl);
}

builder.Services.AddProblemDetails();
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
});

builder.Services.AddSingleton(kafdeckOptions);
builder.Services.AddSingleton(secretResolver);
builder.Services.AddSingleton(maskingPolicy);
var authorizationPolicy = AuthorizationPolicyCompiler.Compile(
    AuthorizationPolicyConfigurationLoader.Load(builder.Configuration));
builder.Services.AddSingleton(authorizationPolicy);
builder.Services.AddSingleton<AuthorizationPolicyEvaluator>();
builder.Services.AddSingleton<KafdeckAuthorizationService>();
builder.Services.AddSingleton<ISecurityAuditSink, LoggingSecurityAuditSink>();
builder.Services.AddSingleton<KafkaSnapshotPolicy>();
builder.Services.AddSingleton<KafkaSnapshotCoordinator>(services =>
    new KafkaSnapshotCoordinator(services.GetRequiredService<KafkaSnapshotPolicy>()));
builder.Services.AddSingleton<IKafkaAdministrationPort>(_ =>
    new ConfluentKafkaAdministrationAdapter(kafdeckOptions.Clusters, secretResolver));
builder.Services.AddSingleton<IKafkaRecordReadPort>(_ =>
    new ConfluentKafkaRecordReadAdapter(kafdeckOptions.Clusters, secretResolver));
builder.Services.AddSingleton<IConsumerGroupReadPort>(_ =>
    new ConfluentKafkaConsumerGroupReadAdapter(kafdeckOptions.Clusters, secretResolver));
builder.Services.AddSingleton<ConsumerExplorerService>();
builder.Services.AddSingleton<IMetricsObservationPort, UnavailableMetricsObservationPort>();
builder.Services.AddSingleton<IHistoryObservationPort, UnavailableHistoryObservationPort>();
builder.Services.AddSingleton<ConsumerDiagnosticsService>();
builder.Services.AddSingleton<IRecordSchemaReadPort>(_ =>
    new ConfluentSchemaRegistryReadAdapter(kafdeckOptions.Clusters, secretResolver));
builder.Services.AddSingleton<ISchemaCatalogReadPort>(_ =>
    new ConfluentSchemaCatalogReadAdapter(kafdeckOptions.Clusters, secretResolver));
builder.Services.AddSingleton<SchemaDiffService>();
builder.Services.AddSingleton<SchemaExplorerService>();
builder.Services.AddSingleton<IRecordDecodePort>(services =>
    new ConfluentRecordDecoder(services.GetRequiredService<IRecordSchemaReadPort>()));
builder.Services.AddSingleton<RecordFilterService>();
builder.Services.AddSingleton<RecordLiveTailService>();
builder.Services.AddSingleton<RecordMaskingService>();
builder.Services.AddSingleton<RecordExportService>();
builder.Services.AddSingleton<ClusterExplorerService>();
builder.Services.AddSingleton<TopicExplorerService>();
builder.Services.AddSingleton<IConnectReadPort>(_ =>
    new KafkaConnectReadAdapter(kafdeckOptions.Clusters, secretResolver));
builder.Services.AddSingleton<IKsqlMetadataReadPort>(_ =>
    new KsqlDbMetadataReadAdapter(kafdeckOptions.Clusters, secretResolver));
builder.Services.AddSingleton<ITopicCatalogProvider>(_ =>
    new ConfigurationTopicCatalogProvider(kafdeckOptions));
builder.Services.AddSingleton<ApiTelemetry>();

if (mutationOptions?.Enabled == true)
{
    var persistence = mutationOptions.Persistence ??
                      throw new KafdeckConfigurationException(
                          "Mutation persistence must be configured when mutation mode is enabled.");

    builder.Services.AddSingleton<IMutationMaterialDigestService>(services =>
        new HmacMutationMaterialDigestService(
            services.GetRequiredService<SecretResolver>()
                .Resolve(mutationOptions.MaterialDigestKey!)
                .Reveal()));

    builder.Services.AddSingleton<IMutationDbConnectionFactory>(services =>
        persistence.Provider switch
        {
            MutationPersistenceProvider.Sqlite =>
                new SqliteMutationDbConnectionFactory(persistence.SqliteDatabasePath!),
            MutationPersistenceProvider.PostgreSql =>
                new PostgreSqlMutationDbConnectionFactory(
                    services.GetRequiredService<SecretResolver>()
                        .Resolve(persistence.ConnectionString!)
                        .Reveal()),
            _ => throw new KafdeckConfigurationException(
                "Unsupported mutation persistence provider."),
        });

    builder.Services.AddSingleton<IMutationOperationRepository>(services =>
        new AdoMutationOperationRepository(
            services.GetRequiredService<IMutationDbConnectionFactory>()));
    builder.Services.AddSingleton<IMutationAuditSink, LoggingMutationAuditSink>();
    builder.Services.AddSingleton<MutationApprovalAuthorizer>();
    builder.Services.AddSingleton<MutationRequestAuthorizationService>();
    builder.Services.AddSingleton<MutationAdmissionService>();
    builder.Services.AddSingleton<MutationCommandService>();
    builder.Services.AddSingleton<MutationExecutionRequestContextAccessor>();
    builder.Services.AddSingleton<MutationDispatchService>();

    builder.Services.AddSingleton<TopicMutationPlanner>();
    builder.Services.AddSingleton<TopicMutationPreconditionValidator>();
    builder.Services.AddSingleton<ITopicMutationPort>(_ =>
        new ConfluentKafkaTopicMutationAdapter(
            kafdeckOptions.Clusters,
            secretResolver));
    builder.Services.AddSingleton<TopicMutationExecutionService>();
    builder.Services.AddSingleton<IMutationExecutionHandler, TopicCreateExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, TopicAlterExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, TopicIncreasePartitionsExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, TopicDeleteExecutionHandler>();

    builder.Services.AddSingleton<RecordProductionPlanner>();
    builder.Services.AddSingleton<RecordProductionPreconditionValidator>();
    builder.Services.AddSingleton<IRecordProduceMutationPort>(_ =>
        new ConfluentKafkaRecordProduceAdapter(
            kafdeckOptions.Clusters,
            secretResolver));
    builder.Services.AddSingleton<RecordProductionExecutionService>();
    builder.Services.AddSingleton<IMutationExecutionHandler, RecordProduceExecutionHandler>();

    builder.Services.AddSingleton<IConsumerMutationObservationPort>(_ =>
        new ConfluentKafkaConsumerMutationObservationAdapter(
            kafdeckOptions.Clusters,
            secretResolver));
    builder.Services.AddSingleton<IConsumerMutationPort>(_ =>
        new ConfluentKafkaConsumerMutationAdapter(
            kafdeckOptions.Clusters,
            secretResolver));
    builder.Services.AddSingleton<ConsumerMutationPlanner>();
    builder.Services.AddSingleton<ConsumerMutationPreconditionValidator>();
    builder.Services.AddSingleton<ConsumerMutationExecutionService>();
    builder.Services.AddSingleton<IMutationExecutionHandler, ConsumerOffsetAlterExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, ConsumerDeleteExecutionHandler>();

    builder.Services.AddSingleton<ConfluentSchemaMutationAdapter>(_ =>
        new ConfluentSchemaMutationAdapter(
            kafdeckOptions.Clusters,
            secretResolver));
    builder.Services.AddSingleton<ISchemaMutationPort>(services =>
        services.GetRequiredService<ConfluentSchemaMutationAdapter>());
    builder.Services.AddSingleton<ISchemaMutationObservationPort>(services =>
        services.GetRequiredService<ConfluentSchemaMutationAdapter>());
    builder.Services.AddSingleton<SchemaMutationPlanner>();
    builder.Services.AddSingleton<SchemaMutationPreconditionValidator>();
    builder.Services.AddSingleton<SchemaMutationExecutionService>();
    builder.Services.AddSingleton<IMutationExecutionHandler, SchemaCreateExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, SchemaAlterExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, SchemaDeleteExecutionHandler>();

    builder.Services.AddSingleton<KafkaConnectMutationAdapter>(_ =>
        new KafkaConnectMutationAdapter(
            kafdeckOptions.Clusters,
            secretResolver));
    builder.Services.AddSingleton<IConnectMutationPort>(services =>
        services.GetRequiredService<KafkaConnectMutationAdapter>());
    builder.Services.AddSingleton<IConnectMutationObservationPort>(services =>
        services.GetRequiredService<KafkaConnectMutationAdapter>());
    builder.Services.AddSingleton<ConnectMutationPlanner>();
    builder.Services.AddSingleton<ConnectMutationPreconditionValidator>();
    builder.Services.AddSingleton<ConnectMutationExecutionService>();
    builder.Services.AddSingleton<IMutationExecutionHandler, ConnectCreateExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, ConnectAlterExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, ConnectDeleteExecutionHandler>();

    builder.Services.AddSingleton<IRecordsPurgeObservationPort>(_ =>
        new ConfluentKafkaRecordsPurgeObservationAdapter(
            kafdeckOptions.Clusters,
            secretResolver));
    builder.Services.AddSingleton<IRecordsPurgeMutationPort>(_ =>
        new ConfluentKafkaRecordsPurgeAdapter(
            kafdeckOptions.Clusters,
            secretResolver));
    builder.Services.AddSingleton<RecordsPurgePlanner>();
    builder.Services.AddSingleton<RecordsPurgePreconditionValidator>();
    builder.Services.AddSingleton<RecordsPurgeExecutionService>();
    builder.Services.AddSingleton<IMutationExecutionHandler, RecordsPurgeExecutionHandler>();

    builder.Services.AddSingleton<IMutationPreDispatchGuard, W39MutationPreDispatchGuard>();
    builder.Services.AddSingleton<MutationExecutionHandlerRegistry>(services =>
        new MutationExecutionHandlerRegistry(
            services.GetServices<IMutationExecutionHandler>()));
    builder.Services.AddSingleton(
        new MutationExecutorPolicy(
            mutationOptions.MaxConcurrentPerCluster,
            TimeSpan.FromSeconds(30),
            TimeSpan.FromMinutes(2)));
    builder.Services.AddSingleton<MutationExecutor>();
    builder.Services.AddSingleton<MutationRecoveryCoordinator>();
    builder.Services.AddHostedService<MutationRecoveryHostedService>();
}

var app = builder.Build();

app.Logger.LogInformation(
    "Kafdeck startup configuration: {@Configuration}",
    SafeConfigurationDiagnostics.Create(kafdeckOptions));

if (mutationOptions?.Enabled == true)
{
    _ = app.Services.GetRequiredService<IMutationMaterialDigestService>();
    var mutationRepository = app.Services.GetRequiredService<IMutationOperationRepository>();
    await mutationRepository.InitializeAsync().ConfigureAwait(false);

    var mutationRecovery = app.Services.GetRequiredService<MutationRecoveryCoordinator>();
    var recoveredMutations = await mutationRecovery
        .RecoverInterruptedExecutionsAsync(
            ignoreActiveExecutionLeases:
                mutationOptions.Persistence!.ExecutionMode == MutationExecutionMode.Standalone)
        .ConfigureAwait(false);

    app.Logger.LogInformation(
        "Kafdeck mutation runtime reconciled {RecoveredMutationCount} interrupted execution(s).",
        recoveredMutations);

    app.Logger.LogInformation(
        "Kafdeck mutation runtime initialized with governed W39 topic, record-production, consumer, schema, Connect and controlled-purge dispatch, persistence provider {PersistenceProvider} and execution mode {ExecutionMode}.",
        mutationOptions.Persistence!.Provider,
        mutationOptions.Persistence.ExecutionMode);
}

foreach (var cluster in kafdeckOptions.Clusters.Where(cluster =>
             cluster.SecurityProtocol is KafkaSecurityProtocol.Plaintext or KafkaSecurityProtocol.SaslPlaintext))
{
    app.Logger.LogWarning(
        "Cluster profile {ClusterId} uses insecure Kafka transport protocol {SecurityProtocol}.",
        cluster.Id,
        cluster.SecurityProtocol);
}

app.UseExceptionHandler();
app.UseMiddleware<ApiTelemetryMiddleware>();

if (kafdeckOptions.Deployment.Mode == AccessMode.Oidc)
{
    app.UseAuthentication();
}

app.UseDefaultFiles();
app.UseStaticFiles();

if (deploymentAccessToken is not null)
{
    app.UseWhen(
        context => context.Request.Path.StartsWithSegments("/api"),
        branch => branch.UseMiddleware<DeploymentAccessTokenMiddleware>(deploymentAccessToken));
}

if (kafdeckOptions.Deployment.Mode == AccessMode.Oidc)
{
    app.UseWhen(
        context =>
            context.Request.Path.StartsWithSegments("/api/v1") &&
            !context.Request.Path.StartsWithSegments("/api/v1/auth/login"),
        branch => branch.UseMiddleware<OidcApiAuthenticationBoundaryMiddleware>());
}

app.MapGet("/healthz", () => Results.Ok(new
    {
        status = "ok",
        product = ProductIdentity.Name,
    }))
    .WithName("healthz");

if (kafdeckOptions.Deployment.Mode == AccessMode.Oidc)
{
    app.MapKafdeckOidcSessionEndpoints();
    app.MapKafdeckAntiforgeryEndpoint();
}

app.MapKafdeckV01(kafdeckOptions);
app.MapKafdeckRecordEndpoints(kafdeckOptions);
app.MapKafdeckV04ReadViews(kafdeckOptions);
if (mutationOptions?.Enabled == true)
{
    app.MapKafdeckMutationEndpoints();
    app.MapKafdeckTopicMutationEndpoints();
    app.MapKafdeckRecordProductionEndpoints();
    app.MapKafdeckConsumerMutationEndpoints();
    app.MapKafdeckSchemaMutationEndpoints();
    app.MapKafdeckConnectMutationEndpoints();
    app.MapKafdeckRecordsPurgeEndpoints();
}
app.MapFallbackToFile("index.html");

app.Run();

public partial class Program;
