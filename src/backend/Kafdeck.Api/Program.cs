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
using Kafdeck.Infrastructure.SchemaRegistry;
using Kafdeck.Infrastructure.Security;
using Kafdeck.Modules.Clusters;
using Kafdeck.Modules.Consumers;
using Kafdeck.Modules.Records;
using Kafdeck.Modules.Schemas;
using Kafdeck.Modules.Topics;

Activity.DefaultIdFormat = ActivityIdFormat.W3C;
Activity.ForceDefaultIdFormat = true;

var builder = WebApplication.CreateBuilder(args);

var kafdeckOptions = KafdeckConfigurationLoader.Load(builder.Configuration);
KafdeckConfigurationValidator.ValidateAndThrow(kafdeckOptions);
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

var app = builder.Build();

app.Logger.LogInformation(
    "Kafdeck startup configuration: {@Configuration}",
    SafeConfigurationDiagnostics.Create(kafdeckOptions));

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
}

app.MapKafdeckV01(kafdeckOptions);
app.MapKafdeckRecordEndpoints(kafdeckOptions);
app.MapKafdeckV04ReadViews(kafdeckOptions);
app.MapFallbackToFile("index.html");

app.Run();

public partial class Program;
