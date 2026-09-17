using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Api;
using Kafdeck.Core;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Infrastructure.Security;
using Kafdeck.Modules.Clusters;
using Kafdeck.Modules.Topics;

Activity.DefaultIdFormat = ActivityIdFormat.W3C;
Activity.ForceDefaultIdFormat = true;

var builder = WebApplication.CreateBuilder(args);

var kafdeckOptions = KafdeckConfigurationLoader.Load(builder.Configuration);
KafdeckConfigurationValidator.ValidateAndThrow(kafdeckOptions);

builder.WebHost.UseUrls(kafdeckOptions.Deployment.ListenUrl);

var secretResolver = new SecretResolver();
var deploymentAccessToken = kafdeckOptions.Deployment.AccessToken is null
    ? null
    : secretResolver.Resolve(kafdeckOptions.Deployment.AccessToken).Reveal();

builder.Services.AddProblemDetails();
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
});

builder.Services.AddSingleton(kafdeckOptions);
builder.Services.AddSingleton(secretResolver);
builder.Services.AddSingleton<KafkaSnapshotPolicy>();
builder.Services.AddSingleton<KafkaSnapshotCoordinator>(services =>
    new KafkaSnapshotCoordinator(services.GetRequiredService<KafkaSnapshotPolicy>()));
builder.Services.AddSingleton<IKafkaAdministrationPort>(_ =>
    new ConfluentKafkaAdministrationAdapter(kafdeckOptions.Clusters, secretResolver));
builder.Services.AddSingleton<ClusterExplorerService>();
builder.Services.AddSingleton<TopicExplorerService>();
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

if (deploymentAccessToken is not null)
{
    app.UseMiddleware<DeploymentAccessTokenMiddleware>(deploymentAccessToken);
}

app.MapGet("/healthz", () => Results.Ok(new
    {
        status = "ok",
        product = ProductIdentity.Name,
    }))
    .WithName("healthz");

app.MapKafdeckV01(kafdeckOptions);

app.Run();

public partial class Program;
