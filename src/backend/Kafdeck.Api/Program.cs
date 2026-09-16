using Kafdeck.Core;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Security;

var builder = WebApplication.CreateBuilder(args);

var kafdeckOptions = KafdeckConfigurationLoader.Load(builder.Configuration);
KafdeckConfigurationValidator.ValidateAndThrow(kafdeckOptions);

builder.WebHost.UseUrls(kafdeckOptions.Deployment.ListenUrl);

var secretResolver = new SecretResolver();
var deploymentAccessToken = kafdeckOptions.Deployment.AccessToken is null
    ? null
    : secretResolver.Resolve(kafdeckOptions.Deployment.AccessToken).Reveal();

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

if (deploymentAccessToken is not null)
{
    app.UseMiddleware<DeploymentAccessTokenMiddleware>(deploymentAccessToken);
}

app.MapGet("/healthz", () => Results.Ok(new
{
    status = "ok",
    product = ProductIdentity.Name,
}));

app.Run();

public partial class Program;
