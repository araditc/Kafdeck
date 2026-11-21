using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Clusters;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class FleetSetupLiveTests
{
    [FleetSetupLiveFact]
    public async Task Dashboard_prepares_a_bare_host_installs_the_agent_and_verifies_fresh_metrics()
    {
        string Env(string name) => Environment.GetEnvironmentVariable("KAFDECK_SETUP_" + name)
            ?? throw new InvalidOperationException("Missing setup fixture setting " + name);
        using var identity = new ManagedProvisioningFixture();
        var origin = Environment.GetEnvironmentVariable("KAFDECK_SETUP_ORIGIN") ?? "https://127.0.0.1:18090";
        var profile = new ClusterProfile("test-cluster", Env("KAFKA").Split(','), KafkaSecurityProtocol.Plaintext, null, null);
        var options = new KafdeckOptions(new DeploymentOptions(origin, null, AccessMode.Oidc, null), [profile]);
        using var adapter = new ConfluentKafkaAdministrationAdapter([profile], new SecretResolver());
        using var rsa = RSA.Create(3072);
        var request = new CertificateRequest("CN=Kafdeck setup acceptance", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder(); san.AddIpAddress(IPAddress.Parse(new Uri(origin).Host));
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true));
        using var generated = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));
        var pfx = identity.StatePath + ".pfx";
        File.WriteAllBytes(pfx, generated.Export(X509ContentType.Pfx));
        using var certificate = X509CertificateLoader.LoadPkcs12FromFile(pfx, null, X509KeyStorageFlags.EphemeralKeySet);
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls(origin);
        builder.WebHost.ConfigureKestrel(server => server.ConfigureHttpsDefaults(https => https.ServerCertificate = certificate));
        builder.Configuration["Kestrel:Certificates:Default:Path"] = pfx;
        builder.Services.AddSingleton(TimeProvider.System);
        var database = identity.StatePath + ".monitoring";
        builder.Services.AddSingleton(new MonitoringSettings(database, []));
        builder.Services.AddSingleton<MonitoringStore>();
        builder.Services.AddSingleton<ISecurityAuditSink>(identity.Audit);
        builder.Services.AddSingleton(identity.Store);
        builder.Services.AddSingleton<IKafkaAdministrationPort>(adapter);
        builder.Services.AddSingleton(new ClusterExplorerService(adapter, new KafkaSnapshotCoordinator()));
        var policy = AuthorizationPolicyCompiler.Compile(new AuthorizationPolicyDefinition([], [], []));
        builder.Services.AddSingleton(new KafdeckAuthorizationService(options, new AuthorizationPolicyEvaluator(policy)) { ManagedIdentity = identity.Store });
        builder.Services.AddKafdeckAntiforgery(origin);
        await using var app = builder.Build();
        var actor = OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity(
            [new Claim("sub", identity.OwnerId.ToString("D"))], "oidc")), ManagedProvisioningFixture.Issuer, null, DateTimeOffset.UtcNow);
        app.UseRouting(); app.Use((context, next) => { context.User = actor; return next(context); });
        app.UseAntiforgery(); app.MapKafdeckAntiforgeryEndpoint();
        var files = new PhysicalFileProvider(Environment.GetEnvironmentVariable("KAFDECK_HOST_UI_DIST")!);
        app.UseDefaultFiles(new DefaultFilesOptions { FileProvider = files });
        app.UseStaticFiles(new StaticFileOptions { FileProvider = files });
        app.MapGet("/api/v1/identity/users", () => new { users = identity.Store.Read().Users });
        app.MapGet("/api/v1/clusters/test-cluster", async (ClusterExplorerService explorer) => new { data = await explorer.GetClusterAsync("test-cluster") });
        var hostOptions = new BrokerHostOptions();
        var fleet = new FleetSettingsStore(identity.StatePath + ".fleet.sqlite", FleetKeyRing.Open(identity.StatePath + ".keys"), hostOptions, new(1, 0, [], [], []));
        hostOptions.Runtime = fleet.Options;
        var registry = new BrokerHostStore(identity.StatePath + ".hosts.json", fleet);
        var ssh = new BrokerHostSsh(hostOptions, fleet);
        var recovery = new BrokerAgentRecoveryStore(identity.StatePath + ".agent-operations.json");
        app.MapFleetSettings(identity.Store, fleet, recovery);
        app.MapFleetBootstrap(identity.Store, hostOptions, fleet, registry, ssh, new FleetBootstrapSsh(), recovery);
        app.MapBrokerHosts(identity.Store, hostOptions, registry, ssh);
        app.MapBrokerLogs(identity.Store, hostOptions, registry, ssh);
        app.MapBrokerAgents(identity.Store, hostOptions, registry, ssh, recovery);
        KafkaMetricSources.Map(app, options);
        await app.StartAsync();
        try
        {
            var start = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
            start.ArgumentList.Add(Env("BROWSER_SCRIPT")); start.ArgumentList.Add(origin);
            using var browser = Process.Start(start)!;
            var output = browser.StandardOutput.ReadToEndAsync(); var errors = browser.StandardError.ReadToEndAsync();
            using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(8));
            try { await browser.WaitForExitAsync(deadline.Token); }
            finally { if (!browser.HasExited) browser.Kill(entireProcessTree: true); }
            Assert.True(browser.ExitCode == 0, await output + await errors);
            Assert.Single(registry.Read().Bindings);
            Assert.Empty(Assert.Single(fleet.Read().Operations!).Payload);
            var temporarySecret = Environment.GetEnvironmentVariable("KAFDECK_SETUP_PASSWORD") ?? File.ReadAllText(Env("ADMIN_KEY"));
            Assert.DoesNotContain(temporarySecret, JsonSerializer.Serialize(fleet.Read()));
            Assert.Equal(2, fleet.Read().Credentials.Length);
            Assert.Null(recovery.Find("test-cluster"));
            Assert.Contains(identity.Audit.Events, item => item.ReasonCategory == "fleet_host_preparation_verified");
            Assert.Contains(identity.Audit.Events, item => item.ReasonCategory == "agent_recovery_observed");
        }
        finally { await app.StopAsync(); V11BrokerAgentTests.ClearMonitoringPool(database); }
    }
}

internal sealed class FleetSetupLiveFactAttribute : FactAttribute
{
    public FleetSetupLiveFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("KAFDECK_RUN_FLEET_SETUP_LIVE") != "1")
            Skip = "Requires an explicitly provisioned bare SSH/Kafka host and rendered browser fixture.";
    }
}
