using System.Text.Json;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Kafdeck.Infrastructure.Security;
using Microsoft.AspNetCore.Http;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class ConfigurationSecurityBoundaryTests
{
    [Fact]
    public void Loopback_binding_does_not_require_deployment_token()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            Array.Empty<ClusterProfile>());

        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }

    [Fact]
    public void Non_loopback_binding_without_token_fails_closed()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions("http://0.0.0.0:8080", null),
            Array.Empty<ClusterProfile>());

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains("requires an access-token", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("secret:abc")]
    [InlineData("env:")]
    [InlineData("env:NOT-VALID")]
    [InlineData("file:relative/path")]
    public void Invalid_secret_references_fail_validation(string value)
    {
        Assert.Throws<KafdeckConfigurationException>(() => SecretReference.Parse(value));
    }


    [Fact]
    public void Loader_prefers_listen_urls_and_preserves_legacy_primary_url()
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://127.0.0.1:9999",
            ["Kafdeck:Deployment:ListenUrls:0"] = "http://127.0.0.1:8080",
            ["Kafdeck:Deployment:ListenUrls:1"] = "http://192.168.10.20:8080",
            ["Kafdeck:Deployment:AccessMode"] = "Token",
            ["Kafdeck:Deployment:AccessToken"] = "env:KAFDECK_DEPLOYMENT_TOKEN",
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

        var options = KafdeckConfigurationLoader.Load(configuration);

        Assert.Equal("http://127.0.0.1:8080", options.Deployment.ListenUrl);
        Assert.Equal(
            new[]
            {
                "http://127.0.0.1:8080",
                "http://192.168.10.20:8080",
            },
            options.Deployment.ListenUrls);

        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }

    [Fact]
    public void Local_mode_rejects_any_remote_endpoint_in_multi_binding()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions(
                "http://127.0.0.1:8080",
                null,
                AccessMode.Local,
                null,
                new[] { "http://192.168.10.20:8080" }),
            Array.Empty<ClusterProfile>());

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains(
            "Non-loopback deployment binding requires",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Token_mode_accepts_wildcard_binding_and_uses_wildcard_host_policy()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions(
                "http://0.0.0.0:8080",
                SecretReference.Parse("env:KAFDECK_DEPLOYMENT_TOKEN"),
                AccessMode.Token,
                null),
            Array.Empty<ClusterProfile>());

        KafdeckConfigurationValidator.ValidateAndThrow(options);

        Assert.Equal(new[] { "*" }, DeploymentHostPolicy.BuildAllowedHosts(options.Deployment));
    }

    [Fact]
    public void Concrete_multi_binding_derives_exact_allowed_hosts()
    {
        var deployment = new DeploymentOptions(
            "http://127.0.0.1:8080",
            SecretReference.Parse("env:KAFDECK_DEPLOYMENT_TOKEN"),
            AccessMode.Token,
            null,
            new[]
            {
                "http://192.168.10.20:8080",
                "http://kafdeck.internal:8080",
            });

        var hosts = DeploymentHostPolicy.BuildAllowedHosts(deployment);

        Assert.Contains("127.0.0.1", hosts);
        Assert.Contains("192.168.10.20", hosts);
        Assert.Contains("kafdeck.internal", hosts);
        Assert.DoesNotContain("*", hosts);
    }

    [Fact]
    public void Oidc_requires_https_for_every_remote_listen_url()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions(
                "https://127.0.0.1:8443",
                null,
                AccessMode.Oidc,
                new OidcProfile(
                    "https://idp.example",
                    "kafdeck",
                    null,
                    "groups",
                    new[] { "openid", "profile" }),
                new[] { "http://192.168.10.20:8080" }),
            Array.Empty<ClusterProfile>());

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains(
            "Every non-loopback OIDC deployment binding requires HTTPS",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Legacy_deployment_options_infer_local_or_token_mode()
    {
        var local = new DeploymentOptions("http://127.0.0.1:8080", null);
        var token = new DeploymentOptions(
            "http://0.0.0.0:8080",
            SecretReference.Parse("env:KAFDECK_DEPLOYMENT_TOKEN"));

        Assert.Equal(AccessMode.Local, local.Mode);
        Assert.Equal(AccessMode.Token, token.Mode);
    }

    [Fact]
    public void Loader_preserves_v01_token_mode_when_access_mode_is_omitted()
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://0.0.0.0:8080",
            ["Kafdeck:Deployment:AccessToken"] = "env:KAFDECK_DEPLOYMENT_TOKEN",
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

        var options = KafdeckConfigurationLoader.Load(configuration);

        Assert.Equal(AccessMode.Token, options.Deployment.Mode);
        Assert.NotNull(options.Deployment.AccessToken);
        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }

    [Fact]
    public void Loader_parses_valid_oidc_contract()
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "https://0.0.0.0:8443",
            ["Kafdeck:Deployment:AccessMode"] = "Oidc",
            ["Kafdeck:Deployment:Oidc:Issuer"] = "https://idp.example",
            ["Kafdeck:Deployment:Oidc:ClientId"] = "kafdeck",
            ["Kafdeck:Deployment:Oidc:ClientSecret"] = "env:KAFDECK_OIDC_CLIENT_SECRET",
            ["Kafdeck:Deployment:Oidc:GroupClaim"] = "groups",
            ["Kafdeck:Deployment:Oidc:Scopes:0"] = "openid",
            ["Kafdeck:Deployment:Oidc:Scopes:1"] = "profile",
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

        var options = KafdeckConfigurationLoader.Load(configuration);

        Assert.Equal(AccessMode.Oidc, options.Deployment.Mode);
        Assert.NotNull(options.Deployment.Oidc);
        Assert.Equal("https://idp.example", options.Deployment.Oidc!.Issuer);
        Assert.Equal("kafdeck", options.Deployment.Oidc.ClientId);
        Assert.Equal(new[] { "openid", "profile" }, options.Deployment.Oidc.Scopes);

        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }

    [Fact]
    public void Non_loopback_oidc_binding_requires_https()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions(
                "http://0.0.0.0:8080",
                null,
                AccessMode.Oidc,
                new OidcProfile(
                    "https://idp.example",
                    "kafdeck",
                    null,
                    "groups",
                    new[] { "openid", "profile" })),
            Array.Empty<ClusterProfile>());

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains("requires HTTPS", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Oidc_mode_rejects_deployment_access_token()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions(
                "https://0.0.0.0:8443",
                SecretReference.Parse("env:KAFDECK_DEPLOYMENT_TOKEN"),
                AccessMode.Oidc,
                new OidcProfile(
                    "https://idp.example",
                    "kafdeck",
                    null,
                    "groups",
                    new[] { "openid", "profile" })),
            Array.Empty<ClusterProfile>());

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains("must not configure a deployment access token", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Oidc_safe_diagnostics_never_expose_client_secret_reference()
    {
        const string secretVariable = "KAFDECK_OIDC_CLIENT_SECRET";

        var options = new KafdeckOptions(
            new DeploymentOptions(
                "https://0.0.0.0:8443",
                null,
                AccessMode.Oidc,
                new OidcProfile(
                    "https://idp.example",
                    "kafdeck",
                    SecretReference.Parse($"env:{secretVariable}"),
                    "groups",
                    new[] { "openid", "profile" })),
            Array.Empty<ClusterProfile>());

        var json = JsonSerializer.Serialize(SafeConfigurationDiagnostics.Create(options));

        Assert.DoesNotContain(secretVariable, json, StringComparison.Ordinal);
        Assert.DoesNotContain("ClientSecret", json, StringComparison.OrdinalIgnoreCase);
    }


    [Fact]
    public void Loader_parses_read_only_schema_registry_profile_from_secret_references()
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://127.0.0.1:8080",
            ["Kafdeck:Clusters:0:Id"] = "prod",
            ["Kafdeck:Clusters:0:BootstrapServers:0"] = "broker.example:9092",
            ["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext",
            ["Kafdeck:Clusters:0:SchemaRegistry:Url"] = "https://registry.example",
            ["Kafdeck:Clusters:0:SchemaRegistry:Username"] = "env:KAFDECK_SR_USER",
            ["Kafdeck:Clusters:0:SchemaRegistry:Password"] = "env:KAFDECK_SR_PASSWORD",
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

        var options = KafdeckConfigurationLoader.Load(configuration);

        Assert.Single(options.Clusters);
        Assert.NotNull(options.Clusters[0].SchemaRegistry);
        Assert.Equal("https://registry.example", options.Clusters[0].SchemaRegistry!.Url);

        KafdeckConfigurationValidator.ValidateAndThrow(options);

        var diagnosticJson = JsonSerializer.Serialize(SafeConfigurationDiagnostics.Create(options));
        Assert.Contains("SchemaRegistryConfigured", diagnosticJson, StringComparison.Ordinal);
        Assert.DoesNotContain("KAFDECK_SR_USER", diagnosticJson, StringComparison.Ordinal);
        Assert.DoesNotContain("KAFDECK_SR_PASSWORD", diagnosticJson, StringComparison.Ordinal);
    }

    [Fact]
    public void Remote_schema_registry_basic_auth_requires_https()
    {
        var cluster = new ClusterProfile(
            "prod",
            ["broker.example:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            new SchemaRegistryProfile(
                "http://registry.example",
                SecretReference.Parse("env:KAFDECK_SR_USER"),
                SecretReference.Parse("env:KAFDECK_SR_PASSWORD")));

        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            [cluster]);

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains("requires HTTPS", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Schema_registry_basic_auth_requires_username_and_password_together()
    {
        var cluster = new ClusterProfile(
            "prod",
            ["broker.example:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            new SchemaRegistryProfile(
                "https://registry.example",
                SecretReference.Parse("env:KAFDECK_SR_USER"),
                null));

        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            [cluster]);

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains("requires both username and password", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Tls_verification_cannot_be_silently_disabled()
    {
        var cluster = new ClusterProfile(
            "prod",
            new[] { "broker.example:9093" },
            KafkaSecurityProtocol.Ssl,
            new TlsProfile(false, null, null, null),
            null);

        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            new[] { cluster });

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains("cannot disable TLS", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Secret_references_and_values_do_not_serialize_secret_material()
    {
        var variableName = $"KAFDECK_TEST_SECRET_{Guid.NewGuid():N}";
        const string secret = "super-secret-value";
        Environment.SetEnvironmentVariable(variableName, secret);

        try
        {
            var reference = SecretReference.Parse($"env:{variableName}");
            var resolved = new SecretResolver().Resolve(reference);

            Assert.Equal(secret, resolved.Reveal());
            Assert.Equal("[redacted-secret-reference]", reference.ToString());
            Assert.Equal("[redacted-secret]", resolved.ToString());

            var referenceJson = JsonSerializer.Serialize(reference);
            var valueJson = JsonSerializer.Serialize(resolved);
            Assert.DoesNotContain(variableName, referenceJson, StringComparison.Ordinal);
            Assert.DoesNotContain(secret, referenceJson, StringComparison.Ordinal);
            Assert.DoesNotContain(secret, valueJson, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable(variableName, null);
        }
    }

    [Fact]
    public void Safe_diagnostics_exclude_secret_reference_locators()
    {
        const string tokenVariable = "KAFDECK_DEPLOYMENT_TOKEN";
        const string usernameVariable = "KAFDECK_KAFKA_USER";
        const string passwordVariable = "KAFDECK_KAFKA_PASSWORD";

        var cluster = new ClusterProfile(
            "secured",
            new[] { "broker.example:9093" },
            KafkaSecurityProtocol.SaslSsl,
            new TlsProfile(true, null, null, null),
            new SaslProfile(
                SaslMechanism.ScramSha512,
                SecretReference.Parse($"env:{usernameVariable}"),
                SecretReference.Parse($"env:{passwordVariable}")));

        var options = new KafdeckOptions(
            new DeploymentOptions(
                "http://0.0.0.0:8080",
                SecretReference.Parse($"env:{tokenVariable}")),
            new[] { cluster });

        var json = JsonSerializer.Serialize(SafeConfigurationDiagnostics.Create(options));

        Assert.DoesNotContain(tokenVariable, json, StringComparison.Ordinal);
        Assert.DoesNotContain(usernameVariable, json, StringComparison.Ordinal);
        Assert.DoesNotContain(passwordVariable, json, StringComparison.Ordinal);
    }

    [Fact]
    public void Deployment_token_comparison_is_exact_and_failure_attempts_are_bounded()
    {
        Assert.True(DeploymentAccessTokenValidator.Matches("expected-token", "expected-token"));
        Assert.False(DeploymentAccessTokenValidator.Matches("expected-token", "wrong-token"));

        var limiter = new FailedAccessAttemptLimiter(
            maxAttempts: 2,
            window: TimeSpan.FromMinutes(1),
            maxTrackedClients: 2);

        var now = DateTimeOffset.UtcNow;
        Assert.True(limiter.TryRecordFailure("client-a", now));
        Assert.True(limiter.TryRecordFailure("client-a", now));
        Assert.False(limiter.TryRecordFailure("client-a", now));
    }

    [Fact]
    public async Task Unauthorized_response_never_echoes_access_tokens()
    {
        const string expectedToken = "expected-token-secret";
        const string suppliedToken = "supplied-token-secret";
        var nextInvoked = false;

        var middleware = new DeploymentAccessTokenMiddleware(
            _ =>
            {
                nextInvoked = true;
                return Task.CompletedTask;
            },
            expectedToken);

        var context = new DefaultHttpContext();
        context.Response.Body = new MemoryStream();
        context.Request.Headers[DeploymentAccessTokenMiddleware.HeaderName] = suppliedToken;

        await middleware.InvokeAsync(context, new CapturingAuditSink());

        Assert.False(nextInvoked);
        Assert.Equal(StatusCodes.Status401Unauthorized, context.Response.StatusCode);

        context.Response.Body.Position = 0;
        using var reader = new StreamReader(context.Response.Body);
        var body = await reader.ReadToEndAsync();
        Assert.DoesNotContain(expectedToken, body, StringComparison.Ordinal);
        Assert.DoesNotContain(suppliedToken, body, StringComparison.Ordinal);
    }
    private sealed class CapturingAuditSink : ISecurityAuditSink
    {
        public List<SecurityAuditEvent> Events { get; } = new();

        public ValueTask WriteAsync(SecurityAuditEvent auditEvent, CancellationToken cancellationToken = default)
        {
            Events.Add(auditEvent);
            return ValueTask.CompletedTask;
        }
    }
}

