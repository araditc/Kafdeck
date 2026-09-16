using System.Text.Json;
using Kafdeck.Infrastructure.Configuration;
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

        await middleware.InvokeAsync(context);

        Assert.False(nextInvoked);
        Assert.Equal(StatusCodes.Status401Unauthorized, context.Response.StatusCode);

        context.Response.Body.Position = 0;
        using var reader = new StreamReader(context.Response.Body);
        var body = await reader.ReadToEndAsync();
        Assert.DoesNotContain(expectedToken, body, StringComparison.Ordinal);
        Assert.DoesNotContain(suppliedToken, body, StringComparison.Ordinal);
    }
}
