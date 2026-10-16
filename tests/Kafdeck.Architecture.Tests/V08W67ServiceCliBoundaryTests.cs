using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Cli;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67ServiceCliBoundaryTests
{
    [KeycloakG2Fact]
    public async Task Genuine_Keycloak_service_exchange_returns_exact_memory_identity_and_rejects_wrong_key_and_subject()
    {
        using var lab = JsonDocument.Parse(await File.ReadAllTextAsync(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")!));
        string Field(string name) => lab.RootElement.GetProperty(name).GetString()!;
        var profile = new CliServiceOidcProfile(Field("issuer"), Field("audience"), Field("clientId"),
            Field("subject"), Field("clientPrivateKeyFile"), Field("caPemFile"));
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        using var session = await CliServiceOidcAuthenticator.AcquireAsync(profile, deadline.Token);
        Assert.Equal(profile.ExpectedSubject, session.Subject);
        Assert.Equal(profile.ClientId, session.ClientId);
        Assert.True(session.ExpiresAtUtc > DateTimeOffset.UtcNow);
        Assert.DoesNotContain("eyJ", session.ToString(), StringComparison.Ordinal);
        var wrongSubject = await Assert.ThrowsAsync<CliOidcAuthenticationException>(() =>
            CliServiceOidcAuthenticator.AcquireAsync(profile with { ExpectedSubject = "unregistered-subject-sentinel" }, deadline.Token));
        Assert.Equal(CliOidcFailureStage.BoundIdentity, wrongSubject.Stage);
        var otherKeyFile = Path.GetTempFileName();
        try
        {
            using var otherKey = RSA.Create(2048);
            await File.WriteAllTextAsync(otherKeyFile, otherKey.ExportPkcs8PrivateKeyPem(), deadline.Token);
            var rejected = await Assert.ThrowsAsync<CliOidcAuthenticationException>(() =>
                CliServiceOidcAuthenticator.AcquireAsync(profile with { PrivateKeyFile = otherKeyFile }, deadline.Token));
            Assert.Equal(CliOidcFailureStage.CodeExchange, rejected.Stage);
        }
        finally { File.Delete(otherKeyFile); }
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("[]")]
    [InlineData("{\"issuer\":123}")]
    [InlineData("{\"issuer\":\"https://sso.test/realms/g2\",\"audience\":\"api\",\"clientId\":\"machine\",\"expectedSubject\":null,\"privateKeyFile\":\"key.pem\"}")]
    [InlineData("{\"issuer\":\"https://sso.test/realms/g2\",\"audience\":\"api\",\"clientId\":\"machine\",\"expectedSubject\":\"subject\",\"privateKeyFile\":\"key.pem\",\"clientSecret\":\"credential-sentinel\"}")]
    [InlineData("{\"issuer\":\"https://sso.test/realms/g2\",\"audience\":\"api\",\"clientId\":\"machine\",\"expectedSubject\":\"subject\",\"privateKeyFile\":\"key.pem\",\"clientId\":\"another\"}")]
    public async Task Service_profile_is_closed_typed_bounded_and_never_echoes_credentials(string json)
    {
        var file = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(file, json);
            var error = await Assert.ThrowsAsync<CliUsageException>(() => CliServiceOidcProfile.LoadAsync(file, default));
            Assert.DoesNotContain("credential-sentinel", error.ToString(), StringComparison.Ordinal);
        }
        finally { File.Delete(file); }
    }

    [Theory]
    [InlineData(1024, true)]
    [InlineData(2048, false)]
    public async Task Service_private_key_rejects_weak_RSA_and_public_only_material(int bits, bool includePrivate)
    {
        using var key = RSA.Create(bits);
        var file = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(file, includePrivate ? key.ExportPkcs8PrivateKeyPem() : key.ExportSubjectPublicKeyInfoPem());
            await Assert.ThrowsAsync<CliOidcAuthenticationException>(() => CliServiceOidcAuthenticator.LoadPrivateKeyAsync(file, default));
        }
        finally { File.Delete(file); }
    }

    [Fact]
    public async Task Private_key_file_is_bounded_before_import_and_failure_is_redacted()
    {
        var file = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(file, "credential-sentinel" + new string('x', 16 * 1024));
            var error = await Assert.ThrowsAsync<CliOidcAuthenticationException>(() => CliServiceOidcAuthenticator.LoadPrivateKeyAsync(file, default));
            Assert.DoesNotContain("credential-sentinel", error.ToString(), StringComparison.Ordinal);
        }
        finally { File.Delete(file); }
    }

    [Fact]
    public async Task Fresh_assertions_are_signed_exact_client_endpoint_short_lifetime_and_unique_jti()
    {
        using var key = RSA.Create(2048);
        var file = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(file, key.ExportPkcs8PrivateKeyPem());
            using var loaded = await CliServiceOidcAuthenticator.LoadPrivateKeyAsync(file, default);
            var now = DateTimeOffset.UtcNow;
            const string endpoint = "https://sso.test/realms/g2/protocol/openid-connect/token";
            var first = CliServiceOidcAuthenticator.CreateAssertion(loaded, "machine", endpoint, now).Split('.');
            var second = CliServiceOidcAuthenticator.CreateAssertion(loaded, "machine", endpoint, now).Split('.');
            using var claims = JsonDocument.Parse(Decode(first[1]));
            using var nextClaims = JsonDocument.Parse(Decode(second[1]));
            Assert.Equal("machine", claims.RootElement.GetProperty("iss").GetString());
            Assert.Equal("machine", claims.RootElement.GetProperty("sub").GetString());
            Assert.Equal(endpoint, claims.RootElement.GetProperty("aud").GetString());
            Assert.Equal(45, claims.RootElement.GetProperty("exp").GetInt64() - claims.RootElement.GetProperty("iat").GetInt64());
            Assert.NotEqual(claims.RootElement.GetProperty("jti").GetString(), nextClaims.RootElement.GetProperty("jti").GetString());
            Assert.True(key.VerifyData(Encoding.ASCII.GetBytes(first[0] + "." + first[1]), Decode(first[2]), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
        }
        finally { File.Delete(file); }
    }

    [Theory]
    [InlineData("wrong-subject", "machine")]
    [InlineData("subject", "another-client")]
    public void Expected_service_subject_and_authorized_client_are_exact(string subject, string client)
    {
        using var claims = JsonDocument.Parse(JsonSerializer.Serialize(new { sub = subject, azp = client }));
        var profile = new CliServiceOidcProfile("https://sso.test/realms/g2", "api", "machine", "subject", "key.pem");
        Assert.Throws<CliOidcAuthenticationException>(() => CliServiceOidcAuthenticator.RequireExpectedIdentity(claims.RootElement, profile));
    }

    [Fact]
    public void Service_and_human_or_legacy_credentials_are_mutually_exclusive_but_named_commands_remain_typed()
    {
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "service.json", "--oidc-profile", "human.json", "system", "info"]));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "service.json", "--token-file", "legacy.key", "system", "info"]));
        var invocation = CliParser.Parse(["--service-profile", "service.json", "operations", "execute", "00000000-0000-0000-0000-000000000001"]);
        Assert.Equal("service.json", invocation.ServiceProfileFile);
        Assert.Null(invocation.OidcProfileFile);
        Assert.Equal(CliCommand.OperationExecute, invocation.Command);
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "service.json", "http", "post", "/arbitrary"]));
    }

    private static byte[] Decode(string text)
    {
        var value = text.Replace('-', '+').Replace('_', '/');
        return Convert.FromBase64String(value.PadRight((value.Length + 3) / 4 * 4, '='));
    }
}
