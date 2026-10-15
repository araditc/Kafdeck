using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Security;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67InertRs256EvidenceVerifierTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);

    private static NonBrowserServicePrincipalProposal Candidate() => new(
        "service-one", "platform-ops", "https://sso.test/realms/kafdeck",
        "kafdeck-api", "service-account-one", "automation-one",
        [AuthorizationAction.SystemRead]);

    private static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private static string Sign(RSA key, object? header = null, object? payload = null)
    {
        var h = Base64Url(JsonSerializer.SerializeToUtf8Bytes(
            header ?? new { alg = "RS256", typ = "JWT", kid = "pinned-key-1" }));
        var p = Base64Url(JsonSerializer.SerializeToUtf8Bytes(
            payload ?? Claims()));
        var signed = Encoding.ASCII.GetBytes(h + "." + p);
        return h + "." + p + "." +
            Base64Url(key.SignData(signed, HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1));
    }

    private static object Claims(
        string issuer = "https://sso.test/realms/kafdeck",
        string subject = "service-account-one",
        string client = "automation-one",
        object? audience = null,
        string purpose = "Bearer",
        long? iat = null,
        long? nbf = null,
        long? exp = null) => new
    {
        iss = issuer,
        sub = subject,
        azp = client,
        aud = audience ?? "kafdeck-api",
        typ = purpose,
        iat = iat ?? Now.ToUnixTimeSeconds() - 60,
        nbf = nbf ?? Now.ToUnixTimeSeconds() - 60,
        exp = exp ?? Now.ToUnixTimeSeconds() + 180,
    };

    private static NonBrowserVerifiedJwtEvidence Verify(
        RSA signingKey, string jwt, RSA? verifyingKey = null,
        string expectedKid = "pinned-key-1") =>
        NonBrowserRs256JwtEvidenceVerifier.Verify(
            jwt, verifyingKey ?? signingKey, expectedKid, Candidate(), Now);

    [Fact]
    public void Signed_matching_short_lived_access_token_creates_evidence_not_a_grant()
    {
        using var rsa = RSA.Create(2048);
        var evidence = Verify(rsa, Sign(rsa));
        Assert.Equal("service-account-one", evidence.Subject);
        Assert.Equal("automation-one", evidence.ClientId);
        Assert.Equal("kafdeck-api", evidence.ApiAudience);
        Assert.Equal(Now.AddSeconds(180), evidence.ExpiresAtUtc);
        Assert.Null(typeof(NonBrowserRs256JwtEvidenceVerifier).GetMethod("Authenticate"));
        Assert.Null(typeof(NonBrowserRs256JwtEvidenceVerifier).GetMethod("Authorize"));
        Assert.Null(typeof(NonBrowserRs256JwtEvidenceVerifier).GetMethod("Enable"));
    }

    [Fact]
    public void A_token_signed_by_an_unapproved_RSA_key_cannot_create_evidence()
    {
        using var approved = RSA.Create(2048);
        using var attacker = RSA.Create(2048);
        Assert.Throws<NonBrowserJwtEvidenceException>(
            () => Verify(attacker, Sign(attacker), approved));
    }

    [Theory]
    [InlineData("another-key", "RS256", "JWT")]
    [InlineData("pinned-key-1", "none", "JWT")]
    [InlineData("pinned-key-1", "HS256", "JWT")]
    [InlineData("pinned-key-1", "RS256", "id+jwt")]
    public void Header_algorithm_key_and_purpose_cannot_be_substituted(
        string kid, string alg, string typ)
    {
        using var rsa = RSA.Create(2048);
        var token = Sign(rsa, new { alg, kid, typ });
        Assert.Throws<NonBrowserJwtEvidenceException>(() => Verify(rsa, token));
    }

    [Fact]
    public void Attacker_controlled_jwk_jku_or_critical_header_is_denied()
    {
        using var rsa = RSA.Create(2048);
        var jwt = Sign(rsa, new
        {
            alg = "RS256", kid = "pinned-key-1", typ = "JWT",
            jku = "https://attacker.test/public-keys",
        });
        Assert.Throws<NonBrowserJwtEvidenceException>(() => Verify(rsa, jwt));
    }

    [Theory]
    [InlineData("https://wrong.test/realms/kafdeck", "service-account-one", "automation-one", "Bearer")]
    [InlineData("https://sso.test/realms/kafdeck", "somebody-else", "automation-one", "Bearer")]
    [InlineData("https://sso.test/realms/kafdeck", "service-account-one", "another-client", "Bearer")]
    [InlineData("https://sso.test/realms/kafdeck", "service-account-one", "automation-one", "ID")]
    public void Issuer_subject_client_and_access_token_purpose_are_exact(
        string issuer, string subject, string client, string purpose)
    {
        using var rsa = RSA.Create(2048);
        var token = Sign(rsa, payload: Claims(
            issuer: issuer, subject: subject, client: client, purpose: purpose));
        Assert.Throws<NonBrowserJwtEvidenceException>(() => Verify(rsa, token));
    }

    [Theory]
    [InlineData("different-api")]
    [InlineData("KAFDECK-API")]
    public void Other_api_audiences_are_denied(string audience)
    {
        using var rsa = RSA.Create(2048);
        Assert.Throws<NonBrowserJwtEvidenceException>(() =>
            Verify(rsa, Sign(rsa, payload: Claims(audience: audience))));
    }

    [Fact]
    public void Additional_audience_even_in_signed_array_is_denied()
    {
        using var rsa = RSA.Create(2048);
        Assert.Throws<NonBrowserJwtEvidenceException>(() =>
            Verify(rsa, Sign(rsa, payload: Claims(audience: new[] { "kafdeck-api", "some-other-api" }))));
        var validArray = Verify(rsa, Sign(rsa, payload: Claims(audience: new[] { "kafdeck-api" })));
        Assert.Equal("kafdeck-api", validArray.ApiAudience);
    }

    [Fact]
    public void Token_must_have_original_short_lifetime_finite_claims_and_valid_time()
    {
        using var rsa = RSA.Create(2048);
        var now = Now.ToUnixTimeSeconds();
        var cases = new object[]
        {
            Claims(iat: now - 2000, nbf: now - 2000, exp: now + 60),
            Claims(iat: now + 120, nbf: now + 120, exp: now + 300),
            Claims(iat: now - 1000, nbf: now - 1000, exp: now - 80),
            Claims(iat: now - 20, nbf: now + 120, exp: now + 200),
            // Both nbf and exp are individually within the skew window, but
            // the signed token's interval is impossible: nbf follows exp.
            Claims(iat: now - 10, nbf: now + 20, exp: now + 5),
            Claims(iat: now + 100, nbf: now - 120, exp: now + 180),
        };
        foreach (var claims in cases)
        {
            Assert.Throws<NonBrowserJwtEvidenceException>(() =>
                Verify(rsa, Sign(rsa, payload: claims)));
        }
    }

    [Fact]
    public void Payload_forgery_and_invalid_compact_encoding_are_fail_closed()
    {
        using var rsa = RSA.Create(2048);
        var original = Sign(rsa);
        var fields = original.Split('.');
        fields[1] = Base64Url(JsonSerializer.SerializeToUtf8Bytes(
            Claims(subject: "forged-actor")));
        Assert.Throws<NonBrowserJwtEvidenceException>(() =>
            Verify(rsa, string.Join('.', fields)));
        Assert.Throws<NonBrowserJwtEvidenceException>(() =>
            Verify(rsa, original + ".extra"));
        Assert.Throws<NonBrowserJwtEvidenceException>(() =>
            Verify(rsa, new string('a', 16 * 1024 + 1)));
    }

    [Fact]
    public void Keycloak_optional_nbf_uses_mandatory_signed_iat_and_preserves_original_lifetime_cap()
    {
        using var rsa = RSA.Create(2048);
        var now = Now.ToUnixTimeSeconds();
        var withoutNbf = new { iss = "https://sso.test/realms/kafdeck", sub = "service-account-one",
            azp = "automation-one", aud = "kafdeck-api", typ = "Bearer", iat = now - 10, exp = now + 180 };
        Assert.Equal("service-account-one", Verify(rsa, Sign(rsa, payload: withoutNbf)).Subject);
        var withoutNbfFutureIat = new { iss = "https://sso.test/realms/kafdeck", sub = "service-account-one",
            azp = "automation-one", aud = "kafdeck-api", typ = "Bearer", iat = now + 120, exp = now + 240 };
        Assert.Throws<NonBrowserJwtEvidenceException>(() => Verify(rsa, Sign(rsa, payload: withoutNbfFutureIat)));
        var malformedNbf = new { iss = "https://sso.test/realms/kafdeck", sub = "service-account-one",
            azp = "automation-one", aud = "kafdeck-api", typ = "Bearer", iat = now - 10, exp = now + 180, nbf = "invalid" };
        Assert.Throws<NonBrowserJwtEvidenceException>(() => Verify(rsa, Sign(rsa, payload: malformedNbf)));
    }

    [Fact]
    public void Duplicate_security_critical_claims_are_denied_even_if_signed()
    {
        using var rsa = RSA.Create(2048);
        var header = Base64Url(Encoding.UTF8.GetBytes(
            "{\"alg\":\"RS256\",\"typ\":\"JWT\",\"kid\":\"pinned-key-1\"}"));
        var payload = Base64Url(Encoding.UTF8.GetBytes(
            "{\"iss\":\"https://sso.test/realms/kafdeck\",\"iss\":\"https://attacker.test\"," +
            "\"sub\":\"service-account-one\",\"azp\":\"automation-one\",\"typ\":\"Bearer\"," +
            "\"aud\":\"kafdeck-api\",\"iat\":1792065540,\"nbf\":1792065540,\"exp\":1792065800}"));
        var bytes = Encoding.ASCII.GetBytes(header + "." + payload);
        var token = header + "." + payload + "." +
            Base64Url(rsa.SignData(bytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
        Assert.Throws<NonBrowserJwtEvidenceException>(() => Verify(rsa, token));
    }

    [Fact]
    public void Rejection_never_reflects_token_or_protected_claim()
    {
        using var rsa = RSA.Create(2048);
        var secret = "sensitive-subject-value";
        var invalid = Sign(rsa, payload: Claims(subject: secret));
        var exception = Assert.Throws<NonBrowserJwtEvidenceException>(() =>
            Verify(rsa, invalid));
        Assert.DoesNotContain(secret, exception.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(invalid, exception.ToString(), StringComparison.Ordinal);
    }
}
