using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Kafdeck.Core.Security;

/// <summary>
/// Cryptographic evidence only; NOT a Kafdeck principal, authentication scheme,
/// registration, session or authorization decision. The opt-in read host uses
/// the evidence behind a separate live registration and backend RBAC gate.
/// Keys must be obtained and pinned through a future, separately admitted IdP
/// trust configuration. Caller must independently enforce live registration,
/// grant, revocation, resource authorization and risk approval.
/// </summary>
public sealed record NonBrowserVerifiedJwtEvidence(
    string Issuer,
    string Subject,
    string ClientId,
    string ApiAudience,
    DateTimeOffset ExpiresAtUtc);

public sealed class NonBrowserJwtEvidenceException : Exception
{
    public NonBrowserJwtEvidenceException()
        : base("Non-browser JWT evidence was rejected.")
    {
    }
}

/// <summary>
/// An inert, offline RS256 verification seam for reference Keycloak access
/// tokens. It never downloads JWKS, parses untrusted key URLs, or grants rights.
/// It deliberately supports one pinned key id and exactly one token audience;
/// multi-key rotation requires a separately admitted finite trust registry.
/// </summary>
public static class NonBrowserRs256JwtEvidenceVerifier
{
    private const int MaxCompactBytes = 16 * 1024;
    private const int MaxHeaderBytes = 1024;
    private const int MaxPayloadBytes = 8 * 1024;
    private const long MaxOriginalLifetimeSeconds = 15 * 60;
    private const long ClockSkewSeconds = 30;

    public static NonBrowserVerifiedJwtEvidence Verify(
        string compactJwt,
        RSA approvedVerificationKey,
        string approvedKeyId,
        NonBrowserServicePrincipalProposal proposal,
        DateTimeOffset observedAtUtc)
    {
        ArgumentNullException.ThrowIfNull(approvedVerificationKey);
        ArgumentNullException.ThrowIfNull(proposal);
        ArgumentException.ThrowIfNullOrWhiteSpace(approvedKeyId);

        // Never quote compactJwt, a JWT fragment, crypto provider detail,
        // arbitrary claim or raw validation exception in the failure.
        try
        {
            if (string.IsNullOrWhiteSpace(compactJwt) ||
                compactJwt.Length > MaxCompactBytes ||
                approvedKeyId.Length is < 1 or > 256 ||
                compactJwt.Any(ch => ch > 127 || ch < 33))
            {
                throw new NonBrowserJwtEvidenceException();
            }

            var parts = compactJwt.Split('.');
            if (parts.Length != 3 || parts.Any(string.IsNullOrEmpty))
                throw new NonBrowserJwtEvidenceException();

            var header = DecodeUrl(parts[0], MaxHeaderBytes);
            var payload = DecodeUrl(parts[1], MaxPayloadBytes);
            var signature = DecodeUrl(parts[2], 1024);
            using var headerJson = JsonDocument.Parse(header, new JsonDocumentOptions { MaxDepth = 8 });
            RequireUniqueFields(headerJson.RootElement);
            var alg = RequireString(headerJson.RootElement, "alg");
            var kid = RequireString(headerJson.RootElement, "kid");
            var tokenHeaderType = RequireString(headerJson.RootElement, "typ");
            if (alg != "RS256" ||
                !string.Equals(kid, approvedKeyId, StringComparison.Ordinal) ||
                (tokenHeaderType != "JWT" && tokenHeaderType != "at+jwt") ||
                headerJson.RootElement.TryGetProperty("jku", out _) ||
                headerJson.RootElement.TryGetProperty("x5u", out _) ||
                headerJson.RootElement.TryGetProperty("jwk", out _) ||
                headerJson.RootElement.TryGetProperty("crit", out _))
            {
                throw new NonBrowserJwtEvidenceException();
            }

            // Authentication evidence only exists after signature verification.
            var signedBytes = Encoding.ASCII.GetBytes(parts[0] + "." + parts[1]);
            if (!approvedVerificationKey.VerifyData(
                    signedBytes, signature, HashAlgorithmName.SHA256,
                    RSASignaturePadding.Pkcs1))
            {
                throw new NonBrowserJwtEvidenceException();
            }

            using var claimsJson = JsonDocument.Parse(payload, new JsonDocumentOptions { MaxDepth = 12 });
            var claims = claimsJson.RootElement;
            RequireUniqueFields(claims);

            var issuer = RequireString(claims, "iss");
            var subject = RequireString(claims, "sub");
            var clientId = RequireString(claims, "azp");
            var purpose = RequireString(claims, "typ");
            if (!string.Equals(issuer, proposal.Issuer, StringComparison.Ordinal) ||
                !string.Equals(subject, proposal.Subject, StringComparison.Ordinal) ||
                !string.Equals(clientId, proposal.ClientId, StringComparison.Ordinal) ||
                purpose != "Bearer")
            {
                throw new NonBrowserJwtEvidenceException();
            }

            if (claims.TryGetProperty("client_id", out var additionalClient) &&
                (additionalClient.ValueKind != JsonValueKind.String ||
                 !string.Equals(additionalClient.GetString(), proposal.ClientId, StringComparison.Ordinal)))
            {
                throw new NonBrowserJwtEvidenceException();
            }

            RequireExactAudience(claims, proposal.ApiAudience);
            var iat = RequireSeconds(claims, "iat");
            // Keycloak access tokens can omit the optional JWT nbf claim.
            // A present nbf must remain a finite valid timestamp; iat and the
            // original lifetime cap are always mandatory.
            var nbf = claims.TryGetProperty("nbf", out _) ? RequireSeconds(claims, "nbf") : iat;
            var exp = RequireSeconds(claims, "exp");
            var now = observedAtUtc.ToUnixTimeSeconds();

            // Validate ORIGINAL lifetime, not remaining life at request time.
            if (iat < 0 || nbf < 0 || exp <= iat || nbf >= exp ||
                exp - iat > MaxOriginalLifetimeSeconds ||
                iat > now + ClockSkewSeconds ||
                nbf > now + ClockSkewSeconds ||
                nbf < iat - ClockSkewSeconds ||
                exp <= now - ClockSkewSeconds)
            {
                throw new NonBrowserJwtEvidenceException();
            }

            return new NonBrowserVerifiedJwtEvidence(
                issuer, subject, clientId, proposal.ApiAudience,
                DateTimeOffset.FromUnixTimeSeconds(exp));
        }
        catch (NonBrowserJwtEvidenceException)
        {
            throw;
        }
        catch (Exception ex) when (ex is FormatException or JsonException or
                                   OverflowException or CryptographicException or
                                   ArgumentException or DecoderFallbackException)
        {
            throw new NonBrowserJwtEvidenceException();
        }
    }

    private static byte[] DecodeUrl(string part, int limit)
    {
        if (part.Length == 0 || part.Length > ((limit + 2) / 3) * 4 ||
            part.Any(ch => !((ch is >= 'A' and <= 'Z') ||
                             (ch is >= 'a' and <= 'z') ||
                             (ch is >= '0' and <= '9') ||
                             ch is '-' or '_')))
        {
            throw new NonBrowserJwtEvidenceException();
        }

        var base64 = part.Replace('-', '+').Replace('_', '/');
        base64 = base64.PadRight((base64.Length + 3) / 4 * 4, '=');
        var result = Convert.FromBase64String(base64);
        if (result.Length > limit)
            throw new NonBrowserJwtEvidenceException();
        return result;
    }

    private static void RequireUniqueFields(JsonElement value)
    {
        if (value.ValueKind != JsonValueKind.Object)
            throw new NonBrowserJwtEvidenceException();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in value.EnumerateObject())
        {
            if (!seen.Add(property.Name))
                throw new NonBrowserJwtEvidenceException();
        }
    }

    private static string RequireString(JsonElement objectValue, string name)
    {
        if (!objectValue.TryGetProperty(name, out var value) ||
            value.ValueKind != JsonValueKind.String ||
            string.IsNullOrWhiteSpace(value.GetString()) ||
            value.GetString()!.Length > 2048)
            throw new NonBrowserJwtEvidenceException();
        return value.GetString()!;
    }

    private static long RequireSeconds(JsonElement objectValue, string name)
    {
        if (!objectValue.TryGetProperty(name, out var value) ||
            value.ValueKind != JsonValueKind.Number ||
            !value.TryGetInt64(out var seconds))
            throw new NonBrowserJwtEvidenceException();
        return seconds;
    }

    private static void RequireExactAudience(JsonElement claims, string expected)
    {
        if (!claims.TryGetProperty("aud", out var audience))
            throw new NonBrowserJwtEvidenceException();

        if (audience.ValueKind == JsonValueKind.String &&
            string.Equals(audience.GetString(), expected, StringComparison.Ordinal))
            return;

        if (audience.ValueKind == JsonValueKind.Array &&
            audience.GetArrayLength() == 1 &&
            audience[0].ValueKind == JsonValueKind.String &&
            string.Equals(audience[0].GetString(), expected, StringComparison.Ordinal))
            return;

        throw new NonBrowserJwtEvidenceException();
    }
}
