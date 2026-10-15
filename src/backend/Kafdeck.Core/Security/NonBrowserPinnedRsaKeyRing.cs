using System.Security.Cryptography;

namespace Kafdeck.Core.Security;

/// <summary>
/// Immutable input from a FUTURE deployment-approved trust configuration.
/// Neither JWT headers nor API callers may provide verification keys.
/// This object is NOT a live JWKS fetcher, authentication provider or grant.
/// </summary>
public sealed record NonBrowserPinnedRsaKeySpec(
    string KeyId,
    RSAParameters PublicParameters,
    string ExpectedSpkiSha256Hex);

/// <summary>
/// Inert, immutable, finite and fingerprint-pinned RSA verification key ring.
/// All key material is supplied out-of-band at construction; never retrieved
/// from untrusted JWT headers, public network URLs or runtime HTTP requests.
/// No host DI or authentication scheme registers this class.
/// </summary>
public sealed class NonBrowserPinnedRsaKeyRing
{
    private sealed record StoredKey(byte[] Modulus, byte[] Exponent);
    private readonly Dictionary<string, StoredKey> _keys;

    public NonBrowserPinnedRsaKeyRing(
        string approvedIssuer,
        string approvedApiAudience,
        IReadOnlyList<NonBrowserPinnedRsaKeySpec> approvedKeys)
    {
        Issuer = RequireIdentity(approvedIssuer, nameof(approvedIssuer), 2048);
        ApiAudience = RequireIdentity(approvedApiAudience, nameof(approvedApiAudience), 256);
        ArgumentNullException.ThrowIfNull(approvedKeys);
        if (approvedKeys.Count is < 1 or > 4)
            throw new ArgumentException("Pinned RSA trust set must contain 1 to 4 keys.", nameof(approvedKeys));

        _keys = new Dictionary<string, StoredKey>(StringComparer.Ordinal);
        var fingerprints = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in approvedKeys)
        {
            ArgumentNullException.ThrowIfNull(item);
            var kid = RequireKeyId(item.KeyId);
            var rsa = item.PublicParameters;
            // Snapshot all mutable arrays BEFORE evaluating key policy.
            // Checking caller-owned Exponent and copying it later admits
            // a race between validation and key import.
            var modulus = rsa.Modulus?.ToArray();
            var exponent = rsa.Exponent?.ToArray();
            if (modulus is null || modulus.Length is < 256 or > 512 ||
                exponent is not [0x01, 0x00, 0x01] ||
                rsa.D is not null || rsa.P is not null ||
                rsa.Q is not null || rsa.DP is not null ||
                rsa.DQ is not null || rsa.InverseQ is not null)
            {
                throw new ArgumentException(
                    "Pinned RSA material must contain only an approved 2048-to-4096-bit public key.",
                    nameof(approvedKeys));
            }
            byte[] fingerprint;
            try
            {
                using var key = RSA.Create();
                key.ImportParameters(new RSAParameters
                {
                    Modulus = modulus,
                    Exponent = exponent,
                });
                if (key.KeySize is < 2048 or > 4096)
                    throw new ArgumentException("Pinned RSA public key size is unsupported.", nameof(approvedKeys));
                fingerprint = SHA256.HashData(key.ExportSubjectPublicKeyInfo());
            }
            catch (CryptographicException)
            {
                throw new ArgumentException("Pinned RSA public key is invalid.", nameof(approvedKeys));
            }

            byte[] expectedFingerprint;
            try
            {
                if (item.ExpectedSpkiSha256Hex is null || item.ExpectedSpkiSha256Hex.Length != 64 ||
                    item.ExpectedSpkiSha256Hex.Any(ch => !Uri.IsHexDigit(ch)))
                    throw new FormatException();
                expectedFingerprint = Convert.FromHexString(item.ExpectedSpkiSha256Hex);
            }
            catch (FormatException)
            {
                throw new ArgumentException(
                    "Pinned RSA key fingerprint must be exact SHA-256 SPKI hex.",
                    nameof(approvedKeys));
            }

            if (!CryptographicOperations.FixedTimeEquals(fingerprint, expectedFingerprint) ||
                !_keys.TryAdd(kid, new StoredKey(modulus, exponent)) ||
                !fingerprints.Add(Convert.ToHexString(fingerprint)))
            {
                throw new ArgumentException(
                    "Pinned RSA key fingerprint mismatch or duplicate trust identity.",
                    nameof(approvedKeys));
            }
        }
    }

    public string Issuer { get; }
    public string ApiAudience { get; }
    public int KeyCount => _keys.Count;

    /// <summary>
    /// Caller-selected key ID is just a bounded lookup. The signed header's
    /// kid must independently match this value inside the RS256 verifier.
    /// The resulting evidence is NOT an authorization or active identity.
    /// </summary>
    public NonBrowserVerifiedJwtEvidence Verify(
        string compactJwt,
        string expectedKeyId,
        NonBrowserServicePrincipalProposal proposal,
        DateTimeOffset observedAtUtc)
    {
        ArgumentNullException.ThrowIfNull(proposal);
        if (!string.Equals(proposal.Issuer, Issuer, StringComparison.Ordinal) ||
            !string.Equals(proposal.ApiAudience, ApiAudience, StringComparison.Ordinal) ||
            string.IsNullOrEmpty(expectedKeyId) ||
            !_keys.TryGetValue(expectedKeyId, out var key))
            throw new NonBrowserJwtEvidenceException();

        using var verifier = RSA.Create();
        verifier.ImportParameters(new RSAParameters
        {
            Modulus = key.Modulus.ToArray(),
            Exponent = key.Exponent.ToArray(),
        });
        return NonBrowserRs256JwtEvidenceVerifier.Verify(
            compactJwt, verifier, expectedKeyId, proposal, observedAtUtc);
    }

    private static string RequireKeyId(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 128 ||
            value.Any(ch => !((ch is >= 'a' and <= 'z') ||
                              (ch is >= 'A' and <= 'Z') ||
                              (ch is >= '0' and <= '9') ||
                              ch is '.' or '_' or '-')))
            throw new ArgumentException("Pinned RSA key ID is not an exact bounded identifier.");
        return value;
    }

    private static string RequireIdentity(string? value, string name, int limit)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > limit ||
            value != value.Trim() || value.Any(char.IsControl) ||
            value.Contains('*') || value.Contains('?'))
            throw new ArgumentException("Pinned RSA trust identity is invalid.", name);
        return value;
    }
}
