using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Security;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67PinnedRsaKeyRingTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);

    private static NonBrowserServicePrincipalProposal Candidate(
        string audience = "kafdeck-api", string issuer = "https://sso.test/realms/kafdeck") =>
        new("svc", "ops", issuer, audience, "service-account-one", "automation-one",
            [AuthorizationAction.SystemRead]);

    private static NonBrowserPinnedRsaKeySpec Spec(RSA rsa, string kid = "key-one") =>
        new(kid, rsa.ExportParameters(false),
            Convert.ToHexString(SHA256.HashData(rsa.ExportSubjectPublicKeyInfo())));

    private static NonBrowserPinnedRsaKeyRing Ring(params NonBrowserPinnedRsaKeySpec[] keys) =>
        new("https://sso.test/realms/kafdeck", "kafdeck-api", keys);

    private static string B64(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private static string Sign(RSA rsa, string kid = "key-one")
    {
        var header = B64(JsonSerializer.SerializeToUtf8Bytes(
            new { alg = "RS256", typ = "JWT", kid }));
        var now = Now.ToUnixTimeSeconds();
        var payload = B64(JsonSerializer.SerializeToUtf8Bytes(new
        {
            iss = "https://sso.test/realms/kafdeck",
            sub = "service-account-one", azp = "automation-one",
            aud = "kafdeck-api", typ = "Bearer",
            iat = now - 60, nbf = now - 60, exp = now + 180,
        }));
        var signingInput = Encoding.ASCII.GetBytes(header + "." + payload);
        var sig = rsa.SignData(signingInput, HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        return header + "." + payload + "." + B64(sig);
    }

    [Fact]
    public void Exact_pinned_spki_and_kid_verify_offline_evidence_only()
    {
        using var rsa = RSA.Create(2048);
        var ring = Ring(Spec(rsa));
        Assert.Equal(1, ring.KeyCount);
        var evidence = ring.Verify(Sign(rsa), "key-one", Candidate(), Now);
        Assert.Equal("service-account-one", evidence.Subject);
        Assert.Equal("kafdeck-api", evidence.ApiAudience);
        Assert.Null(typeof(NonBrowserPinnedRsaKeyRing).GetMethod("Authenticate"));
        Assert.Null(typeof(NonBrowserPinnedRsaKeyRing).GetMethod("Authorize"));
        Assert.Null(typeof(NonBrowserPinnedRsaKeyRing).GetMethod("FetchJwks"));
    }

    private sealed class InconsistentCountKeyList : IReadOnlyList<NonBrowserPinnedRsaKeySpec>
    {
        private readonly IReadOnlyList<NonBrowserPinnedRsaKeySpec> _actual;

        public InconsistentCountKeyList(IReadOnlyList<NonBrowserPinnedRsaKeySpec> actual) =>
            _actual = actual;

        // Deliberately lies about count while enumerating five entries.
        public int Count => 1;
        public NonBrowserPinnedRsaKeySpec this[int index] => _actual[index];

        public IEnumerator<NonBrowserPinnedRsaKeySpec> GetEnumerator() =>
            _actual.GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }

    [Fact]
    public void Snapshot_count_not_retained_mutable_list_count_controls_trust_budget()
    {
        using var rsa = RSA.Create(2048);
        var deceptive = new InconsistentCountKeyList(
            Enumerable.Range(0, 5)
                .Select(i => Spec(rsa, "key-" + i))
                .ToArray());

        // Snapshot reads at most five elements and rejects the fifth;
        // no expensive RSA import or unbounded enumeration is admitted.
        Assert.Throws<ArgumentException>(() =>
            new NonBrowserPinnedRsaKeyRing(
                "https://sso.test/realms/kafdeck",
                "kafdeck-api",
                deceptive));
    }

    [Fact]
    public void Key_input_is_defensively_copied_before_any_future_verification()
    {
        using var rsa = RSA.Create(2048);
        var parameters = rsa.ExportParameters(false);
        var fingerprint = Convert.ToHexString(
            SHA256.HashData(rsa.ExportSubjectPublicKeyInfo()));
        var ring = Ring(new NonBrowserPinnedRsaKeySpec("key-one", parameters, fingerprint));
        Array.Fill(parameters.Modulus!, (byte)0);
        Array.Fill(parameters.Exponent!, (byte)0);
        Assert.Equal("service-account-one",
            ring.Verify(Sign(rsa), "key-one", Candidate(), Now).Subject);
    }

    [Fact]
    public void Key_id_is_not_a_downgrade_or_key_selection_bypass()
    {
        using var rsa = RSA.Create(2048);
        var ring = Ring(Spec(rsa));
        var token = Sign(rsa);
        Assert.Throws<NonBrowserJwtEvidenceException>(() =>
            ring.Verify(token, "different", Candidate(), Now));
        Assert.Throws<NonBrowserJwtEvidenceException>(() =>
            ring.Verify(token, "key-one", Candidate(audience: "other-api"), Now));
        Assert.Throws<NonBrowserJwtEvidenceException>(() =>
            ring.Verify(token, "key-one",
                Candidate(issuer: "https://other.test/realms/kafdeck"), Now));
        Assert.Throws<NonBrowserJwtEvidenceException>(() =>
            ring.Verify(Sign(rsa, "other-key"), "key-one", Candidate(), Now));
    }

    [Fact]
    public void Public_exponent_is_admitted_only_from_the_validated_snapshot()
    {
        using var rsa = RSA.Create(2048);
        var parameters = rsa.ExportParameters(false);
        var expectedFingerprint = Convert.ToHexString(
            SHA256.HashData(rsa.ExportSubjectPublicKeyInfo()));

        // An otherwise valid public modulus must not make an unsupported
        // caller-owned exponent acceptable, even under a valid key pin.
        parameters.Exponent![2] = 0x03;
        Assert.Throws<ArgumentException>(() =>
            Ring(new NonBrowserPinnedRsaKeySpec(
                "key-one", parameters, expectedFingerprint)));

        // Conversely, once the approved snapshot has been constructed,
        // later writes to the original arrays cannot change its verification.
        var valid = rsa.ExportParameters(false);
        var ring = Ring(new NonBrowserPinnedRsaKeySpec(
            "key-one", valid, expectedFingerprint));
        valid.Exponent![2] = 0x03;
        Assert.Equal("service-account-one",
            ring.Verify(Sign(rsa), "key-one", Candidate(), Now).Subject);
    }

    [Fact]
    public void Pin_fingerprint_prevents_unreviewed_public_key_substitution()
    {
        using var approved = RSA.Create(2048);
        using var attacker = RSA.Create(2048);
        Assert.Throws<ArgumentException>(() => Ring(new NonBrowserPinnedRsaKeySpec(
            "key-one", attacker.ExportParameters(false),
            Convert.ToHexString(SHA256.HashData(approved.ExportSubjectPublicKeyInfo())))));
    }

    [Fact]
    public void Duplicate_id_and_duplicate_key_under_different_id_are_rejected()
    {
        using var rsa = RSA.Create(2048);
        using var other = RSA.Create(2048);
        Assert.Throws<ArgumentException>(() =>
            Ring(Spec(rsa, "key-one"), Spec(other, "key-one")));
        Assert.Throws<ArgumentException>(() =>
            Ring(Spec(rsa, "key-one"), Spec(rsa, "key-two")));
    }

    [Fact]
    public void Different_explicit_pins_can_coexist_without_implicit_fallback()
    {
        using var original = RSA.Create(2048);
        using var rotated = RSA.Create(2048);
        var ring = Ring(Spec(original, "key-one"), Spec(rotated, "key-two"));
        Assert.Equal(2, ring.KeyCount);
        Assert.Equal("service-account-one",
            ring.Verify(Sign(rotated, "key-two"), "key-two", Candidate(), Now).Subject);
        Assert.Throws<NonBrowserJwtEvidenceException>(() =>
            ring.Verify(Sign(original, "key-one"), "key-two", Candidate(), Now));
    }

    [Fact]
    public void Private_key_material_is_not_admitted_as_trust_input()
    {
        using var rsa = RSA.Create(2048);
        var privateKey = rsa.ExportParameters(true);
        Assert.Throws<ArgumentException>(() =>
            Ring(new NonBrowserPinnedRsaKeySpec("key-one", privateKey,
                Convert.ToHexString(SHA256.HashData(rsa.ExportSubjectPublicKeyInfo())))));
    }

    [Theory]
    [InlineData("bad key")]
    [InlineData("a/b")]
    [InlineData("a*b")]
    [InlineData("../other")]
    public void Unsafe_key_identifier_is_rejected(string kid)
    {
        using var rsa = RSA.Create(2048);
        Assert.Throws<ArgumentException>(() => Ring(Spec(rsa, kid)));
    }

    [Fact]
    public void Fingerprint_has_fixed_exact_shape_and_set_is_bounded()
    {
        using var rsa = RSA.Create(2048);
        Assert.Throws<ArgumentException>(() => Ring(
            new NonBrowserPinnedRsaKeySpec("key-one", rsa.ExportParameters(false), "BAD")));
        Assert.Throws<ArgumentException>(() => Ring());
        var keys = Enumerable.Range(0, 5).Select(i =>
        {
            using var key = RSA.Create(2048);
            return Spec(key, "kid-" + i);
        }).ToArray();
        Assert.Throws<ArgumentException>(() => Ring(keys));
    }

    [Fact]
    public void Oversized_public_arrays_are_rejected_before_allocating_snapshots()
    {
        using var rsa = RSA.Create(2048);
        var parameters = rsa.ExportParameters(false);
        var fingerprint = Convert.ToHexString(SHA256.HashData(rsa.ExportSubjectPublicKeyInfo()));
        // Allocate caller input before measuring so only the constructor's
        // additional allocation is counted. A clone would add at least 1 MiB.
        parameters.Modulus = new byte[1024 * 1024];
        var before = GC.GetAllocatedBytesForCurrentThread();
        Assert.Throws<ArgumentException>(() =>
            Ring(new NonBrowserPinnedRsaKeySpec("key-one", parameters, fingerprint)));
        Assert.True(GC.GetAllocatedBytesForCurrentThread() - before < 128 * 1024);

        parameters = rsa.ExportParameters(false);
        parameters.Exponent = new byte[1024 * 1024];
        before = GC.GetAllocatedBytesForCurrentThread();
        Assert.Throws<ArgumentException>(() =>
            Ring(new NonBrowserPinnedRsaKeySpec("key-one", parameters, fingerprint)));
        Assert.True(GC.GetAllocatedBytesForCurrentThread() - before < 128 * 1024);
    }

    [Fact]
    public void Test_key_referencing_a_weak_RSA_key_is_not_admitted()
    {
        using var weak = RSA.Create(1024);
        Assert.Throws<ArgumentException>(() => Ring(Spec(weak)));
    }
}
