using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KeycloakG2HaFactAttribute : FactAttribute
{
    public KeycloakG2HaFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_HA_LAB")))
            Skip = "Requires two genuine Keycloak nodes with shared PostgreSQL and distributed replay cache.";
    }
}

public sealed class V08G2KeycloakHaReplayTests
{
    [KeycloakG2HaFact]
    public async Task Fresh_same_assertion_races_across_two_real_nodes_have_exactly_one_winner()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var labPath = Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_HA_LAB")!;
        using var manifest = JsonDocument.Parse(await File.ReadAllTextAsync(labPath, deadline.Token));
        var root = manifest.RootElement;
        string Field(string name) => root.GetProperty(name).GetString()!;
        var issuer = Field("issuer");
        var clientId = Field("clientId");
        var audience = Field("audience");
        var subject = Field("subject");
        var nodes = root.GetProperty("nodes").EnumerateArray().Select(node => node.GetProperty("endpoint").GetString()!).ToArray();
        Assert.Equal(2, nodes.Length);
        Assert.NotEqual(nodes[0], nodes[1]);
        using var ca = X509Certificate2.CreateFromPem(await File.ReadAllTextAsync(Field("caPemFile"), deadline.Token));
        using var handler = new SocketsHttpHandler
        {
            AllowAutoRedirect = false, UseProxy = false, UseCookies = false, MaxConnectionsPerServer = 8,
            SslOptions = new SslClientAuthenticationOptions
            {
                CertificateChainPolicy = new X509ChainPolicy
                {
                    TrustMode = X509ChainTrustMode.CustomRootTrust, RevocationMode = X509RevocationMode.NoCheck, CustomTrustStore = { ca },
                },
            },
        };
        using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(15) };
        foreach (var node in nodes)
        {
            using var discovery = await http.GetAsync(node + "/.well-known/openid-configuration", deadline.Token);
            Assert.Equal(HttpStatusCode.OK, discovery.StatusCode);
            using var metadata = JsonDocument.Parse(await discovery.Content.ReadAsStringAsync(deadline.Token));
            Assert.Equal(issuer, metadata.RootElement.GetProperty("issuer").GetString());
        }
        using var signingKey = RSA.Create();
        signingKey.ImportFromPem(await File.ReadAllTextAsync(Field("clientPrivateKeyFile"), deadline.Token));
        var settings = new Dictionary<string, string?>
        {
            ["Kafdeck:NonBrowserRead:Enabled"] = "true", ["Kafdeck:NonBrowserRead:Issuer"] = issuer,
            ["Kafdeck:NonBrowserRead:ApiAudience"] = audience, ["Kafdeck:NonBrowserRead:TrustedCaPemFile"] = Field("caPemFile"),
            ["Kafdeck:NonBrowserRead:Registrations:0:Id"] = "g2-ha-service",
            ["Kafdeck:NonBrowserRead:Registrations:0:OwnerId"] = "g2-fixture-owner",
            ["Kafdeck:NonBrowserRead:Registrations:0:Subject"] = subject,
            ["Kafdeck:NonBrowserRead:Registrations:0:ClientId"] = clientId,
            ["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "true",
            ["Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(5).ToString("O"),
            ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0"] = "SystemRead",
        };
        using var verifier = new NonBrowserReadTokenVerifier(new NonBrowserReadRegistry(new ConfigurationBuilder().AddInMemoryCollection(settings).Build()));
        const int rounds = 10;
        const int contenders = 8;
        var successes = new int[rounds];
        var statuses = new List<int[]>();
        for (var round = 0; round < rounds; round++)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var expires = now + 45;
            var header = Encode(JsonSerializer.SerializeToUtf8Bytes(new { alg = "RS256", typ = "JWT" }));
            var payload = Encode(JsonSerializer.SerializeToUtf8Bytes(new
            {
                iss = clientId, sub = clientId, aud = issuer + "/protocol/openid-connect/token",
                iat = now, nbf = now, exp = expires, jti = Guid.NewGuid().ToString("N"),
            }));
            var signingInput = header + "." + payload;
            // This is an OAuth client assertion, never an imitation API token.
            var assertion = signingInput + "." + Encode(signingKey.SignData(Encoding.ASCII.GetBytes(signingInput), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
            var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var arrived = 0;
            async Task<(int Status, string? AccessToken)> Exchange(int index, bool synchronize)
            {
                if (synchronize)
                {
                    Interlocked.Increment(ref arrived);
                    await barrier.Task.WaitAsync(deadline.Token);
                }
                using var response = await http.PostAsync(nodes[index % nodes.Length] + "/protocol/openid-connect/token",
                    new FormUrlEncodedContent(new Dictionary<string, string>
                    {
                        ["grant_type"] = "client_credentials", ["client_id"] = clientId,
                        ["client_assertion_type"] = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer", ["client_assertion"] = assertion,
                    }), deadline.Token);
                if (!response.IsSuccessStatusCode) return ((int)response.StatusCode, null);
                Assert.True(response.Content.Headers.ContentLength is null or <= 64 * 1024, "Unexpectedly large token response.");
                using var tokens = JsonDocument.Parse(await response.Content.ReadAsStringAsync(deadline.Token));
                return ((int)response.StatusCode, tokens.RootElement.GetProperty("access_token").GetString());
            }
            var requests = Enumerable.Range(0, contenders).Select(index => Exchange(index, synchronize: true)).ToArray();
            Assert.Equal(contenders, Volatile.Read(ref arrived));
            Assert.True(DateTimeOffset.UtcNow.ToUnixTimeSeconds() < expires - 5, "The fresh assertion expired before the concurrency barrier.");
            barrier.SetResult();
            var results = await Task.WhenAll(requests);
            successes[round] = results.Count(result => result.Status == 200);
            Assert.Equal(1, successes[round]);
            Assert.All(results.Where(result => result.Status != 200), result => Assert.Equal(400, result.Status));
            var winner = results.Single(result => result.Status == 200).AccessToken;
            Assert.NotNull(winner);
            var identity = await verifier.VerifyAsync(winner, deadline.Token);
            Assert.NotNull(identity);
            Assert.Equal(subject, identity.Evidence.Subject);
            Assert.Equal(clientId, identity.Evidence.ClientId);
            Assert.Equal(NonBrowserPrincipalKind.Service, identity.Kind);
            Assert.True(DateTimeOffset.UtcNow.ToUnixTimeSeconds() < expires - 2, "Replay checks must occur while the consumed assertion is still time-valid.");
            for (var node = 0; node < nodes.Length; node++)
                Assert.Equal(400, (await Exchange(node, synchronize: false)).Status);
            statuses.Add(results.Select(result => result.Status).ToArray());
        }
        var directory = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.GetDirectoryName(labPath)!;
        Directory.CreateDirectory(directory);
        await File.WriteAllTextAsync(Path.Combine(directory, "keycloak-ha-first-use-replay-evidence.json"), JsonSerializer.Serialize(new
        {
            observedAtUtc = DateTimeOffset.UtcNow, keycloakVersion = Field("keycloakVersion"), issuer, audience,
            nodes = root.GetProperty("nodes").EnumerateArray().Select(node => node.GetProperty("name").GetString()).ToArray(),
            cache = Field("cache"), stack = Field("stack"), clusterMtls = root.GetProperty("clusterMtls").GetBoolean(),
            rounds, contendersPerRound = contenders, requestsPerNodePerRound = contenders / nodes.Length,
            synchronization = "all eight contenders reached a fresh per-round barrier before release",
            firstUseSuccessesPerRound = successes, firstUseHttpStatuses = statuses,
            timeValidConsumedReplayRejections = rounds * nodes.Length,
            accessTokensVerifiedByProductionVerifier = rounds,
            assertionLifetimeSeconds = 45,
            caveat = "Two genuine nodes on one Windows host with shared PostgreSQL; no load-balancer, restart, signing/client-key rotation or version-upgrade proof in this test.",
        }, new JsonSerializerOptions { WriteIndented = true }), deadline.Token);
    }

    private static string Encode(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}
