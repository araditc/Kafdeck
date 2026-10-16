using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Kafdeck.Api;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KeycloakLifecycleFactAttribute : FactAttribute
{
    public KeycloakLifecycleFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LIFECYCLE_LAB")))
            Skip = "Requires a separate genuine Keycloak lifecycle realm with explicitly enabled disposable control; never mutates the ordinary G2 identity lab.";
    }
}

public sealed class V08G2IdentityLifecycleLiveTests
{
    [KeycloakLifecycleFact]
    public async Task Real_signing_and_client_key_rotation_and_live_registration_revocation_reach_shipped_API()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        using var identity = new G2KeycloakLifecycleClient(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LIFECYCLE_LAB")!);
        var root = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.GetTempPath();
        Directory.CreateDirectory(root);
        await using var api = new G2ReloadingIdentityApi(identity, Path.Combine(root, "identity-api-" + Guid.NewGuid().ToString("N")));
        using var originalKey = RSA.Create();
        originalKey.ImportFromPem(await File.ReadAllTextAsync(identity.ClientKeyPath, token));
        var initial = await identity.ExchangeAsync(originalKey, token);
        Assert.True(initial.Status == HttpStatusCode.OK && initial.Token is not null, "Genuine initial private_key_jwt acquisition failed; response withheld.");
        var initialToken = initial.Token!;
        var originalKid = G2KeycloakLifecycleClient.KeyId(initialToken);
        await api.StartAsync(token);
        Assert.Equal(200, await api.GetAsync(initialToken, token));

        var activeKid = await identity.RotateSigningKeyAsync(token);
        Assert.NotEqual(originalKid, activeKid);
        var rotated = await identity.ExchangeAsync(originalKey, token);
        Assert.True(rotated.Status == HttpStatusCode.OK && rotated.Token is not null, "Genuine rotated signing-key acquisition failed; response withheld.");
        Assert.Equal(activeKid, G2KeycloakLifecycleClient.KeyId(rotated.Token!));
        await api.WaitStatusAsync(rotated.Token!, 200, token);
        Assert.Equal(200, await api.GetAsync(initialToken, token));
        await api.ChangeAsync("Kafdeck:NonBrowserRead:RevokedSigningKeyIds:0", originalKid, token);
        await api.WaitStatusAsync(initialToken, 401, token);
        Assert.Equal(200, await api.GetAsync(rotated.Token!, token));

        using var replacementKey = RSA.Create(2048);
        var certificateRequest = new CertificateRequest("CN=g2-rotated-client", replacementKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var replacementCert = certificateRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddHours(1));
        using var originalCertificate = new CertificateRequest("CN=kafdeck-g2-service", originalKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1)
            .CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddHours(1));
        try
        {
            await identity.ReplaceServiceCertificateAsync(Convert.ToBase64String(replacementCert.RawData), token);
            var oldClientKey = await identity.ExchangeAsync(originalKey, token);
            Assert.True(oldClientKey.Status != HttpStatusCode.OK && oldClientKey.Token is null, "Retired client private key was accepted.");
            var fresh = await identity.ExchangeAsync(replacementKey, token);
            Assert.True(fresh.Status == HttpStatusCode.OK && fresh.Token is not null, "Replacement client private key was refused.");
            Assert.Equal(200, await api.GetAsync(fresh.Token!, token));
            await identity.SetServiceEnabledAsync(false, token);
            var disabled = await identity.ExchangeAsync(replacementKey, token);
            Assert.True(disabled.Status != HttpStatusCode.OK && disabled.Token is null, "Disabled Keycloak client acquired credentials.");
            // Offline access JWTs remain signed until expiry. Application-side
            // current registration revocation is the immediate effect barrier.
            await api.ChangeAsync("Kafdeck:NonBrowserRead:Registrations:0:Enabled", "false", token);
            await api.WaitStatusAsync(fresh.Token!, 401, token);
            await api.ChangeAsync("Kafdeck:NonBrowserRead:Registrations:0:Enabled", "true", token);
            await api.WaitStatusAsync(fresh.Token!, 200, token);
            await api.ChangeAsync("Kafdeck:NonBrowserRead:Registrations:0:OwnerId", "", token);
            await api.WaitStatusAsync(fresh.Token!, 401, token);
            await api.ChangeAsync("Kafdeck:NonBrowserRead:Registrations:0:OwnerId", "g2-lab-owner", token);
            await api.WaitStatusAsync(fresh.Token!, 200, token);
            await api.ChangeAsync("Kafdeck:Authorization:Roles:0:Permissions:0:Action", "ClusterRead", token);
            await api.WaitStatusAsync(fresh.Token!, 403, token);
            await api.ChangeAsync("Kafdeck:Authorization:Roles:0:Permissions:0:Action", "SystemRead", token);
            await api.WaitStatusAsync(fresh.Token!, 200, token);
            var registration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafdeck:NonBrowserRead:Enabled"]="true", ["Kafdeck:NonBrowserRead:Issuer"]=identity.Issuer,
                ["Kafdeck:NonBrowserRead:ApiAudience"]=identity.Audience, ["Kafdeck:NonBrowserRead:TrustedCaPemFile"]=identity.CaPath,
                ["Kafdeck:NonBrowserRead:Registrations:0:Id"]="offline-lifecycle", ["Kafdeck:NonBrowserRead:Registrations:0:OwnerId"]="owner",
                ["Kafdeck:NonBrowserRead:Registrations:0:Subject"]=identity.Subject,["Kafdeck:NonBrowserRead:Registrations:0:ClientId"]=identity.ClientId,
                ["Kafdeck:NonBrowserRead:Registrations:0:Enabled"]="true",["Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc"]=DateTimeOffset.UtcNow.AddMinutes(5).ToString("O"),
                ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0"]="SystemRead",
            }).Build();
            using var restartedVerifier = new NonBrowserReadTokenVerifier(new NonBrowserReadRegistry(registration));
            Assert.NotNull(await restartedVerifier.VerifyAsync(fresh.Token!, token));
            await File.WriteAllBytesAsync(Path.Combine(root, "keycloak-live-key-registration-lifecycle-evidence.json"),
                JsonSerializer.SerializeToUtf8Bytes(new
                {
                    genuineKeycloak = true, realShippedApiProcess = true, actualTcpRequests = true,
                    issuerSigningKeyRotated = true, newKeyAcceptedByAlreadyRunningApi = true,
                    oldSigningKeyOverlapAcceptedBeforeRevocation = true, cachedSigningKeyRevocationDenied = true,
                    clientCertificateRotated = true, retiredPrivateKeyDeniedAtIssuer = true, replacementPrivateKeyAccepted = true,
                    disabledClientAcquisitionDenied = true, liveApplicationRegistrationRevocationDeniedIssuedToken = true,
                    ownerRemovalDeniedIssuedToken = true, currentBackendRbacRemovalDeniedIssuedToken = true,
                    freshVerifierAfterRestartAcceptedCurrentKey = true, tokensSaved = false, rawResponsesSaved = false,
                    offlineJwtCaveat = "Issuer disable prevents acquisition; current application registration revocation provides immediate rejection of already-issued offline JWTs.",
                }, new JsonSerializerOptions { WriteIndented = true }), token);
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await identity.SetServiceEnabledAsync(true, cleanup.Token);
            await identity.ReplaceServiceCertificateAsync(Convert.ToBase64String(originalCertificate.RawData), cleanup.Token);
        }
    }
}
