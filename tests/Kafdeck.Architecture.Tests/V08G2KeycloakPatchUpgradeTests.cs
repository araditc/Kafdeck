using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Api;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KeycloakPatchUpgradeFactAttribute : FactAttribute
{
    public KeycloakPatchUpgradeFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_UPGRADE_INPUTS")))
            Skip = "Requires explicit pinned old/new Keycloak archives, Java21 and an isolated PostgreSQL fixture; no same version substitute.";
    }
}

[Collection("Native notification recovery")]
public sealed class V08G2KeycloakPatchUpgradeTests
{
    [KeycloakPatchUpgradeFact]
    public async Task Real_patch_upgrade_preserves_cross_node_assertion_replay_and_production_token_verification()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(18));
        var token = deadline.Token;
        var inputPath = Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_UPGRADE_INPUTS")!;
        Assert.True(Path.IsPathFullyQualified(inputPath));
        var input = await File.ReadAllBytesAsync(inputPath, token);
        Assert.InRange(input.Length, 1, 16 * 1024);
        var options = JsonSerializer.Deserialize<UpgradeInputs>(input,
            new JsonSerializerOptions { UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow })!;
        Assert.NotNull(options);
        foreach (var path in new[] { options.RootDirectory, options.IdentityDirectory, options.PostgresManifest,
                     options.OldArchive, options.NewArchive, options.JavaHome })
            Assert.True(Path.IsPathFullyQualified(path), "Upgrade inputs must use explicit absolute fixture paths.");
        var repository = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(repository.FullName, "Kafdeck.slnx")))
            repository = repository.Parent ?? throw new InvalidOperationException("Repository root unavailable.");
        var scope = Path.Combine(options.RootDirectory, "patch-" + Guid.NewGuid().ToString("N"));
        var script = Path.Combine(repository.FullName, "deploy", "dev", "prove-keycloak-g2-upgrade.ps1");
        try
        {
            await RunAsync(script, scope, "Prepare", options, token);
            await RunAsync(script, scope, "Prove", options, token);
            var nativeBytes = await File.ReadAllBytesAsync(Path.Combine(scope, "keycloak-patch-rolling-upgrade-evidence.json"), token);
            Assert.InRange(nativeBytes.Length, 1, 16 * 1024);
            using var evidence = JsonDocument.Parse(nativeBytes);
            var observed = evidence.RootElement;
            Assert.Equal("26.8.0", observed.GetProperty("oldVersion").GetString());
            Assert.Equal("26.8.1", observed.GetProperty("newVersion").GetString());
            Assert.True(observed.GetProperty("normalTls").GetBoolean());
            Assert.True(observed.GetProperty("mixedVersionPhaseObserved").GetBoolean());
            Assert.True(observed.GetProperty("bothNewVersionsObserved").GetBoolean());
            Assert.True(observed.GetProperty("actualTwoNodeMembership").GetBoolean());
            Assert.True(observed.GetProperty("freshTokenDuringEachSingleNodeReplacement").GetBoolean());
            Assert.True(observed.GetProperty("sameVersionBounceDistinctFromUpgrade").GetBoolean());
            Assert.Equal([0, 0], observed.GetProperty("compatibilityCheckExitCodes").EnumerateArray().Select(x => x.GetInt32()).ToArray());
            Assert.Equal([1, 1], observed.GetProperty("firstUseWinnersPerRound").EnumerateArray().Select(x => x.GetInt32()).ToArray());
            Assert.Equal(16, observed.GetProperty("timeValidConsumedReplayRejections").GetInt32());

            using var manifest = JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(scope, "upgrade-lab.json"), token));
            string Field(string name) => manifest.RootElement.GetProperty(name).GetString()!;
            var settings = new Dictionary<string, string?>
            {
                ["Kafdeck:NonBrowserRead:Enabled"] = "true",
                ["Kafdeck:NonBrowserRead:Issuer"] = Field("issuer"),
                ["Kafdeck:NonBrowserRead:ApiAudience"] = Field("audience"),
                ["Kafdeck:NonBrowserRead:TrustedCaPemFile"] = Field("caPemFile"),
                ["Kafdeck:NonBrowserRead:Registrations:0:Id"] = "patch-upgrade-fixture",
                ["Kafdeck:NonBrowserRead:Registrations:0:OwnerId"] = "isolated-fixture-owner",
                ["Kafdeck:NonBrowserRead:Registrations:0:Subject"] = Field("subject"),
                ["Kafdeck:NonBrowserRead:Registrations:0:ClientId"] = Field("clientId"),
                ["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "true",
                ["Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(5).ToString("O"),
                ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0"] = "SystemRead",
            };
            using var verifier = new NonBrowserReadTokenVerifier(new NonBrowserReadRegistry(
                new ConfigurationBuilder().AddInMemoryCollection(settings).Build()));
            var accessToken = await File.ReadAllTextAsync(Path.Combine(scope, "final-access-token-private.txt"), token);
            Assert.InRange(accessToken.Length, 1, 32 * 1024);
            var identity = await verifier.VerifyAsync(accessToken, token);
            Assert.NotNull(identity);
            Assert.Equal(Field("subject"), identity.Evidence.Subject);
            Assert.Equal(Field("clientId"), identity.Evidence.ClientId);
            Assert.Equal(NonBrowserPrincipalKind.Service, identity.Kind);
            var directory = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.Combine(repository.FullName, "artifacts", "g2-upgrade");
            Directory.CreateDirectory(directory);
            await File.WriteAllBytesAsync(Path.Combine(directory, "keycloak-patch-rolling-upgrade-evidence.json"), nativeBytes, token);
            await File.WriteAllBytesAsync(Path.Combine(directory, "keycloak-patch-upgrade-production-verifier-evidence.json"),
                JsonSerializer.SerializeToUtf8Bytes(new
                {
                    observedAtUtc = DateTimeOffset.UtcNow, oldVersion = "26.8.0", newVersion = "26.8.1",
                    nativeEvidenceSha256 = Convert.ToHexString(SHA256.HashData(nativeBytes)).ToLowerInvariant(),
                    productionVerifierChecked = true, actualSignedAccessToken = true, normalTls = true,
                    exactSubjectAndClient = true, rawCredentialsInEvidence = false,
                }, new JsonSerializerOptions { WriteIndented = true }), token);
        }
        finally
        {
            if (File.Exists(Path.Combine(scope, "upgrade-lab.json")))
            {
                using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(45));
                await RunAsync(script, scope, "Stop", options, cleanup.Token);
            }
        }
    }

    private static async Task RunAsync(string script, string scope, string action, UpgradeInputs inputs, CancellationToken token)
    {
        var start = new ProcessStartInfo("pwsh") { UseShellExecute = false, CreateNoWindow = true,
            RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var value in new[] { "-NoProfile", "-NonInteractive", "-File", script, "-Action", action,
                     "-OutputDirectory", scope, "-IdentityDirectory", inputs.IdentityDirectory, "-PostgresManifest", inputs.PostgresManifest,
                     "-OldArchive", inputs.OldArchive, "-NewArchive", inputs.NewArchive, "-JavaHome", inputs.JavaHome })
            start.ArgumentList.Add(value);
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Upgrade control process did not start.");
        using var capture = CancellationTokenSource.CreateLinkedTokenSource(token);
        var output = Task.WhenAll(PostgreSqlRecoveryScenario.ReadBoundedAsync(process.StandardOutput, capture.Token),
            PostgreSqlRecoveryScenario.ReadBoundedAsync(process.StandardError, capture.Token));
        try
        {
            while (!process.HasExited)
            {
                if (output.IsFaulted) await output;
                await Task.Delay(50, token);
            }
            token.ThrowIfCancellationRequested();
            Assert.True(process.ExitCode == 0, "Actual Keycloak upgrade control action refused: " + action + "; retained private fixture diagnostics are not public evidence.");
        }
        finally
        {
            if (!process.HasExited) process.Kill(true);
            capture.Cancel();
            process.StandardOutput.Dispose(); process.StandardError.Dispose();
            try { await output.WaitAsync(TimeSpan.FromSeconds(2), CancellationToken.None); }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            catch (IOException) { }
            catch (TimeoutException)
            {
                _ = output.ContinueWith(static t => { _ = t.Exception; }, CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
        }
    }

    private sealed record UpgradeInputs(string RootDirectory, string IdentityDirectory, string PostgresManifest,
        string OldArchive, string NewArchive, string JavaHome);
}
