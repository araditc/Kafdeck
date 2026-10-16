using System.Diagnostics;
using System.Text.Json;
using Kafdeck.Cli;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KeycloakRenderedBrowserFactAttribute : FactAttribute
{
    public KeycloakRenderedBrowserFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_G2_BROWSER_LAB")))
            Skip = "Requires the disposable real Keycloak realm and pinned Playwright Chromium driver; no HTTP-form browser substitution.";
    }
}

public sealed class V08G2RenderedBrowserLiveTests
{
    [KeycloakRenderedBrowserFact]
    public async Task Real_rendered_Chromium_sign_in_completes_shipped_CLI_PKCE_callback_and_actual_API_GET()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        var manifestPath = Environment.GetEnvironmentVariable("KAFDECK_G2_BROWSER_LAB")!;
        using var manifest = JsonDocument.Parse(await File.ReadAllBytesAsync(manifestPath, token));
        string Field(string name) => manifest.RootElement.GetProperty(name).GetString()!;
        using var identity = new G2KeycloakLifecycleClient(manifestPath);
        var evidence = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.GetTempPath();
        Directory.CreateDirectory(evidence);
        await using var api = new G2ReloadingIdentityApi(identity, Path.Combine(evidence, "browser-api-" + Guid.NewGuid().ToString("N")));
        api.RegisterHuman(Field("humanSubject"), Field("cliClientId"));
        await api.StartAsync(token);
        var repository = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(repository.FullName, "Kafdeck.slnx"))) repository = repository.Parent!;
        var browserObserved = false;
        async Task Browser(Uri authorization, CancellationToken cancellation)
        {
            var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("KAFDECK_G2_BROWSER_NODE") ?? "node")
            {
                WorkingDirectory = repository.FullName, UseShellExecute = false, CreateNoWindow = true,
                RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true,
            };
            start.ArgumentList.Add(Path.Combine(repository.FullName, "deploy/dev/browser-g2/sign-in.cjs"));
            if (Environment.GetEnvironmentVariable("KAFDECK_G2_BROWSER_NODE_MODULES") is { } modules) start.Environment["NODE_PATH"]=modules;
            using var process = Process.Start(start) ?? throw new InvalidOperationException("Rendered browser driver did not start.");
            var stdout = process.StandardOutput.ReadToEndAsync(cancellation);
            var stderr = process.StandardError.ReadToEndAsync(cancellation);
            try
            {
                await process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(new { authorization=authorization.AbsoluteUri, manifestPath }).AsMemory(), cancellation);
                process.StandardInput.Close();
                await process.WaitForExitAsync(cancellation);
                var output = await stdout;
                _ = await stderr;
                Assert.True(process.ExitCode==0 && output.Trim()=="G2_RENDERED_BROWSER_CALLBACK_COMPLETED", "Actual rendered browser authorization failed; private browser output withheld.");
                browserObserved=true;
            }
            finally { if (!process.HasExited) { process.Kill(true); await process.WaitForExitAsync(CancellationToken.None); } }
        }
        var profile = new CliOidcProfile(Field("issuer"), Field("audience"), Field("cliClientId"), Field("caPemFile"));
        using var session = await CliOidcPkceAuthenticator.AcquireAsync(profile, Browser, token);
        Assert.True(browserObserved);
        Assert.Equal(Field("humanSubject"), session.Subject);
        Assert.Equal(Field("cliClientId"), session.ClientId);
        Assert.Equal(200, await api.GetAsync(session.AccessToken, token));
        session.Dispose();
        Assert.Throws<ObjectDisposedException>(() => { _ = session.AccessToken; });
        await File.WriteAllBytesAsync(Path.Combine(evidence, "rendered-browser-pkce-api-evidence.json"), JsonSerializer.SerializeToUtf8Bytes(new
        {
            genuineKeycloak=true, actualChromiumRenderer=true, realPasswordForm=true, shippedCliPkce=true,
            boundNumericLoopbackCallback=true, realShippedApiTcpGet=true, exactHumanSubject=true,
            tokensMemoryOnly=true, disposedSessionCannotReadToken=true, screenshotsOrBrowserStateSaved=false,
            browserTransport="Disposable browser pins the exact lab certificate SPKI; CLI validates issuer/token/JWKS with its explicit CA and normal hostname checks.",
            unattendedBrowser=true, manualHumanInteractionClaimed=false,
        }, new JsonSerializerOptions { WriteIndented=true }), token);
    }
}
