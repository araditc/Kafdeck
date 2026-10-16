using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Kafdeck.Cli;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67CliSocketCallbackTests
{
    [Fact]
    public async Task Real_listener_rejects_wrong_state_path_host_method_duplicate_query_issuer_and_missing_code()
    {
        await using var fixture = await MetadataFixture.StartAsync();
        var rejected = 0;
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var error = await Assert.ThrowsAsync<CliOidcAuthenticationException>(() => CliOidcPkceAuthenticator.AcquireAsync(fixture.Profile,
            async (authorization, token) =>
            {
                var query = QueryHelpers.ParseQuery(authorization.Query);
                var redirect = new Uri(query["redirect_uri"].ToString());
                Assert.Equal("127.0.0.1", redirect.Host);
                Assert.Equal("/oauth/callback", redirect.AbsolutePath);
                Assert.Equal("S256", query["code_challenge_method"].ToString());
                var state = query["state"].ToString();
                var target = "/oauth/callback?code=untrusted-code-sentinel&state=" + state;
                var attempts = new[]
                {
                    ("GET", "/oauth/callback?code=untrusted-code-sentinel&state=wrong", redirect.Authority),
                    ("GET", "/wrong?code=untrusted-code-sentinel&state=" + state, redirect.Authority),
                    ("GET", target, "localhost:" + redirect.Port),
                    ("POST", target, redirect.Authority),
                    ("GET", target + "&state=" + state, redirect.Authority),
                    ("GET", target + "&iss=https%3A%2F%2Fwrong.test", redirect.Authority),
                    ("GET", "/oauth/callback?state=" + state, redirect.Authority),
                    ("GET", target + "&%73tate=" + state, redirect.Authority),
                };
                foreach (var (method, path, host) in attempts)
                {
                    using var socket = new TcpClient(AddressFamily.InterNetwork);
                    await socket.ConnectAsync(IPAddress.Loopback, redirect.Port, token);
                    await using var stream = socket.GetStream();
                    await stream.WriteAsync(Encoding.ASCII.GetBytes($"{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"), token);
                    using var response = new MemoryStream();
                    await stream.CopyToAsync(response, token);
                    var text = Encoding.ASCII.GetString(response.ToArray());
                    Assert.StartsWith("HTTP/1.1 400", text, StringComparison.Ordinal);
                    Assert.DoesNotContain("untrusted-code-sentinel", text, StringComparison.Ordinal);
                    rejected++;
                }
            }, deadline.Token));
        Assert.Equal(8, rejected);
        Assert.DoesNotContain("untrusted-code-sentinel", error.ToString(), StringComparison.Ordinal);
        Assert.Equal(1, fixture.DiscoveryRequests);
        Assert.Equal(0, fixture.TokenRequests);
    }

    [Fact]
    public async Task Actual_partial_header_connection_has_finite_deadline_without_waiting_for_user_flow_timeout()
    {
        await using var fixture = await MetadataFixture.StartAsync();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        TcpClient? stalled = null;
        var watch = Stopwatch.StartNew();
        try
        {
            await Assert.ThrowsAsync<CliOidcAuthenticationException>(() => CliOidcPkceAuthenticator.AcquireAsync(fixture.Profile,
                async (authorization, token) =>
                {
                    var redirect = new Uri(QueryHelpers.ParseQuery(authorization.Query)["redirect_uri"].ToString());
                    stalled = new TcpClient(AddressFamily.InterNetwork);
                    await stalled.ConnectAsync(IPAddress.Loopback, redirect.Port, token);
                    await stalled.GetStream().WriteAsync(Encoding.ASCII.GetBytes("GET /oauth/callback HTTP/1.1\r\nHost:"), token);
                }, deadline.Token));
            Assert.False(deadline.IsCancellationRequested);
            Assert.True(watch.Elapsed < TimeSpan.FromSeconds(15));
            Assert.Equal(0, fixture.TokenRequests);
        }
        finally { stalled?.Dispose(); }
    }

    // An HTTPS metadata fixture only: no token or authenticated positive path is
    // fabricated. Success acceptance belongs to the real Keycloak test.
    private sealed class MetadataFixture : IAsyncDisposable
    {
        private readonly WebApplication _app;
        private readonly X509Certificate2 _certificate;
        private readonly string _caPath;
        public CliOidcProfile Profile { get; }
        public int DiscoveryRequests { get; private set; }
        public int TokenRequests { get; private set; }
        private MetadataFixture(WebApplication app, X509Certificate2 certificate, string caPath, string issuer)
        { _app = app; _certificate = certificate; _caPath = caPath; Profile = new(issuer, "fixture-api", "fixture-cli", caPath); }

        public static async Task<MetadataFixture> StartAsync()
        {
            using var key = RSA.Create(2048);
            var request = new CertificateRequest("CN=Kafdeck CLI local metadata fixture", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            var san = new SubjectAlternativeNameBuilder(); san.AddIpAddress(IPAddress.Loopback); request.CertificateExtensions.Add(san.Build());
            using var generated = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddHours(1));
            var certificate = X509CertificateLoader.LoadPkcs12(generated.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.DefaultKeySet);
            var caPath = Path.Combine(Path.GetTempPath(), "kafdeck-cli-fixture-" + Guid.NewGuid().ToString("N") + ".pem");
            await File.WriteAllTextAsync(caPath, certificate.ExportCertificatePem());
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(options => options.Listen(IPAddress.Loopback, 0, endpoint => endpoint.UseHttps(certificate)));
            var app = builder.Build();
            MetadataFixture? fixture = null;
            app.MapGet("/realms/offline/.well-known/openid-configuration", () =>
            {
                fixture!.DiscoveryRequests++;
                var issuer = fixture.Profile.Issuer;
                return new { issuer, authorization_endpoint = issuer + "/protocol/openid-connect/auth",
                    token_endpoint = issuer + "/protocol/openid-connect/token", jwks_uri = issuer + "/protocol/openid-connect/certs",
                    code_challenge_methods_supported = new[] { "S256" } };
            });
            app.MapPost("/realms/offline/protocol/openid-connect/token", () => { fixture!.TokenRequests++; return Microsoft.AspNetCore.Http.Results.Unauthorized(); });
            await app.StartAsync();
            fixture = new(app, certificate, caPath, app.Urls.Single().TrimEnd('/') + "/realms/offline");
            return fixture;
        }
        public async ValueTask DisposeAsync()
        { await _app.StopAsync(); await _app.DisposeAsync(); _certificate.Dispose(); File.Delete(_caPath); }
    }
}
