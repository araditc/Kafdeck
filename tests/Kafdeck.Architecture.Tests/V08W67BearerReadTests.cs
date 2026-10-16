using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KeycloakG1FactAttribute : FactAttribute
{
    public KeycloakG1FactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G1_LAB")))
            Skip = "Requires genuine running Keycloak 26.8.1 TLS lab; set KAFDECK_KEYCLOAK_G1_LAB to generated lab.json.";
    }
}

public sealed class V08W67BearerReadTests
{
    [Fact]
    public async Task Disabled_default_does_not_accept_bearer_even_in_local_deployment_mode()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection().Build();
        var registry = new NonBrowserReadRegistry(config);
        Assert.False(registry.Trust.Enabled);
        using var verifier = new NonBrowserReadTokenVerifier(registry);
        Assert.Null(await verifier.VerifyAsync("sentinel-invalid-token", default));
        var context = new DefaultHttpContext();
        context.Request.Method = "GET";
        context.Request.Headers.Authorization = "Bearer sentinel-invalid-token";
        context.Response.Body = new MemoryStream();
        var called = false;
        var middleware = new NonBrowserReadBoundaryMiddleware(_ => { called = true; return Task.CompletedTask; });
        var audit = new Audit();
        await middleware.InvokeAsync(context, verifier, audit);
        Assert.Equal(401, context.Response.StatusCode);
        Assert.False(called);
        context.Response.Body.Position = 0;
        Assert.DoesNotContain("sentinel", await new StreamReader(context.Response.Body).ReadToEndAsync());
        Assert.DoesNotContain("sentinel", JsonSerializer.Serialize(audit.Events));
    }

    [Theory]
    [InlineData("POST", false, false)]
    [InlineData("GET", true, false)]
    [InlineData("GET", false, true)]
    public async Task Cookie_deployment_collision_and_non_get_are_rejected_without_fallback(string method, bool cookie, bool deployment)
    {
        var registry = new NonBrowserReadRegistry(new ConfigurationBuilder().Build());
        using var verifier = new NonBrowserReadTokenVerifier(registry);
        var context = new DefaultHttpContext();
        context.Request.Method = method;
        context.Request.Headers.Authorization = "Bearer sentinel-token";
        if (cookie) context.Request.Headers.Cookie = "Kafdeck.Session=sentinel-cookie";
        if (deployment) context.Request.Headers["X-Kafdeck-Access-Token"] = "sentinel-deployment";
        context.Response.Body = new MemoryStream();
        var middleware = new NonBrowserReadBoundaryMiddleware(_ => throw new InvalidOperationException("Fallback must not run."));
        await middleware.InvokeAsync(context, verifier, new Audit());
        Assert.Equal(401, context.Response.StatusCode);
    }

    [Fact]
    public void Direct_subject_rbac_does_not_import_external_group_or_other_subject_permissions()
    {
        var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new(
            [new("viewer", [new(AuthorizationAction.ClusterRead, ["permitted"])])],
            [new("https://sso.test/realms/kafdeck", "registered", ["viewer"])],
            [new("admin", ["viewer"])])));
        Assert.True(evaluator.EvaluateDirectSubject(new("https://sso.test/realms/kafdeck", "registered"),
            new(AuthorizationAction.ClusterRead, "permitted")).IsAllowed);
        Assert.False(evaluator.EvaluateDirectSubject(new("https://sso.test/realms/kafdeck", "registered"),
            new(AuthorizationAction.ClusterRead, "denied")).IsAllowed);
        Assert.False(evaluator.EvaluateDirectSubject(new("https://sso.test/realms/kafdeck", "unknown"),
            new(AuthorizationAction.ClusterRead, "permitted")).IsAllowed);
    }

    [Fact]
    public void Revocation_reloads_registration_configuration_and_trust_change_fails_closed()
    {
        var config = RegistrationConfiguration("https://sso.test/realms/kafdeck", "service", "client");
        var registry = new NonBrowserReadRegistry(config);
        Assert.NotNull(registry.Find("service", "client"));
        Assert.Null(registry.Find("other-subject", "client"));
        Assert.Null(registry.Find("service", "other-client"));
        config["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "false";
        Assert.Null(registry.Find("service", "client"));
        config["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "true";
        config["Kafdeck:NonBrowserRead:Issuer"] = "https://other.test/realms/kafdeck";
        Assert.Null(registry.Find("service", "client"));
    }

    [Fact]
    public void Changing_trusted_ca_requires_restart_and_denies_old_trust()
    {
        var config = RegistrationConfiguration("https://sso.test/realms/kafdeck", "service", "client");
        config["Kafdeck:NonBrowserRead:TrustedCaPemFile"] = "approved-ca.pem";
        var registry = new NonBrowserReadRegistry(config);
        Assert.NotNull(registry.Find("service", "client"));
        config["Kafdeck:NonBrowserRead:TrustedCaPemFile"] = "different-ca.pem";
        Assert.Null(registry.Find("service", "client"));
    }

    [Fact]
    public async Task Stalled_discovery_body_and_queued_refresh_have_total_deadlines()
    {
        var registry = new NonBrowserReadRegistry(RegistrationConfiguration("https://sso.test/realms/kafdeck", "service", "client"));
        using var http = new HttpClient(new StalledDiscoveryHandler());
        using var verifier = new NonBrowserReadTokenVerifier(registry, http, TimeSpan.FromMilliseconds(150));
        string Encode(object value) => Convert.ToBase64String(JsonSerializer.SerializeToUtf8Bytes(value)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        // Untrusted selector claims only. No signed access token or auth success
        // is fabricated: trust fetch must time out before signature validation.
        var token = Encode(new { kid = "key" }) + "." + Encode(new { sub = "service", azp = "client" }) + ".x";
        var watch = Stopwatch.StartNew();
        var results = await Task.WhenAll(verifier.VerifyAsync(token, default), verifier.VerifyAsync(token, default));
        Assert.All(results, Assert.Null);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(3));
    }

    [KeycloakG1Fact]
    public async Task Genuine_private_key_jwt_Keycloak_token_reaches_actual_host_allows_GET_and_denies_cross_scope()
    {
        var labPath = Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G1_LAB")!;
        var evidenceDirectory = Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G1_EVIDENCE_DIRECTORY") ?? Path.GetDirectoryName(labPath)!;
        Directory.CreateDirectory(evidenceDirectory);
        using var lab = JsonDocument.Parse(await File.ReadAllTextAsync(labPath));
        string Field(string name) => lab.RootElement.GetProperty(name).GetString()!;
        var issuer = Field("issuer");
        var client = Field("clientId");
        var subject = Field("subject");
        using var ca = X509Certificate2.CreateFromPem(await File.ReadAllTextAsync(Field("caPemFile")));
        using var keycloak = new HttpClient(new SocketsHttpHandler
        {
            AllowAutoRedirect = false, UseProxy = false,
            SslOptions = new SslClientAuthenticationOptions
            {
                CertificateChainPolicy = new X509ChainPolicy
                {
                    TrustMode = X509ChainTrustMode.CustomRootTrust, RevocationMode = X509RevocationMode.NoCheck,
                    CustomTrustStore = { ca },
                },
            },
        });
        var tokenEndpoint = issuer + "/protocol/openid-connect/token";
        using var key = RSA.Create();
        key.ImportFromPem(await File.ReadAllTextAsync(Field("clientPrivateKeyFile")));
        string Encode(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var h = Encode(JsonSerializer.SerializeToUtf8Bytes(new { alg = "RS256", typ = "JWT" }));
        var p = Encode(JsonSerializer.SerializeToUtf8Bytes(new { iss = client, sub = client, aud = tokenEndpoint,
            iat = now, nbf = now, exp = now + 45, jti = Guid.NewGuid().ToString("N") }));
        var assertion = h + "." + p + "." + Encode(key.SignData(Encoding.ASCII.GetBytes(h + "." + p),
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
        async Task<HttpResponseMessage> Exchange(string value) => await keycloak.PostAsync(tokenEndpoint,
            new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "client_credentials", ["client_id"] = client,
                ["client_assertion_type"] = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer", ["client_assertion"] = value,
            }));
        using var exchange = await Exchange(assertion);
        Assert.True(exchange.IsSuccessStatusCode, "Real Keycloak signed client assertion exchange failed (response withheld).");
        using var tokenDocument = JsonDocument.Parse(await exchange.Content.ReadAsStringAsync());
        var token = tokenDocument.RootElement.GetProperty("access_token").GetString()!;
        using var replay = await Exchange(assertion);
        Assert.False(replay.IsSuccessStatusCode);
        using var claims = JsonDocument.Parse(Convert.FromBase64String(Pad(token.Split('.')[1])));
        Assert.Equal(issuer, claims.RootElement.GetProperty("iss").GetString());
        Assert.Equal(subject, claims.RootElement.GetProperty("sub").GetString());
        Assert.Equal("kafdeck-api-g1", claims.RootElement.GetProperty("aud").GetString());

        var directConfig = RegistrationConfiguration(issuer, subject, client);
        directConfig["Kafdeck:NonBrowserRead:ApiAudience"] = "kafdeck-api-g1";
        directConfig["Kafdeck:NonBrowserRead:TrustedCaPemFile"] = Field("caPemFile");
        var directRegistry = new NonBrowserReadRegistry(directConfig);
        using var directVerifier = new NonBrowserReadTokenVerifier(directRegistry);
        var authenticated = await directVerifier.VerifyAsync(token, default);
        Assert.NotNull(authenticated);
        Assert.False(OperatorSessionContextFactory.TryCreate(new System.Security.Claims.ClaimsPrincipal(authenticated), out _));
        directConfig["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "false";
        Assert.Null(await directVerifier.VerifyAsync(token, default));
        directConfig["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "true";
        directConfig["Kafdeck:NonBrowserRead:Registrations:0:Subject"] = "unregistered-subject";
        Assert.Null(await directVerifier.VerifyAsync(token, default));
        directConfig["Kafdeck:NonBrowserRead:Registrations:0:Subject"] = subject;
        directConfig["Kafdeck:NonBrowserRead:Registrations:0:ClientId"] = "unregistered-client";
        Assert.Null(await directVerifier.VerifyAsync(token, default));
        directConfig["Kafdeck:NonBrowserRead:Registrations:0:ClientId"] = client;
        directConfig["Kafdeck:NonBrowserRead:ApiAudience"] = "wrong-api-audience";
        using var wrongAudienceVerifier = new NonBrowserReadTokenVerifier(new NonBrowserReadRegistry(directConfig));
        Assert.Null(await wrongAudienceVerifier.VerifyAsync(token, default));

        // Exercise the shipped Program.cs/DI/middleware and actual existing v1 routes,
        // not a test-only endpoint or a locally signed access token.
        var root = FindRoot();
        var buildConfiguration = typeof(V08W67BearerReadTests).Assembly
            .GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration ?? "Debug";
        var apiDll = Path.Combine(root, "src/backend/Kafdeck.Api/bin", buildConfiguration, "net10.0/Kafdeck.Api.dll");
        var processInfo = new ProcessStartInfo("dotnet") { WorkingDirectory = Path.GetDirectoryName(apiDll)!,
            UseShellExecute = false, CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true };
        processInfo.ArgumentList.Add(apiDll);
        var config = RegistrationConfiguration(issuer, subject, client);
        config["Kafdeck:NonBrowserRead:TrustedCaPemFile"] = Field("caPemFile");
        config["Kafdeck:NonBrowserRead:ApiAudience"] = "kafdeck-api-g1";
        config["Kafdeck:Deployment:ListenUrls:0"] = "http://127.0.0.1:18086";
        config["Kafdeck:Authorization:Roles:0:Id"] = "g1-reader";
        config["Kafdeck:Authorization:Roles:0:Permissions:0:Action"] = "SystemRead";
        config["Kafdeck:Authorization:Roles:0:Permissions:1:Action"] = "ClusterRead";
        config["Kafdeck:Authorization:Roles:0:Permissions:1:ClusterIds:0"] = "permitted";
        config["Kafdeck:Authorization:SubjectBindings:0:Issuer"] = issuer;
        config["Kafdeck:Authorization:SubjectBindings:0:Subject"] = subject;
        config["Kafdeck:Authorization:SubjectBindings:0:RoleIds:0"] = "g1-reader";
        foreach (var item in config.AsEnumerable().Where(item => item.Value is not null))
            processInfo.Environment[item.Key.Replace(":", "__", StringComparison.Ordinal)] = item.Value!;
        using var process = Process.Start(processInfo)!;
        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();
        using var api = new HttpClient { BaseAddress = new("http://127.0.0.1:18086"), Timeout = TimeSpan.FromSeconds(10) };
        try
        {
            var ready = false;
            for (var attempt = 0; attempt < 80 && !process.HasExited; attempt++)
            {
                try { using var health = await api.GetAsync("/healthz"); ready = health.IsSuccessStatusCode; if (ready) break; }
                catch (HttpRequestException) { }
                await Task.Delay(250);
            }
            Assert.True(ready, "Actual Kafdeck host failed to become ready; inspect redacted lab host logs.");
            async Task<int> Get(string path, string bearer, string? extraHeader = null, string? extraValue = null, string scheme = "Bearer")
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, path);
                request.Headers.Authorization = new AuthenticationHeaderValue(scheme, bearer);
                if (extraHeader is not null) request.Headers.TryAddWithoutValidation(extraHeader, extraValue);
                using var response = await api.SendAsync(request);
                var responseBody = await response.Content.ReadAsStringAsync();
                Assert.DoesNotContain(token, responseBody);
                return (int)response.StatusCode;
            }
            var acceptedSchemes = new[] { "Bearer", "bearer", "BEARER" };
            foreach (var scheme in acceptedSchemes)
            {
                Assert.Equal(200, await Get("/api/v1/system/info", token, scheme: scheme));
                Assert.Equal(403, await Get("/api/v1/clusters/denied/health", token, scheme: scheme));
                Assert.Equal(401, await Get("/api/v1/system/info", token, "Cookie", "Kafdeck.Session=sentinel-cookie", scheme));
                Assert.Equal(401, await Get("/api/v1/system/info", token, "X-Kafdeck-Access-Token", "sentinel-deployment", scheme));
            }
            // Only the scheme is case-insensitive. Token bytes and all existing
            // limits remain exact, and other/malformed schemes never fall back.
            Assert.False(string.Equals(token, token.ToLowerInvariant(), StringComparison.Ordinal),
                "The real fixture token must contain case-sensitive characters.");
            Assert.Equal(401, await Get("/api/v1/system/info", token.ToLowerInvariant(), scheme: "bearer"));
            Assert.Equal(401, await Get("/api/v1/system/info", token + new string('a', 16 * 1024), scheme: "bearer"));
            var malformedHeaders = new[]
            {
                "Basic " + token, "BearerX " + token, "Bearer" + token,
                "Bearer\t" + token, "Bearer", "Bearer " + token + ", Bearer " + token,
            };
            foreach (var header in malformedHeaders)
            {
                using var malformed = new HttpRequestMessage(HttpMethod.Get, "/api/v1/system/info");
                Assert.True(malformed.Headers.TryAddWithoutValidation("Authorization", header));
                using var rejected = await api.SendAsync(malformed);
                Assert.Equal(HttpStatusCode.Unauthorized, rejected.StatusCode);
                Assert.False((await rejected.Content.ReadAsStringAsync()).Contains(token, StringComparison.Ordinal),
                    "Rejected response reflected bearer credentials.");
            }
            Assert.Equal(401, await Get("/api/v1/auth/session", token));
            Assert.Equal(403, await Get("/api/v1/clusters", token));
            Assert.Equal(401, await Get("/api/v1/system/info", token, "Cookie", "Kafdeck.Session=sentinel-cookie"));
            Assert.Equal(401, await Get("/api/v1/system/info", token, "X-Kafdeck-Access-Token", "sentinel-deployment"));
            Assert.Equal(401, await Get("/api/v1/system/info", "sentinel-invalid-token"));
            Assert.Equal(401, await Get("/api/v1/system/info", assertion));
            using var mutation = new HttpRequestMessage(HttpMethod.Post, "/api/v1/clusters/permitted/topics/example/mutations/create/preview");
            mutation.Headers.Authorization = new("Bearer", token);
            using var mutationResponse = await api.SendAsync(mutation);
            Assert.Equal(HttpStatusCode.Unauthorized, mutationResponse.StatusCode);

            var evidence = new { keycloak = Field("keycloakVersion"), java = Field("javaVersion"),
                issuer, audience = "kafdeck-api-g1", grant = "client_credentials/private_key_jwt",
                observedAtUtc = DateTimeOffset.UtcNow, accessTokenHadNbf = claims.RootElement.TryGetProperty("nbf", out _),
                allowedSystemGet = 200, acceptedBearerSchemes = acceptedSchemes,
                malformedSchemeCasesRejected = malformedHeaders.Length, alteredTokenCase = 401, oversizedBearer = 401,
                deniedCrossClusterGet = 403, cookieCollision = 401, deploymentCollision = 401,
                clientAssertionAsAccessToken = 401, nonAdmittedList = 401, sessionImpersonation = 401, mutation = 401,
                assertionReplay = (int)replay.StatusCode, caveat = "Single-node local laboratory; A1/A2 partial, HA/rotation/human CLI/mutation remain unaccepted." };
            await File.WriteAllTextAsync(Path.Combine(evidenceDirectory, "keycloak-evidence.json"),
                JsonSerializer.Serialize(evidence, new JsonSerializerOptions { WriteIndented = true }));
        }
        finally
        {
            if (!process.HasExited) process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync();
            var stdout = await stdoutTask;
            var stderr = await stderrTask;
            Assert.DoesNotContain(token, stdout + stderr);
            Assert.DoesNotContain(assertion, stdout + stderr);
            await File.WriteAllTextAsync(Path.Combine(evidenceDirectory, "kafdeck-host.log"), stdout + stderr);
        }
    }

    private static string Pad(string value)
    {
        var text = value.Replace('-', '+').Replace('_', '/');
        return text.PadRight((text.Length + 3) / 4 * 4, '=');
    }

    private static string FindRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Kafdeck.sln")) &&
            !File.Exists(Path.Combine(directory.FullName, "Kafdeck.slnx"))) directory = directory.Parent;
        return directory?.FullName ?? throw new InvalidOperationException("Repository root not found.");
    }

    private static IConfigurationRoot RegistrationConfiguration(string issuer, string subject, string client) =>
        new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Kafdeck:NonBrowserRead:Enabled"] = "true", ["Kafdeck:NonBrowserRead:Issuer"] = issuer,
            ["Kafdeck:NonBrowserRead:ApiAudience"] = "kafdeck-api", ["Kafdeck:NonBrowserRead:Registrations:0:Id"] = "g1-service",
            ["Kafdeck:NonBrowserRead:Registrations:0:OwnerId"] = "local-g1-owner", ["Kafdeck:NonBrowserRead:Registrations:0:Subject"] = subject,
            ["Kafdeck:NonBrowserRead:Registrations:0:ClientId"] = client, ["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "true",
            ["Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(10).ToString("O"),
            ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0"] = "SystemRead", ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:1"] = "ClusterRead",
        }).Build();

    private sealed class Audit : ISecurityAuditSink
    {
        public List<SecurityAuditEvent> Events { get; } = [];
        public ValueTask WriteAsync(SecurityAuditEvent auditEvent, CancellationToken cancellationToken = default)
        { Events.Add(auditEvent); return ValueTask.CompletedTask; }
    }

    private sealed class StalledDiscoveryHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new StalledBody()) });
    }

    private sealed class StalledBody : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        { await Task.Delay(Timeout.Infinite, cancellationToken); return 0; }
        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
