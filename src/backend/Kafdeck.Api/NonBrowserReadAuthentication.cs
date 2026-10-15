using System.Net.Security;
using System.Net;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Kafdeck.Core.Security;

namespace Kafdeck.Api;

/// <summary>Explicitly opt-in, read-only bearer admission. It cannot create an operator session.</summary>
public sealed class NonBrowserReadOptions
{
    public bool Enabled { get; set; }
    public string Issuer { get; set; } = "";
    public string ApiAudience { get; set; } = "";
    public string? TrustedCaPemFile { get; set; }
    public List<NonBrowserReadRegistration> Registrations { get; set; } = [];
}

public sealed class NonBrowserReadRegistration
{
    public string Id { get; set; } = "";
    public string OwnerId { get; set; } = "";
    public string Subject { get; set; } = "";
    public string ClientId { get; set; } = "";
    public bool Enabled { get; set; }
    public DateTimeOffset ExpiresAtUtc { get; set; }
    public List<AuthorizationAction> ReadActions { get; set; } = [];
}

/// <summary>Runtime registration is a second gate in addition to current direct backend RBAC.</summary>
public sealed class NonBrowserReadRegistry
{
    private readonly IConfiguration _configuration;
    public NonBrowserReadRegistry(IConfiguration configuration)
    {
        _configuration = configuration;
        Trust = Read();
    }

    public NonBrowserReadOptions Trust { get; }

    private NonBrowserReadOptions Read()
    {
        var options = _configuration.GetSection("Kafdeck:NonBrowserRead").Get<NonBrowserReadOptions>() ?? new();
        if (!options.Enabled) return options;
        if (options.Registrations.Count is < 1 or > 128)
            throw new InvalidOperationException("Non-browser registrations must be finite and nonempty.");
        var proposals = options.Registrations.Select(r => new NonBrowserServicePrincipalProposal(
            r.Id, r.OwnerId, options.Issuer, options.ApiAudience, r.Subject, r.ClientId, r.ReadActions)).ToArray();
        _ = new NonBrowserPrincipalProposalSet(proposals);
        return options;
    }

    public NonBrowserServicePrincipalProposal? Find(string subject, string client, string? registrationId = null)
    {
        try
        {
            // Re-read deployment configuration for disable/revocation on every
            // request and authorization decision. Trust changes need restart.
            var current = Read();
            if (!Trust.Enabled || !current.Enabled || current.Issuer != Trust.Issuer || current.ApiAudience != Trust.ApiAudience ||
                current.TrustedCaPemFile != Trust.TrustedCaPemFile)
                return null;
            var r = current.Registrations.SingleOrDefault(r => r.Subject == subject && r.ClientId == client &&
                (registrationId is null || r.Id == registrationId));
            return r is not null && r.Enabled && r.ExpiresAtUtc > DateTimeOffset.UtcNow
                ? new(r.Id, r.OwnerId, Trust.Issuer, Trust.ApiAudience, r.Subject, r.ClientId, r.ReadActions)
                : null;
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            return null;
        }
    }

    public bool IsAllowed(NonBrowserReadIdentity identity, AuthorizationRequest request,
        AuthorizationPolicyEvaluator evaluator)
    {
        var registration = Find(identity.Evidence.Subject, identity.Evidence.ClientId, identity.RegistrationId);
        return registration is not null && identity.Evidence.ExpiresAtUtc > DateTimeOffset.UtcNow &&
            registration.RequestedReadActions.Contains(request.Action) &&
            evaluator.EvaluateDirectSubject(new(identity.Evidence.Issuer, identity.Evidence.Subject), request).IsAllowed;
    }
}

public sealed class NonBrowserReadIdentity : ClaimsIdentity
{
    internal NonBrowserReadIdentity(string registrationId, NonBrowserVerifiedJwtEvidence evidence)
        : base("Kafdeck.NonBrowserRead")
    {
        RegistrationId = registrationId;
        Evidence = evidence;
    }

    public string RegistrationId { get; }
    public NonBrowserVerifiedJwtEvidence Evidence { get; }
    public string AuditId => "oidc-service:" + Uri.EscapeDataString(Evidence.Issuer) + "|" +
        Uri.EscapeDataString(Evidence.Subject) + "|" + Uri.EscapeDataString(Evidence.ClientId) + "|" +
        Uri.EscapeDataString(RegistrationId);
}

/// <summary>Fixed issuer discovery + JWKS over verified TLS, no redirects, bounded documents/keys and 60s cache.</summary>
public sealed class NonBrowserReadTokenVerifier : IDisposable
{
    private readonly NonBrowserReadRegistry _registry;
    private readonly HttpClient _http;
    private readonly SemaphoreSlim _refresh = new(1, 1);
    private Dictionary<string, string> _keys = new(StringComparer.Ordinal);
    private DateTimeOffset _validUntil;
    private X509Certificate2? _ca;
    private readonly TimeSpan _requestDeadline = TimeSpan.FromSeconds(10);

    internal NonBrowserReadTokenVerifier(NonBrowserReadRegistry registry, HttpClient http, TimeSpan requestDeadline)
    {
        if (requestDeadline <= TimeSpan.Zero || requestDeadline > TimeSpan.FromSeconds(10))
            throw new ArgumentOutOfRangeException(nameof(requestDeadline));
        _registry = registry;
        _http = http;
        _requestDeadline = requestDeadline;
    }

    public NonBrowserReadTokenVerifier(NonBrowserReadRegistry registry)
    {
        _registry = registry;
        var handler = new SocketsHttpHandler { AllowAutoRedirect = false, UseProxy = false,
            ConnectTimeout = TimeSpan.FromSeconds(5), PooledConnectionLifetime = TimeSpan.FromMinutes(1) };
        if (registry.Trust.Enabled && registry.Trust.TrustedCaPemFile is { } path)
        {
            _ca = X509Certificate2.CreateFromPem(File.ReadAllText(path));
            handler.SslOptions = new SslClientAuthenticationOptions
            {
                CertificateChainPolicy = new X509ChainPolicy
                {
                    TrustMode = X509ChainTrustMode.CustomRootTrust,
                    RevocationMode = X509RevocationMode.NoCheck,
                    CustomTrustStore = { _ca },
                },
            };
        }
        _http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) };
    }

    public async Task<NonBrowserReadIdentity?> VerifyAsync(string token, CancellationToken cancellationToken)
    {
        try
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(_requestDeadline);
            if (!_registry.Trust.Enabled || token.Length is < 1 or > 16 * 1024) return null;
            var parts = token.Split('.');
            if (parts.Length != 3 || parts[0].Length > 1400 || parts[1].Length > 11000) return null;
            using var header = JsonDocument.Parse(Decode(parts[0]), new JsonDocumentOptions { MaxDepth = 8 });
            using var payload = JsonDocument.Parse(Decode(parts[1]), new JsonDocumentOptions { MaxDepth = 12 });
            var kid = header.RootElement.GetProperty("kid").GetString();
            var subject = payload.RootElement.GetProperty("sub").GetString();
            var client = payload.RootElement.GetProperty("azp").GetString();
            if (kid is null || subject is null || client is null) return null;
            var registration = _registry.Find(subject, client);
            if (registration is null) return null;
            var keys = await GetKeysAsync(deadline.Token).ConfigureAwait(false);
            if (!keys.TryGetValue(kid, out var pem)) return null;
            using var rsa = RSA.Create();
            rsa.ImportFromPem(pem);
            var evidence = NonBrowserRs256JwtEvidenceVerifier.Verify(token, rsa, kid, registration, DateTimeOffset.UtcNow);
            return new(registration.RegistrationId, evidence);
        }
        catch (Exception ex) when (ex is NonBrowserJwtEvidenceException or JsonException or FormatException or
            InvalidOperationException or KeyNotFoundException or ArgumentException or CryptographicException or
            HttpRequestException or OperationCanceledException)
        {
            // No raw token, claim, TLS exception, IdP response or crypto details escape.
            return null;
        }
    }

    private async Task<Dictionary<string, string>> GetKeysAsync(CancellationToken cancellationToken)
    {
        await _refresh.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_validUntil > DateTimeOffset.UtcNow) return _keys;
            var issuer = _registry.Trust.Issuer;
            using var discovery = await GetDocumentAsync(issuer + "/.well-known/openid-configuration", cancellationToken);
            var expectedKeys = issuer + "/protocol/openid-connect/certs";
            if (discovery.RootElement.GetProperty("issuer").GetString() != issuer ||
                discovery.RootElement.GetProperty("jwks_uri").GetString() != expectedKeys)
                throw new NonBrowserJwtEvidenceException();
            using var jwks = await GetDocumentAsync(expectedKeys, cancellationToken);
            var array = jwks.RootElement.GetProperty("keys");
            if (array.ValueKind != JsonValueKind.Array || array.GetArrayLength() is < 1 or > 16)
                throw new NonBrowserJwtEvidenceException();
            var keys = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var key in array.EnumerateArray())
            {
                if (key.GetProperty("kty").GetString() != "RSA" ||
                    key.GetProperty("use").GetString() != "sig" ||
                    key.GetProperty("alg").GetString() != "RS256") continue;
                var kid = key.GetProperty("kid").GetString();
                if (string.IsNullOrWhiteSpace(kid) || kid.Length > 256) throw new NonBrowserJwtEvidenceException();
                var encodedModulus = key.GetProperty("n").GetString();
                var encodedExponent = key.GetProperty("e").GetString();
                if (encodedModulus is null || encodedModulus.Length > 684 || encodedExponent is null || encodedExponent.Length > 4)
                    throw new NonBrowserJwtEvidenceException();
                var modulus = Decode(encodedModulus);
                var exponent = Decode(encodedExponent);
                if (modulus.Length is < 256 or > 512 || modulus[0] == 0 ||
                    (modulus.Length == 256 && modulus[0] < 128) ||
                    !exponent.AsSpan().SequenceEqual(new byte[] { 1, 0, 1 }))
                    throw new NonBrowserJwtEvidenceException();
                using var rsa = RSA.Create();
                rsa.ImportParameters(new RSAParameters { Modulus = modulus, Exponent = exponent });
                if (rsa.KeySize is < 2048 or > 4096) throw new NonBrowserJwtEvidenceException();
                if (!keys.TryAdd(kid, rsa.ExportSubjectPublicKeyInfoPem())) throw new NonBrowserJwtEvidenceException();
            }
            if (keys.Count is < 1 or > 8) throw new NonBrowserJwtEvidenceException();
            _keys = keys;
            _validUntil = DateTimeOffset.UtcNow.AddSeconds(60);
            return keys;
        }
        finally { _refresh.Release(); }
    }

    private async Task<JsonDocument> GetDocumentAsync(string url, CancellationToken cancellationToken)
    {
        using var response = await _http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        if (response.Content.Headers.ContentLength > 64 * 1024) throw new NonBrowserJwtEvidenceException();
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var buffer = new MemoryStream();
        var chunk = new byte[4096];
        int count;
        while ((count = await stream.ReadAsync(chunk, cancellationToken)) > 0)
        {
            if (buffer.Length + count > 64 * 1024) throw new NonBrowserJwtEvidenceException();
            buffer.Write(chunk, 0, count);
        }
        return JsonDocument.Parse(buffer.ToArray(), new JsonDocumentOptions { MaxDepth = 16 });
    }

    private static byte[] Decode(string value)
    {
        if (value.Length is < 1 or > 11000 || value.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '-' and not '_'))
            throw new NonBrowserJwtEvidenceException();
        var text = value.Replace('-', '+').Replace('_', '/');
        return Convert.FromBase64String(text.PadRight((text.Length + 3) / 4 * 4, '='));
    }

    public void Dispose() { _http.Dispose(); _refresh.Dispose(); _ca?.Dispose(); }
}

public sealed class NonBrowserReadBoundaryMiddleware(RequestDelegate next)
{
    // First admitted surface only. No collection, protected config, payload,
    // mutation, operation/status, approval or browser-session route is included.
    private static readonly HashSet<string> Routes = new(StringComparer.Ordinal)
    {
        "v01-system-info", "v01-system-health", "v01-clusters-detail", "v01-clusters-health",
        "v01-clusters-capabilities", "v01-brokers-list", "v01-brokers-detail",
    };

    public async Task InvokeAsync(HttpContext context, NonBrowserReadTokenVerifier verifier, ISecurityAuditSink audit)
    {
        if (!context.Request.Headers.TryGetValue("Authorization", out var authorization))
        {
            await next(context);
            return;
        }
        var route = context.GetEndpoint()?.Metadata.GetMetadata<IEndpointNameMetadata>()?.EndpointName;
        var header = authorization.Count == 1 ? authorization[0] : null;
        var collision = context.Request.Headers.ContainsKey("Cookie") ||
            context.Request.Headers.ContainsKey("X-Kafdeck-Access-Token") || context.User.Identity?.IsAuthenticated == true;
        var secureTransport = context.Request.IsHttps ||
            context.Connection.RemoteIpAddress is { } remote && IPAddress.IsLoopback(remote);
        var admitted = secureTransport && HttpMethods.IsGet(context.Request.Method) && route is not null && Routes.Contains(route);
        var identity = !collision && admitted && header is not null && header.StartsWith("Bearer ", StringComparison.Ordinal) &&
            header.Length <= 16 * 1024 + 7
            ? await verifier.VerifyAsync(header[7..], context.RequestAborted)
            : null;
        if (identity is null)
        {
            await audit.WriteAsync(new SecurityAuditEvent(DateTimeOffset.UtcNow, SecurityAuditEventType.LoginFailed,
                SecurityAuditPrincipal.Anonymous, context.TraceIdentifier, null, null, SecurityAuditOutcome.Denied,
                collision ? "bearer_credential_collision" : admitted ? "bearer_rejected" : "bearer_route_not_admitted"), context.RequestAborted);
            context.Response.StatusCode = 401;
            context.Response.Headers.WWWAuthenticate = "Bearer";
            await context.Response.WriteAsJsonAsync(new { type = "urn:kafdeck:problem:bearer-rejected", title = "Bearer authentication rejected", status = 401 }, context.RequestAborted);
            return;
        }
        context.User = new ClaimsPrincipal(identity);
        await audit.WriteAsync(new SecurityAuditEvent(DateTimeOffset.UtcNow, SecurityAuditEventType.LoginSucceeded,
            identity.AuditId, context.TraceIdentifier, null, null, SecurityAuditOutcome.Succeeded, "registered_bearer_read"), context.RequestAborted);
        await next(context);
    }
}

public static class NonBrowserReadServiceExtensions
{
    public static IServiceCollection AddKafdeckNonBrowserReads(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton(new NonBrowserReadRegistry(configuration));
        services.AddSingleton<NonBrowserReadTokenVerifier>();
        return services;
    }
}
