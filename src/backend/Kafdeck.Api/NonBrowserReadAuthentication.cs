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
    public List<NonBrowserReadRegistration> HumanRegistrations { get; set; } = [];
    public bool GovernedMutationsEnabled { get; set; }
    public bool GovernedApprovalsEnabled { get; set; }
}

public enum NonBrowserPrincipalKind { Service = 1, Human = 2 }

public sealed class NonBrowserReadRegistration
{
    public string Id { get; set; } = "";
    public string OwnerId { get; set; } = "";
    public string Subject { get; set; } = "";
    public string ClientId { get; set; } = "";
    public bool Enabled { get; set; }
    public DateTimeOffset ExpiresAtUtc { get; set; }
    public List<AuthorizationAction> ReadActions { get; set; } = [];
    public List<AuthorizationAction> MutationActions { get; set; } = [];
    public List<AuthorizationAction> ApprovalActions { get; set; } = [];
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
        var registrations = options.Registrations.Concat(options.HumanRegistrations).ToArray();
        if (registrations.Length is < 1 or > 128)
            throw new InvalidOperationException("Non-browser registrations must be finite and nonempty.");
        var proposals = registrations.Select(r => new NonBrowserServicePrincipalProposal(
            r.Id, r.OwnerId, options.Issuer, options.ApiAudience, r.Subject, r.ClientId, r.ReadActions)).ToArray();
        _ = new NonBrowserPrincipalProposalSet(proposals);
        if (registrations.Any(r => r.MutationActions.Count > 2 || r.MutationActions.Distinct().Count() != r.MutationActions.Count ||
                r.MutationActions.Any(action => action is not (AuthorizationAction.TopicCreate or AuthorizationAction.TopicDelete))) ||
            options.Registrations.Any(r => r.ApprovalActions.Count != 0) ||
            options.HumanRegistrations.Any(r => r.ApprovalActions.Count > 1 || r.ApprovalActions.Any(a => a != AuthorizationAction.TopicDelete)))
            throw new InvalidOperationException("Only explicit TopicCreate/TopicDelete effects and human TopicDelete approvals are admitted.");
        return options;
    }

    public NonBrowserServicePrincipalProposal? Find(string subject, string client, string? registrationId = null, NonBrowserPrincipalKind? kind = null)
    {
        try
        {
            // Re-read deployment configuration for disable/revocation on every
            // request and authorization decision. Trust changes need restart.
            var current = Read();
            if (!Trust.Enabled || !current.Enabled || current.Issuer != Trust.Issuer || current.ApiAudience != Trust.ApiAudience ||
                current.TrustedCaPemFile != Trust.TrustedCaPemFile)
                return null;
            var candidates = kind switch { NonBrowserPrincipalKind.Human => current.HumanRegistrations,
                NonBrowserPrincipalKind.Service => current.Registrations, _ => current.Registrations.Concat(current.HumanRegistrations) };
            var r = candidates.SingleOrDefault(r => r.Subject == subject && r.ClientId == client &&
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

    internal NonBrowserPrincipalKind FindKind(string subject, string client) =>
        Read().HumanRegistrations.Any(r => r.Subject == subject && r.ClientId == client)
            ? NonBrowserPrincipalKind.Human : NonBrowserPrincipalKind.Service;

    public bool IsAllowed(NonBrowserReadIdentity identity, AuthorizationRequest request,
        AuthorizationPolicyEvaluator evaluator)
    {
        var registration = Find(identity.Evidence.Subject, identity.Evidence.ClientId, identity.RegistrationId, identity.Kind);
        return registration is not null && identity.Evidence.ExpiresAtUtc > DateTimeOffset.UtcNow &&
            (registration.RequestedReadActions.Contains(request.Action) || IsGovernedMutationAllowed(identity, request.Action)) &&
            evaluator.EvaluateDirectSubject(new(identity.Evidence.Issuer, identity.Evidence.Subject), request).IsAllowed;
    }

    public bool IsGovernedMutationAllowed(NonBrowserReadIdentity identity, AuthorizationAction action = AuthorizationAction.TopicCreate)
    {
        try
        {
            var current = Read();
            if (!Trust.GovernedMutationsEnabled || !current.GovernedMutationsEnabled ||
                Find(identity.Evidence.Subject, identity.Evidence.ClientId, identity.RegistrationId, identity.Kind) is null ||
                identity.Evidence.ExpiresAtUtc <= DateTimeOffset.UtcNow) return false;
            var registrations = identity.Kind == NonBrowserPrincipalKind.Human ? current.HumanRegistrations : current.Registrations;
            return action is AuthorizationAction.TopicCreate or AuthorizationAction.TopicDelete &&
                registrations.Single(r => r.Id == identity.RegistrationId).MutationActions.Contains(action);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException) { return false; }
    }

    public bool GovernedApprovalsActive
    {
        get
        {
            try { var current = Read(); return Trust.Enabled && current.Enabled && Trust.GovernedApprovalsEnabled &&
                current.GovernedApprovalsEnabled && Trust.Issuer == current.Issuer && Trust.ApiAudience == current.ApiAudience &&
                Trust.TrustedCaPemFile == current.TrustedCaPemFile; }
            catch (Exception ex) when (ex is ArgumentException or InvalidOperationException) { return false; }
        }
    }

    public bool IsGovernedApprovalAllowed(NonBrowserReadIdentity identity, AuthorizationAction action) =>
        identity.Kind == NonBrowserPrincipalKind.Human && identity.Evidence.ExpiresAtUtc > DateTimeOffset.UtcNow &&
        IsCurrentApprovalRegistrationAllowed(new(new(identity.Evidence.Issuer, identity.Evidence.Subject), identity.Evidence.ClientId, identity.RegistrationId), action);

    public bool IsCurrentApprovalRegistrationAllowed(NonBrowserHumanPrincipalId principal, AuthorizationAction action)
    {
        try
        {
            return GovernedApprovalsActive && action == AuthorizationAction.TopicDelete && principal.HumanKey.Issuer == Trust.Issuer &&
                Find(principal.HumanKey.Subject, principal.ClientId, principal.RegistrationId, NonBrowserPrincipalKind.Human) is not null &&
                Read().HumanRegistrations.Single(r => r.Id == principal.RegistrationId).ApprovalActions.Contains(action);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException) { return false; }
    }

    public bool IsCollectionAllowed(NonBrowserReadIdentity identity, AuthorizationAction action, string? clusterId,
        AuthorizationPolicyEvaluator evaluator)
    {
        var registration = Find(identity.Evidence.Subject, identity.Evidence.ClientId, identity.RegistrationId, identity.Kind);
        return identity.Kind == NonBrowserPrincipalKind.Human && registration is not null &&
            identity.Evidence.ExpiresAtUtc > DateTimeOffset.UtcNow && registration.RequestedReadActions.Contains(action) &&
            evaluator.HasApplicableDirectSubjectPermission(new(identity.Evidence.Issuer, identity.Evidence.Subject), action, clusterId);
    }
}

public sealed class NonBrowserReadIdentity : ClaimsIdentity
{
    internal NonBrowserReadIdentity(string registrationId, NonBrowserVerifiedJwtEvidence evidence, NonBrowserPrincipalKind kind = NonBrowserPrincipalKind.Service)
        : base("Kafdeck.NonBrowserRead")
    {
        RegistrationId = registrationId;
        Evidence = evidence;
        Kind = kind;
    }

    public string RegistrationId { get; }
    public NonBrowserVerifiedJwtEvidence Evidence { get; }
    public NonBrowserPrincipalKind Kind { get; }
    public string AuditId => (Kind == NonBrowserPrincipalKind.Human ? "oidc-cli-human:" : "oidc-service:") + Uri.EscapeDataString(Evidence.Issuer) + "|" +
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
            return new(registration.RegistrationId, evidence, _registry.FindKind(subject, client));
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
        "v01-system-info", "v01-system-health", "v08-bearer-openapi", "v01-clusters-detail", "v01-clusters-health",
        "v01-clusters-capabilities", "v01-brokers-list", "v01-brokers-detail",
    };
    private static readonly HashSet<string> HumanReadRoutes = new(StringComparer.Ordinal)
    {
        "v01-clusters-list", "v01-topics-list", "v01-topics-detail", "v01-topics-partitions",
        "v04-consumer-groups-list", "v04-consumer-groups-detail", "v04-consumer-groups-lag", "v04-consumer-groups-diagnostics",
        "v04-schema-subjects-list", "v04-schema-versions-list", "v04-schema-version-detail", "v04-schema-compatibility", "v04-schema-diff",
    };
    private static readonly HashSet<string> MutationRoutes = new(StringComparer.Ordinal)
    { "v08-bearer-topic-create-preview", "v08-bearer-topic-delete-preview", "v08-bearer-mutation-status", "v08-bearer-mutation-confirm", "v08-bearer-mutation-execute" };
    private static readonly HashSet<string> ApprovalRoutes = new(StringComparer.Ordinal)
    { "v08-bearer-mutation-approval", "v08-bearer-mutation-approve" };

    public async Task InvokeAsync(HttpContext context, NonBrowserReadTokenVerifier verifier, ISecurityAuditSink audit, NonBrowserReadRegistry? registry = null)
    {
        var route = context.GetEndpoint()?.Metadata.GetMetadata<IEndpointNameMetadata>()?.EndpointName;
        var approvalRoute = route is not null && ApprovalRoutes.Contains(route);
        var governedRoute = route is not null && (MutationRoutes.Contains(route) || approvalRoute);
        // Minimal API binding runs before endpoint filters, including for callers
        // without an Authorization header. Bound these dedicated routes first.
        if (governedRoute && context.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpMaxRequestBodySizeFeature>() is { IsReadOnly: false } bodyLimit)
            bodyLimit.MaxRequestBodySize = 64 * 1024;
        if (!context.Request.Headers.TryGetValue("Authorization", out var authorization))
        {
            await next(context);
            return;
        }
        var header = authorization.Count == 1 ? authorization[0] : null;
        var collision = context.Request.Headers.ContainsKey("Cookie") ||
            context.Request.Headers.ContainsKey("X-Kafdeck-Access-Token") || context.User.Identity?.IsAuthenticated == true;
        var secureTransport = context.Request.IsHttps ||
            context.Connection.RemoteIpAddress is { } remote && IPAddress.IsLoopback(remote);
        var admitted = secureTransport && route is not null &&
            (governedRoute || HttpMethods.IsGet(context.Request.Method) && (Routes.Contains(route) || HumanReadRoutes.Contains(route)));
        // HTTP authentication scheme names are case-insensitive; keep the
        // credential substring untouched for signature/claim validation.
        var identity = !collision && admitted && header is not null && header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) &&
            header.Length <= 16 * 1024 + 7
            ? await verifier.VerifyAsync(header[7..], context.RequestAborted)
            : null;
        if (identity is not null && governedRoute)
        {
            var allowed = approvalRoute ? registry?.IsGovernedApprovalAllowed(identity, AuthorizationAction.TopicDelete) == true :
                route == "v08-bearer-topic-create-preview" ? registry?.IsGovernedMutationAllowed(identity, AuthorizationAction.TopicCreate) == true :
                route == "v08-bearer-topic-delete-preview" ? registry?.IsGovernedMutationAllowed(identity, AuthorizationAction.TopicDelete) == true :
                registry?.IsGovernedMutationAllowed(identity, AuthorizationAction.TopicCreate) == true || registry?.IsGovernedMutationAllowed(identity, AuthorizationAction.TopicDelete) == true;
            if (!allowed) identity = null;
        }
        if (identity is not null && route is not null && !governedRoute && !Routes.Contains(route) && identity.Kind != NonBrowserPrincipalKind.Human)
            identity = null;
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
            identity.AuditId, context.TraceIdentifier, null, null, SecurityAuditOutcome.Succeeded,
            approvalRoute ? "registered_bearer_approval" : governedRoute ? "registered_bearer_mutation" : "registered_bearer_read"), context.RequestAborted);
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
