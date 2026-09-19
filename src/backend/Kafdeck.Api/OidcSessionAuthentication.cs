using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Claims;
using System.Security.Cryptography;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Http;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Kafdeck.Api;

public static class KafdeckOidcDefaults
{
    public const string CookieScheme = "Kafdeck.Session";
    public const string OidcScheme = "Kafdeck.Oidc";
    public const string IssuerClaim = "kafdeck:issuer";
    public const string SubjectClaim = "kafdeck:subject";
    public const string ExternalGroupClaim = "kafdeck:external-group";
    public const string SessionIdClaim = "kafdeck:session-id";
    public const string AuthenticatedAtClaim = "kafdeck:authenticated-at";
}

public static class KafdeckOidcServiceCollectionExtensions
{
    public static IServiceCollection AddKafdeckOidc(
        this IServiceCollection services,
        DeploymentOptions deployment,
        SecretResolver secretResolver)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(deployment);
        ArgumentNullException.ThrowIfNull(secretResolver);

        if (deployment.Mode != AccessMode.Oidc || deployment.Oidc is null)
        {
            throw new InvalidOperationException("OIDC services can only be registered for OIDC access mode.");
        }

        var oidc = deployment.Oidc;
        var sessionStore = new BoundedMemoryTicketStore(
            maxEntries: 4096,
            defaultLifetime: TimeSpan.FromHours(8));

        services.AddSingleton(sessionStore);

        services
            .AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = KafdeckOidcDefaults.CookieScheme;
                options.DefaultSignInScheme = KafdeckOidcDefaults.CookieScheme;
                options.DefaultScheme = KafdeckOidcDefaults.CookieScheme;
                options.DefaultChallengeScheme = KafdeckOidcDefaults.OidcScheme;
            })
            .AddCookie(KafdeckOidcDefaults.CookieScheme, options =>
            {
                var listenUri = new Uri(deployment.ListenUrl, UriKind.Absolute);
                var useSecureCookie = string.Equals(
                    listenUri.Scheme,
                    Uri.UriSchemeHttps,
                    StringComparison.OrdinalIgnoreCase);

                options.Cookie.Name = "Kafdeck.Session";
                options.Cookie.HttpOnly = true;
                options.Cookie.SameSite = SameSiteMode.Strict;
                options.Cookie.SecurePolicy = useSecureCookie
                    ? CookieSecurePolicy.Always
                    : CookieSecurePolicy.SameAsRequest;
                options.Cookie.IsEssential = true;
                options.ExpireTimeSpan = TimeSpan.FromHours(8);
                options.SlidingExpiration = false;
                options.SessionStore = sessionStore;
            })
            .AddOpenIdConnect(KafdeckOidcDefaults.OidcScheme, options =>
            {
                var issuerUri = new Uri(oidc.Issuer, UriKind.Absolute);

                options.SignInScheme = KafdeckOidcDefaults.CookieScheme;
                options.Authority = oidc.Issuer;
                options.ClientId = oidc.ClientId;
                options.ClientSecret = oidc.ClientSecret is null
                    ? null
                    : secretResolver.Resolve(oidc.ClientSecret).Reveal();
                options.ResponseType = OpenIdConnectResponseType.Code;
                options.UsePkce = true;
                options.SaveTokens = false;
                options.GetClaimsFromUserInfoEndpoint = false;
                options.MapInboundClaims = false;
                options.RequireHttpsMetadata = string.Equals(
                    issuerUri.Scheme,
                    Uri.UriSchemeHttps,
                    StringComparison.OrdinalIgnoreCase);
                options.BackchannelTimeout = TimeSpan.FromSeconds(10);
                options.RemoteAuthenticationTimeout = TimeSpan.FromMinutes(5);

                options.Scope.Clear();
                foreach (var scope in oidc.Scopes)
                {
                    options.Scope.Add(scope);
                }

                options.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidateIssuer = true,
                    ValidIssuer = oidc.Issuer,
                    ValidateAudience = true,
                    ValidAudience = oidc.ClientId,
                    ValidateIssuerSigningKey = true,
                    RequireSignedTokens = true,
                    RequireExpirationTime = true,
                    ValidateLifetime = true,
                    ClockSkew = TimeSpan.FromMinutes(2),
                    NameClaimType = "name",
                };

                options.CorrelationCookie.HttpOnly = true;
                options.CorrelationCookie.SameSite = SameSiteMode.None;
                options.CorrelationCookie.SecurePolicy = options.RequireHttpsMetadata
                    ? CookieSecurePolicy.Always
                    : CookieSecurePolicy.SameAsRequest;

                options.NonceCookie.HttpOnly = true;
                options.NonceCookie.SameSite = SameSiteMode.None;
                options.NonceCookie.SecurePolicy = options.RequireHttpsMetadata
                    ? CookieSecurePolicy.Always
                    : CookieSecurePolicy.SameAsRequest;

                options.Events = new OpenIdConnectEvents
                {
                    OnTokenValidated = context =>
                    {
                        try
                        {
                            var issuer = context.SecurityToken?.Issuer ?? oidc.Issuer;
                            context.Principal = OidcIdentityNormalizer.Normalize(
                                context.Principal,
                                issuer,
                                oidc.GroupClaim,
                                DateTimeOffset.UtcNow);
                        }
                        catch (Exception exception) when (
                            exception is ArgumentException or
                            InvalidOperationException)
                        {
                            context.Fail("OIDC identity claims are invalid.");
                        }

                        return Task.CompletedTask;
                    },
                };
            });

        return services;
    }
}

public static class OidcIdentityNormalizer
{
    public static ClaimsPrincipal Normalize(
        ClaimsPrincipal? externalPrincipal,
        string issuer,
        string? groupClaim,
        DateTimeOffset authenticatedAtUtc)
    {
        if (externalPrincipal is null)
        {
            throw new InvalidOperationException("OIDC principal is missing.");
        }

        var subject = externalPrincipal.FindFirst("sub")?.Value;
        if (string.IsNullOrWhiteSpace(subject))
        {
            throw new InvalidOperationException("OIDC subject claim is missing.");
        }

        var displayName =
            externalPrincipal.FindFirst("name")?.Value ??
            externalPrincipal.FindFirst("preferred_username")?.Value;
        var email = externalPrincipal.FindFirst("email")?.Value;

        var groups = string.IsNullOrWhiteSpace(groupClaim)
            ? Array.Empty<string>()
            : externalPrincipal.FindAll(groupClaim)
                .Select(claim => claim.Value)
                .ToArray();

        var identity = new OperatorIdentity(
            new OperatorIdentityKey(issuer, subject),
            displayName,
            email,
            groups);
        var sessionId = new SessionId(Guid.NewGuid());

        var claims = new List<Claim>
        {
            new(KafdeckOidcDefaults.IssuerClaim, identity.Key.Issuer),
            new(KafdeckOidcDefaults.SubjectClaim, identity.Key.Subject),
            new(KafdeckOidcDefaults.SessionIdClaim, sessionId.Value.ToString("N", CultureInfo.InvariantCulture)),
            new(
                KafdeckOidcDefaults.AuthenticatedAtClaim,
                authenticatedAtUtc.ToString("O", CultureInfo.InvariantCulture)),
        };

        if (identity.DisplayName is not null)
        {
            claims.Add(new Claim(ClaimTypes.Name, identity.DisplayName));
        }

        if (identity.Email is not null)
        {
            claims.Add(new Claim(ClaimTypes.Email, identity.Email));
        }

        claims.AddRange(identity.ExternalGroups.Select(group =>
            new Claim(KafdeckOidcDefaults.ExternalGroupClaim, group)));

        var normalizedIdentity = new ClaimsIdentity(
            claims,
            KafdeckOidcDefaults.CookieScheme,
            ClaimTypes.Name,
            ClaimTypes.Role);

        return new ClaimsPrincipal(normalizedIdentity);
    }
}

public static class OperatorSessionContextFactory
{
    public static bool TryCreate(ClaimsPrincipal? principal, out OperatorSessionContext? context)
    {
        context = null;

        if (principal?.Identity?.IsAuthenticated != true)
        {
            return false;
        }

        var issuer = principal.FindFirst(KafdeckOidcDefaults.IssuerClaim)?.Value;
        var subject = principal.FindFirst(KafdeckOidcDefaults.SubjectClaim)?.Value;
        var sessionIdValue = principal.FindFirst(KafdeckOidcDefaults.SessionIdClaim)?.Value;
        var authenticatedAtValue = principal.FindFirst(KafdeckOidcDefaults.AuthenticatedAtClaim)?.Value;

        if (string.IsNullOrWhiteSpace(issuer) ||
            string.IsNullOrWhiteSpace(subject) ||
            !Guid.TryParseExact(sessionIdValue, "N", out var sessionGuid) ||
            !DateTimeOffset.TryParseExact(
                authenticatedAtValue,
                "O",
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind,
                out var authenticatedAtUtc))
        {
            return false;
        }

        try
        {
            var identity = new OperatorIdentity(
                new OperatorIdentityKey(issuer, subject),
                principal.FindFirst(ClaimTypes.Name)?.Value,
                principal.FindFirst(ClaimTypes.Email)?.Value,
                principal.FindAll(KafdeckOidcDefaults.ExternalGroupClaim).Select(claim => claim.Value).ToArray());

            context = new OperatorSessionContext(
                identity,
                new SessionId(sessionGuid),
                authenticatedAtUtc);

            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }
}

public sealed class BoundedMemoryTicketStore : ITicketStore
{
    private sealed record Entry(AuthenticationTicket Ticket, DateTimeOffset ExpiresAtUtc);

    private readonly ConcurrentDictionary<string, Entry> _tickets = new(StringComparer.Ordinal);
    private readonly int _maxEntries;
    private readonly TimeSpan _defaultLifetime;
    private readonly object _capacityLock = new();

    public BoundedMemoryTicketStore(int maxEntries, TimeSpan defaultLifetime)
    {
        if (maxEntries <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxEntries));
        }

        if (defaultLifetime <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(defaultLifetime));
        }

        _maxEntries = maxEntries;
        _defaultLifetime = defaultLifetime;
    }

    public Task<string> StoreAsync(AuthenticationTicket ticket)
    {
        ArgumentNullException.ThrowIfNull(ticket);

        lock (_capacityLock)
        {
            PruneExpired(DateTimeOffset.UtcNow);
            if (_tickets.Count >= _maxEntries)
            {
                throw new InvalidOperationException("OIDC session store capacity has been reached.");
            }

            string key;
            do
            {
                key = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
            }
            while (_tickets.ContainsKey(key));

            _tickets[key] = CreateEntry(ticket);
            return Task.FromResult(key);
        }
    }

    public Task RenewAsync(string key, AuthenticationTicket ticket)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(ticket);

        if (_tickets.ContainsKey(key))
        {
            _tickets[key] = CreateEntry(ticket);
        }

        return Task.CompletedTask;
    }

    public Task<AuthenticationTicket?> RetrieveAsync(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        if (!_tickets.TryGetValue(key, out var entry))
        {
            return Task.FromResult<AuthenticationTicket?>(null);
        }

        if (entry.ExpiresAtUtc <= DateTimeOffset.UtcNow)
        {
            _tickets.TryRemove(key, out _);
            return Task.FromResult<AuthenticationTicket?>(null);
        }

        return Task.FromResult<AuthenticationTicket?>(entry.Ticket);
    }

    public Task RemoveAsync(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        _tickets.TryRemove(key, out _);
        return Task.CompletedTask;
    }

    private Entry CreateEntry(AuthenticationTicket ticket)
    {
        var expiresAt = ticket.Properties.ExpiresUtc ?? DateTimeOffset.UtcNow.Add(_defaultLifetime);
        return new Entry(ticket, expiresAt);
    }

    private void PruneExpired(DateTimeOffset nowUtc)
    {
        foreach (var pair in _tickets)
        {
            if (pair.Value.ExpiresAtUtc <= nowUtc)
            {
                _tickets.TryRemove(pair.Key, out _);
            }
        }
    }
}

public sealed class OidcApiAuthenticationBoundaryMiddleware
{
    private readonly RequestDelegate _next;

    public OidcApiAuthenticationBoundaryMiddleware(RequestDelegate next)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
    }

    public async Task InvokeAsync(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.User.Identity?.IsAuthenticated == true)
        {
            await _next(context);
            return;
        }

        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        context.Response.ContentType = "application/problem+json";
        await context.Response.WriteAsJsonAsync(
            new
            {
                type = "urn:kafdeck:problem:operator-authentication-required",
                title = "Operator authentication required",
                status = StatusCodes.Status401Unauthorized,
                code = "operator_authentication_required",
            },
            cancellationToken: context.RequestAborted);
    }
}

public static class OidcReturnUrlPolicy
{
    public static string Normalize(string? returnUrl)
    {
        if (string.IsNullOrWhiteSpace(returnUrl))
        {
            return "/";
        }

        var normalized = returnUrl.Trim();
        if (!normalized.StartsWith("/", StringComparison.Ordinal) ||
            normalized.StartsWith("//", StringComparison.Ordinal) ||
            normalized.Contains('\\') ||
            Uri.TryCreate(normalized, UriKind.Absolute, out _))
        {
            return "/";
        }

        return normalized;
    }
}

public static class KafdeckOidcEndpointExtensions
{
    public static WebApplication MapKafdeckOidcSessionEndpoints(this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapGet("/api/v1/auth/login", (string? returnUrl) =>
            Results.Challenge(
                new AuthenticationProperties
                {
                    RedirectUri = OidcReturnUrlPolicy.Normalize(returnUrl),
                },
                new[] { KafdeckOidcDefaults.OidcScheme }))
            .WithName("v02-auth-login");

        app.MapGet("/api/v1/auth/session", (HttpContext context) =>
        {
            if (!OperatorSessionContextFactory.TryCreate(context.User, out var session) || session is null)
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status401Unauthorized,
                    type: "urn:kafdeck:problem:operator-authentication-required",
                    title: "Operator authentication required");
            }

            return Results.Ok(new
            {
                authenticated = true,
                authenticationMode = "oidc",
                displayName = session.Identity.DisplayName,
                email = session.Identity.Email,
                authenticatedAt = session.AuthenticatedAtUtc,
            });
        }).WithName("v02-auth-session");

        app.MapPost("/api/v1/auth/logout", async (HttpContext context) =>
        {
            await context.SignOutAsync(KafdeckOidcDefaults.CookieScheme);
            await context.SignOutAsync(
                KafdeckOidcDefaults.OidcScheme,
                new AuthenticationProperties
                {
                    RedirectUri = "/",
                });
        }).WithName("v02-auth-logout");

        return app;
    }
}
