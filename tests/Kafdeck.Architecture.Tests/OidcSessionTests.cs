using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class OidcSessionTests
{
    [Fact]
    public void Normalized_principal_contains_only_Kafdeck_identity_claims_and_no_protocol_tokens()
    {
        var external = new ClaimsPrincipal(new ClaimsIdentity(
            new[]
            {
                new Claim("sub", "operator-123"),
                new Claim("name", "Alice Operator"),
                new Claim("email", "alice@example.test"),
                new Claim("groups", "kafka-readers"),
                new Claim("groups", "platform"),
                new Claim("access_token", "must-not-survive"),
                new Claim("refresh_token", "must-not-survive"),
                new Claim("id_token", "must-not-survive"),
            },
            "external"));

        var normalized = OidcIdentityNormalizer.Normalize(
            external,
            "https://idp.example",
            "groups",
            new DateTimeOffset(2026, 9, 20, 0, 0, 0, TimeSpan.Zero));

        Assert.True(normalized.Identity?.IsAuthenticated);
        Assert.Equal("https://idp.example", normalized.FindFirst(KafdeckOidcDefaults.IssuerClaim)?.Value);
        Assert.Equal("operator-123", normalized.FindFirst(KafdeckOidcDefaults.SubjectClaim)?.Value);
        Assert.Equal(2, normalized.FindAll(KafdeckOidcDefaults.ExternalGroupClaim).Count());
        Assert.Null(normalized.FindFirst("access_token"));
        Assert.Null(normalized.FindFirst("refresh_token"));
        Assert.Null(normalized.FindFirst("id_token"));
    }

    [Fact]
    public void Missing_subject_fails_identity_normalization()
    {
        var external = new ClaimsPrincipal(new ClaimsIdentity(
            new[] { new Claim("name", "Alice") },
            "external"));

        Assert.Throws<InvalidOperationException>(() =>
            OidcIdentityNormalizer.Normalize(
                external,
                "https://idp.example",
                "groups",
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Normalized_principal_round_trips_to_operator_session_context()
    {
        var authenticatedAt = new DateTimeOffset(2026, 9, 20, 1, 2, 3, TimeSpan.Zero);
        var external = new ClaimsPrincipal(new ClaimsIdentity(
            new[]
            {
                new Claim("sub", "operator-123"),
                new Claim("name", "Alice"),
                new Claim("groups", "kafka-readers"),
            },
            "external"));

        var normalized = OidcIdentityNormalizer.Normalize(
            external,
            "https://idp.example",
            "groups",
            authenticatedAt);

        Assert.True(OperatorSessionContextFactory.TryCreate(normalized, out var session));
        Assert.NotNull(session);
        Assert.Equal("https://idp.example", session!.Identity.Key.Issuer);
        Assert.Equal("operator-123", session.Identity.Key.Subject);
        Assert.Equal(authenticatedAt, session.AuthenticatedAtUtc);
        Assert.Equal(new[] { "kafka-readers" }, session.Identity.ExternalGroups);
    }

    [Theory]
    [InlineData(null, "/")]
    [InlineData("", "/")]
    [InlineData("https://evil.example/", "/")]
    [InlineData("//evil.example/", "/")]
    [InlineData("/\\evil", "/")]
    [InlineData("/clusters/prod", "/clusters/prod")]
    public void Return_url_policy_allows_only_local_application_paths(string? input, string expected)
    {
        Assert.Equal(expected, OidcReturnUrlPolicy.Normalize(input));
    }

    [Fact]
    public async Task Ticket_store_keeps_identity_server_side_and_supports_expiry_removal()
    {
        var store = new BoundedMemoryTicketStore(8, TimeSpan.FromHours(1));
        var ticket = new AuthenticationTicket(
            new ClaimsPrincipal(new ClaimsIdentity(
                new[] { new Claim(ClaimTypes.Name, "Alice") },
                KafdeckOidcDefaults.CookieScheme)),
            new AuthenticationProperties
            {
                ExpiresUtc = DateTimeOffset.UtcNow.AddMinutes(30),
            },
            KafdeckOidcDefaults.CookieScheme);

        var key = await store.StoreAsync(ticket);
        Assert.NotEmpty(key);
        Assert.NotNull(await store.RetrieveAsync(key));

        await store.RemoveAsync(key);
        Assert.Null(await store.RetrieveAsync(key));
    }

    [Fact]
    public async Task Unauthenticated_API_boundary_returns_safe_401_problem()
    {
        var invoked = false;
        var middleware = new OidcApiAuthenticationBoundaryMiddleware(_ =>
        {
            invoked = true;
            return Task.CompletedTask;
        });

        var context = new DefaultHttpContext();
        context.Response.Body = new MemoryStream();

        await middleware.InvokeAsync(context);

        Assert.False(invoked);
        Assert.Equal(StatusCodes.Status401Unauthorized, context.Response.StatusCode);

        context.Response.Body.Position = 0;
        using var reader = new StreamReader(context.Response.Body);
        var body = await reader.ReadToEndAsync();
        Assert.Contains("operator_authentication_required", body, StringComparison.Ordinal);
        Assert.DoesNotContain("token", body, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Oidc_registration_enforces_PKCE_no_token_persistence_and_server_side_ticket_store()
    {
        var deployment = new DeploymentOptions(
            "https://0.0.0.0:8443",
            null,
            AccessMode.Oidc,
            new OidcProfile(
                "https://idp.example",
                "kafdeck",
                null,
                "groups",
                new[] { "openid", "profile" }));

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKafdeckOidc(deployment, new SecretResolver());

        using var provider = services.BuildServiceProvider();
        var oidc = provider
            .GetRequiredService<IOptionsMonitor<OpenIdConnectOptions>>()
            .Get(KafdeckOidcDefaults.OidcScheme);
        var cookie = provider
            .GetRequiredService<IOptionsMonitor<CookieAuthenticationOptions>>()
            .Get(KafdeckOidcDefaults.CookieScheme);

        Assert.True(oidc.UsePkce);
        Assert.False(oidc.SaveTokens);
        Assert.False(oidc.GetClaimsFromUserInfoEndpoint);
        Assert.Equal("code", oidc.ResponseType);
        Assert.True(oidc.TokenValidationParameters.ValidateIssuer);
        Assert.True(oidc.TokenValidationParameters.ValidateAudience);
        Assert.NotNull(cookie.SessionStore);
        Assert.True(cookie.Cookie.HttpOnly);
        Assert.Equal(SameSiteMode.Strict, cookie.Cookie.SameSite);
        Assert.False(cookie.SlidingExpiration);
    }
}
