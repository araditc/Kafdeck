using Kafdeck.Api;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05AntiforgeryTests
{
    [Fact]
    public void Https_antiforgery_registration_uses_explicit_header_and_strict_secure_cookie()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKafdeckAntiforgery("https://127.0.0.1:8443");

        using var provider = services.BuildServiceProvider();
        var options = provider
            .GetRequiredService<IOptions<AntiforgeryOptions>>()
            .Value;

        Assert.Equal(KafdeckAntiforgeryExtensions.HeaderName, options.HeaderName);
        Assert.Equal(KafdeckAntiforgeryExtensions.CookieName, options.Cookie.Name);
        Assert.True(options.Cookie.HttpOnly);
        Assert.True(options.Cookie.IsEssential);
        Assert.Equal(SameSiteMode.Strict, options.Cookie.SameSite);
        Assert.Equal(CookieSecurePolicy.Always, options.Cookie.SecurePolicy);
    }

    [Fact]
    public void Http_antiforgery_registration_does_not_claim_an_https_only_cookie()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKafdeckAntiforgery("http://127.0.0.1:8080");

        using var provider = services.BuildServiceProvider();
        var options = provider
            .GetRequiredService<IOptions<AntiforgeryOptions>>()
            .Value;

        Assert.Equal(CookieSecurePolicy.SameAsRequest, options.Cookie.SecurePolicy);
    }

    [Fact]
    public void State_changing_endpoint_carries_framework_antiforgery_metadata()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddKafdeckAntiforgery("https://127.0.0.1:8443");

        using var app = builder.Build();
        app.MapPost("/mutation", () => Results.Ok())
            .RequireKafdeckAntiforgery();

        var endpoint = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .Single(item => string.Equals(
                item.RoutePattern.RawText,
                "/mutation",
                StringComparison.Ordinal));
        var metadata = endpoint.Metadata.GetMetadata<IAntiforgeryMetadata>();

        Assert.NotNull(metadata);
        Assert.True(metadata.RequiresValidation);
    }
}
