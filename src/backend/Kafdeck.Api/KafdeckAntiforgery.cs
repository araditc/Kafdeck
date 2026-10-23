using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Options;

namespace Kafdeck.Api;

public static class KafdeckAntiforgeryExtensions
{
    public const string HeaderName = "X-Kafdeck-CSRF";
    public const string CookieName = "Kafdeck.Antiforgery";

    private sealed class RequiredAntiforgeryMetadata : IAntiforgeryMetadata
    {
        public static RequiredAntiforgeryMetadata Instance { get; } = new();

        public bool RequiresValidation => true;
    }

    public static IServiceCollection AddKafdeckAntiforgery(
        this IServiceCollection services,
        string listenUrl)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(listenUrl);

        var listenUri = new Uri(listenUrl, UriKind.Absolute);
        var requireSecureCookie = string.Equals(
            listenUri.Scheme,
            Uri.UriSchemeHttps,
            StringComparison.OrdinalIgnoreCase);

        services.AddAntiforgery(options =>
        {
            options.HeaderName = HeaderName;
            options.Cookie.Name = CookieName;
            options.Cookie.HttpOnly = true;
            options.Cookie.IsEssential = true;
            options.Cookie.SameSite = SameSiteMode.Strict;
            options.Cookie.SecurePolicy = requireSecureCookie
                ? CookieSecurePolicy.Always
                : CookieSecurePolicy.SameAsRequest;
        });

        return services;
    }

    public static WebApplication MapKafdeckAntiforgeryEndpoint(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapGet("/api/v1/auth/csrf", (
                HttpContext context,
                IAntiforgery antiforgery,
                IOptions<AntiforgeryOptions> options) =>
            {
                var tokens = antiforgery.GetAndStoreTokens(context);
                return Results.Ok(new
                {
                    requestToken = tokens.RequestToken,
                    headerName = tokens.HeaderName ?? HeaderName,
                    formFieldName = options.Value.FormFieldName,
                });
            })
            .WithName("v05-auth-csrf");

        return app;
    }

    public static RouteHandlerBuilder RequireKafdeckAntiforgery(
        this RouteHandlerBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Minimal APIs expose DisableAntiforgery but no public RequireAntiforgery
        // convention. Attach the framework-recognized metadata explicitly so
        // UseAntiforgery participates in the endpoint contract, while retaining
        // the Kafdeck filter below for stable Problem Details responses.
        builder.WithMetadata(RequiredAntiforgeryMetadata.Instance);

        return builder.AddEndpointFilter(async (invocation, next) =>
        {
            // Only these compiled, bounded read-only POST contracts may use
            // the separate verified bearer identity. Browser cookies still require CSRF.
            var http = invocation.HttpContext;
            var read = Kafdeck.Contracts.KafdeckReadContracts.Find(http.GetEndpoint()?.Metadata.GetMetadata<IEndpointNameMetadata>()?.EndpointName);
            if (http.User.Identity is NonBrowserReadIdentity && read?.Method == "post" && BearerReadBoundary.Matches(http, read))
                return await next(invocation).ConfigureAwait(false);
            var antiforgery = invocation.HttpContext.RequestServices
                .GetRequiredService<IAntiforgery>();

            try
            {
                // Framework middleware has already rejected this request. A
                // second validation reads its guarded FormFeature and throws
                // InvalidOperationException instead of the stable 403 below.
                if (invocation.HttpContext.Features.Get<IAntiforgeryValidationFeature>() is { IsValid: false })
                    throw new AntiforgeryValidationException("Request antiforgery validation failed.");
                await antiforgery
                    .ValidateRequestAsync(invocation.HttpContext)
                    .ConfigureAwait(false);
            }
            catch (AntiforgeryValidationException)
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status403Forbidden,
                    type: "urn:kafdeck:problem:antiforgery-validation-failed",
                    title: "Antiforgery validation failed",
                    detail: "A valid antiforgery token is required for this state-changing operator request.");
            }

            return await next(invocation).ConfigureAwait(false);
        });
    }
}
