using Microsoft.AspNetCore.Antiforgery;

namespace Kafdeck.Api;

public static class KafdeckAntiforgeryExtensions
{
    public const string HeaderName = "X-Kafdeck-CSRF";
    public const string CookieName = "Kafdeck.Antiforgery";

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
                IAntiforgery antiforgery) =>
            {
                var tokens = antiforgery.GetAndStoreTokens(context);
                return Results.Ok(new
                {
                    requestToken = tokens.RequestToken,
                    headerName = tokens.HeaderName ?? HeaderName,
                });
            })
            .WithName("v05-auth-csrf");

        return app;
    }

    public static RouteHandlerBuilder RequireKafdeckAntiforgery(
        this RouteHandlerBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Attach the framework-recognized antiforgery metadata as well as the
        // Kafdeck filter below. The framework middleware performs the standard
        // token validation pass, while the filter preserves our stable
        // Problem Details response for invalid state-changing requests.
        builder.RequireAntiforgery();

        return builder.AddEndpointFilter(async (invocation, next) =>
        {
            var antiforgery = invocation.HttpContext.RequestServices
                .GetRequiredService<IAntiforgery>();

            try
            {
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
