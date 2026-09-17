using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Kafdeck.Api;

public sealed class ApiTelemetry : IDisposable
{
    public const string InstrumentationName = "Kafdeck.Api";

    private readonly ActivitySource _activitySource = new(InstrumentationName);
    private readonly Meter _meter = new(InstrumentationName);
    private readonly Counter<long> _requestCount;
    private readonly Histogram<double> _requestDurationMs;

    public ApiTelemetry()
    {
        _requestCount = _meter.CreateCounter<long>(
            "kafdeck.api.requests",
            unit: "{request}",
            description: "Number of HTTP requests handled by Kafdeck API.");
        _requestDurationMs = _meter.CreateHistogram<double>(
            "kafdeck.api.request.duration",
            unit: "ms",
            description: "Kafdeck API request duration.");
    }

    public Activity? StartRequest(string routeName, string method)
    {
        var activity = _activitySource.StartActivity("kafdeck.api.request", ActivityKind.Internal);
        activity?.SetTag("kafdeck.route", NormalizeRouteName(routeName));
        activity?.SetTag("http.request.method", NormalizeMethod(method));
        return activity;
    }

    public void RecordRequest(string routeName, string method, int statusCode, double elapsedMilliseconds)
    {
        var tags = new TagList
        {
            { "kafdeck.route", NormalizeRouteName(routeName) },
            { "http.request.method", NormalizeMethod(method) },
            { "http.response.status_code", NormalizeStatusClass(statusCode) },
        };

        _requestCount.Add(1, tags);
        _requestDurationMs.Record(Math.Max(0, elapsedMilliseconds), tags);
    }

    public static string NormalizeRouteName(string? routeName)
    {
        if (string.IsNullOrWhiteSpace(routeName))
        {
            return "unmatched";
        }

        return V01ApiContract.ProductRoutes.Any(route => string.Equals(route.Name, routeName, StringComparison.Ordinal))
            || string.Equals(routeName, "openapi-v01", StringComparison.Ordinal)
            || string.Equals(routeName, "healthz", StringComparison.Ordinal)
            ? routeName
            : "other";
    }

    public static string NormalizeMethod(string? method) =>
        string.Equals(method, "GET", StringComparison.OrdinalIgnoreCase) ? "GET" : "OTHER";

    public static string NormalizeStatusClass(int statusCode) => statusCode switch
    {
        >= 200 and < 300 => "2xx",
        >= 300 and < 400 => "3xx",
        >= 400 and < 500 => "4xx",
        >= 500 and < 600 => "5xx",
        _ => "other",
    };

    public void Dispose()
    {
        _activitySource.Dispose();
        _meter.Dispose();
    }
}

public sealed class ApiTelemetryMiddleware
{
    private readonly RequestDelegate _next;

    public ApiTelemetryMiddleware(RequestDelegate next)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
    }

    public async Task InvokeAsync(HttpContext context, ApiTelemetry telemetry)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(telemetry);

        var started = Stopwatch.GetTimestamp();
        var routeName = context.GetEndpoint()?.Metadata.GetMetadata<Microsoft.AspNetCore.Routing.RouteNameMetadata>()?.RouteName
            ?? "unmatched";

        using var activity = telemetry.StartRequest(routeName, context.Request.Method);
        try
        {
            await _next(context).ConfigureAwait(false);
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            telemetry.RecordRequest(routeName, context.Request.Method, context.Response.StatusCode, elapsed);
        }
    }
}
