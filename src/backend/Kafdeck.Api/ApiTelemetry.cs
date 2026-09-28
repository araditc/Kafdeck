using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

public sealed class ApiTelemetry : IDisposable
{
    public const string InstrumentationName = "Kafdeck.Api";

    private readonly ActivitySource _activitySource = new(InstrumentationName);
    private readonly Meter _meter = new(InstrumentationName);
    private readonly Counter<long> _requestCount;
    private readonly Histogram<double> _requestDurationMs;
    private readonly Counter<long> _droppedMetricSeries;
    private readonly ObservableGauge<int> _activeMetricSeries;
    private readonly PrometheusMetricsRegistry _prometheusMetrics;
    private readonly int _maxTraceAttributes;

    public ApiTelemetry(
        PrometheusMetricsRegistry prometheusMetrics,
        ObservabilityOptions observabilityOptions)
    {
        _prometheusMetrics =
            prometheusMetrics ??
            throw new ArgumentNullException(nameof(prometheusMetrics));
        ArgumentNullException.ThrowIfNull(observabilityOptions);
        _maxTraceAttributes =
            observabilityOptions.MaxTraceAttributes;
        _requestCount = _meter.CreateCounter<long>(
            "kafdeck.api.requests",
            unit: "{request}",
            description: "Number of HTTP requests handled by Kafdeck API.");
        _requestDurationMs = _meter.CreateHistogram<double>(
            "kafdeck.api.request.duration",
            unit: "ms",
            description: "Kafdeck API request duration.");
        _droppedMetricSeries = _meter.CreateCounter<long>(
            "kafdeck.telemetry.dropped.series",
            unit: "{series}",
            description:
                "Number of Kafdeck metric series rejected by the cardinality limit.");
        _activeMetricSeries = _meter.CreateObservableGauge(
            "kafdeck.telemetry.active.series",
            () => _prometheusMetrics.ActiveSeriesCount,
            unit: "{series}",
            description:
                "Number of active bounded Kafdeck metric series.");
    }

    public Activity? StartRequest(string routeName, string method)
    {
        var activity = _activitySource.StartActivity(
            "kafdeck.api.request",
            ActivityKind.Internal);
        if (_maxTraceAttributes >= 1)
        {
            activity?.SetTag(
                "kafdeck.route",
                NormalizeRouteName(routeName));
        }

        if (_maxTraceAttributes >= 2)
        {
            activity?.SetTag(
                "http.request.method",
                NormalizeMethod(method));
        }

        return activity;
    }

    public void SetResponseStatus(
        Activity? activity,
        int statusCode)
    {
        if (_maxTraceAttributes >= 3)
        {
            activity?.SetTag(
                "http.response.status_code",
                NormalizeStatusClass(statusCode));
        }
    }

    public void RecordRequest(
        string routeName,
        string method,
        int statusCode,
        double elapsedMilliseconds)
    {
        var normalizedRoute = NormalizeRouteName(routeName);
        var normalizedMethod = NormalizeMethod(method);
        var normalizedStatus = NormalizeStatusClass(statusCode);

        var duration = Math.Max(0, elapsedMilliseconds);
        if (!_prometheusMetrics.RecordApiRequest(
                normalizedRoute,
                normalizedMethod,
                normalizedStatus,
                duration,
                out var admittedKey))
        {
            _droppedMetricSeries.Add(3);
            return;
        }

        var tags = new TagList
        {
            { "kafdeck.route", admittedKey.Route },
            { "http.request.method", admittedKey.Method },
            { "http.response.status_code", admittedKey.StatusClass },
        };

        _requestCount.Add(1, tags);
        _requestDurationMs.Record(duration, tags);
    }

    public static string NormalizeRouteName(string? routeName)
    {
        if (string.IsNullOrWhiteSpace(routeName))
        {
            return "unmatched";
        }

        var value = routeName.Trim();
        return value.Length <= 128 &&
               value.All(character =>
                   char.IsAsciiLetterOrDigit(character) ||
                   character is '.' or '_' or '-')
            ? value
            : "other";
    }

    public static string NormalizeMethod(string? method)
    {
        var value = method?.Trim().ToUpperInvariant();
        return value switch
        {
            "GET" => "GET",
            "POST" => "POST",
            "PUT" => "PUT",
            "PATCH" => "PATCH",
            "DELETE" => "DELETE",
            "HEAD" => "HEAD",
            "OPTIONS" => "OPTIONS",
            _ => "OTHER",
        };
    }

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

        using var activity =
            telemetry.StartRequest(routeName, context.Request.Method);
        var failed = false;
        try
        {
            await _next(context).ConfigureAwait(false);
        }
        catch
        {
            failed = true;
            activity?.SetStatus(
                ActivityStatusCode.Error,
                "unhandled_request_exception");
            throw;
        }
        finally
        {
            var effectiveStatusCode = failed
                ? StatusCodes.Status500InternalServerError
                : context.Response.StatusCode;
            telemetry.SetResponseStatus(
                activity,
                effectiveStatusCode);

            var elapsed =
                Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            telemetry.RecordRequest(
                routeName,
                context.Request.Method,
                effectiveStatusCode,
                elapsed);
        }
    }
}
