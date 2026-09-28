using System.Globalization;
using System.Net;
using System.Text;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Security;

namespace Kafdeck.Api;

public sealed class PrometheusMetricsRegistry
{
    private sealed class ApiSeries
    {
        private readonly object _gate = new();
        private long _count;
        private double _durationMilliseconds;

        public void Record(double durationMilliseconds)
        {
            lock (_gate)
            {
                _count++;
                _durationMilliseconds += Math.Max(0, durationMilliseconds);
            }
        }

        public (long Count, double DurationMilliseconds) Snapshot()
        {
            lock (_gate)
            {
                return (_count, _durationMilliseconds);
            }
        }
    }

    private readonly object _seriesGate = new();
    private readonly Dictionary<ApiMetricSeriesKey, ApiSeries> _apiSeries = new();
    private readonly int _maxActiveSeries;
    private long _droppedSeries;

    public PrometheusMetricsRegistry(int maxActiveSeries)
    {
        if (maxActiveSeries is < 1 or > 50_000)
        {
            throw new ArgumentOutOfRangeException(nameof(maxActiveSeries));
        }

        _maxActiveSeries = maxActiveSeries;
    }

    public int ActiveSeriesCount
    {
        get
        {
            lock (_seriesGate)
            {
                return _apiSeries.Count;
            }
        }
    }

    public long DroppedSeriesCount => Interlocked.Read(ref _droppedSeries);

    public bool RecordApiRequest(
        string route,
        string method,
        string statusClass,
        double durationMilliseconds)
    {
        var key = new ApiMetricSeriesKey(route, method, statusClass);
        ApiSeries series;

        lock (_seriesGate)
        {
            if (!_apiSeries.TryGetValue(key, out series!))
            {
                if (_apiSeries.Count >= _maxActiveSeries)
                {
                    Interlocked.Increment(ref _droppedSeries);
                    return false;
                }

                series = new ApiSeries();
                _apiSeries.Add(key, series);
            }
        }

        series.Record(durationMilliseconds);
        return true;
    }

    public string Render()
    {
        KeyValuePair<ApiMetricSeriesKey, ApiSeries>[] series;
        lock (_seriesGate)
        {
            series = _apiSeries
                .OrderBy(item => item.Key.Route, StringComparer.Ordinal)
                .ThenBy(item => item.Key.Method, StringComparer.Ordinal)
                .ThenBy(item => item.Key.StatusClass, StringComparer.Ordinal)
                .ToArray();
        }

        var builder = new StringBuilder();
        builder.AppendLine("# HELP kafdeck_api_requests_total Number of HTTP requests handled by Kafdeck API.");
        builder.AppendLine("# TYPE kafdeck_api_requests_total counter");
        foreach (var item in series)
        {
            var snapshot = item.Value.Snapshot();
            AppendSeries(
                builder,
                "kafdeck_api_requests_total",
                item.Key,
                snapshot.Count.ToString(CultureInfo.InvariantCulture));
        }

        builder.AppendLine("# HELP kafdeck_api_request_duration_milliseconds Request duration in milliseconds.");
        builder.AppendLine("# TYPE kafdeck_api_request_duration_milliseconds summary");
        foreach (var item in series)
        {
            var snapshot = item.Value.Snapshot();
            AppendSeries(
                builder,
                "kafdeck_api_request_duration_milliseconds_sum",
                item.Key,
                snapshot.DurationMilliseconds.ToString("R", CultureInfo.InvariantCulture));
            AppendSeries(
                builder,
                "kafdeck_api_request_duration_milliseconds_count",
                item.Key,
                snapshot.Count.ToString(CultureInfo.InvariantCulture));
        }

        builder.AppendLine("# HELP kafdeck_telemetry_active_series Number of active bounded Kafdeck metric series.");
        builder.AppendLine("# TYPE kafdeck_telemetry_active_series gauge");
        builder.Append("kafdeck_telemetry_active_series ")
            .Append(ActiveSeriesCount.ToString(CultureInfo.InvariantCulture))
            .AppendLine();

        builder.AppendLine("# HELP kafdeck_telemetry_dropped_series_total Number of metric series rejected by the cardinality hard limit.");
        builder.AppendLine("# TYPE kafdeck_telemetry_dropped_series_total counter");
        builder.Append("kafdeck_telemetry_dropped_series_total ")
            .Append(DroppedSeriesCount.ToString(CultureInfo.InvariantCulture))
            .AppendLine();

        return builder.ToString();
    }

    private static void AppendSeries(
        StringBuilder builder,
        string metric,
        ApiMetricSeriesKey key,
        string value)
    {
        builder.Append(metric)
            .Append("{route=\"")
            .Append(EscapeLabel(key.Route))
            .Append("\",method=\"")
            .Append(EscapeLabel(key.Method))
            .Append("\",status=\"")
            .Append(EscapeLabel(key.StatusClass))
            .Append("\"} ")
            .Append(value)
            .AppendLine();
    }

    private static string EscapeLabel(string value) =>
        value
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("\"", "\\\"", StringComparison.Ordinal)
            .Replace("\n", "\\n", StringComparison.Ordinal);
}

public readonly record struct ApiMetricSeriesKey(
    string Route,
    string Method,
    string StatusClass);

public static class PrometheusScrapeAccessPolicy
{
    public const string HeaderName = "X-Kafdeck-Metrics-Token";

    public static bool IsAllowed(
        IPAddress? remoteAddress,
        string? expectedToken,
        string? providedToken)
    {
        if (!string.IsNullOrEmpty(expectedToken))
        {
            return DeploymentAccessTokenValidator.Matches(
                expectedToken,
                providedToken);
        }

        return remoteAddress is not null &&
               IPAddress.IsLoopback(remoteAddress);
    }

    public static string? ReadProvidedToken(HttpRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var header = request.Headers[HeaderName].ToString();
        if (!string.IsNullOrWhiteSpace(header))
        {
            return header;
        }

        var authorization = request.Headers.Authorization.ToString();
        const string bearerPrefix = "Bearer ";
        return authorization.StartsWith(
                bearerPrefix,
                StringComparison.OrdinalIgnoreCase)
            ? authorization[bearerPrefix.Length..].Trim()
            : null;
    }
}

public static class KafdeckPrometheusEndpoints
{
    public static IEndpointRouteBuilder MapKafdeckPrometheusMetrics(
        this IEndpointRouteBuilder endpoints,
        PrometheusMetricsOptions options,
        PrometheusMetricsRegistry registry,
        string? scrapeToken)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(registry);

        if (!options.Enabled)
        {
            return endpoints;
        }

        endpoints.MapGet(
                options.Path,
                (HttpContext context) =>
                {
                    var providedToken =
                        PrometheusScrapeAccessPolicy.ReadProvidedToken(
                            context.Request);
                    if (!PrometheusScrapeAccessPolicy.IsAllowed(
                            context.Connection.RemoteIpAddress,
                            scrapeToken,
                            providedToken))
                    {
                        return Results.Problem(
                            statusCode:
                                string.IsNullOrEmpty(scrapeToken)
                                    ? StatusCodes.Status403Forbidden
                                    : StatusCodes.Status401Unauthorized,
                            title: "Prometheus scrape access denied");
                    }

                    context.Response.Headers.CacheControl = "no-store";
                    return Results.Text(
                        registry.Render(),
                        "text/plain; version=0.0.4; charset=utf-8");
                })
            .WithName("prometheus-metrics");

        return endpoints;
    }
}
