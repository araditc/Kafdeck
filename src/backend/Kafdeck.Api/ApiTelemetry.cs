using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

public sealed record ApiTelemetrySeriesSnapshot(
    string Route,
    string Method,
    string StatusClass,
    long RequestCount,
    double DurationSumMilliseconds);

public sealed record ApiTelemetrySnapshot(
    int ActiveSeries,
    int MaxActiveSeries,
    long DroppedSeries,
    IReadOnlyList<ApiTelemetrySeriesSnapshot> Series);

public sealed class ApiTelemetry : IDisposable
{
    public const string InstrumentationName = "Kafdeck.Api";

    private readonly record struct SeriesKey(
        string Route,
        string Method,
        string StatusClass);

    private sealed class SeriesState
    {
        private long _requestCount;
        private long _durationMicroseconds;

        public void Record(double elapsedMilliseconds)
        {
            Interlocked.Increment(ref _requestCount);
            var micros = checked((long)Math.Round(
                Math.Max(0, elapsedMilliseconds) * 1000d,
                MidpointRounding.AwayFromZero));
            Interlocked.Add(ref _durationMicroseconds, micros);
        }

        public (long Count, double DurationMs) Snapshot() =>
            (
                Interlocked.Read(ref _requestCount),
                Interlocked.Read(ref _durationMicroseconds) / 1000d
            );
    }

    private readonly ActivitySource _activitySource =
        new(InstrumentationName);
    private readonly Meter _meter =
        new(InstrumentationName);
    private readonly Counter<long> _requestCount;
    private readonly Histogram<double> _requestDurationMs;
    private readonly Counter<long> _droppedSeriesCounter;
    private readonly Dictionary<SeriesKey, SeriesState> _series = new();
    private readonly object _seriesGate = new();
    private readonly int _maxActiveSeries;
    private long _droppedSeries;

    public ApiTelemetry()
        : this(ObservabilityOptions.DefaultMaxActiveSeries)
    {
    }

    public ApiTelemetry(KafdeckOptions options)
        : this(ObservabilityOptions.Effective(options).MaxActiveSeries)
    {
    }

    public ApiTelemetry(int maxActiveSeries)
    {
        if (maxActiveSeries is < 1 or > ObservabilityOptions.HardMaxActiveSeries)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxActiveSeries));
        }

        _maxActiveSeries = maxActiveSeries;
        _requestCount = _meter.CreateCounter<long>(
            "kafdeck.api.requests",
            unit: "{request}",
            description: "Number of HTTP requests handled by Kafdeck API.");
        _requestDurationMs = _meter.CreateHistogram<double>(
            "kafdeck.api.request.duration",
            unit: "ms",
            description: "Kafdeck API request duration.");
        _droppedSeriesCounter = _meter.CreateCounter<long>(
            "kafdeck.observability.series.dropped",
            unit: "{series}",
            description:
                "Number of metric series rejected by Kafdeck cardinality bounds.");
    }

    public Activity? StartRequest(
        string routeName,
        string method)
    {
        var activity = _activitySource.StartActivity(
            "kafdeck.api.request",
            ActivityKind.Internal);
        activity?.SetTag(
            "kafdeck.route",
            NormalizeRouteName(routeName));
        activity?.SetTag(
            "http.request.method",
            NormalizeMethod(method));
        return activity;
    }

    public void RecordRequest(
        string routeName,
        string method,
        int statusCode,
        double elapsedMilliseconds)
    {
        var route = NormalizeRouteName(routeName);
        var normalizedMethod = NormalizeMethod(method);
        var statusClass = NormalizeStatusClass(statusCode);

        var tags = new TagList
        {
            { "kafdeck.route", route },
            { "http.request.method", normalizedMethod },
            { "http.response.status_code", statusClass },
        };

        _requestCount.Add(1, tags);
        _requestDurationMs.Record(
            Math.Max(0, elapsedMilliseconds),
            tags);

        var state = GetOrCreateSeries(
            new SeriesKey(
                route,
                normalizedMethod,
                statusClass));

        state?.Record(elapsedMilliseconds);
    }

    public ApiTelemetrySnapshot Snapshot()
    {
        List<(SeriesKey Key, SeriesState State)> series;
        lock (_seriesGate)
        {
            series = _series
                .Select(pair => (pair.Key, pair.Value))
                .ToList();
        }

        var snapshots = series
            .Select(item =>
            {
                var values = item.State.Snapshot();
                return new ApiTelemetrySeriesSnapshot(
                    item.Key.Route,
                    item.Key.Method,
                    item.Key.StatusClass,
                    values.Count,
                    values.DurationMs);
            })
            .OrderBy(item => item.Route, StringComparer.Ordinal)
            .ThenBy(item => item.Method, StringComparer.Ordinal)
            .ThenBy(item => item.StatusClass, StringComparer.Ordinal)
            .ToArray();

        return new ApiTelemetrySnapshot(
            snapshots.Length,
            _maxActiveSeries,
            Interlocked.Read(ref _droppedSeries),
            Array.AsReadOnly(snapshots));
    }

    public string RenderPrometheus()
    {
        var snapshot = Snapshot();
        var builder = new StringBuilder();

        builder.AppendLine(
            "# HELP kafdeck_api_requests_total Number of HTTP requests handled by Kafdeck.");
        builder.AppendLine(
            "# TYPE kafdeck_api_requests_total counter");
        foreach (var item in snapshot.Series)
        {
            AppendLabels(builder, item);
            builder.Append(' ')
                .Append(item.RequestCount.ToString(
                    CultureInfo.InvariantCulture))
                .AppendLine();
        }

        builder.AppendLine(
            "# HELP kafdeck_api_request_duration_ms_sum Sum of Kafdeck HTTP request duration in milliseconds.");
        builder.AppendLine(
            "# TYPE kafdeck_api_request_duration_ms_sum counter");
        foreach (var item in snapshot.Series)
        {
            builder.Append(
                "kafdeck_api_request_duration_ms_sum");
            AppendLabelSet(builder, item);
            builder.Append(' ')
                .Append(item.DurationSumMilliseconds.ToString(
                    "R",
                    CultureInfo.InvariantCulture))
                .AppendLine();
        }

        builder.AppendLine(
            "# HELP kafdeck_api_request_duration_ms_count Count of Kafdeck HTTP request duration observations.");
        builder.AppendLine(
            "# TYPE kafdeck_api_request_duration_ms_count counter");
        foreach (var item in snapshot.Series)
        {
            builder.Append(
                "kafdeck_api_request_duration_ms_count");
            AppendLabelSet(builder, item);
            builder.Append(' ')
                .Append(item.RequestCount.ToString(
                    CultureInfo.InvariantCulture))
                .AppendLine();
        }

        builder.AppendLine(
            "# HELP kafdeck_observability_active_series Current bounded Kafdeck metric series.");
        builder.AppendLine(
            "# TYPE kafdeck_observability_active_series gauge");
        builder.Append("kafdeck_observability_active_series ")
            .Append(snapshot.ActiveSeries.ToString(
                CultureInfo.InvariantCulture))
            .AppendLine();

        builder.AppendLine(
            "# HELP kafdeck_observability_series_dropped_total Metric series rejected by Kafdeck cardinality bounds.");
        builder.AppendLine(
            "# TYPE kafdeck_observability_series_dropped_total counter");
        builder.Append(
                "kafdeck_observability_series_dropped_total ")
            .Append(snapshot.DroppedSeries.ToString(
                CultureInfo.InvariantCulture))
            .AppendLine();

        return builder.ToString();
    }

    public static string NormalizeRouteName(
        string? routeName)
    {
        if (string.IsNullOrWhiteSpace(routeName))
        {
            return "unmatched";
        }

        var value = routeName.Trim();
        if (value.Length > 128)
        {
            return "other";
        }

        foreach (var character in value)
        {
            if (!(char.IsAsciiLetterOrDigit(character) ||
                  character is '-' or '_' or '.'))
            {
                return "other";
            }
        }

        return value;
    }

    public static string NormalizeMethod(
        string? method)
    {
        if (string.IsNullOrWhiteSpace(method))
        {
            return "OTHER";
        }

        return method.ToUpperInvariant() switch
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

    public static string NormalizeStatusClass(
        int statusCode) => statusCode switch
    {
        >= 200 and < 300 => "2xx",
        >= 300 and < 400 => "3xx",
        >= 400 and < 500 => "4xx",
        >= 500 and < 600 => "5xx",
        _ => "other",
    };

    private SeriesState? GetOrCreateSeries(
        SeriesKey key)
    {
        lock (_seriesGate)
        {
            if (_series.TryGetValue(
                    key,
                    out var existing))
            {
                return existing;
            }

            if (_series.Count >= _maxActiveSeries)
            {
                Interlocked.Increment(
                    ref _droppedSeries);
                _droppedSeriesCounter.Add(1);
                return null;
            }

            var created = new SeriesState();
            _series.Add(key, created);
            return created;
        }
    }

    private static void AppendLabels(
        StringBuilder builder,
        ApiTelemetrySeriesSnapshot item)
    {
        builder.Append(
            "kafdeck_api_requests_total");
        AppendLabelSet(builder, item);
    }

    private static void AppendLabelSet(
        StringBuilder builder,
        ApiTelemetrySeriesSnapshot item)
    {
        builder.Append("{route=\"")
            .Append(EscapeLabel(item.Route))
            .Append("\",method=\"")
            .Append(EscapeLabel(item.Method))
            .Append("\",status_class=\"")
            .Append(EscapeLabel(item.StatusClass))
            .Append("\"}");
    }

    private static string EscapeLabel(
        string value) =>
        value
            .Replace(
                "\\",
                "\\\\",
                StringComparison.Ordinal)
            .Replace(
                "\"",
                "\\\"",
                StringComparison.Ordinal)
            .Replace(
                "\n",
                "\\n",
                StringComparison.Ordinal);

    public void Dispose()
    {
        _activitySource.Dispose();
        _meter.Dispose();
    }
}

public sealed class ApiTelemetryMiddleware
{
    private readonly RequestDelegate _next;

    public ApiTelemetryMiddleware(
        RequestDelegate next)
    {
        _next = next ??
            throw new ArgumentNullException(
                nameof(next));
    }

    public async Task InvokeAsync(
        HttpContext context,
        ApiTelemetry telemetry)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(telemetry);

        var started =
            Stopwatch.GetTimestamp();
        var routeName =
            context.GetEndpoint()?
                .Metadata
                .GetMetadata<
                    Microsoft.AspNetCore.Routing
                        .RouteNameMetadata>()?
                .RouteName ??
            "unmatched";

        using var activity =
            telemetry.StartRequest(
                routeName,
                context.Request.Method);
        try
        {
            await _next(context)
                .ConfigureAwait(false);
        }
        finally
        {
            var elapsed =
                Stopwatch
                    .GetElapsedTime(started)
                    .TotalMilliseconds;
            telemetry.RecordRequest(
                routeName,
                context.Request.Method,
                context.Response.StatusCode,
                elapsed);
        }
    }
}
