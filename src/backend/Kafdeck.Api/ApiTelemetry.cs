using System.Collections;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Kafdeck.Core.Observability;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;

namespace Kafdeck.Api;

public sealed record ApiTelemetrySeriesSnapshot(
    string Route,
    string Method,
    string StatusClass,
    long RequestCount,
    double DurationSumMilliseconds);

public sealed record OperationalTelemetrySeriesSnapshot(
    string Kind,
    string Family,
    string Outcome,
    long OperationCount,
    double DurationSumMilliseconds);

public sealed record ApiTelemetrySnapshot(
    int ActiveSeries,
    int MaxActiveSeries,
    long DroppedSeries,
    IReadOnlyList<ApiTelemetrySeriesSnapshot> Series,
    IReadOnlyList<OperationalTelemetrySeriesSnapshot> OperationalSeries);

public sealed class ApiTelemetry :
    IKafdeckOperationalTelemetry,
    IDisposable
{
    public const string InstrumentationName = "Kafdeck.Api";

    private const int FixedPrometheusSeriesCount = 2;
    private const int PrometheusSeriesPerKey = 3;
    private const int OperationalLogEventId = 8001;

    private readonly record struct SeriesKey(
        string Route,
        string Method,
        string StatusClass);

    private readonly record struct OperationalSeriesKey(
        KafdeckOperationalKind Kind,
        KafdeckOperationalFamily Family,
        KafdeckOperationalOutcome Outcome);

    private sealed class SeriesState
    {
        private long _count;
        private long _durationMicroseconds;

        public void Record(double elapsedMilliseconds)
        {
            Interlocked.Increment(ref _count);
            var micros = checked((long)Math.Round(
                Math.Max(0, elapsedMilliseconds) * 1000d,
                MidpointRounding.AwayFromZero));
            Interlocked.Add(
                ref _durationMicroseconds,
                micros);
        }

        public (long Count, double DurationMs) Snapshot() =>
            (
                Interlocked.Read(ref _count),
                Interlocked.Read(ref _durationMicroseconds) / 1000d
            );
    }

    private sealed class OperationalScope :
        IKafdeckOperationalTelemetryScope
    {
        private readonly ApiTelemetry _owner;
        private readonly KafdeckOperationalKind _kind;
        private readonly KafdeckOperationalFamily _family;
        private readonly long _started;
        private Activity? _activity;
        private int _completed;

        public OperationalScope(
            ApiTelemetry owner,
            KafdeckOperationalKind kind,
            KafdeckOperationalFamily family,
            Activity? activity)
        {
            _owner = owner;
            _kind = kind;
            _family = family;
            _activity = activity;
            _started = Stopwatch.GetTimestamp();
        }

        public void Complete(
            KafdeckOperationalOutcome outcome)
        {
            if (Interlocked.Exchange(
                    ref _completed,
                    1) != 0)
            {
                return;
            }

            var elapsed =
                Stopwatch.GetElapsedTime(_started)
                    .TotalMilliseconds;

            _owner.CompleteOperational(
                _kind,
                _family,
                outcome,
                elapsed,
                _activity);

            _activity?.Dispose();
            _activity = null;
        }

        public void Dispose()
        {
            if (Volatile.Read(ref _completed) == 0)
            {
                Complete(
                    KafdeckOperationalOutcome.Failed);
            }
        }
    }

    private sealed class SafeOperationalLogState :
        IReadOnlyList<KeyValuePair<string, object?>>
    {
        private readonly KeyValuePair<string, object?>[] _items;

        public SafeOperationalLogState(
            int maxAttributes,
            string diagnosticMessage,
            KafdeckOperationalKind kind,
            KafdeckOperationalFamily family,
            KafdeckOperationalOutcome outcome,
            double durationMilliseconds)
        {
            DiagnosticMessage = diagnosticMessage;

            var all =
                new[]
                {
                    new KeyValuePair<string, object?>(
                        "kafdeck.operation.kind",
                        KindLabel(kind)),
                    new KeyValuePair<string, object?>(
                        "kafdeck.operation.family",
                        FamilyLabel(family)),
                    new KeyValuePair<string, object?>(
                        "kafdeck.operation.outcome",
                        OutcomeLabel(outcome)),
                    new KeyValuePair<string, object?>(
                        "kafdeck.operation.duration_ms",
                        Math.Max(
                            0,
                            durationMilliseconds)),
                };

            _items = all
                .Take(
                    Math.Min(
                        maxAttributes,
                        all.Length))
                .ToArray();
        }

        public string DiagnosticMessage { get; }

        public int Count => _items.Length;

        public KeyValuePair<string, object?> this[int index] =>
            _items[index];

        public IEnumerator<KeyValuePair<string, object?>>
            GetEnumerator() =>
            ((IEnumerable<KeyValuePair<string, object?>>)
                _items).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() =>
            _items.GetEnumerator();
    }

    private readonly ActivitySource _activitySource =
        new(InstrumentationName);
    private readonly Meter _meter =
        new(InstrumentationName);

    private readonly Counter<long> _requestCount;
    private readonly Histogram<double> _requestDurationMs;
    private readonly Counter<long> _operationalCount;
    private readonly Histogram<double> _operationalDurationMs;
    private readonly Counter<long> _droppedSeriesCounter;

    private readonly Dictionary<SeriesKey, SeriesState>
        _series = new();
    private readonly Dictionary<OperationalSeriesKey, SeriesState>
        _operationalSeries = new();
    private readonly object _seriesGate = new();

    private readonly int _maxActiveSeries;
    private readonly int _maxMetricLabelValueBytes;
    private readonly int _maxTraceAttributes;
    private readonly int _maxLogAttributes;
    private readonly int _maxDiagnosticStringBytes;
    private readonly ILogger<ApiTelemetry>? _logger;

    private long _droppedSeries;

    public ApiTelemetry()
        : this(
            ObservabilityOptions.DefaultMaxActiveSeries,
            ObservabilityOptions.DefaultMaxMetricLabelValueBytes,
            ObservabilityOptions.DefaultMaxTraceAttributes,
            ObservabilityOptions.DefaultMaxLogAttributes,
            ObservabilityOptions.DefaultMaxDiagnosticStringBytes,
            logger: null)
    {
    }

    public ApiTelemetry(
        KafdeckOptions options)
        : this(
            options,
            logger: null)
    {
    }

    public ApiTelemetry(
        KafdeckOptions options,
        ILogger<ApiTelemetry>? logger)
        : this(
            ObservabilityOptions.Effective(options)
                .MaxActiveSeries,
            ObservabilityOptions.Effective(options)
                .MaxMetricLabelValueBytes,
            ObservabilityOptions.Effective(options)
                .MaxTraceAttributes,
            ObservabilityOptions.Effective(options)
                .MaxLogAttributes,
            ObservabilityOptions.Effective(options)
                .MaxDiagnosticStringBytes,
            logger)
    {
    }

    public ApiTelemetry(
        int maxActiveSeries)
        : this(
            maxActiveSeries,
            ObservabilityOptions.DefaultMaxMetricLabelValueBytes,
            ObservabilityOptions.DefaultMaxTraceAttributes,
            ObservabilityOptions.DefaultMaxLogAttributes,
            ObservabilityOptions.DefaultMaxDiagnosticStringBytes,
            logger: null)
    {
    }

    public ApiTelemetry(
        int maxActiveSeries,
        int maxMetricLabelValueBytes,
        int maxTraceAttributes)
        : this(
            maxActiveSeries,
            maxMetricLabelValueBytes,
            maxTraceAttributes,
            ObservabilityOptions.DefaultMaxLogAttributes,
            ObservabilityOptions.DefaultMaxDiagnosticStringBytes,
            logger: null)
    {
    }

    public ApiTelemetry(
        int maxActiveSeries,
        int maxMetricLabelValueBytes,
        int maxTraceAttributes,
        int maxLogAttributes,
        int maxDiagnosticStringBytes,
        ILogger<ApiTelemetry>? logger = null)
    {
        if (maxActiveSeries <
                ObservabilityOptions.MinimumMaxActiveSeries ||
            maxActiveSeries >
                ObservabilityOptions.HardMaxActiveSeries)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxActiveSeries));
        }

        if (maxMetricLabelValueBytes <
                ObservabilityOptions.MinimumMaxMetricLabelValueBytes ||
            maxMetricLabelValueBytes >
                ObservabilityOptions.HardMaxMetricLabelValueBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxMetricLabelValueBytes));
        }

        if (maxTraceAttributes <
                ObservabilityOptions.MinimumMaxTraceAttributes ||
            maxTraceAttributes >
                ObservabilityOptions.HardMaxTraceAttributes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxTraceAttributes));
        }

        if (maxLogAttributes <
                ObservabilityOptions.MinimumMaxLogAttributes ||
            maxLogAttributes >
                ObservabilityOptions.HardMaxLogAttributes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxLogAttributes));
        }

        if (maxDiagnosticStringBytes <
                ObservabilityOptions.MinimumMaxDiagnosticStringBytes ||
            maxDiagnosticStringBytes >
                ObservabilityOptions.HardMaxDiagnosticStringBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxDiagnosticStringBytes));
        }

        _maxActiveSeries = maxActiveSeries;
        _maxMetricLabelValueBytes =
            maxMetricLabelValueBytes;
        _maxTraceAttributes =
            maxTraceAttributes;
        _maxLogAttributes =
            maxLogAttributes;
        _maxDiagnosticStringBytes =
            maxDiagnosticStringBytes;
        _logger = logger;

        _requestCount =
            _meter.CreateCounter<long>(
                "kafdeck.api.requests",
                unit: "{request}",
                description:
                    "Number of HTTP requests handled by Kafdeck API.");
        _requestDurationMs =
            _meter.CreateHistogram<double>(
                "kafdeck.api.request.duration",
                unit: "ms",
                description:
                    "Kafdeck API request duration.");

        _operationalCount =
            _meter.CreateCounter<long>(
                "kafdeck.operational.operations",
                unit: "{operation}",
                description:
                    "Number of bounded provider/governed/worker operations.");
        _operationalDurationMs =
            _meter.CreateHistogram<double>(
                "kafdeck.operational.operation.duration",
                unit: "ms",
                description:
                    "Bounded provider/governed/worker operation duration.");

        _droppedSeriesCounter =
            _meter.CreateCounter<long>(
                "kafdeck.observability.series.dropped",
                unit: "{series}",
                description:
                    "Number of metric series rejected by Kafdeck cardinality bounds.");
    }

    public Activity? StartRequest(
        string routeName,
        string method)
    {
        var activity =
            _activitySource.StartActivity(
                "kafdeck.api.request",
                ActivityKind.Internal);

        if (_maxTraceAttributes >= 1)
        {
            activity?.SetTag(
                "kafdeck.route",
                NormalizeMetricLabel(
                    NormalizeRouteName(
                        routeName)));
        }

        if (_maxTraceAttributes >= 2)
        {
            activity?.SetTag(
                "http.request.method",
                NormalizeMetricLabel(
                    NormalizeMethod(
                        method)));
        }

        return activity;
    }

    public IKafdeckOperationalTelemetryScope Start(
        KafdeckOperationalKind kind,
        KafdeckOperationalFamily family)
    {
        if (!Enum.IsDefined(kind))
        {
            throw new ArgumentOutOfRangeException(
                nameof(kind));
        }

        if (!Enum.IsDefined(family))
        {
            throw new ArgumentOutOfRangeException(
                nameof(family));
        }

        var activity =
            _activitySource.StartActivity(
                "kafdeck.operational",
                kind ==
                    KafdeckOperationalKind.Provider
                    ? ActivityKind.Client
                    : ActivityKind.Internal);

        if (_maxTraceAttributes >= 1)
        {
            activity?.SetTag(
                "kafdeck.operation.kind",
                KindLabel(kind));
        }

        if (_maxTraceAttributes >= 2)
        {
            activity?.SetTag(
                "kafdeck.operation.family",
                FamilyLabel(family));
        }

        return new OperationalScope(
            this,
            kind,
            family,
            activity);
    }

    public void RecordRequest(
        string routeName,
        string method,
        int statusCode,
        double elapsedMilliseconds)
    {
        var route =
            NormalizeMetricLabel(
                NormalizeRouteName(
                    routeName));
        var normalizedMethod =
            NormalizeMetricLabel(
                NormalizeMethod(
                    method));
        var statusClass =
            NormalizeMetricLabel(
                NormalizeStatusClass(
                    statusCode));

        var state =
            GetOrCreateApiSeries(
                new SeriesKey(
                    route,
                    normalizedMethod,
                    statusClass));

        if (state is null)
        {
            return;
        }

        var tags = new TagList
        {
            { "kafdeck.route", route },
            {
                "http.request.method",
                normalizedMethod
            },
            {
                "http.response.status_code",
                statusClass
            },
        };

        _requestCount.Add(
            1,
            tags);
        _requestDurationMs.Record(
            Math.Max(
                0,
                elapsedMilliseconds),
            tags);

        state.Record(
            elapsedMilliseconds);
    }

    public ApiTelemetrySnapshot Snapshot()
    {
        List<(SeriesKey Key, SeriesState State)>
            apiSeries;
        List<(OperationalSeriesKey Key, SeriesState State)>
            operationalSeries;

        lock (_seriesGate)
        {
            apiSeries =
                _series
                    .Select(
                        pair =>
                            (pair.Key, pair.Value))
                    .ToList();
            operationalSeries =
                _operationalSeries
                    .Select(
                        pair =>
                            (pair.Key, pair.Value))
                    .ToList();
        }

        var apiSnapshots =
            apiSeries
                .Select(item =>
                {
                    var values =
                        item.State.Snapshot();
                    return new ApiTelemetrySeriesSnapshot(
                        item.Key.Route,
                        item.Key.Method,
                        item.Key.StatusClass,
                        values.Count,
                        values.DurationMs);
                })
                .OrderBy(
                    item => item.Route,
                    StringComparer.Ordinal)
                .ThenBy(
                    item => item.Method,
                    StringComparer.Ordinal)
                .ThenBy(
                    item => item.StatusClass,
                    StringComparer.Ordinal)
                .ToArray();

        var operationalSnapshots =
            operationalSeries
                .Select(item =>
                {
                    var values =
                        item.State.Snapshot();
                    return new OperationalTelemetrySeriesSnapshot(
                        KindLabel(item.Key.Kind),
                        FamilyLabel(item.Key.Family),
                        OutcomeLabel(item.Key.Outcome),
                        values.Count,
                        values.DurationMs);
                })
                .OrderBy(
                    item => item.Kind,
                    StringComparer.Ordinal)
                .ThenBy(
                    item => item.Family,
                    StringComparer.Ordinal)
                .ThenBy(
                    item => item.Outcome,
                    StringComparer.Ordinal)
                .ToArray();

        return new ApiTelemetrySnapshot(
            CurrentSeriesCount(
                apiSnapshots.Length,
                operationalSnapshots.Length),
            _maxActiveSeries,
            Interlocked.Read(
                ref _droppedSeries),
            Array.AsReadOnly(
                apiSnapshots),
            Array.AsReadOnly(
                operationalSnapshots));
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
            builder.Append(
                "kafdeck_api_requests_total");
            AppendApiLabelSet(
                builder,
                item);
            builder.Append(' ')
                .Append(
                    item.RequestCount.ToString(
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
            AppendApiLabelSet(
                builder,
                item);
            builder.Append(' ')
                .Append(
                    item.DurationSumMilliseconds
                        .ToString(
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
            AppendApiLabelSet(
                builder,
                item);
            builder.Append(' ')
                .Append(
                    item.RequestCount.ToString(
                        CultureInfo.InvariantCulture))
                .AppendLine();
        }

        builder.AppendLine(
            "# HELP kafdeck_operational_operations_total Number of bounded Kafdeck provider/governed/worker operations.");
        builder.AppendLine(
            "# TYPE kafdeck_operational_operations_total counter");
        foreach (
            var item in
            snapshot.OperationalSeries)
        {
            builder.Append(
                "kafdeck_operational_operations_total");
            AppendOperationalLabelSet(
                builder,
                item);
            builder.Append(' ')
                .Append(
                    item.OperationCount.ToString(
                        CultureInfo.InvariantCulture))
                .AppendLine();
        }

        builder.AppendLine(
            "# HELP kafdeck_operational_operation_duration_ms_sum Sum of bounded Kafdeck provider/governed/worker duration in milliseconds.");
        builder.AppendLine(
            "# TYPE kafdeck_operational_operation_duration_ms_sum counter");
        foreach (
            var item in
            snapshot.OperationalSeries)
        {
            builder.Append(
                "kafdeck_operational_operation_duration_ms_sum");
            AppendOperationalLabelSet(
                builder,
                item);
            builder.Append(' ')
                .Append(
                    item.DurationSumMilliseconds
                        .ToString(
                            "R",
                            CultureInfo.InvariantCulture))
                .AppendLine();
        }

        builder.AppendLine(
            "# HELP kafdeck_operational_operation_duration_ms_count Count of bounded Kafdeck provider/governed/worker duration observations.");
        builder.AppendLine(
            "# TYPE kafdeck_operational_operation_duration_ms_count counter");
        foreach (
            var item in
            snapshot.OperationalSeries)
        {
            builder.Append(
                "kafdeck_operational_operation_duration_ms_count");
            AppendOperationalLabelSet(
                builder,
                item);
            builder.Append(' ')
                .Append(
                    item.OperationCount.ToString(
                        CultureInfo.InvariantCulture))
                .AppendLine();
        }

        builder.AppendLine(
            "# HELP kafdeck_observability_active_series Current bounded Kafdeck metric series.");
        builder.AppendLine(
            "# TYPE kafdeck_observability_active_series gauge");
        builder.Append(
                "kafdeck_observability_active_series ")
            .Append(
                snapshot.ActiveSeries.ToString(
                    CultureInfo.InvariantCulture))
            .AppendLine();

        builder.AppendLine(
            "# HELP kafdeck_observability_series_dropped_total Metric series rejected by Kafdeck cardinality bounds.");
        builder.AppendLine(
            "# TYPE kafdeck_observability_series_dropped_total counter");
        builder.Append(
                "kafdeck_observability_series_dropped_total ")
            .Append(
                snapshot.DroppedSeries.ToString(
                    CultureInfo.InvariantCulture))
            .AppendLine();

        return builder.ToString();
    }

    public static string NormalizeRouteName(
        string? routeName)
    {
        if (string.IsNullOrWhiteSpace(
                routeName))
        {
            return "unmatched";
        }

        var value =
            routeName.Trim();
        if (value.Length > 128)
        {
            return "other";
        }

        foreach (var character in value)
        {
            if (!(char.IsAsciiLetterOrDigit(
                      character) ||
                  character is '-' or '_' or '.'))
            {
                return "other";
            }
        }

        return value;
    }

    public static string NormalizeMethod(
        string? method) =>
        string.Equals(
            method,
            "GET",
            StringComparison.OrdinalIgnoreCase)
            ? "GET"
            : "OTHER";

    public static string NormalizeStatusClass(
        int statusCode) =>
        statusCode switch
        {
            >= 200 and < 300 => "2xx",
            >= 300 and < 400 => "3xx",
            >= 400 and < 500 => "4xx",
            >= 500 and < 600 => "5xx",
            _ => "other",
        };

    public void SetResponseStatus(
        Activity? activity,
        int statusCode)
    {
        if (_maxTraceAttributes >= 3)
        {
            activity?.SetTag(
                "http.response.status_code",
                NormalizeMetricLabel(
                    NormalizeStatusClass(
                        statusCode)));
        }
    }

    private void CompleteOperational(
        KafdeckOperationalKind kind,
        KafdeckOperationalFamily family,
        KafdeckOperationalOutcome outcome,
        double elapsedMilliseconds,
        Activity? activity)
    {
        if (_maxTraceAttributes >= 3)
        {
            activity?.SetTag(
                "kafdeck.operation.outcome",
                OutcomeLabel(
                    outcome));
        }

        if (outcome is
            KafdeckOperationalOutcome.Failed or
            KafdeckOperationalOutcome.Timeout or
            KafdeckOperationalOutcome.Unavailable or
            KafdeckOperationalOutcome.UnknownExternalEffect)
        {
            activity?.SetStatus(
                ActivityStatusCode.Error,
                OutcomeLabel(
                    outcome));
        }

        RecordOperational(
            kind,
            family,
            outcome,
            elapsedMilliseconds);
        WriteOperationalLog(
            kind,
            family,
            outcome,
            elapsedMilliseconds);
    }

    private void RecordOperational(
        KafdeckOperationalKind kind,
        KafdeckOperationalFamily family,
        KafdeckOperationalOutcome outcome,
        double elapsedMilliseconds)
    {
        var key =
            new OperationalSeriesKey(
                kind,
                family,
                outcome);

        var state =
            GetOrCreateOperationalSeries(
                key);
        if (state is null)
        {
            return;
        }

        var tags =
            new TagList
            {
                {
                    "kafdeck.operation.kind",
                    KindLabel(kind)
                },
                {
                    "kafdeck.operation.family",
                    FamilyLabel(family)
                },
                {
                    "kafdeck.operation.outcome",
                    OutcomeLabel(outcome)
                },
            };

        _operationalCount.Add(
            1,
            tags);
        _operationalDurationMs.Record(
            Math.Max(
                0,
                elapsedMilliseconds),
            tags);
        state.Record(
            elapsedMilliseconds);
    }

    private void WriteOperationalLog(
        KafdeckOperationalKind kind,
        KafdeckOperationalFamily family,
        KafdeckOperationalOutcome outcome,
        double elapsedMilliseconds)
    {
        if (_logger is null)
        {
            return;
        }

        var diagnostic =
            $"Kafdeck operational event: {KindLabel(kind)}/{FamilyLabel(family)} -> {OutcomeLabel(outcome)} in {Math.Max(0, elapsedMilliseconds).ToString("0.###", CultureInfo.InvariantCulture)} ms";
        diagnostic =
            TruncateUtf8(
                diagnostic,
                _maxDiagnosticStringBytes);

        var state =
            new SafeOperationalLogState(
                _maxLogAttributes,
                diagnostic,
                kind,
                family,
                outcome,
                elapsedMilliseconds);

        _logger.Log(
            LogLevel.Information,
            new EventId(
                OperationalLogEventId,
                "KafdeckOperational"),
            state,
            exception: null,
            static (
                logState,
                _) =>
                logState.DiagnosticMessage);
    }

    private string NormalizeMetricLabel(
        string value)
    {
        if (string.IsNullOrWhiteSpace(
                value))
        {
            return "other";
        }

        var normalized =
            value.Trim();

        return Encoding.UTF8.GetByteCount(
                   normalized) <=
               _maxMetricLabelValueBytes
            ? normalized
            : "other";
    }

    private SeriesState? GetOrCreateApiSeries(
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

            if (!CanReserveSeries(
                    PrometheusSeriesPerKey))
            {
                RecordDroppedSeries(
                    PrometheusSeriesPerKey);
                return null;
            }

            var created =
                new SeriesState();
            _series.Add(
                key,
                created);
            return created;
        }
    }

    private SeriesState?
        GetOrCreateOperationalSeries(
            OperationalSeriesKey key)
    {
        lock (_seriesGate)
        {
            if (_operationalSeries.TryGetValue(
                    key,
                    out var existing))
            {
                return existing;
            }

            if (!CanReserveSeries(
                    PrometheusSeriesPerKey))
            {
                RecordDroppedSeries(
                    PrometheusSeriesPerKey);
                return null;
            }

            var created =
                new SeriesState();
            _operationalSeries.Add(
                key,
                created);
            return created;
        }
    }

    private bool CanReserveSeries(
        int count) =>
        CurrentSeriesCount(
            _series.Count,
            _operationalSeries.Count) +
        count <=
        _maxActiveSeries;

    private static int CurrentSeriesCount(
        int apiKeys,
        int operationalKeys) =>
        FixedPrometheusSeriesCount +
        ((apiKeys + operationalKeys) *
            PrometheusSeriesPerKey);

    private void RecordDroppedSeries(
        int count)
    {
        Interlocked.Add(
            ref _droppedSeries,
            count);
        _droppedSeriesCounter.Add(
            count);
    }

    private static void AppendApiLabelSet(
        StringBuilder builder,
        ApiTelemetrySeriesSnapshot item)
    {
        builder.Append("{route=\"")
            .Append(
                EscapeLabel(
                    item.Route))
            .Append("\",method=\"")
            .Append(
                EscapeLabel(
                    item.Method))
            .Append("\",status_class=\"")
            .Append(
                EscapeLabel(
                    item.StatusClass))
            .Append("\"}");
    }

    private static void AppendOperationalLabelSet(
        StringBuilder builder,
        OperationalTelemetrySeriesSnapshot item)
    {
        builder.Append("{kind=\"")
            .Append(
                EscapeLabel(
                    item.Kind))
            .Append("\",family=\"")
            .Append(
                EscapeLabel(
                    item.Family))
            .Append("\",outcome=\"")
            .Append(
                EscapeLabel(
                    item.Outcome))
            .Append("\"}");
    }

    private static string KindLabel(
        KafdeckOperationalKind kind) =>
        kind switch
        {
            KafdeckOperationalKind.Provider =>
                "provider",
            KafdeckOperationalKind.GovernedOperation =>
                "governed_operation",
            KafdeckOperationalKind.Worker =>
                "worker",
            _ => "unknown",
        };

    private static string FamilyLabel(
        KafdeckOperationalFamily family) =>
        family switch
        {
            KafdeckOperationalFamily.KafkaMetadataRead =>
                "kafka_metadata_read",
            KafdeckOperationalFamily.KafkaRecordRead =>
                "kafka_record_read",
            KafdeckOperationalFamily.ConsumerGroupRead =>
                "consumer_group_read",
            KafdeckOperationalFamily.SchemaRegistryRead =>
                "schema_registry_read",
            KafdeckOperationalFamily.KafkaConnectRead =>
                "kafka_connect_read",
            KafdeckOperationalFamily.KsqlMetadataRead =>
                "ksql_metadata_read",
            KafdeckOperationalFamily.KsqlQuery =>
                "ksql_query",
            KafdeckOperationalFamily.StreamsTelemetryRead =>
                "streams_telemetry_read",
            KafdeckOperationalFamily.MutationExecution =>
                "mutation_execution",
            KafdeckOperationalFamily.GovernedDataJobWorker =>
                "governed_data_job_worker",
            KafdeckOperationalFamily.DataGeneratorWorker =>
                "data_generator_worker",
            _ => "unknown",
        };

    private static string OutcomeLabel(
        KafdeckOperationalOutcome outcome) =>
        outcome switch
        {
            KafdeckOperationalOutcome.Success =>
                "success",
            KafdeckOperationalOutcome.Denied =>
                "denied",
            KafdeckOperationalOutcome.Unsupported =>
                "unsupported",
            KafdeckOperationalOutcome.Unavailable =>
                "unavailable",
            KafdeckOperationalOutcome.Timeout =>
                "timeout",
            KafdeckOperationalOutcome.Cancelled =>
                "cancelled",
            KafdeckOperationalOutcome.Invalid =>
                "invalid",
            KafdeckOperationalOutcome.Failed =>
                "failed",
            KafdeckOperationalOutcome.UnknownExternalEffect =>
                "unknown_external_effect",
            KafdeckOperationalOutcome.Blocked =>
                "blocked",
            _ => "unknown",
        };

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

    private static string TruncateUtf8(
        string value,
        int maxBytes)
    {
        if (Encoding.UTF8.GetByteCount(
                value) <=
            maxBytes)
        {
            return value;
        }

        var builder =
            new StringBuilder(
                value.Length);

        foreach (var rune in value.EnumerateRunes())
        {
            var candidateBytes =
                Encoding.UTF8.GetByteCount(
                    rune.ToString());
            var currentBytes =
                Encoding.UTF8.GetByteCount(
                    builder.ToString());

            if (currentBytes +
                    candidateBytes >
                maxBytes)
            {
                break;
            }

            builder.Append(
                rune.ToString());
        }

        return builder.ToString();
    }

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
        _next =
            next ??
            throw new ArgumentNullException(
                nameof(next));
    }

    public async Task InvokeAsync(
        HttpContext context,
        ApiTelemetry telemetry)
    {
        ArgumentNullException.ThrowIfNull(
            context);
        ArgumentNullException.ThrowIfNull(
            telemetry);

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

        var failed = false;
        try
        {
            await _next(context)
                .ConfigureAwait(false);
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
            var effectiveStatusCode =
                failed
                    ? StatusCodes
                        .Status500InternalServerError
                    : context.Response.StatusCode;

            telemetry.SetResponseStatus(
                activity,
                effectiveStatusCode);

            var elapsed =
                Stopwatch
                    .GetElapsedTime(
                        started)
                    .TotalMilliseconds;

            telemetry.RecordRequest(
                routeName,
                context.Request.Method,
                effectiveStatusCode,
                elapsed);
        }
    }
}
