using System.Diagnostics;
using Kafdeck.Infrastructure.Configuration;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Microsoft.Extensions.Logging;

namespace Kafdeck.Api;

public static class KafdeckOpenTelemetryRegistration
{
    public static IServiceCollection AddKafdeckOpenTelemetry(
        this IServiceCollection services,
        ObservabilityOptions observability,
        string? resolvedOtlpHeaders)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(observability);

        var health = new OtlpExporterHealthState();
        services.AddSingleton(health);

        services
            .AddOpenTelemetry()
            .ConfigureResource(resource =>
                resource.AddService(
                    serviceName: "Kafdeck"))
            .WithTracing(tracing =>
            {
                tracing.AddSource(ApiTelemetry.InstrumentationName);

                if (observability.Otlp.Enabled)
                {
                    var options = new OtlpExporterOptions();
                    ConfigureExporter(
                        options,
                        observability.Otlp,
                        resolvedOtlpHeaders,
                        OtlpSignalKind.Traces);

                    tracing.AddProcessor(
                        new BatchActivityExportProcessor(
                            new HealthTrackingOtlpTraceExporter(
                                options,
                                health)));
                }
            })
            .WithMetrics(metrics =>
            {
                metrics.AddMeter(ApiTelemetry.InstrumentationName);

                if (observability.Otlp.Enabled)
                {
                    var options = new OtlpExporterOptions();
                    ConfigureExporter(
                        options,
                        observability.Otlp,
                        resolvedOtlpHeaders,
                        OtlpSignalKind.Metrics);

                    metrics.AddReader(
                        new PeriodicExportingMetricReader(
                            new HealthTrackingOtlpMetricExporter(
                                options,
                                health)));
                }
            });

        services.AddLogging(logging =>
        {
            logging.AddFilter<OpenTelemetryLoggerProvider>(
                (category, level) =>
                    string.Equals(
                        category,
                        typeof(ApiTelemetry).FullName,
                        StringComparison.Ordinal) &&
                    level >= LogLevel.Information);

            logging.AddOpenTelemetry(options =>
            {
                options.IncludeScopes = false;
                options.IncludeFormattedMessage = true;
                options.ParseStateValues = true;

                if (observability.Otlp.Enabled)
                {
                    var exporterOptions =
                        new OtlpExporterOptions();
                    ConfigureExporter(
                        exporterOptions,
                        observability.Otlp,
                        resolvedOtlpHeaders,
                        OtlpSignalKind.Logs);

                    options.AddProcessor(
                        new BatchLogRecordExportProcessor(
                            new HealthTrackingOtlpLogExporter(
                                exporterOptions,
                                health)));
                }
            });
        });

        return services;
    }

    internal static void ConfigureExporter(
        OtlpExporterOptions exporter,
        OtlpObservabilityOptions options,
        string? resolvedHeaders,
        OtlpSignalKind signal)
    {
        ArgumentNullException.ThrowIfNull(exporter);
        ArgumentNullException.ThrowIfNull(options);

        if (!options.Enabled)
        {
            throw new InvalidOperationException(
                "OTLP exporter configuration requires enabled OTLP observability.");
        }

        if (!Uri.TryCreate(
                options.Endpoint,
                UriKind.Absolute,
                out var endpoint))
        {
            throw new KafdeckConfigurationException(
                "OTLP endpoint must be validated before exporter registration.");
        }

        exporter.Protocol = options.Protocol switch
        {
            OtlpObservabilityProtocol.Grpc =>
                OtlpExportProtocol.Grpc,
            OtlpObservabilityProtocol.HttpProtobuf =>
                OtlpExportProtocol.HttpProtobuf,
            _ => throw new KafdeckConfigurationException(
                "OTLP protocol is unsupported."),
        };

        exporter.Endpoint =
            exporter.Protocol == OtlpExportProtocol.HttpProtobuf
                ? BuildHttpSignalEndpoint(endpoint, signal)
                : endpoint;

        if (exporter.Protocol == OtlpExportProtocol.HttpProtobuf)
        {
            exporter.HttpClientFactory = static () =>
                new HttpClient(
                    CreateNoRedirectHttpHandler(),
                    disposeHandler: true);
        }

        // Always override ambient OpenTelemetry header configuration.
        // Kafdeck accepts OTLP credentials only through the validated
        // secret-reference path; environment-provided exporter headers
        // must not bypass size/isolation/security validation.
        exporter.Headers =
            string.IsNullOrWhiteSpace(resolvedHeaders)
                ? string.Empty
                : resolvedHeaders;
    }

    internal static Uri BuildHttpSignalEndpoint(
        Uri baseEndpoint,
        OtlpSignalKind signal)
    {
        ArgumentNullException.ThrowIfNull(baseEndpoint);

        var suffix = signal switch
        {
            OtlpSignalKind.Traces => "v1/traces",
            OtlpSignalKind.Metrics => "v1/metrics",
            OtlpSignalKind.Logs => "v1/logs",
            _ => throw new ArgumentOutOfRangeException(
                nameof(signal)),
        };

        var builder = new UriBuilder(baseEndpoint);
        var path = builder.Path;
        if (!path.EndsWith("/", StringComparison.Ordinal))
        {
            path += "/";
        }

        builder.Path = path + suffix;
        builder.Query = string.Empty;
        builder.Fragment = string.Empty;
        return builder.Uri;
    }

    internal static HttpClientHandler CreateNoRedirectHttpHandler() =>
        new()
        {
            AllowAutoRedirect = false,
        };
}

public enum OtlpRuntimeHealth
{
    Unknown = 0,
    Supported = 1,
    Unavailable = 2,
}

public sealed class OtlpExporterHealthState
{
    // 0 = no evidence, 1 = latest export succeeded, -1 = latest export failed.
    private int _traceState;
    private int _metricState;
    private int _logState;

    public OtlpRuntimeHealth Current
    {
        get
        {
            var trace = Volatile.Read(ref _traceState);
            var metric = Volatile.Read(ref _metricState);
            var log = Volatile.Read(ref _logState);

            if (trace < 0 || metric < 0 || log < 0)
            {
                return OtlpRuntimeHealth.Unavailable;
            }

            return trace > 0 &&
                   metric > 0 &&
                   log > 0
                ? OtlpRuntimeHealth.Supported
                : OtlpRuntimeHealth.Unknown;
        }
    }

    internal void Record(
        OtlpSignalKind signal,
        ExportResult result)
    {
        var value =
            result == ExportResult.Success
                ? 1
                : -1;

        switch (signal)
        {
            case OtlpSignalKind.Traces:
                Volatile.Write(ref _traceState, value);
                break;
            case OtlpSignalKind.Metrics:
                Volatile.Write(ref _metricState, value);
                break;
            case OtlpSignalKind.Logs:
                Volatile.Write(ref _logState, value);
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(signal));
        }
    }
}

internal sealed class HealthTrackingOtlpTraceExporter
    : OtlpTraceExporter
{
    private readonly OtlpExporterHealthState _health;

    public HealthTrackingOtlpTraceExporter(
        OtlpExporterOptions options,
        OtlpExporterHealthState health)
        : base(options)
    {
        _health =
            health ??
            throw new ArgumentNullException(nameof(health));
    }

    public override ExportResult Export(
        in Batch<Activity> batch)
    {
        var result = base.Export(in batch);
        _health.Record(
            OtlpSignalKind.Traces,
            result);
        return result;
    }
}

internal sealed class HealthTrackingOtlpMetricExporter
    : OtlpMetricExporter
{
    private readonly OtlpExporterHealthState _health;

    public HealthTrackingOtlpMetricExporter(
        OtlpExporterOptions options,
        OtlpExporterHealthState health)
        : base(options)
    {
        _health =
            health ??
            throw new ArgumentNullException(nameof(health));
    }

    public override ExportResult Export(
        in Batch<Metric> batch)
    {
        var result = base.Export(in batch);
        _health.Record(
            OtlpSignalKind.Metrics,
            result);
        return result;
    }
}

internal sealed class HealthTrackingOtlpLogExporter
    : OtlpLogExporter
{
    private readonly OtlpExporterHealthState _health;

    public HealthTrackingOtlpLogExporter(
        OtlpExporterOptions options,
        OtlpExporterHealthState health)
        : base(options)
    {
        _health =
            health ??
            throw new ArgumentNullException(nameof(health));
    }

    public override ExportResult Export(
        in Batch<LogRecord> batch)
    {
        var result = base.Export(in batch);
        _health.Record(
            OtlpSignalKind.Logs,
            result);
        return result;
    }
}

internal enum OtlpSignalKind
{
    Traces = 1,
    Metrics = 2,
    Logs = 3,
}
