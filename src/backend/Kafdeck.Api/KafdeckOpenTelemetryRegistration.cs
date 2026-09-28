using Kafdeck.Infrastructure.Configuration;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

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

        var builder = services
            .AddOpenTelemetry()
            .ConfigureResource(resource =>
                resource.AddService(
                    serviceName: "Kafdeck"))
            .WithTracing(tracing =>
            {
                tracing.AddSource(ApiTelemetry.InstrumentationName);

                if (observability.Otlp.Enabled)
                {
                    tracing.AddOtlpExporter(options =>
                        ConfigureExporter(
                            options,
                            observability.Otlp,
                            resolvedOtlpHeaders,
                            OtlpSignalKind.Traces));
                }
            })
            .WithMetrics(metrics =>
            {
                metrics.AddMeter(ApiTelemetry.InstrumentationName);

                if (observability.Otlp.Enabled)
                {
                    metrics.AddOtlpExporter(options =>
                        ConfigureExporter(
                            options,
                            observability.Otlp,
                            resolvedOtlpHeaders,
                            OtlpSignalKind.Metrics));
                }
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

        if (!string.IsNullOrWhiteSpace(resolvedHeaders))
        {
            exporter.Headers = resolvedHeaders;
        }
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

internal enum OtlpSignalKind
{
    Traces = 1,
    Metrics = 2,
}
