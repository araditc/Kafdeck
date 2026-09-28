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
                            resolvedOtlpHeaders));
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
                            resolvedOtlpHeaders));
                }
            });

        return services;
    }

    internal static void ConfigureExporter(
        OtlpExporterOptions exporter,
        OtlpObservabilityOptions options,
        string? resolvedHeaders)
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

        exporter.Endpoint = endpoint;
        exporter.Protocol = options.Protocol switch
        {
            OtlpObservabilityProtocol.Grpc =>
                OtlpExportProtocol.Grpc,
            OtlpObservabilityProtocol.HttpProtobuf =>
                OtlpExportProtocol.HttpProtobuf,
            _ => throw new KafdeckConfigurationException(
                "OTLP protocol is unsupported."),
        };

        if (!string.IsNullOrWhiteSpace(resolvedHeaders))
        {
            exporter.Headers = resolvedHeaders;
        }
    }
}
