using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Kafdeck.Api;

public sealed record ObservabilityCapabilityValue(
    string State,
    string? ReasonCode = null);

public sealed record ObservabilityCapabilitiesData(
    ObservabilityCapabilityValue Instrumentation,
    ObservabilityCapabilityValue Prometheus,
    ObservabilityCapabilityValue Otlp,
    ObservabilityCapabilityValue OtlpLogs,
    string PrometheusPath,
    int MaxActiveSeries,
    int HardMaxActiveSeries,
    int MaxMetricLabelsPerSeries,
    int MaxMetricLabelValueBytes,
    int MaxTraceAttributes,
    int MaxLogAttributes,
    int MaxDiagnosticStringBytes);

public static class KafdeckObservabilityEndpoints
{
    public const string CapabilitiesRoute =
        "/api/v1/observability/capabilities";

    internal static ObservabilityCapabilityValue OtlpCapability(
        bool enabled,
        OtlpRuntimeHealth health)
    {
        if (!enabled)
        {
            return new ObservabilityCapabilityValue(
                "unconfigured",
                "otlp_not_enabled");
        }

        return health switch
        {
            OtlpRuntimeHealth.Supported =>
                new ObservabilityCapabilityValue(
                    "supported"),
            OtlpRuntimeHealth.Unavailable =>
                new ObservabilityCapabilityValue(
                    "unavailable",
                    "otlp_export_failed"),
            _ =>
                new ObservabilityCapabilityValue(
                    "unknown",
                    "otlp_export_not_observed"),
        };
    }

    internal static ObservabilityCapabilityValue OtlpLogsCapability(
        bool enabled) =>
        enabled
            ? new ObservabilityCapabilityValue(
                "unknown",
                "otlp_log_export_not_observed")
            : new ObservabilityCapabilityValue(
                "unconfigured",
                "otlp_not_enabled");

    public static bool IsPrometheusScrapePath(
        PathString path)
    {
        var value = path.Value;
        if (string.IsNullOrEmpty(value))
        {
            return false;
        }

        return string.Equals(
                   value,
                   PrometheusObservabilityOptions.Path,
                   StringComparison.OrdinalIgnoreCase) ||
               string.Equals(
                   value,
                   PrometheusObservabilityOptions.Path + "/",
                   StringComparison.OrdinalIgnoreCase);
    }

    public static IEndpointRouteBuilder
        MapKafdeckV08Observability(
            this IEndpointRouteBuilder endpoints,
            KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(options);

        var observability =
            ObservabilityOptions.Effective(options);

        endpoints.MapGet(
                CapabilitiesRoute,
                (HttpContext context) =>
                {
                    var otlpHealth =
                        context.RequestServices
                            .GetRequiredService<
                                OtlpExporterHealthState>();

                    return Results.Ok(
                        new ObservabilityCapabilitiesData(
                            new ObservabilityCapabilityValue(
                                "supported"),
                            new ObservabilityCapabilityValue(
                                observability.Prometheus.Enabled
                                    ? "supported"
                                    : "unconfigured",
                                observability.Prometheus.Enabled
                                    ? null
                                    : "prometheus_not_enabled"),
                            OtlpCapability(
                                observability.Otlp.Enabled,
                                otlpHealth.Current),
                            OtlpLogsCapability(
                                observability.Otlp.Enabled),
                            PrometheusObservabilityOptions.Path,
                            observability.MaxActiveSeries,
                            ObservabilityOptions.HardMaxActiveSeries,
                            observability.MaxMetricLabelsPerSeries,
                            observability.MaxMetricLabelValueBytes,
                            observability.MaxTraceAttributes,
                            observability.MaxLogAttributes,
                            observability.MaxDiagnosticStringBytes));
                })
            .WithName("v08-observability-capabilities");

        if (observability.Prometheus.Enabled)
        {
            endpoints.MapGet(
                    PrometheusObservabilityOptions.Path,
                    (ApiTelemetry telemetry) =>
                        Results.Text(
                            telemetry.RenderPrometheus(),
                            "text/plain; version=0.0.4; charset=utf-8"))
                .WithName("v08-prometheus-scrape");
        }

        return endpoints;
    }
}
