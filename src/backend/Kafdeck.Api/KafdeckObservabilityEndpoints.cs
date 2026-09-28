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
    string PrometheusPath,
    int MaxActiveSeries,
    int HardMaxActiveSeries);

public static class KafdeckObservabilityEndpoints
{
    public const string CapabilitiesRoute =
        "/api/v1/observability/capabilities";

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
                () => Results.Ok(
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
                        new ObservabilityCapabilityValue(
                            "unsupported",
                            "otlp_export_not_implemented_in_current_w62_slice"),
                        PrometheusObservabilityOptions.Path,
                        observability.MaxActiveSeries,
                        ObservabilityOptions.HardMaxActiveSeries)))
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
