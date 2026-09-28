using System.Net;
using Kafdeck.Api;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W62ObservabilityFoundationTests
{
    [Fact]
    public void Observability_configuration_loads_bounded_prometheus_settings()
    {
        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Observability:Prometheus:Enabled"] = "true",
            ["Kafdeck:Observability:Prometheus:Path"] = "/metrics",
            ["Kafdeck:Observability:Prometheus:ScrapeToken"] =
                "env:KAFDECK_METRICS_TOKEN",
            ["Kafdeck:Observability:MaxActiveMetricSeries"] = "10000",
            ["Kafdeck:Observability:MaxMetricLabelsPerSeries"] = "8",
            ["Kafdeck:Observability:MaxMetricLabelValueBytes"] = "64",
            ["Kafdeck:Observability:MaxTraceAttributes"] = "24",
            ["Kafdeck:Observability:MaxLogAttributes"] = "24",
            ["Kafdeck:Observability:MaxDiagnosticStringBytes"] = "512",
        });

        KafdeckConfigurationValidator.ValidateAndThrow(options);

        var observability = Assert.IsType<ObservabilityOptions>(
            options.Observability);
        Assert.True(observability.Prometheus.Enabled);
        Assert.Equal("/metrics", observability.Prometheus.Path);
        Assert.NotNull(observability.Prometheus.ScrapeToken);
        Assert.Equal(10_000, observability.MaxActiveMetricSeries);
        Assert.Equal(8, observability.MaxMetricLabelsPerSeries);
        Assert.Equal(64, observability.MaxMetricLabelValueBytes);
        Assert.Equal(24, observability.MaxTraceAttributes);
        Assert.Equal(24, observability.MaxLogAttributes);
        Assert.Equal(512, observability.MaxDiagnosticStringBytes);
    }

    [Theory]
    [InlineData("MaxActiveMetricSeries", "50001")]
    [InlineData("MaxMetricLabelsPerSeries", "13")]
    [InlineData("MaxMetricLabelValueBytes", "129")]
    [InlineData("MaxTraceAttributes", "49")]
    [InlineData("MaxLogAttributes", "49")]
    [InlineData("MaxDiagnosticStringBytes", "2049")]
    public void Observability_cap_plus_one_values_fail_closed(
        string key,
        string value)
    {
        var options = Load(new Dictionary<string, string?>
        {
            [$"Kafdeck:Observability:{key}"] = value,
        });

        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
    }

    [Theory]
    [InlineData("/")]
    [InlineData("/healthz")]
    [InlineData("/api/v1/metrics")]
    [InlineData("metrics")]
    [InlineData("/metrics?format=text")]
    public void Prometheus_path_must_not_collide_with_product_routes(
        string path)
    {
        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Observability:Prometheus:Enabled"] = "true",
            ["Kafdeck:Observability:Prometheus:Path"] = path,
        });

        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
    }

    [Fact]
    public void Prometheus_registry_enforces_active_series_hard_boundary()
    {
        var registry = new PrometheusMetricsRegistry(maxActiveSeries: 2);

        Assert.True(registry.RecordApiRequest(
            "route-a",
            "GET",
            "2xx",
            10));
        Assert.True(registry.RecordApiRequest(
            "route-b",
            "GET",
            "2xx",
            20));
        Assert.False(registry.RecordApiRequest(
            "route-c",
            "GET",
            "2xx",
            30));

        Assert.Equal(2, registry.ActiveSeriesCount);
        Assert.Equal(1, registry.DroppedSeriesCount);

        var output = registry.Render();
        Assert.Contains(
            "kafdeck_api_requests_total{route=\"route-a\"",
            output,
            StringComparison.Ordinal);
        Assert.Contains(
            "kafdeck_telemetry_active_series 2",
            output,
            StringComparison.Ordinal);
        Assert.Contains(
            "kafdeck_telemetry_dropped_series_total 1",
            output,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "route-c",
            output,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Api_telemetry_keeps_route_and_method_dimensions_bounded()
    {
        Assert.Equal(
            "v07-ksql-query",
            ApiTelemetry.NormalizeRouteName("v07-ksql-query"));
        Assert.Equal(
            "other",
            ApiTelemetry.NormalizeRouteName("topic/orders/123"));
        Assert.Equal("POST", ApiTelemetry.NormalizeMethod("post"));
        Assert.Equal("OTHER", ApiTelemetry.NormalizeMethod("TRACE"));
        Assert.Equal("5xx", ApiTelemetry.NormalizeStatusClass(503));
    }

    [Fact]
    public void Prometheus_scrape_is_loopback_only_without_a_token()
    {
        Assert.True(
            PrometheusScrapeAccessPolicy.IsAllowed(
                IPAddress.Loopback,
                expectedToken: null,
                providedToken: null));
        Assert.False(
            PrometheusScrapeAccessPolicy.IsAllowed(
                IPAddress.Parse("192.0.2.10"),
                expectedToken: null,
                providedToken: null));
    }

    [Fact]
    public void Prometheus_scrape_token_allows_remote_access_by_fixed_time_check()
    {
        var remote = IPAddress.Parse("192.0.2.10");

        Assert.True(
            PrometheusScrapeAccessPolicy.IsAllowed(
                remote,
                expectedToken: "metrics-secret",
                providedToken: "metrics-secret"));
        Assert.False(
            PrometheusScrapeAccessPolicy.IsAllowed(
                remote,
                expectedToken: "metrics-secret",
                providedToken: "wrong"));
    }

    private static KafdeckOptions Load(
        IReadOnlyDictionary<string, string?> values) =>
        KafdeckConfigurationLoader.Load(
            new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build());
}
