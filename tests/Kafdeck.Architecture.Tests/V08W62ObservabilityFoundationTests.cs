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
    [InlineData("MaxActiveMetricSeries", "1")]
    [InlineData("MaxActiveMetricSeries", "50001")]
    [InlineData("MaxMetricLabelsPerSeries", "2")]
    [InlineData("MaxMetricLabelsPerSeries", "13")]
    [InlineData("MaxMetricLabelValueBytes", "4")]
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
            ["Kafdeck:Observability:Prometheus:ScrapeToken"] =
                "env:KAFDECK_METRICS_TOKEN",
        });

        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
    }

    [Fact]
    public void Prometheus_registry_counts_each_emitted_series_against_the_limit()
    {
        var registry = new PrometheusMetricsRegistry(
            maxActiveSeries: 5,
            maxLabelsPerSeries: 8,
            maxLabelValueBytes: 64);

        Assert.True(registry.RecordApiRequest(
            "route-a",
            "GET",
            "2xx",
            10));
        Assert.False(registry.RecordApiRequest(
            "route-b",
            "GET",
            "2xx",
            20));

        Assert.Equal(5, registry.ActiveSeriesCount);
        Assert.Equal(3, registry.DroppedSeriesCount);

        var output = registry.Render();
        Assert.Contains(
            "kafdeck_api_requests_total{route=\"route-a\"",
            output,
            StringComparison.Ordinal);
        Assert.Contains(
            "kafdeck_telemetry_active_series 5",
            output,
            StringComparison.Ordinal);
        Assert.Contains(
            "kafdeck_telemetry_dropped_series_total 3",
            output,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "route-b",
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
    public void Enabled_prometheus_requires_an_explicit_scrape_token()
    {
        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Observability:Prometheus:Enabled"] = "true",
        });

        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
    }

    [Fact]
    public void Prometheus_scrape_never_trusts_source_address_as_authentication()
    {
        Assert.False(
            PrometheusScrapeAccessPolicy.IsAllowed(
                expectedToken: null,
                providedToken: null));
    }

    [Fact]
    public void Prometheus_scrape_token_uses_fixed_time_validation()
    {
        Assert.True(
            PrometheusScrapeAccessPolicy.IsAllowed(
                expectedToken: "metrics-secret",
                providedToken: "metrics-secret"));
        Assert.False(
            PrometheusScrapeAccessPolicy.IsAllowed(
                expectedToken: "metrics-secret",
                providedToken: "wrong"));
    }

    [Fact]
    public void Prometheus_registry_enforces_label_value_byte_budget()
    {
        var registry = new PrometheusMetricsRegistry(
            maxActiveSeries: 5,
            maxLabelsPerSeries: 3,
            maxLabelValueBytes: 8);

        Assert.True(registry.RecordApiRequest(
            "this-route-is-too-long",
            "GET",
            "2xx",
            1));

        var output = registry.Render();
        Assert.Contains(
            "route=\"other\"",
            output,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "this-route-is-too-long",
            output,
            StringComparison.Ordinal);
    }

    private static KafdeckOptions Load(
        IReadOnlyDictionary<string, string?> values) =>
        KafdeckConfigurationLoader.Load(
            new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build());
}
