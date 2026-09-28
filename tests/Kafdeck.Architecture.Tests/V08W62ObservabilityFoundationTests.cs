using System.Diagnostics;
using System.Diagnostics.Metrics;
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
    }

    [Theory]
    [InlineData("MaxActiveMetricSeries", "1")]
    [InlineData("MaxActiveMetricSeries", "50001")]
    [InlineData("MaxMetricLabelsPerSeries", "2")]
    [InlineData("MaxMetricLabelsPerSeries", "13")]
    [InlineData("MaxMetricLabelValueBytes", "4")]
    [InlineData("MaxMetricLabelValueBytes", "129")]
    [InlineData("MaxTraceAttributes", "49")]
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

    [Fact]
    public void Meter_exports_only_admitted_normalized_dimensions()
    {
        var registry = new PrometheusMetricsRegistry(
            maxActiveSeries: 5,
            maxLabelsPerSeries: 3,
            maxLabelValueBytes: 8);
        var options = ObservabilityOptions.Default with
        {
            MaxActiveMetricSeries = 5,
            MaxMetricLabelsPerSeries = 3,
            MaxMetricLabelValueBytes = 8,
        };

        using var telemetry = new ApiTelemetry(
            registry,
            options);
        using var listener = new MeterListener();

        var requestMeasurements =
            new List<IReadOnlyDictionary<string, object?>>();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (string.Equals(
                    instrument.Meter.Name,
                    ApiTelemetry.InstrumentationName,
                    StringComparison.Ordinal))
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, _, tags, _) =>
            {
                if (!string.Equals(
                        instrument.Name,
                        "kafdeck.api.requests",
                        StringComparison.Ordinal))
                {
                    return;
                }

                requestMeasurements.Add(
                    tags.ToArray().ToDictionary(
                        item => item.Key,
                        item => item.Value,
                        StringComparer.Ordinal));
            });
        listener.Start();

        telemetry.RecordRequest(
            "route-name-is-over-eight-bytes",
            "POST",
            204,
            5);

        var tags = Assert.Single(requestMeasurements);
        Assert.Equal("other", tags["kafdeck.route"]);
        Assert.Equal("POST", tags["http.request.method"]);
        Assert.Equal("2xx", tags["http.response.status_code"]);

        telemetry.RecordRequest(
            "second",
            "GET",
            200,
            5);

        Assert.Single(requestMeasurements);
        Assert.Equal(3, registry.DroppedSeriesCount);
    }

    [Theory]
    [InlineData(1, false, false)]
    [InlineData(2, true, false)]
    [InlineData(3, true, true)]
    public void Trace_attributes_honor_configured_limit(
        int maxTraceAttributes,
        bool expectMethod,
        bool expectStatus)
    {
        var registry = new PrometheusMetricsRegistry(
            maxActiveSeries: 5,
            maxLabelsPerSeries: 3,
            maxLabelValueBytes: 64);
        var options = ObservabilityOptions.Default with
        {
            MaxTraceAttributes = maxTraceAttributes,
        };

        using var telemetry = new ApiTelemetry(
            registry,
            options);
        using var listener = new ActivityListener
        {
            ShouldListenTo = source =>
                string.Equals(
                    source.Name,
                    ApiTelemetry.InstrumentationName,
                    StringComparison.Ordinal),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);

        using var activity = telemetry.StartRequest(
            "v01-topics-list",
            "GET");
        Assert.NotNull(activity);

        telemetry.SetResponseStatus(activity, 200);

        var tags = activity!.TagObjects.ToDictionary(
            item => item.Key,
            item => item.Value,
            StringComparer.Ordinal);

        Assert.True(tags.Count <= maxTraceAttributes);
        Assert.Equal(
            "v01-topics-list",
            tags["kafdeck.route"]);
        Assert.Equal(
            expectMethod,
            tags.ContainsKey("http.request.method"));
        Assert.Equal(
            expectStatus,
            tags.ContainsKey("http.response.status_code"));
    }

    private static KafdeckOptions Load(
        IReadOnlyDictionary<string, string?> values) =>
        KafdeckConfigurationLoader.Load(
            new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build());
}
