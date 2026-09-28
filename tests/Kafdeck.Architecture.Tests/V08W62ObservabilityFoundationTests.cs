using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Security;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W62ObservabilityFoundationTests
{
    [Fact]
    public void Observability_defaults_are_bounded_and_prometheus_disabled()
    {
        var options = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "http://127.0.0.1:8080",
            });

        var observability =
            ObservabilityOptions.Effective(options);

        Assert.Equal(
            ObservabilityOptions.DefaultMaxActiveSeries,
            observability.MaxActiveSeries);
        Assert.Equal(
            ObservabilityOptions.DefaultMaxMetricLabelsPerSeries,
            observability.MaxMetricLabelsPerSeries);
        Assert.Equal(
            ObservabilityOptions.DefaultMaxMetricLabelValueBytes,
            observability.MaxMetricLabelValueBytes);
        Assert.Equal(
            ObservabilityOptions.DefaultMaxTraceAttributes,
            observability.MaxTraceAttributes);
        Assert.False(observability.Prometheus.Enabled);
        Assert.Null(observability.Prometheus.AccessToken);
        Assert.False(observability.Otlp.Enabled);
        Assert.Null(observability.Otlp.Endpoint);
        Assert.Equal(
            OtlpObservabilityProtocol.Grpc,
            observability.Otlp.Protocol);
        Assert.Null(observability.Otlp.Headers);

        KafdeckConfigurationValidator
            .ValidateAndThrow(options);
    }

    [Fact]
    public void Prometheus_scrape_token_must_not_reuse_deployment_token_reference()
    {
        var options = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "https://0.0.0.0:8443",
                ["Kafdeck:Deployment:AccessMode"] =
                    "Token",
                ["Kafdeck:Deployment:AccessToken"] =
                    "env:SHARED_TOKEN",
                ["Kafdeck:Observability:Prometheus:Enabled"] =
                    "true",
                ["Kafdeck:Observability:Prometheus:AccessToken"] =
                    "env:SHARED_TOKEN",
            });

        var exception =
            Assert.Throws<KafdeckConfigurationException>(
                () => KafdeckConfigurationValidator
                    .ValidateAndThrow(options));

        Assert.Contains(
            "distinct secret reference",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Prometheus_requires_a_dedicated_token_even_on_loopback()
    {
        var options = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "http://127.0.0.1:8080",
                ["Kafdeck:Observability:Prometheus:Enabled"] =
                    "true",
            });

        var exception =
            Assert.Throws<KafdeckConfigurationException>(
                () => KafdeckConfigurationValidator
                    .ValidateAndThrow(options));

        Assert.Contains(
            "including loopback/proxied deployments",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Remote_prometheus_requires_https_and_dedicated_secret_reference()
    {
        var missingToken = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "https://0.0.0.0:8443",
                ["Kafdeck:Deployment:AccessMode"] =
                    "Token",
                ["Kafdeck:Deployment:AccessToken"] =
                    "env:KAFDECK_DEPLOYMENT_TOKEN",
                ["Kafdeck:Observability:Prometheus:Enabled"] =
                    "true",
            });

        var missingTokenException =
            Assert.Throws<KafdeckConfigurationException>(
                () => KafdeckConfigurationValidator
                    .ValidateAndThrow(missingToken));
        Assert.Contains(
            "require a dedicated access-token secret reference",
            missingTokenException.Message,
            StringComparison.Ordinal);

        var insecure = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "http://0.0.0.0:8080",
                ["Kafdeck:Deployment:AccessMode"] =
                    "Token",
                ["Kafdeck:Deployment:AccessToken"] =
                    "env:KAFDECK_DEPLOYMENT_TOKEN",
                ["Kafdeck:Observability:Prometheus:Enabled"] =
                    "true",
                ["Kafdeck:Observability:Prometheus:AccessToken"] =
                    "env:KAFDECK_METRICS_TOKEN",
            });

        var insecureException =
            Assert.Throws<KafdeckConfigurationException>(
                () => KafdeckConfigurationValidator
                    .ValidateAndThrow(insecure));
        Assert.Contains(
            "requires HTTPS",
            insecureException.Message,
            StringComparison.Ordinal);

        var valid = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "https://0.0.0.0:8443",
                ["Kafdeck:Deployment:AccessMode"] =
                    "Token",
                ["Kafdeck:Deployment:AccessToken"] =
                    "env:KAFDECK_DEPLOYMENT_TOKEN",
                ["Kafdeck:Observability:Prometheus:Enabled"] =
                    "true",
                ["Kafdeck:Observability:Prometheus:AccessToken"] =
                    "env:KAFDECK_METRICS_TOKEN",
            });

        KafdeckConfigurationValidator
            .ValidateAndThrow(valid);
    }

    [Fact]
    public void Http_protobuf_uses_signal_specific_otlp_endpoints()
    {
        var options = new OtlpObservabilityOptions(
            true,
            "https://collector.example:4318/base",
            OtlpObservabilityProtocol.HttpProtobuf,
            null);

        var traces = new OtlpExporterOptions();
        KafdeckOpenTelemetryRegistration.ConfigureExporter(
            traces,
            options,
            resolvedHeaders: null,
            OtlpSignalKind.Traces);

        var metrics = new OtlpExporterOptions();
        KafdeckOpenTelemetryRegistration.ConfigureExporter(
            metrics,
            options,
            resolvedHeaders: null,
            OtlpSignalKind.Metrics);

        Assert.Equal(
            "https://collector.example:4318/base/v1/traces",
            traces.Endpoint.AbsoluteUri.TrimEnd('/'));
        Assert.Equal(
            "https://collector.example:4318/base/v1/metrics",
            metrics.Endpoint.AbsoluteUri.TrimEnd('/'));
        Assert.Equal(OtlpExportProtocol.HttpProtobuf, traces.Protocol);
        Assert.Equal(OtlpExportProtocol.HttpProtobuf, metrics.Protocol);
    }

    [Fact]
    public void Grpc_keeps_the_configured_base_endpoint()
    {
        var options = new OtlpObservabilityOptions(
            true,
            "https://collector.example:4317/base",
            OtlpObservabilityProtocol.Grpc,
            null);
        var exporter = new OtlpExporterOptions();

        KafdeckOpenTelemetryRegistration.ConfigureExporter(
            exporter,
            options,
            resolvedHeaders: null,
            OtlpSignalKind.Traces);

        Assert.Equal(
            "https://collector.example:4317/base",
            exporter.Endpoint.AbsoluteUri.TrimEnd('/'));
        Assert.Equal(OtlpExportProtocol.Grpc, exporter.Protocol);
    }

    [Fact]
    public void Http_protobuf_client_disables_redirects()
    {
        using var handler =
            KafdeckOpenTelemetryRegistration
                .CreateNoRedirectHttpHandler();

        Assert.False(handler.AllowAutoRedirect);
    }

    [Theory]
    [InlineData(
        "deployment-token",
        "metrics-token",
        "x-api-key=deployment-token",
        "deployment access token")]
    [InlineData(
        "deployment-token",
        "metrics-token",
        "x-api-key=metrics-token",
        "Prometheus scrape token")]
    [InlineData(
        "deployment token",
        "metrics-token",
        "x-api-key=deployment%20token",
        "deployment access token")]
    public void Resolved_otlp_header_values_must_not_reuse_local_credentials(
        string deploymentToken,
        string prometheusToken,
        string otlpHeaders,
        string expectedMessage)
    {
        var exception =
            Assert.Throws<KafdeckConfigurationException>(
                () =>
                    ObservabilityStartupSecurity
                        .ValidateResolvedCredentialIsolation(
                            deploymentToken,
                            prometheusToken,
                            otlpHeaders));

        Assert.Contains(
            expectedMessage,
            exception.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Distinct_resolved_otlp_header_value_is_admitted()
    {
        ObservabilityStartupSecurity
            .ValidateResolvedCredentialIsolation(
                "deployment-token",
                "metrics-token",
                "x-api-key=collector-token");
    }

    [Fact]
    public void Stable_otlp_runtime_registers_trace_and_metric_providers()
    {
        var observability = new ObservabilityOptions(
            100,
            PrometheusObservabilityOptions.Disabled,
            new OtlpObservabilityOptions(
                true,
                "http://127.0.0.1:4317",
                OtlpObservabilityProtocol.Grpc,
                null));

        var services = new ServiceCollection();
        services.AddKafdeckOpenTelemetry(
            observability,
            resolvedOtlpHeaders: null);

        using var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetService<TracerProvider>());
        Assert.NotNull(provider.GetService<MeterProvider>());
    }

    [Fact]
    public void Disabled_otlp_does_not_require_header_secret_resolution()
    {
        var observability = new ObservabilityOptions(
            100,
            PrometheusObservabilityOptions.Disabled,
            OtlpObservabilityOptions.Disabled);

        var services = new ServiceCollection();
        services.AddKafdeckOpenTelemetry(
            observability,
            resolvedOtlpHeaders: null);

        using var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetService<TracerProvider>());
        Assert.NotNull(provider.GetService<MeterProvider>());
    }

    [Fact]
    public void Otlp_loopback_http_and_remote_https_are_admitted()
    {
        var loopback = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "http://127.0.0.1:8080",
                ["Kafdeck:Observability:Otlp:Enabled"] =
                    "true",
                ["Kafdeck:Observability:Otlp:Endpoint"] =
                    "http://127.0.0.1:4317",
                ["Kafdeck:Observability:Otlp:Protocol"] =
                    "Grpc",
            });

        KafdeckConfigurationValidator
            .ValidateAndThrow(loopback);

        Assert.True(loopback.Observability!.Otlp.Enabled);
        Assert.Equal(
            OtlpObservabilityProtocol.Grpc,
            loopback.Observability.Otlp.Protocol);

        var remote = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "https://0.0.0.0:8443",
                ["Kafdeck:Deployment:AccessMode"] =
                    "Token",
                ["Kafdeck:Deployment:AccessToken"] =
                    "env:KAFDECK_DEPLOYMENT_TOKEN",
                ["Kafdeck:Observability:Otlp:Enabled"] =
                    "true",
                ["Kafdeck:Observability:Otlp:Endpoint"] =
                    "https://otel.internal.example:4318",
                ["Kafdeck:Observability:Otlp:Protocol"] =
                    "HttpProtobuf",
                ["Kafdeck:Observability:Otlp:Headers"] =
                    "env:KAFDECK_OTLP_HEADERS",
            });

        KafdeckConfigurationValidator
            .ValidateAndThrow(remote);

        Assert.Equal(
            OtlpObservabilityProtocol.HttpProtobuf,
            remote.Observability!.Otlp.Protocol);
    }

    [Fact]
    public void Remote_otlp_http_fails_closed()
    {
        var options = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "https://0.0.0.0:8443",
                ["Kafdeck:Deployment:AccessMode"] =
                    "Token",
                ["Kafdeck:Deployment:AccessToken"] =
                    "env:KAFDECK_DEPLOYMENT_TOKEN",
                ["Kafdeck:Observability:Otlp:Enabled"] =
                    "true",
                ["Kafdeck:Observability:Otlp:Endpoint"] =
                    "http://otel.internal.example:4317",
            });

        var exception =
            Assert.Throws<KafdeckConfigurationException>(
                () => KafdeckConfigurationValidator
                    .ValidateAndThrow(options));

        Assert.Contains(
            "Remote OTLP export requires HTTPS",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("https://user:secret@otel.example:4317")]
    [InlineData("https://otel.example:4317?token=secret")]
    [InlineData("https://otel.example:4317#fragment")]
    public void Otlp_endpoint_rejects_credential_query_and_fragment_components(
        string endpoint)
    {
        var options = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "http://127.0.0.1:8080",
                ["Kafdeck:Observability:Otlp:Enabled"] =
                    "true",
                ["Kafdeck:Observability:Otlp:Endpoint"] =
                    endpoint,
            });

        var exception =
            Assert.Throws<KafdeckConfigurationException>(
                () => KafdeckConfigurationValidator
                    .ValidateAndThrow(options));

        Assert.Contains(
            "must not contain user-info, query-string, or fragment",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Otlp_headers_must_not_reuse_deployment_or_prometheus_secret()
    {
        var deploymentReuse = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "https://0.0.0.0:8443",
                ["Kafdeck:Deployment:AccessMode"] =
                    "Token",
                ["Kafdeck:Deployment:AccessToken"] =
                    "env:SHARED_TOKEN",
                ["Kafdeck:Observability:Otlp:Enabled"] =
                    "true",
                ["Kafdeck:Observability:Otlp:Endpoint"] =
                    "https://otel.example:4317",
                ["Kafdeck:Observability:Otlp:Headers"] =
                    "env:SHARED_TOKEN",
            });

        var deploymentException =
            Assert.Throws<KafdeckConfigurationException>(
                () => KafdeckConfigurationValidator
                    .ValidateAndThrow(deploymentReuse));

        Assert.Contains(
            "distinct secret reference from the deployment access token",
            deploymentException.Message,
            StringComparison.Ordinal);

        var prometheusReuse = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "https://0.0.0.0:8443",
                ["Kafdeck:Deployment:AccessMode"] =
                    "Token",
                ["Kafdeck:Deployment:AccessToken"] =
                    "env:KAFDECK_DEPLOYMENT_TOKEN",
                ["Kafdeck:Observability:Prometheus:Enabled"] =
                    "true",
                ["Kafdeck:Observability:Prometheus:AccessToken"] =
                    "env:SHARED_METRICS_SECRET",
                ["Kafdeck:Observability:Otlp:Enabled"] =
                    "true",
                ["Kafdeck:Observability:Otlp:Endpoint"] =
                    "https://otel.example:4317",
                ["Kafdeck:Observability:Otlp:Headers"] =
                    "env:SHARED_METRICS_SECRET",
            });

        var prometheusException =
            Assert.Throws<KafdeckConfigurationException>(
                () => KafdeckConfigurationValidator
                    .ValidateAndThrow(prometheusReuse));

        Assert.Contains(
            "distinct secret reference from the Prometheus scrape token",
            prometheusException.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Disabled_otlp_rejects_dangling_endpoint_or_headers()
    {
        var endpoint = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "http://127.0.0.1:8080",
                ["Kafdeck:Observability:Otlp:Endpoint"] =
                    "http://127.0.0.1:4317",
            });

        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator
                .ValidateAndThrow(endpoint));

        var headers = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "http://127.0.0.1:8080",
                ["Kafdeck:Observability:Otlp:Headers"] =
                    "env:KAFDECK_OTLP_HEADERS",
            });

        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator
                .ValidateAndThrow(headers));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(50001)]
    public void Active_series_cap_plus_invalid_values_fail_closed(
        int value)
    {
        var options = new KafdeckOptions(
            new DeploymentOptions(
                "http://127.0.0.1:8080",
                null),
            Array.Empty<ClusterProfile>(),
            Observability:
                new ObservabilityOptions(
                    value,
                    PrometheusObservabilityOptions.Disabled));

        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator
                .ValidateAndThrow(options));
    }

    [Theory]
    [InlineData("MaxMetricLabelsPerSeries", "2")]
    [InlineData("MaxMetricLabelsPerSeries", "13")]
    [InlineData("MaxMetricLabelValueBytes", "4")]
    [InlineData("MaxMetricLabelValueBytes", "129")]
    [InlineData("MaxTraceAttributes", "0")]
    [InlineData("MaxTraceAttributes", "49")]
    public void Metric_and_trace_budget_invalid_values_fail_closed(
        string key,
        string value)
    {
        var options = Load(
            new Dictionary<string, string?>
            {
                [$"Kafdeck:Observability:{key}"] = value,
            });

        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator
                .ValidateAndThrow(options));
    }

    [Fact]
    public void Resolved_metrics_credential_must_not_equal_deployment_credential()
    {
        var exception =
            Assert.Throws<KafdeckConfigurationException>(
                () =>
                    ObservabilityStartupSecurity
                        .ValidateResolvedCredentialIsolation(
                            "same-resolved-value",
                            "same-resolved-value"));

        Assert.Contains(
            "distinct from the deployment access token",
            exception.Message,
            StringComparison.Ordinal);

        ObservabilityStartupSecurity
            .ValidateResolvedCredentialIsolation(
                "deployment-value",
                "metrics-value");
        ObservabilityStartupSecurity
            .ValidateResolvedCredentialIsolation(
                null,
                "metrics-value");
    }

    [Fact]
    public void Rejected_series_are_not_emitted_to_meter_listeners()
    {
        var observedRoutes = new List<string>();

        using var listener = new MeterListener();
        listener.InstrumentPublished =
            (instrument, activeListener) =>
            {
                if (string.Equals(
                        instrument.Meter.Name,
                        ApiTelemetry.InstrumentationName,
                        StringComparison.Ordinal) &&
                    (string.Equals(
                         instrument.Name,
                         "kafdeck.api.requests",
                         StringComparison.Ordinal) ||
                     string.Equals(
                         instrument.Name,
                         "kafdeck.api.request.duration",
                         StringComparison.Ordinal)))
                {
                    activeListener.EnableMeasurementEvents(
                        instrument);
                }
            };

        listener.SetMeasurementEventCallback<long>(
            (_, _, tags, _) =>
            {
                var route = RouteTag(tags);
                if (route is not null)
                {
                    observedRoutes.Add(route);
                }
            });
        listener.SetMeasurementEventCallback<double>(
            (_, _, tags, _) =>
            {
                var route = RouteTag(tags);
                if (route is not null)
                {
                    observedRoutes.Add(route);
                }
            });
        listener.Start();

        using var telemetry =
            new ApiTelemetry(maxActiveSeries: 8);

        telemetry.RecordRequest(
            "route-one",
            "GET",
            200,
            1.0);
        telemetry.RecordRequest(
            "route-two",
            "POST",
            500,
            2.0);
        telemetry.RecordRequest(
            "route-three",
            "GET",
            200,
            3.0);

        Assert.Contains(
            "route-one",
            observedRoutes);
        Assert.Contains(
            "route-two",
            observedRoutes);
        Assert.DoesNotContain(
            "route-three",
            observedRoutes);
        Assert.Equal(
            3,
            telemetry.Snapshot().DroppedSeries);
    }

    [Fact]
    public void Api_telemetry_enforces_series_cap_without_unbounded_growth()
    {
        using var telemetry =
            new ApiTelemetry(maxActiveSeries: 8);

        telemetry.RecordRequest(
            "route-one",
            "GET",
            200,
            1.25);
        telemetry.RecordRequest(
            "route-two",
            "POST",
            500,
            2.5);
        telemetry.RecordRequest(
            "route-three",
            "DELETE",
            403,
            3.75);
        telemetry.RecordRequest(
            "route-one",
            "GET",
            200,
            1.0);

        var snapshot = telemetry.Snapshot();

        Assert.Equal(8, snapshot.ActiveSeries);
        Assert.Equal(8, snapshot.MaxActiveSeries);
        Assert.Equal(3, snapshot.DroppedSeries);

        var first = snapshot.Series.Single(
            item => item.Route == "route-one");
        Assert.Equal(2, first.RequestCount);
        Assert.Equal(
            2.25,
            first.DurationSumMilliseconds,
            precision: 6);
    }

    [Fact]
    public void Meter_measurements_use_the_admitted_label_value_budget()
    {
        var observedRoutes = new List<string>();

        using var listener = new MeterListener();
        listener.InstrumentPublished =
            (instrument, activeListener) =>
            {
                if (string.Equals(
                        instrument.Meter.Name,
                        ApiTelemetry.InstrumentationName,
                        StringComparison.Ordinal) &&
                    string.Equals(
                        instrument.Name,
                        "kafdeck.api.requests",
                        StringComparison.Ordinal))
                {
                    activeListener.EnableMeasurementEvents(
                        instrument);
                }
            };
        listener.SetMeasurementEventCallback<long>(
            (_, _, tags, _) =>
            {
                var route = RouteTag(tags);
                if (route is not null)
                {
                    observedRoutes.Add(route);
                }
            });
        listener.Start();

        using var telemetry =
            new ApiTelemetry(
                maxActiveSeries: 10,
                maxMetricLabelValueBytes: 5,
                maxTraceAttributes: 3);

        telemetry.RecordRequest(
            "route-name-longer-than-five-bytes",
            "GET",
            200,
            1.0);

        Assert.Contains(
            "other",
            observedRoutes);
        Assert.DoesNotContain(
            "route-name-longer-than-five-bytes",
            observedRoutes);
    }

    [Fact]
    public void Trace_attributes_respect_the_configured_budget()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source =>
                string.Equals(
                    source.Name,
                    ApiTelemetry.InstrumentationName,
                    StringComparison.Ordinal),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);

        using var telemetry =
            new ApiTelemetry(
                maxActiveSeries: 10,
                maxMetricLabelValueBytes: 64,
                maxTraceAttributes: 1);
        using var activity =
            telemetry.StartRequest(
                "v08-observability-capabilities",
                "GET");

        Assert.NotNull(activity);
        telemetry.SetResponseStatus(activity, 200);

        var tags = activity!.Tags.ToArray();
        Assert.Single(tags);
        Assert.Equal(
            "kafdeck.route",
            tags[0].Key);
        Assert.DoesNotContain(
            tags,
            tag => string.Equals(
                tag.Key,
                "http.request.method",
                StringComparison.Ordinal));
        Assert.DoesNotContain(
            tags,
            tag => string.Equals(
                tag.Key,
                "http.response.status_code",
                StringComparison.Ordinal));
    }

    [Fact]
    public void Route_and_method_dimensions_fail_to_bounded_categories()
    {
        Assert.Equal(
            "v08-observability-capabilities",
            ApiTelemetry.NormalizeRouteName(
                "v08-observability-capabilities"));
        Assert.Equal(
            "other",
            ApiTelemetry.NormalizeRouteName(
                "topics/{user-controlled}"));
        Assert.Equal(
            "other",
            ApiTelemetry.NormalizeRouteName(
                new string('a', 129)));

        Assert.Equal(
            "OTHER",
            ApiTelemetry.NormalizeMethod("patch"));
        Assert.Equal(
            "OTHER",
            ApiTelemetry.NormalizeMethod("CUSTOM"));
    }

    [Fact]
    public void Prometheus_text_contains_only_safe_bounded_dimensions()
    {
        using var telemetry =
            new ApiTelemetry(maxActiveSeries: 10);

        telemetry.RecordRequest(
            "v08-observability-capabilities",
            "GET",
            200,
            12.5);

        var text = telemetry.RenderPrometheus();

        Assert.Contains(
            "kafdeck_api_requests_total",
            text,
            StringComparison.Ordinal);
        Assert.Contains(
            "route=\"v08-observability-capabilities\"",
            text,
            StringComparison.Ordinal);
        Assert.Contains(
            "status_class=\"2xx\"",
            text,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "authorization",
            text,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Metrics_token_middleware_never_echoes_tokens()
    {
        const string expected =
            "expected-metrics-token";
        const string supplied =
            "wrong-metrics-token";
        var nextInvoked = false;

        var middleware =
            new PrometheusScrapeTokenMiddleware(
                _ =>
                {
                    nextInvoked = true;
                    return Task.CompletedTask;
                },
                expected);

        var context = new DefaultHttpContext();
        context.Response.Body =
            new MemoryStream();
        context.Request.Headers[
            PrometheusScrapeTokenMiddleware.HeaderName] =
            supplied;

        var audit =
            new CapturingAuditSink();

        await middleware.InvokeAsync(
            context,
            audit);

        Assert.False(nextInvoked);
        Assert.Equal(
            StatusCodes.Status401Unauthorized,
            context.Response.StatusCode);
        Assert.Contains(
            audit.Events,
            item =>
                item.EventType ==
                    SecurityAuditEventType
                        .MetricsScrapeRequest &&
                item.Outcome ==
                    SecurityAuditOutcome.Denied);

        context.Response.Body.Position = 0;
        using var reader =
            new StreamReader(
                context.Response.Body);
        var body =
            await reader.ReadToEndAsync();

        Assert.DoesNotContain(
            expected,
            body,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            supplied,
            body,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Runtime_mapping_exposes_prometheus_only_when_enabled()
    {
        var disabled = new KafdeckOptions(
            new DeploymentOptions(
                "http://127.0.0.1:8080",
                null),
            Array.Empty<ClusterProfile>());

        var disabledRoutes =
            Routes(disabled);
        Assert.True(
            KafdeckObservabilityEndpoints
                .IsPrometheusScrapePath(
                    new PathString("/metrics")));
        Assert.True(
            KafdeckObservabilityEndpoints
                .IsPrometheusScrapePath(
                    new PathString("/METRICS")));
        Assert.True(
            KafdeckObservabilityEndpoints
                .IsPrometheusScrapePath(
                    new PathString("/metrics/")));
        Assert.True(
            KafdeckObservabilityEndpoints
                .IsPrometheusScrapePath(
                    new PathString("/METRICS/")));
        Assert.False(
            KafdeckObservabilityEndpoints
                .IsPrometheusScrapePath(
                    new PathString("/metrics//")));
        Assert.False(
            KafdeckObservabilityEndpoints
                .IsPrometheusScrapePath(
                    new PathString("/metrics/extra")));

        Assert.Contains(
            KafdeckObservabilityEndpoints.CapabilitiesRoute,
            disabledRoutes);
        Assert.DoesNotContain(
            PrometheusObservabilityOptions.Path,
            disabledRoutes);

        var enabled = disabled with
        {
            Observability =
                new ObservabilityOptions(
                    100,
                    new PrometheusObservabilityOptions(
                        true,
                        null)),
        };

        var enabledRoutes =
            Routes(enabled);
        Assert.Contains(
            PrometheusObservabilityOptions.Path,
            enabledRoutes);
    }

    [Fact]
    public void Safe_diagnostics_never_expose_otlp_endpoint_or_header_reference()
    {
        const string headerVariable =
            "KAFDECK_OTLP_SECRET_HEADERS";
        const string endpointHost =
            "collector.secret.internal.example";

        var options = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "http://127.0.0.1:8080",
                ["Kafdeck:Observability:Otlp:Enabled"] =
                    "true",
                ["Kafdeck:Observability:Otlp:Endpoint"] =
                    $"https://{endpointHost}:4318",
                ["Kafdeck:Observability:Otlp:Protocol"] =
                    "HttpProtobuf",
                ["Kafdeck:Observability:Otlp:Headers"] =
                    $"env:{headerVariable}",
            });

        KafdeckConfigurationValidator
            .ValidateAndThrow(options);

        var json =
            System.Text.Json.JsonSerializer.Serialize(
                SafeConfigurationDiagnostics.Create(
                    options));

        Assert.Contains(
            "\"otlpEnabled\":true",
            json,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "\"otlpHeadersConfigured\":true",
            json,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            headerVariable,
            json,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            endpointHost,
            json,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Safe_diagnostics_never_expose_metrics_token_reference()
    {
        const string variable =
            "KAFDECK_METRICS_TOKEN";

        var options = new KafdeckOptions(
            new DeploymentOptions(
                "https://0.0.0.0:8443",
                SecretReference.Parse(
                    "env:KAFDECK_DEPLOYMENT_TOKEN")),
            Array.Empty<ClusterProfile>(),
            Observability:
                new ObservabilityOptions(
                    10000,
                    new PrometheusObservabilityOptions(
                        true,
                        SecretReference.Parse(
                            $"env:{variable}"))));

        var json =
            System.Text.Json.JsonSerializer.Serialize(
                SafeConfigurationDiagnostics.Create(
                    options));

        Assert.DoesNotContain(
            variable,
            json,
            StringComparison.Ordinal);
        Assert.Contains(
            "\"prometheusEnabled\":true",
            json,
            StringComparison.OrdinalIgnoreCase);
    }

    private static string? RouteTag(
        ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(
                    tag.Key,
                    "kafdeck.route",
                    StringComparison.Ordinal))
            {
                return tag.Value?.ToString();
            }
        }

        return null;
    }

    private static string[] Routes(
        KafdeckOptions options)
    {
        var builder =
            WebApplication.CreateBuilder();
        builder.Services.AddSingleton(
            new ApiTelemetry(options));

        var app = builder.Build();
        app.MapKafdeckV08Observability(
            options);

        return ((IEndpointRouteBuilder)app)
            .DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .Select(endpoint =>
                endpoint.RoutePattern.RawText ??
                string.Empty)
            .ToArray();
    }

    private static KafdeckOptions Load(
        IReadOnlyDictionary<string, string?>
            values) =>
        KafdeckConfigurationLoader.Load(
            new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build());

    private sealed class CapturingAuditSink
        : ISecurityAuditSink
    {
        public List<SecurityAuditEvent> Events { get; } =
            new();

        public ValueTask WriteAsync(
            SecurityAuditEvent auditEvent,
            CancellationToken cancellationToken =
                default)
        {
            Events.Add(auditEvent);
            return ValueTask.CompletedTask;
        }
    }
}
