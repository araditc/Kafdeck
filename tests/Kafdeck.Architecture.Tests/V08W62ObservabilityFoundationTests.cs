using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Security;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
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
        Assert.False(observability.Prometheus.Enabled);
        Assert.Null(observability.Prometheus.AccessToken);

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
    public void Prometheus_local_scrape_can_be_enabled_without_a_token()
    {
        var options = Load(
            new Dictionary<string, string?>
            {
                ["Kafdeck:Deployment:ListenUrl"] =
                    "http://127.0.0.1:8080",
                ["Kafdeck:Observability:Prometheus:Enabled"] =
                    "true",
                ["Kafdeck:Observability:MaxActiveSeries"] =
                    "10000",
            });

        KafdeckConfigurationValidator
            .ValidateAndThrow(options);

        Assert.True(
            options.Observability!.Prometheus.Enabled);
        Assert.Null(
            options.Observability.Prometheus.AccessToken);
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
            "requires a dedicated access-token",
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
        Assert.Equal(1, snapshot.DroppedSeries);

        var first = snapshot.Series.Single(
            item => item.Route == "route-one");
        Assert.Equal(2, first.RequestCount);
        Assert.Equal(
            2.25,
            first.DurationSumMilliseconds,
            precision: 6);
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
