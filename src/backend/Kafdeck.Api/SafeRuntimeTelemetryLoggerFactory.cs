using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using OpenTelemetry.Logs;
using OpenTelemetry.Resources;

namespace Kafdeck.Api;

public sealed class SafeRuntimeTelemetryLoggerFactory : IDisposable
{
    private readonly ILoggerFactory _factory;
    private readonly bool _ownsFactory;

    public SafeRuntimeTelemetryLoggerFactory(
        ObservabilityOptions observability,
        string? resolvedOtlpHeaders)
    {
        ArgumentNullException.ThrowIfNull(observability);

        if (!observability.Otlp.Enabled)
        {
            _factory = NullLoggerFactory.Instance;
            _ownsFactory = false;
            return;
        }

        _factory = LoggerFactory.Create(
            builder =>
            {
                builder.SetMinimumLevel(
                    LogLevel.Information);
                builder.AddOpenTelemetry(
                    options =>
                    {
                        // Only RuntimeTelemetry writes through this isolated
                        // logger factory. Host/application logs never enter
                        // this exporter, avoiding accidental provider/request
                        // body, exception or secret export.
                        options.IncludeFormattedMessage = false;
                        options.IncludeScopes = false;
                        options.ParseStateValues = true;
                        options.SetResourceBuilder(
                            ResourceBuilder
                                .CreateDefault()
                                .AddService("Kafdeck"));
                        options.AddOtlpExporter(
                            exporter =>
                                KafdeckOpenTelemetryRegistration
                                    .ConfigureExporter(
                                        exporter,
                                        observability.Otlp,
                                        resolvedOtlpHeaders,
                                        OtlpSignalKind.Logs));
                    });
            });
        _ownsFactory = true;
    }

    public ILogger CreateLogger() =>
        _factory.CreateLogger(
            RuntimeTelemetry.LogCategory);

    public void Dispose()
    {
        if (_ownsFactory)
        {
            _factory.Dispose();
        }
    }
}
