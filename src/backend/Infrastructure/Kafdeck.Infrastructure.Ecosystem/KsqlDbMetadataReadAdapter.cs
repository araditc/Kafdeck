using System.Text.Json;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Ecosystem;

public sealed class KsqlDbMetadataReadAdapter : IKsqlMetadataReadPort, IDisposable
{
    private const int DefaultMaxConcurrencyPerCluster = 4;
    private readonly IReadOnlyDictionary<string, HttpReadRuntime> _runtimes;
    private readonly IReadOnlyDictionary<string, SemaphoreSlim> _gates;
    private readonly TimeProvider _timeProvider;

    public KsqlDbMetadataReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
        : this(
            clusterProfiles,
            secretResolver,
            static _ => new HttpClientHandler { AllowAutoRedirect = false },
            timeProvider)
    {
    }

    internal KsqlDbMetadataReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        Func<ClusterProfile, HttpMessageHandler> handlerFactory,
        TimeProvider? timeProvider = null,
        int maxConcurrencyPerCluster = DefaultMaxConcurrencyPerCluster)
    {
        ArgumentNullException.ThrowIfNull(clusterProfiles);
        ArgumentNullException.ThrowIfNull(secretResolver);
        ArgumentNullException.ThrowIfNull(handlerFactory);

        if (maxConcurrencyPerCluster is < 1 or > 64)
        {
            throw new ArgumentOutOfRangeException(nameof(maxConcurrencyPerCluster));
        }

        _timeProvider = timeProvider ?? TimeProvider.System;
        _runtimes = clusterProfiles
            .Where(profile => profile.KsqlDb is not null)
            .ToDictionary(
                profile => profile.Id,
                profile =>
                {
                    var ksql = profile.KsqlDb!;
                    return ReadOnlyHttpSupport.CreateRuntime(
                        ksql.Url,
                        ksql.Username,
                        ksql.Password,
                        secretResolver,
                        handlerFactory(profile));
                },
                StringComparer.Ordinal);

        _gates = _runtimes.Keys.ToDictionary(
            clusterId => clusterId,
            _ => new SemaphoreSlim(maxConcurrencyPerCluster, maxConcurrencyPerCluster),
            StringComparer.Ordinal);
    }

    public Task<ReadViewResult<KsqlServerInfo>> GetServerInfoAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        ExecuteAsync(
            clusterId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var info = await ReadOnlyHttpSupport.GetJsonAsync(
                        runtime,
                        "info",
                        operation.MaxResponseBytes,
                        token)
                    .ConfigureAwait(false);

                if (info.ValueKind != JsonValueKind.Object ||
                    !info.TryGetProperty("KsqlServerInfo", out var server) ||
                    server.ValueKind != JsonValueKind.Object)
                {
                    throw new JsonException("ksqlDB info response is invalid.");
                }

                string? state = null;
                try
                {
                    var health = await ReadOnlyHttpSupport.GetJsonAsync(
                            runtime,
                            "healthcheck",
                            operation.MaxResponseBytes,
                            token)
                        .ConfigureAwait(false);

                    if (health.ValueKind == JsonValueKind.Object &&
                        health.TryGetProperty("isHealthy", out var isHealthy) &&
                        isHealthy.ValueKind is JsonValueKind.True or JsonValueKind.False)
                    {
                        state = isHealthy.GetBoolean() ? "Healthy" : "Unhealthy";
                    }
                }
                catch (ReadViewHttpException)
                {
                    // Server identity is still a valid bounded observation when healthcheck
                    // is unsupported or separately unavailable.
                    state = null;
                }

                return new KsqlServerInfo(
                    GetOptionalString(server, "version"),
                    GetOptionalString(server, "kafkaClusterId"),
                    state);
            });

    public Task<ReadViewResult<IReadOnlyList<KsqlMetadataItem>>> ListMetadataAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        if (!_runtimes.ContainsKey(clusterId))
        {
            return Task.FromResult(Failed<IReadOnlyList<KsqlMetadataItem>>(
                ReadViewFailureCategory.NotConfigured,
                "ksql_not_configured",
                "ksqlDB is not configured for the requested cluster.",
                false));
        }

        return Task.FromResult(Failed<IReadOnlyList<KsqlMetadataItem>>(
            ReadViewFailureCategory.Unsupported,
            "ksql_metadata_requires_statement_execution",
            "This ksqlDB deployment does not expose a genuine read-only metadata endpoint supported by Kafdeck v0.4.",
            false));
    }

    public void Dispose()
    {
        foreach (var runtime in _runtimes.Values)
        {
            runtime.Client.Dispose();
        }

        foreach (var gate in _gates.Values)
        {
            gate.Dispose();
        }
    }

    private async Task<ReadViewResult<T>> ExecuteAsync<T>(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken,
        Func<HttpReadRuntime, CancellationToken, Task<T>> action)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        if (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(ReadViewFailureCategory.Cancelled, "operation_cancelled", "ksqlDB read operation was cancelled.", false);
        }

        if (operation.DeadlineUtc <= _timeProvider.GetUtcNow())
        {
            return Failed<T>(ReadViewFailureCategory.Timeout, "deadline_exceeded", "ksqlDB read operation exceeded its deadline.", true);
        }

        if (!_runtimes.TryGetValue(clusterId, out var runtime) ||
            !_gates.TryGetValue(clusterId, out var gate))
        {
            return Failed<T>(
                ReadViewFailureCategory.NotConfigured,
                "ksql_not_configured",
                "ksqlDB is not configured for the requested cluster.",
                false);
        }

        var gateAcquired = false;
        try
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var remaining = operation.DeadlineUtc - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                return Failed<T>(ReadViewFailureCategory.Timeout, "deadline_exceeded", "ksqlDB read operation exceeded its deadline.", true);
            }

            deadline.CancelAfter(remaining);
            await gate.WaitAsync(deadline.Token).ConfigureAwait(false);
            gateAcquired = true;

            var value = await action(runtime, deadline.Token).ConfigureAwait(false);
            return ReadViewResult<T>.Success(value);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(ReadViewFailureCategory.Cancelled, "operation_cancelled", "ksqlDB read operation was cancelled.", false);
        }
        catch (OperationCanceledException)
        {
            return Failed<T>(ReadViewFailureCategory.Timeout, "deadline_exceeded", "ksqlDB read operation exceeded its deadline.", true);
        }
        catch (ReadViewHttpException exception)
        {
            return Failed<T>(exception.Category, exception.Code, exception.SafeMessage, exception.Retryable);
        }
        catch (ResponseBoundExceededException)
        {
            return Failed<T>(ReadViewFailureCategory.ResponseTooLarge, "ksql_response_too_large", "ksqlDB response exceeded the configured bound.", false);
        }
        catch (HttpRequestException)
        {
            return Failed<T>(ReadViewFailureCategory.Unavailable, "ksql_unavailable", "ksqlDB is temporarily unavailable.", true);
        }
        catch (JsonException)
        {
            return Failed<T>(ReadViewFailureCategory.InvalidResponse, "invalid_ksql_response", "ksqlDB returned an invalid response.", false);
        }
        finally
        {
            if (gateAcquired)
            {
                gate.Release();
            }
        }
    }

    private static string? GetOptionalString(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var value) || value.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        return value.ValueKind == JsonValueKind.String ? value.GetString() : value.ToString();
    }

    private static ReadViewResult<T> Failed<T>(
        ReadViewFailureCategory category,
        string code,
        string message,
        bool retryable) =>
        ReadViewResult<T>.Failed(new ReadViewFailure(category, code, message, retryable));
}
