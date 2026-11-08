using System.Net.Http.Headers;
using System.Net.Http.Json;
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
    private readonly Func<string, HttpClient?>? _registeredClient;
    private readonly SemaphoreSlim _registeredGate = new(DefaultMaxConcurrencyPerCluster, DefaultMaxConcurrencyPerCluster);

    public KsqlDbMetadataReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null,
        Func<string, HttpClient?>? registeredClient = null)
        : this(
            clusterProfiles,
            secretResolver,
            static _ => new HttpClientHandler { AllowAutoRedirect = false },
            timeProvider)
    {
        _registeredClient = registeredClient;
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

        if (_runtimes.ContainsKey(clusterId))
        {
            return Task.FromResult(Failed<IReadOnlyList<KsqlMetadataItem>>(
                ReadViewFailureCategory.Unsupported,
                "ksql_metadata_requires_statement_execution",
                "The configured ksqlDB profile does not admit statement-based metadata discovery.",
                false));
        }

        return ExecuteAsync<IReadOnlyList<KsqlMetadataItem>>(
            clusterId, operation, cancellationToken, async (runtime, token) =>
            {
                var items = new List<KsqlMetadataItem>();
                await AppendMetadataAsync(runtime, "SHOW STREAMS;", "streams", "stream", items, operation, token)
                    .ConfigureAwait(false);
                await AppendMetadataAsync(runtime, "SHOW TABLES;", "tables", "table", items, operation, token)
                    .ConfigureAwait(false);
                return items;
            });
    }

    private static async Task AppendMetadataAsync(
        HttpReadRuntime runtime, string statement, string collection, string kind, List<KsqlMetadataItem> items,
        ReadViewOperationContext operation, CancellationToken token)
    {
        const string mediaType = "application/vnd.ksql.v1+json";
        using var request = new HttpRequestMessage(HttpMethod.Post, "ksql")
        {
            Content = JsonContent.Create(new { ksql = statement, streamsProperties = new Dictionary<string, string>() },
                mediaType: new MediaTypeHeaderValue(mediaType)),
        };
        request.Headers.Accept.ParseAdd(mediaType);
        var response = await ReadOnlyHttpSupport.SendJsonAsync(runtime, request, operation.MaxResponseBytes, token)
            .ConfigureAwait(false);
        if (response.ValueKind != JsonValueKind.Array || response.GetArrayLength() != 1 ||
            response[0].ValueKind != JsonValueKind.Object || !response[0].TryGetProperty(collection, out var entries) ||
            entries.ValueKind != JsonValueKind.Array)
            throw new JsonException("ksqlDB metadata response is invalid.");
        if (entries.GetArrayLength() > operation.MaxItems - items.Count)
            throw new ResponseBoundExceededException();

        foreach (var entry in entries.EnumerateArray())
        {
            if (entry.ValueKind != JsonValueKind.Object || GetOptionalString(entry, "name") is not { Length: > 0 } name)
                throw new JsonException("ksqlDB metadata entry is invalid.");
            items.Add(new KsqlMetadataItem(kind, name, GetOptionalString(entry, "topic"),
                GetOptionalString(entry, "valueFormat") ?? GetOptionalString(entry, "format"),
                GetOptionalString(entry, "keyFormat")));
        }
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
        _registeredGate.Dispose();
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

        using var client = _runtimes.ContainsKey(clusterId) ? null : _registeredClient?.Invoke(clusterId);
        if (!_runtimes.TryGetValue(clusterId, out var runtime) && client is null)
        {
            return Failed<T>(
                ReadViewFailureCategory.NotConfigured,
                "ksql_not_configured",
                "ksqlDB is not configured for the requested cluster.",
                false);
        }
        runtime ??= new HttpReadRuntime(client!, null, IsRegistered: true);
        var gate = runtime.IsRegistered ? _registeredGate : _gates[clusterId];

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
