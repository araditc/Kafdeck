using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.SchemaRegistry;

public sealed class SchemaRegistryCacheOptions
{
    public const int DefaultMaxEntries = 256;
    public const int HardMaxEntries = 2_048;
    public static readonly TimeSpan DefaultTtl = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan HardMaxTtl = TimeSpan.FromHours(1);

    public SchemaRegistryCacheOptions(
        int maxEntries = DefaultMaxEntries,
        TimeSpan? ttl = null)
    {
        if (maxEntries < 1 || maxEntries > HardMaxEntries)
        {
            throw new ArgumentOutOfRangeException(nameof(maxEntries));
        }

        var actualTtl = ttl ?? DefaultTtl;
        if (actualTtl <= TimeSpan.Zero || actualTtl > HardMaxTtl)
        {
            throw new ArgumentOutOfRangeException(nameof(ttl));
        }

        MaxEntries = maxEntries;
        Ttl = actualTtl;
    }

    public int MaxEntries { get; }

    public TimeSpan Ttl { get; }
}

public sealed class ConfluentSchemaRegistryReadAdapter : IRecordSchemaReadPort, IDisposable
{
    private const int MaxResponseBytes = 4 * 1024 * 1024;

    private readonly IReadOnlyDictionary<string, RegistryRuntime> _registries;
    private readonly ConcurrentDictionary<SchemaCacheKey, SchemaCacheEntry> _cache = new();
    private readonly SchemaRegistryCacheOptions _cacheOptions;
    private readonly TimeProvider _timeProvider;
    private readonly object _cacheTrimLock = new();

    public ConfluentSchemaRegistryReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        SchemaRegistryCacheOptions? cacheOptions = null,
        TimeProvider? timeProvider = null)
        : this(
            clusterProfiles,
            secretResolver,
            static _ => new HttpClientHandler(),
            cacheOptions,
            timeProvider)
    {
    }

    internal ConfluentSchemaRegistryReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        Func<ClusterProfile, HttpMessageHandler> handlerFactory,
        SchemaRegistryCacheOptions? cacheOptions = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(clusterProfiles);
        ArgumentNullException.ThrowIfNull(secretResolver);
        ArgumentNullException.ThrowIfNull(handlerFactory);

        _cacheOptions = cacheOptions ?? new SchemaRegistryCacheOptions();
        _timeProvider = timeProvider ?? TimeProvider.System;

        _registries = clusterProfiles
            .Where(profile => profile.SchemaRegistry is not null)
            .ToDictionary(
                profile => profile.Id,
                profile => CreateRuntime(profile, secretResolver, handlerFactory(profile)),
                StringComparer.Ordinal);
    }

    public async Task<RecordSchemaResult<RecordSchemaDocument>> GetSchemaByIdAsync(
        string clusterId,
        int schemaId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        if (schemaId <= 0)
        {
            return Failed(
                RecordSchemaFailureCategory.InvalidResponse,
                "invalid_schema_id",
                "Schema ID must be a positive integer.",
                false);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return Cancelled();
        }

        var now = _timeProvider.GetUtcNow();
        if (operation.IsExpired(now))
        {
            return Timeout();
        }

        if (!_registries.TryGetValue(clusterId, out var runtime))
        {
            return Failed(
                RecordSchemaFailureCategory.RegistryNotConfigured,
                "schema_registry_not_configured",
                "Schema Registry is not configured for the requested cluster.",
                false);
        }

        var cacheKey = new SchemaCacheKey(clusterId, schemaId);
        if (_cache.TryGetValue(cacheKey, out var cached) && cached.ExpiresAtUtc > now)
        {
            return RecordSchemaResult<RecordSchemaDocument>.Success(cached.Document);
        }

        try
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var remaining = operation.Remaining(_timeProvider.GetUtcNow());
            if (remaining <= TimeSpan.Zero)
            {
                return Timeout();
            }

            deadline.CancelAfter(remaining);

            var response = await GetSchemaResponseAsync(
                    runtime,
                    schemaId,
                    format: null,
                    deadline.Token)
                .ConfigureAwait(false);

            if (!response.IsSuccess)
            {
                return response;
            }

            var document = response.Value!;
            if (document.Format == RecordSchemaFormat.Avro &&
                document.References.Count > 0)
            {
                var resolved = await GetSchemaResponseAsync(
                        runtime,
                        schemaId,
                        "resolved",
                        deadline.Token)
                    .ConfigureAwait(false);

                if (resolved.IsSuccess)
                {
                    document = resolved.Value!;
                }
                else
                {
                    return resolved;
                }
            }

            AddCache(cacheKey, document);
            return RecordSchemaResult<RecordSchemaDocument>.Success(document);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return Cancelled();
        }
        catch (OperationCanceledException)
        {
            return Timeout();
        }
        catch (HttpRequestException)
        {
            return Failed(
                RecordSchemaFailureCategory.Unavailable,
                "schema_registry_unavailable",
                "Schema Registry is temporarily unavailable.",
                true);
        }
        catch (JsonException)
        {
            return Failed(
                RecordSchemaFailureCategory.InvalidResponse,
                "invalid_schema_registry_response",
                "Schema Registry returned an invalid response.",
                false);
        }
        catch (InvalidOperationException)
        {
            return Failed(
                RecordSchemaFailureCategory.InvalidResponse,
                "invalid_schema_registry_response",
                "Schema Registry returned an invalid response.",
                false);
        }
    }

    public void Dispose()
    {
        foreach (var runtime in _registries.Values)
        {
            runtime.Client.Dispose();
        }

        _cache.Clear();
    }

    private async Task<RecordSchemaResult<RecordSchemaDocument>> GetSchemaResponseAsync(
        RegistryRuntime runtime,
        int schemaId,
        string? format,
        CancellationToken cancellationToken)
    {
        var relative = $"schemas/ids/{schemaId}";
        if (!string.IsNullOrEmpty(format))
        {
            relative += $"?format={Uri.EscapeDataString(format)}";
        }

        using var request = new HttpRequestMessage(HttpMethod.Get, relative);
        request.Headers.Accept.ParseAdd("application/vnd.schemaregistry.v1+json");

        if (runtime.BasicAuthorization is not null)
        {
            request.Headers.Authorization = runtime.BasicAuthorization;
        }

        using var response = await runtime.Client
            .SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return Failed(
                RecordSchemaFailureCategory.SchemaNotFound,
                "schema_not_found",
                "Schema Registry did not contain the requested schema.",
                false);
        }

        if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
        {
            return Failed(
                RecordSchemaFailureCategory.Unauthorized,
                "schema_registry_authorization_denied",
                "Schema Registry denied the requested operation.",
                false);
        }

        if ((int)response.StatusCode >= 500)
        {
            return Failed(
                RecordSchemaFailureCategory.Unavailable,
                "schema_registry_unavailable",
                "Schema Registry is temporarily unavailable.",
                true);
        }

        if (!response.IsSuccessStatusCode)
        {
            return Failed(
                RecordSchemaFailureCategory.InvalidResponse,
                "schema_registry_request_failed",
                "Schema Registry rejected the requested operation.",
                false);
        }

        var bytes = await ReadBoundedAsync(
                response.Content,
                MaxResponseBytes,
                cancellationToken)
            .ConfigureAwait(false);

        using var json = JsonDocument.Parse(bytes);
        var root = json.RootElement;

        if (!root.TryGetProperty("schema", out var schemaElement) ||
            schemaElement.ValueKind != JsonValueKind.String)
        {
            throw new JsonException("Schema field is missing.");
        }

        var schemaText = schemaElement.GetString()!;
        var schemaType = root.TryGetProperty("schemaType", out var typeElement)
            ? typeElement.GetString()
            : null;

        var formatValue = ParseFormat(schemaType);

        var references = new List<RecordSchemaReference>();
        if (root.TryGetProperty("references", out var referencesElement) &&
            referencesElement.ValueKind == JsonValueKind.Array)
        {
            foreach (var reference in referencesElement.EnumerateArray())
            {
                var name = reference.GetProperty("name").GetString() ?? string.Empty;
                var subject = reference.GetProperty("subject").GetString() ?? string.Empty;
                var version = reference.GetProperty("version").GetInt32();

                if (string.IsNullOrWhiteSpace(name) ||
                    string.IsNullOrWhiteSpace(subject) ||
                    version <= 0)
                {
                    throw new JsonException("Schema reference is invalid.");
                }

                references.Add(new RecordSchemaReference(name, subject, version));
            }
        }

        return RecordSchemaResult<RecordSchemaDocument>.Success(
            new RecordSchemaDocument(
                schemaId,
                formatValue,
                schemaText,
                references));
    }

    private RegistryRuntime CreateRuntime(
        ClusterProfile profile,
        SecretResolver secretResolver,
        HttpMessageHandler handler)
    {
        var registry = profile.SchemaRegistry
            ?? throw new InvalidOperationException("Schema Registry profile is required.");

        var baseUri = new Uri(
            registry.Url.EndsWith("/", StringComparison.Ordinal)
                ? registry.Url
                : registry.Url + "/",
            UriKind.Absolute);

        var client = new HttpClient(handler, disposeHandler: true)
        {
            BaseAddress = baseUri,
            Timeout = Timeout.InfiniteTimeSpan,
        };

        AuthenticationHeaderValue? authorization = null;
        if (registry.Username is not null && registry.Password is not null)
        {
            var username = secretResolver.Resolve(registry.Username).Reveal();
            var password = secretResolver.Resolve(registry.Password).Reveal();
            var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
            authorization = new AuthenticationHeaderValue("Basic", encoded);
        }

        return new RegistryRuntime(client, authorization);
    }

    private void AddCache(SchemaCacheKey key, RecordSchemaDocument document)
    {
        var now = _timeProvider.GetUtcNow();
        _cache[key] = new SchemaCacheEntry(
            document,
            now,
            now + _cacheOptions.Ttl);

        if (_cache.Count <= _cacheOptions.MaxEntries)
        {
            return;
        }

        lock (_cacheTrimLock)
        {
            if (_cache.Count <= _cacheOptions.MaxEntries)
            {
                return;
            }

            var removeCount = _cache.Count - _cacheOptions.MaxEntries;
            foreach (var item in _cache
                         .OrderBy(pair => pair.Value.CreatedAtUtc)
                         .Take(removeCount)
                         .ToArray())
            {
                _cache.TryRemove(item.Key, out _);
            }
        }
    }

    private static RecordSchemaFormat ParseFormat(string? schemaType)
    {
        if (string.IsNullOrWhiteSpace(schemaType) ||
            string.Equals(schemaType, "AVRO", StringComparison.OrdinalIgnoreCase))
        {
            return RecordSchemaFormat.Avro;
        }

        if (string.Equals(schemaType, "PROTOBUF", StringComparison.OrdinalIgnoreCase))
        {
            return RecordSchemaFormat.Protobuf;
        }

        if (string.Equals(schemaType, "JSON", StringComparison.OrdinalIgnoreCase))
        {
            return RecordSchemaFormat.JsonSchema;
        }

        throw new JsonException("Schema type is unsupported.");
    }

    private static async Task<byte[]> ReadBoundedAsync(
        HttpContent content,
        int maxBytes,
        CancellationToken cancellationToken)
    {
        await using var stream = await content
            .ReadAsStreamAsync(cancellationToken)
            .ConfigureAwait(false);

        using var buffer = new MemoryStream();
        var chunk = new byte[16 * 1024];

        while (true)
        {
            var read = await stream
                .ReadAsync(chunk.AsMemory(), cancellationToken)
                .ConfigureAwait(false);

            if (read == 0)
            {
                break;
            }

            if (buffer.Length + read > maxBytes)
            {
                throw new JsonException("Schema Registry response exceeded the configured bound.");
            }

            buffer.Write(chunk, 0, read);
        }

        return buffer.ToArray();
    }

    private static RecordSchemaResult<RecordSchemaDocument> Failed(
        RecordSchemaFailureCategory category,
        string code,
        string message,
        bool retryable) =>
        RecordSchemaResult<RecordSchemaDocument>.Failed(
            new RecordSchemaFailure(category, code, message, retryable));

    private static RecordSchemaResult<RecordSchemaDocument> Cancelled() =>
        Failed(
            RecordSchemaFailureCategory.Cancelled,
            "schema_registry_cancelled",
            "Schema Registry operation was cancelled.",
            false);

    private static RecordSchemaResult<RecordSchemaDocument> Timeout() =>
        Failed(
            RecordSchemaFailureCategory.Timeout,
            "schema_registry_timeout",
            "Schema Registry operation exceeded its deadline.",
            true);

    private sealed record RegistryRuntime(
        HttpClient Client,
        AuthenticationHeaderValue? BasicAuthorization);

    private readonly record struct SchemaCacheKey(
        string ClusterId,
        int SchemaId);

    private sealed record SchemaCacheEntry(
        RecordSchemaDocument Document,
        DateTimeOffset CreatedAtUtc,
        DateTimeOffset ExpiresAtUtc);
}
