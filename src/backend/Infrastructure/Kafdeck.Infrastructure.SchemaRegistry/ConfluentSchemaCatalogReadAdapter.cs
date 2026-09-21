using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.SchemaRegistry;

public sealed class ConfluentSchemaCatalogReadAdapter : ISchemaCatalogReadPort, IDisposable
{
    private const int MaxSubjectLength = 1_024;
    private readonly IReadOnlyDictionary<string, RegistryRuntime> _registries;
    private readonly TimeProvider _timeProvider;

    public ConfluentSchemaCatalogReadAdapter(
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

    internal ConfluentSchemaCatalogReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        Func<ClusterProfile, HttpMessageHandler> handlerFactory,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(clusterProfiles);
        ArgumentNullException.ThrowIfNull(secretResolver);
        ArgumentNullException.ThrowIfNull(handlerFactory);

        _timeProvider = timeProvider ?? TimeProvider.System;
        _registries = clusterProfiles
            .Where(profile => profile.SchemaRegistry is not null)
            .ToDictionary(
                profile => profile.Id,
                profile => CreateRuntime(profile, secretResolver, handlerFactory(profile)),
                StringComparer.Ordinal);
    }

    public Task<ReadViewResult<IReadOnlyList<SchemaSubjectSummary>>> ListSubjectsAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        ExecuteAsync<IReadOnlyList<SchemaSubjectSummary>>(
            clusterId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var root = await GetJsonAsync(runtime, "subjects", operation.MaxResponseBytes, token)
                    .ConfigureAwait(false);

                if (root.ValueKind != JsonValueKind.Array)
                {
                    throw new JsonException("Schema subjects response must be an array.");
                }

                var subjects = root.EnumerateArray()
                    .Select(item => item.ValueKind == JsonValueKind.String
                        ? item.GetString()
                        : throw new JsonException("Schema subject must be a string."))
                    .Where(subject => !string.IsNullOrWhiteSpace(subject))
                    .Select(subject => subject!.Trim())
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(subject => subject, StringComparer.Ordinal)
                    .Take(operation.MaxItems + 1)
                    .ToArray();

                if (subjects.Length > operation.MaxItems)
                {
                    throw new ResponseBoundExceededException();
                }

                return subjects
                    .Select(subject => new SchemaSubjectSummary(subject))
                    .ToArray();
            });

    public Task<ReadViewResult<IReadOnlyList<SchemaVersionSummary>>> ListVersionsAsync(
        string clusterId,
        string subject,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken)
    {
        var normalized = NormalizeSubject(subject);
        if (normalized is null)
        {
            return Task.FromResult(Invalid<IReadOnlyList<SchemaVersionSummary>>("invalid_schema_subject"));
        }

        return ExecuteAsync<IReadOnlyList<SchemaVersionSummary>>(
            clusterId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var path = $"subjects/{Uri.EscapeDataString(normalized)}/versions";
                var root = await GetJsonAsync(runtime, path, operation.MaxResponseBytes, token)
                    .ConfigureAwait(false);

                if (root.ValueKind != JsonValueKind.Array)
                {
                    throw new JsonException("Schema versions response must be an array.");
                }

                var versions = root.EnumerateArray()
                    .Select(item => item.TryGetInt32(out var version) && version > 0
                        ? version
                        : throw new JsonException("Schema version is invalid."))
                    .Distinct()
                    .OrderBy(version => version)
                    .Take(operation.MaxItems + 1)
                    .ToArray();

                if (versions.Length > operation.MaxItems)
                {
                    throw new ResponseBoundExceededException();
                }

                var summaries = new List<SchemaVersionSummary>(versions.Length);
                foreach (var version in versions)
                {
                    var detail = await GetVersionDocumentAsync(
                            runtime,
                            normalized,
                            version,
                            operation.MaxResponseBytes,
                            token)
                        .ConfigureAwait(false);

                    summaries.Add(new SchemaVersionSummary(
                        normalized,
                        version,
                        detail.Schema.Id,
                        detail.Schema.Format,
                        detail.Schema.References));
                }

                return summaries;
            });
    }

    public Task<ReadViewResult<SchemaVersionDetail>> GetVersionAsync(
        string clusterId,
        string subject,
        int version,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken)
    {
        var normalized = NormalizeSubject(subject);
        if (normalized is null || version <= 0)
        {
            return Task.FromResult(Invalid<SchemaVersionDetail>("invalid_schema_version_request"));
        }

        return ExecuteAsync(
            clusterId,
            operation,
            cancellationToken,
            (runtime, token) => GetVersionDocumentAsync(
                runtime,
                normalized,
                version,
                operation.MaxResponseBytes,
                token));
    }

    public Task<ReadViewResult<SchemaCompatibilityObservation>> GetCompatibilityAsync(
        string clusterId,
        string subject,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken)
    {
        var normalized = NormalizeSubject(subject);
        if (normalized is null)
        {
            return Task.FromResult(Invalid<SchemaCompatibilityObservation>("invalid_schema_subject"));
        }

        return ExecuteAsync(
            clusterId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var subjectPath = $"config/{Uri.EscapeDataString(normalized)}";
                var subjectRoot = await TryGetJsonAsync(
                        runtime,
                        subjectPath,
                        operation.MaxResponseBytes,
                        token)
                    .ConfigureAwait(false);

                if (subjectRoot.HasValue)
                {
                    return new SchemaCompatibilityObservation(
                        normalized,
                        ParseCompatibilityResponse(subjectRoot.Value),
                        false);
                }

                var globalRoot = await GetJsonAsync(
                        runtime,
                        "config",
                        operation.MaxResponseBytes,
                        token)
                    .ConfigureAwait(false);

                return new SchemaCompatibilityObservation(
                    normalized,
                    ParseCompatibilityResponse(globalRoot),
                    true);
            });
    }

    public void Dispose()
    {
        foreach (var runtime in _registries.Values)
        {
            runtime.Client.Dispose();
        }
    }

    private async Task<ReadViewResult<T>> ExecuteAsync<T>(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken,
        Func<RegistryRuntime, CancellationToken, Task<T>> action)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        if (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(ReadViewFailureCategory.Cancelled, "operation_cancelled", "Schema Registry operation was cancelled.", false);
        }

        if (operation.DeadlineUtc <= _timeProvider.GetUtcNow())
        {
            return Failed<T>(ReadViewFailureCategory.Timeout, "deadline_exceeded", "Schema Registry operation exceeded its deadline.", true);
        }

        if (!_registries.TryGetValue(clusterId, out var runtime))
        {
            return Failed<T>(
                ReadViewFailureCategory.NotConfigured,
                "schema_registry_not_configured",
                "Schema Registry is not configured for the requested cluster.",
                false);
        }

        try
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var remaining = operation.DeadlineUtc - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                return Failed<T>(ReadViewFailureCategory.Timeout, "deadline_exceeded", "Schema Registry operation exceeded its deadline.", true);
            }

            deadline.CancelAfter(remaining);
            var value = await action(runtime, deadline.Token).ConfigureAwait(false);
            return ReadViewResult<T>.Success(value);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(ReadViewFailureCategory.Cancelled, "operation_cancelled", "Schema Registry operation was cancelled.", false);
        }
        catch (OperationCanceledException)
        {
            return Failed<T>(ReadViewFailureCategory.Timeout, "deadline_exceeded", "Schema Registry operation exceeded its deadline.", true);
        }
        catch (RegistryStatusException exception)
        {
            return Failed<T>(exception.Category, exception.Code, exception.SafeMessage, exception.Retryable);
        }
        catch (ResponseBoundExceededException)
        {
            return Failed<T>(
                ReadViewFailureCategory.ResponseTooLarge,
                "schema_registry_response_too_large",
                "Schema Registry response exceeded the configured bound.",
                false);
        }
        catch (HttpRequestException)
        {
            return Failed<T>(
                ReadViewFailureCategory.Unavailable,
                "schema_registry_unavailable",
                "Schema Registry is temporarily unavailable.",
                true);
        }
        catch (JsonException)
        {
            return Failed<T>(
                ReadViewFailureCategory.InvalidResponse,
                "invalid_schema_registry_response",
                "Schema Registry returned an invalid response.",
                false);
        }
    }

    private static async Task<SchemaVersionDetail> GetVersionDocumentAsync(
        RegistryRuntime runtime,
        string subject,
        int version,
        long maxResponseBytes,
        CancellationToken cancellationToken)
    {
        var path = $"subjects/{Uri.EscapeDataString(subject)}/versions/{version}";
        var root = await GetJsonAsync(runtime, path, maxResponseBytes, cancellationToken)
            .ConfigureAwait(false);

        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty("schema", out var schemaElement) ||
            schemaElement.ValueKind != JsonValueKind.String ||
            !root.TryGetProperty("id", out var idElement) ||
            !idElement.TryGetInt32(out var schemaId) ||
            schemaId <= 0)
        {
            throw new JsonException("Schema version response is invalid.");
        }

        var schemaType = root.TryGetProperty("schemaType", out var typeElement) &&
            typeElement.ValueKind == JsonValueKind.String
            ? typeElement.GetString()
            : null;

        var references = ParseReferences(root);
        var schema = new RecordSchemaDocument(
            schemaId,
            ParseFormat(schemaType),
            schemaElement.GetString()!,
            references);

        return new SchemaVersionDetail(subject, version, schema);
    }

    private static IReadOnlyList<RecordSchemaReference> ParseReferences(JsonElement root)
    {
        if (!root.TryGetProperty("references", out var referencesElement))
        {
            return Array.Empty<RecordSchemaReference>();
        }

        if (referencesElement.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException("Schema references must be an array.");
        }

        var references = new List<RecordSchemaReference>();
        foreach (var item in referencesElement.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object ||
                !item.TryGetProperty("name", out var name) ||
                name.ValueKind != JsonValueKind.String ||
                !item.TryGetProperty("subject", out var subject) ||
                subject.ValueKind != JsonValueKind.String ||
                !item.TryGetProperty("version", out var versionElement) ||
                !versionElement.TryGetInt32(out var version) ||
                version <= 0)
            {
                throw new JsonException("Schema reference is invalid.");
            }

            var nameValue = name.GetString();
            var subjectValue = subject.GetString();
            if (string.IsNullOrWhiteSpace(nameValue) || string.IsNullOrWhiteSpace(subjectValue))
            {
                throw new JsonException("Schema reference is invalid.");
            }

            references.Add(new RecordSchemaReference(nameValue.Trim(), subjectValue.Trim(), version));
        }

        return references;
    }

    private static async Task<JsonElement?> TryGetJsonAsync(
        RegistryRuntime runtime,
        string relativePath,
        long maxResponseBytes,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, relativePath);
        request.Headers.Accept.ParseAdd("application/vnd.schemaregistry.v1+json");
        if (runtime.BasicAuthorization is not null)
        {
            request.Headers.Authorization = runtime.BasicAuthorization;
        }

        using var response = await runtime.Client.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        return await ParseJsonResponseAsync(response, maxResponseBytes, cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<JsonElement> GetJsonAsync(
        RegistryRuntime runtime,
        string relativePath,
        long maxResponseBytes,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, relativePath);
        request.Headers.Accept.ParseAdd("application/vnd.schemaregistry.v1+json");
        if (runtime.BasicAuthorization is not null)
        {
            request.Headers.Authorization = runtime.BasicAuthorization;
        }

        using var response = await runtime.Client.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        return await ParseJsonResponseAsync(response, maxResponseBytes, cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<JsonElement> ParseJsonResponseAsync(
        HttpResponseMessage response,
        long maxResponseBytes,
        CancellationToken cancellationToken)
    {
        if ((int)response.StatusCode is >= 300 and < 400)
        {
            throw new RegistryStatusException(
                ReadViewFailureCategory.InvalidResponse,
                "schema_registry_redirect_rejected",
                "Schema Registry redirect was rejected.",
                false);
        }

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            throw new RegistryStatusException(
                ReadViewFailureCategory.Unavailable,
                "schema_registry_resource_not_found",
                "Schema Registry did not contain the requested resource.",
                false);
        }

        if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
        {
            throw new RegistryStatusException(
                ReadViewFailureCategory.Unauthorized,
                "schema_registry_authorization_denied",
                "Schema Registry denied the requested operation.",
                false);
        }

        if ((int)response.StatusCode >= 500)
        {
            throw new RegistryStatusException(
                ReadViewFailureCategory.Unavailable,
                "schema_registry_unavailable",
                "Schema Registry is temporarily unavailable.",
                true);
        }

        if (!response.IsSuccessStatusCode)
        {
            throw new RegistryStatusException(
                ReadViewFailureCategory.InvalidResponse,
                "schema_registry_request_failed",
                "Schema Registry rejected the requested operation.",
                false);
        }

        var bytes = await ReadBoundedAsync(response.Content, maxResponseBytes, cancellationToken)
            .ConfigureAwait(false);

        using var document = JsonDocument.Parse(bytes);
        return document.RootElement.Clone();
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
            authorization = new AuthenticationHeaderValue(
                "Basic",
                Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));
        }

        return new RegistryRuntime(client, authorization);
    }

    private static async Task<byte[]> ReadBoundedAsync(
        HttpContent content,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        if (content.Headers.ContentLength is long declared && declared > maxBytes)
        {
            throw new ResponseBoundExceededException();
        }

        await using var stream = await content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using var buffer = new MemoryStream();
        var chunk = new byte[16 * 1024];

        while (true)
        {
            var read = await stream.ReadAsync(chunk.AsMemory(), cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                break;
            }

            if (buffer.Length + read > maxBytes)
            {
                throw new ResponseBoundExceededException();
            }

            buffer.Write(chunk, 0, read);
        }

        return buffer.ToArray();
    }

    private static string? NormalizeSubject(string? subject)
    {
        if (string.IsNullOrWhiteSpace(subject))
        {
            return null;
        }

        var normalized = subject.Trim();
        return normalized.Length <= MaxSubjectLength && !normalized.Any(char.IsControl)
            ? normalized
            : null;
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

    private static SchemaCompatibilityMode ParseCompatibilityResponse(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty("compatibilityLevel", out var level) ||
            level.ValueKind != JsonValueKind.String)
        {
            throw new JsonException("Schema compatibility response is invalid.");
        }

        return ParseCompatibility(level.GetString());
    }

    private static SchemaCompatibilityMode ParseCompatibility(string? value)
    {
        var normalized = value?.Trim().Replace("-", "_", StringComparison.Ordinal).ToUpperInvariant();
        return normalized switch
        {
            "NONE" => SchemaCompatibilityMode.None,
            "BACKWARD" => SchemaCompatibilityMode.Backward,
            "BACKWARD_TRANSITIVE" => SchemaCompatibilityMode.BackwardTransitive,
            "FORWARD" => SchemaCompatibilityMode.Forward,
            "FORWARD_TRANSITIVE" => SchemaCompatibilityMode.ForwardTransitive,
            "FULL" => SchemaCompatibilityMode.Full,
            "FULL_TRANSITIVE" => SchemaCompatibilityMode.FullTransitive,
            _ => SchemaCompatibilityMode.Unknown,
        };
    }

    private static ReadViewResult<T> Invalid<T>(string code) =>
        Failed<T>(ReadViewFailureCategory.InvalidRequest, code, "Schema Registry request is invalid.", false);

    private static ReadViewResult<T> Failed<T>(
        ReadViewFailureCategory category,
        string code,
        string message,
        bool retryable) =>
        ReadViewResult<T>.Failed(new ReadViewFailure(category, code, message, retryable));

    private sealed record RegistryRuntime(
        HttpClient Client,
        AuthenticationHeaderValue? BasicAuthorization);

    private sealed class ResponseBoundExceededException : Exception;

    private sealed class RegistryStatusException : Exception
    {
        public RegistryStatusException(
            ReadViewFailureCategory category,
            string code,
            string safeMessage,
            bool retryable)
        {
            Category = category;
            Code = code;
            SafeMessage = safeMessage;
            Retryable = retryable;
        }

        public ReadViewFailureCategory Category { get; }
        public string Code { get; }
        public string SafeMessage { get; }
        public bool Retryable { get; }
    }
}
