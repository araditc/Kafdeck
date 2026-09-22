using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.SchemaRegistry;

public sealed class ConfluentSchemaMutationAdapter :
    ISchemaMutationPort,
    ISchemaMutationObservationPort,
    IDisposable
{
    private static readonly JsonSerializerOptions RequestJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private const int MaxSubjectLength = 1_024;
    private const int MaxSchemaBytes = 4 * 1024 * 1024;
    private const int MaxReferences = 64;
    private const int MaxReferenceNameLength = 1_024;
    private const int MaxResponseBytes = 4 * 1024 * 1024;

    private readonly IReadOnlyDictionary<string, RegistryRuntime> _registries;
    private readonly TimeProvider _timeProvider;

    public ConfluentSchemaMutationAdapter(
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

    internal ConfluentSchemaMutationAdapter(
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
                profile => CreateRuntime(
                    profile,
                    secretResolver,
                    handlerFactory(profile)),
                StringComparer.Ordinal);
    }

    public Task<SchemaMutationObservationResult<SchemaMutationCapabilities>>
        GetCapabilitiesAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
    {
        if (!_registries.ContainsKey(clusterId))
        {
            return Task.FromResult(
                ObservationFailed<SchemaMutationCapabilities>(
                    SchemaMutationObservationFailureCategory.NotConfigured,
                    "schema_registry_not_configured",
                    "Schema Registry is not configured for the requested cluster.",
                    false));
        }

        return Task.FromResult(
            SchemaMutationObservationResult<SchemaMutationCapabilities>.Success(
                new SchemaMutationCapabilities(
                    SupportsCompatibilityValidation: true,
                    SupportsRegistration: true,
                    SupportsCompatibilityMutation: true,
                    SupportsSoftDelete: true,
                    SupportsPermanentDelete: true)));
    }

    public Task<SchemaMutationObservationResult<SchemaCompatibilityCheckObservation>>
        TestCompatibilityAsync(
            string clusterId,
            SchemaCompatibilityCheckRequest request,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        string subject;
        SchemaRequestBody body;
        try
        {
            subject = RequireSubject(request.Subject);
            body = BuildBody(
                request.Format,
                request.Schema,
                request.References);
        }
        catch (ArgumentException)
        {
            return Task.FromResult(
                ObservationFailed<SchemaCompatibilityCheckObservation>(
                    SchemaMutationObservationFailureCategory.InvalidRequest,
                    "invalid_schema_compatibility_request",
                    "Schema compatibility request is invalid.",
                    false));
        }

        return ExecuteObservationAsync(
            clusterId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var root = await SendJsonForObservationAsync(
                        runtime,
                        HttpMethod.Post,
                        $"compatibility/subjects/{Uri.EscapeDataString(subject)}/versions/latest",
                        body,
                        token)
                    .ConfigureAwait(false);

                if (root.ValueKind != JsonValueKind.Object ||
                    !root.TryGetProperty("is_compatible", out var compatible) ||
                    compatible.ValueKind is not (
                        JsonValueKind.True or JsonValueKind.False))
                {
                    throw new JsonException(
                        "Schema compatibility response is invalid.");
                }

                return new SchemaCompatibilityCheckObservation(
                    compatible.GetBoolean(),
                    compatible.GetBoolean()
                        ? "schema_compatible"
                        : "schema_incompatible");
            });
    }

    public Task<SchemaMutationObservationResult<SchemaDeleteTargetObservation>>
        ObserveDeleteTargetAsync(
            string clusterId,
            SchemaDeleteObservationRequest request,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        string subject;
        string activePath;
        string deletedPath;
        try
        {
            subject = RequireSubject(request.Subject);
            if (request.Version is <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(request.Version));
            }

            activePath = request.Version.HasValue
                ? $"subjects/{Uri.EscapeDataString(subject)}/versions/{request.Version.Value}"
                : $"subjects/{Uri.EscapeDataString(subject)}/versions";

            deletedPath = activePath.Contains('?', StringComparison.Ordinal)
                ? activePath + "&deleted=true"
                : activePath + "?deleted=true";
        }
        catch (ArgumentException)
        {
            return Task.FromResult(
                ObservationFailed<SchemaDeleteTargetObservation>(
                    SchemaMutationObservationFailureCategory.InvalidRequest,
                    "invalid_schema_delete_observation",
                    "Schema delete observation request is invalid.",
                    false));
        }

        return ExecuteObservationAsync(
            clusterId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var active = await ObserveDeletePathAsync(
                        runtime,
                        activePath,
                        request.Version,
                        token)
                    .ConfigureAwait(false);
                var includingDeleted = await ObserveDeletePathAsync(
                        runtime,
                        deletedPath,
                        request.Version,
                        token)
                    .ConfigureAwait(false);

                return new SchemaDeleteTargetObservation(
                    active.Exists,
                    includingDeleted.Exists,
                    IsSoftDeleted:
                        !active.Exists && includingDeleted.Exists,
                    active.Versions,
                    includingDeleted.Versions);
            });
    }

    public Task<MutationProviderResult> CreateAsync(
        SchemaCreateMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string subject;
        SchemaRequestBody body;
        try
        {
            subject = RequireSubject(request.Subject);
            body = BuildBody(
                request.Format,
                request.Schema,
                request.References.Select(reference =>
                    new RecordSchemaReference(
                        reference.Name,
                        reference.Subject,
                        reference.Version))
                    .ToArray());
        }
        catch (ArgumentException)
        {
            return Task.FromResult(
                FailedDefinitive(
                    "schema_create_invalid_request"));
        }

        return ExecuteMutationAsync(
            request.ClusterId,
            "schema_create",
            cancellationToken,
            async (runtime, token) =>
            {
                var root = await SendJsonForMutationAsync(
                        runtime,
                        HttpMethod.Post,
                        $"subjects/{Uri.EscapeDataString(subject)}/versions",
                        body,
                        token)
                    .ConfigureAwait(false);

                if (root.ValueKind != JsonValueKind.Object ||
                    !root.TryGetProperty("id", out var id) ||
                    !id.TryGetInt32(out var schemaId) ||
                    schemaId <= 0)
                {
                    return Unknown("schema_create_invalid_provider_response");
                }

                return Accepted(
                    "schema_create_accepted",
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["provider.accepted"] = "true",
                        ["schema.id"] = schemaId.ToString(
                            System.Globalization.CultureInfo.InvariantCulture),
                    });
            });
    }

    public Task<MutationProviderResult> AlterCompatibilityAsync(
        SchemaAlterMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string path;
        string mode;
        try
        {
            mode = RequireCompatibilityMode(request.CompatibilityMode);
            path = request.Scope switch
            {
                SchemaCompatibilityScope.Global
                    when request.Subject is null =>
                    "config",

                SchemaCompatibilityScope.Subject
                    when request.Subject is not null =>
                    $"config/{Uri.EscapeDataString(RequireSubject(request.Subject))}",

                _ => throw new ArgumentException(
                    "Schema compatibility mutation scope is invalid."),
            };
        }
        catch (ArgumentException)
        {
            return Task.FromResult(
                FailedDefinitive(
                    "schema_compatibility_invalid_request"));
        }

        return ExecuteMutationAsync(
            request.ClusterId,
            "schema_compatibility",
            cancellationToken,
            async (runtime, token) =>
            {
                var root = await SendJsonForMutationAsync(
                        runtime,
                        HttpMethod.Put,
                        path,
                        new CompatibilityRequestBody(mode),
                        token)
                    .ConfigureAwait(false);

                if (root.ValueKind != JsonValueKind.Object ||
                    !root.TryGetProperty("compatibility", out var compatibility) ||
                    compatibility.ValueKind != JsonValueKind.String)
                {
                    return Unknown(
                        "schema_compatibility_invalid_provider_response");
                }

                string returned;
                try
                {
                    returned = RequireCompatibilityMode(
                        compatibility.GetString() ?? string.Empty);
                }
                catch (ArgumentException)
                {
                    return Unknown(
                        "schema_compatibility_invalid_provider_response");
                }

                if (!string.Equals(returned, mode, StringComparison.Ordinal))
                {
                    return Unknown(
                        "schema_compatibility_provider_result_mismatch");
                }

                return Accepted(
                    "schema_compatibility_accepted",
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["provider.accepted"] = "true",
                        ["compatibility.mode"] = mode,
                    });
            });
    }

    public Task<MutationProviderResult> DeleteAsync(
        SchemaDeleteMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string subject;
        string path;
        try
        {
            subject = RequireSubject(request.Subject);
            if (request.Version is <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(request.Version));
            }

            var suffix = request.Permanent
                ? "?permanent=true"
                : string.Empty;

            path = request.Version.HasValue
                ? $"subjects/{Uri.EscapeDataString(subject)}/versions/{request.Version.Value}{suffix}"
                : $"subjects/{Uri.EscapeDataString(subject)}{suffix}";
        }
        catch (ArgumentException)
        {
            return Task.FromResult(
                FailedDefinitive(
                    "schema_delete_invalid_request"));
        }

        return ExecuteMutationAsync(
            request.ClusterId,
            "schema_delete",
            cancellationToken,
            async (runtime, token) =>
            {
                var root = await SendJsonForMutationAsync(
                        runtime,
                        HttpMethod.Delete,
                        path,
                        requestBody: null,
                        token)
                    .ConfigureAwait(false);

                if (request.Version.HasValue)
                {
                    if (root.ValueKind != JsonValueKind.Number ||
                        !root.TryGetInt32(out var deletedVersion) ||
                        deletedVersion != request.Version.Value)
                    {
                        return Unknown(
                            "schema_delete_invalid_provider_response");
                    }
                }
                else
                {
                    if (root.ValueKind != JsonValueKind.Array)
                    {
                        return Unknown(
                            "schema_delete_invalid_provider_response");
                    }

                    foreach (var item in root.EnumerateArray())
                    {
                        if (!item.TryGetInt32(out var version) || version <= 0)
                        {
                            return Unknown(
                                "schema_delete_invalid_provider_response");
                        }
                    }
                }

                return Accepted(
                    request.Permanent
                        ? "schema_delete_permanent_accepted"
                        : "schema_delete_soft_accepted",
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["provider.accepted"] = "true",
                        ["delete.permanent"] =
                            request.Permanent ? "true" : "false",
                        ["delete.scope"] =
                            request.Version.HasValue ? "version" : "subject",
                    });
            });
    }

    public void Dispose()
    {
        foreach (var runtime in _registries.Values)
        {
            runtime.Client.Dispose();
        }
    }

    private async Task<SchemaMutationObservationResult<T>>
        ExecuteObservationAsync<T>(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken,
            Func<RegistryRuntime, CancellationToken, Task<T>> action)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ObservationFailed<T>(
                SchemaMutationObservationFailureCategory.Cancelled,
                "schema_registry_cancelled",
                "Schema Registry operation was cancelled.",
                false);
        }

        if (operation.DeadlineUtc <= _timeProvider.GetUtcNow())
        {
            return ObservationFailed<T>(
                SchemaMutationObservationFailureCategory.Timeout,
                "schema_registry_timeout",
                "Schema Registry operation exceeded its deadline.",
                true);
        }

        if (!_registries.TryGetValue(clusterId, out var runtime))
        {
            return ObservationFailed<T>(
                SchemaMutationObservationFailureCategory.NotConfigured,
                "schema_registry_not_configured",
                "Schema Registry is not configured for the requested cluster.",
                false);
        }

        try
        {
            using var deadline =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);
            var remaining =
                operation.DeadlineUtc - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                return ObservationFailed<T>(
                    SchemaMutationObservationFailureCategory.Timeout,
                    "schema_registry_timeout",
                    "Schema Registry operation exceeded its deadline.",
                    true);
            }

            deadline.CancelAfter(remaining);
            var value = await action(runtime, deadline.Token)
                .ConfigureAwait(false);
            return SchemaMutationObservationResult<T>.Success(value);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return ObservationFailed<T>(
                SchemaMutationObservationFailureCategory.Cancelled,
                "schema_registry_cancelled",
                "Schema Registry operation was cancelled.",
                false);
        }
        catch (OperationCanceledException)
        {
            return ObservationFailed<T>(
                SchemaMutationObservationFailureCategory.Timeout,
                "schema_registry_timeout",
                "Schema Registry operation exceeded its deadline.",
                true);
        }
        catch (RegistryObservationStatusException exception)
        {
            return ObservationFailed<T>(
                exception.Category,
                exception.Code,
                exception.SafeMessage,
                exception.Retryable);
        }
        catch (HttpRequestException)
        {
            return ObservationFailed<T>(
                SchemaMutationObservationFailureCategory.Unavailable,
                "schema_registry_unavailable",
                "Schema Registry is temporarily unavailable.",
                true);
        }
        catch (JsonException)
        {
            return ObservationFailed<T>(
                SchemaMutationObservationFailureCategory.InvalidResponse,
                "invalid_schema_registry_response",
                "Schema Registry returned an invalid response.",
                false);
        }
        catch (ArgumentException)
        {
            return ObservationFailed<T>(
                SchemaMutationObservationFailureCategory.InvalidRequest,
                "invalid_schema_registry_request",
                "Schema Registry request is invalid.",
                false);
        }
        catch
        {
            return ObservationFailed<T>(
                SchemaMutationObservationFailureCategory.InvalidResponse,
                "schema_registry_observation_failed",
                "Schema Registry observation failed.",
                false);
        }
    }

    private async Task<MutationProviderResult> ExecuteMutationAsync(
        string clusterId,
        string operationCode,
        CancellationToken cancellationToken,
        Func<RegistryRuntime, CancellationToken, Task<MutationProviderResult>> action)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        if (cancellationToken.IsCancellationRequested)
        {
            return Unknown(
                $"{operationCode}_cancelled_or_timeout");
        }

        if (!_registries.TryGetValue(clusterId, out var runtime))
        {
            return FailedDefinitive(
                $"{operationCode}_registry_not_configured");
        }

        try
        {
            return await action(runtime, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return Unknown(
                $"{operationCode}_cancelled_or_timeout");
        }
        catch (HttpRequestException)
        {
            return Unknown(
                $"{operationCode}_registry_unavailable");
        }
        catch (RegistryMutationStatusException exception)
        {
            return exception.Ambiguous
                ? Unknown($"{operationCode}_{exception.Code}")
                : FailedDefinitive(
                    $"{operationCode}_{exception.Code}");
        }
        catch (JsonException)
        {
            return Unknown(
                $"{operationCode}_invalid_provider_response");
        }
        catch (ArgumentException)
        {
            return FailedDefinitive(
                $"{operationCode}_invalid_request");
        }
        catch
        {
            return Unknown(
                $"{operationCode}_provider_exception");
        }
    }

    private static async Task<DeletePathObservation> ObserveDeletePathAsync(
        RegistryRuntime runtime,
        string relativePath,
        int? requestedVersion,
        CancellationToken cancellationToken)
    {
        using var request = BuildRequest(
            runtime,
            HttpMethod.Get,
            relativePath,
            body: null);
        using var response = await runtime.Client.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        if ((int)response.StatusCode is >= 300 and < 400)
        {
            throw new RegistryObservationStatusException(
                SchemaMutationObservationFailureCategory.InvalidResponse,
                "schema_registry_redirect_rejected",
                "Schema Registry redirect was rejected.",
                false);
        }

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return new DeletePathObservation(
                false,
                Array.Empty<int>());
        }

        if (response.StatusCode is
            HttpStatusCode.Unauthorized or
            HttpStatusCode.Forbidden)
        {
            throw new RegistryObservationStatusException(
                SchemaMutationObservationFailureCategory.Unauthorized,
                "schema_registry_authorization_denied",
                "Schema Registry denied the requested operation.",
                false);
        }

        if ((int)response.StatusCode >= 500)
        {
            throw new RegistryObservationStatusException(
                SchemaMutationObservationFailureCategory.Unavailable,
                "schema_registry_unavailable",
                "Schema Registry is temporarily unavailable.",
                true);
        }

        if (!response.IsSuccessStatusCode)
        {
            throw new RegistryObservationStatusException(
                SchemaMutationObservationFailureCategory.InvalidResponse,
                "schema_registry_request_failed",
                "Schema Registry rejected the requested observation.",
                false);
        }

        var root = await ReadJsonAsync(
                response,
                MaxResponseBytes,
                cancellationToken)
            .ConfigureAwait(false);

        if (root.ValueKind == JsonValueKind.Array)
        {
            var versions = root.EnumerateArray()
                .Select(item =>
                    item.TryGetInt32(out var version) && version > 0
                        ? version
                        : throw new JsonException(
                            "Schema Registry delete observation version is invalid."))
                .Distinct()
                .OrderBy(version => version)
                .ToArray();

            return new DeletePathObservation(
                versions.Length > 0,
                Array.AsReadOnly(versions));
        }

        if (root.ValueKind == JsonValueKind.Object &&
            requestedVersion.HasValue &&
            root.TryGetProperty("version", out var versionElement) &&
            versionElement.TryGetInt32(out var observedVersion) &&
            observedVersion == requestedVersion.Value)
        {
            return new DeletePathObservation(
                true,
                new[] { observedVersion });
        }

        throw new JsonException(
            "Schema Registry delete observation response is invalid.");
    }

    private static async Task<JsonElement> SendJsonForObservationAsync(
        RegistryRuntime runtime,
        HttpMethod method,
        string relativePath,
        object? requestBody,
        CancellationToken cancellationToken)
    {
        using var request = BuildRequest(
            runtime,
            method,
            relativePath,
            requestBody);
        using var response = await runtime.Client.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        if ((int)response.StatusCode is >= 300 and < 400)
        {
            throw new RegistryObservationStatusException(
                SchemaMutationObservationFailureCategory.InvalidResponse,
                "schema_registry_redirect_rejected",
                "Schema Registry redirect was rejected.",
                false);
        }

        if (response.StatusCode is
            HttpStatusCode.Unauthorized or
            HttpStatusCode.Forbidden)
        {
            throw new RegistryObservationStatusException(
                SchemaMutationObservationFailureCategory.Unauthorized,
                "schema_registry_authorization_denied",
                "Schema Registry denied the requested operation.",
                false);
        }

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            throw new RegistryObservationStatusException(
                SchemaMutationObservationFailureCategory.InvalidRequest,
                "schema_registry_resource_not_found",
                "Schema Registry did not contain the requested resource.",
                false);
        }

        if ((int)response.StatusCode >= 500)
        {
            throw new RegistryObservationStatusException(
                SchemaMutationObservationFailureCategory.Unavailable,
                "schema_registry_unavailable",
                "Schema Registry is temporarily unavailable.",
                true);
        }

        if (!response.IsSuccessStatusCode)
        {
            throw new RegistryObservationStatusException(
                SchemaMutationObservationFailureCategory.InvalidResponse,
                "schema_registry_request_failed",
                "Schema Registry rejected the requested observation.",
                false);
        }

        return await ReadJsonAsync(
                response,
                MaxResponseBytes,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<JsonElement> SendJsonForMutationAsync(
        RegistryRuntime runtime,
        HttpMethod method,
        string relativePath,
        object? requestBody,
        CancellationToken cancellationToken)
    {
        using var request = BuildRequest(
            runtime,
            method,
            relativePath,
            requestBody);
        using var response = await runtime.Client.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        if ((int)response.StatusCode is >= 300 and < 400)
        {
            throw new RegistryMutationStatusException(
                "redirect_rejected",
                Ambiguous: false);
        }

        if (response.StatusCode is
            HttpStatusCode.Unauthorized or
            HttpStatusCode.Forbidden)
        {
            throw new RegistryMutationStatusException(
                "authorization_denied",
                Ambiguous: false);
        }

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            throw new RegistryMutationStatusException(
                "resource_not_found",
                Ambiguous: false);
        }

        if (response.StatusCode is
            HttpStatusCode.Conflict or
            HttpStatusCode.UnprocessableEntity or
            HttpStatusCode.BadRequest)
        {
            throw new RegistryMutationStatusException(
                "rejected",
                Ambiguous: false);
        }

        if ((int)response.StatusCode >= 500 ||
            response.StatusCode is
                HttpStatusCode.RequestTimeout or
                HttpStatusCode.TooManyRequests)
        {
            throw new RegistryMutationStatusException(
                "ambiguous",
                Ambiguous: true);
        }

        if (!response.IsSuccessStatusCode)
        {
            throw new RegistryMutationStatusException(
                "unknown_status",
                Ambiguous: true);
        }

        return await ReadJsonAsync(
                response,
                MaxResponseBytes,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static HttpRequestMessage BuildRequest(
        RegistryRuntime runtime,
        HttpMethod method,
        string relativePath,
        object? body)
    {
        var request = new HttpRequestMessage(
            method,
            relativePath);
        request.Headers.Accept.ParseAdd(
            "application/vnd.schemaregistry.v1+json");

        if (runtime.BasicAuthorization is not null)
        {
            request.Headers.Authorization =
                runtime.BasicAuthorization;
        }

        if (body is not null)
        {
            var json = JsonSerializer.Serialize(
                body,
                RequestJsonOptions);
            request.Content = new StringContent(
                json,
                Encoding.UTF8,
                "application/vnd.schemaregistry.v1+json");
        }

        return request;
    }

    private static async Task<JsonElement> ReadJsonAsync(
        HttpResponseMessage response,
        int maxBytes,
        CancellationToken cancellationToken)
    {
        var bytes = await ReadBoundedAsync(
                response.Content,
                maxBytes,
                cancellationToken)
            .ConfigureAwait(false);
        using var document = JsonDocument.Parse(bytes);
        return document.RootElement.Clone();
    }

    private static async Task<byte[]> ReadBoundedAsync(
        HttpContent content,
        int maxBytes,
        CancellationToken cancellationToken)
    {
        if (content.Headers.ContentLength is long declared &&
            declared > maxBytes)
        {
            throw new JsonException(
                "Schema Registry response exceeded the configured bound.");
        }

        await using var stream =
            await content.ReadAsStreamAsync(cancellationToken)
                .ConfigureAwait(false);
        using var buffer = new MemoryStream();
        var chunk = new byte[16 * 1024];

        while (true)
        {
            var read = await stream.ReadAsync(
                    chunk.AsMemory(),
                    cancellationToken)
                .ConfigureAwait(false);
            if (read == 0)
                break;

            if (buffer.Length + read > maxBytes)
            {
                throw new JsonException(
                    "Schema Registry response exceeded the configured bound.");
            }

            buffer.Write(chunk, 0, read);
        }

        return buffer.ToArray();
    }

    private RegistryRuntime CreateRuntime(
        ClusterProfile profile,
        SecretResolver secretResolver,
        HttpMessageHandler handler)
    {
        var registry = profile.SchemaRegistry
            ?? throw new InvalidOperationException(
                "Schema Registry profile is required.");

        var baseUri = new Uri(
            registry.Url.EndsWith("/", StringComparison.Ordinal)
                ? registry.Url
                : registry.Url + "/",
            UriKind.Absolute);

        var client = new HttpClient(
            handler,
            disposeHandler: true)
        {
            BaseAddress = baseUri,
            Timeout = Timeout.InfiniteTimeSpan,
        };

        AuthenticationHeaderValue? authorization = null;
        if (registry.Username is not null &&
            registry.Password is not null)
        {
            var username =
                secretResolver.Resolve(registry.Username).Reveal();
            var password =
                secretResolver.Resolve(registry.Password).Reveal();

            authorization = new AuthenticationHeaderValue(
                "Basic",
                Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(
                        $"{username}:{password}")));
        }

        return new RegistryRuntime(
            client,
            authorization);
    }

    private static SchemaRequestBody BuildBody(
        RecordSchemaFormat format,
        string schema,
        IReadOnlyList<RecordSchemaReference> references)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        ArgumentNullException.ThrowIfNull(references);

        if (!Enum.IsDefined(format))
        {
            throw new ArgumentOutOfRangeException(nameof(format));
        }

        if (Encoding.UTF8.GetByteCount(schema) > MaxSchemaBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(schema),
                "Schema exceeds the admitted byte ceiling.");
        }

        if (references.Count > MaxReferences)
        {
            throw new ArgumentOutOfRangeException(
                nameof(references),
                "Schema references exceed the admitted count ceiling.");
        }

        var normalized = new Dictionary<
            (string Name, string Subject, int Version),
            SchemaReferenceBody>();

        foreach (var reference in references)
        {
            ArgumentNullException.ThrowIfNull(reference);

            var name = RequireIdentifier(
                reference.Name,
                "Schema reference name",
                MaxReferenceNameLength);
            var subject = RequireSubject(reference.Subject);
            if (reference.Version <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(references),
                    "Schema reference version must be positive.");
            }

            normalized.TryAdd(
                (name, subject, reference.Version),
                new SchemaReferenceBody(
                    name,
                    subject,
                    reference.Version));
        }

        return new SchemaRequestBody(
            FormatName(format),
            schema,
            normalized.Values
                .OrderBy(item => item.Name, StringComparer.Ordinal)
                .ThenBy(item => item.Subject, StringComparer.Ordinal)
                .ThenBy(item => item.Version)
                .ToArray());
    }

    private static string RequireSubject(string value) =>
        RequireIdentifier(
            value,
            "Schema subject",
            MaxSubjectLength);

    private static string RequireIdentifier(
        string value,
        string fieldName,
        int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();
        if (!string.Equals(value, normalized, StringComparison.Ordinal) ||
            normalized.Length > maxLength ||
            normalized.Any(char.IsControl))
        {
            throw new ArgumentException(
                $"{fieldName} is invalid.",
                fieldName);
        }

        return normalized;
    }

    private static string RequireCompatibilityMode(string value)
    {
        var normalized = value?.Trim()
            .Replace("-", "_", StringComparison.Ordinal)
            .ToUpperInvariant();

        return normalized switch
        {
            "NONE" => "NONE",
            "BACKWARD" => "BACKWARD",
            "BACKWARD_TRANSITIVE" => "BACKWARD_TRANSITIVE",
            "FORWARD" => "FORWARD",
            "FORWARD_TRANSITIVE" => "FORWARD_TRANSITIVE",
            "FULL" => "FULL",
            "FULL_TRANSITIVE" => "FULL_TRANSITIVE",
            _ => throw new ArgumentException(
                "Schema compatibility mode is invalid.",
                nameof(value)),
        };
    }

    private static string FormatName(
        RecordSchemaFormat format) =>
        format switch
        {
            RecordSchemaFormat.Avro => "AVRO",
            RecordSchemaFormat.Protobuf => "PROTOBUF",
            RecordSchemaFormat.JsonSchema => "JSON",
            _ => throw new ArgumentOutOfRangeException(
                nameof(format)),
        };

    private static MutationProviderResult Accepted(
        string code,
        IReadOnlyDictionary<string, string> evidence) =>
        new(
            MutationExecutionResultKind.AppliedUnverified,
            code,
            evidence);

    private static MutationProviderResult FailedDefinitive(
        string code) =>
        new(
            MutationExecutionResultKind.FailedDefinitive,
            code);

    private static MutationProviderResult Unknown(
        string code) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            code);

    private static SchemaMutationObservationResult<T>
        ObservationFailed<T>(
            SchemaMutationObservationFailureCategory category,
            string code,
            string safeMessage,
            bool retryable) =>
        SchemaMutationObservationResult<T>.Failed(
            new SchemaMutationObservationFailure(
                category,
                code,
                safeMessage,
                retryable));

    private sealed record RegistryRuntime(
        HttpClient Client,
        AuthenticationHeaderValue? BasicAuthorization);

    private sealed record DeletePathObservation(
        bool Exists,
        IReadOnlyList<int> Versions);

    private sealed record SchemaRequestBody(
        string SchemaType,
        string Schema,
        IReadOnlyList<SchemaReferenceBody> References);

    private sealed record SchemaReferenceBody(
        string Name,
        string Subject,
        int Version);

    private sealed record CompatibilityRequestBody(
        string Compatibility);

    private sealed class RegistryObservationStatusException :
        Exception
    {
        public RegistryObservationStatusException(
            SchemaMutationObservationFailureCategory category,
            string code,
            string safeMessage,
            bool retryable)
        {
            Category = category;
            Code = code;
            SafeMessage = safeMessage;
            Retryable = retryable;
        }

        public SchemaMutationObservationFailureCategory Category { get; }
        public string Code { get; }
        public string SafeMessage { get; }
        public bool Retryable { get; }
    }

    private sealed class RegistryMutationStatusException :
        Exception
    {
        public RegistryMutationStatusException(
            string code,
            bool Ambiguous)
        {
            Code = code;
            this.Ambiguous = Ambiguous;
        }

        public string Code { get; }
        public bool Ambiguous { get; }
    }
}
