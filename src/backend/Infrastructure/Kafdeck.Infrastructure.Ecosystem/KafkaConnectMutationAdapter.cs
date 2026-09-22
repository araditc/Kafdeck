using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Ecosystem;

public sealed class KafkaConnectMutationAdapter :
    IConnectMutationPort,
    IConnectMutationObservationPort,
    IDisposable
{
    private const int MaxConnectorNameCharacters = 512;
    private const int MaxConfigurationItems = 256;
    private const int MaxConfigurationKeyCharacters = 512;
    private const int MaxConfigurationValueBytes = 64 * 1024;
    private const int MaxConfigurationBytes = 1024 * 1024;
    private const int MaxTasks = 256;
    private const int MaxResponseBytes = 2 * 1024 * 1024;
    private const int MaxSafeValueCharacters = 4_096;

    private static readonly JsonSerializerOptions RequestJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly IReadOnlyDictionary<string, HttpReadRuntime> _runtimes;
    private readonly TimeProvider _timeProvider;

    public KafkaConnectMutationAdapter(
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

    internal KafkaConnectMutationAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        Func<ClusterProfile, HttpMessageHandler> handlerFactory,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(clusterProfiles);
        ArgumentNullException.ThrowIfNull(secretResolver);
        ArgumentNullException.ThrowIfNull(handlerFactory);

        _timeProvider = timeProvider ?? TimeProvider.System;
        _runtimes = clusterProfiles
            .Where(profile => profile.Connect is not null)
            .ToDictionary(
                profile => profile.Id,
                profile =>
                {
                    var connect = profile.Connect!;
                    return ReadOnlyHttpSupport.CreateRuntime(
                        connect.Url,
                        connect.Username,
                        connect.Password,
                        secretResolver,
                        handlerFactory(profile));
                },
                StringComparer.Ordinal);
    }

    public Task<ConnectMutationObservationResult<ConnectMutationCapabilities>>
        GetCapabilitiesAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromResult(
                ObservationFailed<ConnectMutationCapabilities>(
                    ConnectMutationObservationFailureCategory.Cancelled,
                    "connect_operation_cancelled",
                    "Kafka Connect operation was cancelled.",
                    false));
        }

        if (operation.DeadlineUtc <= _timeProvider.GetUtcNow())
        {
            return Task.FromResult(
                ObservationFailed<ConnectMutationCapabilities>(
                    ConnectMutationObservationFailureCategory.Timeout,
                    "connect_deadline_exceeded",
                    "Kafka Connect operation exceeded its deadline.",
                    true));
        }

        if (!_runtimes.ContainsKey(clusterId))
        {
            return Task.FromResult(
                ObservationFailed<ConnectMutationCapabilities>(
                    ConnectMutationObservationFailureCategory.NotConfigured,
                    "connect_not_configured",
                    "Kafka Connect is not configured for the requested cluster.",
                    false));
        }

        return Task.FromResult(
            ConnectMutationObservationResult<ConnectMutationCapabilities>.Success(
                new ConnectMutationCapabilities(
                    SupportsCreate: true,
                    SupportsUpdate: true,
                    SupportsPause: true,
                    SupportsResume: true,
                    SupportsRestart: true,
                    SupportsTaskRestart: true,
                    SupportsDelete: true)));
    }

    public Task<ConnectMutationObservationResult<ConnectMutationObservation>>
        ObserveConnectorAsync(
            string clusterId,
            string connectorName,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
    {
        string normalized;
        try
        {
            normalized = RequireConnectorName(connectorName);
        }
        catch (ArgumentException)
        {
            return Task.FromResult(
                ObservationFailed<ConnectMutationObservation>(
                    ConnectMutationObservationFailureCategory.InvalidRequest,
                    "invalid_connector_name",
                    "Kafka Connect connector name is invalid.",
                    false));
        }

        return ExecuteObservationAsync(
            clusterId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var escaped = Uri.EscapeDataString(normalized);
                var status = await TryGetJsonAsync(
                        runtime,
                        $"connectors/{escaped}/status",
                        token)
                    .ConfigureAwait(false);

                if (!status.Exists)
                {
                    return MissingObservation(normalized);
                }

                var config = await TryGetJsonAsync(
                        runtime,
                        $"connectors/{escaped}/config",
                        token)
                    .ConfigureAwait(false);

                if (!config.Exists)
                {
                    throw new JsonException(
                        "Kafka Connect status/config existence is inconsistent.");
                }

                return ProjectObservation(
                    normalized,
                    status.Value,
                    config.Value);
            });
    }

    public Task<MutationProviderResult> CreateAsync(
        ConnectCreateMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string name;
        IReadOnlyDictionary<string, string> configuration;
        try
        {
            name = RequireConnectorName(request.ConnectorName);
            configuration = NormalizeConfiguration(request.Configuration);
        }
        catch (ArgumentException)
        {
            return Task.FromResult(
                FailedDefinitive(
                    "connect_create_invalid_request"));
        }

        return ExecuteMutationAsync(
            request.ClusterId,
            "connect_create",
            cancellationToken,
            async (runtime, token) =>
            {
                await SendMutationAsync(
                        runtime,
                        HttpMethod.Post,
                        "connectors",
                        new ConnectorCreateBody(
                            name,
                            configuration),
                        token)
                    .ConfigureAwait(false);

                return Accepted(
                    "connect_create_accepted");
            });
    }

    public Task<MutationProviderResult> AlterAsync(
        ConnectAlterMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string name;
        IReadOnlyDictionary<string, string> configuration;
        try
        {
            name = RequireConnectorName(request.ConnectorName);
            configuration = NormalizeConfiguration(request.Configuration);
        }
        catch (ArgumentException)
        {
            return Task.FromResult(
                FailedDefinitive(
                    "connect_update_invalid_request"));
        }

        return ExecuteMutationAsync(
            request.ClusterId,
            "connect_update",
            cancellationToken,
            async (runtime, token) =>
            {
                await SendMutationAsync(
                        runtime,
                        HttpMethod.Put,
                        $"connectors/{Uri.EscapeDataString(name)}/config",
                        configuration,
                        token)
                    .ConfigureAwait(false);

                return Accepted(
                    "connect_update_accepted");
            });
    }

    public Task<MutationProviderResult> ControlAsync(
        ConnectControlMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string name;
        HttpMethod method;
        string path;
        string operationCode;

        try
        {
            name = RequireConnectorName(request.ConnectorName);
            var escaped = Uri.EscapeDataString(name);

            switch (request.Action)
            {
                case ConnectControlAction.Pause
                    when request.TaskId is null:
                    method = HttpMethod.Put;
                    path = $"connectors/{escaped}/pause";
                    operationCode = "connect_pause";
                    break;

                case ConnectControlAction.Resume
                    when request.TaskId is null:
                    method = HttpMethod.Put;
                    path = $"connectors/{escaped}/resume";
                    operationCode = "connect_resume";
                    break;

                case ConnectControlAction.Restart
                    when request.TaskId is null:
                    method = HttpMethod.Post;
                    path = $"connectors/{escaped}/restart";
                    operationCode = "connect_restart";
                    break;

                case ConnectControlAction.Restart
                    when request.TaskId is >= 0:
                    method = HttpMethod.Post;
                    path =
                        $"connectors/{escaped}/tasks/{request.TaskId.Value.ToString(CultureInfo.InvariantCulture)}/restart";
                    operationCode = "connect_task_restart";
                    break;

                default:
                    throw new ArgumentException(
                        "Kafka Connect control request is invalid.");
            }
        }
        catch (ArgumentException)
        {
            return Task.FromResult(
                FailedDefinitive(
                    "connect_control_invalid_request"));
        }

        return ExecuteMutationAsync(
            request.ClusterId,
            operationCode,
            cancellationToken,
            async (runtime, token) =>
            {
                await SendMutationAsync(
                        runtime,
                        method,
                        path,
                        body: null,
                        token)
                    .ConfigureAwait(false);

                return Accepted(
                    $"{operationCode}_accepted");
            });
    }

    public Task<MutationProviderResult> DeleteAsync(
        ConnectDeleteMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string name;
        try
        {
            name = RequireConnectorName(request.ConnectorName);
        }
        catch (ArgumentException)
        {
            return Task.FromResult(
                FailedDefinitive(
                    "connect_delete_invalid_request"));
        }

        return ExecuteMutationAsync(
            request.ClusterId,
            "connect_delete",
            cancellationToken,
            async (runtime, token) =>
            {
                await SendMutationAsync(
                        runtime,
                        HttpMethod.Delete,
                        $"connectors/{Uri.EscapeDataString(name)}",
                        body: null,
                        token)
                    .ConfigureAwait(false);

                return Accepted(
                    "connect_delete_accepted");
            });
    }

    public void Dispose()
    {
        foreach (var runtime in _runtimes.Values)
        {
            runtime.Client.Dispose();
        }
    }

    internal static ConnectMutationObservation ProjectObservation(
        string connectorName,
        JsonElement status,
        JsonElement config)
    {
        if (status.ValueKind != JsonValueKind.Object ||
            config.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException(
                "Kafka Connect mutation observation is invalid.");
        }

        if (!status.TryGetProperty("connector", out var connector) ||
            connector.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException(
                "Kafka Connect connector status is missing.");
        }

        var state = GetOptionalString(
                connector,
                "state")
            ?.Trim()
            .ToUpperInvariant() ?? "UNKNOWN";

        var tasks = new List<ConnectMutationTaskObservation>();
        if (status.TryGetProperty("tasks", out var taskArray))
        {
            if (taskArray.ValueKind != JsonValueKind.Array)
            {
                throw new JsonException(
                    "Kafka Connect task status must be an array.");
            }

            foreach (var task in taskArray.EnumerateArray())
            {
                if (tasks.Count >= MaxTasks ||
                    task.ValueKind != JsonValueKind.Object ||
                    !task.TryGetProperty("id", out var idElement) ||
                    !idElement.TryGetInt32(out var id) ||
                    id < 0)
                {
                    throw new JsonException(
                        "Kafka Connect task status exceeds bounds or is invalid.");
                }

                var taskState = GetOptionalString(
                        task,
                        "state")
                    ?.Trim()
                    .ToUpperInvariant() ?? "UNKNOWN";

                tasks.Add(
                    new ConnectMutationTaskObservation(
                        id,
                        taskState));
            }
        }

        var items =
            new List<ConnectConfigurationObservationItem>();
        long totalBytes = 0;

        foreach (var property in config.EnumerateObject())
        {
            if (items.Count >= MaxConfigurationItems)
            {
                throw new JsonException(
                    "Kafka Connect configuration exceeds the admitted item bound.");
            }

            var key = RequireConfigurationKey(property.Name);
            var value = ToCanonicalProviderValue(property.Value);
            var valueBytes = Encoding.UTF8.GetByteCount(value.HashInput);
            totalBytes = checked(
                totalBytes +
                Encoding.UTF8.GetByteCount(key) +
                valueBytes);

            if (valueBytes > MaxConfigurationValueBytes ||
                totalBytes > MaxConfigurationBytes)
            {
                throw new JsonException(
                    "Kafka Connect configuration exceeds admitted byte bounds.");
            }

            var digest = Sha256(
                Encoding.UTF8.GetBytes(value.HashInput));
            var safeValue =
                ConnectSafeConfigurationPolicy
                    .IsExplicitlySafeConfigKey(key) &&
                !ConnectSafeConfigurationPolicy.IsSecretKey(key)
                    ? SafePreviewValue(value.DisplayValue, digest)
                    : "[REDACTED]";

            items.Add(
                new ConnectConfigurationObservationItem(
                    key,
                    digest,
                    safeValue));
        }

        var sortedItems = items
            .OrderBy(item => item.Key, StringComparer.Ordinal)
            .ToArray();
        var fingerprint =
            ConfigurationFingerprint(sortedItems);

        return new ConnectMutationObservation(
            connectorName,
            true,
            state,
            Array.AsReadOnly(
                tasks
                    .OrderBy(item => item.Id)
                    .ToArray()),
            Array.AsReadOnly(sortedItems),
            fingerprint);
    }

    private async Task<ConnectMutationObservationResult<T>>
        ExecuteObservationAsync<T>(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken,
            Func<HttpReadRuntime, CancellationToken, Task<T>> action)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ObservationFailed<T>(
                ConnectMutationObservationFailureCategory.Cancelled,
                "connect_operation_cancelled",
                "Kafka Connect operation was cancelled.",
                false);
        }

        if (operation.DeadlineUtc <= _timeProvider.GetUtcNow())
        {
            return ObservationFailed<T>(
                ConnectMutationObservationFailureCategory.Timeout,
                "connect_deadline_exceeded",
                "Kafka Connect operation exceeded its deadline.",
                true);
        }

        if (!_runtimes.TryGetValue(
                clusterId,
                out var runtime))
        {
            return ObservationFailed<T>(
                ConnectMutationObservationFailureCategory.NotConfigured,
                "connect_not_configured",
                "Kafka Connect is not configured for the requested cluster.",
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
                    ConnectMutationObservationFailureCategory.Timeout,
                    "connect_deadline_exceeded",
                    "Kafka Connect operation exceeded its deadline.",
                    true);
            }

            deadline.CancelAfter(remaining);
            var value = await action(
                    runtime,
                    deadline.Token)
                .ConfigureAwait(false);

            return ConnectMutationObservationResult<T>.Success(
                value);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return ObservationFailed<T>(
                ConnectMutationObservationFailureCategory.Cancelled,
                "connect_operation_cancelled",
                "Kafka Connect operation was cancelled.",
                false);
        }
        catch (OperationCanceledException)
        {
            return ObservationFailed<T>(
                ConnectMutationObservationFailureCategory.Timeout,
                "connect_deadline_exceeded",
                "Kafka Connect operation exceeded its deadline.",
                true);
        }
        catch (ConnectObservationHttpException exception)
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
                ConnectMutationObservationFailureCategory.Unavailable,
                "connect_unavailable",
                "Kafka Connect is temporarily unavailable.",
                true);
        }
        catch (JsonException)
        {
            return ObservationFailed<T>(
                ConnectMutationObservationFailureCategory.InvalidResponse,
                "invalid_connect_response",
                "Kafka Connect returned an invalid response.",
                false);
        }
        catch (ArgumentException)
        {
            return ObservationFailed<T>(
                ConnectMutationObservationFailureCategory.InvalidRequest,
                "invalid_connect_request",
                "Kafka Connect request is invalid.",
                false);
        }
        catch
        {
            return ObservationFailed<T>(
                ConnectMutationObservationFailureCategory.InvalidResponse,
                "connect_observation_failed",
                "Kafka Connect observation failed.",
                false);
        }
    }

    private async Task<MutationProviderResult> ExecuteMutationAsync(
        string clusterId,
        string operationCode,
        CancellationToken cancellationToken,
        Func<HttpReadRuntime, CancellationToken, Task<MutationProviderResult>> action)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        if (cancellationToken.IsCancellationRequested)
        {
            return Unknown(
                $"{operationCode}_cancelled_or_timeout");
        }

        if (!_runtimes.TryGetValue(
                clusterId,
                out var runtime))
        {
            return FailedDefinitive(
                $"{operationCode}_not_configured");
        }

        try
        {
            return await action(
                    runtime,
                    cancellationToken)
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
                $"{operationCode}_unavailable");
        }
        catch (ConnectMutationHttpException exception)
        {
            return exception.Ambiguous
                ? Unknown(
                    $"{operationCode}_{exception.Code}")
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

    private static async Task<OptionalJson> TryGetJsonAsync(
        HttpReadRuntime runtime,
        string relativePath,
        CancellationToken cancellationToken)
    {
        using var request =
            BuildRequest(
                runtime,
                HttpMethod.Get,
                relativePath,
                body: null);
        using var response =
            await runtime.Client.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken)
                .ConfigureAwait(false);

        if ((int)response.StatusCode is >= 300 and < 400)
        {
            throw new ConnectObservationHttpException(
                ConnectMutationObservationFailureCategory.InvalidResponse,
                "connect_redirect_rejected",
                "Kafka Connect redirect was rejected.",
                false);
        }

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return OptionalJson.Missing();
        }

        if (response.StatusCode is
            HttpStatusCode.Unauthorized or
            HttpStatusCode.Forbidden)
        {
            throw new ConnectObservationHttpException(
                ConnectMutationObservationFailureCategory.Unauthorized,
                "connect_authorization_denied",
                "Kafka Connect denied the requested operation.",
                false);
        }

        if ((int)response.StatusCode >= 500)
        {
            throw new ConnectObservationHttpException(
                ConnectMutationObservationFailureCategory.Unavailable,
                "connect_unavailable",
                "Kafka Connect is temporarily unavailable.",
                true);
        }

        if (!response.IsSuccessStatusCode)
        {
            throw new ConnectObservationHttpException(
                ConnectMutationObservationFailureCategory.InvalidResponse,
                "connect_request_failed",
                "Kafka Connect rejected the requested observation.",
                false);
        }

        var root = await ReadJsonAsync(
                response,
                MaxResponseBytes,
                cancellationToken)
            .ConfigureAwait(false);
        return OptionalJson.Present(root);
    }

    private static async Task SendMutationAsync(
        HttpReadRuntime runtime,
        HttpMethod method,
        string relativePath,
        object? body,
        CancellationToken cancellationToken)
    {
        using var request =
            BuildRequest(
                runtime,
                method,
                relativePath,
                body);

        using var response =
            await runtime.Client.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken)
                .ConfigureAwait(false);

        if ((int)response.StatusCode is >= 300 and < 400)
        {
            throw new ConnectMutationHttpException(
                "redirect_rejected",
                Ambiguous: false);
        }

        if (response.StatusCode is
            HttpStatusCode.Unauthorized or
            HttpStatusCode.Forbidden)
        {
            throw new ConnectMutationHttpException(
                "authorization_denied",
                Ambiguous: false);
        }

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            throw new ConnectMutationHttpException(
                "resource_not_found",
                Ambiguous: false);
        }

        if (response.StatusCode is
            HttpStatusCode.BadRequest or
            HttpStatusCode.Conflict or
            HttpStatusCode.UnprocessableEntity)
        {
            throw new ConnectMutationHttpException(
                "rejected",
                Ambiguous: false);
        }

        if ((int)response.StatusCode >= 500 ||
            response.StatusCode is
                HttpStatusCode.RequestTimeout or
                HttpStatusCode.TooManyRequests)
        {
            throw new ConnectMutationHttpException(
                "ambiguous",
                Ambiguous: true);
        }

        if (!response.IsSuccessStatusCode)
        {
            throw new ConnectMutationHttpException(
                "unknown_status",
                Ambiguous: true);
        }

        if (response.Content.Headers.ContentLength is 0)
        {
            return;
        }

        _ = await ReadBoundedAsync(
                response.Content,
                MaxResponseBytes,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static HttpRequestMessage BuildRequest(
        HttpReadRuntime runtime,
        HttpMethod method,
        string relativePath,
        object? body)
    {
        var request =
            new HttpRequestMessage(
                method,
                relativePath);
        request.Headers.Accept.ParseAdd(
            "application/json");

        if (runtime.Authorization is not null)
        {
            request.Headers.Authorization =
                runtime.Authorization;
        }

        if (body is not null)
        {
            var json = JsonSerializer.Serialize(
                body,
                RequestJsonOptions);
            var bytes = Encoding.UTF8.GetByteCount(json);
            if (bytes > MaxConfigurationBytes * 2)
            {
                request.Dispose();
                throw new ArgumentOutOfRangeException(
                    nameof(body),
                    "Kafka Connect mutation request exceeds admitted bounds.");
            }

            request.Content =
                new StringContent(
                    json,
                    Encoding.UTF8,
                    "application/json");
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
        using var document =
            JsonDocument.Parse(bytes);
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
                "Kafka Connect response exceeded admitted bounds.");
        }

        await using var stream =
            await content.ReadAsStreamAsync(
                    cancellationToken)
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
                    "Kafka Connect response exceeded admitted bounds.");
            }

            buffer.Write(
                chunk,
                0,
                read);
        }

        return buffer.ToArray();
    }

    private static IReadOnlyDictionary<string, string>
        NormalizeConfiguration(
            IReadOnlyDictionary<string, string> configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        if (configuration.Count is < 1 or > MaxConfigurationItems)
        {
            throw new ArgumentOutOfRangeException(
                nameof(configuration));
        }

        long total = 0;
        var result = new SortedDictionary<string, string>(
            StringComparer.Ordinal);

        foreach (var pair in configuration)
        {
            var key = RequireConfigurationKey(pair.Key);
            ArgumentNullException.ThrowIfNull(pair.Value);
            var valueBytes =
                Encoding.UTF8.GetByteCount(pair.Value);

            if (valueBytes > MaxConfigurationValueBytes)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(configuration));
            }

            total = checked(
                total +
                Encoding.UTF8.GetByteCount(key) +
                valueBytes);
            if (total > MaxConfigurationBytes)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(configuration));
            }

            result.Add(
                key,
                pair.Value);
        }

        return result;
    }

    private static string RequireConnectorName(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();
        if (!string.Equals(
                value,
                normalized,
                StringComparison.Ordinal) ||
            normalized.Length > MaxConnectorNameCharacters ||
            normalized.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Kafka Connect connector name is invalid.",
                nameof(value));
        }

        return normalized;
    }

    private static string RequireConfigurationKey(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();
        if (!string.Equals(
                value,
                normalized,
                StringComparison.Ordinal) ||
            normalized.Length > MaxConfigurationKeyCharacters ||
            normalized.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Kafka Connect configuration key is invalid.",
                nameof(value));
        }

        return normalized;
    }

    private static ConnectMutationObservation MissingObservation(
        string connectorName)
    {
        var fingerprint = Sha256(
            Encoding.UTF8.GetBytes(
                $"missing:{connectorName}"));

        return new ConnectMutationObservation(
            connectorName,
            false,
            "MISSING",
            Array.Empty<ConnectMutationTaskObservation>(),
            Array.Empty<ConnectConfigurationObservationItem>(),
            fingerprint);
    }

    private static CanonicalProviderValue ToCanonicalProviderValue(
        JsonElement value) =>
        value.ValueKind switch
        {
            JsonValueKind.String =>
                new CanonicalProviderValue(
                    value.GetString() ?? string.Empty,
                    value.GetString() ?? string.Empty),

            JsonValueKind.Number =>
                new CanonicalProviderValue(
                    "\u0000number:" + value.ToString(),
                    value.ToString()),

            JsonValueKind.True or
            JsonValueKind.False =>
                new CanonicalProviderValue(
                    "\u0000boolean:" + value.ToString(),
                    value.ToString()),

            JsonValueKind.Null =>
                new CanonicalProviderValue(
                    "\u0000null",
                    string.Empty),

            _ => throw new JsonException(
                "Kafka Connect configuration values must be scalar."),
        };

    private static string ConfigurationFingerprint(
        IEnumerable<ConnectConfigurationObservationItem> values)
    {
        var builder = new StringBuilder();
        foreach (var item in values
                     .OrderBy(value => value.Key, StringComparer.Ordinal))
        {
            builder.Append(item.Key)
                .Append('=')
                .Append(
                    Convert.ToBase64String(
                        Encoding.UTF8.GetBytes(
                            item.ValueSha256)))
                .Append('\n');
        }

        return Sha256(
            Encoding.UTF8.GetBytes(
                builder.ToString()));
    }

    private static string SafePreviewValue(
        string value,
        string digest) =>
        value.Length <= MaxSafeValueCharacters
            ? value
            : $"[SAFE_VALUE_SHA256:{digest}]";

    private static string Sha256(
        ReadOnlySpan<byte> value) =>
        Convert.ToHexString(
                SHA256.HashData(value))
            .ToLowerInvariant();

    private static string? GetOptionalString(
        JsonElement root,
        string propertyName)
    {
        if (!root.TryGetProperty(
                propertyName,
                out var value) ||
            value.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        return value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : value.ToString();
    }

    private static MutationProviderResult Accepted(
        string code) =>
        new(
            MutationExecutionResultKind.AppliedUnverified,
            code,
            new Dictionary<string, string>(
                StringComparer.Ordinal)
            {
                ["provider.accepted"] = "true",
            });

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

    private static ConnectMutationObservationResult<T>
        ObservationFailed<T>(
            ConnectMutationObservationFailureCategory category,
            string code,
            string safeMessage,
            bool retryable) =>
        ConnectMutationObservationResult<T>.Failed(
            new ConnectMutationObservationFailure(
                category,
                code,
                safeMessage,
                retryable));

    private sealed record OptionalJson(
        bool Exists,
        JsonElement Value)
    {
        public static OptionalJson Missing() =>
            new(false, default);

        public static OptionalJson Present(
            JsonElement value) =>
            new(true, value);
    }

    private sealed record CanonicalProviderValue(
        string HashInput,
        string DisplayValue);

    private sealed record ConnectorCreateBody(
        string Name,
        IReadOnlyDictionary<string, string> Config);

    private sealed class ConnectObservationHttpException :
        Exception
    {
        public ConnectObservationHttpException(
            ConnectMutationObservationFailureCategory category,
            string code,
            string safeMessage,
            bool retryable)
        {
            Category = category;
            Code = code;
            SafeMessage = safeMessage;
            Retryable = retryable;
        }

        public ConnectMutationObservationFailureCategory Category { get; }
        public string Code { get; }
        public string SafeMessage { get; }
        public bool Retryable { get; }
    }

    private sealed class ConnectMutationHttpException :
        Exception
    {
        public ConnectMutationHttpException(
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
