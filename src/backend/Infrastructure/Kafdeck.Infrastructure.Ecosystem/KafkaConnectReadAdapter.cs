using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Ecosystem;

public sealed class KafkaConnectReadAdapter : IConnectReadPort, IDisposable
{
    private const int MaxConnectorNameLength = 512;
    private const int MaxConnectorClassLength = 1024;
    private const int MaxPluginTextLength = 512;
    private const int MaxValidationConfigurationItems = 256;
    private const int MaxValidationConfigurationKeyLength = 512;
    private const int MaxValidationConfigurationValueBytes = 64 * 1024;
    private const int MaxValidationRequestBytes = 1024 * 1024;
    private const int MaxValidationMessagesPerField = 32;
    private const int MaxTraceCharacters = 8_192;
    private const int DefaultMaxConcurrencyPerProfile = 4;
    private readonly IReadOnlyDictionary<ConnectRuntimeKey, HttpReadRuntime> _runtimes;
    private readonly IReadOnlyDictionary<ConnectRuntimeKey, SemaphoreSlim> _gates;
    private readonly IReadOnlyDictionary<string, IReadOnlyList<ConnectProfileSummary>> _profilesByCluster;
    private readonly TimeProvider _timeProvider;

    public KafkaConnectReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
        : this(
            clusterProfiles,
            secretResolver,
            static (_, _) => new HttpClientHandler { AllowAutoRedirect = false },
            timeProvider,
            DefaultMaxConcurrencyPerProfile)
    {
    }

    internal KafkaConnectReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        Func<ClusterProfile, HttpMessageHandler> handlerFactory,
        TimeProvider? timeProvider = null,
        int maxConcurrencyPerCluster = DefaultMaxConcurrencyPerProfile)
        : this(
            clusterProfiles,
            secretResolver,
            (cluster, _) => handlerFactory(cluster),
            timeProvider,
            maxConcurrencyPerCluster)
    {
    }

    internal KafkaConnectReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        Func<ClusterProfile, KafkaConnectProfile, HttpMessageHandler> handlerFactory,
        TimeProvider? timeProvider = null,
        int maxConcurrencyPerProfile = DefaultMaxConcurrencyPerProfile)
    {
        ArgumentNullException.ThrowIfNull(clusterProfiles);
        ArgumentNullException.ThrowIfNull(secretResolver);
        ArgumentNullException.ThrowIfNull(handlerFactory);

        if (maxConcurrencyPerProfile is < 1 or > 64)
        {
            throw new ArgumentOutOfRangeException(nameof(maxConcurrencyPerProfile));
        }

        _timeProvider = timeProvider ?? TimeProvider.System;

        var bindings = clusterProfiles
            .SelectMany(cluster =>
                KafkaConnectProfileSet.Effective(cluster)
                    .Select(profile => new ConnectProfileBinding(cluster, profile)))
            .ToArray();

        _runtimes = bindings.ToDictionary(
            binding => new ConnectRuntimeKey(
                binding.Cluster.Id,
                binding.Profile.Id),
            binding => ReadOnlyHttpSupport.CreateRuntime(
                binding.Profile.Url,
                binding.Profile.Username,
                binding.Profile.Password,
                secretResolver,
                handlerFactory(binding.Cluster, binding.Profile)));

        _gates = _runtimes.Keys.ToDictionary(
            key => key,
            _ => new SemaphoreSlim(
                maxConcurrencyPerProfile,
                maxConcurrencyPerProfile));

        _profilesByCluster = bindings
            .GroupBy(
                binding => binding.Cluster.Id,
                StringComparer.Ordinal)
            .ToDictionary(
                group => group.Key,
                group => (IReadOnlyList<ConnectProfileSummary>)group
                    .Select(binding => new ConnectProfileSummary(
                        binding.Profile.Id,
                        string.Equals(
                            binding.Profile.Id,
                            KafkaConnectProfileSet.DefaultProfileId,
                            StringComparison.Ordinal),
                        binding.Profile.MutationProviderProfile.ToString()))
                    .OrderBy(profile => profile.Id, StringComparer.Ordinal)
                    .ToArray(),
                StringComparer.Ordinal);
    }

    public Task<ReadViewResult<IReadOnlyList<ConnectProfileSummary>>> ListProfilesAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromResult(Failed<IReadOnlyList<ConnectProfileSummary>>(
                ReadViewFailureCategory.Cancelled,
                "operation_cancelled",
                "Kafka Connect profile read was cancelled.",
                false));
        }

        if (operation.DeadlineUtc <= _timeProvider.GetUtcNow())
        {
            return Task.FromResult(Failed<IReadOnlyList<ConnectProfileSummary>>(
                ReadViewFailureCategory.Timeout,
                "deadline_exceeded",
                "Kafka Connect profile read exceeded its deadline.",
                true));
        }

        if (!_profilesByCluster.TryGetValue(clusterId, out var profiles))
        {
            return Task.FromResult(Failed<IReadOnlyList<ConnectProfileSummary>>(
                ReadViewFailureCategory.NotConfigured,
                "connect_not_configured",
                "Kafka Connect is not configured for the requested cluster.",
                false));
        }

        if (profiles.Count > operation.MaxItems)
        {
            return Task.FromResult(Failed<IReadOnlyList<ConnectProfileSummary>>(
                ReadViewFailureCategory.ResponseTooLarge,
                "connect_profiles_response_too_large",
                "Kafka Connect profile list exceeded the configured bound.",
                false));
        }

        return Task.FromResult(
            ReadViewResult<IReadOnlyList<ConnectProfileSummary>>
                .Success(profiles));
    }

    public Task<ReadViewResult<IReadOnlyList<ConnectPluginSummary>>> ListPluginsAsync(
        string clusterId,
        string connectProfileId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        ExecuteAsync<IReadOnlyList<ConnectPluginSummary>>(
            clusterId,
            connectProfileId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var root = await ReadOnlyHttpSupport.GetJsonAsync(
                        runtime,
                        "connector-plugins",
                        operation.MaxResponseBytes,
                        token)
                    .ConfigureAwait(false);

                if (root.ValueKind != JsonValueKind.Array)
                {
                    throw new JsonException(
                        "Kafka Connect plugin list must be an array.");
                }

                var plugins = new List<ConnectPluginSummary>();
                foreach (var item in root.EnumerateArray())
                {
                    if (plugins.Count >= operation.MaxItems)
                    {
                        throw new ResponseBoundExceededException();
                    }

                    if (item.ValueKind != JsonValueKind.Object)
                    {
                        throw new JsonException(
                            "Kafka Connect plugin entry is invalid.");
                    }

                    var className = GetRequiredBoundedString(
                        item,
                        "class",
                        MaxConnectorClassLength);
                    var type = GetRequiredBoundedString(
                        item,
                        "type",
                        MaxPluginTextLength);
                    var version = GetOptionalBoundedString(
                        item,
                        "version",
                        MaxPluginTextLength);

                    plugins.Add(
                        new ConnectPluginSummary(
                            className,
                            type,
                            version));
                }

                return plugins
                    .OrderBy(plugin => plugin.Class, StringComparer.Ordinal)
                    .ToArray();
            });

    public Task<ReadViewResult<ConnectPluginValidationResult>> ValidateConfigurationAsync(
        string clusterId,
        string connectProfileId,
        string connectorClass,
        IReadOnlyDictionary<string, string> configuration,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken)
    {
        string normalizedClass;
        IReadOnlyDictionary<string, string> normalizedConfiguration;
        try
        {
            normalizedClass = NormalizeConnectorClass(connectorClass);
            normalizedConfiguration = NormalizeValidationConfiguration(
                configuration);
        }
        catch (ArgumentException)
        {
            return Task.FromResult(
                Invalid<ConnectPluginValidationResult>(
                    "invalid_connect_plugin_validation_request",
                    "Kafka Connect plugin validation request is invalid."));
        }

        return ExecuteAsync(
            clusterId,
            connectProfileId,
            operation,
            cancellationToken,
            (runtime, token) => ValidatePluginConfigurationAsync(
                runtime,
                normalizedClass,
                normalizedConfiguration,
                operation,
                token));
    }

    public Task<ReadViewResult<ConnectClusterInfo>> GetClusterInfoAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        GetClusterInfoAsync(
            clusterId,
            KafkaConnectProfileSet.DefaultProfileId,
            operation,
            cancellationToken);

    public Task<ReadViewResult<ConnectClusterInfo>> GetClusterInfoAsync(
        string clusterId,
        string connectProfileId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        ExecuteAsync(
            clusterId,
            connectProfileId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var root = await ReadOnlyHttpSupport.GetJsonAsync(
                        runtime,
                        string.Empty,
                        operation.MaxResponseBytes,
                        token)
                    .ConfigureAwait(false);

                if (root.ValueKind != JsonValueKind.Object)
                {
                    throw new JsonException("Connect root response must be an object.");
                }

                return new ConnectClusterInfo(
                    GetOptionalString(root, "version"),
                    GetOptionalString(root, "commit"),
                    GetOptionalString(root, "kafka_cluster_id"));
            });

    public Task<ReadViewResult<IReadOnlyList<ConnectConnectorSummary>>> ListConnectorsAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        ListConnectorsAsync(
            clusterId,
            KafkaConnectProfileSet.DefaultProfileId,
            operation,
            cancellationToken);

    public Task<ReadViewResult<IReadOnlyList<ConnectConnectorSummary>>> ListConnectorsAsync(
        string clusterId,
        string connectProfileId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        ExecuteAsync<IReadOnlyList<ConnectConnectorSummary>>(
            clusterId,
            connectProfileId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var root = await ReadOnlyHttpSupport.GetJsonAsync(
                        runtime,
                        "connectors",
                        operation.MaxResponseBytes,
                        token)
                    .ConfigureAwait(false);

                if (root.ValueKind != JsonValueKind.Array)
                {
                    throw new JsonException("Connect connector list must be an array.");
                }

                var names = root.EnumerateArray()
                    .Select(item => item.ValueKind == JsonValueKind.String
                        ? NormalizeConnectorName(item.GetString())
                        : null)
                    .Where(name => name is not null)
                    .Select(name => name!)
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(name => name, StringComparer.Ordinal)
                    .Take(operation.MaxItems + 1)
                    .ToArray();

                if (names.Length > operation.MaxItems)
                {
                    throw new ResponseBoundExceededException();
                }

                return names
                    .Select(name => new ConnectConnectorSummary(name))
                    .ToArray();
            });

    public Task<ReadViewResult<ConnectConnectorDetail>> GetConnectorAsync(
        string clusterId,
        string connectorName,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        GetConnectorAsync(
            clusterId,
            KafkaConnectProfileSet.DefaultProfileId,
            connectorName,
            operation,
            cancellationToken);

    public Task<ReadViewResult<ConnectConnectorDetail>> GetConnectorAsync(
        string clusterId,
        string connectProfileId,
        string connectorName,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken)
    {
        var normalized = NormalizeConnectorName(connectorName);
        if (normalized is null)
        {
            return Task.FromResult(Invalid<ConnectConnectorDetail>(
                "invalid_connector_name",
                "Kafka Connect connector name is invalid."));
        }

        return ExecuteAsync(
            clusterId,
            connectProfileId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var escaped = Uri.EscapeDataString(normalized);
                var status = await ReadOnlyHttpSupport.GetJsonAsync(
                        runtime,
                        $"connectors/{escaped}/status",
                        operation.MaxResponseBytes,
                        token)
                    .ConfigureAwait(false);

                var config = await ReadOnlyHttpSupport.GetJsonAsync(
                        runtime,
                        $"connectors/{escaped}/config",
                        operation.MaxResponseBytes,
                        token)
                    .ConfigureAwait(false);

                return ProjectConnector(
                    normalized,
                    status,
                    config,
                    operation.MaxItems);
            });
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

    internal static ConnectConnectorDetail ProjectConnector(
        string connectorName,
        JsonElement status,
        JsonElement config,
        int maxItems)
    {
        if (status.ValueKind != JsonValueKind.Object ||
            config.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException("Connect connector response is invalid.");
        }

        if (!status.TryGetProperty("connector", out var connectorStatus) ||
            connectorStatus.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException("Connect connector status is missing.");
        }

        var connectorState = GetOptionalString(connectorStatus, "state") ?? "UNKNOWN";
        var workerId = GetOptionalString(connectorStatus, "worker_id");

        var tasks = new List<ConnectTaskStatus>();
        if (status.TryGetProperty("tasks", out var taskArray))
        {
            if (taskArray.ValueKind != JsonValueKind.Array)
            {
                throw new JsonException("Connect task status must be an array.");
            }

            foreach (var task in taskArray.EnumerateArray())
            {
                if (tasks.Count >= maxItems)
                {
                    throw new ResponseBoundExceededException();
                }

                if (task.ValueKind != JsonValueKind.Object ||
                    !task.TryGetProperty("id", out var idElement) ||
                    !idElement.TryGetInt32(out var id))
                {
                    throw new JsonException("Connect task status is invalid.");
                }

                tasks.Add(new ConnectTaskStatus(
                    id,
                    GetOptionalString(task, "state") ?? "UNKNOWN",
                    GetOptionalString(task, "worker_id"),
                    SanitizeTrace(GetOptionalString(task, "trace"))));
            }
        }

        var safeConfig = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (var property in config.EnumerateObject())
        {
            if (safeConfig.Count >= maxItems)
            {
                throw new ResponseBoundExceededException();
            }

            safeConfig[property.Name] = IsExplicitlySafeConfigKey(property.Name)
                ? SafeConfigValue(property.Value)
                : "[REDACTED]";
        }

        return new ConnectConnectorDetail(
            connectorName,
            connectorState,
            workerId,
            tasks,
            safeConfig);
    }

    internal static bool IsExplicitlySafeConfigKey(string key) =>
        ConnectSafeConfigurationPolicy.IsExplicitlySafeConfigKey(key);

    internal static bool IsSecretKey(string key) =>
        ConnectSafeConfigurationPolicy.IsSecretKey(key);

    internal static string? SanitizeTrace(string? trace)
    {
        if (string.IsNullOrWhiteSpace(trace))
        {
            return null;
        }

        var normalized = trace
            .Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace('\r', '\n');

        var safeLines = normalized
            .Split('\n')
            .Where(line =>
                !line.Contains("authorization:", StringComparison.OrdinalIgnoreCase) &&
                !line.Contains("bearer ", StringComparison.OrdinalIgnoreCase) &&
                !line.Contains("basic ", StringComparison.OrdinalIgnoreCase))
            .Select(line => Regex.Replace(
                line,
                @"(?<key>[A-Za-z0-9_.-]+)\s*[:=]\s*(?<value>""[^""]*""|'[^']*'|[^\s,;]+)",
                match => IsSecretKey(match.Groups["key"].Value)
                    ? $"{match.Groups["key"].Value}=[REDACTED]"
                    : match.Value,
                RegexOptions.CultureInvariant,
                TimeSpan.FromMilliseconds(50)))
            .ToArray();

        var result = string.Join("\n", safeLines);
        return result.Length <= MaxTraceCharacters
            ? result
            : result[..MaxTraceCharacters] + "…";
    }

    private async Task<ReadViewResult<T>> ExecuteAsync<T>(
        string clusterId,
        string connectProfileId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken,
        Func<HttpReadRuntime, CancellationToken, Task<T>> action)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        var normalizedProfileId = NormalizeProfileId(connectProfileId);
        if (normalizedProfileId is null)
        {
            return Invalid<T>(
                "invalid_connect_profile_id",
                "Kafka Connect profile ID is invalid.");
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(ReadViewFailureCategory.Cancelled, "operation_cancelled", "Kafka Connect read operation was cancelled.", false);
        }

        if (operation.DeadlineUtc <= _timeProvider.GetUtcNow())
        {
            return Failed<T>(ReadViewFailureCategory.Timeout, "deadline_exceeded", "Kafka Connect read operation exceeded its deadline.", true);
        }

        var key = new ConnectRuntimeKey(
            clusterId,
            normalizedProfileId);
        if (!_runtimes.TryGetValue(key, out var runtime) ||
            !_gates.TryGetValue(key, out var gate))
        {
            var isDefault = string.Equals(
                normalizedProfileId,
                KafkaConnectProfileSet.DefaultProfileId,
                StringComparison.Ordinal);
            return Failed<T>(
                ReadViewFailureCategory.NotConfigured,
                isDefault
                    ? "connect_not_configured"
                    : "connect_profile_not_configured",
                isDefault
                    ? "Kafka Connect is not configured for the requested cluster."
                    : "Kafka Connect profile is not configured for the requested cluster.",
                false);
        }

        var gateAcquired = false;
        try
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var remaining = operation.DeadlineUtc - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                return Failed<T>(ReadViewFailureCategory.Timeout, "deadline_exceeded", "Kafka Connect read operation exceeded its deadline.", true);
            }

            deadline.CancelAfter(remaining);
            await gate.WaitAsync(deadline.Token).ConfigureAwait(false);
            gateAcquired = true;

            var value = await action(runtime, deadline.Token).ConfigureAwait(false);
            return ReadViewResult<T>.Success(value);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(ReadViewFailureCategory.Cancelled, "operation_cancelled", "Kafka Connect read operation was cancelled.", false);
        }
        catch (OperationCanceledException)
        {
            return Failed<T>(ReadViewFailureCategory.Timeout, "deadline_exceeded", "Kafka Connect read operation exceeded its deadline.", true);
        }
        catch (ReadViewHttpException exception)
        {
            return Failed<T>(exception.Category, exception.Code, exception.SafeMessage, exception.Retryable);
        }
        catch (ResponseBoundExceededException)
        {
            return Failed<T>(ReadViewFailureCategory.ResponseTooLarge, "connect_response_too_large", "Kafka Connect response exceeded the configured bound.", false);
        }
        catch (HttpRequestException)
        {
            return Failed<T>(ReadViewFailureCategory.Unavailable, "connect_unavailable", "Kafka Connect is temporarily unavailable.", true);
        }
        catch (JsonException)
        {
            return Failed<T>(ReadViewFailureCategory.InvalidResponse, "invalid_connect_response", "Kafka Connect returned an invalid response.", false);
        }
        finally
        {
            if (gateAcquired)
            {
                gate.Release();
            }
        }
    }

    private static async Task<ConnectPluginValidationResult>
        ValidatePluginConfigurationAsync(
            HttpReadRuntime runtime,
            string connectorClass,
            IReadOnlyDictionary<string, string> configuration,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(configuration);
        if (body.Length > MaxValidationRequestBytes)
        {
            throw new ResponseBoundExceededException();
        }

        using var request = new HttpRequestMessage(
            HttpMethod.Put,
            $"connector-plugins/{Uri.EscapeDataString(connectorClass)}/config/validate");
        request.Headers.Accept.ParseAdd("application/json");
        if (runtime.Authorization is not null)
        {
            request.Headers.Authorization = runtime.Authorization;
        }

        request.Content = new ByteArrayContent(body);
        request.Content.Headers.ContentType =
            new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");

        using var response = await runtime.Client.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        if ((int)response.StatusCode is >= 300 and < 400)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.InvalidResponse,
                "connect_plugin_validation_redirect_rejected",
                "Kafka Connect plugin validation redirect was rejected.",
                false);
        }

        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.Unsupported,
                "connect_plugin_validation_unsupported",
                "Kafka Connect plugin validation is unsupported for the requested plugin.",
                false);
        }

        if (response.StatusCode is
            System.Net.HttpStatusCode.Unauthorized or
            System.Net.HttpStatusCode.Forbidden)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.Unauthorized,
                "connect_plugin_validation_authorization_denied",
                "Kafka Connect denied plugin validation.",
                false);
        }

        if ((int)response.StatusCode >= 500)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.Unavailable,
                "connect_plugin_validation_unavailable",
                "Kafka Connect plugin validation is temporarily unavailable.",
                true);
        }

        if (!response.IsSuccessStatusCode)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.InvalidResponse,
                "connect_plugin_validation_rejected",
                "Kafka Connect rejected the plugin validation request.",
                false);
        }

        var bytes = await ReadBoundedContentAsync(
                response.Content,
                operation.MaxResponseBytes,
                cancellationToken)
            .ConfigureAwait(false);

        using var document = JsonDocument.Parse(bytes);
        return ProjectPluginValidation(
            connectorClass,
            document.RootElement,
            operation.MaxItems);
    }

    internal static ConnectPluginValidationResult ProjectPluginValidation(
        string connectorClass,
        JsonElement root,
        int maxItems)
    {
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty("error_count", out var errorCountElement) ||
            !errorCountElement.TryGetInt32(out var errorCount) ||
            errorCount < 0 ||
            !root.TryGetProperty("configs", out var configs) ||
            configs.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException(
                "Kafka Connect plugin validation response is invalid.");
        }

        var fields = new List<ConnectPluginValidationField>();
        foreach (var config in configs.EnumerateArray())
        {
            if (fields.Count >= maxItems)
            {
                throw new ResponseBoundExceededException();
            }

            if (config.ValueKind != JsonValueKind.Object ||
                !config.TryGetProperty("definition", out var definition) ||
                definition.ValueKind != JsonValueKind.Object)
            {
                throw new JsonException(
                    "Kafka Connect plugin validation field is invalid.");
            }

            var name = GetRequiredBoundedString(
                definition,
                "name",
                MaxValidationConfigurationKeyLength);
            var type = GetRequiredBoundedString(
                definition,
                "type",
                MaxPluginTextLength);
            var required = definition.TryGetProperty(
                    "required",
                    out var requiredElement) &&
                requiredElement.ValueKind == JsonValueKind.True;

            var errors = Array.Empty<string>();
            var recommended = Array.Empty<string>();
            if (config.TryGetProperty("value", out var value) &&
                value.ValueKind == JsonValueKind.Object)
            {
                errors = ReadBoundedStringArray(
                    value,
                    "errors",
                    MaxValidationMessagesPerField,
                    MaxPluginTextLength);
                recommended = ReadBoundedStringArray(
                    value,
                    "recommended_values",
                    MaxValidationMessagesPerField,
                    MaxPluginTextLength);
            }

            fields.Add(
                new ConnectPluginValidationField(
                    name,
                    type,
                    required,
                    errors,
                    recommended));
        }

        return new ConnectPluginValidationResult(
            connectorClass,
            errorCount,
            fields);
    }

    private static IReadOnlyDictionary<string, string>
        NormalizeValidationConfiguration(
            IReadOnlyDictionary<string, string> configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        if (configuration.Count is < 1 or > MaxValidationConfigurationItems)
        {
            throw new ArgumentOutOfRangeException(nameof(configuration));
        }

        long totalBytes = 0;
        var normalized = new SortedDictionary<string, string>(
            StringComparer.Ordinal);

        foreach (var pair in configuration)
        {
            if (string.IsNullOrWhiteSpace(pair.Key))
            {
                throw new ArgumentException(
                    "Kafka Connect configuration key is invalid.",
                    nameof(configuration));
            }

            var key = pair.Key.Trim();
            if (!string.Equals(key, pair.Key, StringComparison.Ordinal) ||
                key.Length > MaxValidationConfigurationKeyLength ||
                key.Any(char.IsControl))
            {
                throw new ArgumentException(
                    "Kafka Connect configuration key is invalid.",
                    nameof(configuration));
            }

            ArgumentNullException.ThrowIfNull(pair.Value);
            var valueBytes = Encoding.UTF8.GetByteCount(pair.Value);
            if (valueBytes > MaxValidationConfigurationValueBytes)
            {
                throw new ArgumentOutOfRangeException(nameof(configuration));
            }

            totalBytes = checked(
                totalBytes +
                Encoding.UTF8.GetByteCount(key) +
                valueBytes);
            if (totalBytes > MaxValidationRequestBytes)
            {
                throw new ArgumentOutOfRangeException(nameof(configuration));
            }

            normalized.Add(key, pair.Value);
        }

        return normalized;
    }

    private static string NormalizeConnectorClass(string connectorClass)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectorClass);
        var normalized = connectorClass.Trim();
        if (!string.Equals(
                normalized,
                connectorClass,
                StringComparison.Ordinal) ||
            normalized.Length > MaxConnectorClassLength ||
            normalized.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Kafka Connect connector class is invalid.",
                nameof(connectorClass));
        }

        return normalized;
    }

    private static string GetRequiredBoundedString(
        JsonElement root,
        string propertyName,
        int maxLength)
    {
        if (!root.TryGetProperty(propertyName, out var value) ||
            value.ValueKind != JsonValueKind.String)
        {
            throw new JsonException(
                "Kafka Connect response string field is invalid.");
        }

        var text = value.GetString();
        if (string.IsNullOrWhiteSpace(text) ||
            text.Length > maxLength ||
            text.Any(char.IsControl))
        {
            throw new JsonException(
                "Kafka Connect response string field is invalid.");
        }

        return text;
    }

    private static string? GetOptionalBoundedString(
        JsonElement root,
        string propertyName,
        int maxLength)
    {
        if (!root.TryGetProperty(propertyName, out var value) ||
            value.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        if (value.ValueKind != JsonValueKind.String)
        {
            throw new JsonException(
                "Kafka Connect response string field is invalid.");
        }

        var text = value.GetString();
        if (text is null ||
            text.Length > maxLength ||
            text.Any(char.IsControl))
        {
            throw new JsonException(
                "Kafka Connect response string field is invalid.");
        }

        return text;
    }

    private static string[] ReadBoundedStringArray(
        JsonElement root,
        string propertyName,
        int maxItems,
        int maxLength)
    {
        if (!root.TryGetProperty(propertyName, out var value))
        {
            return Array.Empty<string>();
        }

        if (value.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException(
                "Kafka Connect validation list is invalid.");
        }

        var result = new List<string>();
        foreach (var item in value.EnumerateArray())
        {
            if (result.Count >= maxItems ||
                item.ValueKind != JsonValueKind.String)
            {
                throw new JsonException(
                    "Kafka Connect validation list exceeded its bound.");
            }

            var text = item.GetString();
            if (text is null ||
                text.Length > maxLength ||
                text.Any(char.IsControl))
            {
                throw new JsonException(
                    "Kafka Connect validation list item is invalid.");
            }

            result.Add(text);
        }

        return result.ToArray();
    }

    private static async Task<byte[]> ReadBoundedContentAsync(
        HttpContent content,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        if (content.Headers.ContentLength is long declared &&
            declared > maxBytes)
        {
            throw new ResponseBoundExceededException();
        }

        await using var stream = await content.ReadAsStreamAsync(
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

    private static string? NormalizeProfileId(string? profileId)
    {
        if (string.IsNullOrWhiteSpace(profileId))
        {
            return null;
        }

        var normalized = profileId.Trim();
        return normalized.Length <= KafkaConnectProfileSet.MaxProfileIdLength &&
               !normalized.Any(char.IsControl) &&
               normalized.All(character =>
                   char.IsLetterOrDigit(character) ||
                   character is '-' or '_' or '.')
            ? normalized
            : null;
    }

    private static string? NormalizeConnectorName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        var normalized = name.Trim();
        return normalized.Length <= MaxConnectorNameLength &&
               !normalized.Any(char.IsControl)
            ? normalized
            : null;
    }

    private static string? SafeConfigValue(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.String => value.GetString(),
        JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False => value.ToString(),
        JsonValueKind.Null => null,
        _ => "[NON_SCALAR]",
    };

    private static string? GetOptionalString(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var value) || value.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        return value.ValueKind == JsonValueKind.String ? value.GetString() : value.ToString();
    }

    private static ReadViewResult<T> Invalid<T>(string code, string message) =>
        Failed<T>(ReadViewFailureCategory.InvalidRequest, code, message, false);

    private static ReadViewResult<T> Failed<T>(
        ReadViewFailureCategory category,
        string code,
        string message,
        bool retryable) =>
        ReadViewResult<T>.Failed(new ReadViewFailure(category, code, message, retryable));

    private readonly record struct ConnectRuntimeKey(
        string ClusterId,
        string ProfileId);

    private sealed record ConnectProfileBinding(
        ClusterProfile Cluster,
        KafkaConnectProfile Profile);
}
