using System.Text.Json;
using System.Text.RegularExpressions;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Ecosystem;

public sealed class KafkaConnectReadAdapter : IConnectReadPort, IDisposable
{
    private const int MaxConnectorNameLength = 512;
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

        var normalizedProfileId = NormalizeProfileId(connectProfileId);
        if (normalizedProfileId is null)
        {
            return Invalid<T>(
                "invalid_connect_profile_id",
                "Kafka Connect profile ID is invalid.");
        }

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
