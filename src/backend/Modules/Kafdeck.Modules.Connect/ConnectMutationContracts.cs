using System.Collections.ObjectModel;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Connect;

public enum ConnectMutationPlanningFailureCode
{
    InvalidInput = 1,
    LimitExceeded = 2,
    ProviderNotConfigured = 3,
    ProviderUnauthorized = 4,
    ProviderUnsupported = 5,
    ProviderUnavailable = 6,
    ObservationFailed = 7,
    ConnectorAlreadyExists = 8,
    ConnectorNotFound = 9,
    TaskNotFound = 10,
    NoChange = 11,
}

public enum ConnectConfigurationChangeKind
{
    Added = 1,
    Changed = 2,
    Removed = 3,
}

public enum ConnectAlterIntentKind
{
    ConfigurationUpdate = 1,
    Control = 2,
}

public sealed record ConnectMutationPlanningFailure(
    ConnectMutationPlanningFailureCode Code,
    string SafeMessage);

public sealed record ConnectCreateRequest(
    string ClusterId,
    string ConnectorName,
    IReadOnlyDictionary<string, string> Configuration);

public sealed record ConnectUpdateRequest(
    string ClusterId,
    string ConnectorName,
    IReadOnlyDictionary<string, string> Configuration);

public sealed record ConnectControlRequest(
    string ClusterId,
    string ConnectorName,
    ConnectControlAction Action,
    int? TaskId = null);

public sealed record ConnectDeleteRequest(
    string ClusterId,
    string ConnectorName);

public sealed record ConnectConfigurationCanonicalItem(
    string Key,
    string ValueSha256,
    string? SafeValue);

public sealed record ConnectConfigurationDiffItem(
    string Key,
    ConnectConfigurationChangeKind ChangeKind,
    string? CurrentSafeValue,
    string? RequestedSafeValue);

public sealed record ConnectCreateCanonicalIntent(
    string ClusterId,
    string ConnectorName,
    string MaterialName,
    string RequestedConfigurationFingerprint,
    IReadOnlyList<ConnectConfigurationCanonicalItem> RequestedConfiguration,
    string StateFingerprint);

public sealed record ConnectUpdateCanonicalIntent(
    ConnectAlterIntentKind AlterKind,
    string ClusterId,
    string ConnectorName,
    string MaterialName,
    string CurrentConfigurationFingerprint,
    string RequestedConfigurationFingerprint,
    IReadOnlyList<ConnectConfigurationCanonicalItem> RequestedConfiguration,
    IReadOnlyList<ConnectConfigurationDiffItem> Diff,
    string CurrentState,
    string StateFingerprint);

public sealed record ConnectControlCanonicalIntent(
    ConnectAlterIntentKind AlterKind,
    string ClusterId,
    string ConnectorName,
    ConnectControlAction Action,
    int? TaskId,
    string CurrentState,
    string? CurrentTaskState,
    string StateFingerprint);

public sealed record ConnectDeleteCanonicalIntent(
    string ClusterId,
    string ConnectorName,
    string CurrentState,
    string CurrentConfigurationFingerprint,
    string StateFingerprint);

public sealed record ConnectMutationPlan<TCanonical>(
    TCanonical Canonical,
    MutationIntentDescriptor Intent,
    MutationRiskDecision Risk)
    where TCanonical : class;

public sealed record ConnectMutationPlanningResult<TCanonical>
    where TCanonical : class
{
    private ConnectMutationPlanningResult(
        ConnectMutationPlan<TCanonical>? plan,
        MutationExecutionMaterial? executionMaterial,
        ConnectMutationPlanningFailure? failure)
    {
        Plan = plan;
        ExecutionMaterial = executionMaterial;
        Failure = failure;
    }

    public ConnectMutationPlan<TCanonical>? Plan { get; }
    public MutationExecutionMaterial? ExecutionMaterial { get; }
    public ConnectMutationPlanningFailure? Failure { get; }
    public bool IsSuccess => Plan is not null && Failure is null;

    public static ConnectMutationPlanningResult<TCanonical> Success(
        ConnectMutationPlan<TCanonical> plan,
        MutationExecutionMaterial? executionMaterial = null) =>
        new(
            plan ?? throw new ArgumentNullException(nameof(plan)),
            executionMaterial,
            null);

    public static ConnectMutationPlanningResult<TCanonical> Failed(
        ConnectMutationPlanningFailure failure) =>
        new(
            null,
            null,
            failure ?? throw new ArgumentNullException(nameof(failure)));
}

public sealed record ConnectMutationPolicy
{
    public const int HardMaxConfigurationItems = 256;
    public const int HardMaxConfigurationKeyCharacters = 512;
    public const int HardMaxConfigurationValueBytes = 64 * 1024;
    public const int HardMaxConfigurationBytes = 1024 * 1024;
    public const int HardMaxTasks = 256;
    public const int HardMaxConnectorNameCharacters = 512;
    public const int MaxSafeValueCharacters = 4_096;

    public static ConnectMutationPolicy Default { get; } = new();

    public ConnectMutationPolicy(
        int maxConfigurationItems = 128,
        int maxConfigurationValueBytes = 32 * 1024,
        int maxConfigurationBytes = 512 * 1024,
        int maxTasks = 128,
        TimeSpan? observationTimeout = null,
        string policyVersion = "v0.5-connect-mutation-p1")
    {
        var timeout =
            observationTimeout ?? TimeSpan.FromSeconds(10);

        if (maxConfigurationItems is < 1 or > HardMaxConfigurationItems)
            throw new ArgumentOutOfRangeException(nameof(maxConfigurationItems));
        if (maxConfigurationValueBytes is < 1 or > HardMaxConfigurationValueBytes)
            throw new ArgumentOutOfRangeException(nameof(maxConfigurationValueBytes));
        if (maxConfigurationBytes is < 1 or > HardMaxConfigurationBytes)
            throw new ArgumentOutOfRangeException(nameof(maxConfigurationBytes));
        if (maxTasks is < 1 or > HardMaxTasks)
            throw new ArgumentOutOfRangeException(nameof(maxTasks));
        if (timeout < TimeSpan.FromSeconds(1) ||
            timeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(nameof(observationTimeout));
        }

        MaxConfigurationItems = maxConfigurationItems;
        MaxConfigurationValueBytes = maxConfigurationValueBytes;
        MaxConfigurationBytes = maxConfigurationBytes;
        MaxTasks = maxTasks;
        ObservationTimeout = timeout;
        PolicyVersion = ConnectMutationCanonicalization.RequireIdentifier(
            policyVersion,
            "Connect mutation policy version",
            256);
    }

    public int MaxConfigurationItems { get; }
    public int MaxConfigurationValueBytes { get; }
    public int MaxConfigurationBytes { get; }
    public int MaxTasks { get; }
    public TimeSpan ObservationTimeout { get; }
    public string PolicyVersion { get; }
}

internal static class ConnectMutationCanonicalization
{
    private static readonly string RedactedValueFingerprint =
        Sha256(Encoding.UTF8.GetBytes("Kafdeck.Connect.RedactedValue.v1"));

    internal static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    public static string RequireIdentifier(
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

    public static string RequireConnectorName(string value) =>
        RequireIdentifier(
            value,
            "Connector name",
            ConnectMutationPolicy.HardMaxConnectorNameCharacters);

    public static IReadOnlyDictionary<string, string> NormalizeConfiguration(
        IReadOnlyDictionary<string, string> configuration,
        ConnectMutationPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(policy);

        if (configuration.Count is < 1 ||
            configuration.Count > policy.MaxConfigurationItems)
        {
            throw new ArgumentOutOfRangeException(
                nameof(configuration),
                "Connector configuration item count is outside the configured bound.");
        }

        long totalBytes = 0;
        var normalized = new SortedDictionary<string, string>(
            StringComparer.Ordinal);

        foreach (var pair in configuration)
        {
            var key = RequireIdentifier(
                pair.Key,
                "Connector configuration key",
                ConnectMutationPolicy.HardMaxConfigurationKeyCharacters);
            ArgumentNullException.ThrowIfNull(pair.Value);

            var keyBytes = Encoding.UTF8.GetByteCount(key);
            var valueBytes = Encoding.UTF8.GetByteCount(pair.Value);
            if (valueBytes > policy.MaxConfigurationValueBytes)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(configuration),
                    "Connector configuration value exceeds the configured byte ceiling.");
            }

            totalBytes = checked(totalBytes + keyBytes + valueBytes);
            if (totalBytes > policy.MaxConfigurationBytes)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(configuration),
                    "Connector configuration exceeds the configured total byte ceiling.");
            }

            if (!normalized.TryAdd(key, pair.Value))
            {
                throw new ArgumentException(
                    "Connector configuration keys must be unique.",
                    nameof(configuration));
            }
        }

        return new ReadOnlyDictionary<string, string>(normalized);
    }

    public static IReadOnlyList<ConnectConfigurationCanonicalItem>
        ProjectConfiguration(
            IReadOnlyDictionary<string, string> configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        return Array.AsReadOnly(
            configuration
                .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .Select(pair =>
                {
                    var explicitlySafe =
                        ConnectSafeConfigurationPolicy
                            .IsExplicitlySafeConfigKey(pair.Key) &&
                        !ConnectSafeConfigurationPolicy.IsSecretKey(pair.Key);
                    var digest = explicitlySafe
                        ? Sha256(Encoding.UTF8.GetBytes(pair.Value))
                        : RedactedValueFingerprint;
                    var safeValue = explicitlySafe
                        ? SafePreviewValue(pair.Value, digest)
                        : "[REDACTED]";

                    return new ConnectConfigurationCanonicalItem(
                        pair.Key,
                        digest,
                        safeValue);
                })
                .ToArray());
    }

    public static string ConfigurationFingerprint(
        IEnumerable<ConnectConfigurationCanonicalItem> values) =>
        FingerprintText(
            values
                .OrderBy(item => item.Key, StringComparer.Ordinal)
                .Select(item =>
                    new KeyValuePair<string, string>(
                        item.Key,
                        item.ValueSha256)));

    public static IReadOnlyList<ConnectConfigurationDiffItem> Diff(
        IReadOnlyList<ConnectConfigurationObservationItem> current,
        IReadOnlyList<ConnectConfigurationCanonicalItem> requested)
    {
        ArgumentNullException.ThrowIfNull(current);
        ArgumentNullException.ThrowIfNull(requested);

        var left = current.ToDictionary(
            item => item.Key,
            StringComparer.Ordinal);
        var right = requested.ToDictionary(
            item => item.Key,
            StringComparer.Ordinal);

        var keys = left.Keys
            .Concat(right.Keys)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(key => key, StringComparer.Ordinal);

        var result = new List<ConnectConfigurationDiffItem>();
        foreach (var key in keys)
        {
            left.TryGetValue(key, out var currentItem);
            right.TryGetValue(key, out var requestedItem);

            if (currentItem is null)
            {
                result.Add(
                    new ConnectConfigurationDiffItem(
                        key,
                        ConnectConfigurationChangeKind.Added,
                        null,
                        requestedItem!.SafeValue));
            }
            else if (requestedItem is null)
            {
                result.Add(
                    new ConnectConfigurationDiffItem(
                        key,
                        ConnectConfigurationChangeKind.Removed,
                        currentItem.SafeValue,
                        null));
            }
            else if (!string.Equals(
                         currentItem.ValueSha256,
                         requestedItem.ValueSha256,
                         StringComparison.Ordinal))
            {
                result.Add(
                    new ConnectConfigurationDiffItem(
                        key,
                        ConnectConfigurationChangeKind.Changed,
                        currentItem.SafeValue,
                        requestedItem.SafeValue));
            }
        }

        return Array.AsReadOnly(result.ToArray());
    }

    public static string ObservationFingerprint(
        ConnectMutationObservation observation)
    {
        ArgumentNullException.ThrowIfNull(observation);

        var values = new List<KeyValuePair<string, string>>
        {
            new("connector", observation.ConnectorName),
            new("exists", observation.Exists ? "1" : "0"),
            new("state", observation.State),
            new("configuration", observation.ConfigurationFingerprint),
        };

        foreach (var task in observation.Tasks
                     .OrderBy(item => item.Id))
        {
            values.Add(
                new(
                    "task",
                    $"{task.Id}|{task.State}"));
        }

        return FingerprintText(values);
    }

    public static byte[] EncodeConfiguration(
        IReadOnlyDictionary<string, string> configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        return JsonSerializer.SerializeToUtf8Bytes(
            configuration,
            JsonOptions);
    }

    public static IReadOnlyDictionary<string, string> DecodeConfiguration(
        ReadOnlyMemory<byte> material)
    {
        if (material.Length is < 1 or >
            ConnectMutationPolicy.HardMaxConfigurationBytes * 2)
        {
            throw new MutationStateException(
                "Connect configuration material is outside admitted bounds.");
        }

        var values = JsonSerializer.Deserialize<
            SortedDictionary<string, string>>(
            material.Span,
            JsonOptions)
            ?? throw new MutationStateException(
                "Connect configuration material is invalid.");

        return new ReadOnlyDictionary<string, string>(values);
    }

    public static string Serialize<T>(T value) =>
        JsonSerializer.Serialize(value, JsonOptions);

    public static T Deserialize<T>(string value)
        where T : class =>
        JsonSerializer.Deserialize<T>(value, JsonOptions)
        ?? throw new MutationStateException(
            $"Connect mutation canonical intent '{typeof(T).Name}' could not be deserialized.");

    public static string ResourceKey(
        string clusterId,
        string connectorName) =>
        $"cluster/{clusterId}/connect/{connectorName}";

    public static string AuthorizationResource(
        string connectorName) =>
        $"connector/{connectorName}";

    public static string Sha256(ReadOnlySpan<byte> value) =>
        Convert.ToHexString(
                SHA256.HashData(value))
            .ToLowerInvariant();

    private static string SafePreviewValue(
        string value,
        string digest)
    {
        if (value.Length <=
            ConnectMutationPolicy.MaxSafeValueCharacters)
        {
            return value;
        }

        return $"[SAFE_VALUE_SHA256:{digest}]";
    }

    private static string FingerprintText(
        IEnumerable<KeyValuePair<string, string>> values)
    {
        var builder = new StringBuilder();
        foreach (var pair in values)
        {
            builder.Append(pair.Key)
                .Append('=')
                .Append(
                    Convert.ToBase64String(
                        Encoding.UTF8.GetBytes(pair.Value)))
                .Append('\n');
        }

        return Sha256(
            Encoding.UTF8.GetBytes(builder.ToString()));
    }
}
