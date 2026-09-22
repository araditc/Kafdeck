using Kafdeck.Core.ReadViews;

namespace Kafdeck.Core.Ecosystem;

public static class ConnectSafeConfigurationPolicy
{
    public static bool IsExplicitlySafeConfigKey(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var normalized = key.Trim().ToLowerInvariant();

        return normalized is
            "connector.class" or
            "name" or
            "tasks.max" or
            "topics" or
            "topics.regex" or
            "key.converter" or
            "value.converter" or
            "header.converter" or
            "errors.tolerance";
    }

    public static bool IsSecretKey(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var normalized = new string(
            key.Trim()
                .ToLowerInvariant()
                .Where(char.IsLetterOrDigit)
                .ToArray());

        return normalized.Contains("password", StringComparison.Ordinal) ||
               normalized.Contains("passwd", StringComparison.Ordinal) ||
               normalized.Contains("secret", StringComparison.Ordinal) ||
               normalized.Contains("token", StringComparison.Ordinal) ||
               normalized.Contains("credential", StringComparison.Ordinal) ||
               normalized.Contains("sasljaasconfig", StringComparison.Ordinal) ||
               normalized.Contains("privatekey", StringComparison.Ordinal) ||
               normalized.EndsWith("apikey", StringComparison.Ordinal) ||
               normalized.EndsWith("accesskey", StringComparison.Ordinal) ||
               normalized.EndsWith("clientsecret", StringComparison.Ordinal);
    }
}

public sealed record ConnectMutationCapabilities(
    bool SupportsCreate,
    bool SupportsUpdate,
    bool SupportsPause,
    bool SupportsResume,
    bool SupportsRestart,
    bool SupportsTaskRestart,
    bool SupportsDelete);

public sealed record ConnectConfigurationObservationItem(
    string Key,
    string ValueSha256,
    string? SafeValue);

public sealed record ConnectMutationTaskObservation(
    int Id,
    string State);

public sealed record ConnectMutationObservation(
    string ConnectorName,
    bool Exists,
    string State,
    IReadOnlyList<ConnectMutationTaskObservation> Tasks,
    IReadOnlyList<ConnectConfigurationObservationItem> Configuration,
    string ConfigurationFingerprint);

public enum ConnectMutationObservationFailureCategory
{
    NotConfigured = 1,
    Unauthorized = 2,
    Unsupported = 3,
    Unavailable = 4,
    Timeout = 5,
    Cancelled = 6,
    InvalidRequest = 7,
    InvalidResponse = 8,
}

public sealed record ConnectMutationObservationFailure(
    ConnectMutationObservationFailureCategory Category,
    string Code,
    string SafeMessage,
    bool IsRetryable);

public sealed record ConnectMutationObservationResult<T>
{
    private ConnectMutationObservationResult(
        T? value,
        ConnectMutationObservationFailure? failure)
    {
        Value = value;
        Failure = failure;
    }

    public T? Value { get; }
    public ConnectMutationObservationFailure? Failure { get; }
    public bool IsSuccess => Failure is null;

    public static ConnectMutationObservationResult<T> Success(T value) =>
        new(value, null);

    public static ConnectMutationObservationResult<T> Failed(
        ConnectMutationObservationFailure failure) =>
        new(default, failure ?? throw new ArgumentNullException(nameof(failure)));
}

public interface IConnectMutationObservationPort
{
    Task<ConnectMutationObservationResult<ConnectMutationCapabilities>>
        GetCapabilitiesAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken);

    Task<ConnectMutationObservationResult<ConnectMutationObservation>>
        ObserveConnectorAsync(
            string clusterId,
            string connectorName,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken);
}
