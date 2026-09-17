using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Clusters;

namespace Kafdeck.Api;

public sealed record ApiEnvelope<T>(
    T Data,
    ApiObservation Observation,
    IReadOnlyList<ApiLimitation> Limitations);

public sealed record ApiObservation(
    DateTimeOffset ObservedAt,
    string Freshness,
    long CacheAgeMs,
    bool Partial);

public sealed record ApiLimitation(
    string? Capability,
    string State,
    string? Reason);

public sealed record ConfigurationEntryData(
    string Name,
    string? Value,
    bool IsSensitive,
    bool IsReadOnly,
    string? Source);

public sealed record ApiProblemDefinition(
    int Status,
    string Type,
    string Title,
    string Detail,
    string Code);

public static class ApiConfigurationMapper
{
    public static ConfigurationEntryData Create(KafkaConfigurationEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        return new ConfigurationEntryData(
            entry.Name,
            entry.IsSensitive ? null : entry.Value,
            entry.IsSensitive,
            entry.IsReadOnly,
            entry.Source);
    }
}

public static class ApiObservationMapper
{
    public static ApiObservation Create(
        ObservationMetadata observation,
        bool partial,
        DateTimeOffset? nowUtc = null)
    {
        ArgumentNullException.ThrowIfNull(observation);

        var now = nowUtc ?? DateTimeOffset.UtcNow;
        var age = now - observation.ObservedAtUtc;
        var cacheAgeMs = Math.Max(0L, (long)age.TotalMilliseconds);
        var freshness = observation.Source == ObservationSource.StaleSnapshot
            ? "stale"
            : "fresh";

        return new ApiObservation(
            observation.ObservedAtUtc,
            freshness,
            cacheAgeMs,
            partial);
    }

    public static IReadOnlyList<ApiLimitation> FromCluster(
        IReadOnlyList<ClusterAccessLimitation> limitations) =>
        limitations
            .Select(limitation => new ApiLimitation(
                limitation.Capability?.ToString(),
                CapabilityState(limitation.State),
                limitation.Reason))
            .ToArray();

    public static IReadOnlyList<ApiLimitation> FromCapabilities(KafkaCapabilities capabilities) =>
        capabilities.Items
            .Where(item => item.State != KafkaCapabilityState.Available)
            .OrderBy(item => item.Capability)
            .Select(item => new ApiLimitation(
                item.Capability.ToString(),
                CapabilityState(item.State),
                item.Reason))
            .ToArray();

    public static string CapabilityState(KafkaCapabilityState state) => state switch
    {
        KafkaCapabilityState.Available => "available",
        KafkaCapabilityState.Unauthorized => "denied",
        KafkaCapabilityState.Unsupported => "unsupported",
        KafkaCapabilityState.Unavailable => "temporarilyUnavailable",
        _ => "unknown",
    };
}

public static class ApiProblemMapper
{
    public static ApiProblemDefinition InvalidClusterId(string clusterId) => new(
        StatusCodes.Status404NotFound,
        "urn:kafdeck:problem:invalid-cluster-id",
        "Invalid cluster ID",
        $"Cluster '{clusterId}' is not configured in this Kafdeck instance.",
        "invalid_cluster_id");

    public static ApiProblemDefinition BrokerNotFound(int brokerId) => new(
        StatusCodes.Status404NotFound,
        "urn:kafdeck:problem:broker-not-found",
        "Broker not found",
        $"Broker '{brokerId}' is not present in the current cluster projection.",
        "broker_not_found");

    public static ApiProblemDefinition InvalidPageSize() => new(
        StatusCodes.Status400BadRequest,
        "urn:kafdeck:problem:invalid-page-size",
        "Invalid page size",
        "pageSize must be between 1 and 200.",
        "invalid_page_size");

    public static ApiProblemDefinition InvalidCursor(string detail) => new(
        StatusCodes.Status400BadRequest,
        "urn:kafdeck:problem:invalid-cursor",
        "Invalid cursor",
        detail,
        "invalid_cursor");

    public static ApiProblemDefinition StaleCursor(string detail) => new(
        StatusCodes.Status409Conflict,
        "urn:kafdeck:problem:stale-cursor",
        "Cursor no longer matches the active snapshot",
        detail,
        "stale_cursor");

    public static ApiProblemDefinition FromKafka(KafkaFailure failure)
    {
        ArgumentNullException.ThrowIfNull(failure);

        if (failure.Code == "cluster_not_configured")
        {
            return new ApiProblemDefinition(
                StatusCodes.Status404NotFound,
                "urn:kafdeck:problem:invalid-cluster-id",
                "Invalid cluster ID",
                failure.SafeMessage,
                failure.Code);
        }

        return failure.Category switch
        {
            KafkaFailureCategory.Unauthorized => new(
                StatusCodes.Status403Forbidden,
                "urn:kafdeck:problem:authorization-denied",
                "Authorization denied",
                failure.SafeMessage,
                failure.Code),
            KafkaFailureCategory.AuthenticationFailed => new(
                StatusCodes.Status502BadGateway,
                "urn:kafdeck:problem:authentication-failed",
                "Kafka authentication failed",
                failure.SafeMessage,
                failure.Code),
            KafkaFailureCategory.TlsFailure => new(
                StatusCodes.Status502BadGateway,
                "urn:kafdeck:problem:tls-validation-failed",
                "Kafka TLS validation failed",
                failure.SafeMessage,
                failure.Code),
            KafkaFailureCategory.Timeout => new(
                StatusCodes.Status504GatewayTimeout,
                "urn:kafdeck:problem:operation-timeout",
                "Kafka operation timed out",
                failure.SafeMessage,
                failure.Code),
            KafkaFailureCategory.Unavailable => new(
                StatusCodes.Status503ServiceUnavailable,
                "urn:kafdeck:problem:cluster-unavailable",
                "Kafka cluster unavailable",
                failure.SafeMessage,
                failure.Code),
            KafkaFailureCategory.InvalidConfiguration => new(
                StatusCodes.Status503ServiceUnavailable,
                "urn:kafdeck:problem:invalid-cluster-configuration",
                "Invalid Kafka cluster configuration",
                failure.SafeMessage,
                failure.Code),
            KafkaFailureCategory.NotSupported => new(
                StatusCodes.Status501NotImplemented,
                "urn:kafdeck:problem:unsupported-capability",
                "Kafka capability is unsupported",
                failure.SafeMessage,
                failure.Code),
            KafkaFailureCategory.Cancelled => new(
                StatusCodes.Status408RequestTimeout,
                "urn:kafdeck:problem:operation-cancelled",
                "Kafka operation was cancelled",
                failure.SafeMessage,
                failure.Code),
            _ => new(
                StatusCodes.Status502BadGateway,
                "urn:kafdeck:problem:kafka-protocol-error",
                "Kafka operation failed",
                failure.SafeMessage,
                failure.Code),
        };
    }
}

public static class ApiResults
{
    public static IResult Problem(ApiProblemDefinition problem) =>
        Results.Problem(
            statusCode: problem.Status,
            type: problem.Type,
            title: problem.Title,
            detail: problem.Detail,
            extensions: new Dictionary<string, object?>
            {
                ["code"] = problem.Code,
            });
}
