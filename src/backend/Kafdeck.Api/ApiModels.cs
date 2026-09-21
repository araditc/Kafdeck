using Kafdeck.Core.Kafka;
using Kafdeck.Core.ReadViews;
using Kafdeck.Modules.Clusters;

namespace Kafdeck.Api;

public sealed record ApiEnvelope<T>(
    T Data,
    ApiObservation Observation,
    IReadOnlyList<ApiLimitation> Limitations);

public sealed record ReadViewApiEnvelope<T>(
    T Data,
    bool Partial,
    IReadOnlyList<ReadViewLimitationData> Limitations);

public sealed record ReadViewLimitationData(
    string Code,
    string Message);

public static class ReadViewApiMapper
{
    public static ReadViewApiEnvelope<T> Envelope<T>(ReadViewResult<T> result)
    {
        if (!result.IsSuccess || result.Value is null)
        {
            throw new InvalidOperationException("Read-view result must be successful before projection.");
        }

        var limitations = result.Limitations
            .Select(item => new ReadViewLimitationData(item.Code, item.SafeMessage))
            .ToArray();

        return new ReadViewApiEnvelope<T>(
            result.Value,
            limitations.Length > 0,
            limitations);
    }

    public static ReadViewApiEnvelope<T> Envelope<T>(
        T value,
        IReadOnlyList<ReadViewLimitation>? limitations = null)
    {
        var projected = (limitations ?? Array.Empty<ReadViewLimitation>())
            .Select(item => new ReadViewLimitationData(item.Code, item.SafeMessage))
            .ToArray();

        return new ReadViewApiEnvelope<T>(
            value,
            projected.Length > 0,
            projected);
    }
}

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

    public static ApiProblemDefinition FromReadView(ReadViewFailure failure)
    {
        ArgumentNullException.ThrowIfNull(failure);

        return failure.Category switch
        {
            ReadViewFailureCategory.NotConfigured => new(
                StatusCodes.Status404NotFound,
                "urn:kafdeck:problem:read-view-not-configured",
                "Read view not configured",
                failure.SafeMessage,
                failure.Code),
            ReadViewFailureCategory.Unauthorized => new(
                StatusCodes.Status403Forbidden,
                "urn:kafdeck:problem:authorization-denied",
                "Authorization denied",
                failure.SafeMessage,
                failure.Code),
            ReadViewFailureCategory.Unsupported => new(
                StatusCodes.Status501NotImplemented,
                "urn:kafdeck:problem:unsupported-capability",
                "Read-view capability is unsupported",
                failure.SafeMessage,
                failure.Code),
            ReadViewFailureCategory.Unavailable => new(
                StatusCodes.Status503ServiceUnavailable,
                "urn:kafdeck:problem:upstream-unavailable",
                "Read-view upstream unavailable",
                failure.SafeMessage,
                failure.Code),
            ReadViewFailureCategory.Timeout => new(
                StatusCodes.Status504GatewayTimeout,
                "urn:kafdeck:problem:operation-timeout",
                "Read-view operation timed out",
                failure.SafeMessage,
                failure.Code),
            ReadViewFailureCategory.Cancelled => new(
                StatusCodes.Status408RequestTimeout,
                "urn:kafdeck:problem:operation-cancelled",
                "Read-view operation was cancelled",
                failure.SafeMessage,
                failure.Code),
            ReadViewFailureCategory.ResponseTooLarge => new(
                StatusCodes.Status422UnprocessableEntity,
                "urn:kafdeck:problem:read-view-bound-exceeded",
                "Read-view bound exceeded",
                failure.SafeMessage,
                failure.Code),
            ReadViewFailureCategory.InvalidRequest => new(
                StatusCodes.Status400BadRequest,
                "urn:kafdeck:problem:invalid-read-view-request",
                "Invalid read-view request",
                failure.SafeMessage,
                failure.Code),
            _ => new(
                StatusCodes.Status502BadGateway,
                "urn:kafdeck:problem:invalid-upstream-response",
                "Invalid read-view upstream response",
                failure.SafeMessage,
                failure.Code),
        };
    }

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
