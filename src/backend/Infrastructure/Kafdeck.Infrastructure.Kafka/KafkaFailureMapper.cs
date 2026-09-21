using Confluent.Kafka;
using Kafdeck.Core.Kafka;

namespace Kafdeck.Infrastructure.Kafka;

internal static class KafkaFailureMapper
{
    public static KafkaFailure FromKafka(Error error)
    {
        ArgumentNullException.ThrowIfNull(error);

        return error.Code switch
        {
            ErrorCode.ClusterAuthorizationFailed or
            ErrorCode.TopicAuthorizationFailed or
            ErrorCode.TransactionalIdAuthorizationFailed or
            ErrorCode.DelegationTokenAuthorizationFailed =>
                Failure(KafkaFailureCategory.Unauthorized, error.Code, "Kafka denied the requested operation.", false),

            ErrorCode.SaslAuthenticationFailed or
            ErrorCode.Local_Authentication =>
                Failure(KafkaFailureCategory.AuthenticationFailed, error.Code, "Kafka authentication failed.", false),

            ErrorCode.Local_Ssl =>
                Failure(KafkaFailureCategory.TlsFailure, error.Code, "Kafka TLS negotiation or certificate validation failed.", false),

            ErrorCode.Local_InvalidArg or
            ErrorCode.Local_NotConfigured or
            ErrorCode.InvalidConfig =>
                Failure(KafkaFailureCategory.InvalidConfiguration, error.Code, "Kafka client configuration is invalid.", false),

            ErrorCode.Local_TimedOut or
            ErrorCode.Local_TimedOutQueue or
            ErrorCode.RequestTimedOut =>
                Failure(KafkaFailureCategory.Timeout, error.Code, "Kafka operation exceeded its deadline.", true),

            ErrorCode.Local_AllBrokersDown or
            ErrorCode.Local_Transport or
            ErrorCode.Local_Resolve or
            ErrorCode.BrokerNotAvailable or
            ErrorCode.LeaderNotAvailable or
            ErrorCode.NetworkException =>
                Failure(KafkaFailureCategory.Unavailable, error.Code, "Kafka is temporarily unavailable.", true),

            ErrorCode.Local_UnsupportedFeature =>
                Failure(KafkaFailureCategory.NotSupported, error.Code, "Kafka does not support the requested capability.", false),

            ErrorCode.OffsetOutOfRange =>
                OffsetOutOfRange(),

            _ => Failure(
                KafkaFailureCategory.ProtocolError,
                error.Code,
                "Kafka returned an error for the requested operation.",
                false),
        };
    }

    public static KafkaFailure DeadlineExceeded() =>
        new(KafkaFailureCategory.Timeout, "deadline_exceeded", "Kafka operation exceeded its deadline.", true);

    public static KafkaFailure Cancelled() =>
        new(KafkaFailureCategory.Cancelled, "operation_cancelled", "Kafka operation was cancelled.", false);

    public static KafkaFailure ClusterNotConfigured() =>
        new(KafkaFailureCategory.InvalidConfiguration, "cluster_not_configured", "Kafka cluster profile is not configured.", false);

    public static KafkaFailure InvalidConfiguration() =>
        new(KafkaFailureCategory.InvalidConfiguration, "invalid_configuration", "Kafka client configuration is invalid.", false);

    public static KafkaFailure OffsetOutOfRange() =>
        new(KafkaFailureCategory.ProtocolError, "offset_out_of_range", "The requested Kafka record offset is outside the available partition range.", false);

    public static KafkaFailure Unknown() =>
        new(KafkaFailureCategory.Unknown, "unknown_kafka_failure", "Kafka operation failed.", false);

    private static KafkaFailure Failure(
        KafkaFailureCategory category,
        ErrorCode errorCode,
        string safeMessage,
        bool retryable) =>
        new(category, $"kafka_{errorCode.ToString().ToLowerInvariant()}", safeMessage, retryable);
}
