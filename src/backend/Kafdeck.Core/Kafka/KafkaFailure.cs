namespace Kafdeck.Core.Kafka;

public enum KafkaFailureCategory
{
    Unavailable = 1,
    Timeout = 2,
    Cancelled = 3,
    Unauthorized = 4,
    AuthenticationFailed = 5,
    TlsFailure = 6,
    InvalidConfiguration = 7,
    NotSupported = 8,
    ProtocolError = 9,
    Unknown = 10,
}

public sealed record KafkaFailure(
    KafkaFailureCategory Category,
    string Code,
    string SafeMessage,
    bool IsRetryable);
