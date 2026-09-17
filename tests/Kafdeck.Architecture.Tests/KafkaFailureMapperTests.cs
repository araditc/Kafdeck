using Confluent.Kafka;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaFailureMapperTests
{
    [Theory]
    [InlineData(ErrorCode.ClusterAuthorizationFailed, KafkaFailureCategory.Unauthorized, false)]
    [InlineData(ErrorCode.TopicAuthorizationFailed, KafkaFailureCategory.Unauthorized, false)]
    [InlineData(ErrorCode.SaslAuthenticationFailed, KafkaFailureCategory.AuthenticationFailed, false)]
    [InlineData(ErrorCode.Local_Authentication, KafkaFailureCategory.AuthenticationFailed, false)]
    [InlineData(ErrorCode.Local_Ssl, KafkaFailureCategory.TlsFailure, false)]
    [InlineData(ErrorCode.Local_InvalidArg, KafkaFailureCategory.InvalidConfiguration, false)]
    [InlineData(ErrorCode.Local_TimedOut, KafkaFailureCategory.Timeout, true)]
    [InlineData(ErrorCode.Local_AllBrokersDown, KafkaFailureCategory.Unavailable, true)]
    [InlineData(ErrorCode.Local_UnsupportedFeature, KafkaFailureCategory.NotSupported, false)]
    public void Known_Kafka_errors_are_normalized_to_stable_Kafdeck_categories(
        ErrorCode errorCode,
        KafkaFailureCategory expectedCategory,
        bool expectedRetryable)
    {
        var failure = KafkaFailureMapper.FromKafka(new Error(errorCode));

        Assert.Equal(expectedCategory, failure.Category);
        Assert.Equal(expectedRetryable, failure.IsRetryable);
        Assert.StartsWith("kafka_", failure.Code, StringComparison.Ordinal);
        Assert.False(string.IsNullOrWhiteSpace(failure.SafeMessage));
    }

    [Fact]
    public void Protocol_fallback_does_not_expose_broker_error_reason()
    {
        const string sensitiveBrokerReason = "credential-bearing broker diagnostic";
        var failure = KafkaFailureMapper.FromKafka(
            new Error(ErrorCode.OffsetOutOfRange, sensitiveBrokerReason));

        Assert.Equal(KafkaFailureCategory.ProtocolError, failure.Category);
        Assert.DoesNotContain(sensitiveBrokerReason, failure.SafeMessage, StringComparison.Ordinal);
    }
}
