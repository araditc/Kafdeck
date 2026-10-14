using Kafdeck.Core.Notifications;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66WebhookHttpOutcomePolicyTests
{
    [Theory]
    [InlineData(200)]
    [InlineData(201)]
    [InlineData(204)]
    public void Definite_http_receipt_ack_is_delivered_without_exposing_response(int status)
    {
        var result = NotificationWebhookHttpOutcomePolicy.FromResponse(status);

        Assert.Equal(NotificationWebhookTransportOutcome.Delivered, result.Outcome);
        Assert.Equal("http_receipt_ack", result.OutcomeCode);
        Assert.Null(result.ProviderRequestId);
        Assert.Equal(NotificationDeliveryDispatchOutcome.Delivered,
            NotificationProviderOutcomeMapper.ToDeliveryOutcome(result.Outcome));
    }

    [Theory]
    [InlineData(400)]
    [InlineData(401)]
    [InlineData(403)]
    [InlineData(404)]
    [InlineData(405)]
    [InlineData(410)]
    [InlineData(413)]
    [InlineData(415)]
    [InlineData(422)]
    public void Explicit_definitive_rejections_do_not_retry(int status)
    {
        var result = NotificationWebhookHttpOutcomePolicy.FromResponse(status);
        Assert.Equal(NotificationWebhookTransportOutcome.PermanentFailure, result.Outcome);
        Assert.Equal("http_definitive_reject", result.OutcomeCode);
        Assert.Equal(NotificationDeliveryDispatchOutcome.PermanentFailure,
            NotificationProviderOutcomeMapper.ToDeliveryOutcome(result.Outcome));
    }

    [Theory]
    [InlineData(202)]
    [InlineData(206)]
    [InlineData(301)]
    [InlineData(302)]
    [InlineData(307)]
    [InlineData(308)]
    [InlineData(408)]
    [InlineData(409)]
    [InlineData(425)]
    [InlineData(429)]
    [InlineData(500)]
    [InlineData(502)]
    [InlineData(503)]
    [InlineData(504)]
    public void Ambiguous_or_async_ack_cannot_cause_blind_replay(int status)
    {
        var result = NotificationWebhookHttpOutcomePolicy.FromResponse(status);

        Assert.Equal(NotificationWebhookTransportOutcome.UnknownExternalEffect, result.Outcome);
        Assert.Equal("http_effect_ambiguous", result.OutcomeCode);
        Assert.Equal(NotificationDeliveryDispatchOutcome.UnknownExternalEffect,
            NotificationProviderOutcomeMapper.ToDeliveryOutcome(result.Outcome));
    }

    [Fact]
    public void All_ambiguous_http_codes_fail_closed_in_the_finite_vocabulary()
    {
        for (var code = 100; code <= 599; code++)
        {
            var result = NotificationWebhookHttpOutcomePolicy.FromResponse(code);
            Assert.NotEqual(NotificationWebhookTransportOutcome.RetryableFailure,
                result.Outcome);
            Assert.Null(result.ProviderRequestId);
            Assert.DoesNotContain(code.ToString(), result.OutcomeCode,
                StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(99)]
    [InlineData(600)]
    [InlineData(999)]
    public void Invalid_http_status_is_rejected(int status)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => NotificationWebhookHttpOutcomePolicy.FromResponse(status));
    }

    [Fact]
    public void Only_proven_pre_send_failure_may_be_retryable()
    {
        var before = NotificationWebhookHttpOutcomePolicy.BeforePhysicalSendFailure();
        var after = NotificationWebhookHttpOutcomePolicy.AfterPhysicalSendUncertain();

        Assert.Equal(NotificationWebhookTransportOutcome.RetryableFailure, before.Outcome);
        Assert.Equal(NotificationDeliveryDispatchOutcome.RetryableFailure,
            NotificationProviderOutcomeMapper.ToDeliveryOutcome(before.Outcome));
        Assert.Equal(NotificationWebhookTransportOutcome.UnknownExternalEffect, after.Outcome);
        Assert.Equal(NotificationDeliveryDispatchOutcome.UnknownExternalEffect,
            NotificationProviderOutcomeMapper.ToDeliveryOutcome(after.Outcome));
    }
}
