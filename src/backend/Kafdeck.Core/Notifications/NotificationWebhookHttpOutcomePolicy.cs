namespace Kafdeck.Core.Notifications;

/// <summary>
/// Closed-world HTTP outcome policy for the opt-in typed Webhook transport.
/// It does NOT activate physical HTTP delivery, prove provider-specific semantics,
/// or authorize a retry of a possibly committed external side effect.
/// </summary>
public static class NotificationWebhookHttpOutcomePolicy
{
    /// <summary>
    /// Maps only conclusively acknowledged or rejected HTTP status codes.
    /// A 202/redirect/timeout/429/5xx can reflect an accepted or queued side
    /// effect, and therefore MUST NOT become an automatic retry.
    /// </summary>
    public static NotificationWebhookTransportResult FromResponse(int statusCode)
    {
        if (statusCode is < 100 or > 599)
            throw new ArgumentOutOfRangeException(nameof(statusCode));

        var outcome = statusCode switch
        {
            // HTTP-level receipt acknowledgments: these do not claim the
            // recipient processed the notification or ran downstream work.
            200 or 201 or 204 => NotificationWebhookTransportOutcome.Delivered,

            // A narrow set of definitive request/auth rejection responses.
            // No assumption that other 4xx (especially 409/408/429) are safe.
            400 or 401 or 403 or 404 or 405 or 410 or 413 or 415 or 422 =>
                NotificationWebhookTransportOutcome.PermanentFailure,

            // This includes 202 Accepted, 3xx, 408, 409, 425, 429 and all
            // 5xx: a remote effect or queued work cannot be ruled out.
            _ => NotificationWebhookTransportOutcome.UnknownExternalEffect,
        };

        var code = outcome switch
        {
            NotificationWebhookTransportOutcome.Delivered => "http_receipt_ack",
            NotificationWebhookTransportOutcome.PermanentFailure => "http_definitive_reject",
            _ => "http_effect_ambiguous",
        };

        // Fixed reason vocabulary: never echo response text, headers, URLs,
        // provider request identifiers, credentials or protected event body.
        return new NotificationWebhookTransportResult(outcome, code);
    }

    /// <summary>
    /// Only a failure proven before the physical send/effect boundary may
    /// enter the existing ledger's bounded RetryableFailure path.
    /// </summary>
    public static NotificationWebhookTransportResult BeforePhysicalSendFailure() =>
        new(NotificationWebhookTransportOutcome.RetryableFailure,
            "http_pre_send_retryable");

    /// <summary>
    /// Once the physical send is initiated, any timeout, cancellation,
    /// transport exception or missing response must be reconciled, not
    /// blindly retried. Callers retain cancellation propagation separately.
    /// </summary>
    public static NotificationWebhookTransportResult AfterPhysicalSendUncertain() =>
        new(NotificationWebhookTransportOutcome.UnknownExternalEffect,
            "http_effect_ambiguous");
}
