using Kafdeck.Core.Notifications;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66NotificationDeliveryContractsTests
{
    [Fact]
    public void Initial_delivery_requires_pending_zero_attempt_state()
    {
        var pending =
            Snapshot(
                NotificationDeliveryState.Pending,
                attemptCount: 0);

        NotificationDeliveryTransition
            .ValidateInitial(
                pending);

        Assert.Throws<ArgumentException>(
            () =>
                NotificationDeliveryTransition
                    .ValidateInitial(
                        Snapshot(
                            NotificationDeliveryState.InFlight,
                            attemptCount: 1)));
    }

    [Fact]
    public void Delivery_transition_preserves_identity_and_attempt_semantics()
    {
        var pending =
            Snapshot(
                NotificationDeliveryState.Pending,
                attemptCount: 0);
        var inFlight =
            Snapshot(
                NotificationDeliveryState.InFlight,
                attemptCount: 1);

        NotificationDeliveryTransition
            .ValidateReplacement(
                pending,
                inFlight);

        var failed =
            Snapshot(
                NotificationDeliveryState.Failed,
                attemptCount: 1,
                nextAttemptAtUtc:
                    pending.CreatedAtUtc
                        .AddSeconds(5),
                outcomeCode:
                    NotificationDeliveryOutcomeCodes.RetryableFailure);

        NotificationDeliveryTransition
            .ValidateReplacement(
                inFlight,
                failed);

        Assert.Throws<ArgumentException>(
            () =>
                NotificationDeliveryTransition
                    .ValidateReplacement(
                        failed,
                        Snapshot(
                            NotificationDeliveryState.InFlight,
                            attemptCount: 1)));
    }

    [Fact]
    public void Durable_transition_rejects_provider_request_id_without_safe_projection()
    {
        var previous =
            Snapshot(
                NotificationDeliveryState.InFlight,
                attemptCount: 1);
        var next =
            new NotificationDeliverySnapshot(
                previous.NotificationId,
                previous.DestinationId,
                previous.PayloadFingerprint,
                NotificationDeliveryState.Delivered,
                1,
                previous.CreatedAtUtc,
                outcomeCode:
                    NotificationDeliveryOutcomeCodes.Delivered,
                providerRequestId:
                    "secret-looking-provider-id");

        Assert.Throws<ArgumentException>(
            () =>
                NotificationDeliveryTransition
                    .ValidateReplacement(
                        previous,
                        next));
    }

    [Fact]
    public void Durable_transition_rejects_arbitrary_outcome_codes()
    {
        var inFlight =
            Snapshot(
                NotificationDeliveryState.InFlight,
                attemptCount: 1);
        var failed =
            Snapshot(
                NotificationDeliveryState.Failed,
                attemptCount: 1,
                nextAttemptAtUtc:
                    inFlight.CreatedAtUtc
                        .AddSeconds(5),
                outcomeCode:
                    "retryable-http-503");

        Assert.Throws<ArgumentException>(
            () =>
                NotificationDeliveryTransition
                    .ValidateReplacement(
                        inFlight,
                        failed));
    }

    [Fact]
    public void Unknown_external_effect_is_terminal()
    {
        var inFlight =
            Snapshot(
                NotificationDeliveryState.InFlight,
                attemptCount: 1);
        var unknown =
            Snapshot(
                NotificationDeliveryState.UnknownExternalEffect,
                attemptCount: 1,
                outcomeCode:
                    NotificationDeliveryOutcomeCodes.UnknownExternalEffect);

        NotificationDeliveryTransition
            .ValidateReplacement(
                inFlight,
                unknown);

        Assert.Throws<ArgumentException>(
            () =>
                NotificationDeliveryTransition
                    .ValidateReplacement(
                        unknown,
                        Snapshot(
                            NotificationDeliveryState.InFlight,
                            attemptCount: 2)));
    }

    [Fact]
    public void Due_query_and_page_are_hard_bounded()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () =>
                new NotificationDeliveryDueQuery(
                    DateTimeOffset.UtcNow,
                    NotificationDeliveryDueQuery
                        .HardMaxResults + 1));

        var createdAt =
            new DateTimeOffset(
                2026,
                10,
                5,
                12,
                0,
                0,
                TimeSpan.Zero);
        var snapshot =
            Snapshot(
                NotificationDeliveryState.Pending,
                attemptCount: 0);
        var record =
            new NotificationDeliveryRecord(
                snapshot,
                revision: 1,
                updatedAtUtc:
                    createdAt);
        var cursor =
            new NotificationDeliveryDueCursor(
                snapshot.NextAttemptAtUtc ??
                snapshot.CreatedAtUtc,
                snapshot.NotificationId,
                snapshot.DestinationId);

        var page =
            new NotificationDeliveryPage(
                [record],
                truncated: true,
                cursor);

        Assert.True(
            page.Truncated);
        Assert.NotNull(
            page.NextCursor);
    }

    private static NotificationDeliverySnapshot
        Snapshot(
            NotificationDeliveryState state,
            int attemptCount,
            DateTimeOffset? nextAttemptAtUtc = null,
            string? outcomeCode = null)
    {
        var createdAt =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);

        return new NotificationDeliverySnapshot(
            Guid.Parse(
                "11111111-1111-1111-1111-111111111111"),
            "ops-webhook",
            new string(
                'a',
                64),
            state,
            attemptCount,
            createdAt,
            nextAttemptAtUtc,
            outcomeCode);
    }
}
