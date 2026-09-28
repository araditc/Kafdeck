using System.Net;
using Kafdeck.Core.Notifications;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66NotificationContractsTests
{
    [Fact]
    public void Webhook_destination_requires_configured_https_endpoint()
    {
        Assert.Throws<ArgumentException>(
            () => new NotificationDestinationProfile(
                "ops-webhook",
                NotificationProviderKind.Webhook,
                "Operations",
                [NotificationEventClass.Operational]));
    }

    [Theory]
    [InlineData("http://hooks.example.com/events")]
    [InlineData("https://user:pass@hooks.example.com/events")]
    [InlineData("https://hooks.example.com/events?target=x")]
    [InlineData("https://localhost/events")]
    [InlineData("https://127.0.0.1/events")]
    [InlineData("https://10.0.0.1/events")]
    [InlineData("https://169.254.169.254/latest")]
    [InlineData("https://192.168.1.10/events")]
    [InlineData("https://[fec0::1]/events")]
    [InlineData("https://[::ffff:0:127.0.0.1]/events")]
    [InlineData("https://[64:ff9b::127.0.0.1]/events")]
    [InlineData("https://[64:ff9b:1::127.0.0.1]/events")]
    [InlineData("https://[2002:7f00:1::1]/events")]
    public void Https_endpoint_policy_rejects_unsafe_destinations(
        string endpoint)
    {
        Assert.Throws<ArgumentException>(
            () => NotificationHttpsEndpointPolicy.ValidateAndNormalize(
                new Uri(endpoint)));
    }

    [Fact]
    public void Https_endpoint_policy_accepts_configured_public_origin()
    {
        var endpoint =
            NotificationHttpsEndpointPolicy.ValidateAndNormalize(
                new Uri("https://Hooks.Example.com/events"));

        Assert.Equal(
            "https",
            endpoint.Scheme);
        Assert.Equal(
            "hooks.example.com",
            endpoint.Host);
        Assert.Equal(
            "/events",
            endpoint.AbsolutePath);
    }

    [Fact]
    public void Credential_reference_rejects_raw_secret_values()
    {
        Assert.Throws<ArgumentException>(
            () => NotificationCredentialReference.Parse(
                "super-secret-token"));

        var environment =
            NotificationCredentialReference.Parse(
                "env:KAFDECK_WEBHOOK_TOKEN");

        Assert.Equal(
            NotificationCredentialReferenceKind.Environment,
            environment.Kind);
        Assert.Equal(
            "[redacted-notification-credential-reference]",
            environment.ToString());
    }

    [Fact]
    public void Destination_accepts_typed_credential_reference_only()
    {
        var profile =
            new NotificationDestinationProfile(
                "ops-webhook",
                NotificationProviderKind.Webhook,
                "Operations",
                [NotificationEventClass.Operational],
                new Uri("https://hooks.example.com/events"),
                NotificationCredentialReference.Parse(
                    "env:KAFDECK_WEBHOOK_TOKEN"));

        Assert.NotNull(
            profile.CredentialReference);
        Assert.IsType<NotificationCredentialReference>(
            profile.CredentialReference);
    }

    [Fact]
    public void Pinned_resolution_rejects_private_dns_answers()
    {
        Assert.Throws<ArgumentException>(
            () => new NotificationPinnedEndpoint(
                new Uri("https://hooks.example.com/events"),
                [
                    IPAddress.Parse("1.1.1.1"),
                    IPAddress.Parse("127.0.0.1"),
                ],
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Pinned_resolution_retains_only_validated_connection_addresses()
    {
        var pinned =
            new NotificationPinnedEndpoint(
                new Uri("https://hooks.example.com/events"),
                [
                    IPAddress.Parse("1.1.1.1"),
                    IPAddress.Parse("8.8.8.8"),
                ],
                DateTimeOffset.UtcNow);

        Assert.Equal(
            2,
            pinned.Addresses.Count);
        Assert.All(
            pinned.Addresses,
            address =>
                Assert.False(
                    NotificationHttpsEndpointPolicy
                        .IsProhibitedAddress(
                            address)));
    }

    [Fact]
    public void Credential_reference_exposes_locator_but_never_secret_value()
    {
        var reference =
            NotificationCredentialReference.Parse(
                "env:KAFDECK_WEBHOOK_TOKEN");

        Assert.Equal(
            "KAFDECK_WEBHOOK_TOKEN",
            reference.Locator);
        Assert.Equal(
            "[redacted-notification-credential-reference]",
            reference.ToString());

        var value =
            new NotificationCredentialValue(
                "secret");
        Assert.Equal(
            "secret",
            value.Reveal());
        Assert.Equal(
            "[redacted-notification-credential]",
            value.ToString());
    }

    [Fact]
    public void Delivery_policy_enforces_all_hard_caps()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new NotificationDeliveryPolicy(
                maxAttempts:
                    NotificationDeliveryPolicy.HardMaxAttempts + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new NotificationDeliveryPolicy(
                maxPayloadBytes:
                    NotificationDeliveryPolicy.HardMaxPayloadBytes + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new NotificationDeliveryPolicy(
                ratePerSecond:
                    NotificationDeliveryPolicy.HardMaxRatePerSecond + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new NotificationDeliveryPolicy(
                maxConcurrency:
                    NotificationDeliveryPolicy.HardMaxConcurrency + 1));
    }

    [Theory]
    [InlineData(" ops-webhook ")]
    [InlineData("ops/webhook")]
    [InlineData(" مقصد ")]
    public void Delivery_snapshot_reuses_destination_identity_grammar(
        string destinationId)
    {
        Assert.Throws<ArgumentException>(
            () => new NotificationDeliverySnapshot(
                Guid.NewGuid(),
                destinationId,
                new string('a', 64),
                NotificationDeliveryState.Pending,
                attemptCount: 0,
                createdAtUtc: DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Delivery_snapshot_rejects_next_attempt_beyond_hard_lifetime()
    {
        var createdAt =
            DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new NotificationDeliverySnapshot(
                Guid.NewGuid(),
                "ops-webhook",
                new string('a', 64),
                NotificationDeliveryState.Pending,
                attemptCount: 1,
                createdAtUtc: createdAt,
                nextAttemptAtUtc:
                    createdAt +
                    NotificationDeliveryPolicy.HardMaxLifetime +
                    TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void Delivery_state_contains_fingerprint_not_raw_payload()
    {
        var snapshot =
            new NotificationDeliverySnapshot(
                Guid.NewGuid(),
                "ops-webhook",
                new string('a', 64),
                NotificationDeliveryState.Pending,
                attemptCount: 0,
                createdAtUtc: DateTimeOffset.UtcNow);

        Assert.Equal(
            new string('a', 64),
            snapshot.PayloadFingerprint);

        var propertyNames =
            typeof(NotificationDeliverySnapshot)
                .GetProperties()
                .Select(property => property.Name)
                .ToArray();

        Assert.DoesNotContain(
            "Payload",
            propertyNames);
        Assert.DoesNotContain(
            "Secret",
            propertyNames);
        Assert.DoesNotContain(
            "Token",
            propertyNames);
    }

    [Fact]
    public void Destination_event_classes_are_unique_and_bounded()
    {
        Assert.Throws<ArgumentException>(
            () => new NotificationDestinationProfile(
                "ops-webhook",
                NotificationProviderKind.Webhook,
                "Operations",
                [
                    NotificationEventClass.Operational,
                    NotificationEventClass.Operational,
                ],
                new Uri("https://hooks.example.com/events")));
    }
}
