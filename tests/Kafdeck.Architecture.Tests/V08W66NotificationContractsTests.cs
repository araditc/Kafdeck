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

    [Theory]
    [InlineData("env:KAFDECK_DEPLOYMENT_TOKEN")]
    [InlineData("file:/run/secrets/root-token")]
    [InlineData("binding with spaces")]
    public void Credential_binding_id_rejects_locator_or_unscoped_values(
        string value)
    {
        Assert.Throws<ArgumentException>(
            () => new NotificationCredentialBindingId(
                value));
    }

    [Fact]
    public void Destination_uses_opaque_preprovisioned_credential_binding()
    {
        var binding =
            new NotificationCredentialBindingId(
                "notifications.ops-webhook");
        var profile =
            new NotificationDestinationProfile(
                "ops-webhook",
                NotificationProviderKind.Webhook,
                "Operations",
                [NotificationEventClass.Operational],
                new Uri("https://hooks.example.com/events"),
                binding);

        Assert.Same(
            binding,
            profile.CredentialBindingId);

        var request =
            new NotificationCredentialResolutionRequest(
                profile);

        Assert.Equal(
            "notifications.ops-webhook",
            request.BindingId.Value);
        Assert.Equal(
            "ops-webhook",
            request.DestinationId);
        Assert.Equal(
            NotificationProviderKind.Webhook,
            request.Provider);
        Assert.Equal(
            profile.RevisionFingerprint,
            request.ProfileRevisionFingerprint);

        var changedEndpoint =
            new NotificationDestinationProfile(
                "ops-webhook",
                NotificationProviderKind.Webhook,
                "Operations",
                [NotificationEventClass.Operational],
                new Uri("https://hooks.example.com/changed"),
                binding);

        Assert.NotEqual(
            profile.RevisionFingerprint,
            changedEndpoint.RevisionFingerprint);
    }

    [Fact]
    public void Pinned_resolution_rejects_private_dns_answers()
    {
        Assert.Throws<ArgumentException>(
            () => NotificationAddressPolicy
                .NoConfiguredNat64
                .ValidatePinnedEndpoint(
                    new Uri("https://hooks.example.com/events"),
                    [
                        IPAddress.Parse("1.1.1.1"),
                        IPAddress.Parse("127.0.0.1"),
                    ],
                    DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Pinned_resolution_rejects_private_ipv4_embedded_in_configured_nat64_prefix()
    {
        var policy =
            new NotificationAddressPolicy(
                [
                    new NotificationNat64Prefix(
                        IPAddress.Parse(
                            "2001:db8:1234:5678:9abc:def0::"),
                        96),
                ]);

        Assert.Throws<ArgumentException>(
            () => policy.ValidatePinnedEndpoint(
                new Uri("https://hooks.example.com/events"),
                [
                    IPAddress.Parse(
                        "2001:db8:1234:5678:9abc:def0:7f00:1"),
                ],
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Configured_nat64_rejects_malformed_reserved_u_octet()
    {
        var policy =
            new NotificationAddressPolicy(
                [
                    new NotificationNat64Prefix(
                        IPAddress.Parse(
                            "2001:db8:1234::"),
                        48),
                ]);

        // Prefix bits match /48, but RFC6052's reserved u-octet
        // (byte 8) is non-zero. This must be Malformed, not NoMatch.
        Assert.Throws<ArgumentException>(
            () => policy.ValidatePinnedEndpoint(
                new Uri("https://hooks.example.com/events"),
                [
                    IPAddress.Parse(
                        "2001:db8:1234:7f00:100::"),
                ],
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Configured_specific_nat64_cannot_hide_prohibited_implicit_local_use_decoding()
    {
        var policy =
            new NotificationAddressPolicy(
                [
                    new NotificationNat64Prefix(
                        IPAddress.Parse(
                            "64:ff9b:1:7f00:0:100::"),
                        96),
                ]);

        Assert.Throws<ArgumentException>(
            () => policy.ValidatePinnedEndpoint(
                new Uri("https://hooks.example.com/events"),
                [
                    IPAddress.Parse(
                        "64:ff9b:1:7f00:0:100:808:808"),
                ],
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Configured_nat64_policy_rejects_overlapping_prefixes()
    {
        Assert.Throws<ArgumentException>(
            () => new NotificationAddressPolicy(
                [
                    new NotificationNat64Prefix(
                        IPAddress.Parse(
                            "64:ff9b:1::"),
                        48),
                    new NotificationNat64Prefix(
                        IPAddress.Parse(
                            "64:ff9b:1:808:800::"),
                        96),
                ]));

        Assert.Throws<ArgumentException>(
            () => new NotificationAddressPolicy(
                [
                    new NotificationNat64Prefix(
                        IPAddress.Parse(
                            "64:ff9b:1:808:800::"),
                        96),
                    new NotificationNat64Prefix(
                        IPAddress.Parse(
                            "64:ff9b:1::"),
                        48),
                ]));
    }

    [Fact]
    public void Configured_standard_nat64_allows_public_embedded_ipv4()
    {
        var policy =
            new NotificationAddressPolicy(
                [
                    new NotificationNat64Prefix(
                        IPAddress.Parse(
                            "64:ff9b::"),
                        96),
                ]);

        var pinned =
            policy.ValidatePinnedEndpoint(
                new Uri("https://hooks.example.com/events"),
                [
                    IPAddress.Parse(
                        "64:ff9b::101:101"),
                ],
                DateTimeOffset.UtcNow);

        Assert.Single(
            pinned.Addresses);
    }

    [Fact]
    public void Configured_standard_nat64_rejects_private_embedded_ipv4()
    {
        var policy =
            new NotificationAddressPolicy(
                [
                    new NotificationNat64Prefix(
                        IPAddress.Parse(
                            "64:ff9b::"),
                        96),
                ]);

        Assert.Throws<ArgumentException>(
            () => policy.ValidatePinnedEndpoint(
                new Uri("https://hooks.example.com/events"),
                [
                    IPAddress.Parse(
                        "64:ff9b::7f00:1"),
                ],
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Configured_nat64_prefix_allows_public_embedded_ipv4()
    {
        var policy =
            new NotificationAddressPolicy(
                [
                    new NotificationNat64Prefix(
                        IPAddress.Parse(
                            "2001:db8:1234:5678:9abc:def0::"),
                        96),
                ]);

        var pinned =
            policy.ValidatePinnedEndpoint(
                new Uri("https://hooks.example.com/events"),
                [
                    IPAddress.Parse(
                        "2001:db8:1234:5678:9abc:def0:0101:0101"),
                ],
                DateTimeOffset.UtcNow);

        Assert.Single(
            pinned.Addresses);
    }

    [Fact]
    public void Pinned_resolution_retains_only_validated_connection_addresses()
    {
        var pinned =
            NotificationAddressPolicy
                .NoConfiguredNat64
                .ValidatePinnedEndpoint(
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
    public void Pinned_resolution_binds_authoritative_policy_fingerprint()
    {
        var policy =
            new NotificationAddressPolicy(
                [
                    new NotificationNat64Prefix(
                        IPAddress.Parse(
                            "2001:db8:1234:5678:9abc:def0::"),
                        96),
                ]);

        var pinned =
            policy.ValidatePinnedEndpoint(
                new Uri("https://hooks.example.com/events"),
                [IPAddress.Parse("1.1.1.1")],
                DateTimeOffset.UtcNow);

        Assert.Equal(
            policy.Fingerprint,
            pinned.AddressPolicyFingerprint);
        Assert.NotEqual(
            NotificationAddressPolicy
                .NoConfiguredNat64
                .Fingerprint,
            pinned.AddressPolicyFingerprint);
    }

    [Fact]
    public void Credential_value_remains_separate_and_redacted()
    {
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
