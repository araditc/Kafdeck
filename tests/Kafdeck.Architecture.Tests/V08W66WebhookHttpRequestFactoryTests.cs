using System.Net;
using System.Text;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66WebhookHttpRequestFactoryTests
{
    private const string Endpoint = "https://hooks.example.org/notify";

    private static NotificationDestinationProfile Profile(
        string destination = "ops-hook",
        string endpoint = Endpoint,
        bool withCredential = false) =>
        new(
            destination,
            NotificationProviderKind.Webhook,
            "Operations",
            [NotificationEventClass.Operational],
            new Uri(endpoint),
            withCredential
                ? new NotificationCredentialBindingId("ops-hook-key")
                : null);

    private static NotificationPinnedWebhookRequest Request(
        NotificationDestinationProfile profile,
        string? pinnedEndpoint = null,
        byte[]? payload = null,
        NotificationCredentialValue? credential = null)
    {
        var pin = NotificationAddressPolicy.NoConfiguredNat64.ValidatePinnedEndpoint(
            new Uri(pinnedEndpoint ?? Endpoint),
            [IPAddress.Parse("1.1.1.1")],
            DateTimeOffset.UtcNow);

        return new NotificationPinnedWebhookRequest(
            profile.DestinationId,
            profile.RevisionFingerprint,
            pin,
            payload ?? Encoding.UTF8.GetBytes("{\"eventType\":\"operational.alert\"}"),
            credential);
    }

    [Fact]
    public void Produces_only_immutable_canonical_JSON_POST_intent()
    {
        var profile = Profile();
        var approved = Request(profile);
        var intent = new NotificationWebhookHttpRequestFactory().Create(
            profile, approved);

        Assert.Equal("POST", intent.Method);
        Assert.Equal("application/json", intent.ContentType);
        Assert.Equal(profile.ConfiguredEndpoint, intent.Endpoint);
        Assert.Same(approved.PinnedEndpoint, intent.PinnedEndpoint);
        Assert.Equal(approved.PinnedEndpoint.ResolvedAtUtc,
            intent.PinnedEndpoint.ResolvedAtUtc);
        Assert.Equal(approved.PinnedEndpoint.AddressPolicyFingerprint,
            intent.PinnedEndpoint.AddressPolicyFingerprint);
        Assert.Equal(approved.PinnedEndpoint.Addresses[0].ToString(),
            intent.PinnedEndpoint.Addresses[0].ToString());
        Assert.Equal(profile.DestinationId, intent.DestinationId);
        Assert.Equal(profile.RevisionFingerprint,
            intent.ProfileRevisionFingerprint);
        Assert.Equal(approved.JsonPayload.ToArray(), intent.JsonPayload.ToArray());

        // A mutable HTTP request escapes neither this factory nor the
        // intent type; physical send-time enforcement is separate.
        Assert.False(typeof(HttpRequestMessage)
            .IsAssignableFrom(typeof(NotificationWebhookHttpIntent)));
        Assert.Empty(typeof(NotificationWebhookHttpIntent).GetConstructors());
        Assert.All(typeof(NotificationWebhookHttpIntent).GetProperties(),
            property => Assert.False(property.SetMethod?.IsPublic ?? false));
        Assert.Null(typeof(NotificationWebhookHttpIntent).GetProperty("Headers"));
        Assert.Null(typeof(NotificationWebhookHttpIntent).GetProperty("Content"));
    }

    [Fact]
    public void DNS_pin_remains_identical_to_approved_connection_evidence()
    {
        var profile = Profile();
        var approved = Request(profile);
        var intent = new NotificationWebhookHttpRequestFactory().Create(
            profile, approved);
        Assert.Same(approved.PinnedEndpoint, intent.PinnedEndpoint);

        // The pin returns a defensive IPAddress snapshot; mutating a
        // returned instance cannot swap the authorized dial target.
        var externalAddress = intent.PinnedEndpoint.Addresses[0];
#pragma warning disable CS0618
        externalAddress.Address = IPAddress.Loopback.Address;
#pragma warning restore CS0618

        Assert.Equal("1.1.1.1", intent.PinnedEndpoint.Addresses[0].ToString());
        Assert.Equal(approved.PinnedEndpoint.ResolvedAtUtc,
            intent.PinnedEndpoint.ResolvedAtUtc);
        Assert.Equal(approved.PinnedEndpoint.AddressPolicyFingerprint,
            intent.PinnedEndpoint.AddressPolicyFingerprint);
    }

    [Fact]
    public void Rejects_mismatched_destination_even_when_url_matches()
    {
        var owner = Profile();
        var different = Profile("other-hook");
        Assert.Throws<KafdeckConfigurationException>(() =>
            new NotificationWebhookHttpRequestFactory().Create(
                different, Request(owner)));
    }

    [Fact]
    public void Rejects_mismatched_approved_revision_and_endpoint()
    {
        var original = Profile();
        var changed = Profile(endpoint: "https://hooks.example.org/different");
        var factory = new NotificationWebhookHttpRequestFactory();

        Assert.Throws<KafdeckConfigurationException>(() =>
            factory.Create(changed, Request(original)));
        Assert.Throws<KafdeckConfigurationException>(() =>
            factory.Create(original, Request(original,
                pinnedEndpoint: "https://hooks.example.org/different")));
    }

    [Fact]
    public void No_guesswork_credentials_are_attached_or_leaked()
    {
        var profile = Profile(withCredential: true);
        var secret = new NotificationCredentialValue("do-not-print-provider-secret");
        var request = Request(profile, credential: secret);
        var exception = Assert.Throws<KafdeckConfigurationException>(() =>
            new NotificationWebhookHttpRequestFactory().Create(profile, request));

        Assert.DoesNotContain(secret.Reveal(), exception.ToString(),
            StringComparison.Ordinal);
        Assert.Contains("not yet admitted", exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Credential_bound_destination_cannot_silently_downgrade_to_anonymous()
    {
        var profile = Profile(withCredential: true);
        var request = Request(profile);
        Assert.Throws<KafdeckConfigurationException>(() =>
            new NotificationWebhookHttpRequestFactory().Create(profile, request));
    }

    [Fact]
    public void Payload_above_configured_budget_is_rejected_before_request_creation()
    {
        var profile = Profile();
        var request = Request(profile,
            payload: Enumerable.Repeat((byte)'x',
                NotificationDeliveryPolicy.DefaultPayloadBytes + 1).ToArray());

        Assert.Throws<KafdeckConfigurationException>(() =>
            new NotificationWebhookHttpRequestFactory().Create(profile, request));
    }

    [Fact]
    public void Intent_payload_is_immutable_and_never_references_mutable_source()
    {
        var profile = Profile();
        var source = Encoding.UTF8.GetBytes("{\"safe\":true}");
        var approved = Request(profile, payload: source);
        var intent = new NotificationWebhookHttpRequestFactory().Create(
            profile, approved);

        source.AsSpan().Fill((byte)'x');
        var callerCopy = intent.JsonPayload.ToArray();
        callerCopy.AsSpan().Fill((byte)'y');

        Assert.Equal(Encoding.UTF8.GetBytes("{\"safe\":true}"),
            intent.JsonPayload.ToArray());
    }
}
