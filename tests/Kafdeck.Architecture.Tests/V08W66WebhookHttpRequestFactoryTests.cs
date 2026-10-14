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
    public async Task Produces_only_canonical_json_POST_without_arbitrary_headers()
    {
        var profile = Profile();
        var approved = Request(profile);
        using var http = new NotificationWebhookHttpRequestFactory().Create(profile, approved);

        Assert.Equal(HttpMethod.Post, http.Method);
        Assert.Equal(profile.ConfiguredEndpoint, http.RequestUri);
        Assert.Equal(HttpVersion.Version11, http.Version);
        Assert.Equal(HttpVersionPolicy.RequestVersionExact, http.VersionPolicy);
        Assert.Null(http.Headers.Host);
        Assert.Null(http.Headers.Authorization);
        Assert.Single(http.Content!.Headers);
        Assert.Equal("application/json", http.Content.Headers.ContentType!.MediaType);
        Assert.Equal(approved.JsonPayload.ToArray(),
            await http.Content.ReadAsByteArrayAsync());
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
    public async Task Request_material_is_copied_and_never_references_mutable_source()
    {
        var profile = Profile();
        var source = Encoding.UTF8.GetBytes("{\"safe\":true}");
        var approved = Request(profile, payload: source);
        using var wire = new NotificationWebhookHttpRequestFactory().Create(
            profile, approved);
        source.AsSpan().Fill((byte)'x');

        var contents = await wire.Content!.ReadAsByteArrayAsync();
        Assert.Equal(Encoding.UTF8.GetBytes("{\"safe\":true}"), contents);
    }
}
