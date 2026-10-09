using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66ConfiguredCredentialBindingTests
{
    private static NotificationDestinationProfile Profile(string id = "ops-email",
        string binding = "ops-credential") =>
        new(id, NotificationProviderKind.Email, "Ops",
            [NotificationEventClass.Operational],
            credentialBindingId: new NotificationCredentialBindingId(binding),
            emailRecipientAddress: "ops@example.com");

    private static ConfiguredNotificationCredentialBinding Provision(
        NotificationDestinationProfile profile, SecretReference secret) =>
        new(profile.DestinationId, profile.Provider, profile.RevisionFingerprint,
            profile.CredentialBindingId!, secret);

    [Fact]
    public async Task Only_matching_preconfigured_identity_can_resolve_secret()
    {
        var key = "KAFDECK_W66_CREDENTIAL_" + Guid.NewGuid().ToString("N");
        var profile = Profile();
        Environment.SetEnvironmentVariable(key, "sentinel-secret-not-for-logs");
        try
        {
            var resolver = new ConfiguredNotificationCredentialResolver(
                [Provision(profile, SecretReference.Parse("env:" + key))],
                new SecretResolver());
            var value = await resolver.ResolveAsync(
                new NotificationCredentialResolutionRequest(profile), CancellationToken.None);
            Assert.Equal("sentinel-secret-not-for-logs", value.Reveal());
            Assert.DoesNotContain("sentinel-secret", value.ToString());
            Assert.DoesNotContain(key, resolver.ToString() ?? string.Empty);
        }
        finally
        {
            Environment.SetEnvironmentVariable(key, null);
        }
    }

    [Fact]
    public async Task Missing_or_revision_drift_is_denied_before_secret_resolution()
    {
        var profile = Profile();
        var reference = SecretReference.Parse("env:KAFDECK_NOT_CONFIGURED_DO_NOT_LOAD");
        var resolver = new ConfiguredNotificationCredentialResolver(
            [Provision(profile, reference)], new SecretResolver());

        await Assert.ThrowsAsync<KafdeckConfigurationException>(
            async () => { _ = await resolver.ResolveAsync(
                new NotificationCredentialResolutionRequest(Profile("other")), CancellationToken.None); });
        await Assert.ThrowsAsync<KafdeckConfigurationException>(
            async () => { _ = await resolver.ResolveAsync(
                new NotificationCredentialResolutionRequest(Profile(binding: "different")), CancellationToken.None); });
        var drift = new NotificationDestinationProfile(
            "ops-email", NotificationProviderKind.Email, "Ops",
            [NotificationEventClass.Security],
            credentialBindingId: new NotificationCredentialBindingId("ops-credential"),
            emailRecipientAddress: "ops@example.com");
        await Assert.ThrowsAsync<KafdeckConfigurationException>(
            async () => { _ = await resolver.ResolveAsync(
                new NotificationCredentialResolutionRequest(drift), CancellationToken.None); });
    }

    [Fact]
    public void Duplicate_and_unknown_destinations_never_shadow_credentials()
    {
        var a = Profile();
        var binding = Provision(a, SecretReference.Parse("env:KAFDECK_W66_TEST_SECRET"));
        Assert.Throws<ArgumentException>(() =>
            new ConfiguredNotificationCredentialResolver([binding, binding], new SecretResolver()));
    }

    [Fact]
    public void Invalid_revision_fingerprint_never_enables_secret_access()
    {
        var profile = Profile();
        var binding = Provision(profile, SecretReference.Parse("env:KAFDECK_W66_TEST_SECRET"));
        Assert.Throws<ArgumentException>(() =>
            new ConfiguredNotificationCredentialResolver(
                [binding with { ProfileRevisionFingerprint = "invalid" }], new SecretResolver()));
    }

    [Fact]
    public async Task Cancellation_precedes_all_secret_resolution()
    {
        var profile = Profile();
        var resolver = new ConfiguredNotificationCredentialResolver(
            [Provision(profile, SecretReference.Parse("env:KAFDECK_W66_TEST_SECRET"))],
            new SecretResolver());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => { _ = await resolver.ResolveAsync(
                new NotificationCredentialResolutionRequest(profile), cts.Token); });
    }
}
