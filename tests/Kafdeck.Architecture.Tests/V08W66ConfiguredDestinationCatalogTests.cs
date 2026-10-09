using Kafdeck.Core.Notifications;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66ConfiguredDestinationCatalogTests
{
    private static NotificationDestinationProfile Profile(string id) =>
        new(id, NotificationProviderKind.Email, id,
            [NotificationEventClass.Operational],
            credentialBindingId: new NotificationCredentialBindingId("email-binding"),
            emailRecipientAddress: "ops@example.com");

    [Fact]
    public async Task Empty_configured_catalog_fails_closed_without_fallback()
    {
        var catalog = new ConfiguredNotificationDestinationProfileCatalog(
            Array.Empty<NotificationDestinationProfile>());
        Assert.Null(await catalog.GetAsync("unknown", CancellationToken.None));
    }

    [Fact]
    public async Task Snapshot_keeps_exact_case_and_original_revision()
    {
        var a = Profile("Ops");
        var b = Profile("ops");
        var configured = new List<NotificationDestinationProfile> { a, b };
        var catalog = new ConfiguredNotificationDestinationProfileCatalog(configured);
        configured.Clear();
        configured.Add(Profile("newly-added"));

        Assert.Same(a, await catalog.GetAsync("Ops", CancellationToken.None));
        Assert.Same(b, await catalog.GetAsync("ops", CancellationToken.None));
        Assert.Null(await catalog.GetAsync("newly-added", CancellationToken.None));
        Assert.Equal(a.RevisionFingerprint,
            (await catalog.GetAsync("Ops", CancellationToken.None))!.RevisionFingerprint);
    }

    [Fact]
    public void Rejects_duplicate_exact_identity_instead_of_shadowing()
    {
        Assert.Throws<ArgumentException>(() =>
            new ConfiguredNotificationDestinationProfileCatalog(
                [Profile("ops"), Profile("ops")]));
    }

    [Fact]
    public void Rejects_null_entry()
    {
        Assert.Throws<ArgumentException>(() =>
            new ConfiguredNotificationDestinationProfileCatalog(
                new NotificationDestinationProfile[] { Profile("ops"), null! }));
    }

    [Fact]
    public async Task Hard_limit_is_finite_and_not_truncated()
    {
        var allowed = Enumerable.Range(0,
            ConfiguredNotificationDestinationProfileCatalog.HardMaxProfiles)
            .Select(i => Profile($"dest-{i}")).ToArray();
        var catalog = new ConfiguredNotificationDestinationProfileCatalog(allowed);
        Assert.NotNull(await catalog.GetAsync("dest-499", CancellationToken.None));
        Assert.Throws<ArgumentException>(() =>
            new ConfiguredNotificationDestinationProfileCatalog(
                allowed.Append(Profile("over-limit"))));
    }

    [Theory]
    [InlineData(".")]
    [InlineData("..")]
    public async Task Json_body_valid_dot_only_destinations_keep_exact_lookup(string id)
    {
        var profile = Profile(id);
        var catalog = new ConfiguredNotificationDestinationProfileCatalog([profile]);
        Assert.Same(profile, await catalog.GetAsync(id, CancellationToken.None));
    }

    [Fact]
    public async Task Cancellation_and_invalid_id_never_perform_catalog_lookup()
    {
        var catalog = new ConfiguredNotificationDestinationProfileCatalog([Profile("ops")]);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => { _ = await catalog.GetAsync("ops", cancelled.Token); });
        await Assert.ThrowsAsync<ArgumentException>(
            async () => { _ = await catalog.GetAsync("https://unapproved", CancellationToken.None); });
    }
}
