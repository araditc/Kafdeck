using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Microsoft.AspNetCore.DataProtection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class FleetSettingsStoreTests
{
    private static readonly BrokerHostState EmptyRegistry = new(1, 0, [], [], []);
    private static SecurityAuditEvent Audit(string code = "credential_saved") => new(DateTimeOffset.UtcNow,
        SecurityAuditEventType.BrokerHostManagement, "fleet-test-owner", Guid.NewGuid().ToString("N"),
        null, null, SecurityAuditOutcome.Succeeded, code);

    [Fact]
    public void Encrypted_database_survives_restart_and_backup_with_matching_keys()
    {
        using var fixture = new ManagedProvisioningFixture();
        var directory = Path.GetDirectoryName(fixture.StatePath)!;
        var keys = Path.Combine(directory, "fleet-keys");
        var database = Path.Combine(directory, "fleet.sqlite");
        var options = new BrokerHostOptions();
        var store = new FleetSettingsStore(database, FleetKeyRing.Open(keys), options, EmptyRegistry);
        const string secret = "SECRET-MUST-NOT-APPEAR-IN-DATABASE-483643";
        store.SaveCredential(new(0, "office", "password", secret, null, "probe"), Audit());
        Assert.DoesNotContain(secret, Encoding.UTF8.GetString(File.ReadAllBytes(database)));
        Assert.DoesNotContain(secret, JsonSerializer.Serialize(FleetSettingsEndpoints.View(store)));
        var reopened = new FleetSettingsStore(database, FleetKeyRing.Open(keys), options, EmptyRegistry);
        Assert.Equal(secret, reopened.Credential("office")!.Secret);
        var restoredDatabase = Path.Combine(directory, "restored.sqlite");
        File.Copy(database, restoredDatabase);
        var restoredKeys = Path.Combine(directory, "restored-keys");
        Directory.CreateDirectory(restoredKeys);
        foreach (var file in Directory.GetFiles(keys)) File.Copy(file, Path.Combine(restoredKeys, Path.GetFileName(file)));
        var restored = new FleetSettingsStore(restoredDatabase, FleetKeyRing.Open(restoredKeys), options, EmptyRegistry);
        Assert.Equal(secret, restored.Credential("office")!.Secret);
        Assert.Single(restored.Read().Registry.PendingAudit);
        var wrongKeys = new EphemeralDataProtectionProvider();
        Assert.Equal("fleet_keys_or_storage_unavailable", Assert.Throws<BrokerHostFailure>(() =>
            new FleetSettingsStore(restoredDatabase, wrongKeys, options, EmptyRegistry).Read()).Code);
    }

    [Fact]
    public void Credential_rotation_revocation_and_revision_conflicts_are_atomic()
    {
        using var fixture = new ManagedProvisioningFixture();
        var options = V11BrokerHostTests.Options();
        var store = new FleetSettingsStore(fixture.StatePath + ".sqlite", new EphemeralDataProtectionProvider(), options, EmptyRegistry);
        store.SaveCredential(new(0, "probe", "password", "first-secret", null, "probe"), Audit());
        Assert.Equal("fleet_revision_conflict", Assert.Throws<BrokerHostFailure>(() =>
            store.SaveCredential(new(0, "probe", "password", "stale-secret", null, "probe"), Audit())).Code);
        Assert.Equal("first-secret", store.Credential("probe")!.Secret);
        store.SaveCredential(new(1, "probe", "password", "replacement-secret", null, "probe"), Audit());
        Assert.Equal("replacement-secret", store.Credential("probe")!.Secret);
        store.SaveCredential(new(2, "probe", "password", null, null, "probe", true), Audit("credential_revoked"));
        Assert.Empty(store.Options().Credentials);
        Assert.Equal("fleet_credential_revoked", Assert.Throws<BrokerHostFailure>(() => store.Credential("probe")).Code);
        Assert.Equal(3, store.Read().Registry.PendingAudit.Length);
    }

    [Fact]
    public void Dynamic_configuration_updates_without_restart_and_preserves_host_revision()
    {
        using var fixture = new ManagedProvisioningFixture();
        var legacy = new BrokerHostOptions();
        var existingHost = new BrokerHost(Guid.NewGuid(), V11BrokerHostTests.Draft(), DateTimeOffset.UtcNow);
        var initial = EmptyRegistry with { Revision = 7, Hosts = [existingHost] };
        var store = new FleetSettingsStore(fixture.StatePath + ".sqlite", new EphemeralDataProtectionProvider(), legacy, initial);
        legacy.Runtime = store.Options;
        var configuration = new FleetConfiguration(true, ["127.0.0.1/32"], [], [], [], [], []);
        store.SaveConfiguration(new(0, configuration), Audit("settings_changed"));
        Assert.True(legacy.Current.Enabled);
        Assert.True(legacy.Allows("127.0.0.1"));
        Assert.False(legacy.Allows("192.0.2.10"));
        Assert.Equal(7, store.Read().Registry.Revision);
        Assert.Equal(existingHost, Assert.Single(store.Read().Registry.Hosts));
        var registry = new BrokerHostStore(fixture.StatePath + ".hosts.json", store);
        registry.Commit(7, current => current with { Bindings = [] }, Audit());
        Assert.Equal(8, registry.Read().Revision);
        Assert.Equal(1, store.Read().SettingsRevision);
    }

    [Fact]
    public async Task Credential_audit_is_durable_and_contains_no_secret_after_retry()
    {
        using var fixture = new ManagedProvisioningFixture();
        var store = new FleetSettingsStore(fixture.StatePath + ".sqlite", new EphemeralDataProtectionProvider(), new(), EmptyRegistry);
        store.SaveCredential(new(0, "office", "password", "PRIVATE-PASSWORD", null, "probe"), Audit());
        fixture.Audit.FailNext = true;
        await Assert.ThrowsAsync<IOException>(() => store.DrainAuditAsync(fixture.Audit, default));
        Assert.Single(store.Read().Registry.PendingAudit);
        await store.DrainAuditAsync(fixture.Audit, default);
        Assert.Empty(store.Read().Registry.PendingAudit);
        Assert.DoesNotContain("PRIVATE-PASSWORD", JsonSerializer.Serialize(fixture.Audit.Events));
    }
}
