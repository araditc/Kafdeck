using System.Text;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66BoundedCredentialMaterialTests
{
    private static NotificationDestinationProfile Profile() =>
        new("ops-email", NotificationProviderKind.Email, "Operations",
            [NotificationEventClass.Operational],
            credentialBindingId: new NotificationCredentialBindingId("ops-binding"),
            emailRecipientAddress: "ops@example.com");

    private static ConfiguredNotificationCredentialResolver Resolver(
        SecretReference reference)
    {
        var profile = Profile();
        return new ConfiguredNotificationCredentialResolver(
            [new ConfiguredNotificationCredentialBinding(
                profile.DestinationId, profile.Provider,
                profile.RevisionFingerprint, profile.CredentialBindingId!,
                reference)],
            new SecretResolver());
    }

    private static ValueTask<NotificationCredentialValue> ResolveAsync(
        ConfiguredNotificationCredentialResolver resolver) =>
        resolver.ResolveAsync(
            new NotificationCredentialResolutionRequest(Profile()),
            CancellationToken.None);

    [Fact]
    public async Task Oversized_environment_credential_is_rejected_without_secret_disclosure()
    {
        var key = "KAFDECK_W66_CAPPED_" + Guid.NewGuid().ToString("N");
        const string marker = "never-print-this-token";
        Environment.SetEnvironmentVariable(key,
            new string('a', ConfiguredNotificationCredentialResolver.MaxCredentialBytes) + marker);
        try
        {
            var resolver = Resolver(SecretReference.Parse("env:" + key));
            var error = await Assert.ThrowsAsync<KafdeckConfigurationException>(
                async () => { _ = await ResolveAsync(resolver); });
            Assert.Contains("bounded size limit", error.Message,
                StringComparison.Ordinal);
            Assert.DoesNotContain(marker, error.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain(key, error.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable(key, null);
        }
    }

    [Fact]
    public async Task Exact_limit_environment_credential_is_allowed()
    {
        var key = "KAFDECK_W66_CAPPED_" + Guid.NewGuid().ToString("N");
        Environment.SetEnvironmentVariable(key,
            new string('x', ConfiguredNotificationCredentialResolver.MaxCredentialBytes));
        try
        {
            var value = await ResolveAsync(Resolver(SecretReference.Parse("env:" + key)));
            Assert.Equal(ConfiguredNotificationCredentialResolver.MaxCredentialBytes,
                value.Reveal().Length);
        }
        finally
        {
            Environment.SetEnvironmentVariable(key, null);
        }
    }

    [Fact]
    public async Task Oversized_file_credential_is_rejected_without_path_disclosure()
    {
        var file = TemporarySecretFile();
        try
        {
            await File.WriteAllBytesAsync(file,
                Enumerable.Repeat((byte)'x',
                    ConfiguredNotificationCredentialResolver.MaxCredentialBytes + 6).ToArray());
            var error = await Assert.ThrowsAsync<KafdeckConfigurationException>(
                async () => { _ = await ResolveAsync(Resolver(SecretReference.Parse("file:" + file))); });
            Assert.Contains("bounded size limit", error.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(file, error.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public async Task Exact_limit_with_bom_and_crlf_resolves_without_expanding_secret_budget()
    {
        var file = TemporarySecretFile();
        try
        {
            await File.WriteAllTextAsync(file,
                new string('x', ConfiguredNotificationCredentialResolver.MaxCredentialBytes) + "\r\n",
                Encoding.UTF8);
            var value = await ResolveAsync(Resolver(SecretReference.Parse("file:" + file)));
            Assert.Equal(ConfiguredNotificationCredentialResolver.MaxCredentialBytes,
                value.Reveal().Length);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public async Task Framing_allowance_cannot_increase_normalized_secret_limit()
    {
        var file = TemporarySecretFile();
        try
        {
            await File.WriteAllTextAsync(file,
                new string('x', ConfiguredNotificationCredentialResolver.MaxCredentialBytes + 1) + "\n",
                Encoding.UTF8);
            await Assert.ThrowsAsync<KafdeckConfigurationException>(
                async () => { _ = await ResolveAsync(Resolver(SecretReference.Parse("file:" + file))); });
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public async Task Invalid_utf8_file_fails_closed_without_exposing_raw_material()
    {
        var file = TemporarySecretFile();
        try
        {
            await File.WriteAllBytesAsync(file, [0xC3, 0x28]);
            var error = await Assert.ThrowsAsync<KafdeckConfigurationException>(
                async () => { _ = await ResolveAsync(Resolver(SecretReference.Parse("file:" + file))); });
            Assert.Contains("valid UTF-8", error.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(file, error.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(file);
        }
    }

    private static string TemporarySecretFile() =>
        Path.Combine(Path.GetTempPath(), $"kafdeck-w66-secret-{Guid.NewGuid():N}");
}
