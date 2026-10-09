using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66CredentialReferenceOptionsTests
{
    private static Dictionary<string, string?> Base() => new()
    {
        ["Kafdeck:Notifications:Enabled"] = "true",
        ["Kafdeck:Notifications:Provider"] = "Sqlite",
        ["Kafdeck:Notifications:SqliteDatabasePath"] =
            Path.Combine(Path.GetTempPath(), "w66-refs.db"),
        ["Kafdeck:Notifications:Destinations:0:DestinationId"] = "ops-email",
        ["Kafdeck:Notifications:Destinations:0:Provider"] = "Email",
        ["Kafdeck:Notifications:Destinations:0:DisplayName"] = "Operations",
        ["Kafdeck:Notifications:Destinations:0:EnabledEvents:0"] = "Operational",
        ["Kafdeck:Notifications:Destinations:0:CredentialBindingId"] = "email-binding",
        ["Kafdeck:Notifications:Destinations:0:EmailRecipientAddress"] = "ops@example.com",
        ["Kafdeck:Notifications:CredentialBindings:0:DestinationId"] = "ops-email",
        ["Kafdeck:Notifications:CredentialBindings:0:BindingId"] = "email-binding",
        ["Kafdeck:Notifications:CredentialBindings:0:SecretReference"] =
            "env:KAFDECK_NOTIFY_PROVISIONED",
    };

    private static KafdeckOptions Load(Dictionary<string, string?> values) =>
        KafdeckConfigurationLoader.Load(new ConfigurationBuilder()
            .AddInMemoryCollection(values).Build());

    [Fact]
    public void Parses_only_preprovisioned_secret_reference()
    {
        var options = Load(Base());
        var binding = Assert.Single(options.Notifications!.CredentialBindings!);
        Assert.Equal("ops-email", binding.DestinationId);
        Assert.Equal("email-binding", binding.BindingId.Value);
        Assert.Equal(SecretReferenceKind.Environment, binding.Secret.Kind);
        Assert.Equal("[redacted-secret-reference]", binding.Secret.ToString());
        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }

    [Theory]
    [InlineData("secret-in-plain-text")]
    [InlineData("http://untrusted.example")]
    [InlineData("file:relative-location")]
    public void Rejects_raw_or_unapproved_secret_locators(string value)
    {
        var config = Base();
        config["Kafdeck:Notifications:CredentialBindings:0:SecretReference"] = value;
        Assert.Throws<KafdeckConfigurationException>(() => Load(config));
    }

    [Fact]
    public void Rejects_credential_bound_to_missing_or_different_profile()
    {
        foreach (var key in new[] { "DestinationId", "BindingId" })
        {
            var config = Base();
            config["Kafdeck:Notifications:CredentialBindings:0:" + key] = "other";
            var options = Load(config);
            Assert.Throws<KafdeckConfigurationException>(
                () => KafdeckConfigurationValidator.ValidateAndThrow(options));
        }
    }

    [Fact]
    public void Disabled_notifications_cannot_be_used_as_a_secret_registry()
    {
        var config = Base();
        config["Kafdeck:Notifications:Enabled"] = "false";
        var options = Load(config);
        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
    }

    [Theory]
    [InlineData("CredentialBindings:00:DestinationId")]
    [InlineData("CredentialBindings:0:SecretReference:RawValue")]
    [InlineData("CredentialBindings:0:ProviderToken")]
    public void Rejects_noncanonical_or_nested_config(string badPath)
    {
        var config = Base();
        config["Kafdeck:Notifications:" + badPath] = "injected";
        Assert.Throws<KafdeckConfigurationException>(() => Load(config));
    }

    [Fact]
    public void Duplicate_bindings_for_exact_destination_are_rejected()
    {
        var config = Base();
        config["Kafdeck:Notifications:CredentialBindings:1:DestinationId"] = "ops-email";
        config["Kafdeck:Notifications:CredentialBindings:1:BindingId"] = "email-binding";
        config["Kafdeck:Notifications:CredentialBindings:1:SecretReference"] =
            "env:KAFDECK_NOTIFY_PROVISIONED";
        Assert.Throws<KafdeckConfigurationException>(() => Load(config));
    }

    [Fact]
    public void No_binding_configuration_causes_no_secret_resolution_or_send()
    {
        var config = Base();
        foreach (var key in config.Keys.Where(x =>
            x.StartsWith("Kafdeck:Notifications:CredentialBindings:", StringComparison.Ordinal))
            .ToArray())
            config.Remove(key);
        var options = Load(config);
        Assert.Empty(options.Notifications!.CredentialBindings!);
        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }
}
