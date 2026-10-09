using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66ConfiguredDestinationOptionsTests
{
    private static Dictionary<string, string?> ValidEmailConfig() => new()
    {
        ["Kafdeck:Notifications:Enabled"] = "true",
        ["Kafdeck:Notifications:Provider"] = "Sqlite",
        ["Kafdeck:Notifications:SqliteDatabasePath"] =
            Path.Combine(Path.GetTempPath(), "w66-notification-profiles.db"),
        ["Kafdeck:Notifications:Destinations:0:DestinationId"] = "ops-email",
        ["Kafdeck:Notifications:Destinations:0:Provider"] = "Email",
        ["Kafdeck:Notifications:Destinations:0:DisplayName"] = "Operations",
        ["Kafdeck:Notifications:Destinations:0:EnabledEvents:0"] = "Operational",
        ["Kafdeck:Notifications:Destinations:0:CredentialBindingId"] =
            "ops-email-credential",
        ["Kafdeck:Notifications:Destinations:0:EmailRecipientAddress"] =
            "ops@example.com",
    };

    private static KafdeckOptions Load(Dictionary<string, string?> values) =>
        KafdeckConfigurationLoader.Load(new ConfigurationBuilder()
            .AddInMemoryCollection(values).Build());

    [Fact]
    public void Parses_exact_typed_destination_with_opaque_credential_binding()
    {
        var cfg = Load(ValidEmailConfig());
        var profile = Assert.Single(cfg.Notifications!.DestinationProfiles!);
        Assert.Equal("ops-email", profile.DestinationId);
        Assert.Equal(NotificationProviderKind.Email, profile.Provider);
        Assert.Equal("ops@example.com", profile.EmailRecipientAddress);
        Assert.Equal("ops-email-credential", profile.CredentialBindingId!.Value);
        Assert.Null(profile.ConfiguredEndpoint);
        Assert.Single(profile.EnabledEvents);
        Assert.Equal(NotificationEventClass.Operational,
            profile.EnabledEvents[0]);
    }

    [Fact]
    public void Configured_destinations_never_activate_a_worker_or_provider_transport()
    {
        var cfg = Load(ValidEmailConfig());
        Assert.True(cfg.Notifications!.Enabled);
        Assert.False(cfg.Notifications.ManagementEnabled);
        Assert.Single(cfg.Notifications.DestinationProfiles!);
        var program = File.ReadAllText(Path.Combine(
            FindRoot(), "src", "backend", "Kafdeck.Api", "Program.cs"));
        Assert.DoesNotContain("AddHostedService<NotificationDeliveryWorker>", program);
        Assert.DoesNotContain("AddSingleton<NotificationProviderDeliveryDispatcher>", program);
    }

    [Fact]
    public void Refuses_unknown_destination_fields_and_raw_provider_material()
    {
        var config = ValidEmailConfig();
        config["Kafdeck:Notifications:Destinations:0:CredentialSecret"] =
            "raw-provider-secret";
        Assert.Throws<KafdeckConfigurationException>(() => Load(config));
    }

    [Fact]
    public void Refuses_duplicate_exact_destination_identity()
    {
        var config = ValidEmailConfig();
        config["Kafdeck:Notifications:Destinations:1:DestinationId"] = "ops-email";
        config["Kafdeck:Notifications:Destinations:1:Provider"] = "Email";
        config["Kafdeck:Notifications:Destinations:1:DisplayName"] = "Duplicate";
        config["Kafdeck:Notifications:Destinations:1:EnabledEvents:0"] =
            "Operational";
        config["Kafdeck:Notifications:Destinations:1:CredentialBindingId"] =
            "other-binding";
        config["Kafdeck:Notifications:Destinations:1:EmailRecipientAddress"] =
            "other@example.com";
        Assert.Throws<KafdeckConfigurationException>(() => Load(config));
    }

    [Fact]
    public void Refuses_unsupported_provider_origin_on_typed_email_destination()
    {
        var config = ValidEmailConfig();
        config["Kafdeck:Notifications:Destinations:0:ConfiguredEndpoint"] =
            "https://external.example/somewhere";
        Assert.Throws<KafdeckConfigurationException>(() => Load(config));
    }

    [Fact]
    public void Refuses_unbound_typed_provider_credentials()
    {
        var config = ValidEmailConfig();
        config.Remove("Kafdeck:Notifications:Destinations:0:CredentialBindingId");
        Assert.Throws<KafdeckConfigurationException>(() => Load(config));
    }

    [Fact]
    public void Refuses_unordered_array_indices_and_unclassified_event()
    {
        var config = ValidEmailConfig();
        config["Kafdeck:Notifications:Destinations:1:DestinationId"] = "gap";
        config.Remove("Kafdeck:Notifications:Destinations:0:DestinationId");
        Assert.Throws<KafdeckConfigurationException>(() => Load(config));

        config = ValidEmailConfig();
        config["Kafdeck:Notifications:Destinations:0:EnabledEvents:0"] = "Unknown";
        Assert.Throws<KafdeckConfigurationException>(() => Load(config));
    }

    [Fact]
    public void Disabled_observation_refuses_loaded_destination_definitions()
    {
        var config = ValidEmailConfig();
        config["Kafdeck:Notifications:Enabled"] = "false";
        var options = Load(config);
        var ex = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
        Assert.Contains("profiles require explicitly enabled", ex.Message);
    }

    [Fact]
    public void Programmatic_null_catalog_entry_fails_with_configuration_error()
    {
        var configured = Load(ValidEmailConfig());
        var malformed = configured with
        {
            Notifications = configured.Notifications! with
            {
                DestinationProfiles = new NotificationDestinationProfile[] { null! },
            },
        };
        var error = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(malformed));
        Assert.Contains("bounded and have distinct IDs", error.Message);
    }

    private static string FindRoot()
    {
        DirectoryInfo? cursor = new(AppContext.BaseDirectory);
        while (cursor is not null)
        {
            if (File.Exists(Path.Combine(cursor.FullName, "Kafdeck.slnx")))
                return cursor.FullName;
            cursor = cursor.Parent;
        }
        throw new DirectoryNotFoundException("Kafdeck root is unavailable.");
    }
}
