using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

public static class NotificationPhysicalDeliveryAdmission
{
    public static void Validate(NotificationReadOptions options)
    {
        if (!options.Enabled || options.ExecutionMode != NotificationExecutionMode.Standalone ||
            options.DestinationProfiles is not { Count: > 0 and <= 500 })
            throw new KafdeckConfigurationException("Physical notification delivery requires standalone persistence and bounded server-owned profiles.");
        foreach (var profile in options.DestinationProfiles)
        {
            if (profile is null) throw new KafdeckConfigurationException("Physical destination catalog contains an invalid profile.");
            if (!profile.Enabled) continue;
            if (profile.Provider == NotificationProviderKind.Webhook)
            {
                if (!options.WebhookDeliveryEnabled ||
                    (profile.WebhookAuthentication == NotificationWebhookAuthentication.Anonymous && profile.CredentialBindingId is not null))
                    throw new KafdeckConfigurationException("Physical Webhook requires its explicit activation and admitted authentication mode.");
            }
            else if (profile.Provider == NotificationProviderKind.Email)
            {
                if (!options.EmailDeliveryEnabled || profile.Smtp is null || profile.CredentialBindingId is null ||
                    !NotificationSmtpProfile.IsMailbox(profile.EmailRecipientAddress))
                    throw new KafdeckConfigurationException("Physical Email requires explicit implicit-TLS SMTP activation and exact server/sender/recipient/credential ownership.");
            }
            else if (!options.HttpProviderDeliveryEnabled || profile.ConfiguredEndpoint is not null ||
                     profile.CredentialBindingId is null ||
                     !NotificationHttpProviderTargetPolicy.IsValid(profile.Provider, profile.ProviderResourceId, profile.ProviderSecondaryResourceId))
                throw new KafdeckConfigurationException("Physical provider family or exact typed target is not admitted.");
            if (profile.CredentialBindingId is not null &&
                !(options.CredentialBindings ?? Array.Empty<NotificationCredentialReferenceOptions>()).Any(binding =>
                    binding is not null && string.Equals(binding.DestinationId, profile.DestinationId, StringComparison.Ordinal) &&
                    string.Equals(binding.BindingId?.Value, profile.CredentialBindingId.Value, StringComparison.Ordinal)))
                throw new KafdeckConfigurationException("Physical provider requires an exact configured credential binding.");
        }
    }
}
