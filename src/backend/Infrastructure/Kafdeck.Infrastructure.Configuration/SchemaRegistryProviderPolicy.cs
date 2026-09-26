namespace Kafdeck.Infrastructure.Configuration;

public sealed record SchemaRegistryProviderCapabilities(
    string ProviderProfile,
    bool UsesConfluentCompatibleApi,
    bool SupportsRead,
    bool SupportsCompatibilityValidation,
    bool SupportsRegistration,
    bool SupportsCompatibilityMutation,
    bool SupportsSoftDelete,
    bool SupportsPermanentDelete,
    string? LimitationCode);

public static class SchemaRegistryProviderPolicy
{
    public static SchemaRegistryProviderCapabilities Get(
        SchemaRegistryProviderProfile profile) =>
        profile switch
        {
            SchemaRegistryProviderProfile.ConfluentCompatibleV1 =>
                new(
                    nameof(SchemaRegistryProviderProfile.ConfluentCompatibleV1),
                    UsesConfluentCompatibleApi: true,
                    SupportsRead: true,
                    SupportsCompatibilityValidation: true,
                    SupportsRegistration: true,
                    SupportsCompatibilityMutation: true,
                    SupportsSoftDelete: true,
                    SupportsPermanentDelete: true,
                    LimitationCode: null),

            SchemaRegistryProviderProfile.KarapaceCompatibleV1 =>
                new(
                    nameof(SchemaRegistryProviderProfile.KarapaceCompatibleV1),
                    UsesConfluentCompatibleApi: true,
                    SupportsRead: true,
                    SupportsCompatibilityValidation: true,
                    SupportsRegistration: true,
                    SupportsCompatibilityMutation: true,
                    SupportsSoftDelete: true,
                    SupportsPermanentDelete: true,
                    LimitationCode: null),

            SchemaRegistryProviderProfile.ApicurioV3 =>
                new(
                    nameof(SchemaRegistryProviderProfile.ApicurioV3),
                    UsesConfluentCompatibleApi: false,
                    SupportsRead: false,
                    SupportsCompatibilityValidation: false,
                    SupportsRegistration: false,
                    SupportsCompatibilityMutation: false,
                    SupportsSoftDelete: false,
                    SupportsPermanentDelete: false,
                    LimitationCode: "schema_registry_apicurio_adapter_not_admitted"),

            _ => throw new ArgumentOutOfRangeException(
                nameof(profile),
                profile,
                "Schema Registry provider profile is unsupported."),
        };
}
