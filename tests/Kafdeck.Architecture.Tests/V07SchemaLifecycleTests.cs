using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.SchemaRegistry;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07SchemaLifecycleTests
{
    [Fact]
    public void Default_profile_preserves_the_released_confluent_compatible_behavior()
    {
        var profile = new SchemaRegistryProfile(
            "https://registry.example",
            null,
            null);

        Assert.Equal(
            SchemaRegistryProviderProfile.ConfluentCompatibleV1,
            profile.ProviderProfile);

        var capabilities =
            SchemaRegistryProviderPolicy.Get(
                profile.ProviderProfile);

        Assert.True(capabilities.UsesConfluentCompatibleApi);
        Assert.True(capabilities.SupportsRead);
        Assert.True(capabilities.SupportsRegistration);
        Assert.Null(capabilities.LimitationCode);
    }

    [Fact]
    public void Capability_projection_separates_provider_support_from_mutation_activation()
    {
        var disabled =
            SchemaRegistryCapabilityProjection.Create(
                Options(
                    SchemaRegistryProviderProfile.KarapaceCompatibleV1,
                    mutationsEnabled: false),
                "cluster-a");

        Assert.Equal(
            nameof(SchemaRegistryProviderProfile.KarapaceCompatibleV1),
            disabled.ProviderProfile);
        Assert.Equal(
            "supported",
            disabled.Capabilities["subjectRead"].State);
        Assert.Equal(
            "blocked",
            disabled.Capabilities["registration"].State);
        Assert.Equal(
            "mutation_mode_disabled",
            disabled.Capabilities["registration"].ReasonCode);

        var enabled =
            SchemaRegistryCapabilityProjection.Create(
                Options(
                    SchemaRegistryProviderProfile.KarapaceCompatibleV1,
                    mutationsEnabled: true),
                "cluster-a");

        Assert.Equal(
            "supported",
            enabled.Capabilities["registration"].State);
        Assert.Equal(
            "supported",
            enabled.Capabilities["permanentDelete"].State);
        Assert.Equal(
            new[] { "Avro", "Protobuf", "JsonSchema" },
            enabled.SchemaTypes);
    }

    [Fact]
    public void Apicurio_capability_projection_is_explicitly_unsupported()
    {
        var result =
            SchemaRegistryCapabilityProjection.Create(
                Options(
                    SchemaRegistryProviderProfile.ApicurioV3,
                    mutationsEnabled: true),
                "cluster-a");

        Assert.Equal(
            nameof(SchemaRegistryProviderProfile.ApicurioV3),
            result.ProviderProfile);
        Assert.Empty(result.SchemaTypes);
        Assert.All(
            result.Capabilities.Values,
            capability =>
            {
                Assert.Equal("unsupported", capability.State);
                Assert.Equal(
                    "schema_registry_apicurio_adapter_not_admitted",
                    capability.ReasonCode);
            });
    }

    [Fact]
    public async Task Apicurio_catalog_reads_fail_as_unsupported_before_http()
    {
        using var adapter =
            new ConfluentSchemaCatalogReadAdapter(
                Options(
                    SchemaRegistryProviderProfile.ApicurioV3,
                    mutationsEnabled: false).Clusters,
                new SecretResolver());

        var result = await adapter.ListSubjectsAsync(
            "cluster-a",
            new ReadViewOperationContext(
                DateTimeOffset.UtcNow.AddSeconds(10)),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ReadViewFailureCategory.Unsupported,
            result.Failure!.Category);
        Assert.Equal(
            "schema_registry_apicurio_adapter_not_admitted",
            result.Failure.Code);
    }

    [Fact]
    public async Task Apicurio_record_schema_reads_fail_as_provider_unsupported_before_http()
    {
        using var adapter =
            new ConfluentSchemaRegistryReadAdapter(
                Options(
                    SchemaRegistryProviderProfile.ApicurioV3,
                    mutationsEnabled: false).Clusters,
                new SecretResolver());

        var result = await adapter.GetSchemaByIdAsync(
            "cluster-a",
            1,
            new KafkaOperationContext(
                DateTimeOffset.UtcNow.AddSeconds(10)),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            RecordSchemaFailureCategory.ProviderUnsupported,
            result.Failure!.Category);
        Assert.Equal(
            "schema_registry_apicurio_adapter_not_admitted",
            result.Failure.Code);
    }

    [Fact]
    public void Released_schema_authorization_identifiers_remain_canonical()
    {
        Assert.Equal(
            AuthorizationAction.SchemaCreate,
            MutationAuthorization.ExpectedAction(
                MutationOperationKind.SchemaCreate));
        Assert.Equal(
            AuthorizationAction.SchemaAlter,
            MutationAuthorization.ExpectedAction(
                MutationOperationKind.SchemaAlter));
        Assert.Equal(
            AuthorizationAction.SchemaDelete,
            MutationAuthorization.ExpectedAction(
                MutationOperationKind.SchemaDelete));
    }

    [Fact]
    public void Configuration_validation_rejects_unknown_provider_profile()
    {
        var options =
            Options(
                (SchemaRegistryProviderProfile)999,
                mutationsEnabled: false);

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(
                options));

        Assert.Contains(
            "Schema Registry provider profile is unsupported",
            exception.Message,
            StringComparison.Ordinal);
    }

    private static KafdeckOptions Options(
        SchemaRegistryProviderProfile providerProfile,
        bool mutationsEnabled)
    {
        var administration = mutationsEnabled
            ? new AdministrationOptions(
                new MutationOptions(
                    Enabled: true,
                    Persistence: null,
                    MaterialDigestKey: null,
                    PreviewTtl: TimeSpan.FromMinutes(5),
                    MaxConcurrentPerCluster: 2))
            : null;

        return new KafdeckOptions(
            new DeploymentOptions(
                "http://127.0.0.1:8080",
                null),
            [
                new ClusterProfile(
                    "cluster-a",
                    ["localhost:9092"],
                    KafkaSecurityProtocol.Plaintext,
                    null,
                    null,
                    new SchemaRegistryProfile(
                        "https://registry.example",
                        null,
                        null,
                        providerProfile)),
            ],
            Administration: administration);
    }
}
