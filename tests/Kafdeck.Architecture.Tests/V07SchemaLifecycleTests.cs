using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.SchemaRegistry;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
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
    public async Task Schema_capability_endpoint_is_versioned_and_get_only()
    {
        var builder = WebApplication.CreateBuilder();
        await using var app = builder.Build();

        app.MapKafdeckV07SchemaCapabilities(
            Options(
                SchemaRegistryProviderProfile.ConfluentCompatibleV1,
                mutationsEnabled: false));

        var endpoint =
            ((IEndpointRouteBuilder)app).DataSources
                .SelectMany(source => source.Endpoints)
                .OfType<RouteEndpoint>()
                .Single(item =>
                    string.Equals(
                        item.RoutePattern.RawText,
                        "/api/v1/clusters/{clusterId}/schemas/capabilities",
                        StringComparison.Ordinal));

        var methods =
            endpoint.Metadata.GetMetadata<HttpMethodMetadata>();

        Assert.NotNull(methods);
        Assert.Equal(
            new[] { HttpMethods.Get },
            methods!.HttpMethods);
    }

    [Fact]
    public void Configuration_loader_reads_explicit_schema_registry_provider_profile()
    {
        var configuration =
            new ConfigurationBuilder()
                .AddInMemoryCollection(
                    new Dictionary<string, string?>
                    {
                        ["Kafdeck:Deployment:ListenUrl"] =
                            "http://127.0.0.1:8080",
                        ["Kafdeck:Clusters:0:Id"] = "cluster-a",
                        ["Kafdeck:Clusters:0:BootstrapServers:0"] =
                            "localhost:9092",
                        ["Kafdeck:Clusters:0:SecurityProtocol"] =
                            "Plaintext",
                        ["Kafdeck:Clusters:0:SchemaRegistry:Url"] =
                            "https://registry.example",
                        ["Kafdeck:Clusters:0:SchemaRegistry:ProviderProfile"] =
                            "KarapaceCompatibleV1",
                    })
                .Build();

        var options =
            KafdeckConfigurationLoader.Load(
                configuration);

        Assert.Equal(
            SchemaRegistryProviderProfile.KarapaceCompatibleV1,
            Assert.Single(options.Clusters)
                .SchemaRegistry!
                .ProviderProfile);
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
