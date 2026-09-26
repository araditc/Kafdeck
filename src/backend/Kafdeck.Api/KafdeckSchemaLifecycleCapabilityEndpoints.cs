using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

public sealed record SchemaRegistryCapabilityState(
    string State,
    string? ReasonCode);

public sealed record SchemaRegistryCapabilitiesData(
    string ProviderProfile,
    bool MutationModeEnabled,
    IReadOnlyList<string> SchemaTypes,
    IReadOnlyDictionary<string, SchemaRegistryCapabilityState> Capabilities);

public static class SchemaRegistryCapabilityProjection
{
    public static SchemaRegistryCapabilitiesData Create(
        KafdeckOptions options,
        string clusterId)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        var cluster = options.Clusters.SingleOrDefault(item =>
            string.Equals(item.Id, clusterId, StringComparison.Ordinal));

        if (cluster is null)
        {
            throw new ArgumentException(
                "Cluster is not configured.",
                nameof(clusterId));
        }

        var mutationModeEnabled =
            options.Administration?.Mutations.Enabled == true;
        var registry = cluster.SchemaRegistry;
        if (registry is null)
        {
            return new SchemaRegistryCapabilitiesData(
                "Unconfigured",
                mutationModeEnabled,
                Array.Empty<string>(),
                BuildUniform(
                    "unconfigured",
                    "schema_registry_not_configured"));
        }

        var provider =
            SchemaRegistryProviderPolicy.Get(
                registry.ProviderProfile);

        var readState = provider.SupportsRead
            ? Supported()
            : Unsupported(provider.LimitationCode);

        SchemaRegistryCapabilityState MutationState(
            bool providerSupported) =>
            !providerSupported
                ? Unsupported(provider.LimitationCode)
                : mutationModeEnabled
                    ? Supported()
                    : Blocked("mutation_mode_disabled");

        var capabilities =
            new SortedDictionary<
                string,
                SchemaRegistryCapabilityState>(
                StringComparer.Ordinal)
            {
                ["compatibilityMutation"] =
                    MutationState(
                        provider.SupportsCompatibilityMutation),
                ["compatibilityRead"] = readState,
                ["compatibilityValidation"] =
                    provider.SupportsCompatibilityValidation
                        ? Supported()
                        : Unsupported(provider.LimitationCode),
                ["permanentDelete"] =
                    MutationState(
                        provider.SupportsPermanentDelete),
                ["registration"] =
                    MutationState(
                        provider.SupportsRegistration),
                ["schemaRead"] = readState,
                ["softDelete"] =
                    MutationState(
                        provider.SupportsSoftDelete),
                ["subjectRead"] = readState,
            };

        return new SchemaRegistryCapabilitiesData(
            provider.ProviderProfile,
            mutationModeEnabled,
            provider.SupportsRead
                ? new[] { "Avro", "Protobuf", "JsonSchema" }
                : Array.Empty<string>(),
            capabilities);
    }

    private static IReadOnlyDictionary<
        string,
        SchemaRegistryCapabilityState> BuildUniform(
        string state,
        string reasonCode)
    {
        var result =
            new SortedDictionary<
                string,
                SchemaRegistryCapabilityState>(
                StringComparer.Ordinal);

        foreach (var name in new[]
                 {
                     "compatibilityMutation",
                     "compatibilityRead",
                     "compatibilityValidation",
                     "permanentDelete",
                     "registration",
                     "schemaRead",
                     "softDelete",
                     "subjectRead",
                 })
        {
            result[name] =
                new SchemaRegistryCapabilityState(
                    state,
                    reasonCode);
        }

        return result;
    }

    private static SchemaRegistryCapabilityState Supported() =>
        new("supported", null);

    private static SchemaRegistryCapabilityState Unsupported(
        string? reasonCode) =>
        new(
            "unsupported",
            reasonCode ??
                "schema_registry_provider_unsupported");

    private static SchemaRegistryCapabilityState Blocked(
        string reasonCode) =>
        new("blocked", reasonCode);
}

public static class KafdeckSchemaLifecycleCapabilityEndpoints
{
    public static WebApplication MapKafdeckV07SchemaCapabilities(
        this WebApplication app,
        KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(options);

        app.MapGet(
                "/api/v1/clusters/{clusterId}/schemas/capabilities",
                (string clusterId) =>
                {
                    if (!options.Clusters.Any(cluster =>
                            string.Equals(
                                cluster.Id,
                                clusterId,
                                StringComparison.Ordinal)))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(
                                clusterId));
                    }

                    return Results.Ok(
                        SchemaRegistryCapabilityProjection.Create(
                            options,
                            clusterId));
                })
            .WithName("v07-schema-registry-capabilities")
            .RequireKafdeckCollectionAuthorization(
                AuthorizationAction.SchemaRead,
                "clusterId");

        return app;
    }
}
