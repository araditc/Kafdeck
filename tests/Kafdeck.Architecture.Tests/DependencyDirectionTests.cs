using System.Reflection;
using Kafdeck.Core;
using Kafdeck.Modules.Clusters;
using Kafdeck.Modules.Topics;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class DependencyDirectionTests
{
    [Fact]
    public void Core_does_not_reference_infrastructure_or_kafka_client_types()
    {
        AssertNoForbiddenReferences(
            typeof(KafdeckCoreAssemblyMarker).Assembly,
            "Kafdeck.Infrastructure",
            "Confluent.Kafka",
            "Microsoft.IdentityModel",
            "Microsoft.AspNetCore.Authentication.OpenIdConnect",
            "IdentityModel");
    }

    [Theory]
    [MemberData(nameof(ModuleAssemblies))]
    public void Modules_do_not_reference_infrastructure(Assembly moduleAssembly)
    {
        AssertNoForbiddenReferences(moduleAssembly, "Kafdeck.Infrastructure", "Confluent.Kafka");
    }

    public static TheoryData<Assembly> ModuleAssemblies => new()
    {
        typeof(ClustersModuleMarker).Assembly,
        typeof(TopicsModuleMarker).Assembly,
    };

    private static void AssertNoForbiddenReferences(Assembly assembly, params string[] forbiddenPrefixes)
    {
        var violations = assembly
            .GetReferencedAssemblies()
            .Select(reference => reference.Name ?? string.Empty)
            .Where(reference => forbiddenPrefixes.Any(prefix => reference.StartsWith(prefix, StringComparison.Ordinal)))
            .OrderBy(reference => reference, StringComparer.Ordinal)
            .ToArray();

        Assert.True(
            violations.Length == 0,
            $"{assembly.GetName().Name} contains forbidden references: {string.Join(", ", violations)}");
    }
}
