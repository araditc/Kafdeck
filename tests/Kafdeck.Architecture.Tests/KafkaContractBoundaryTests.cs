using System.Reflection;
using Kafdeck.Core;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Clusters;
using Kafdeck.Modules.Consumers;
using Kafdeck.Modules.Schemas;
using Kafdeck.Modules.Topics;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaContractBoundaryTests
{
    [Fact]
    public void Product_contract_assemblies_do_not_reference_confluent_kafka()
    {
        Assembly[] contractAssemblies =
        [
            typeof(ProductIdentity).Assembly,
            typeof(ClustersModuleMarker).Assembly,
            typeof(TopicsModuleMarker).Assembly,
            typeof(ConsumersModuleMarker).Assembly,
            typeof(SchemasModuleMarker).Assembly,
        ];

        foreach (var assembly in contractAssemblies)
        {
            Assert.DoesNotContain(
                assembly.GetReferencedAssemblies(),
                reference => string.Equals(reference.Name, "Confluent.Kafka", StringComparison.Ordinal));
        }
    }

    [Fact]
    public void Every_kafka_admin_operation_has_deadline_and_cancellation_contracts()
    {
        var operations = typeof(IKafkaAdministrationPort).GetMethods();

        Assert.NotEmpty(operations);
        Assert.All(operations, operation =>
        {
            var parameterTypes = operation.GetParameters().Select(parameter => parameter.ParameterType).ToArray();
            Assert.Contains(typeof(KafkaOperationContext), parameterTypes);
            Assert.Contains(typeof(CancellationToken), parameterTypes);
        });
    }

    [Fact]
    public void Capability_contract_is_capability_first_and_has_no_version_guess_field()
    {
        var publicPropertyNames = typeof(KafkaCapabilities)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Select(property => property.Name)
            .ToArray();

        Assert.DoesNotContain(publicPropertyNames, name => name.Contains("Version", StringComparison.OrdinalIgnoreCase));
    }
}
