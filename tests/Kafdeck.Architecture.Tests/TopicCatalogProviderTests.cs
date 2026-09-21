using Kafdeck.Infrastructure.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class TopicCatalogProviderTests
{
    [Fact]
    public void Configuration_catalog_is_immutable_descriptive_metadata()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            [
                new ClusterProfile(
                    "prod",
                    ["localhost:9092"],
                    KafkaSecurityProtocol.Plaintext,
                    null,
                    null)
            ],
            Catalog: new TopicCatalogOptions(
            [
                new TopicCatalogEntryProfile(
                    "prod",
                    "payments",
                    "Payment events",
                    "payments-team",
                    "banking",
                    ["pci", "critical"],
                    "docs/payments",
                    "confidential")
            ]));

        var provider = new ConfigurationTopicCatalogProvider(options);
        var entry = provider.GetTopic("prod", "payments");

        Assert.NotNull(entry);
        Assert.Equal("payments-team", entry!.Owner);
        Assert.Equal("banking", entry.Domain);
        Assert.Equal(new[] { "pci", "critical" }, entry.Tags);
        Assert.Null(provider.GetTopic("prod", "missing"));
    }
}
