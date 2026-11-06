using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class W77ProviderFactAttribute : FactAttribute
{
    public W77ProviderFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("KAFDECK_RUN_W77_PROVIDER") != "1")
            Skip = "Requires one explicitly provisioned actual W77 provider/version profile; not inferred from Apache Kafka.";
    }
}

[Collection("W77 bounded laboratory")]
public sealed class V09W77ProviderCapabilityLiveTests
{
    [W77ProviderFact]
    public async Task Actual_provider_core_capability_profile_and_typed_topic_lifecycle()
    {
        var provider = Environment.GetEnvironmentVariable("KAFDECK_W77_PROVIDER")!;
        var version = Environment.GetEnvironmentVariable("KAFDECK_W77_PROVIDER_VERSION")!;
        var bootstrap = Environment.GetEnvironmentVariable("KAFDECK_W77_PROVIDER_BOOTSTRAP")!;
        Assert.Contains(provider, new[] { "apache", "confluent-platform", "redpanda" });
        Assert.Matches("^127\\.0\\.0\\.1:[0-9]{4,5}$", bootstrap);
        var admitted = provider switch
        {
            "apache" => new[] { "4.3.1", "4.2.1", "4.1.2", "3.9.2" },
            "confluent-platform" => new[] { "8.3.2" },
            _ => new[] { "25.3.11" }
        };
        Assert.Contains(version, admitted);
        var profile = new ClusterProfile("w77", [bootstrap], KafkaSecurityProtocol.Plaintext, null, null);
        using var read = new ConfluentKafkaAdministrationAdapter([profile], new SecretResolver());
        using var mutate = new ConfluentKafkaTopicMutationAdapter([profile], new SecretResolver());
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2)); var token = timeout.Token;
        var topic = "w77-capability-" + Guid.NewGuid().ToString("N");
        var transitions = new List<object>(); KafkaCapabilities? capabilities = null;
        async Task Eventually(Func<Task<bool>> predicate)
        {
            for (var attempt = 0; attempt < 40; attempt++) { if (await predicate()) return; await Task.Delay(250, token); }
            Assert.Fail("Actual provider postcondition was absent after the bounded observation window.");
        }
        static KafkaOperationContext Context() => new(DateTimeOffset.UtcNow.AddSeconds(10));
        try
        {
            Assert.Equal(MutationExecutionResultKind.AppliedUnverified, (await mutate.CreateTopicAsync(new TopicCreateMutation("w77", topic, 1, 1,
                new Dictionary<string, string> { ["retention.ms"] = "60000" }), token)).ResultKind);
            await Eventually(async () => { var result = await read.GetTopicMetadataAsync("w77", topic, Context(), token); return result.IsSuccess && result.Value!.Partitions.Count == 1; });
            transitions.Add(new { operation = "TopicCreate", independentlyObserved = true, partitions = 1 });
            var result = await read.GetCapabilitiesAsync("w77", Context(), token); Assert.True(result.IsSuccess);
            capabilities = result.Value!;
            foreach (var kind in new[] { KafkaCapabilityKind.ClusterMetadata, KafkaCapabilityKind.BrokerMetadata, KafkaCapabilityKind.TopicListing,
                KafkaCapabilityKind.TopicMetadata, KafkaCapabilityKind.TopicConfiguration })
                Assert.Contains(capabilities.Items, item => item.Capability == kind && item.State == KafkaCapabilityState.Available);
            Assert.Equal(MutationExecutionResultKind.AppliedUnverified, (await mutate.AlterTopicAsync(new TopicAlterMutation("w77", topic,
                new Dictionary<string, string?> { ["retention.ms"] = "120000" }), token)).ResultKind);
            await Eventually(async () => { var config = await read.GetTopicConfigurationAsync("w77", topic, Context(), token); return config.IsSuccess && config.Value!.Any(item => item.Name == "retention.ms" && item.Value == "120000"); });
            transitions.Add(new { operation = "TopicAlter", independentlyObserved = true, retentionMs = 120000 });
            Assert.Equal(MutationExecutionResultKind.AppliedUnverified, (await mutate.IncreasePartitionsAsync(new TopicIncreasePartitionsMutation("w77", topic, 2), token)).ResultKind);
            await Eventually(async () => { var metadata = await read.GetTopicMetadataAsync("w77", topic, Context(), token); return metadata.IsSuccess && metadata.Value!.Partitions.Count == 2; });
            transitions.Add(new { operation = "TopicIncreasePartitions", independentlyObserved = true, partitions = 2 });
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var deletion = await mutate.DeleteTopicAsync(new TopicDeleteMutation("w77", topic), cleanup.Token);
            Assert.Equal(MutationExecutionResultKind.AppliedUnverified, deletion.ResultKind);
            await Eventually(async () => { var topics = await read.ListTopicsAsync("w77", Context(), cleanup.Token); return topics.IsSuccess && !topics.Value!.Any(item => item.Name == topic); });
            transitions.Add(new { operation = "TopicDelete", independentlyObserved = true, absent = true });
        }
        await W77Laboratory.EvidenceAsync($"w77-h3-{provider}-{version}", new
        {
            acceptance = "H3", sourceSha = Environment.GetEnvironmentVariable("KAFDECK_SOURCE_SHA") ?? Environment.GetEnvironmentVariable("GITHUB_SHA"),
            provider, version, profile = "loopback-plaintext-core-topic-lifecycle", actualBroker = true,
            runtime = Environment.Version.ToString(), client = typeof(Confluent.Kafka.IAdminClient).Assembly.GetName().Version?.ToString(),
            capabilities = capabilities!.Items.Select(item => new { capability = item.Capability.ToString(), state = item.State.ToString() }), transitions,
            topicCleanupVerified = true,
            notClaimed = new[] { "TLS/SASL/ACL/SCRAM on non-Apache profiles", "managed-provider behavior", "Confluent Cloud", "AWS MSK", "Aiven", "Google Managed Kafka", "Azure Event Hubs", "provider-specific APIs", "replication/reassignment/maintenance", "every Kafka administration operation" },
            limitations = "Only the executed exact provider/version/core profile. Existing Apache TLS/SASL/partial-authorization matrix remains separately mandatory. No Kafka version or other-provider inference."
        }, token);
    }
}
