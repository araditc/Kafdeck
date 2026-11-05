using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Catalog;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class ConfluentCatalogTopicIdentityPort(IReadOnlyList<ClusterProfile> profiles, SecretResolver secrets) : ICatalogTopicIdentityPort, IDisposable
{
    private readonly KafkaAdminClientRegistry _clients = new(profiles, secrets);
    public async Task<CatalogTopicIdentity?> ObserveAsync(string clusterId, string topicName, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token); deadline.CancelAfter(TimeSpan.FromSeconds(10));
        try
        {
            var client = _clients.GetClient(clusterId);
            var cluster = await client.DescribeClusterAsync(new DescribeClusterOptions { RequestTimeout = TimeSpan.FromSeconds(10) }).WaitAsync(deadline.Token).ConfigureAwait(false);
            var result = await client.DescribeTopicsAsync(TopicCollection.OfTopicNames([topicName]), new DescribeTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }).WaitAsync(deadline.Token).ConfigureAwait(false);
            var topic = result.TopicDescriptions.Single();
            if (string.IsNullOrWhiteSpace(cluster.ClusterId) || topic.Error.IsError || topic.TopicId is null || topic.IsInternal) return null;
            var id = topic.TopicId.MostSignificantBits.Value.ToString("x16", CultureInfo.InvariantCulture) + topic.TopicId.LeastSignificantBits.Value.ToString("x16", CultureInfo.InvariantCulture);
            return id.All(c => c == '0') ? null : new(cluster.ClusterId, id);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
        catch { return null; }
    }
    public void Dispose() => _clients.Dispose();
}
