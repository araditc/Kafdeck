using System.Collections.Frozen;
using Kafdeck.Core.Catalog;

namespace Kafdeck.Infrastructure.Configuration;

public sealed class ConfigurationTopicCatalogProvider : ITopicCatalogProvider
{
    private readonly FrozenDictionary<(string ClusterId, string TopicName), TopicCatalogEntry> _topics;

    public ConfigurationTopicCatalogProvider(KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _topics = (options.Catalog?.Topics ?? Array.Empty<TopicCatalogEntryProfile>())
            .ToDictionary(
                entry => (entry.ClusterId, entry.TopicName),
                entry => new TopicCatalogEntry(
                    entry.ClusterId,
                    entry.TopicName,
                    entry.Description,
                    entry.Owner,
                    entry.Domain,
                    entry.Tags.ToArray(),
                    entry.DocumentationReference,
                    entry.Classification))
            .ToFrozenDictionary();
    }

    public TopicCatalogEntry? GetTopic(string clusterId, string topicName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);

        return _topics.TryGetValue((clusterId, topicName), out var entry)
            ? entry
            : null;
    }
}
