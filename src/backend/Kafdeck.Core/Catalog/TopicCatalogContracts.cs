namespace Kafdeck.Core.Catalog;

public sealed record TopicCatalogEntry(
    string ClusterId,
    string TopicName,
    string? Description,
    string? Owner,
    string? Domain,
    IReadOnlyList<string> Tags,
    string? DocumentationReference,
    string? Classification);

public interface ITopicCatalogProvider
{
    TopicCatalogEntry? GetTopic(string clusterId, string topicName);
}
