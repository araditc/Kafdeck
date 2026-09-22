using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Consumers;
using Kafdeck.Modules.Records;

namespace Kafdeck.Api;

/// <summary>
/// Builds the exact authorization targets that are known from a mutation HTTP
/// request before any provider-backed planning observation is allowed to run.
/// Malformed requests return null and are left to the typed planner, which
/// rejects them before provider observation.
/// </summary>
public static class MutationDirectRouteAuthorization
{
    public static IReadOnlyList<MutationAuthorizationTarget>? ConsumerOffsetAlter(
        string clusterId,
        string groupId,
        IReadOnlyList<ConsumerOffsetAlterTargetInput>? targets) =>
        ConsumerTargets(
            AuthorizationAction.ConsumerOffsetAlter,
            clusterId,
            groupId,
            targets?.Select(target => target?.TopicName));

    public static IReadOnlyList<MutationAuthorizationTarget>? ConsumerDelete(
        string clusterId,
        string groupId,
        ConsumerDeleteMode mode,
        IReadOnlyList<ConsumerOffsetDeleteTargetInput>? targets)
    {
        if (!IsIdentifier(clusterId, 256) ||
            !IsIdentifier(groupId, 255) ||
            !Enum.IsDefined(mode))
        {
            return null;
        }

        if (mode == ConsumerDeleteMode.Group)
        {
            if (targets is { Count: > 0 })
            {
                return null;
            }

            return Array.AsReadOnly(new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.ConsumerDelete,
                    clusterId,
                    $"consumer-group/{groupId}"),
            });
        }

        return ConsumerTargets(
            AuthorizationAction.ConsumerDelete,
            clusterId,
            groupId,
            targets?.Select(target => target?.TopicName));
    }

    public static IReadOnlyList<MutationAuthorizationTarget>? RecordsPurge(
        string clusterId,
        IReadOnlyList<RecordsPurgeTargetInput>? targets)
    {
        if (!IsIdentifier(clusterId, 256) ||
            targets is null or { Count: 0 } ||
            targets.Count > RecordsPurgePolicy.HardMaxTargets)
        {
            return null;
        }

        var topics = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var target in targets)
        {
            if (target is null ||
                !IsTopic(target.TopicName) ||
                target.Partition < 0 ||
                target.Selector is null)
            {
                return null;
            }

            topics.Add(target.TopicName);
        }

        return Array.AsReadOnly(
            topics.Select(topic => new MutationAuthorizationTarget(
                    AuthorizationAction.RecordsPurge,
                    clusterId,
                    topic))
                .ToArray());
    }

    private static IReadOnlyList<MutationAuthorizationTarget>? ConsumerTargets(
        AuthorizationAction action,
        string clusterId,
        string groupId,
        IEnumerable<string?>? topics)
    {
        if (!IsIdentifier(clusterId, 256) ||
            !IsIdentifier(groupId, 255) ||
            topics is null)
        {
            return null;
        }

        var normalizedTopics = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var topic in topics)
        {
            if (!IsTopic(topic))
            {
                return null;
            }

            normalizedTopics.Add(topic!);
        }

        if (normalizedTopics.Count == 0)
        {
            return null;
        }

        var result = new List<MutationAuthorizationTarget>(normalizedTopics.Count + 1)
        {
            new(action, clusterId, $"consumer-group/{groupId}"),
        };
        result.AddRange(normalizedTopics.Select(topic =>
            new MutationAuthorizationTarget(action, clusterId, $"topic/{topic}")));

        return Array.AsReadOnly(result.ToArray());
    }

    private static bool IsIdentifier(string? value, int maxLength) =>
        !string.IsNullOrWhiteSpace(value) &&
        value.Length <= maxLength &&
        string.Equals(value, value.Trim(), StringComparison.Ordinal) &&
        !value.Any(char.IsControl);

    private static bool IsTopic(string? value) =>
        IsIdentifier(value, 249) &&
        value is not "." and not ".." &&
        value!.All(character =>
            character is >= 'a' and <= 'z' or
                >= 'A' and <= 'Z' or
                >= '0' and <= '9' or
                '.' or '_' or '-');
}
