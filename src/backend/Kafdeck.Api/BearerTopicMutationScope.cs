using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

/// <summary>Closed topic-family admission. A durable operation kind is never caller-selected.</summary>
internal static class BearerTopicMutationScope
{
    public static bool IsAction(AuthorizationAction action) => action is
        AuthorizationAction.TopicCreate or AuthorizationAction.TopicAlter or AuthorizationAction.TopicDelete;

    public static bool IsKind(MutationOperationKind kind) => kind is MutationOperationKind.TopicCreate or
        MutationOperationKind.TopicAlter or MutationOperationKind.TopicIncreasePartitions or MutationOperationKind.TopicDelete;

    public static AuthorizationAction? PreviewAction(string? route) => route switch
    {
        "v08-bearer-topic-create-preview" => AuthorizationAction.TopicCreate,
        "v08-bearer-topic-alter-preview" or "v08-bearer-topic-increase-partitions-preview" => AuthorizationAction.TopicAlter,
        "v08-bearer-topic-delete-preview" => AuthorizationAction.TopicDelete,
        _ => null,
    };
}
