using Kafdeck.Core.ReadViews;

namespace Kafdeck.Core.Consumers;

public enum ConsumerGroupState
{
    Unknown = 0,
    PreparingRebalance = 1,
    CompletingRebalance = 2,
    Stable = 3,
    Empty = 4,
    Dead = 5,
}

public enum ConsumerOffsetState
{
    Observed = 1,
    MissingCommittedOffset = 2,
    EndOffsetUnavailable = 3,
    TopicPartitionUnavailable = 4,
    Unauthorized = 5,
    CommittedOffsetOutOfRange = 6,
}

public sealed record ConsumerPartitionAssignment(string Topic, int Partition);

public sealed record ConsumerMemberProjection(
    string MemberId,
    string? GroupInstanceId,
    string? ClientId,
    string? ClientHost,
    IReadOnlyList<ConsumerPartitionAssignment> Assignments);

public sealed record ConsumerGroupSummary(
    string GroupId,
    ConsumerGroupState State,
    int? MemberCount,
    bool IsSimpleConsumerGroup);

public sealed record ConsumerGroupDetail(
    string GroupId,
    ConsumerGroupState State,
    string? ProtocolType,
    string? Protocol,
    string? Coordinator,
    IReadOnlyList<ConsumerMemberProjection> Members);

public sealed record ConsumerOffsetProjection(
    string Topic,
    int Partition,
    long? CommittedOffset,
    long? EndOffset,
    long? Lag,
    ConsumerOffsetState State);

public sealed record ConsumerLagProjection(
    string GroupId,
    IReadOnlyList<ConsumerOffsetProjection> Partitions,
    long? TotalLag,
    bool IsPartial,
    IReadOnlyList<ReadViewLimitation> Limitations);

public interface IConsumerGroupReadPort
{
    Task<ReadViewResult<IReadOnlyList<ConsumerGroupSummary>>> ListGroupsAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);

    Task<ReadViewResult<ConsumerGroupDetail>> GetGroupAsync(
        string clusterId,
        string groupId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);

    Task<ReadViewResult<IReadOnlyList<ConsumerOffsetProjection>>> GetOffsetsAsync(
        string clusterId,
        string groupId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);
}
