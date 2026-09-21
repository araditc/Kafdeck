namespace Kafdeck.Modules.Administration;

public sealed record MutationProviderResult(
    MutationExecutionResultKind ResultKind,
    string ResultCode,
    IReadOnlyDictionary<string, string>? SafeEvidence = null);

public sealed record TopicCreateMutation(
    string ClusterId,
    string TopicName,
    int PartitionCount,
    short ReplicationFactor,
    IReadOnlyDictionary<string, string> Configurations);

public sealed record TopicAlterMutation(
    string ClusterId,
    string TopicName,
    IReadOnlyDictionary<string, string?> Changes);

public sealed record TopicIncreasePartitionsMutation(
    string ClusterId,
    string TopicName,
    int NewPartitionCount);

public sealed record TopicDeleteMutation(string ClusterId, string TopicName);

public interface ITopicMutationPort
{
    Task<MutationProviderResult> CreateTopicAsync(
        TopicCreateMutation request,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> AlterTopicAsync(
        TopicAlterMutation request,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> IncreasePartitionsAsync(
        TopicIncreasePartitionsMutation request,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> DeleteTopicAsync(
        TopicDeleteMutation request,
        CancellationToken cancellationToken = default);
}

public sealed record RecordProduceMutation(
    string ClusterId,
    string TopicName,
    ReadOnlyMemory<byte>? Key,
    ReadOnlyMemory<byte> Value,
    IReadOnlyDictionary<string, ReadOnlyMemory<byte>> Headers);

public interface IRecordProduceMutationPort
{
    Task<MutationProviderResult> ProduceAsync(
        RecordProduceMutation request,
        CancellationToken cancellationToken = default);
}

public sealed record ConsumerOffsetTarget(
    string TopicName,
    int Partition,
    long Offset);

public sealed record ConsumerOffsetAlterMutation(
    string ClusterId,
    string GroupId,
    IReadOnlyList<ConsumerOffsetTarget> Targets);

public sealed record ConsumerDeleteMutation(
    string ClusterId,
    string GroupId,
    IReadOnlyList<ConsumerOffsetTarget>? Offsets = null);

public interface IConsumerMutationPort
{
    Task<MutationProviderResult> AlterOffsetsAsync(
        ConsumerOffsetAlterMutation request,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> DeleteAsync(
        ConsumerDeleteMutation request,
        CancellationToken cancellationToken = default);
}

public sealed record SchemaCreateMutation(
    string ClusterId,
    string Subject,
    string SchemaType,
    string Schema,
    IReadOnlyList<string> References);

public sealed record SchemaAlterMutation(
    string ClusterId,
    string Subject,
    string CompatibilityMode);

public sealed record SchemaDeleteMutation(
    string ClusterId,
    string Subject,
    int? Version,
    bool Permanent);

public interface ISchemaMutationPort
{
    Task<MutationProviderResult> CreateAsync(
        SchemaCreateMutation request,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> AlterCompatibilityAsync(
        SchemaAlterMutation request,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> DeleteAsync(
        SchemaDeleteMutation request,
        CancellationToken cancellationToken = default);
}

public enum ConnectControlAction
{
    Pause = 1,
    Resume = 2,
    Restart = 3,
}

public sealed record ConnectCreateMutation(
    string ClusterId,
    string ConnectorName,
    IReadOnlyDictionary<string, string> Configuration);

public sealed record ConnectAlterMutation(
    string ClusterId,
    string ConnectorName,
    IReadOnlyDictionary<string, string?> Changes);

public sealed record ConnectControlMutation(
    string ClusterId,
    string ConnectorName,
    int? TaskId,
    ConnectControlAction Action);

public sealed record ConnectDeleteMutation(
    string ClusterId,
    string ConnectorName);

public interface IConnectMutationPort
{
    Task<MutationProviderResult> CreateAsync(
        ConnectCreateMutation request,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> AlterAsync(
        ConnectAlterMutation request,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> ControlAsync(
        ConnectControlMutation request,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> DeleteAsync(
        ConnectDeleteMutation request,
        CancellationToken cancellationToken = default);
}

public sealed record RecordsPurgeTarget(
    string TopicName,
    int Partition,
    long BeforeOffset);

public sealed record RecordsPurgeMutation(
    string ClusterId,
    IReadOnlyList<RecordsPurgeTarget> Targets);

public interface IRecordsPurgeMutationPort
{
    Task<MutationProviderResult> PurgeAsync(
        RecordsPurgeMutation request,
        CancellationToken cancellationToken = default);
}

public interface IMutationMaterialDigestService
{
    string ComputeDigest(ReadOnlySpan<byte> material);
}
