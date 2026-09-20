using System.Text.Json;
using Kafdeck.Core.Kafka;

namespace Kafdeck.Core.Records;

public enum RecordSchemaFormat
{
    Avro = 1,
    Protobuf = 2,
    JsonSchema = 3,
}

public enum RecordSchemaFailureCategory
{
    RegistryNotConfigured = 1,
    SchemaNotFound = 2,
    Unauthorized = 3,
    Unavailable = 4,
    Timeout = 5,
    Cancelled = 6,
    InvalidResponse = 7,
    UnsupportedFormat = 8,
    DecodeFailed = 9,
}

public sealed record RecordSchemaFailure(
    RecordSchemaFailureCategory Category,
    string Code,
    string SafeMessage,
    bool IsRetryable);

public sealed record RecordSchemaReference(
    string Name,
    string Subject,
    int Version);

public sealed record RecordSchemaDocument(
    int Id,
    RecordSchemaFormat Format,
    string SchemaText,
    IReadOnlyList<RecordSchemaReference> References);

public sealed record RecordSchemaResult<T>
{
    private RecordSchemaResult(T? value, RecordSchemaFailure? failure)
    {
        Value = value;
        Failure = failure;
    }

    public T? Value { get; }

    public RecordSchemaFailure? Failure { get; }

    public bool IsSuccess => Failure is null;

    public static RecordSchemaResult<T> Success(T value) =>
        new(value, null);

    public static RecordSchemaResult<T> Failed(RecordSchemaFailure failure) =>
        new(default, failure ?? throw new ArgumentNullException(nameof(failure)));
}

public sealed record RecordDecodeRequest
{
    public RecordDecodeRequest(
        string clusterId,
        string topicName,
        int partition,
        long offset,
        bool isKey,
        ReadOnlyMemory<byte> payload)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);

        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partition));
        }

        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }

        ClusterId = clusterId.Trim();
        TopicName = topicName.Trim();
        Partition = partition;
        Offset = offset;
        IsKey = isKey;
        Payload = payload;
    }

    public string ClusterId { get; }

    public string TopicName { get; }

    public int Partition { get; }

    public long Offset { get; }

    public bool IsKey { get; }

    public ReadOnlyMemory<byte> Payload { get; }
}

public sealed record RecordDecodedValue(
    int SchemaId,
    RecordSchemaFormat Format,
    JsonElement StructuredValue);

public interface IRecordSchemaReadPort
{
    Task<RecordSchemaResult<RecordSchemaDocument>> GetSchemaByIdAsync(
        string clusterId,
        int schemaId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);

    Task<RecordSchemaResult<RecordSchemaDocument>> GetSchemaBySubjectVersionAsync(
        string clusterId,
        string subject,
        int version,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);
}

public interface IRecordDecodePort
{
    Task<RecordSchemaResult<RecordDecodedValue>> DecodeAsync(
        RecordDecodeRequest request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);
}
