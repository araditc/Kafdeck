using System.Text.Json;

namespace Kafdeck.Core.Records;

public enum ControlledSerdeFormat
{
    Cbor = 1,
    Xml = 2,
    MessagePack = 3,
}

public sealed record ControlledSerdeLimits
{
    public const int HardMaxInputBytes = 16 * 1024 * 1024;
    public const int HardMaxOutputBytes = 16 * 1024 * 1024;
    public const int HardMaxDepth = 64;
    public const int HardMaxNodes = 20_000;
    public const int HardMaxCollectionItems = 10_000;
    public const int HardMaxStringCharacters = 4 * 1024 * 1024;
    public const int HardMaxBinaryBytes = 16 * 1024 * 1024;

    public static ControlledSerdeLimits Default { get; } =
        new(
            maxInputBytes: 4 * 1024 * 1024,
            maxOutputBytes: 4 * 1024 * 1024,
            maxDepth: 32,
            maxNodes: 10_000,
            maxCollectionItems: 5_000,
            maxStringCharacters: 1 * 1024 * 1024,
            maxBinaryBytes: 4 * 1024 * 1024);

    public ControlledSerdeLimits(
        int maxInputBytes,
        int maxOutputBytes,
        int maxDepth,
        int maxNodes,
        int maxCollectionItems,
        int maxStringCharacters,
        int maxBinaryBytes)
    {
        if (maxInputBytes is < 1 or > HardMaxInputBytes)
            throw new ArgumentOutOfRangeException(nameof(maxInputBytes));
        if (maxOutputBytes is < 1 or > HardMaxOutputBytes)
            throw new ArgumentOutOfRangeException(nameof(maxOutputBytes));
        if (maxDepth is < 1 or > HardMaxDepth)
            throw new ArgumentOutOfRangeException(nameof(maxDepth));
        if (maxNodes is < 1 or > HardMaxNodes)
            throw new ArgumentOutOfRangeException(nameof(maxNodes));
        if (maxCollectionItems is < 1 or > HardMaxCollectionItems)
            throw new ArgumentOutOfRangeException(nameof(maxCollectionItems));
        if (maxStringCharacters is < 1 or > HardMaxStringCharacters)
            throw new ArgumentOutOfRangeException(nameof(maxStringCharacters));
        if (maxBinaryBytes is < 1 or > HardMaxBinaryBytes)
            throw new ArgumentOutOfRangeException(nameof(maxBinaryBytes));

        MaxInputBytes = maxInputBytes;
        MaxOutputBytes = maxOutputBytes;
        MaxDepth = maxDepth;
        MaxNodes = maxNodes;
        MaxCollectionItems = maxCollectionItems;
        MaxStringCharacters = maxStringCharacters;
        MaxBinaryBytes = maxBinaryBytes;
    }

    public int MaxInputBytes { get; }
    public int MaxOutputBytes { get; }
    public int MaxDepth { get; }
    public int MaxNodes { get; }
    public int MaxCollectionItems { get; }
    public int MaxStringCharacters { get; }
    public int MaxBinaryBytes { get; }
}

public enum ControlledSerdeFailureCategory
{
    InvalidRequest = 1,
    Unsupported = 2,
    MalformedInput = 3,
    BoundExceeded = 4,
    Cancelled = 5,
    Timeout = 6,
}

public sealed record ControlledSerdeFailure(
    ControlledSerdeFailureCategory Category,
    string Code,
    string SafeMessage,
    bool IsRetryable = false);

public sealed record ControlledSerdeCapability(
    ControlledSerdeFormat Format,
    bool DecodeSupported,
    bool EncodeSupported,
    IReadOnlyList<string> Limitations);

public sealed record ControlledSerdeDecodeRequest(
    ControlledSerdeFormat Format,
    ReadOnlyMemory<byte> Payload);

public sealed record ControlledSerdeEncodeRequest(
    ControlledSerdeFormat Format,
    JsonElement StructuredValue);

public sealed record ControlledSerdeDecodedValue(
    ControlledSerdeFormat Format,
    JsonElement StructuredValue);

public sealed record ControlledSerdeEncodedValue(
    ControlledSerdeFormat Format,
    ReadOnlyMemory<byte> Payload);

public sealed record ControlledSerdeResult<T>
{
    private ControlledSerdeResult(T? value, ControlledSerdeFailure? failure)
    {
        Value = value;
        Failure = failure;
    }

    public T? Value { get; }
    public ControlledSerdeFailure? Failure { get; }
    public bool IsSuccess => Failure is null;

    public static ControlledSerdeResult<T> Success(T value) =>
        new(value, null);

    public static ControlledSerdeResult<T> Failed(
        ControlledSerdeFailure failure) =>
        new(default, failure ?? throw new ArgumentNullException(nameof(failure)));
}

public interface IControlledSerdePort
{
    IReadOnlyList<ControlledSerdeCapability> GetCapabilities();

    Task<ControlledSerdeResult<ControlledSerdeDecodedValue>> DecodeAsync(
        ControlledSerdeDecodeRequest request,
        ControlledSerdeLimits limits,
        DateTimeOffset deadlineUtc,
        CancellationToken cancellationToken = default);

    Task<ControlledSerdeResult<ControlledSerdeEncodedValue>> EncodeAsync(
        ControlledSerdeEncodeRequest request,
        ControlledSerdeLimits limits,
        DateTimeOffset deadlineUtc,
        CancellationToken cancellationToken = default);
}
