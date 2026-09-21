namespace Kafdeck.Core.ReadViews;

public enum ReadViewFailureCategory
{
    NotConfigured = 1,
    Unauthorized = 2,
    Unsupported = 3,
    Unavailable = 4,
    Timeout = 5,
    Cancelled = 6,
    InvalidResponse = 7,
    ResponseTooLarge = 8,
    InvalidRequest = 9,
}

public sealed record ReadViewFailure(
    ReadViewFailureCategory Category,
    string Code,
    string SafeMessage,
    bool IsRetryable);

public sealed record ReadViewLimitation(
    string Code,
    string SafeMessage);

public sealed record ReadViewOperationContext
{
    public const int MaxAllowedItems = 10_000;
    public const long MaxAllowedResponseBytes = 16 * 1024 * 1024;

    public ReadViewOperationContext(
        DateTimeOffset deadlineUtc,
        int maxItems = 1_000,
        long maxResponseBytes = 4 * 1024 * 1024)
    {
        if (maxItems <= 0 || maxItems > MaxAllowedItems)
        {
            throw new ArgumentOutOfRangeException(nameof(maxItems));
        }

        if (maxResponseBytes <= 0 || maxResponseBytes > MaxAllowedResponseBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(maxResponseBytes));
        }

        DeadlineUtc = deadlineUtc;
        MaxItems = maxItems;
        MaxResponseBytes = maxResponseBytes;
    }

    public DateTimeOffset DeadlineUtc { get; }
    public int MaxItems { get; }
    public long MaxResponseBytes { get; }
}

public sealed record ReadViewResult<T>
{
    private ReadViewResult(T? value, ReadViewFailure? failure, IReadOnlyList<ReadViewLimitation> limitations)
    {
        Value = value;
        Failure = failure;
        Limitations = limitations;
    }

    public T? Value { get; }
    public ReadViewFailure? Failure { get; }
    public IReadOnlyList<ReadViewLimitation> Limitations { get; }
    public bool IsSuccess => Failure is null;

    public static ReadViewResult<T> Success(T value, IReadOnlyList<ReadViewLimitation>? limitations = null) =>
        new(value, null, limitations ?? Array.Empty<ReadViewLimitation>());

    public static ReadViewResult<T> Failed(ReadViewFailure failure) =>
        new(default, failure ?? throw new ArgumentNullException(nameof(failure)), Array.Empty<ReadViewLimitation>());
}
