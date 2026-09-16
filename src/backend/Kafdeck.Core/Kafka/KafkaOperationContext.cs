namespace Kafdeck.Core.Kafka;

public readonly record struct KafkaOperationContext(DateTimeOffset DeadlineUtc)
{
    public bool IsExpired(DateTimeOffset nowUtc) => nowUtc >= DeadlineUtc;

    public TimeSpan Remaining(DateTimeOffset nowUtc) =>
        IsExpired(nowUtc) ? TimeSpan.Zero : DeadlineUtc - nowUtc;
}
