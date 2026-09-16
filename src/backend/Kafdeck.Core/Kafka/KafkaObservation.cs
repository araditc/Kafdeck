namespace Kafdeck.Core.Kafka;

public enum ObservationSource
{
    Live = 1,
    Snapshot = 2,
    StaleSnapshot = 3,
}

public sealed record ObservationMetadata
{
    public ObservationMetadata(
        DateTimeOffset observedAtUtc,
        DateTimeOffset freshUntilUtc,
        DateTimeOffset staleAfterUtc,
        ObservationSource source)
    {
        if (freshUntilUtc < observedAtUtc)
        {
            throw new ArgumentOutOfRangeException(nameof(freshUntilUtc), "Freshness cannot end before the observation time.");
        }

        if (staleAfterUtc < freshUntilUtc)
        {
            throw new ArgumentOutOfRangeException(nameof(staleAfterUtc), "Stale cutoff cannot precede the freshness cutoff.");
        }

        ObservedAtUtc = observedAtUtc;
        FreshUntilUtc = freshUntilUtc;
        StaleAfterUtc = staleAfterUtc;
        Source = source;
    }

    public DateTimeOffset ObservedAtUtc { get; }

    public DateTimeOffset FreshUntilUtc { get; }

    public DateTimeOffset StaleAfterUtc { get; }

    public ObservationSource Source { get; }

    public bool IsFreshAt(DateTimeOffset nowUtc) => nowUtc <= FreshUntilUtc;

    public bool IsStaleButUsableAt(DateTimeOffset nowUtc) =>
        nowUtc > FreshUntilUtc && nowUtc <= StaleAfterUtc;
}

public sealed record KafkaResult<T>
{
    private KafkaResult(T? value, KafkaFailure? failure, ObservationMetadata observation)
    {
        Value = value;
        Failure = failure;
        Observation = observation;
    }

    public T? Value { get; }

    public KafkaFailure? Failure { get; }

    public ObservationMetadata Observation { get; }

    public bool IsSuccess => Failure is null;

    public static KafkaResult<T> Success(T value, ObservationMetadata observation) =>
        new(value, null, observation);

    public static KafkaResult<T> Failed(KafkaFailure failure, ObservationMetadata observation) =>
        new(default, failure ?? throw new ArgumentNullException(nameof(failure)), observation);
}
