namespace Kafdeck.Core.Kafka;

/// <summary>
/// Bounded runtime policy for read-only Kafka observations. Values are deliberately
/// conservative defaults and may only be overridden within the validated bounds.
/// </summary>
public sealed record KafkaSnapshotPolicy
{
    public const int DefaultPerClusterConcurrency = 8;
    public const int DefaultGlobalConcurrency = 64;
    public static readonly TimeSpan DefaultOperationDeadline = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan DefaultClusterMetadataTtl = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan DefaultTopicMetadataTtl = TimeSpan.FromSeconds(10);

    public KafkaSnapshotPolicy(
        int perClusterConcurrency = DefaultPerClusterConcurrency,
        int globalConcurrency = DefaultGlobalConcurrency,
        TimeSpan? operationDeadline = null,
        TimeSpan? clusterMetadataTtl = null,
        TimeSpan? topicMetadataTtl = null)
    {
        if (perClusterConcurrency is < 1 or > 64)
            throw new ArgumentOutOfRangeException(nameof(perClusterConcurrency));
        if (globalConcurrency is < 1 or > 512)
            throw new ArgumentOutOfRangeException(nameof(globalConcurrency));
        if (globalConcurrency < perClusterConcurrency)
            throw new ArgumentOutOfRangeException(nameof(globalConcurrency), "Global concurrency cannot be lower than the per-cluster limit.");

        PerClusterConcurrency = perClusterConcurrency;
        GlobalConcurrency = globalConcurrency;
        OperationDeadline = ValidateDuration(operationDeadline ?? DefaultOperationDeadline, nameof(operationDeadline), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(60));
        ClusterMetadataTtl = ValidateDuration(clusterMetadataTtl ?? DefaultClusterMetadataTtl, nameof(clusterMetadataTtl), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1));
        TopicMetadataTtl = ValidateDuration(topicMetadataTtl ?? DefaultTopicMetadataTtl, nameof(topicMetadataTtl), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5));
    }

    public int PerClusterConcurrency { get; }
    public int GlobalConcurrency { get; }
    public TimeSpan OperationDeadline { get; }
    public TimeSpan ClusterMetadataTtl { get; }
    public TimeSpan TopicMetadataTtl { get; }

    private static TimeSpan ValidateDuration(TimeSpan value, string parameterName, TimeSpan minimum, TimeSpan maximum)
    {
        if (value < minimum || value > maximum)
            throw new ArgumentOutOfRangeException(parameterName, $"Value must be between {minimum} and {maximum}.");
        return value;
    }
}
