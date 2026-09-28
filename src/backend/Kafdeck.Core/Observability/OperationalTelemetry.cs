namespace Kafdeck.Core.Observability;

public enum KafdeckOperationalKind
{
    Provider = 1,
    GovernedOperation = 2,
    Worker = 3,
}

public enum KafdeckOperationalFamily
{
    KafkaMetadataRead = 1,
    KafkaRecordRead = 2,
    ConsumerGroupRead = 3,
    SchemaRegistryRead = 4,
    KafkaConnectRead = 5,
    KsqlMetadataRead = 6,
    KsqlQuery = 7,
    StreamsTelemetryRead = 8,
    MutationExecution = 9,
    GovernedDataJobWorker = 10,
    DataGeneratorWorker = 11,
}

public enum KafdeckOperationalOutcome
{
    Success = 1,
    Denied = 2,
    Unsupported = 3,
    Unavailable = 4,
    Timeout = 5,
    Cancelled = 6,
    Invalid = 7,
    Failed = 8,
    UnknownExternalEffect = 9,
    Blocked = 10,
}

public interface IKafdeckOperationalTelemetryScope : IDisposable
{
    void Complete(KafdeckOperationalOutcome outcome);
}

public interface IKafdeckOperationalTelemetry
{
    IKafdeckOperationalTelemetryScope Start(
        KafdeckOperationalKind kind,
        KafdeckOperationalFamily family);
}

public sealed class NullKafdeckOperationalTelemetry
    : IKafdeckOperationalTelemetry
{
    private sealed class Scope : IKafdeckOperationalTelemetryScope
    {
        public static Scope Instance { get; } = new();

        public void Complete(KafdeckOperationalOutcome outcome)
        {
            _ = outcome;
        }

        public void Dispose()
        {
        }
    }

    public static NullKafdeckOperationalTelemetry Instance { get; } =
        new();

    private NullKafdeckOperationalTelemetry()
    {
    }

    public IKafdeckOperationalTelemetryScope Start(
        KafdeckOperationalKind kind,
        KafdeckOperationalFamily family)
    {
        _ = kind;
        _ = family;
        return Scope.Instance;
    }
}
