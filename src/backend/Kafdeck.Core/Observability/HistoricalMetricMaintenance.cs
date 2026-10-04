namespace Kafdeck.Core.Observability;

public sealed record HistoricalMetricMaintenancePolicy(
    TimeSpan RawRetention,
    TimeSpan RollupRetention,
    int RollupResolutionSeconds,
    int MaxRollupWindowsPerCycle,
    int MaxRollupDeletesPerCycle,
    TimeSpan LeaseDuration,
    TimeSpan CycleInterval,
    TimeSpan MaxCycleDuration)
{
    public const int DefaultRollupResolutionSeconds = 300;
    public const int DefaultMaxRollupWindowsPerCycle = 24;
    public const int HardMaxRollupWindowsPerCycle = 288;
    public const int DefaultMaxRollupDeletesPerCycle = 1_000;
    public const int HardMaxRollupDeletesPerCycle = 10_000;

    public static readonly TimeSpan DefaultLeaseDuration =
        TimeSpan.FromMinutes(2);
    public static readonly TimeSpan DefaultCycleInterval =
        TimeSpan.FromMinutes(1);
    public static readonly TimeSpan DefaultMaxCycleDuration =
        TimeSpan.FromSeconds(30);

    public void Validate()
    {
        if (RawRetention <= TimeSpan.Zero ||
            RawRetention >
            TimeSpan.FromDays(7))
        {
            throw new ArgumentOutOfRangeException(
                nameof(RawRetention));
        }

        if (RollupRetention <= TimeSpan.Zero ||
            RollupRetention >
            TimeSpan.FromDays(90))
        {
            throw new ArgumentOutOfRangeException(
                nameof(RollupRetention));
        }

        if (RollupResolutionSeconds is < 60 or > 86_400 ||
            86_400 % RollupResolutionSeconds != 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(RollupResolutionSeconds));
        }

        if (MaxRollupWindowsPerCycle is < 1 or >
            HardMaxRollupWindowsPerCycle)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxRollupWindowsPerCycle));
        }

        if (MaxRollupDeletesPerCycle is < 1 or >
            HardMaxRollupDeletesPerCycle)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxRollupDeletesPerCycle));
        }

        if (LeaseDuration <= TimeSpan.Zero ||
            LeaseDuration >
            TimeSpan.FromMinutes(10))
        {
            throw new ArgumentOutOfRangeException(
                nameof(LeaseDuration));
        }

        if (CycleInterval <= TimeSpan.Zero ||
            CycleInterval >
            TimeSpan.FromMinutes(10))
        {
            throw new ArgumentOutOfRangeException(
                nameof(CycleInterval));
        }

        if (MaxCycleDuration <= TimeSpan.Zero ||
            MaxCycleDuration >
            TimeSpan.FromMinutes(1))
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxCycleDuration));
        }
    }
}

public sealed record HistoricalMetricMaintenanceLease(
    string OwnerId,
    long FencingToken,
    DateTimeOffset ExpiresAtUtc);

public sealed record HistoricalMetricMaintenanceResult(
    bool LeaseValid,
    int RolledWindows,
    long RawDeleted,
    long RollupDeleted);

public interface IHistoricalMetricMaintenanceStore
{
    Task InitializeAsync(
        CancellationToken cancellationToken = default);

    Task<HistoricalMetricMaintenanceLease?> TryAcquireLeaseAsync(
        string ownerId,
        DateTimeOffset nowUtc,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    Task<HistoricalMetricMaintenanceResult> RunCycleAsync(
        HistoricalMetricMaintenanceLease lease,
        DateTimeOffset nowUtc,
        HistoricalMetricMaintenancePolicy policy,
        CancellationToken cancellationToken = default);
}


public interface IHistoricalMetricSamplingLeaseStore
{
    Task<HistoricalMetricMaintenanceLease?> TryAcquireSamplingLeaseAsync(
        string ownerId,
        DateTimeOffset nowUtc,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);
}
