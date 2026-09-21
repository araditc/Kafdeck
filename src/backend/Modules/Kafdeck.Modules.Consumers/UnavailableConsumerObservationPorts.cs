using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;

namespace Kafdeck.Modules.Consumers;

public sealed class UnavailableMetricsObservationPort : IMetricsObservationPort
{
    public Task<ReadViewResult<ConsumerRateObservation>> GetConsumerRateAsync(
        string clusterId,
        string groupId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        Task.FromResult(ReadViewResult<ConsumerRateObservation>.Failed(
            new ReadViewFailure(
                ReadViewFailureCategory.NotConfigured,
                "consumer_metrics_provider_not_configured",
                "Consumer rate metrics provider is not configured.",
                false)));
}

public sealed class UnavailableHistoryObservationPort : IHistoryObservationPort
{
    public Task<ReadViewResult<IReadOnlyList<ConsumerHistoryObservation>>> GetConsumerHistoryAsync(
        string clusterId,
        string groupId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        Task.FromResult(ReadViewResult<IReadOnlyList<ConsumerHistoryObservation>>.Failed(
            new ReadViewFailure(
                ReadViewFailureCategory.NotConfigured,
                "consumer_history_provider_not_configured",
                "Consumer history provider is not configured.",
                false)));
}
