using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

public interface IDynamicConfigurationPort
{
    Task<KafkaResult<DynamicConfigurationObservation>> DescribeAsync(
        DynamicConfigurationTarget target,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> AlterAsync(
        DynamicConfigurationMutation mutation,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// W44 keeps the accepted quota contract explicit even though the currently
/// pinned Confluent.Kafka client has no typed ClientQuotas API. This capability
/// boundary is deliberately unavailable rather than tunneled through CLI,
/// protocol, reflection or provider REST.
/// </summary>
public interface IQuotaAdministrationPort
{
    bool IsTypedProviderCapabilityAvailable { get; }
}

public sealed class PinnedClientUnavailableQuotaAdministrationPort :
    IQuotaAdministrationPort
{
    public bool IsTypedProviderCapabilityAvailable => false;
}
