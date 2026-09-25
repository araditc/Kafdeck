using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

/// <summary>
/// Typed read-only ACL observation boundary used by W42 planning, access
/// analysis and pre-dispatch readback. It exposes no generic AdminClient
/// escape hatch and returns only normalized ACL metadata.
/// </summary>
public interface IAclObservationPort
{
    Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> DescribeAsync(
        string clusterId,
        KafkaAclBindingFilter filter,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default);
}
