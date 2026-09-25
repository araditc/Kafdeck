using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

/// <summary>
/// Metadata-only SCRAM observation boundary. It cannot retrieve password,
/// salt, verifier or any other credential material.
/// </summary>
public interface IScramObservationPort
{
    Task<KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>> DescribeUserAsync(
        string clusterId,
        string user,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default);
}
