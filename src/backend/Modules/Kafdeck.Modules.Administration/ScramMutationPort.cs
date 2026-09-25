using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

public sealed record ScramUpsertMutation(
    string ClusterId,
    string User,
    KafkaScramMechanism Mechanism,
    int Iterations);

public sealed record ScramDeleteMutation(
    string ClusterId,
    string User,
    KafkaScramMechanism Mechanism);

/// <summary>
/// Exact typed SCRAM write boundary. Password material is a separate
/// request-scoped memory argument and is never part of the safe mutation
/// request record.
/// </summary>
public interface IScramMutationPort
{
    Task<MutationProviderResult> UpsertAsync(
        ScramUpsertMutation request,
        ReadOnlyMemory<byte> password,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> DeleteAsync(
        ScramDeleteMutation request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default);
}
