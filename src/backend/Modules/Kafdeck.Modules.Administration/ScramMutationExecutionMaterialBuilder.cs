using System.Security.Cryptography;

namespace Kafdeck.Modules.Administration;

/// <summary>
/// Creates one request-scoped SCRAM execution envelope after independent
/// approval. The returned material is consumed only by the common mutation
/// executor, which verifies the durable HMAC digest before handler dispatch.
/// </summary>
public static class ScramMutationExecutionMaterialBuilder
{
    public static MutationExecutionMaterial BuildUpsert(
        MutationOperationSnapshot operation,
        ReadOnlyMemory<byte> password)
    {
        var plan = ScramMutationContract.ValidateBoundOperation(
            operation,
            requireReadyForFinalization: true);
        if (plan.Mode != ScramMutationMode.Upsert)
        {
            throw new MutationStateException(
                "SCRAM upsert finalization cannot execute a delete plan.");
        }

        var context = ScramMutationContract.BuildUpsertMaterialContext(
            operation,
            plan);
        var envelope = ScramCredentialMaterialCodec.Encode(
            context,
            password.Span);
        try
        {
            return new MutationExecutionMaterial(
                new Dictionary<string, ReadOnlyMemory<byte>>(
                    StringComparer.Ordinal)
                {
                    [ScramMutationPlanner.MaterialName] = envelope,
                });
        }
        finally
        {
            CryptographicOperations.ZeroMemory(envelope);
        }
    }
}
