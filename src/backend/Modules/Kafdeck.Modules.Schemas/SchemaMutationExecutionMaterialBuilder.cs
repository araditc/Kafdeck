using System.Security.Cryptography;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Schemas;

/// <summary>
/// Rebuilds schema-create execution material from the source resubmitted by the
/// requester after preview. The W32 executor still performs the authoritative
/// HMAC material-digest comparison before any provider dispatch.
/// </summary>
public static class SchemaMutationExecutionMaterialBuilder
{
    public static MutationExecutionMaterial BuildCreate(
        MutationOperationSnapshot operation,
        string schema)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);

        if (operation.OperationKind != MutationOperationKind.SchemaCreate)
        {
            throw Invalid();
        }

        SchemaCreateCanonicalIntent canonical;
        try
        {
            canonical = SchemaMutationCanonicalization.Deserialize<
                SchemaCreateCanonicalIntent>(operation.CanonicalIntent);
        }
        catch
        {
            throw Invalid();
        }

        byte[] bytes;
        try
        {
            bytes = SchemaMutationCanonicalization.EncodeSchema(
                schema,
                SchemaMutationPolicy.HardMaxSchemaBytes);
        }
        catch (ArgumentException)
        {
            throw Invalid();
        }

        try
        {
            if (bytes.Length != canonical.SchemaBytes ||
                !string.Equals(
                    SchemaMutationCanonicalization.Sha256(bytes),
                    canonical.SchemaSha256,
                    StringComparison.Ordinal) ||
                operation.MaterialDigests.Count != 1 ||
                !string.Equals(
                    operation.MaterialDigests[0].Name,
                    canonical.MaterialName,
                    StringComparison.Ordinal))
            {
                throw Invalid();
            }

            return new MutationExecutionMaterial(
                new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal)
                {
                    [canonical.MaterialName] = bytes,
                });
        }
        finally
        {
            CryptographicOperations.ZeroMemory(bytes);
        }
    }

    private static MutationStateException Invalid() =>
        new("Schema execution material does not match the admitted preview.");
}
