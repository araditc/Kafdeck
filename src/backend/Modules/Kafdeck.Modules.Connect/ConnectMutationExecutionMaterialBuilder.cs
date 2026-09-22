using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Connect;

/// <summary>
/// Rebuilds connector configuration execution material from request-scoped
/// values re-submitted after preview. Canonical preview contains only the
/// admitted safe projection/fingerprints; W32 independently validates the
/// persisted HMAC material digest before provider dispatch.
/// </summary>
public static class ConnectMutationExecutionMaterialBuilder
{
    public static MutationExecutionMaterial BuildConfiguration(
        MutationOperationSnapshot operation,
        IReadOnlyDictionary<string, string> configuration)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(configuration);

        var normalized = ConnectMutationCanonicalization.NormalizeConfiguration(
            configuration,
            ConnectMutationPolicy.Default);
        var projected = ConnectMutationCanonicalization.ProjectConfiguration(
            normalized);
        var fingerprint = ConnectMutationCanonicalization.ConfigurationFingerprint(
            projected);

        string materialName;
        string expectedFingerprint;
        IReadOnlyList<ConnectConfigurationCanonicalItem> expectedProjection;

        switch (operation.OperationKind)
        {
            case MutationOperationKind.ConnectCreate:
            {
                var canonical = DeserializeCreate(operation.CanonicalIntent);
                materialName = canonical.MaterialName;
                expectedFingerprint = canonical.RequestedConfigurationFingerprint;
                expectedProjection = canonical.RequestedConfiguration;
                break;
            }

            case MutationOperationKind.ConnectAlter:
            {
                var canonical = DeserializeUpdate(operation.CanonicalIntent);
                if (canonical.AlterKind != ConnectAlterIntentKind.ConfigurationUpdate)
                {
                    throw Invalid();
                }

                materialName = canonical.MaterialName;
                expectedFingerprint = canonical.RequestedConfigurationFingerprint;
                expectedProjection = canonical.RequestedConfiguration;
                break;
            }

            default:
                throw Invalid();
        }

        if (!string.Equals(fingerprint, expectedFingerprint, StringComparison.Ordinal) ||
            projected.Count != expectedProjection.Count ||
            !projected.SequenceEqual(expectedProjection) ||
            operation.MaterialDigests.Count != 1 ||
            !string.Equals(
                operation.MaterialDigests[0].Name,
                materialName,
                StringComparison.Ordinal))
        {
            throw Invalid();
        }

        var encoded = ConnectMutationCanonicalization.EncodeConfiguration(normalized);
        try
        {
            return new MutationExecutionMaterial(
                new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal)
                {
                    [materialName] = encoded,
                });
        }
        finally
        {
            CryptographicOperations.ZeroMemory(encoded);
        }
    }

    public static bool IsNoMaterialExecution(
        MutationOperationSnapshot operation)
    {
        ArgumentNullException.ThrowIfNull(operation);

        if (operation.OperationKind == MutationOperationKind.ConnectDelete)
        {
            return true;
        }

        if (operation.OperationKind != MutationOperationKind.ConnectAlter)
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(operation.CanonicalIntent);
            return document.RootElement.ValueKind == JsonValueKind.Object &&
                   document.RootElement.TryGetProperty("alterKind", out var element) &&
                   element.TryGetInt32(out var raw) &&
                   raw == (int)ConnectAlterIntentKind.Control;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static ConnectCreateCanonicalIntent DeserializeCreate(string value)
    {
        try
        {
            return ConnectMutationCanonicalization.Deserialize<ConnectCreateCanonicalIntent>(value);
        }
        catch
        {
            throw Invalid();
        }
    }

    private static ConnectUpdateCanonicalIntent DeserializeUpdate(string value)
    {
        try
        {
            return ConnectMutationCanonicalization.Deserialize<ConnectUpdateCanonicalIntent>(value);
        }
        catch
        {
            throw Invalid();
        }
    }

    private static MutationStateException Invalid() =>
        new("Kafka Connect execution material does not match the admitted preview.");
}
