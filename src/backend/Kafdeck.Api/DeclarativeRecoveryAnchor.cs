using System.Security.Cryptography;
using System.Text.Json;

namespace Kafdeck.Api;

/// <summary>Read-only authority outside restored database state. Only an operator changes it.</summary>
public static class DeclarativeRecoveryAnchor
{
    public static bool Allows(DeclarativeRuntimeOptions options, bool maintenance = false)
    {
        try
        {
            if (options.DeploymentGeneration == Guid.Empty || !Path.IsPathFullyQualified(options.RecoveryAnchorFile) ||
                !DeclarativeRuntimeAuthorizer.Hash(options.RecoveryAnchorSha256)) return false;
            using var file = new FileStream(options.RecoveryAnchorFile, FileMode.Open, FileAccess.Read, FileShare.Read);
            if (file.Length is < 32 or > 4096) return false;
            var bytes = new byte[(int)file.Length]; file.ReadExactly(bytes);
            if (file.ReadByte() != -1 || !CryptographicOperations.FixedTimeEquals(SHA256.HashData(bytes),
                Convert.FromHexString(options.RecoveryAnchorSha256))) return false;
            using var document = JsonDocument.Parse(bytes, new JsonDocumentOptions { MaxDepth = 2 });
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return false;
            var names = root.EnumerateObject().Select(property => property.Name).ToArray();
            return names.Length == 4 && names.Distinct(StringComparer.Ordinal).Count() == 4 &&
                names.Order(StringComparer.Ordinal).SequenceEqual(new[] { "deploymentId", "generation", "state", "version" }) &&
                root.GetProperty("version").GetInt32() == 1 && root.GetProperty("deploymentId").GetString() == options.DeploymentId &&
                root.GetProperty("generation").GetString() == options.DeploymentGeneration.ToString("D") &&
                root.GetProperty("state").GetString() == (maintenance ? "quarantined" : "active");
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or JsonException or
            ArgumentException or InvalidOperationException or NotSupportedException or FormatException)
        { return false; }
    }
}
