using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>Refusal-only operator anchor. Does not detect undeclared rollback,
/// enroll stores, reconcile effects or approve resumption.</summary>
public sealed class ConfiguredNotificationDeliveryInterlock(string path, string expectedSha256,
    INotificationDeliveryGenerationReader generations) : INotificationDeliveryActivationGate
{
    public static void ValidateOptions(string? path, string? fingerprint)
    {
        if (path is null && fingerprint is null) return;
        if (string.IsNullOrWhiteSpace(path) || !Path.IsPathFullyQualified(path) ||
            fingerprint is null || fingerprint.Length != 64 || fingerprint.Any(ch => !Uri.IsHexDigit(ch))) throw Refused();
    }

    public async ValueTask VerifyAsync(CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(3));
        try
        {
            ValidateOptions(path, expectedSha256);
            await using var file = new FileStream(path, FileMode.Open, FileAccess.Read,
                FileShare.Read | FileShare.Delete, 1024, FileOptions.Asynchronous);
            var buffer = new byte[1025];
            var length = 0;
            while (length < buffer.Length)
            {
                var read = await file.ReadAsync(buffer.AsMemory(length), deadline.Token).ConfigureAwait(false);
                if (read == 0) break;
                length += read;
            }
            if (length is < 1 or > 1024 || !CryptographicOperations.FixedTimeEquals(
                SHA256.HashData(buffer.AsSpan(0, length)), Convert.FromHexString(expectedSha256))) throw Refused();
            using var document = JsonDocument.Parse(buffer.AsMemory(0, length), new JsonDocumentOptions { MaxDepth = 2 });
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object) throw Refused();
            var properties = root.EnumerateObject().ToArray();
            if (properties.Length != 3 || properties.Select(p => p.Name).Distinct(StringComparer.Ordinal).Count() != 3 ||
                properties.Any(p => p.Name is not ("version" or "generation" or "state")) ||
                root.GetProperty("version").ValueKind != JsonValueKind.Number ||
                !root.GetProperty("version").TryGetInt32(out var version) || version != 1 ||
                root.GetProperty("generation").ValueKind != JsonValueKind.String ||
                !Guid.TryParseExact(root.GetProperty("generation").GetString(), "D", out var generation) || generation == Guid.Empty ||
                root.GetProperty("state").ValueKind != JsonValueKind.String || root.GetProperty("state").GetString() != "active") throw Refused();
            if (await generations.ReadAsync(deadline.Token).ConfigureAwait(false) != generation) throw Refused();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or JsonException or
            InvalidOperationException or ArgumentException or OperationCanceledException or KafdeckConfigurationException)
        { throw Refused(); }
    }

    private static KafdeckConfigurationException Refused() =>
        new("Notification delivery operator interlock refused activation; offline reconciliation and generation review are required.");
}
