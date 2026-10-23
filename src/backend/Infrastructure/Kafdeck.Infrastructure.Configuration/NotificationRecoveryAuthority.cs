using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Microsoft.Extensions.Configuration;

namespace Kafdeck.Infrastructure.Configuration;

public sealed record NotificationRecoveryOptions(Guid TargetGeneration, string AnchorPath, string AnchorSha256)
{
    public static NotificationRecoveryOptions? Read(IConfiguration configuration, KafdeckOptions options, bool productEventsEnabled)
    {
        var section = configuration.GetSection("Kafdeck:Notifications:Recovery");
        if (!section.GetChildren().Any()) return null;
        if (section.Value is not null || section.GetChildren().Any(c => c.Key is not ("Enabled" or "TargetGeneration")) ||
            section["Enabled"] is not { } enabledValue || !bool.TryParse(enabledValue, out var enabled))
            throw Refused();
        if (!enabled) return null;
        var n = options.Notifications;
        if (n is null || !n.Enabled || !n.ManagementEnabled || n.WebhookDeliveryEnabled || n.HttpProviderDeliveryEnabled ||
            n.EmailDeliveryEnabled || productEventsEnabled || options.Administration?.Mutations.Enabled == true ||
            options.Deployment.Mode != AccessMode.Oidc ||
            !Guid.TryParseExact(section["TargetGeneration"], "D", out var generation) || generation == Guid.Empty ||
            n.DeliveryInterlockPath is null || n.DeliveryInterlockSha256 is null) throw Refused();
        ConfiguredNotificationDeliveryInterlock.ValidateOptions(n.DeliveryInterlockPath, n.DeliveryInterlockSha256);
        return new(generation, n.DeliveryInterlockPath, n.DeliveryInterlockSha256.ToLowerInvariant());
    }
    private static KafdeckConfigurationException Refused() => new("Notification recovery requires explicit OIDC maintenance admission, a pinned outside authority and disabled effects and product source routing.");
}

/// <summary>Read-only outside authority. Neither the database nor an HTTP caller chooses its pinned identity.</summary>
public sealed class NotificationRecoveryAuthority(NotificationRecoveryOptions options) : INotificationRecoveryAuthority
{
    public async ValueTask VerifyQuarantinedAsync(Guid generation, string anchorSha256, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(3));
        try
        {
            if (generation != options.TargetGeneration || anchorSha256 != options.AnchorSha256) throw Refused();
            await using var file = new FileStream(options.AnchorPath, FileMode.Open, FileAccess.Read,
                FileShare.Read | FileShare.Delete, 1024, FileOptions.Asynchronous);
            var buffer = new byte[1025];
            var length = 0;
            while (length < buffer.Length)
            {
                var read = await file.ReadAsync(buffer.AsMemory(length), deadline.Token).ConfigureAwait(false);
                if (read == 0) break;
                length += read;
            }
            if (length is < 1 or > 1024 || !CryptographicOperations.FixedTimeEquals(SHA256.HashData(buffer.AsSpan(0, length)),
                Convert.FromHexString(options.AnchorSha256))) throw Refused();
            using var document = JsonDocument.Parse(buffer.AsMemory(0, length), new JsonDocumentOptions { MaxDepth = 2 });
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object) throw Refused();
            var fields = root.EnumerateObject().ToArray();
            if (fields.Length != 3 || fields.Select(p => p.Name).Distinct(StringComparer.Ordinal).Count() != 3 ||
                fields.Any(p => p.Name is not ("version" or "generation" or "state")) ||
                root.GetProperty("version").ValueKind != JsonValueKind.Number || !root.GetProperty("version").TryGetInt32(out var version) || version != 1 ||
                root.GetProperty("generation").ValueKind != JsonValueKind.String ||
                root.GetProperty("generation").GetString() != generation.ToString("D") ||
                root.GetProperty("state").ValueKind != JsonValueKind.String || root.GetProperty("state").GetString() != "quarantined") throw Refused();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or JsonException or InvalidOperationException or
            ArgumentException or OperationCanceledException or KafdeckConfigurationException) { throw Refused(); }
    }
    private static KafdeckConfigurationException Refused() => new("Notification recovery outside authority refused maintenance; exact quarantined generation and fingerprint are required.");
}
