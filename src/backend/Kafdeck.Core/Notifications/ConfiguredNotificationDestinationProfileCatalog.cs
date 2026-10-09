using System.Collections.Frozen;

namespace Kafdeck.Core.Notifications;

/// <summary>
/// Immutable, deployment-owned destination catalog. A catalog is a pure
/// lookup boundary, not a registration API or a notifier activation switch.
/// Provider secrets remain represented only by opaque credential binding IDs.
/// </summary>
public sealed class ConfiguredNotificationDestinationProfileCatalog :
    INotificationDestinationProfileCatalog
{
    public const int HardMaxProfiles = 500;

    private readonly FrozenDictionary<string, NotificationDestinationProfile>
        _profiles;

    public ConfiguredNotificationDestinationProfileCatalog(
        IEnumerable<NotificationDestinationProfile> profiles)
    {
        ArgumentNullException.ThrowIfNull(profiles);

        var snapshot = new Dictionary<string, NotificationDestinationProfile>(
            StringComparer.Ordinal);
        foreach (var profile in profiles)
        {
            if (profile is null)
                throw new ArgumentException(
                    "Notification destination catalog cannot contain null entries.",
                    nameof(profiles));

            // Reject rather than silently shadow a deployment-owned identity.
            // Never let a later entry change a credential or recipient binding.
            if (snapshot.Count >= HardMaxProfiles)
                throw new ArgumentException(
                    "Notification destination catalog exceeds its hard cap.",
                    nameof(profiles));
            if (!snapshot.TryAdd(profile.DestinationId, profile))
                throw new ArgumentException(
                    "Duplicate exact notification destination identity.",
                    nameof(profiles));
        }

        _profiles = snapshot.ToFrozenDictionary(StringComparer.Ordinal);
    }

    public ValueTask<NotificationDestinationProfile?> GetAsync(
        string destinationId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        NotificationDeliveryIdentity.NormalizeDestinationId(destinationId);

        _profiles.TryGetValue(destinationId, out var profile);
        return ValueTask.FromResult(profile);
    }
}
