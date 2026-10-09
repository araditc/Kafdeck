using System.Collections.Frozen;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>
/// Closed set of owner-provisioned credential references. Resolves only an
/// exact destination/provider/profile-revision/binding tuple, with no
/// caller-chosen environment variable, path, URL or fallback lookup.
/// Does not schedule deliveries or issue network requests.
/// </summary>
public sealed record ConfiguredNotificationCredentialBinding(
    string DestinationId,
    NotificationProviderKind Provider,
    string ProfileRevisionFingerprint,
    NotificationCredentialBindingId BindingId,
    SecretReference Secret);

public sealed class ConfiguredNotificationCredentialResolver :
    INotificationCredentialResolver
{
    private readonly FrozenDictionary<string, ConfiguredNotificationCredentialBinding>
        _bindings;
    private readonly SecretResolver _secrets;

    public ConfiguredNotificationCredentialResolver(
        IEnumerable<ConfiguredNotificationCredentialBinding> bindings,
        SecretResolver secrets)
    {
        ArgumentNullException.ThrowIfNull(bindings);
        _secrets = secrets ?? throw new ArgumentNullException(nameof(secrets));
        var snapshot = new Dictionary<string, ConfiguredNotificationCredentialBinding>(
            StringComparer.Ordinal);
        foreach (var binding in bindings)
        {
            if (binding is null || binding.Secret is null || binding.BindingId is null ||
                !Enum.IsDefined(binding.Provider) ||
                binding.ProfileRevisionFingerprint is null ||
                binding.ProfileRevisionFingerprint.Length != 64 ||
                binding.ProfileRevisionFingerprint.Any(ch => !char.IsAsciiHexDigit(ch)))
                throw new ArgumentException("Invalid configured notification credential binding.", nameof(bindings));
            NotificationDeliveryIdentity.NormalizeDestinationId(binding.DestinationId);
            if (snapshot.Count >= 500)
                throw new ArgumentException("Notification credentials exceed the finite cap.", nameof(bindings));
            if (!snapshot.TryAdd(binding.DestinationId, binding))
                throw new ArgumentException("Duplicate notification credential destination identity.", nameof(bindings));
        }
        _bindings = snapshot.ToFrozenDictionary(StringComparer.Ordinal);
    }

    public ValueTask<NotificationCredentialValue> ResolveAsync(
        NotificationCredentialResolutionRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        if (!_bindings.TryGetValue(request.DestinationId, out var binding) ||
            binding.Provider != request.Provider ||
            !string.Equals(binding.BindingId.Value, request.BindingId.Value, StringComparison.Ordinal) ||
            !string.Equals(binding.ProfileRevisionFingerprint,
                request.ProfileRevisionFingerprint, StringComparison.Ordinal))
            throw new KafdeckConfigurationException(
                "Notification credential binding does not match the approved destination revision.");

        var value = _secrets.Resolve(binding.Secret);
        return ValueTask.FromResult(new NotificationCredentialValue(value.Reveal()));
    }
}
