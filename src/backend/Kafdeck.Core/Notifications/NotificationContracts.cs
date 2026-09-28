using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace Kafdeck.Core.Notifications;

public enum NotificationProviderKind
{
    Webhook = 1,
    Email = 2,
    Slack = 3,
    MicrosoftTeams = 4,
    Telegram = 5,
    PagerDuty = 6,
}

public enum NotificationEventClass
{
    Operational = 1,
    Mutation = 2,
    Security = 3,
    Slo = 4,
    DataQuality = 5,
}

public sealed class NotificationCredentialBindingId
{
    public const int MaxLength = 128;

    public NotificationCredentialBindingId(
        string value)
    {
        ArgumentNullException.ThrowIfNull(
            value);

        if (value.Length is < 1 or > MaxLength ||
            !string.Equals(
                value,
                value.Trim(),
                StringComparison.Ordinal) ||
            value.Any(char.IsControl) ||
            value.Any(character =>
                !(char.IsAsciiLetterOrDigit(character) ||
                  character is '.' or '_' or '-')))
        {
            throw new ArgumentException(
                "Notification credential binding ID must be a bounded opaque ASCII identifier.",
                nameof(value));
        }

        Value = value;
    }

    public string Value { get; }

    public override string ToString() =>
        Value;
}

public sealed class NotificationCredentialValue
{
    private readonly string _value;

    public NotificationCredentialValue(
        string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            value);
        _value = value;
    }

    public string Reveal() =>
        _value;

    public override string ToString() =>
        "[redacted-notification-credential]";
}

public sealed record NotificationCredentialResolutionRequest
{
    public NotificationCredentialResolutionRequest(
        NotificationDestinationProfile profile)
    {
        ArgumentNullException.ThrowIfNull(
            profile);

        if (profile.CredentialBindingId is null)
        {
            throw new ArgumentException(
                "Notification destination does not have a credential binding.",
                nameof(profile));
        }

        BindingId =
            profile.CredentialBindingId;
        DestinationId =
            profile.DestinationId;
        Provider =
            profile.Provider;
        ProfileRevisionFingerprint =
            profile.RevisionFingerprint;
    }

    public NotificationCredentialBindingId BindingId { get; }

    public string DestinationId { get; }

    public NotificationProviderKind Provider { get; }

    public string ProfileRevisionFingerprint { get; }
}

public interface INotificationCredentialResolver
{
    ValueTask<NotificationCredentialValue> ResolveAsync(
        NotificationCredentialResolutionRequest request,
        CancellationToken cancellationToken);
}

public sealed record NotificationDestinationProfile
{
    public const int MaxDestinationIdLength = 128;
    public const int MaxDisplayNameLength = 256;

    internal static string NormalizeDestinationId(
        string destinationId)
    {
        ArgumentNullException.ThrowIfNull(
            destinationId);

        if (destinationId.Length is < 1 or >
            MaxDestinationIdLength)
        {
            throw new ArgumentException(
                "Notification destination ID must be a bounded ASCII identifier.",
                nameof(destinationId));
        }

        if (!string.Equals(
                destinationId,
                destinationId.Trim(),
                StringComparison.Ordinal) ||
            destinationId.Any(char.IsControl) ||
            destinationId.Any(character =>
                !(char.IsAsciiLetterOrDigit(character) ||
                  character is '.' or '_' or '-')))
        {
            throw new ArgumentException(
                "Notification destination ID must be a bounded ASCII identifier.",
                nameof(destinationId));
        }

        return destinationId;
    }

    public NotificationDestinationProfile(
        string destinationId,
        NotificationProviderKind provider,
        string displayName,
        IReadOnlyList<NotificationEventClass> enabledEvents,
        Uri? configuredEndpoint = null,
        NotificationCredentialBindingId? credentialBindingId = null)
    {
        var normalizedId =
            NormalizeDestinationId(
                destinationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(displayName);
        ArgumentNullException.ThrowIfNull(enabledEvents);

        if (!Enum.IsDefined(provider))
        {
            throw new ArgumentOutOfRangeException(nameof(provider));
        }

        var normalizedDisplayName = displayName.Trim();
        if (normalizedDisplayName.Length > MaxDisplayNameLength ||
            normalizedDisplayName.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Notification display name is invalid.",
                nameof(displayName));
        }

        var events = enabledEvents
            .Distinct()
            .ToArray();
        if (events.Length is < 1 or > 16 ||
            events.Length != enabledEvents.Count ||
            events.Any(value => !Enum.IsDefined(value)))
        {
            throw new ArgumentException(
                "Notification event classes must be unique and bounded.",
                nameof(enabledEvents));
        }

        Uri? normalizedEndpoint = null;
        if (configuredEndpoint is not null)
        {
            normalizedEndpoint =
                NotificationHttpsEndpointPolicy.ValidateAndNormalize(
                    configuredEndpoint);
        }

        if (provider == NotificationProviderKind.Webhook &&
            normalizedEndpoint is null)
        {
            throw new ArgumentException(
                "Webhook destinations require one configured HTTPS endpoint.",
                nameof(configuredEndpoint));
        }

        DestinationId = normalizedId;
        Provider = provider;
        DisplayName = normalizedDisplayName;
        EnabledEvents = Array.AsReadOnly(events);
        ConfiguredEndpoint = normalizedEndpoint;
        CredentialBindingId = credentialBindingId;
        RevisionFingerprint =
            ComputeRevisionFingerprint(
                DestinationId,
                Provider,
                ConfiguredEndpoint,
                CredentialBindingId,
                EnabledEvents);
    }

    public string DestinationId { get; }
    public NotificationProviderKind Provider { get; }
    public string DisplayName { get; }
    public IReadOnlyList<NotificationEventClass> EnabledEvents { get; }
    public Uri? ConfiguredEndpoint { get; }
    public NotificationCredentialBindingId? CredentialBindingId { get; }

    public string RevisionFingerprint { get; }

    private static string ComputeRevisionFingerprint(
        string destinationId,
        NotificationProviderKind provider,
        Uri? endpoint,
        NotificationCredentialBindingId? bindingId,
        IReadOnlyList<NotificationEventClass> enabledEvents)
    {
        var canonical =
            string.Join(
                "\n",
                destinationId,
                ((int)provider).ToString(
                    System.Globalization.CultureInfo.InvariantCulture),
                endpoint?.AbsoluteUri ?? string.Empty,
                bindingId?.Value ?? string.Empty,
                string.Join(
                    ",",
                    enabledEvents
                        .OrderBy(value => value)
                        .Select(value =>
                            ((int)value).ToString(
                                System.Globalization.CultureInfo.InvariantCulture))));

        return Convert
            .ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(
                        canonical)))
            .ToLowerInvariant();
    }
}

public sealed record NotificationDeliveryPolicy
{
    public const int DefaultMaxAttempts = 5;
    public const int HardMaxAttempts = 10;
    public const int DefaultPayloadBytes = 32 * 1024;
    public const int HardMaxPayloadBytes = 256 * 1024;
    public const int DefaultRatePerSecond = 5;
    public const int HardMaxRatePerSecond = 50;
    public const int DefaultConcurrency = 2;
    public const int HardMaxConcurrency = 8;

    public static readonly TimeSpan DefaultInitialRetry =
        TimeSpan.FromSeconds(5);
    public static readonly TimeSpan MinimumInitialRetry =
        TimeSpan.FromSeconds(1);
    public static readonly TimeSpan DefaultMaxBackoff =
        TimeSpan.FromMinutes(5);
    public static readonly TimeSpan HardMaxBackoff =
        TimeSpan.FromMinutes(30);
    public static readonly TimeSpan DefaultLifetime =
        TimeSpan.FromHours(1);
    public static readonly TimeSpan HardMaxLifetime =
        TimeSpan.FromHours(24);

    public NotificationDeliveryPolicy(
        int maxAttempts = DefaultMaxAttempts,
        TimeSpan? initialRetry = null,
        TimeSpan? maxBackoff = null,
        TimeSpan? lifetime = null,
        int maxPayloadBytes = DefaultPayloadBytes,
        int ratePerSecond = DefaultRatePerSecond,
        int maxConcurrency = DefaultConcurrency)
    {
        var initial = initialRetry ?? DefaultInitialRetry;
        var maximumBackoff = maxBackoff ?? DefaultMaxBackoff;
        var deliveryLifetime = lifetime ?? DefaultLifetime;

        if (maxAttempts is < 1 or > HardMaxAttempts ||
            initial < MinimumInitialRetry ||
            initial > HardMaxBackoff ||
            maximumBackoff < initial ||
            maximumBackoff > HardMaxBackoff ||
            deliveryLifetime < maximumBackoff ||
            deliveryLifetime > HardMaxLifetime ||
            maxPayloadBytes is < 1 or > HardMaxPayloadBytes ||
            ratePerSecond is < 1 or > HardMaxRatePerSecond ||
            maxConcurrency is < 1 or > HardMaxConcurrency)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxAttempts),
                "Notification delivery policy is outside admitted bounds.");
        }

        MaxAttempts = maxAttempts;
        InitialRetry = initial;
        MaxBackoff = maximumBackoff;
        Lifetime = deliveryLifetime;
        MaxPayloadBytes = maxPayloadBytes;
        RatePerSecond = ratePerSecond;
        MaxConcurrency = maxConcurrency;
    }

    public int MaxAttempts { get; }
    public TimeSpan InitialRetry { get; }
    public TimeSpan MaxBackoff { get; }
    public TimeSpan Lifetime { get; }
    public int MaxPayloadBytes { get; }
    public int RatePerSecond { get; }
    public int MaxConcurrency { get; }
}

public enum NotificationDeliveryState
{
    Pending = 1,
    InFlight = 2,
    Delivered = 3,
    Failed = 4,
    UnknownExternalEffect = 5,
    Exhausted = 6,
}

public sealed record NotificationDeliverySnapshot
{
    private static readonly Regex FingerprintPattern =
        new("^[A-Fa-f0-9]{64}$", RegexOptions.CultureInvariant);

    public NotificationDeliverySnapshot(
        Guid notificationId,
        string destinationId,
        string payloadFingerprint,
        NotificationDeliveryState state,
        int attemptCount,
        DateTimeOffset createdAtUtc,
        DateTimeOffset? nextAttemptAtUtc = null,
        string? outcomeCode = null,
        string? providerRequestId = null)
    {
        if (notificationId == Guid.Empty)
        {
            throw new ArgumentException(
                "Notification ID is required.",
                nameof(notificationId));
        }

        destinationId =
            NotificationDestinationProfile
                .NormalizeDestinationId(
                    destinationId);

        ArgumentException.ThrowIfNullOrWhiteSpace(payloadFingerprint);
        if (!FingerprintPattern.IsMatch(payloadFingerprint))
        {
            throw new ArgumentException(
                "Payload fingerprint must be a SHA-256 hex digest.",
                nameof(payloadFingerprint));
        }

        if (!Enum.IsDefined(state))
        {
            throw new ArgumentOutOfRangeException(nameof(state));
        }

        if (attemptCount is < 0 or >
            NotificationDeliveryPolicy.HardMaxAttempts)
        {
            throw new ArgumentOutOfRangeException(nameof(attemptCount));
        }

        var latestAdmittedAttemptUtc =
            createdAtUtc <=
            DateTimeOffset.MaxValue -
            NotificationDeliveryPolicy.HardMaxLifetime
                ? createdAtUtc +
                  NotificationDeliveryPolicy.HardMaxLifetime
                : DateTimeOffset.MaxValue;

        if (createdAtUtc == default ||
            nextAttemptAtUtc is not null &&
            (nextAttemptAtUtc.Value < createdAtUtc ||
             nextAttemptAtUtc.Value >
             latestAdmittedAttemptUtc))
        {
            throw new ArgumentException(
                "Notification delivery timestamps are invalid or exceed the admitted hard lifetime.");
        }

        NotificationId = notificationId;
        DestinationId = destinationId;
        PayloadFingerprint = payloadFingerprint.ToLowerInvariant();
        State = state;
        AttemptCount = attemptCount;
        CreatedAtUtc = createdAtUtc;
        NextAttemptAtUtc = nextAttemptAtUtc;
        OutcomeCode = ValidateSafeOptional(outcomeCode, 128, nameof(outcomeCode));
        ProviderRequestId = ValidateSafeOptional(providerRequestId, 256, nameof(providerRequestId));
    }

    public Guid NotificationId { get; }
    public string DestinationId { get; }
    public string PayloadFingerprint { get; }
    public NotificationDeliveryState State { get; }
    public int AttemptCount { get; }
    public DateTimeOffset CreatedAtUtc { get; }
    public DateTimeOffset? NextAttemptAtUtc { get; }
    public string? OutcomeCode { get; }
    public string? ProviderRequestId { get; }

    private static string? ValidateSafeOptional(
        string? value,
        int maxLength,
        string parameterName)
    {
        if (value is null)
        {
            return null;
        }

        var normalized = value.Trim();
        if (normalized.Length == 0 ||
            normalized.Length > maxLength ||
            normalized.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Notification delivery metadata is invalid.",
                parameterName);
        }

        return normalized;
    }
}

public enum NotificationNat64DecodeResult
{
    NoMatch = 0,
    Valid = 1,
    Malformed = 2,
}

public sealed record NotificationNat64Prefix
{
    private static readonly HashSet<int> AllowedPrefixLengths =
        [32, 40, 48, 56, 64, 96];

    private readonly byte[] _prefixBytes;

    public NotificationNat64Prefix(
        IPAddress prefix,
        int prefixLength)
    {
        ArgumentNullException.ThrowIfNull(
            prefix);

        if (prefix.AddressFamily !=
                System.Net.Sockets.AddressFamily.InterNetworkV6 ||
            !AllowedPrefixLengths.Contains(
                prefixLength))
        {
            throw new ArgumentException(
                "Configured NAT64 prefixes must be IPv6 RFC6052 prefixes with length /32, /40, /48, /56, /64 or /96.");
        }

        _prefixBytes =
            prefix.GetAddressBytes();

        for (var bit = prefixLength;
             bit < 128;
             bit++)
        {
            if (GetBit(
                    _prefixBytes,
                    bit))
            {
                throw new ArgumentException(
                    "Configured NAT64 prefix host bits must be zero.",
                    nameof(prefix));
            }
        }

        Prefix =
            prefix;
        PrefixLength =
            prefixLength;
    }

    public IPAddress Prefix { get; }

    public int PrefixLength { get; }

    public bool Overlaps(
        NotificationNat64Prefix other)
    {
        ArgumentNullException.ThrowIfNull(
            other);

        var bits =
            Math.Min(
                PrefixLength,
                other.PrefixLength);

        for (var bit = 0;
             bit < bits;
             bit++)
        {
            if (GetBit(
                    _prefixBytes,
                    bit) !=
                GetBit(
                    other._prefixBytes,
                    bit))
            {
                return false;
            }
        }

        return true;
    }

    public NotificationNat64DecodeResult DecodeEmbeddedIpv4(
        IPAddress address,
        out IPAddress? embedded)
    {
        embedded = null;

        if (address.AddressFamily !=
            System.Net.Sockets.AddressFamily.InterNetworkV6)
        {
            return NotificationNat64DecodeResult.NoMatch;
        }

        var bytes =
            address.GetAddressBytes();
        for (var bit = 0;
             bit < PrefixLength;
             bit++)
        {
            if (GetBit(
                    bytes,
                    bit) !=
                GetBit(
                    _prefixBytes,
                    bit))
            {
                return NotificationNat64DecodeResult.NoMatch;
            }
        }

        if (PrefixLength <= 64 &&
            bytes[8] != 0)
        {
            return NotificationNat64DecodeResult.Malformed;
        }

        var ipv4 =
            new byte[4];
        var sourceBit =
            PrefixLength;

        for (var targetBit = 0;
             targetBit < 32;
             targetBit++)
        {
            if (sourceBit == 64)
            {
                sourceBit = 72;
            }

            if (sourceBit >= 128)
            {
                return NotificationNat64DecodeResult.Malformed;
            }

            SetBit(
                ipv4,
                targetBit,
                GetBit(
                    bytes,
                    sourceBit));
            sourceBit++;
        }

        embedded =
            new IPAddress(
                ipv4);
        return NotificationNat64DecodeResult.Valid;
    }

    private static bool GetBit(
        byte[] bytes,
        int bitIndex)
    {
        var byteIndex =
            bitIndex / 8;
        var offset =
            7 -
            bitIndex % 8;
        return (bytes[byteIndex] &
                (1 << offset)) != 0;
    }

    private static void SetBit(
        byte[] bytes,
        int bitIndex,
        bool value)
    {
        if (!value)
        {
            return;
        }

        var byteIndex =
            bitIndex / 8;
        var offset =
            7 -
            bitIndex % 8;
        bytes[byteIndex] |=
            (byte)(1 << offset);
    }
}

public sealed class NotificationAddressPolicy
{
    public const int HardMaxNat64Prefixes = 16;

    private static readonly NotificationNat64Prefix
        StandardWellKnownNat64 =
        new(
            IPAddress.Parse("64:ff9b::"),
            96);

    private static readonly NotificationNat64Prefix
        StandardLocalUseNat64 =
        new(
            IPAddress.Parse("64:ff9b:1::"),
            48);

    private readonly IReadOnlyList<NotificationNat64Prefix>
        _nat64Prefixes;

    public NotificationAddressPolicy(
        IReadOnlyList<NotificationNat64Prefix> nat64Prefixes)
    {
        ArgumentNullException.ThrowIfNull(
            nat64Prefixes);

        if (nat64Prefixes.Count >
            HardMaxNat64Prefixes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(nat64Prefixes));
        }

        var prefixes =
            nat64Prefixes.ToArray();
        if (prefixes.Any(prefix =>
                prefix is null))
        {
            throw new ArgumentException(
                "Configured NAT64 prefixes cannot contain null entries.",
                nameof(nat64Prefixes));
        }

        for (var left = 0;
             left < prefixes.Length;
             left++)
        {
            for (var right = left + 1;
                 right < prefixes.Length;
                 right++)
            {
                if (prefixes[left].Overlaps(
                        prefixes[right]))
                {
                    throw new ArgumentException(
                        "Configured NAT64 prefixes must not overlap; one authoritative decoding must exist for each address.",
                        nameof(nat64Prefixes));
                }
            }
        }

        _nat64Prefixes =
            Array.AsReadOnly(
                prefixes);
        Fingerprint =
            ComputeFingerprint(
                prefixes);
    }

    public string Fingerprint { get; }

    public static NotificationAddressPolicy
        NoConfiguredNat64 { get; } =
        new(
            Array.Empty<NotificationNat64Prefix>());

    public NotificationPinnedEndpoint ValidatePinnedEndpoint(
        Uri configuredEndpoint,
        IReadOnlyList<IPAddress> addresses,
        DateTimeOffset resolvedAtUtc) =>
        new(
            configuredEndpoint,
            addresses,
            resolvedAtUtc,
            this);

    public bool IsProhibited(
        IPAddress address)
    {
        ArgumentNullException.ThrowIfNull(
            address);

        if (NotificationHttpsEndpointPolicy
                .IsAlwaysProhibitedAddress(
                    address))
        {
            return true;
        }

        var configuredMatch =
            false;

        foreach (var implicitPrefix in
                 new[]
                 {
                     StandardWellKnownNat64,
                     StandardLocalUseNat64,
                 })
        {
            var decode =
                implicitPrefix
                    .DecodeEmbeddedIpv4(
                        address,
                        out var embedded);

            if (decode ==
                NotificationNat64DecodeResult.Malformed)
            {
                return true;
            }

            if (decode ==
                    NotificationNat64DecodeResult.Valid &&
                (embedded is null ||
                 NotificationHttpsEndpointPolicy
                    .IsProhibitedAddress(
                        embedded)))
            {
                return true;
            }
        }

        foreach (var prefix in
                 _nat64Prefixes)
        {
            var decode =
                prefix.DecodeEmbeddedIpv4(
                    address,
                    out var embedded);

            if (decode ==
                NotificationNat64DecodeResult.NoMatch)
            {
                continue;
            }

            configuredMatch =
                true;

            if (decode ==
                    NotificationNat64DecodeResult.Malformed ||
                embedded is null ||
                NotificationHttpsEndpointPolicy
                    .IsProhibitedAddress(
                        embedded))
            {
                return true;
            }
        }

        if (configuredMatch)
        {
            return false;
        }

        return NotificationHttpsEndpointPolicy
            .IsProhibitedAddress(
                address);
    }

    private static string ComputeFingerprint(
        IReadOnlyList<NotificationNat64Prefix> prefixes)
    {
        var canonical =
            string.Join(
                "\n",
                prefixes
                    .OrderBy(prefix =>
                        prefix.Prefix.ToString(),
                        StringComparer.Ordinal)
                    .ThenBy(prefix =>
                        prefix.PrefixLength)
                    .Select(prefix =>
                        $"{prefix.Prefix}/{prefix.PrefixLength}"));

        return Convert
            .ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(
                        canonical)))
            .ToLowerInvariant();
    }
}

public sealed record NotificationPinnedEndpoint
{
    public const int HardMaxAddresses = 16;

    internal NotificationPinnedEndpoint(
        Uri endpoint,
        IReadOnlyList<IPAddress> addresses,
        DateTimeOffset resolvedAtUtc,
        NotificationAddressPolicy addressPolicy)
    {
        ArgumentNullException.ThrowIfNull(
            endpoint);
        ArgumentNullException.ThrowIfNull(
            addresses);
        ArgumentNullException.ThrowIfNull(
            addressPolicy);

        Endpoint =
            NotificationHttpsEndpointPolicy
                .ValidateAndNormalize(
                    endpoint);

        if (resolvedAtUtc == default)
        {
            throw new ArgumentOutOfRangeException(
                nameof(resolvedAtUtc));
        }

        if (addresses.Count is < 1 or >
            HardMaxAddresses)
        {
            throw new ArgumentOutOfRangeException(
                nameof(addresses));
        }

        var admitted =
            addresses
                .Distinct()
                .ToArray();
        if (admitted.Length != addresses.Count ||
            admitted.Any(
                addressPolicy.IsProhibited))
        {
            throw new ArgumentException(
                "Notification endpoint resolution contains a prohibited, duplicate, or unsafe address.",
                nameof(addresses));
        }

        Addresses =
            Array.AsReadOnly(
                admitted);
        ResolvedAtUtc =
            resolvedAtUtc.ToUniversalTime();
        AddressPolicyFingerprint =
            addressPolicy.Fingerprint;
    }

    public Uri Endpoint { get; }

    public IReadOnlyList<IPAddress> Addresses { get; }

    public DateTimeOffset ResolvedAtUtc { get; }

    public string AddressPolicyFingerprint { get; }
}

public interface INotificationEndpointResolutionPort
{
    Task<NotificationPinnedEndpoint> ResolveForConnectionAsync(
        Uri configuredEndpoint,
        CancellationToken cancellationToken);
}

public static class NotificationHttpsEndpointPolicy
{
    public static Uri ValidateAndNormalize(
        Uri endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);

        if (!endpoint.IsAbsoluteUri ||
            !string.Equals(
                endpoint.Scheme,
                Uri.UriSchemeHttps,
                StringComparison.OrdinalIgnoreCase) ||
            string.IsNullOrWhiteSpace(endpoint.Host) ||
            !string.IsNullOrEmpty(endpoint.UserInfo) ||
            !string.IsNullOrEmpty(endpoint.Query) ||
            !string.IsNullOrEmpty(endpoint.Fragment))
        {
            throw new ArgumentException(
                "Notification endpoint must be an absolute HTTPS URI without credentials, query or fragment.",
                nameof(endpoint));
        }

        var host =
            endpoint.Host.TrimEnd('.');
        if (string.Equals(
                host,
                "localhost",
                StringComparison.OrdinalIgnoreCase) ||
            host.EndsWith(
                ".localhost",
                StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "Localhost notification destinations are prohibited.",
                nameof(endpoint));
        }

        if (IPAddress.TryParse(
                host,
                out var address) &&
            IsProhibitedAddress(address))
        {
            throw new ArgumentException(
                "Local, private, link-local, multicast, unspecified and metadata IP destinations are prohibited.",
                nameof(endpoint));
        }

        return new UriBuilder(endpoint)
        {
            Host = host.ToLowerInvariant(),
            Fragment = string.Empty,
            Query = string.Empty,
        }.Uri;
    }

    private static bool HasPrefix(
        byte[] value,
        params byte[] prefix)
    {
        if (value.Length < prefix.Length)
        {
            return false;
        }

        for (var index = 0;
             index < prefix.Length;
             index++)
        {
            if (value[index] != prefix[index])
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsAlwaysProhibitedTransitionAddress(
        IPAddress address)
    {
        if (address.AddressFamily !=
            System.Net.Sockets.AddressFamily.InterNetworkV6)
        {
            return false;
        }

        if (address.IsIPv4MappedToIPv6)
        {
            return true;
        }

        var bytes =
            address.GetAddressBytes();

        // RFC 6145 IPv4-translatable form ::ffff:0:0/96.
        var rfc6145 =
            bytes.Take(8).All(value => value == 0) &&
            bytes[8] == 0xff &&
            bytes[9] == 0xff &&
            bytes[10] == 0 &&
            bytes[11] == 0;

        // 6to4 and Teredo are never admitted configured destination literals.
        var sixToFour =
            HasPrefix(
                bytes,
                0x20, 0x02);
        var teredo =
            HasPrefix(
                bytes,
                0x20, 0x01, 0x00, 0x00);

        return
            rfc6145 ||
            sixToFour ||
            teredo;
    }

    private static bool IsStandardNat64Address(
        IPAddress address)
    {
        if (address.AddressFamily !=
            System.Net.Sockets.AddressFamily.InterNetworkV6)
        {
            return false;
        }

        var bytes =
            address.GetAddressBytes();

        var wellKnown =
            HasPrefix(
                bytes,
                0x00, 0x64, 0xff, 0x9b,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00);

        var localUse =
            HasPrefix(
                bytes,
                0x00, 0x64, 0xff, 0x9b,
                0x00, 0x01);

        return
            wellKnown ||
            localUse;
    }

    public static bool IsAlwaysProhibitedAddress(
        IPAddress address)
    {
        ArgumentNullException.ThrowIfNull(
            address);

        if (IPAddress.IsLoopback(address) ||
            address.Equals(IPAddress.Any) ||
            address.Equals(IPAddress.IPv6Any) ||
            address.IsIPv6LinkLocal ||
            address.IsIPv6SiteLocal ||
            address.IsIPv6Multicast ||
            IsAlwaysProhibitedTransitionAddress(
                address))
        {
            return true;
        }

        var bytes =
            address.MapToIPv6().IsIPv4MappedToIPv6
                ? address.MapToIPv4().GetAddressBytes()
                : address.GetAddressBytes();

        if (bytes.Length == 4)
        {
            return
                bytes[0] == 0 ||
                bytes[0] == 10 ||
                bytes[0] == 127 ||
                bytes[0] == 169 && bytes[1] == 254 ||
                bytes[0] == 172 && bytes[1] is >= 16 and <= 31 ||
                bytes[0] == 192 && bytes[1] == 168 ||
                bytes[0] == 100 && bytes[1] is >= 64 and <= 127 ||
                bytes[0] == 198 && bytes[1] is 18 or 19 ||
                bytes[0] >= 224;
        }

        return
            bytes[0] == 0xff ||
            (bytes[0] & 0xfe) == 0xfc;
    }

    public static bool IsProhibitedAddress(
        IPAddress address) =>
        IsAlwaysProhibitedAddress(
            address) ||
        IsStandardNat64Address(
            address);
}
