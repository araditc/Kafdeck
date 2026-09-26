using System.Globalization;
using System.Text;

namespace Kafdeck.Modules.Administration;

/// <summary>
/// Typed durable conflict identity used by v0.6 fleet operations. The encoded
/// form is versioned and length-checked so exact resource identities cannot be
/// confused by delimiter-bearing names.
/// </summary>
public enum FleetConflictTargetKind
{
    Topic = 1,
    TopicPartition = 2,
    TopicConfiguration = 3,
    Broker = 4,
    BrokerConfiguration = 5,
    AclBinding = 6,
    ScramCredential = 7,
    QuotaEntity = 8,
    ConnectConnector = 9,
    TransferPair = 10,
}

public sealed record FleetConflictTarget(
    FleetConflictTargetKind Kind,
    string PhysicalClusterId,
    string ResourceId,
    string? SubresourceId = null);

public static class FleetConflictKeyCodec
{
    private const string Prefix = "fck1";
    private const int MaxClusterIdCharacters = 256;
    private const int MaxResourceIdCharacters = 512;
    private const int MaxSubresourceIdCharacters = 512;

    public static string Encode(FleetConflictTarget target)
    {
        ArgumentNullException.ThrowIfNull(target);
        if (!Enum.IsDefined(typeof(FleetConflictTargetKind), target.Kind))
        {
            throw new ArgumentOutOfRangeException(nameof(target), "Unsupported fleet conflict target kind.");
        }

        var clusterId = RequireBounded(
            target.PhysicalClusterId,
            nameof(target.PhysicalClusterId),
            MaxClusterIdCharacters);
        var resourceId = RequireBounded(
            target.ResourceId,
            nameof(target.ResourceId),
            MaxResourceIdCharacters);
        var subresourceId = target.SubresourceId is null
            ? string.Empty
            : RequireBounded(
                target.SubresourceId,
                nameof(target.SubresourceId),
                MaxSubresourceIdCharacters);

        return string.Join(
            '|',
            Prefix,
            ((int)target.Kind).ToString(CultureInfo.InvariantCulture),
            EncodeField(clusterId),
            EncodeField(resourceId),
            EncodeField(subresourceId));
    }

    public static FleetConflictTarget Decode(string encoded)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(encoded);
        if (encoded.Length > 4096 || encoded.Any(char.IsControl))
        {
            throw new MutationStateException("Fleet conflict key is malformed or exceeds the admitted bound.");
        }

        var parts = encoded.Split('|', StringSplitOptions.None);
        if (parts.Length != 5 || !string.Equals(parts[0], Prefix, StringComparison.Ordinal))
        {
            throw new MutationStateException("Fleet conflict key schema/version is unsupported.");
        }

        if (!int.TryParse(
                parts[1],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var numericKind) ||
            !Enum.IsDefined(typeof(FleetConflictTargetKind), numericKind))
        {
            throw new MutationStateException("Fleet conflict key contains an unsupported target kind.");
        }

        var target = new FleetConflictTarget(
            (FleetConflictTargetKind)numericKind,
            DecodeField(parts[2], "physical cluster"),
            DecodeField(parts[3], "resource"),
            DecodeField(parts[4], "subresource"));

        target = target with
        {
            PhysicalClusterId = RequireBounded(
                target.PhysicalClusterId,
                nameof(target.PhysicalClusterId),
                MaxClusterIdCharacters),
            ResourceId = RequireBounded(
                target.ResourceId,
                nameof(target.ResourceId),
                MaxResourceIdCharacters),
            SubresourceId = target.SubresourceId!.Length == 0
                ? null
                : RequireBounded(
                    target.SubresourceId,
                    nameof(target.SubresourceId),
                    MaxSubresourceIdCharacters),
        };

        if (!string.Equals(Encode(target), encoded, StringComparison.Ordinal))
        {
            throw new MutationStateException("Fleet conflict key is not in canonical encoded form.");
        }

        return target;
    }

    public static string Topic(string physicalClusterId, string topicName) =>
        Encode(new FleetConflictTarget(
            FleetConflictTargetKind.Topic,
            physicalClusterId,
            topicName));

    public static string TopicPartition(
        string physicalClusterId,
        string topicName,
        int partitionId)
    {
        if (partitionId < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionId));
        }

        return Encode(new FleetConflictTarget(
            FleetConflictTargetKind.TopicPartition,
            physicalClusterId,
            topicName,
            partitionId.ToString(CultureInfo.InvariantCulture)));
    }

    public static string TopicConfiguration(
        string physicalClusterId,
        string topicName,
        string configurationKey) =>
        Encode(new FleetConflictTarget(
            FleetConflictTargetKind.TopicConfiguration,
            physicalClusterId,
            topicName,
            configurationKey));

    public static string AclBinding(
        string physicalClusterId,
        string bindingHash)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bindingHash);
        var normalized = bindingHash.Trim().ToLowerInvariant();
        if (normalized.Length != 64 ||
            normalized.Any(character => !char.IsAsciiHexDigit(character)))
        {
            throw new MutationStateException(
                "ACL fleet conflict identity requires one full SHA-256 binding hash.");
        }

        return Encode(new FleetConflictTarget(
            FleetConflictTargetKind.AclBinding,
            physicalClusterId,
            normalized));
    }

    public static string Broker(
        string physicalClusterId,
        string brokerIdentity) =>
        Encode(new FleetConflictTarget(
            FleetConflictTargetKind.Broker,
            physicalClusterId,
            brokerIdentity));

    public static string BrokerConfiguration(
        string physicalClusterId,
        string brokerIdentity,
        string configurationKey) =>
        Encode(new FleetConflictTarget(
            FleetConflictTargetKind.BrokerConfiguration,
            physicalClusterId,
            brokerIdentity,
            configurationKey));

    public static string TransferPair(
        string firstPhysicalClusterId,
        string secondPhysicalClusterId)
    {
        var first = RequireBounded(
            firstPhysicalClusterId,
            nameof(firstPhysicalClusterId),
            MaxClusterIdCharacters);
        var second = RequireBounded(
            secondPhysicalClusterId,
            nameof(secondPhysicalClusterId),
            MaxClusterIdCharacters);
        if (string.Equals(first, second, StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Transfer pair requires two distinct physical clusters.");
        }

        var ordered = new[] { first, second }
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();
        return Encode(new FleetConflictTarget(
            FleetConflictTargetKind.TransferPair,
            ordered[0],
            ordered[1]));
    }

    private static string EncodeField(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var encoded = Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
        return $"{bytes.Length.ToString(CultureInfo.InvariantCulture)}:{encoded}";
    }

    private static string DecodeField(string encoded, string fieldName)
    {
        var separator = encoded.IndexOf(':');
        if (separator <= 0 ||
            (separator == encoded.Length - 1 &&
             !encoded.StartsWith("0:", StringComparison.Ordinal)))
        {
            throw new MutationStateException($"Fleet conflict {fieldName} field is malformed.");
        }

        if (!int.TryParse(
                encoded.AsSpan(0, separator),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var expectedByteLength) ||
            expectedByteLength < 0)
        {
            throw new MutationStateException($"Fleet conflict {fieldName} byte length is invalid.");
        }

        var base64Url = encoded[(separator + 1)..];
        var padded = base64Url
            .Replace('-', '+')
            .Replace('_', '/');
        padded += (padded.Length % 4) switch
        {
            0 => string.Empty,
            2 => "==",
            3 => "=",
            _ => throw new MutationStateException($"Fleet conflict {fieldName} encoding is invalid."),
        };

        byte[] bytes;
        try
        {
            bytes = Convert.FromBase64String(padded);
        }
        catch (FormatException)
        {
            throw new MutationStateException(
                $"Fleet conflict {fieldName} encoding is invalid.");
        }

        if (bytes.Length != expectedByteLength)
        {
            throw new MutationStateException(
                $"Fleet conflict {fieldName} byte length does not match its canonical encoding.");
        }

        return Encoding.UTF8.GetString(bytes);
    }

    private static string RequireBounded(string value, string fieldName, int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new MutationStateException(
                $"Fleet conflict {fieldName} is invalid or exceeds the admitted bound.");
        }

        return value;
    }
}

public static class FleetConflictRelation
{
    public static bool Conflicts(FleetConflictTarget left, FleetConflictTarget right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);

        if (!string.Equals(
                left.PhysicalClusterId,
                right.PhysicalClusterId,
                StringComparison.Ordinal))
        {
            return false;
        }

        if (IsTopicFamily(left.Kind) && IsTopicFamily(right.Kind))
        {
            // W41 intentionally uses a conservative parent/child relation. Topic,
            // partition and topic-config effects on the same physical topic block
            // one another until a later workstream proves a narrower relation safe.
            return string.Equals(left.ResourceId, right.ResourceId, StringComparison.Ordinal);
        }

        if (IsBrokerFamily(left.Kind) && IsBrokerFamily(right.Kind))
        {
            return string.Equals(left.ResourceId, right.ResourceId, StringComparison.Ordinal);
        }

        return left.Kind == right.Kind &&
               string.Equals(left.ResourceId, right.ResourceId, StringComparison.Ordinal) &&
               string.Equals(left.SubresourceId, right.SubresourceId, StringComparison.Ordinal);
    }

    public static bool ConflictsWithLegacyResourceKey(
        string fleetConflictKey,
        string legacyResourceKey)
    {
        var fleetTarget = FleetConflictKeyCodec.Decode(fleetConflictKey);
        var legacyTarget = ParseLegacyTopicResourceKey(legacyResourceKey);
        return Conflicts(fleetTarget, legacyTarget);
    }

    public static FleetConflictTarget ParseLegacyTopicResourceKey(string resourceKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resourceKey);
        const string prefix = "cluster/";
        const string marker = "/topic/";

        if (!resourceKey.StartsWith(prefix, StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Legacy mutation resource key is not an admitted topic-scoped conflict identity.");
        }

        var markerIndex = resourceKey.LastIndexOf(marker, StringComparison.Ordinal);
        if (markerIndex <= prefix.Length || markerIndex + marker.Length >= resourceKey.Length)
        {
            throw new MutationStateException(
                "Legacy mutation resource key is not an admitted topic-scoped conflict identity.");
        }

        var clusterId = resourceKey[prefix.Length..markerIndex];
        var topicName = resourceKey[(markerIndex + marker.Length)..];
        if (topicName.Contains('/'))
        {
            throw new MutationStateException(
                "Legacy topic resource key contains an ambiguous topic identifier.");
        }

        return new FleetConflictTarget(
            FleetConflictTargetKind.Topic,
            clusterId,
            topicName);
    }

    private static bool IsTopicFamily(FleetConflictTargetKind kind) => kind is
        FleetConflictTargetKind.Topic or
        FleetConflictTargetKind.TopicPartition or
        FleetConflictTargetKind.TopicConfiguration;

    private static bool IsBrokerFamily(FleetConflictTargetKind kind) => kind is
        FleetConflictTargetKind.Broker or
        FleetConflictTargetKind.BrokerConfiguration;
}
