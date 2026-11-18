using System.Text.RegularExpressions;

namespace Kafdeck.Api;

internal sealed record BrokerAgentTarget(string ClusterId, int BrokerId, string MonitoringInstance,
    string CredentialId, int ExpectedBrokers);
internal sealed record BrokerAgentPackage(string Version, string Digest);
internal sealed record BrokerAgentPreviewInput(string ClusterId, int BrokerId, string Version);
internal sealed record BrokerAgentPlanInput(string Plan);
internal sealed record BrokerAgentCommand(string Action, string OperationId, string TargetId, int BrokerId, string KafkaClusterId,
    string Version, string Digest, string? Instance = null, string? CurrentDigest = null, string? LaunchMode = null, long? ApplyUntil = null);
internal sealed record BrokerAgentHostResult(string Stage, string? Code, string? Version, string? Digest, string? Adapter,
    string? CurrentVersion, string? CurrentDigest, string? Instance, string? LaunchMode, bool RestartRequired,
    long? MaintenanceFrom, long? MaintenanceTo, long? DispatchedAt, long? ProcessStartedAt);
internal sealed record BrokerAgentPlan(string Principal, string ClusterId, BrokerAgentTarget Target, BrokerAgentPackage Package,
    BrokerHostBinding Binding, BrokerHostDraft Connection, BrokerAgentCommand Command, long CreatedAt);

internal static partial class BrokerAgentModels
{
    [GeneratedRegex("\\A[0-9]+\\.[0-9]+\\.[0-9]+(?:[-.][A-Za-z0-9.-]{1,56})?\\z", RegexOptions.CultureInvariant)]
    private static partial Regex VersionPattern();
    internal static bool ValidVersion(string? version) => version is { Length: <= 72 } && VersionPattern().IsMatch(version);
    internal static bool ValidDigest(string? digest) => digest is { Length: 64 } && digest.All(c => char.IsAsciiDigit(c) || c is >= 'a' and <= 'f');
}
