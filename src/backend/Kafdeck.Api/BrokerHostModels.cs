using System.Net;
using System.Text.RegularExpressions;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

internal sealed class BrokerHostOptions
{
    internal Func<BrokerHostOptions>? Runtime { get; set; }
    internal BrokerHostOptions Current => Runtime?.Invoke() ?? this;
    public bool Enabled { get; init; }
    public string[] AllowedNetworks { get; init; } = [];
    public BrokerHostCredential[] Credentials { get; init; } = [];
    public Guid[] LogReaderUserIds { get; init; } = [];
    public BrokerLogSource[] LogSources { get; init; } = [];
    public Guid[] AgentInstallerUserIds { get; init; } = [];
    public BrokerAgentTarget[] AgentTargets { get; init; } = [];
    public BrokerAgentPackage[] AgentPackages { get; init; } = [];

    internal void Validate()
    {
        if (!Enabled) return;
        if (AllowedNetworks.Length is 0 or > 64 || AllowedNetworks.Any(value => !IPNetwork.TryParse(value, out _)) ||
            Credentials.Length is 0 or > 64 || Credentials.Select(value => value.Id).Distinct().Count() != Credentials.Length)
            throw new KafdeckConfigurationException("Invalid broker host network or credential configuration.");
        foreach (var credential in Credentials)
        {
            if (!BrokerHostValidation.Name(credential.Id) || credential.Authentication is not ("privateKey" or "password"))
                throw new KafdeckConfigurationException("Invalid broker host credential binding.");
            _ = SecretReference.Parse(credential.Secret);
            if (credential.Passphrase is not null) _ = SecretReference.Parse(credential.Passphrase);
        }
        if (LogReaderUserIds.Length > 256 || LogReaderUserIds.Contains(Guid.Empty) ||
            LogReaderUserIds.Distinct().Count() != LogReaderUserIds.Length ||
            LogSources.Length > 1024 || LogSources.Any(source => !BrokerHostValidation.ClusterId(source.ClusterId) ||
                source.BrokerId < 0 || string.IsNullOrWhiteSpace(source.MonitoringInstance) ||
                source.MonitoringInstance.Length > 160 || source.MonitoringInstance.Any(char.IsControl) ||
                source.MonitoringInstance == MonitoringSettings.MetadataInstance) ||
            LogSources.Select(source => (source.ClusterId, source.BrokerId)).Distinct().Count() != LogSources.Length ||
            LogSources.Select(source => (source.ClusterId, source.MonitoringInstance)).Distinct().Count() != LogSources.Length)
            throw new KafdeckConfigurationException("Invalid broker log readers or monitoring source mappings.");
        if (AgentInstallerUserIds.Length > 256 || AgentInstallerUserIds.Contains(Guid.Empty) ||
            AgentInstallerUserIds.Distinct().Count() != AgentInstallerUserIds.Length ||
            AgentTargets.Length > 32 || AgentTargets.Select(t => t.ClusterId).Distinct().Count() != AgentTargets.Length ||
            AgentTargets.Any(t => !BrokerHostValidation.ClusterId(t.ClusterId) || t.BrokerId < 0 ||
                !BrokerHostValidation.Name(t.MonitoringInstance) || t.MonitoringInstance == MonitoringSettings.MetadataInstance ||
                !Credentials.Any(c => c.Id == t.CredentialId && c.Authentication == "privateKey") ||
                t.ExpectedBrokers is < 1 or > 128) ||
            AgentPackages.Length > 64 || AgentPackages.Select(p => p.Version).Distinct().Count() != AgentPackages.Length ||
            AgentPackages.Select(p => p.Digest).Distinct().Count() != AgentPackages.Length ||
            AgentPackages.Any(p => !BrokerAgentModels.ValidVersion(p.Version) || !BrokerAgentModels.ValidDigest(p.Digest)))
            throw new KafdeckConfigurationException("Invalid broker agent lifecycle configuration.");
    }

    internal bool Allows(string address) => IPAddress.TryParse(address, out var ip) &&
        Current.AllowedNetworks.Any(value => IPNetwork.Parse(value).Contains(ip));
}

internal sealed class BrokerHostCredential
{
    public string Id { get; init; } = "";
    public string Authentication { get; init; } = "privateKey";
    public string Secret { get; init; } = "";
    public string? Passphrase { get; init; }
}

internal sealed record BrokerHostDraft(string Name, string Address, int Port, string User,
    string Fingerprint, string CredentialId);
internal sealed record BrokerHost(Guid Id, BrokerHostDraft Connection, DateTimeOffset VerifiedAtUtc);
internal sealed record BrokerServiceTarget(string Id, string Adapter, string? Unit, string? Project,
    string? Service, int? BrokerId, string? KafkaClusterId, string? Instance, string Status);
internal sealed record BrokerHostObservation(int Version, BrokerServiceTarget[] Targets);
internal sealed record BrokerHostBinding(string ClusterId, int BrokerId, Guid HostId, string TargetId,
    string KafkaClusterId, string Instance, DateTimeOffset VerifiedAtUtc);
internal sealed record BrokerHostWrite(long ExpectedRevision, BrokerHostDraft Host);
internal sealed record BrokerHostBind(long ExpectedRevision, string ClusterId, int BrokerId, Guid HostId, string TargetId);
internal sealed record BrokerLogSource(string ClusterId, int BrokerId, string MonitoringInstance);
internal sealed record BrokerLogRead(string ClusterId, int BrokerId, long From, long To);
internal sealed record BrokerLogCommand(string TargetId, int BrokerId, string KafkaClusterId, long From, long To);
internal sealed record BrokerLogLine(long TimeMs, string Text);
internal sealed record BrokerLogObservation(int Version, string Adapter, BrokerLogLine[] Lines, bool Truncated);

internal static partial class BrokerHostValidation
{
    [GeneratedRegex("\\A[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}\\z", RegexOptions.CultureInvariant)]
    private static partial Regex Names();
    internal static bool Name(string? value) => value is not null && Names().IsMatch(value);
    internal static bool ClusterId(string? value) => value is { Length: > 0 and <= 128 } && !value.Any(char.IsControl);
    internal static bool Draft(BrokerHostDraft? draft, BrokerHostOptions options) => draft is not null &&
        !string.IsNullOrWhiteSpace(draft.Name) && draft.Name.Length <= 100 && !draft.Name.Any(char.IsControl) &&
        options.Allows(draft.Address) && draft.Port is >= 1 and <= 65535 && Name(draft.User) &&
        options.Current.Credentials.Any(value => value.Id == draft.CredentialId) && Fingerprint(draft.Fingerprint);

    internal static bool Fingerprint(string? fingerprint)
    {
        if (fingerprint is null || !fingerprint.StartsWith("SHA256:", StringComparison.Ordinal)) return false;
        Span<byte> bytes = stackalloc byte[32];
        return fingerprint.Length == 50 && Convert.TryFromBase64String(fingerprint[7..] + "=", bytes, out var length) && length == 32;
    }
}

internal sealed class BrokerHostFailure(string code) : Exception(code)
{
    internal string Code { get; } = code;
}
