using System.Net;
using System.Text.RegularExpressions;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

internal sealed class BrokerHostOptions
{
    public bool Enabled { get; init; }
    public string[] AllowedNetworks { get; init; } = [];
    public BrokerHostCredential[] Credentials { get; init; } = [];

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
    }

    internal bool Allows(string address) => IPAddress.TryParse(address, out var ip) &&
        AllowedNetworks.Any(value => IPNetwork.Parse(value).Contains(ip));
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

internal static partial class BrokerHostValidation
{
    [GeneratedRegex("\\A[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}\\z", RegexOptions.CultureInvariant)]
    private static partial Regex Names();
    internal static bool Name(string? value) => value is not null && Names().IsMatch(value);
    internal static bool Draft(BrokerHostDraft? draft, BrokerHostOptions options) => draft is not null &&
        !string.IsNullOrWhiteSpace(draft.Name) && draft.Name.Length <= 100 && !draft.Name.Any(char.IsControl) &&
        options.Allows(draft.Address) && draft.Port is >= 1 and <= 65535 && Name(draft.User) &&
        options.Credentials.Any(value => value.Id == draft.CredentialId) && Fingerprint(draft.Fingerprint);

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
