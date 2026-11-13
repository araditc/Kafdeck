using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

internal sealed record ManagedUser(
    Guid Id, string Email, string Role, string[] ClusterIds, bool Enabled, DateTimeOffset SessionsValidFrom);

internal sealed record ManagedIdentityState(
    string Issuer, Guid? OwnerId, bool Provisioning, string? CodeHash, DateTimeOffset CodeExpires,
    ManagedUser[] Users, RegisteredKafkaConnection[]? Connections = null,
    ManagedUserProvisioning[]? UserProvisionings = null);

internal sealed record ManagedUserProvisioning(
    Guid RequestId, Guid RequestedBy, string Email, string Role, string[] ClusterIds,
    string Proof, DateTimeOffset CreatedAtUtc, Guid? UserId = null,
    bool Completed = false, bool Audited = false, string? RejectionCode = null);

internal sealed record RegisteredKafkaConnection(
    string Id, string Name, string[] BootstrapServers, string Protocol, string? Mechanism,
    RegisteredHttpServiceSettings? SchemaRegistry = null, RegisteredHttpServiceSettings? Connect = null,
    RegisteredHttpServiceSettings? KsqlDb = null)
{
    internal RegisteredHttpServiceSettings? GetService(RegisteredHttpService service) => service switch
    {
        RegisteredHttpService.SchemaRegistry => SchemaRegistry,
        RegisteredHttpService.KafkaConnect => Connect,
        RegisteredHttpService.KsqlDb => KsqlDb,
        _ => throw new ArgumentOutOfRangeException(nameof(service)),
    };
}

internal enum RegisteredHttpService { SchemaRegistry, KafkaConnect, KsqlDb }

internal sealed record RegisteredHttpServiceSettings(string Url, string Authentication, string? ProtectedCredential, Guid Revision);

internal sealed class ManagedIdentityStore(string path, string issuer)
{
    // Freeze the legacy v0.9 representation, including the admitted optional
    // provisioning receipts. Unknown newer fields must never be silently lost.
    private static readonly JsonSerializerOptions Format = new() { UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow };

    internal ManagedIdentityState Read()
    {
        try
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);
            using var document = JsonDocument.Parse(stream);
            RejectDuplicateProperties(document.RootElement);
            var state = document.RootElement.Deserialize<ManagedIdentityState>(Format);
            if (state is null || state.Issuer != issuer || state.Users is null ||
                (state.OwnerId is { } owner && !state.Users.Any(user => user.Id == owner && user.Role == "owner" && user.Enabled)))
                throw new InvalidDataException("Managed identity state is invalid for this realm.");
            if (state.UserProvisionings is { } intents && (intents.Length > 2048 ||
                intents.Select(intent => intent.RequestId).Distinct().Count() != intents.Length ||
                intents.Any(intent => intent.RequestId == Guid.Empty || intent.RequestedBy == Guid.Empty ||
                    intent.Proof.Length != 64 || !intent.Proof.All(char.IsAsciiHexDigit))))
                throw new InvalidDataException("Managed user provisioning state is invalid.");
            return state;
        }
        catch (FileNotFoundException) { return new(issuer, null, false, null, default, []); }
        catch (JsonException error) { throw new InvalidDataException("Managed identity state uses an invalid or unsupported format; restore a verified compatible backup.", error); }
    }

    internal FileStream? TryLock()
    {
        try { return new FileStream(path + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None); }
        catch (IOException) { return null; }
    }

    internal void Write(ManagedIdentityState state)
    {
        // A normal writer must not replace an unreadable/newer state. Callers
        // retain their existing exclusive operation lease and atomic rename.
        if (File.Exists(path)) _ = Read();
        var temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            using (var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(stream, state, Format);
                stream.Flush(flushToDisk: true);
            }
            File.Move(temporary, path, overwrite: true);
        }
        finally { File.Delete(temporary); }
    }

    internal string IssueCode()
    {
        using var lease = TryLock() ?? throw new InvalidOperationException("Another identity operation is in progress.");
        var state = Read();
        if (state.OwnerId is not null || state.Provisioning)
            throw new InvalidOperationException("This installation is already initialized or requires recovery.");

        var code = Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16));
        Write(state with { CodeHash = Convert.ToHexStringLower(Hash(code)), CodeExpires = DateTimeOffset.UtcNow.AddMinutes(15) });
        return code;
    }

    internal static bool MatchesCode(ManagedIdentityState state, string code) =>
        code.Length == 32 && state.CodeHash is { } hash && state.CodeExpires > DateTimeOffset.UtcNow &&
        CryptographicOperations.FixedTimeEquals(Convert.FromHexString(hash), Hash(code));

    internal ManagedUser? Find(OperatorSessionContext session) => Find(session.Identity.Key);

    internal ManagedUser? Find(OperatorIdentityKey key) =>
        key.Issuer == issuer && Guid.TryParse(key.Subject, out var id)
            ? Read().Users.SingleOrDefault(user => user.Id == id)
            : null;

    internal ManagedUser? FindPrincipal(string principal)
    {
        var prefix = $"oidc:{issuer}|";
        return principal.StartsWith(prefix, StringComparison.Ordinal) &&
            Guid.TryParseExact(principal[prefix.Length..], "D", out var id)
            ? Read().Users.SingleOrDefault(user => user.Id == id) : null;
    }

    internal static bool SessionAllowed(ManagedUser? user, OperatorSessionContext session) =>
        user is { Enabled: true } && session.AuthenticatedAtUtc >= user.SessionsValidFrom;

    private static byte[] Hash(string code) => SHA256.HashData(Encoding.UTF8.GetBytes(code));

    private static void RejectDuplicateProperties(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Object)
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var property in value.EnumerateObject())
            {
                if (!names.Add(property.Name)) throw new InvalidDataException("Managed identity state contains ambiguous duplicate properties.");
                RejectDuplicateProperties(property.Value);
            }
        }
        else if (value.ValueKind == JsonValueKind.Array)
            foreach (var item in value.EnumerateArray()) RejectDuplicateProperties(item);
    }
}

internal sealed class ManagedIdentityOptions
{
    public bool Enabled { get; init; }
    public string? StateFile { get; init; }
    public string? ServiceClientId { get; init; }
    public string? ServiceSecretFile { get; init; }

    internal ManagedIdentityStore ValidateAndCreate(KafdeckOptions options)
    {
        if (!Enabled || options.Deployment.Mode != AccessMode.Oidc || options.Deployment.Oidc is not { } oidc ||
            !options.Deployment.ListenUrls.All(url => new Uri(url).Scheme == "https") ||
            !Uri.TryCreate(oidc.Issuer, UriKind.Absolute, out var issuer) || issuer.Scheme != "https" ||
            issuer.Segments.Length < 3 || issuer.Segments[^2] != "realms/" || issuer.Query.Length != 0 ||
            issuer.Fragment.Length != 0 || issuer.UserInfo.Length != 0 ||
            string.IsNullOrWhiteSpace(ServiceClientId) || string.IsNullOrWhiteSpace(ServiceSecretFile) ||
            !Path.IsPathFullyQualified(ServiceSecretFile) || string.IsNullOrWhiteSpace(StateFile) ||
            !Path.IsPathFullyQualified(StateFile) || !Directory.Exists(Path.GetDirectoryName(StateFile)))
            throw new KafdeckConfigurationException("Managed identity requires HTTPS, a dedicated Keycloak realm, a service client and an absolute private state path.");
        if (string.Equals(issuer.Segments[^1].TrimEnd('/'), "master", StringComparison.OrdinalIgnoreCase))
            throw new KafdeckConfigurationException("Managed identity cannot use the Keycloak master realm.");
        return new ManagedIdentityStore(StateFile, oidc.Issuer);
    }
}
