using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Data.Sqlite;

namespace Kafdeck.Api;

internal sealed record FleetConfiguration(bool Enabled, string[] AllowedNetworks, Guid[] LogReaderUserIds,
    BrokerLogSource[] LogSources, Guid[] AgentInstallerUserIds, BrokerAgentTarget[] AgentTargets,
    BrokerAgentPackage[] AgentPackages);
internal sealed record FleetCredential(string Id, string Authentication, string Secret, string? Passphrase,
    string Purpose, bool Revoked, long Revision);
internal sealed record FleetSettingsState(int Version, long Revision, long SettingsRevision,
    FleetConfiguration Configuration, FleetCredential[] Credentials, BrokerHostState Registry,
    FleetBootstrapOperation[]? Operations = null);
internal sealed record FleetCredentialInput(long ExpectedRevision, string Id, string Authentication,
    string? Secret, string? Passphrase, string Purpose, bool Revoke = false);
internal sealed record FleetConfigurationInput(long ExpectedRevision, FleetConfiguration Configuration);

// One encrypted, versioned document keeps configuration, secrets and audit effects atomic.
// The database belongs to this installation. Its key ring is backed up separately.
internal sealed class FleetSettingsStore
{
    private static readonly JsonSerializerOptions Json = new()
    { UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow };
    private readonly string _connection;
    private readonly IDataProtector _protector;
    private readonly BrokerHostOptions _legacy;
    internal string DatabasePath { get; }

    internal FleetSettingsStore(string path, IDataProtectionProvider protection, BrokerHostOptions legacy,
        BrokerHostState initialRegistry)
    {
        DatabasePath = path;
        _legacy = legacy;
        _protector = protection.CreateProtector("Kafdeck.Fleet.Settings.v1");
        _connection = new SqliteConnectionStringBuilder
        { DataSource = path, Pooling = false, DefaultTimeout = 5 }.ToString();
        using var db = Open();
        using var transaction = db.BeginTransaction(deferred: false);
        using var schema = db.CreateCommand();
        schema.Transaction = transaction;
        schema.CommandText = "CREATE TABLE IF NOT EXISTS fleet_state (id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL, ciphertext TEXT NOT NULL)";
        schema.ExecuteNonQuery();
        using var existing = db.CreateCommand();
        existing.Transaction = transaction;
        existing.CommandText = "SELECT count(*) FROM fleet_state";
        if ((long)existing.ExecuteScalar()! == 0)
        {
            var configuration = new FleetConfiguration(legacy.Enabled, legacy.AllowedNetworks,
                legacy.LogReaderUserIds, legacy.LogSources, legacy.AgentInstallerUserIds,
                legacy.AgentTargets, legacy.AgentPackages);
            var initial = new FleetSettingsState(1, 0, 0, configuration, [], initialRegistry);
            using var insert = db.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = "INSERT INTO fleet_state VALUES(1,0,$value)";
            insert.Parameters.AddWithValue("$value", Encode(initial));
            insert.ExecuteNonQuery();
        }
        transaction.Commit();
        // The encrypted database and its rollback journal inherit a private directory.
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
    }

    private SqliteConnection Open()
    {
        var db = new SqliteConnection(_connection);
        db.Open();
        return db;
    }

    internal FleetSettingsState Read()
    {
        using var db = Open();
        return Read(db, null);
    }

    private FleetSettingsState Read(SqliteConnection db, SqliteTransaction? transaction)
    {
        using var command = db.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT revision,ciphertext FROM fleet_state WHERE id=1";
        using var row = command.ExecuteReader();
        if (!row.Read() || row.GetString(1).Length > 16 * 1024 * 1024)
            throw new BrokerHostFailure("fleet_storage_invalid");
        try
        {
            var state = JsonSerializer.Deserialize<FleetSettingsState>(_protector.Unprotect(row.GetString(1)), Json);
            if (state is null || state.Version != 1 || state.Revision != row.GetInt64(0))
                throw new BrokerHostFailure("fleet_storage_invalid");
            return state;
        }
        catch (Exception error) when (error is CryptographicException or JsonException)
        { throw new BrokerHostFailure("fleet_keys_or_storage_unavailable"); }
    }

    private string Encode(FleetSettingsState state) => _protector.Protect(JsonSerializer.Serialize(state, Json));

    internal string SealPreview(FleetBootstrapPreview preview) =>
        _protector.CreateProtector("bootstrap-preview").Protect(JsonSerializer.Serialize(preview));

    internal FleetBootstrapPreview? ReadPreview(string? value)
    {
        if (value is not { Length: > 0 and <= 32768 }) return null;
        try { return JsonSerializer.Deserialize<FleetBootstrapPreview>(_protector.CreateProtector("bootstrap-preview").Unprotect(value)); }
        catch (Exception error) when (error is CryptographicException or JsonException) { return null; }
    }

    internal FleetSettingsState Change(Func<FleetSettingsState, FleetSettingsState> update)
    {
        try
        {
            using var db = Open();
            using var transaction = db.BeginTransaction(deferred: false);
            var current = Read(db, transaction);
            var next = update(current) with { Revision = checked(current.Revision + 1) };
            using var command = db.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "UPDATE fleet_state SET revision=$revision,ciphertext=$value WHERE id=1 AND revision=$previous";
            command.Parameters.AddWithValue("$revision", next.Revision);
            command.Parameters.AddWithValue("$previous", current.Revision);
            command.Parameters.AddWithValue("$value", Encode(next));
            if (command.ExecuteNonQuery() != 1) throw new BrokerHostFailure("fleet_revision_conflict");
            transaction.Commit();
            return next;
        }
        catch (SqliteException error) when (error.SqliteErrorCode is 5 or 6)
        { throw new BrokerHostFailure("fleet_revision_conflict"); }
    }

    internal BrokerHostOptions Options()
    {
        var state = Read();
        var configuration = state.Configuration;
        var replaced = state.Credentials.Select(item => item.Id).ToHashSet(StringComparer.Ordinal);
        return new()
        {
            Enabled = configuration.Enabled, AllowedNetworks = configuration.AllowedNetworks,
            Credentials = [.. _legacy.Credentials.Where(item => !replaced.Contains(item.Id)),
                .. state.Credentials.Where(item => !item.Revoked).Select(item => new BrokerHostCredential
                { Id = item.Id, Authentication = item.Authentication, Secret = "env:KAFDECK_FLEET_MANAGED" })],
            LogReaderUserIds = configuration.LogReaderUserIds, LogSources = configuration.LogSources,
            AgentInstallerUserIds = configuration.AgentInstallerUserIds, AgentTargets = configuration.AgentTargets,
            AgentPackages = configuration.AgentPackages,
        };
    }

    internal FleetCredential? Credential(string id)
    {
        var credential = Read().Credentials.SingleOrDefault(item => item.Id == id);
        if (credential?.Revoked == true) throw new BrokerHostFailure("fleet_credential_revoked");
        return credential;
    }

    internal FleetSettingsState SaveCredential(FleetCredentialInput input, SecurityAuditEvent audit)
    {
        if (!BrokerHostValidation.Name(input.Id) || input.Authentication is not ("privateKey" or "password") ||
            input.Purpose is not ("probe" or "agent")) throw new BrokerHostFailure("fleet_credential_invalid");
        if (!input.Revoke)
        {
            if (input.Secret is not { Length: > 0 and <= 65536 } || input.Secret.Contains('\0') ||
                input.Passphrase is { Length: > 4096 } || input.Passphrase?.Contains('\0') == true)
                throw new BrokerHostFailure("fleet_credential_invalid");
            // Parse before committing, without retaining a readable key file on disk.
            if (input.Authentication == "privateKey")
            {
                try
                {
                    using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(input.Secret));
                    using var key = new Renci.SshNet.PrivateKeyFile(stream, input.Passphrase);
                }
                catch (Exception error) when (error is ArgumentException or Renci.SshNet.Common.SshException or InvalidOperationException)
                { throw new BrokerHostFailure("fleet_private_key_invalid"); }
            }
        }
        return Change(state =>
        {
            if (state.SettingsRevision != input.ExpectedRevision) throw new BrokerHostFailure("fleet_revision_conflict");
            var current = state.Credentials.SingleOrDefault(item => item.Id == input.Id);
            if (current is null && state.Credentials.Length >= 64) throw new BrokerHostFailure("fleet_credential_limit");
            var credential = new FleetCredential(input.Id, input.Authentication, input.Revoke ? "" : input.Secret!,
                input.Revoke ? null : input.Passphrase, input.Purpose, input.Revoke, (current?.Revision ?? 0) + 1);
            return AddAudit(state with
            {
                SettingsRevision = checked(state.SettingsRevision + 1),
                Credentials = [.. state.Credentials.Where(item => item.Id != input.Id), credential],
            }, audit);
        });
    }

    internal FleetSettingsState SaveConfiguration(FleetConfigurationInput input, SecurityAuditEvent audit)
    {
        var configuration = input.Configuration;
        var options = Options();
        // Reuse the existing configuration validator, including target and permission bounds.
        var validation = new BrokerHostOptions
        {
            Enabled = true, AllowedNetworks = configuration.AllowedNetworks,
            Credentials = options.Credentials.Length == 0 ? [new() { Id = "validation", Secret = "env:VALIDATION" }] : options.Credentials,
            LogReaderUserIds = configuration.LogReaderUserIds, LogSources = configuration.LogSources,
            AgentInstallerUserIds = configuration.AgentInstallerUserIds, AgentTargets = configuration.AgentTargets,
            AgentPackages = configuration.AgentPackages,
        };
        try { validation.Validate(); }
        catch (KafdeckConfigurationException) { throw new BrokerHostFailure("fleet_configuration_invalid"); }
        return Change(state => state.SettingsRevision != input.ExpectedRevision
            ? throw new BrokerHostFailure("fleet_revision_conflict")
            : AddAudit(state with { Configuration = configuration, SettingsRevision = checked(state.SettingsRevision + 1) }, audit));
    }

    internal static FleetSettingsState AddAudit(FleetSettingsState state, SecurityAuditEvent audit)
    {
        if (state.Registry.PendingAudit.Length >= 128) throw new BrokerHostFailure("host_audit_unavailable");
        return state with { Registry = state.Registry with { PendingAudit = [.. state.Registry.PendingAudit, audit] } };
    }

    internal async Task DrainAuditAsync(ISecurityAuditSink sink, CancellationToken cancellation)
    {
        foreach (var audit in Read().Registry.PendingAudit)
        {
            await sink.WriteAsync(audit, cancellation);
            Change(state => state with { Registry = state.Registry with
            { PendingAudit = state.Registry.PendingAudit.Where(item => item.SessionCorrelationId != audit.SessionCorrelationId).ToArray() } });
        }
    }
}
