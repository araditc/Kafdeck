using System.Data.Common;
using System.Globalization;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>Shared authoritative state with transactional CAS/audit. Never stores secret material or locators.</summary>
public sealed class AdoNotificationGovernanceStore(INotificationDeliveryDbConnectionFactory connections) : INotificationGovernanceStore
{
    public const int AuditRowsPerDestination = 1000;
    public async Task InitializeAsync(CancellationToken token)
    {
        await using var connection = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        if (connections.SupportsSelectForUpdate)
            await ExecuteAsync(connection, transaction, "SELECT pg_advisory_xact_lock(64964006602)", token).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction, """
            CREATE TABLE IF NOT EXISTS notification_governance (
              destination_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, candidate_fingerprint TEXT NOT NULL,
              revision BIGINT NOT NULL CHECK(revision>0), profile_enabled INTEGER NOT NULL CHECK(profile_enabled IN (0,1)),
              credential_enabled INTEGER NOT NULL CHECK(credential_enabled IN (0,1)), updated_at_utc TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS notification_governance_audit (
              destination_id TEXT NOT NULL, revision BIGINT NOT NULL, candidate_id TEXT NOT NULL,
              profile_enabled INTEGER NOT NULL, credential_enabled INTEGER NOT NULL,
              actor TEXT NOT NULL, policy_fingerprint TEXT NOT NULL, updated_at_utc TEXT NOT NULL,
              PRIMARY KEY(destination_id,revision));
            """, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
    }
    public async Task<NotificationGovernanceState?> GetAsync(string destinationId, CancellationToken token)
    {
        _ = new NotificationCredentialBindingId(destinationId);
        await using var connection = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT destination_id,candidate_id,candidate_fingerprint,revision,profile_enabled,credential_enabled,updated_at_utc FROM notification_governance WHERE destination_id=@id";
        Add(command,"@id",destinationId);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        return await reader.ReadAsync(token).ConfigureAwait(false) ? ReadState(reader) : null;
    }
    public async Task<NotificationGovernanceState?> ChangeAsync(NotificationGovernanceChange change, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(change);
        await using var connection = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        await using var clock = connection.CreateCommand();
        clock.Transaction=transaction; clock.CommandText=connections.DatabaseUtcNowSql;
        var time = await clock.ExecuteScalarAsync(token).ConfigureAwait(false);
        var now = time is DateTime dt ? new DateTimeOffset(DateTime.SpecifyKind(dt,DateTimeKind.Utc)) :
            time is DateTimeOffset offset ? offset.ToUniversalTime() : DateTimeOffset.Parse(Convert.ToString(time,CultureInfo.InvariantCulture)!,CultureInfo.InvariantCulture).ToUniversalTime();
        var stamp=now.ToString("O",CultureInfo.InvariantCulture);
        var revision=checked(change.ExpectedRevision+1);
        await using var command = connection.CreateCommand();
        command.Transaction=transaction;
        command.CommandText=change.ExpectedRevision==0 ? """
            INSERT INTO notification_governance(destination_id,candidate_id,candidate_fingerprint,revision,profile_enabled,credential_enabled,updated_at_utc)
            VALUES(@id,@candidate,@fingerprint,@revision,@profile,@credential,@stamp) ON CONFLICT(destination_id) DO NOTHING
            """ : """
            UPDATE notification_governance SET candidate_id=@candidate,candidate_fingerprint=@fingerprint,
              revision=@revision,profile_enabled=@profile,credential_enabled=@credential,updated_at_utc=@stamp
              WHERE destination_id=@id AND revision=@expected
            """;
        Add(command,"@id",change.DestinationId); Add(command,"@candidate",change.CandidateId);
        Add(command,"@fingerprint",change.CandidateFingerprint); Add(command,"@revision",revision);
        Add(command,"@profile",change.ProfileEnabled?1:0); Add(command,"@credential",change.CredentialEnabled?1:0);
        Add(command,"@stamp",stamp);
        if(change.ExpectedRevision!=0) Add(command,"@expected",change.ExpectedRevision);
        if(await command.ExecuteNonQueryAsync(token).ConfigureAwait(false)!=1)
        { await transaction.RollbackAsync(token).ConfigureAwait(false); return null; }
        await using var audit=connection.CreateCommand(); audit.Transaction=transaction;
        audit.CommandText="""
            INSERT INTO notification_governance_audit(destination_id,revision,candidate_id,profile_enabled,credential_enabled,actor,policy_fingerprint,updated_at_utc)
              VALUES(@id,@revision,@candidate,@profile,@credential,@actor,@policy,@stamp);
            DELETE FROM notification_governance_audit WHERE destination_id=@id AND revision<=@cutoff;
            """;
        Add(audit,"@id",change.DestinationId); Add(audit,"@revision",revision); Add(audit,"@candidate",change.CandidateId);
        Add(audit,"@profile",change.ProfileEnabled?1:0); Add(audit,"@credential",change.CredentialEnabled?1:0);
        Add(audit,"@actor",change.Actor); Add(audit,"@policy",change.PolicyFingerprint); Add(audit,"@stamp",stamp);
        Add(audit,"@cutoff",revision-AuditRowsPerDestination);
        await audit.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new(change.DestinationId,change.CandidateId,change.CandidateFingerprint,revision,change.ProfileEnabled,change.CredentialEnabled,now);
    }
    public async Task<IReadOnlyList<NotificationGovernanceAudit>> ReadAuditAsync(string destinationId,long afterRevision,int maxResults,CancellationToken token)
    {
        _ = new NotificationCredentialBindingId(destinationId);
        if(afterRevision<0 || maxResults is <1 or >100) throw new ArgumentOutOfRangeException(nameof(maxResults));
        await using var connection=await connections.OpenAsync(token).ConfigureAwait(false);
        await using var command=connection.CreateCommand();
        command.CommandText="SELECT destination_id,revision,candidate_id,profile_enabled,credential_enabled,actor,policy_fingerprint,updated_at_utc FROM notification_governance_audit WHERE destination_id=@id AND revision>@after ORDER BY revision LIMIT @take";
        Add(command,"@id",destinationId); Add(command,"@after",afterRevision); Add(command,"@take",maxResults);
        await using var reader=await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        var result=new List<NotificationGovernanceAudit>();
        while(await reader.ReadAsync(token).ConfigureAwait(false))
            result.Add(new(reader.GetString(0),reader.GetInt64(1),reader.GetString(2),reader.GetInt32(3)==1,reader.GetInt32(4)==1,
                reader.GetString(5),reader.GetString(6),DateTimeOffset.Parse(reader.GetString(7),CultureInfo.InvariantCulture)));
        return result;
    }
    private static NotificationGovernanceState ReadState(DbDataReader reader) => new(reader.GetString(0),reader.GetString(1),reader.GetString(2),reader.GetInt64(3),
        reader.GetInt32(4)==1,reader.GetInt32(5)==1,DateTimeOffset.Parse(reader.GetString(6),CultureInfo.InvariantCulture));
    private static void Add(DbCommand command,string name,object value) { var p=command.CreateParameter();p.ParameterName=name;p.Value=value;command.Parameters.Add(p); }
    private static async Task ExecuteAsync(DbConnection connection,DbTransaction transaction,string sql,CancellationToken token)
    { await using var command=connection.CreateCommand();command.Transaction=transaction;command.CommandText=sql;await command.ExecuteNonQueryAsync(token).ConfigureAwait(false); }
}
