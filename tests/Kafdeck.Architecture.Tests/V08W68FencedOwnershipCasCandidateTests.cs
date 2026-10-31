using Kafdeck.Infrastructure.Persistence;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W68PostgresLeaseSchema")]
public sealed class V08W68FencedOwnershipCasCandidateTests
{
    [Fact]
    public async Task RealPostgresRejectsWrongOwnerEpochAndStaleRevision()
    {
        var cs=Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if(string.IsNullOrWhiteSpace(cs))return;
        var d="cas-"+Guid.NewGuid().ToString("N");
        var generation=Guid.NewGuid();
        var srcGeneration=Guid.NewGuid();
        var fingerprint=new string('a',64);
        await using var db=new NpgsqlConnection(cs);
        await db.OpenAsync();
        foreach(var ddl in W68OwnershipLedgerSchema.PostgreSqlStatements.Concat(
            new[]{W68LeaseSchemaCandidate.PostgreSql}))
        {
            await using var cmd=db.CreateCommand();
            cmd.CommandText=ddl;
            await cmd.ExecuteNonQueryAsync();
        }
        try
        {
            await using(var cmd=db.CreateCommand())
            {
                cmd.CommandText="""
                INSERT INTO kafdeck_declarative_key_versions
                (deployment_id,scope_kind,scope_id,resource_kind,resource_id,revision_high_water)
                VALUES (@d,'Tenant','scope','NotificationSubscription','sub',1);
                INSERT INTO kafdeck_declarative_ownership
                (deployment_id,scope_kind,scope_id,resource_kind,resource_id,
                 registered_owner_id,registered_source_id,registered_source_generation_id,
                 pinned_source_revision,ownership_generation_id,ownership_revision,
                 stored_lease_epoch,ownership_state,evidence_fingerprint,
                 claimant_principal_id,policy_version,audit_operation_id,updated_at_utc)
                VALUES (@d,'Tenant','scope','NotificationSubscription','sub',
                        'owner','source',@sg,@fp,@og,1,1,'Managed',@fp,'principal','v1','audit0',clock_timestamp());
                INSERT INTO kafdeck_declarative_controller_leases
                (deployment_id,lease_scope_id,holder_id,epoch,expires_at_utc)
                VALUES (@d,'writer','controller',2,clock_timestamp()+interval '2 minutes');
                """;
                cmd.Parameters.AddWithValue("@d",d);
                cmd.Parameters.AddWithValue("@sg",srcGeneration);
                cmd.Parameters.AddWithValue("@og",generation);
                cmd.Parameters.AddWithValue("@fp",fingerprint);
                await cmd.ExecuteNonQueryAsync();
            }
            async Task<long> Update(string owner,long epoch,long rev)
            {
                await using var tx=await db.BeginTransactionAsync();
                var result=await W68PostgreSqlFencedOwnershipCasCandidate.TryUpdateAsync(
                    db,tx,d,"Tenant","scope","NotificationSubscription","sub",
                    owner,"source","writer","controller",epoch,rev,
                    fingerprint,"principal","v1","audit1");
                if(result>0)await tx.CommitAsync();
                else await tx.RollbackAsync();
                return result;
            }
            Assert.Equal(0,await Update("foreign",2,1));
            Assert.Equal(0,await Update("owner",1,1));
            Assert.Equal(2,await Update("owner",2,1));
            await using(var ledger=db.CreateCommand())
            {
                ledger.CommandText="""
                    SELECT revision_high_water FROM kafdeck_declarative_key_versions
                    WHERE deployment_id=@d AND scope_kind='Tenant'
                      AND scope_id='scope' AND resource_kind='NotificationSubscription'
                      AND resource_id='sub'
                    """;
                ledger.Parameters.AddWithValue("@d",d);
                Assert.Equal(2L,(long)(await ledger.ExecuteScalarAsync())!);
            }
            Assert.Equal(0,await Update("owner",2,1));
        }
        finally
        {
            await using var cmd=db.CreateCommand();
            cmd.CommandText="""
                DELETE FROM kafdeck_declarative_ownership WHERE deployment_id=@d;
                DELETE FROM kafdeck_declarative_key_versions WHERE deployment_id=@d;
                DELETE FROM kafdeck_declarative_controller_leases WHERE deployment_id=@d;
                """;
            cmd.Parameters.AddWithValue("@d",d);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
