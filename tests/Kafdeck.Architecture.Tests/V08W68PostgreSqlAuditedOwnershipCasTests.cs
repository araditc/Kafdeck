using Kafdeck.Infrastructure.Persistence;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W68PostgresLeaseSchema")]
public sealed class V08W68PostgreSqlAuditedOwnershipCasTests
{
    [Fact]
    public async Task Duplicate_audit_operation_rolls_back_revision_and_ownership()
    {
        var cs = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(cs)) return;
        var d = "audit-" + Guid.NewGuid().ToString("N");
        var fingerprint = new string('a', 64);
        await using var db = new NpgsqlConnection(cs);
        await db.OpenAsync();
        foreach (var ddl in W68OwnershipLedgerSchema.PostgreSqlStatements.Concat(new [] {
                     W68LeaseSchemaCandidate.PostgreSql, W68PostgreSqlOwnershipAuditCandidate.Schema }))
        {
            await using var create = db.CreateCommand();
            create.CommandText = ddl;
            await create.ExecuteNonQueryAsync();
        }
        try
        {
            await using (var seed = db.CreateCommand())
            {
                seed.CommandText = """
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
                            'owner','source',@sg,@fp,@og,1,1,'Managed',@fp,
                            'principal','v1','audit0',clock_timestamp());
                    INSERT INTO kafdeck_declarative_controller_leases
                    (deployment_id,lease_scope_id,holder_id,epoch,expires_at_utc)
                    VALUES (@d,'writer','controller',2,clock_timestamp()+interval '3 minutes');
                    """;
                seed.Parameters.AddWithValue("@d", d);
                seed.Parameters.AddWithValue("@sg", Guid.NewGuid());
                seed.Parameters.AddWithValue("@og", Guid.NewGuid());
                seed.Parameters.AddWithValue("@fp", fingerprint);
                await seed.ExecuteNonQueryAsync();
            }
            async Task<long> Apply(string op,long expected) =>
                await W68PostgreSqlAuditedOwnershipCasCandidate.TryUpdateAsync(
                    db,d,"Tenant","scope","NotificationSubscription","sub",
                    "owner","source","writer","controller",2,expected,
                    fingerprint,"principal","v1",op);

            Assert.Equal(2L, await Apply("audit1",1));
            // Reusing the audit id must fail, and its preceding CAS is rolled back.
            await Assert.ThrowsAsync<PostgresException>(() => Apply("audit1",2));
            await using var check = db.CreateCommand();
            check.CommandText = """
                SELECT o.ownership_revision,k.revision_high_water,
                    (SELECT count(*) FROM kafdeck_declarative_ownership_audit a
                     WHERE a.deployment_id=@d)
                FROM kafdeck_declarative_ownership o
                JOIN kafdeck_declarative_key_versions k
                 ON o.deployment_id=k.deployment_id AND o.scope_kind=k.scope_kind
                AND o.scope_id=k.scope_id AND o.resource_kind=k.resource_kind
                AND o.resource_id=k.resource_id
                WHERE o.deployment_id=@d
                """;
            check.Parameters.AddWithValue("@d",d);
            await using var reader = await check.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(2L,reader.GetInt64(0));
            Assert.Equal(2L,reader.GetInt64(1));
            Assert.Equal(1L,reader.GetInt64(2));
        }
        finally
        {
            await using var cleanup = db.CreateCommand();
            cleanup.CommandText = """
                DELETE FROM kafdeck_declarative_ownership_audit WHERE deployment_id=@d;
                DELETE FROM kafdeck_declarative_ownership WHERE deployment_id=@d;
                DELETE FROM kafdeck_declarative_key_versions WHERE deployment_id=@d;
                DELETE FROM kafdeck_declarative_controller_leases WHERE deployment_id=@d;
                """;
            cleanup.Parameters.AddWithValue("@d",d);
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
