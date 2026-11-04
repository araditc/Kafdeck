using System.Data.Common;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W68PostgresLeaseSchema")]
public sealed class V08G3DeclarativeMigrationTests
{
    [Theory]
    [InlineData("fresh")]
    [InlineData("v1-upgrade")]
    [InlineData("legacy-adopt")]
    [InlineData("partial-rollback")]
    [InlineData("bad-check-rollback")]
    public Task Sqlite_explicit_operator_migration_preserves_or_refuses_existing_state(string scenario) => Run(false, scenario);

    [W68G3PostgresTheory]
    [InlineData("fresh")]
    [InlineData("v1-upgrade")]
    [InlineData("legacy-adopt")]
    [InlineData("partial-rollback")]
    [InlineData("bad-unique-rollback")]
    [InlineData("bad-highwater-rollback")]
    public Task PostgreSql_explicit_operator_migration_preserves_or_refuses_existing_state(string scenario) => Run(true, scenario);

    private static async Task Run(bool postgres, string scenario)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2)); var token = timeout.Token;
        string? schema = null; string? admin = null;
        INotificationDeliveryDbConnectionFactory factory;
        if (postgres)
        {
            admin = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")!;
            schema = "w68migration_" + Guid.NewGuid().ToString("N");
            await using var c = new NpgsqlConnection(admin); await c.OpenAsync(token);
            await Execute(c, "CREATE SCHEMA \"" + schema + "\"", token);
            factory = new PostgreSqlNotificationDeliveryDbConnectionFactory(new NpgsqlConnectionStringBuilder(admin)
                { SearchPath = schema, Pooling = false, IncludeErrorDetail = false }.ConnectionString);
        }
        else
        {
            var folder = Path.Combine(Path.GetTempPath(), "kafdeck-w68-migration-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(folder);
            factory = new SqliteNotificationDeliveryDbConnectionFactory(Path.Combine(folder, "schema.db"));
        }
        try
        {
            if (scenario == "partial-rollback")
            {
                await Assert.ThrowsAsync<InvalidOperationException>(() => W68RuntimeSchema.InstallOrUpgradeAsync(factory, true, W68G3PersistenceFixture.R1, token));
                await using var c = await factory.OpenAsync(token);
                Assert.False(await Exists(c, "kafdeck_declarative_schema_migrations", token));
                await Execute(c, "CREATE TABLE kafdeck_declarative_controller_leases(unmanaged INTEGER)", token);
                await Assert.ThrowsAsync<InvalidOperationException>(() => W68RuntimeSchema.InstallOrUpgradeAsync(factory, true, W68G3PersistenceFixture.R1, token));
                Assert.False(await Exists(c, "kafdeck_declarative_schema_migrations", token));
                Assert.False(await Exists(c, "kafdeck_declarative_plans", token));
                return;
            }
            if (scenario == "fresh")
            {
                await W68RuntimeSchema.InstallOrUpgradeAsync(factory, token);
                await W68RuntimeSchema.InstallOrUpgradeAsync(factory, token);
                await W68RuntimeSchema.VerifyAsync(factory, token);
                await using var c = await factory.OpenAsync(token);
                await using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM kafdeck_declarative_schema_migrations";
                Assert.Equal(2L, Convert.ToInt64(await cmd.ExecuteScalarAsync(token)));
                return;
            }
            await Baseline(factory, postgres, token);
            await using (var c = await factory.OpenAsync(token))
            {
                if (scenario == "bad-unique-rollback")
                {
                    await using var name = c.CreateCommand(); name.CommandText = """
                        SELECT con.conname FROM pg_constraint con JOIN pg_class r ON r.oid=con.conrelid
                        JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname=current_schema()
                        AND r.relname='kafdeck_declarative_ownership' AND con.contype='u'
                        """;
                    var constraint = (string)(await name.ExecuteScalarAsync(token))!;
                    await Execute(c, "ALTER TABLE kafdeck_declarative_ownership DROP CONSTRAINT \"" + constraint.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"", token);
                    await Execute(c, "DROP TABLE kafdeck_declarative_schema_migrations", token);
                }
                if (scenario == "bad-check-rollback")
                {
                    await Execute(c, "DROP TABLE kafdeck_declarative_ownership", token);
                    await Execute(c, W68OwnershipLedgerSchema.SqliteStatements[1].Replace(
                        "CHECK (ownership_state IN ('Managed','Relinquished','TransferPending'))", "", StringComparison.Ordinal), token);
                    await Execute(c, "DROP TABLE kafdeck_declarative_schema_migrations", token);
                }
                if (scenario == "legacy-adopt") await Execute(c, "DROP TABLE kafdeck_declarative_schema_migrations", token);
                if (scenario is "v1-upgrade" or "legacy-adopt" or "bad-highwater-rollback") await SeedExisting(c, postgres, scenario == "bad-highwater-rollback", token);
            }
            if (scenario == "legacy-adopt")
            {
                await Assert.ThrowsAsync<InvalidOperationException>(() => W68RuntimeSchema.InstallOrUpgradeAsync(factory, token));
                await W68RuntimeSchema.InstallOrUpgradeAsync(factory, true, W68G3PersistenceFixture.R1, token);
                await W68RuntimeSchema.InstallOrUpgradeAsync(factory, true, W68G3PersistenceFixture.R1, token);
            }
            else if (scenario is "bad-unique-rollback" or "bad-check-rollback" or "bad-highwater-rollback")
            {
                await Assert.ThrowsAsync<InvalidOperationException>(() => W68RuntimeSchema.InstallOrUpgradeAsync(factory, true, W68G3PersistenceFixture.R1, token));
                await using var c = await factory.OpenAsync(token);
                Assert.False(await Exists(c, "kafdeck_declarative_plans", token));
                if (scenario != "bad-highwater-rollback") Assert.False(await Exists(c, "kafdeck_declarative_schema_migrations", token));
                return;
            }
            else await W68RuntimeSchema.InstallOrUpgradeAsync(factory, token);
            await W68RuntimeSchema.VerifyAsync(factory, token);
            var store = new W68RuntimeStore(factory);
            var key = new W68RuntimeResourceKey("legacy-deployment", "Deployment", "legacy-deployment", "NotificationSubscription", "legacy-subscription");
            var retained = await store.GetOwnershipAsync(key, token); Assert.NotNull(retained);
            Assert.Equal("TransferPending", retained.State); Assert.Equal(3, retained.Revision);
            Assert.Null(await store.GetRegistrationAsync(W68RuntimeRegistrationKind.Owner, key.DeploymentId, "legacy-owner", token));
            await using var verify = await factory.OpenAsync(token); await using var audit = verify.CreateCommand();
            audit.CommandText = "SELECT COUNT(*) FROM kafdeck_declarative_runtime_audit WHERE event_kind='SchemaAdoption'";
            Assert.Equal(scenario == "legacy-adopt" ? 1L : 0L, Convert.ToInt64(await audit.ExecuteScalarAsync(token)));
        }
        finally
        {
            if (postgres && schema is not null)
            {
                await using var c = new NpgsqlConnection(admin); await c.OpenAsync(); await Execute(c, "DROP SCHEMA \"" + schema + "\" CASCADE", CancellationToken.None);
            }
        }
    }

    private static async Task Baseline(INotificationDeliveryDbConnectionFactory f, bool pg, CancellationToken token)
    {
        await using var c = await f.OpenAsync(token);
        if (pg)
        {
            var assembly = typeof(V08G3DeclarativeMigrationTests).Assembly;
            using var stream = assembly.GetManifestResourceStream(assembly.GetManifestResourceNames().Single(x => x.EndsWith("w68-g2-baseline-v1.sql", StringComparison.Ordinal)))!;
            using var reader = new StreamReader(stream);
            await using var tx = await c.BeginTransactionAsync(token); await using var cmd = c.CreateCommand(); cmd.Transaction = tx; cmd.CommandText = await reader.ReadToEndAsync(token);
            await cmd.ExecuteNonQueryAsync(token); await tx.CommitAsync(token);
        }
        else
        {
            await W68RuntimeSchema.InstallOrUpgradeAsync(f, token);
            foreach (var table in new[] { "commit_receipts", "lease_bindings", "plans", "registrations", "generation_history", "runtime_audit", "runtime_schema", "deployments" })
                await Execute(c, "DROP TABLE kafdeck_declarative_" + table, token);
            await Execute(c, "DELETE FROM kafdeck_declarative_schema_migrations WHERE version=2", token);
        }
    }
    private static async Task SeedExisting(DbConnection c, bool pg, bool corruptHighWater, CancellationToken token)
    {
        await Execute(c, """
            INSERT INTO kafdeck_declarative_key_versions VALUES('legacy-deployment','Deployment','legacy-deployment','NotificationSubscription','legacy-subscription',3);
            INSERT INTO kafdeck_declarative_lease_epoch_high_water VALUES('legacy-deployment','legacy-scope',2)
            """, token);
        await using var cmd = c.CreateCommand();
        cmd.CommandText = """
            INSERT INTO kafdeck_declarative_controller_leases VALUES('legacy-deployment','legacy-scope','legacy-holder',2,@now);
            INSERT INTO kafdeck_declarative_ownership VALUES('legacy-deployment','Deployment','legacy-deployment','NotificationSubscription','legacy-subscription',
                'legacy-owner','legacy-source',@source,@hash,@generation,3,2,'TransferPending',@hash,'legacy-principal','legacy-policy','legacy-audit',@now)
            """;
        foreach (var (name, value) in new (string, object)[]
        {
            ("@now",pg?DateTime.UtcNow:DateTimeOffset.UtcNow.ToString("O")),("@hash",W68G3PersistenceFixture.R1),
            ("@source",pg?Guid.NewGuid():Guid.NewGuid().ToString("D")),("@generation",pg?Guid.NewGuid():Guid.NewGuid().ToString("D")),
        }) { var p=cmd.CreateParameter();p.ParameterName=name;p.Value=value;cmd.Parameters.Add(p); }
        await cmd.ExecuteNonQueryAsync(token);
        if(corruptHighWater) await Execute(c,"UPDATE kafdeck_declarative_key_versions SET revision_high_water=1",token);
    }
    private static async Task<bool> Exists(DbConnection c,string name,CancellationToken token)
    {
        await using var cmd=c.CreateCommand();cmd.CommandText=c is SqliteConnection?
            "SELECT name FROM sqlite_master WHERE type='table' AND name=@name":"SELECT to_regclass(@name)::text";
        var p=cmd.CreateParameter();p.ParameterName="@name";p.Value=name;cmd.Parameters.Add(p);
        return await cmd.ExecuteScalarAsync(token) is string;
    }
    private static async Task Execute(DbConnection c,string sql,CancellationToken token)
    { await using var cmd=c.CreateCommand();cmd.CommandText=sql;await cmd.ExecuteNonQueryAsync(token); }
}
