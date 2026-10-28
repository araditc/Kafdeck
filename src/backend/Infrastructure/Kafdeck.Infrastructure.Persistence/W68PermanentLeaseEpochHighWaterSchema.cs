namespace Kafdeck.Infrastructure.Persistence;

// Internal schema candidate only. No migration/initializer executes these statements.
// The high-water row MUST outlive holder release and lease-row deletion.
// A future Acquire MUST atomically advance this row and claim the lease in
// one transaction, with authenticated controller identity and DB server time.
internal static class W68PermanentLeaseEpochHighWaterSchema
{
    internal const string Sqlite = """
        CREATE TABLE IF NOT EXISTS kafdeck_declarative_lease_epoch_high_water (
          deployment_id TEXT NOT NULL COLLATE BINARY,
          lease_scope_id TEXT NOT NULL COLLATE BINARY,
          epoch_high_water INTEGER NOT NULL
            CHECK (typeof(epoch_high_water) = 'integer' AND epoch_high_water >= 0),
          PRIMARY KEY (deployment_id, lease_scope_id),
          CHECK (length(deployment_id) BETWEEN 1 AND 128),
          CHECK (length(lease_scope_id) BETWEEN 1 AND 128)
        )
        """;

    internal const string PostgreSql = """
        CREATE TABLE IF NOT EXISTS kafdeck_declarative_lease_epoch_high_water (
          deployment_id VARCHAR(128) COLLATE "C" NOT NULL,
          lease_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
          epoch_high_water BIGINT NOT NULL CHECK (epoch_high_water >= 0),
          PRIMARY KEY (deployment_id, lease_scope_id),
          CHECK (length(deployment_id) > 0 AND length(lease_scope_id) > 0)
        )
        """;

    // Schema only, NOT an approved acquisition implementation. Caller must
    // lock the exact row, check overflow, update the high-water value and
    // take the authoritative lease in the same DB transaction.
}
