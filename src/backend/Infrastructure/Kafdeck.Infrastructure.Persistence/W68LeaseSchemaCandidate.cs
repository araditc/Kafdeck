namespace Kafdeck.Infrastructure.Persistence;

// Candidate SQL-only lease table design. No application initializer executes it.
// Authoritative controller identity must come from authenticated backend context,
// not caller-supplied manifest or tool arguments.
internal static class W68LeaseSchemaCandidate
{
    internal const string Sqlite = """
        CREATE TABLE IF NOT EXISTS kafdeck_declarative_controller_leases (
          deployment_id TEXT NOT NULL COLLATE BINARY,
          lease_scope_id TEXT NOT NULL COLLATE BINARY,
          holder_id TEXT NOT NULL COLLATE BINARY,
          epoch INTEGER NOT NULL CHECK (typeof(epoch) = 'integer' AND epoch >= 1),
          expires_at_utc TEXT NOT NULL,
          PRIMARY KEY (deployment_id, lease_scope_id),
          CHECK (length(deployment_id) BETWEEN 1 AND 128),
          CHECK (length(lease_scope_id) BETWEEN 1 AND 128),
          CHECK (length(holder_id) BETWEEN 1 AND 128)
        )
        """;
    internal const string PostgreSql = """
        CREATE TABLE IF NOT EXISTS kafdeck_declarative_controller_leases (
          deployment_id VARCHAR(128) COLLATE "C" NOT NULL,
          lease_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
          holder_id VARCHAR(128) COLLATE "C" NOT NULL,
          epoch BIGINT NOT NULL CHECK (epoch >= 1),
          expires_at_utc TIMESTAMPTZ NOT NULL,
          PRIMARY KEY (deployment_id, lease_scope_id),
          CHECK (length(deployment_id) > 0 AND length(lease_scope_id) > 0 AND length(holder_id) > 0)
        )
        """;
}
