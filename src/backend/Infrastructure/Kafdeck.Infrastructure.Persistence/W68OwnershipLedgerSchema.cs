namespace Kafdeck.Infrastructure.Persistence;

// G2 schema candidate only. NO initializer currently invokes these statements.
// Future admission requires cross-connection CAS, authority and migration tests.
internal static class W68OwnershipLedgerSchema
{
    internal static IReadOnlyList<string> SqliteStatements { get; } =
    [
        """
        CREATE TABLE IF NOT EXISTS kafdeck_declarative_key_versions (
          deployment_id TEXT NOT NULL COLLATE BINARY,
          scope_kind TEXT NOT NULL COLLATE BINARY,
          scope_id TEXT NOT NULL COLLATE BINARY,
          resource_kind TEXT NOT NULL COLLATE BINARY,
          resource_id TEXT NOT NULL COLLATE BINARY,
          revision_high_water INTEGER NOT NULL CHECK (revision_high_water >= 0),
          PRIMARY KEY (deployment_id, scope_kind, scope_id, resource_kind, resource_id),
          CHECK (length(deployment_id) BETWEEN 1 AND 128),
          CHECK (length(scope_kind) BETWEEN 1 AND 64),
          CHECK (length(scope_id) BETWEEN 1 AND 128),
          CHECK (length(resource_kind) BETWEEN 1 AND 64),
          CHECK (length(resource_id) BETWEEN 1 AND 128)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS kafdeck_declarative_ownership (
          deployment_id TEXT NOT NULL COLLATE BINARY,
          scope_kind TEXT NOT NULL COLLATE BINARY,
          scope_id TEXT NOT NULL COLLATE BINARY,
          resource_kind TEXT NOT NULL COLLATE BINARY,
          resource_id TEXT NOT NULL COLLATE BINARY,
          registered_owner_id TEXT NOT NULL,
          registered_source_id TEXT NOT NULL,
          registered_source_generation_id TEXT NOT NULL,
          pinned_source_revision TEXT NOT NULL CHECK (length(pinned_source_revision) = 64),
          ownership_generation_id TEXT NOT NULL UNIQUE,
          ownership_revision INTEGER NOT NULL CHECK (ownership_revision >= 1),
          stored_lease_epoch INTEGER NOT NULL CHECK (stored_lease_epoch >= 0),
          ownership_state TEXT NOT NULL CHECK (ownership_state IN ('Managed','Relinquished','TransferPending')),
          evidence_fingerprint TEXT NOT NULL CHECK (length(evidence_fingerprint) = 64),
          claimant_principal_id TEXT NOT NULL,
          policy_version TEXT NOT NULL,
          audit_operation_id TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL,
          PRIMARY KEY (deployment_id, scope_kind, scope_id, resource_kind, resource_id),
          FOREIGN KEY (deployment_id, scope_kind, scope_id, resource_kind, resource_id)
            REFERENCES kafdeck_declarative_key_versions
              (deployment_id, scope_kind, scope_id, resource_kind, resource_id)
        )
        """
    ];

    internal static IReadOnlyList<string> PostgreSqlStatements { get; } =
    [
        """
        CREATE TABLE IF NOT EXISTS kafdeck_declarative_key_versions (
          deployment_id VARCHAR(128) COLLATE "C" NOT NULL,
          scope_kind VARCHAR(64) COLLATE "C" NOT NULL,
          scope_id VARCHAR(128) COLLATE "C" NOT NULL,
          resource_kind VARCHAR(64) COLLATE "C" NOT NULL,
          resource_id VARCHAR(128) COLLATE "C" NOT NULL,
          revision_high_water BIGINT NOT NULL CHECK (revision_high_water >= 0),
          PRIMARY KEY (deployment_id, scope_kind, scope_id, resource_kind, resource_id),
          CHECK (length(deployment_id) > 0 AND length(scope_kind) > 0 AND length(scope_id) > 0 AND length(resource_kind) > 0 AND length(resource_id) > 0)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS kafdeck_declarative_ownership (
          deployment_id VARCHAR(128) COLLATE "C" NOT NULL,
          scope_kind VARCHAR(64) COLLATE "C" NOT NULL,
          scope_id VARCHAR(128) COLLATE "C" NOT NULL,
          resource_kind VARCHAR(64) COLLATE "C" NOT NULL,
          resource_id VARCHAR(128) COLLATE "C" NOT NULL,
          registered_owner_id VARCHAR(128) NOT NULL,
          registered_source_id VARCHAR(128) NOT NULL,
          registered_source_generation_id UUID NOT NULL,
          pinned_source_revision CHAR(64) NOT NULL,
          ownership_generation_id UUID NOT NULL UNIQUE,
          ownership_revision BIGINT NOT NULL CHECK (ownership_revision >= 1),
          stored_lease_epoch BIGINT NOT NULL CHECK (stored_lease_epoch >= 0),
          ownership_state VARCHAR(32) NOT NULL CHECK (ownership_state IN ('Managed','Relinquished','TransferPending')),
          evidence_fingerprint CHAR(64) NOT NULL,
          claimant_principal_id VARCHAR(256) NOT NULL,
          policy_version VARCHAR(128) NOT NULL,
          audit_operation_id VARCHAR(128) NOT NULL,
          updated_at_utc TIMESTAMPTZ NOT NULL,
          PRIMARY KEY (deployment_id, scope_kind, scope_id, resource_kind, resource_id),
          FOREIGN KEY (deployment_id, scope_kind, scope_id, resource_kind, resource_id)
            REFERENCES kafdeck_declarative_key_versions
              (deployment_id, scope_kind, scope_id, resource_kind, resource_id)
        )
        """
    ];
}
