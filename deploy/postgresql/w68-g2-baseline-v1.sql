-- W68 G2 operational baseline v1. This file is intentionally explicit-operator-run.
-- Invoke through scripts/ops/w68-schema-apply.sh with --single-transaction.
-- Requires trusted DBA connection and a deliberately chosen search_path.
SELECT pg_advisory_xact_lock(68080041);

DO $guard$
DECLARE existing_count integer; ledger_exists boolean; verified boolean;
BEGIN
 IF current_schema() IS NULL OR current_schema() IN ('pg_catalog','information_schema')
 THEN RAISE EXCEPTION 'Unsafe schema for W68 migration'; END IF;
 SELECT count(*)::integer INTO existing_count FROM pg_class c
 JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=current_schema() AND c.relname IN (
   'kafdeck_declarative_key_versions',
   'kafdeck_declarative_ownership',
   'kafdeck_declarative_controller_leases',
   'kafdeck_declarative_lease_epoch_high_water',
   'kafdeck_declarative_ownership_audit');
 SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname=current_schema() AND c.relname='kafdeck_declarative_schema_migrations')
 INTO ledger_exists;
 IF existing_count<>0 OR ledger_exists THEN
   IF existing_count<>5 OR NOT ledger_exists THEN
     RAISE EXCEPTION 'W68 partial/unmanaged schema: manual migration required';
   END IF;
   EXECUTE 'SELECT EXISTS (SELECT 1 FROM kafdeck_declarative_schema_migrations
              WHERE version=1 AND manifest_id=''w68-g2-postgresql-baseline-v1'')'
      INTO verified;
   IF NOT COALESCE(verified,false) THEN
     RAISE EXCEPTION 'Unverified existing W68 baseline';
   END IF;
 END IF;
END
$guard$;

CREATE TABLE IF NOT EXISTS kafdeck_declarative_schema_migrations (
 version INTEGER PRIMARY KEY CHECK (version > 0),
 manifest_id VARCHAR(128) COLLATE "C" NOT NULL,
 applied_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS kafdeck_declarative_key_versions (
          deployment_id VARCHAR(128) COLLATE "C" NOT NULL,
          scope_kind VARCHAR(64) COLLATE "C" NOT NULL,
          scope_id VARCHAR(128) COLLATE "C" NOT NULL,
          resource_kind VARCHAR(64) COLLATE "C" NOT NULL,
          resource_id VARCHAR(128) COLLATE "C" NOT NULL,
          revision_high_water BIGINT NOT NULL CHECK (revision_high_water >= 0),
          PRIMARY KEY (deployment_id, scope_kind, scope_id, resource_kind, resource_id),
          CHECK (length(deployment_id) > 0 AND length(scope_kind) > 0 AND length(scope_id) > 0 AND length(resource_kind) > 0 AND length(resource_id) > 0)
        );

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
        );

CREATE TABLE IF NOT EXISTS kafdeck_declarative_controller_leases (
          deployment_id VARCHAR(128) COLLATE "C" NOT NULL,
          lease_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
          holder_id VARCHAR(128) COLLATE "C" NOT NULL,
          epoch BIGINT NOT NULL CHECK (epoch >= 1),
          expires_at_utc TIMESTAMPTZ NOT NULL,
          PRIMARY KEY (deployment_id, lease_scope_id),
          CHECK (length(deployment_id) > 0 AND length(lease_scope_id) > 0 AND length(holder_id) > 0)
        );

CREATE TABLE IF NOT EXISTS kafdeck_declarative_lease_epoch_high_water (
          deployment_id VARCHAR(128) COLLATE "C" NOT NULL,
          lease_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
          epoch_high_water BIGINT NOT NULL CHECK (epoch_high_water >= 0),
          PRIMARY KEY (deployment_id, lease_scope_id),
          CHECK (length(deployment_id) > 0 AND length(lease_scope_id) > 0)
        );

CREATE TABLE IF NOT EXISTS kafdeck_declarative_ownership_audit (
          operation_id VARCHAR(128) COLLATE "C" PRIMARY KEY,
          deployment_id VARCHAR(128) COLLATE "C" NOT NULL,
          scope_kind VARCHAR(64) COLLATE "C" NOT NULL,
          scope_id VARCHAR(128) COLLATE "C" NOT NULL,
          resource_kind VARCHAR(64) COLLATE "C" NOT NULL,
          resource_id VARCHAR(128) COLLATE "C" NOT NULL,
          principal_id VARCHAR(128) COLLATE "C" NOT NULL,
          owner_id VARCHAR(128) COLLATE "C" NOT NULL,
          source_id VARCHAR(128) COLLATE "C" NOT NULL,
          lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
          new_revision BIGINT NOT NULL CHECK (new_revision > 0),
          event_fingerprint CHAR(64) NOT NULL,
          recorded_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        );

INSERT INTO kafdeck_declarative_schema_migrations (version,manifest_id)
VALUES (1,'w68-g2-postgresql-baseline-v1')
ON CONFLICT (version) DO NOTHING;

DO $verify$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM kafdeck_declarative_schema_migrations
    WHERE version=1 AND manifest_id='w68-g2-postgresql-baseline-v1')
 THEN RAISE EXCEPTION 'W68 migration ledger version mismatch'; END IF;
 IF (SELECT count(*) FROM information_schema.tables
      WHERE table_schema=current_schema()
        AND table_name IN (
          'kafdeck_declarative_key_versions',
          'kafdeck_declarative_ownership',
          'kafdeck_declarative_controller_leases',
          'kafdeck_declarative_lease_epoch_high_water',
          'kafdeck_declarative_ownership_audit'))<>5
 THEN RAISE EXCEPTION 'W68 baseline incomplete'; END IF;
END
$verify$;
