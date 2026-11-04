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
 -- Do not accept a pre-existing same-name table with an absent/wrong primary key.
 IF (WITH required(tbl, columns) AS (
       VALUES
       ('kafdeck_declarative_key_versions', ARRAY['deployment_id','scope_kind','scope_id','resource_kind','resource_id']),
       ('kafdeck_declarative_ownership', ARRAY['deployment_id','scope_kind','scope_id','resource_kind','resource_id']),
       ('kafdeck_declarative_controller_leases', ARRAY['deployment_id','lease_scope_id']),
       ('kafdeck_declarative_lease_epoch_high_water', ARRAY['deployment_id','lease_scope_id']),
       ('kafdeck_declarative_ownership_audit', ARRAY['operation_id']),
       ('kafdeck_declarative_schema_migrations', ARRAY['version'])
     )
     SELECT count(*) FROM required r WHERE EXISTS (
       SELECT 1 FROM pg_class tbl
       JOIN pg_namespace ns ON ns.oid=tbl.relnamespace
       JOIN pg_constraint pk ON pk.conrelid=tbl.oid AND pk.contype='p'
       WHERE ns.nspname=current_schema() AND tbl.relname=r.tbl AND tbl.relkind='r'
         AND ARRAY(
           SELECT a.attname::text FROM unnest(pk.conkey) WITH ORDINALITY k(attnum,ord)
           JOIN pg_attribute a ON a.attrelid=tbl.oid AND a.attnum=k.attnum
           ORDER BY k.ord
         )=r.columns
     ))<>6 THEN
   RAISE EXCEPTION 'W68 schema lacks exact required primary keys';
 END IF;
 -- Verify the exact column set, PostgreSQL types and NOT NULL on ALL six
 -- persisted W68 tables, not merely selected fencing/audit fields.
 IF (WITH required(tbl, col, kind) AS (
     VALUES
       ('kafdeck_declarative_schema_migrations','version','integer'),
       ('kafdeck_declarative_schema_migrations','manifest_id','character varying(128)'),
       ('kafdeck_declarative_schema_migrations','applied_at_utc','timestamp with time zone'),
       ('kafdeck_declarative_key_versions','deployment_id','character varying(128)'),
       ('kafdeck_declarative_key_versions','scope_kind','character varying(64)'),
       ('kafdeck_declarative_key_versions','scope_id','character varying(128)'),
       ('kafdeck_declarative_key_versions','resource_kind','character varying(64)'),
       ('kafdeck_declarative_key_versions','resource_id','character varying(128)'),
       ('kafdeck_declarative_key_versions','revision_high_water','bigint'),
       ('kafdeck_declarative_ownership','deployment_id','character varying(128)'),
       ('kafdeck_declarative_ownership','scope_kind','character varying(64)'),
       ('kafdeck_declarative_ownership','scope_id','character varying(128)'),
       ('kafdeck_declarative_ownership','resource_kind','character varying(64)'),
       ('kafdeck_declarative_ownership','resource_id','character varying(128)'),
       ('kafdeck_declarative_ownership','registered_owner_id','character varying(128)'),
       ('kafdeck_declarative_ownership','registered_source_id','character varying(128)'),
       ('kafdeck_declarative_ownership','registered_source_generation_id','uuid'),
       ('kafdeck_declarative_ownership','pinned_source_revision','character(64)'),
       ('kafdeck_declarative_ownership','ownership_generation_id','uuid'),
       ('kafdeck_declarative_ownership','ownership_revision','bigint'),
       ('kafdeck_declarative_ownership','stored_lease_epoch','bigint'),
       ('kafdeck_declarative_ownership','ownership_state','character varying(32)'),
       ('kafdeck_declarative_ownership','evidence_fingerprint','character(64)'),
       ('kafdeck_declarative_ownership','claimant_principal_id','character varying(256)'),
       ('kafdeck_declarative_ownership','policy_version','character varying(128)'),
       ('kafdeck_declarative_ownership','audit_operation_id','character varying(128)'),
       ('kafdeck_declarative_ownership','updated_at_utc','timestamp with time zone'),
       ('kafdeck_declarative_controller_leases','deployment_id','character varying(128)'),
       ('kafdeck_declarative_controller_leases','lease_scope_id','character varying(128)'),
       ('kafdeck_declarative_controller_leases','holder_id','character varying(128)'),
       ('kafdeck_declarative_controller_leases','epoch','bigint'),
       ('kafdeck_declarative_controller_leases','expires_at_utc','timestamp with time zone'),
       ('kafdeck_declarative_lease_epoch_high_water','deployment_id','character varying(128)'),
       ('kafdeck_declarative_lease_epoch_high_water','lease_scope_id','character varying(128)'),
       ('kafdeck_declarative_lease_epoch_high_water','epoch_high_water','bigint'),
       ('kafdeck_declarative_ownership_audit','operation_id','character varying(128)'),
       ('kafdeck_declarative_ownership_audit','deployment_id','character varying(128)'),
       ('kafdeck_declarative_ownership_audit','scope_kind','character varying(64)'),
       ('kafdeck_declarative_ownership_audit','scope_id','character varying(128)'),
       ('kafdeck_declarative_ownership_audit','resource_kind','character varying(64)'),
       ('kafdeck_declarative_ownership_audit','resource_id','character varying(128)'),
       ('kafdeck_declarative_ownership_audit','principal_id','character varying(128)'),
       ('kafdeck_declarative_ownership_audit','owner_id','character varying(128)'),
       ('kafdeck_declarative_ownership_audit','source_id','character varying(128)'),
       ('kafdeck_declarative_ownership_audit','lease_epoch','bigint'),
       ('kafdeck_declarative_ownership_audit','new_revision','bigint'),
       ('kafdeck_declarative_ownership_audit','event_fingerprint','character(64)'),
       ('kafdeck_declarative_ownership_audit','recorded_at_utc','timestamp with time zone')
   ), observed AS (
     SELECT rel.relname::text AS tbl, att.attname::text AS col,
            pg_catalog.format_type(att.atttypid,att.atttypmod) AS kind,
            att.attnotnull AS required_not_null
     FROM pg_class rel
     JOIN pg_namespace ns ON ns.oid=rel.relnamespace
     JOIN pg_attribute att ON att.attrelid=rel.oid
     WHERE ns.nspname=current_schema() AND rel.relkind='r'
       AND rel.relname IN (SELECT DISTINCT tbl FROM required)
       AND att.attnum>0 AND NOT att.attisdropped
   )
   SELECT (SELECT count(*) FROM observed)=(SELECT count(*) FROM required)
     AND NOT EXISTS(
       SELECT 1 FROM required r LEFT JOIN observed o
         ON o.tbl=r.tbl AND o.col=r.col
       WHERE o.col IS NULL OR o.kind<>r.kind OR NOT o.required_not_null
     )
 ) IS NOT TRUE THEN
   RAISE EXCEPTION 'W68 column set/type/nullability differs from versioned baseline';
 END IF;
 -- Require a validated FK to the exact ledger relation in this schema,
 -- with the same ordered five resource-key columns on both sides.
 IF NOT EXISTS (
   SELECT 1 FROM pg_constraint fk
   JOIN pg_class child ON child.oid=fk.conrelid
   JOIN pg_namespace child_ns ON child_ns.oid=child.relnamespace
   JOIN pg_class parent ON parent.oid=fk.confrelid
   JOIN pg_namespace parent_ns ON parent_ns.oid=parent.relnamespace
   WHERE child_ns.nspname=current_schema()
     AND parent_ns.nspname=current_schema()
     AND child.relname='kafdeck_declarative_ownership'
     AND parent.relname='kafdeck_declarative_key_versions'
     AND fk.conrelid=to_regclass(format('%I.%I',current_schema(),'kafdeck_declarative_ownership'))
     AND fk.confrelid=to_regclass(format('%I.%I',current_schema(),'kafdeck_declarative_key_versions'))
     AND fk.contype='f' AND fk.convalidated
     AND ARRAY(
       SELECT attr.attname::text FROM unnest(fk.conkey) WITH ORDINALITY k(attnum,ord)
       JOIN pg_attribute attr ON attr.attrelid=child.oid AND attr.attnum=k.attnum
       ORDER BY k.ord
     )=ARRAY['deployment_id','scope_kind','scope_id','resource_kind','resource_id']
     AND ARRAY(
       SELECT attr.attname::text FROM unnest(fk.confkey) WITH ORDINALITY k(attnum,ord)
       JOIN pg_attribute attr ON attr.attrelid=parent.oid AND attr.attnum=k.attnum
       ORDER BY k.ord
     )=ARRAY['deployment_id','scope_kind','scope_id','resource_kind','resource_id']
 ) THEN
   RAISE EXCEPTION 'W68 ownership FK is missing, unvalidated or incorrectly targeted';
 END IF;
 IF NOT EXISTS (
   SELECT 1 FROM information_schema.columns c
   WHERE c.table_schema=current_schema()
     AND c.table_name='kafdeck_declarative_ownership_audit'
     AND c.column_name='recorded_at_utc'
     AND c.column_default LIKE '%clock_timestamp()%'
 ) THEN
   RAISE EXCEPTION 'W68 audit timestamp default invariant absent';
 END IF;
END
$verify$;
