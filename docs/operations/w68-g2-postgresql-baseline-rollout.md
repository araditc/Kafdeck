# W68 G2 PostgreSQL baseline — controlled deployment

This is an **operator-invoked, transactionally applied baseline for an empty target schema**, not an automatic migration, authenticated controller, API activation, or permission to publish v0.8.

Before applying: take and verify a PostgreSQL backup/snapshot; confirm maintenance/change approval and rollback; identify the actual PostgreSQL writer and the dedicated deployment schema. Apply on primary with an authorized DBA role and TLS-verified connection. Do not use this script against an existing W68 schema installed by earlier candidates: five existing tables without a migration ledger are intentionally rejected and require a separately reviewed adoption/upgrade procedure.

Example (credentials supplied securely via `.pgpass` or a secret manager, never written to shell history):

```bash
export PGHOST=your-primary-host PGPORT=5432 PGDATABASE=kafdeck PGUSER=kafdeck_migrator
export PGSSLMODE=verify-full
export KAFDECK_W68_PG_SCHEMA=kafdeck
bash scripts/ops/w68-schema-apply.sh --apply --confirm-g2-baseline
```

The entrypoint enforces a simple explicit schema name and `psql -X --single-transaction --set ON_ERROR_STOP=1`. The SQL script takes a transaction advisory lock, rejects partial or unverified existing tables, installs all five dependent tables plus `kafdeck_declarative_schema_migrations`, then checks critical keys, column types, foreign-key and audit timestamp invariants before committing. Re-running on an unchanged, ledger-verified baseline is idempotent. If validation fails, PostgreSQL rolls back the transaction; **never** remove or alter the ledger to force admission.

Post-run acceptance: verify migration ledger version 1 and row `w68-g2-postgresql-baseline-v1`, five declarative tables in the selected schema, durable audit uniqueness, and the application remaining disabled until the separate identity/owner-source/risk/HA gate is admitted. For HA, verify on the primary and a promoted replica as part of a separate operational exercise; this script does not prove failover safety.

The migration is not currently wired into Kafdeck startup, and neither creates an authenticated GitOps controller nor enables Plan/Apply. Existing databases, alternate schemes and upgrades require additional governed migration work.
