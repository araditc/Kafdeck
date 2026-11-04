#!/usr/bin/env bash
set -euo pipefail
# Explicit DBA action only. There is deliberately no API-startup auto-migration.
if [[ "${1:-}" != "--apply" || "${2:-}" != "--confirm-g2-baseline" || $# -ne 2 ]]; then
  echo "Usage: PGHOST=... PGPORT=... PGDATABASE=... PGUSER=... KAFDECK_W68_PG_SCHEMA=... $0 --apply --confirm-g2-baseline" >&2
  exit 64
fi
: "${PGHOST:?PGHOST required}"
: "${PGDATABASE:?PGDATABASE required}"
: "${PGUSER:?PGUSER required}"
: "${KAFDECK_W68_PG_SCHEMA:?Explicit target schema required}"
if ! [[ "$KAFDECK_W68_PG_SCHEMA" =~ ^[a-z_][a-z0-9_]*$ ]]; then
  echo "Schema must be an explicit simple lowercase PostgreSQL identifier." >&2
  exit 64
fi
if [[ -n "${PGOPTIONS:-}" ]]; then
  echo "PGOPTIONS must be unset: migration sets the exact search_path." >&2
  exit 64
fi
if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required." >&2
  exit 69
fi
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PGOPTIONS="-c search_path=${KAFDECK_W68_PG_SCHEMA},pg_catalog -c lock_timeout=10000"
# Sensitive credentials remain in standard PG* environment/.pgpass, not arguments.
# psql's --single-transaction guarantees rollback if ANY statement fails.
exec psql -X --set ON_ERROR_STOP=1 --single-transaction \
  --file "$ROOT/deploy/postgresql/w68-g2-baseline-v1.sql"
