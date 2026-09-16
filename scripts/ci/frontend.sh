#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f src/frontend/package.json ]]; then
  echo "No frontend scaffold found; frontend CI is not active yet."
  exit 0
fi

cd src/frontend
if [[ ! -f package-lock.json ]]; then
  echo "Frontend package-lock.json is required for reproducible npm CI." >&2
  exit 1
fi

npm ci --ignore-scripts
npm run lint
npm run typecheck
npm test -- --run
npm run build
