#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f src/frontend/package.json ]]; then
  echo "No frontend scaffold found; frontend CI is not active yet."
  exit 0
fi

cd src/frontend

# Temporary W01 review-fix bootstrap: update and print the npm lockfile after
# adding the approved Vite build dependency. This path is removed before merge.
npm install --package-lock-only --ignore-scripts

echo "BEGIN_UPDATED_NPM_LOCKFILE"
cat package-lock.json
echo "END_UPDATED_NPM_LOCKFILE"

npm ci --ignore-scripts
npm run lint
npm run typecheck
npm test
npm run build
