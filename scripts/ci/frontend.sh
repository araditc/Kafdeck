#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f src/frontend/package.json ]]; then
  echo "No frontend scaffold found; frontend CI is not active yet."
  exit 0
fi

cd src/frontend
if [[ ! -f package-lock.json ]]; then
  echo "Bootstrap mode: generating package-lock.json for the new W01 scaffold."
  npm install --package-lock-only --ignore-scripts
  echo "BEGIN_GENERATED_NPM_LOCKFILE"
  cat package-lock.json
  echo "END_GENERATED_NPM_LOCKFILE"
fi

npm ci --ignore-scripts
npm run lint
npm run typecheck
npm test
npm run build
