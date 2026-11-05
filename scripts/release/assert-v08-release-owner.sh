#!/usr/bin/env bash
set -euo pipefail
# Protected local policy reads only: reject both actors before GH/artifact/registry IO.
policy=.github/release/release-owners.json
[[ "$(jq -r .schemaVersion "$policy")" == 1 ]]
expected="$(awk '$1 == "/.github/" { for(i=2;i<=NF;i++) if(substr($i,1,1)=="@") print tolower(substr($i,2)) }' .github/CODEOWNERS | sort -u)"
configured="$(jq -r '.actors[]' "$policy" | sort -u)"
[[ -n "$expected" && "$configured" == "$expected" ]] || { echo 'Protected release owner policy differs from CODEOWNERS.' >&2; exit 1; }
for actor in "${GITHUB_ACTOR:-}" "${GITHUB_TRIGGERING_ACTOR:-}"; do
  [[ "$actor" =~ ^[A-Za-z0-9-]+$ ]] && jq -e --arg actor "${actor,,}" '.actors | index($actor) != null' "$policy" >/dev/null || { echo 'Release action requires an approved original and triggering owner identity.' >&2; exit 1; }
done
