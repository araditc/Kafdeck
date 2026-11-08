#!/usr/bin/env bash
set -euo pipefail

archive="${1:?Pass a gzip-compressed Docker image archive}"
# Docker's containerd store may report a manifest/index digest as .Id.
# Bind the provenance to the single image's actual config bytes instead.
config_entry="$(tar -xOzf "$archive" manifest.json | jq -er '
  if type == "array" and length == 1 then .[0].Config else error("Expected one Docker archive image") end
')"
[[ "$config_entry" =~ ^([0-9a-f]{64}\.json|blobs/sha256/[0-9a-f]{64})$ ]] || {
  echo 'Unexpected Docker archive config path.' >&2
  exit 1
}
digest="$(tar -xOzf "$archive" "$config_entry" | sha256sum | cut -d' ' -f1)"
[[ "$config_entry" == "$digest.json" || "$config_entry" == "blobs/sha256/$digest" ]] || {
  echo 'Docker archive config bytes differ from the declared digest.' >&2
  exit 1
}
printf 'sha256:%s\n' "$digest"
