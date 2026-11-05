#!/usr/bin/env bash
set -euo pipefail
# Local conversion only. No registry reference is accepted by this helper.
archive="$1"; expected_config="$2"; layout="$3"
[[ "$archive" != /* && "$layout" != /* && "$archive" != *..* && "$layout" != *..* ]]
[[ "$expected_config" =~ ^sha256:[0-9a-f]{64}$ ]]
source scripts/release/skopeo-v08-tool.sh
mkdir -p "$layout"
gzip -dc "$archive" > "$layout/candidate-image.tar"
version="$(skopeo_pinned --version)"; [[ "$version" =~ ^skopeo\ version\ [0-9]+\. ]]
printf '%s\n' "$version" > "$layout/skopeo-version.txt"
skopeo_pinned copy --dest-compress --dest-compress-format gzip --dest-compress-level 6 "docker-archive:/work/$layout/candidate-image.tar" "dir:/work/$layout/image"
[[ "$(jq -r .config.digest "$layout/image/manifest.json")" == "$expected_config" ]]
printf '%s  %s\n' "${expected_config#sha256:}" "$layout/image/${expected_config#sha256:}" | sha256sum --check
digest="sha256:$(sha256sum "$layout/image/manifest.json" | cut -d' ' -f1)"
printf '%s\n' "$digest" > "$layout/manifest-digest.txt"
