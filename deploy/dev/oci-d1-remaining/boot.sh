#!/usr/bin/env bash
set -euo pipefail
private="$1"; evidence="$2"; arch="$3"; mode="$4"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
case "$arch:$(uname -m)" in amd64:x86_64|arm64:aarch64) ;; *) exit 1;; esac
archive="$private/signed/media/distribution/w81-oci-$arch-01c7553425fbc61ba4a7a21918265c562086ba02/kafdeck-linux-$arch.docker.tar.gz"
docker image load --input "$archive" >/dev/null
export D1_IMAGE_ID="$(docker image inspect --format '{{.Id}}' kafdeck:w81-fixture)"
test "$(docker image inspect --format '{{.Architecture}}' "$D1_IMAGE_ID")" = "$arch"
export D1_CONFIG_DIGEST="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["content"]["imageConfigDigests"][sys.argv[2]])' "$private/signed/manifest.json" "$arch")"
export D1_PG_IMAGE="$(cat "$private/postgres-image.txt" 2>/dev/null || true)"
mkdir "$private/state"
openssl rand -hex 32 > "$private/material-key.txt"
sudo chown -R 1654:1654 "$private"
sudo chmod 755 "$private"
anchor="$(docker create --name d1-anchor --pull=never --network=none --read-only --ulimit=core=0 --user=1654:1654 --cap-drop=ALL --security-opt=no-new-privileges --entrypoint /bin/sleep "$D1_IMAGE_ID" 2200)"
trap 'docker rm -f d1-anchor >/dev/null 2>&1 || true' EXIT
docker start "$anchor" >/dev/null
pid="$(docker inspect --format '{{.State.Pid}}' "$anchor")"
sudo nsenter --target "$pid" --net env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin HOME="$private" \
  D1_ANCHOR="$anchor" D1_IMAGE_ID="$D1_IMAGE_ID" D1_CONFIG_DIGEST="$D1_CONFIG_DIGEST" D1_PG_IMAGE="$D1_PG_IMAGE" \
  D1_CANONICAL_RUN=43013654037 GITHUB_SHA="$GITHUB_SHA" GITHUB_RUN_ID="$GITHUB_RUN_ID" \
  python3 -B "$root/deploy/dev/oci-d1-remaining/run.py" "$private" "$evidence" "$arch" "$mode"
