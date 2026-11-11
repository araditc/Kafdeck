#!/usr/bin/env bash
set -euo pipefail
private="$1"
evidence="$2"
arch="$3"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
case "$arch:$(uname -m)" in amd64:x86_64|arm64:aarch64) ;; *) echo 'Native architecture mismatch' >&2; exit 1;; esac
archive="$private/signed/media/distribution/w81-oci-$arch-01c7553425fbc61ba4a7a21918265c562086ba02/kafdeck-linux-$arch.docker.tar.gz"
docker image load --input "$archive" >/dev/null
export D1_IMAGE_ID="$(docker image inspect --format '{{.Id}}' kafdeck:w81-fixture)"
test "$(docker image inspect --format '{{.Architecture}}' "$D1_IMAGE_ID")" = "$arch"
export D1_ANCHOR=d1-anchor D1_API_CONTAINER=d1-api
export D1_GENERATION="$(python3 -c 'import uuid;print(uuid.uuid4())')"
mkdir "$private/state"
openssl rand -hex 32 > "$private/material-key.txt"
sudo chown -R 1654:1654 "$private"
sudo chmod 755 "$private"
anchor="$(docker create --name "$D1_ANCHOR" --pull=never --network=none --read-only --user=1654:1654 --cap-drop=ALL --security-opt=no-new-privileges --entrypoint /bin/sleep "$D1_IMAGE_ID" 2000)"
cleanup() { docker rm -f d1-fresh d1-stale d1-admission "$D1_API_CONTAINER" "$D1_ANCHOR" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker start "$anchor" >/dev/null
docker create --name "$D1_API_CONTAINER" --pull=never --network="container:$anchor" --read-only --user=1654:1654 --cap-drop=ALL --security-opt=no-new-privileges \
  --tmpfs=/tmp:rw,nosuid,nodev,size=64m --env=HOME=/state \
  --mount "type=bind,src=$private,dst=$private,readonly" --mount "type=bind,src=$private/state,dst=/state" \
  "$D1_IMAGE_ID" --contentRoot "$private" --webroot /app/wwwroot >/dev/null
pid="$(docker inspect --format '{{.State.Pid}}' "$anchor")"
sudo nsenter --target "$pid" --net env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin HOME="$private" \
  JAVA_HOME="$JAVA_HOME" D1_ANCHOR="$anchor" D1_API_CONTAINER="$D1_API_CONTAINER" \
  D1_IMAGE_ID="$D1_IMAGE_ID" D1_GENERATION="$D1_GENERATION" \
  python3 -B "$root/deploy/dev/oci-d1/journey.py" "$private" "$evidence"
ARCH="$arch" IMAGE_ID="$D1_IMAGE_ID" EVIDENCE="$evidence" PRIVATE="$private" python3 -B - <<'PY'
import datetime,hashlib,json,os,pathlib,subprocess
p=pathlib.Path(os.environ['PRIVATE']); e=pathlib.Path(os.environ['EVIDENCE']); m=json.loads((p/'signed/manifest.json').read_text())
arch=os.environ['ARCH']; folder=p/f'signed/media/distribution/w81-oci-{arch}-{m["sourceSha"]}'
proof=json.loads((folder/'oci-native-startup.json').read_text())
report={'sourceSha':m['sourceSha'],'manifestSha256':hashlib.sha256((p/'signed/manifest.json').read_bytes()).hexdigest(),
 'architecture':arch,'machine':os.uname().machine,'emulated':False,'configDigest':proof['configDigest'],
 'archiveSha256':proof['archiveSha256'],'daemonImageId':os.environ['IMAGE_ID'],'uid':1654,'readOnlyRoot':True,'capabilities':'ALL dropped',
 'network':'container network namespace held by network-none anchor','completedUtc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
 'driverSha':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
 'driverFiles':{str(x):hashlib.sha256(x.read_bytes()).hexdigest() for x in sorted(pathlib.Path('deploy/dev/oci-d1').glob('*')) if x.is_file()},
 'scope':'actual signed OCI API runtime authenticated Keycloak/Kafka fixture reads, restart and SQLite backup/restore; no product rebuild or browser/business approval claim'}
(e/'oci-identity.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
PY
