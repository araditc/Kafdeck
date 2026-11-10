#!/usr/bin/env bash
set -euo pipefail
umask 077

provider=$1
schedule=$2
source_sha=$3
evidence=/evidence
[[ "$provider" == sqlite || "$provider" == postgres ]]
[[ "$schedule" == preflight || "$schedule" == reference ]]
[[ "$source_sha" =~ ^[0-9a-f]{40}$ ]]
for counter in cpu.stat cpu.max memory.current memory.max memory.peak; do
  test -r "/sys/fs/cgroup/$counter"
done

private=$(mktemp -d)
source="$private/source"
mkdir "$source"
cp -R /checkout/. "$source/"
cd "$source"
git diff --quiet "$source_sha" HEAD -- src Directory.Build.props Directory.Packages.props global.json
git diff --quiet -- src
harness_sha=$(git rev-parse HEAD)
lab="$private/lab"
measurements="$private/measurements"
reference="$measurements/$provider-reference"
mkdir "$lab" "$measurements"
controller="$source/deploy/dev/w82-reference/physical-lab-control.ps1"
export JAVA_HOME
JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")
export DOTNET_CLI_TELEMETRY_OPTOUT=1
export DOTNET_NOLOGO=1

dotnet restore tests/Kafdeck.Architecture.Tests/Kafdeck.Architecture.Tests.csproj --locked-mode
dotnet build tests/Kafdeck.Architecture.Tests/Kafdeck.Architecture.Tests.csproj -c Release --no-restore -p:SourceRevisionId="$source_sha"
dotnet tool install dotnet-counters --tool-path "$private/tools" --version 10.0.750501
test_dll="$source/tests/Kafdeck.Architecture.Tests/bin/Release/net10.0/Kafdeck.Architecture.Tests.dll"
api_dll="$source/src/backend/Kafdeck.Api/bin/Release/net10.0/Kafdeck.Api.dll"

curl --fail --silent --show-error --location --retry 2 --max-time 180 \
  https://github.com/keycloak/keycloak/releases/download/26.8.1/keycloak-26.8.1.zip --output "$lab/keycloak.zip"
printf '%s  %s\n' 4cd312c889af358a819a74a6c6f068f9ea159a6959e49924607cec95cfd4c1a4 "$lab/keycloak.zip" | sha256sum --check
unzip -q "$lab/keycloak.zip" -d "$lab"
verified=0
for host in https://downloads.apache.org/kafka https://dlcdn.apache.org/kafka https://archive.apache.org/dist/kafka; do
  if curl --fail --silent --show-error --location --retry 1 --max-time 180 \
      "$host/4.3.1/kafka_2.13-4.3.1.tgz" --output "$lab/kafka.tgz" &&
    printf '%s  %s\n' c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c \
      "$lab/kafka.tgz" | sha512sum --check; then
    verified=1
    break
  fi
done
(( verified == 1 ))
tar -xzf "$lab/kafka.tgz" -C "$lab"

keycloak="$lab/keycloak-26.8.1"
identity="$lab/identity"
chmod +x "$keycloak/bin/kc.sh"
pwsh -NoProfile -File deploy/dev/prepare-keycloak-g2.ps1 -KeycloakHome "$keycloak" -OutputDirectory "$identity" -HttpsPort 18450
keycloak_pid=
sampler_pid=
pair="$lab/pair"
pair_args=(-OutputDirectory "$pair" -BinDirectory /usr/lib/postgresql/18/bin -PrimaryPort 15432 -StandbyPort 15433 -RestorePort 15434)
cleanup() {
  local code=$?
  trap - EXIT
  if [[ -n "$sampler_pid" ]]; then
    kill "$sampler_pid" 2>/dev/null || code=1
    wait "$sampler_pid" 2>/dev/null || true
  fi
  if [[ -n "$keycloak_pid" ]]; then
    kill "$keycloak_pid" 2>/dev/null || code=1
    wait "$keycloak_pid" 2>/dev/null || true
  fi
  if [[ -f "$pair/lab.json" ]]; then
    pwsh -NoProfile -File "$controller" -Action Stop "${pair_args[@]}" || code=1
  fi
  if [[ -d "$reference" ]]; then
    destination="$evidence/$provider-reference"
    mkdir "$destination"
    for file in checkpoint.json operations.ndjson resources.ndjson cancellation.json primary-runtime-counters.csv peer-runtime-counters.csv; do
      if [[ -f "$reference/$file" ]]; then
        cp "$reference/$file" "$destination/" || code=1
      fi
    done
  fi
  if (( code == 0 )); then
    printf 'true\n' > "$evidence/owned-infrastructure-stopped"
  fi
  chmod -R a+rX "$evidence"
  exit "$code"
}
trap cleanup EXIT

KC_HTTPS_KEY_STORE_PASSWORD=$(<"$identity/https-password.txt") \
  KC_DB_URL="jdbc:h2:file:$identity/keycloakdb;NON_KEYWORDS=VALUE" \
  JAVA_OPTS='-Xms64m -Xmx448m -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8' \
  "$keycloak/bin/kc.sh" start --http-host=127.0.0.1 --http-enabled=false --db=dev-file \
  --https-port=18450 --hostname=https://localhost:18450 --https-key-store-file="$identity/https.p12" \
  --cache=local --import-realm >"$lab/keycloak.log" 2>&1 &
keycloak_pid=$!
issuer=https://localhost:18450/realms/kafdeck-g2
for ((attempt = 0; attempt < 120; attempt++)); do
  if curl --fail --silent --max-time 2 --cacert "$identity/ca.pem" "$issuer/.well-known/openid-configuration" \
    >"$lab/discovery.json" && python3 -c 'import json,sys; assert json.load(sys.stdin)["issuer"] == sys.argv[1]' "$issuer" <"$lab/discovery.json"; then
    break
  fi
  kill -0 "$keycloak_pid"
  sleep 2
done
(( attempt < 120 ))

if [[ "$provider" == postgres ]]; then
  pwsh -NoProfile -File "$controller" -Action Prepare "${pair_args[@]}" -DisableWalArchiving
  export KAFDECK_TEST_POSTGRES="$(<"$pair/primary-connection-string.txt");Maximum Pool Size=16"
  export KAFDECK_W82_POSTGRES_TWO_NODE=1 KAFDECK_W82_POSTGRES_PHYSICAL_PAIR=1
  cp "$pair/replication-evidence.json" "$evidence/"
  test_filter='PostgreSQL_reference_schedule'
else
  test_filter='SQLite_reference_schedule'
fi

export KAFDECK_RUN_W77=1 KAFDECK_W82_FULL_DATASET=1 KAFDECK_G2_KAFKA_DELETE_CAPABLE=0
export KAFDECK_SOURCE_SHA="$source_sha" KAFDECK_W82_REFERENCE_SCHEDULE="$schedule"
export KAFDECK_KEYCLOAK_G2_LAB="$identity/lab.json" KAFDECK_G2_EVIDENCE_DIRECTORY="$measurements"
export KAFDECK_W77_KAFKA_HOME="$lab/kafka_2.13-4.3.1" KAFDECK_W77_JAVA_HOME="$JAVA_HOME"
export KAFDECK_G2_KAFKA_BOOTSTRAP=127.0.0.1:19092 KAFDECK_W82_COUNTERS="$private/tools/dotnet-counters"
export KAFDECK_W82_NETWORK='isolated container loopback'
export KAFDECK_W82_CPU_MODEL
KAFDECK_W82_CPU_MODEL=$(awk -F ': ' '/^model name/ { print $2; exit }' /proc/cpuinfo)

{
  printf 'sourceSha=%s\nharnessSha=%s\nprovider=%s\nschedule=%s\n' "$source_sha" "$harness_sha" "$provider" "$schedule"
  printf 'cpu.max=%s\nmemory.max=%s\n' "$(< /sys/fs/cgroup/cpu.max)" "$(< /sys/fs/cgroup/memory.max)"
  uname -a
  df -T "$lab"
  dotnet --info
  postgres --version
  java -version 2>&1
  "$KAFDECK_W82_COUNTERS" --version
  sha256sum "$api_dll" "$test_dll" "$KAFDECK_W82_COUNTERS" | awk '{print $1, $2}' | sed "s|$private/|fixture/|"
} >"$evidence/provenance.txt"

python3 deploy/dev/w82-reference/sample-cgroup.py "$evidence/container-samples.csv" &
sampler_pid=$!
dotnet test "$test_dll" --filter "FullyQualifiedName~V09W82ReferenceLoad.$test_filter" \
  --results-directory "$private" --logger 'trx;LogFileName=reference.trx'
python3 - "$private/reference.trx" "$reference/checkpoint.json" <<'PY'
import json
import sys
import xml.etree.ElementTree as et

counters = et.parse(sys.argv[1]).find(".//{*}Counters").attrib
assert counters["total"] == counters["passed"] == "1", counters
with open(sys.argv[2]) as checkpoint:
    result = json.load(checkpoint)
assert result["completed"] and result["cleaned"] and result["errors"] == 0, result["phase"]
PY
if [[ "$provider" == postgres ]]; then
  pwsh -NoProfile -File "$controller" -Action Sync "${pair_args[@]}"
  cp "$pair/latest-sync-evidence.json" "$evidence/"
fi
(( $(wc -l <"$evidence/container-samples.csv") >= 60 ))
