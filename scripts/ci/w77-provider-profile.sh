#!/usr/bin/env bash
set -euo pipefail

# Explicitly disposable local provider cells; never accepts a remote target/container ID.
provider="${1:?provider required}"; version="${2:?exact version required}"
case "$provider:$version" in
  apache:4.3.1|apache:4.2.1|apache:4.1.2|apache:3.9.2) image="apache/kafka:$version" ;;
  confluent-platform:8.3.2) image="confluentinc/cp-kafka:$version" ;;
  redpanda:25.3.11) image="docker.redpanda.com/redpandadata/redpanda:v$version" ;;
  *) echo 'Unadmitted provider/version profile.' >&2; exit 2 ;;
esac
out="$(pwd)/artifacts/w77/$provider-$version"
mkdir -p "$out"
export KAFDECK_G2_EVIDENCE_DIRECTORY="$out"
export KAFDECK_SOURCE_SHA="$(git rev-parse HEAD)"
export KAFDECK_RUN_W77_PROVIDER=1 KAFDECK_W77_PROVIDER="$provider" KAFDECK_W77_PROVIDER_VERSION="$version"
port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
export KAFDECK_W77_PROVIDER_BOOTSTRAP="127.0.0.1:$port"
name="kafdeck-w77-${provider}-$$"
cid=''
cleanup() {
  result=$?
  if [[ -n "$cid" ]]; then
    docker rm --force --volumes "$cid" >/dev/null
    if docker container inspect "$cid" >/dev/null 2>&1; then echo 'Owned provider cleanup failed.' >&2; return 1; fi
    printf '{"ownedContainerRemoved":true,"testExitCode":%s}\n' "$result" > "$out/cleanup.json"
  fi
}
trap cleanup EXIT
common=(--detach --name "$name" --label kafdeck.lab=w77 --memory=1g --cpus=1 -p "127.0.0.1:$port:$port")
if [[ "$provider" == redpanda ]]; then
  cid="$(docker run "${common[@]}" "$image" redpanda start --overprovisioned --smp 1 --memory 512M --reserve-memory 0M --node-id 0 --check=false \
    --kafka-addr "0.0.0.0:$port" --advertise-kafka-addr "127.0.0.1:$port")"
  ready=(docker exec "$cid" rpk cluster info -X "brokers=127.0.0.1:$port")
  version_command=(docker exec "$cid" rpk version)
else
  cid="$(docker run "${common[@]}" \
    -e CLUSTER_ID=4L6g3nShT-eMCtK--X86sw -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e "KAFKA_LISTENERS=PLAINTEXT://:$port,CONTROLLER://:29093" -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$port" \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false -e 'KAFKA_HEAP_OPTS=-Xms128m -Xmx384m' "$image")"
  cli=/opt/kafka/bin/kafka-topics.sh
  [[ "$provider" == confluent-platform ]] && cli=kafka-topics
  ready=(docker exec "$cid" "$cli" --bootstrap-server "localhost:$port" --list)
  version_command=(docker exec "$cid" "$cli" --version)
fi
ready_ok=0
for attempt in $(seq 1 60); do
  if timeout 5s "${ready[@]}" >/dev/null 2>&1; then ready_ok=1; break; fi
  sleep 1
done
[[ "$ready_ok" == 1 ]] || { echo 'Actual provider did not become ready.' >&2; exit 1; }
"${version_command[@]}" > "$out/actual-version.txt"
docker image inspect "$image" --format '{{json .}}' | python3 -c 'import json,sys; x=json.load(sys.stdin); print(json.dumps({k:x.get(k) for k in ("Id","RepoDigests","Architecture","Os")}))' > "$out/image-identity.json"
dotnet test tests/Kafdeck.Architecture.Tests/Kafdeck.Architecture.Tests.csproj --configuration Release --no-restore \
  --filter FullyQualifiedName~V09W77ProviderCapabilityLiveTests --results-directory "$out" --logger 'trx;LogFileName=profile.trx'
python3 scripts/ci/require-complete-trx.py "$out/profile.trx" V09W77ProviderCapabilityLiveTests:1
test -s "$out/w77-h3-$provider-$version-evidence.json"
