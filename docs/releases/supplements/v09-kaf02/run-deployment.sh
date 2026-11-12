#!/usr/bin/env bash
set -euo pipefail

[[ ${WSL_DISTRO_NAME:-} == Kafdeck-KAF02-Deploy-8f4f8b2 && $(id -u) == 0 ]]
assets=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(realpath "$assets/../..")
source="$root/deploy/dev/.generated/w77-admission-worktree"
lab=/opt/kafdeck-cni-lab
evidence=$(realpath -m -- "${1:?Pass a fresh evidence directory}")
[[ ! -e "$evidence" ]]
mkdir -p -- "$evidence"
source_sha=8f4f8b228f91ef6b51e9d679b09a47463a40154c
node_pid=$(cat "$lab/node.pid")
[[ "$node_pid" =~ ^[1-9][0-9]+$ && $(readlink "/proc/$node_pid/ns/net") != $(readlink /proc/self/ns/net) ]]
[[ $(cat "/proc/$node_pid/comm") == k3s-server || $(cat "/proc/$node_pid/comm") == k3s ]]
kafka_home="$lab/kafka"
archive="$root/artifacts/v09-w82-offline-stage/exact-main/kafdeck-offline-candidate.tar.gz"
provenance="$root/artifacts/v09-w82-offline-stage/exact-main/candidate-provenance.json"
helm="$root/artifacts/w81-tools/windows-amd64/helm.exe"
[[ $(jq -er .sourceSha "$provenance") == "$source_sha" ]]
archive_sha=$(jq -er .archiveSha256 "$provenance")
printf '%s  %s\n' "$archive_sha" "$archive" | sha256sum -c -
approved=172.30.71.20
outside=172.30.72.20
bootstrap="$approved:9092"
changed="$outside:9092"
topic=kaf02-deployment-probe
namespace=kaf02-fixture
release=kaf02
workload=kaf02-kafdeck
hostname=kafdeck.example.test
scratch=$(mktemp -d "$lab/fixture.XXXXXXXX")
broker_ns=
broker_pid=
trace_pid=

node_net() { nsenter --target "$node_pid" --net -- "$@"; }
broker_net() { nsenter --target "$broker_ns" --net -- "$@"; }
kubectl() { node_net "$lab/k3s" kubectl --kubeconfig "$lab/kubeconfig" "$@"; }
ctr() { "$lab/k3s" ctr --address /run/k3s/containerd/containerd.sock --namespace k8s.io "$@"; }

stop_broker() {
  if [[ -n "$broker_pid" ]]; then kill -TERM "$broker_pid" 2>/dev/null || true; wait "$broker_pid" 2>/dev/null || true; broker_pid=; fi
}
stop_trace() {
  if [[ -n "$trace_pid" ]]; then kill -INT "$trace_pid" 2>/dev/null || true; wait "$trace_pid" 2>/dev/null || true; trace_pid=; fi
}
cleanup() {
  stop_trace
  stop_broker
  if [[ -n "$broker_ns" ]]; then kill -TERM "$broker_ns" 2>/dev/null || true; wait "$broker_ns" 2>/dev/null || true; fi
  kubectl delete namespace "$namespace" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl delete pv kaf02-fixture-pv --ignore-not-found --wait=false >/dev/null 2>&1 || true
  rm -rf -- "$scratch"
}
trap cleanup EXIT

kubectl wait node/kaf02-fixture --for=condition=Ready --timeout=90s
kubectl version -o json >"$evidence/kubernetes-version.json"
ctr images import "$archive" >"$evidence/image-import.txt"
archive_ref=$(tar -xOzf "$archive" manifest.json | jq -er '.[0].RepoTags[0]')
read -r image_ref image_digest < <(ctr images ls | awk -v ref="$archive_ref" '$1 == ref || $1 == "docker.io/library/"ref { print $1, $3 }')
[[ "$image_digest" =~ ^sha256:[a-f0-9]{64}$ ]]
image_repo=docker.io/library/kafdeck
if ! ctr images ls | awk -v tag="$image_repo@$image_digest" '$1 == tag { found=1 } END { exit !found }'; then
  ctr images tag "$image_ref" "$image_repo@$image_digest" >/dev/null
fi

unshare --net -- sleep 900 &
broker_ns=$!
for _ in {1..30}; do
  if [[ $(readlink "/proc/$broker_ns/ns/net") != $(readlink /proc/self/ns/net) ]]; then break; fi
  sleep .1
done
[[ $(readlink "/proc/$broker_ns/ns/net") != $(readlink /proc/self/ns/net) ]]
broker_net ip link set lo up
attach_link() {
  local broker_link=$1 node_link=$2 broker_address=$3 node_address=$4
  ip link add "$broker_link" type veth peer name "$node_link"
  ip link set "$broker_link" netns "$broker_ns"
  ip link set "$node_link" netns "$node_pid"
  broker_net ip address add "$broker_address/24" dev "$broker_link"
  node_net ip address add "$node_address/24" dev "$node_link"
  broker_net ip link set "$broker_link" up
  node_net ip link set "$node_link" up
}
attach_link kaf02-approved k02node-in "$approved" 172.30.71.1
attach_link kaf02-outside k02node-out "$outside" 172.30.72.1
[[ -z $(broker_net ip route show default) ]]

openssl req -x509 -newkey rsa:2048 -noenc -days 1 -subj /CN=kaf02-broker \
  -addext "subjectAltName=IP:$approved,IP:$outside" -keyout "$scratch/broker.key" -out "$scratch/broker.crt" 2>/dev/null
openssl rand -hex 24 | tr -d '\n' >"$scratch/broker-password"
openssl pkcs12 -export -inkey "$scratch/broker.key" -in "$scratch/broker.crt" \
  -out "$scratch/broker.p12" -passout "file:$scratch/broker-password"

write_broker_config() {
  cat >"$scratch/broker.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
controller.listener.names=CONTROLLER
listeners=CLIENT://0.0.0.0:9092,CONTROLLER://127.0.0.1:9093,INTERNAL://127.0.0.1:9094
advertised.listeners=CLIENT://$1,INTERNAL://127.0.0.1:9094
listener.security.protocol.map=CLIENT:SSL,CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
listener.name.client.ssl.keystore.type=PKCS12
listener.name.client.ssl.keystore.location=$scratch/broker.p12
listener.name.client.ssl.keystore.password=$(cat "$scratch/broker-password")
listener.name.client.ssl.key.password=$(cat "$scratch/broker-password")
listener.name.client.ssl.client.auth=none
log.dirs=$scratch/broker-data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
EOF
}

start_broker() {
  write_broker_config "$1"
  nsenter --target "$broker_ns" --net -- env KAFKA_HEAP_OPTS='-Xms128m -Xmx384m' \
    bash "$kafka_home/bin/kafka-server-start.sh" "$scratch/broker.properties" >"$scratch/broker.log" 2>&1 &
  broker_pid=$!
  for _ in {1..20}; do
    kill -0 "$broker_pid" 2>/dev/null || break
    if broker_net timeout 6 bash "$kafka_home/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:9094 --list >/dev/null 2>&1; then return; fi
    sleep 1
  done
  printf 'Kafka fixture did not become ready.\n' >&2
  return 1
}

write_broker_config "$bootstrap"
broker_net bash "$kafka_home/bin/kafka-storage.sh" format -t VjBHNnpPVUR5T1RncHRLZg -c "$scratch/broker.properties" >"$evidence/storage-format.log"
start_broker "$bootstrap"
broker_net bash "$kafka_home/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:9094 \
  --create --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null

kubectl create namespace "$namespace" >/dev/null
install -d -m 0700 -o 1654 -g 1654 "$scratch/state"
kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
  name: kaf02-fixture-pv
spec:
  storageClassName: ""
  capacity: { storage: 128Mi }
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  claimRef: { namespace: $namespace, name: fixture-state }
  hostPath: { path: $scratch/state, type: Directory }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: fixture-state, namespace: $namespace }
spec:
  storageClassName: ""
  volumeName: kaf02-fixture-pv
  accessModes: [ReadWriteOnce]
  resources: { requests: { storage: 128Mi } }
EOF
openssl req -x509 -newkey rsa:2048 -noenc -days 1 -subj "/CN=$hostname" \
  -addext "subjectAltName=DNS:$hostname" -keyout "$scratch/tls.key" -out "$scratch/tls.crt" 2>/dev/null
openssl rand -hex 24 | tr -d '\n' > "$scratch/tls-password"
openssl pkcs12 -export -inkey "$scratch/tls.key" -in "$scratch/tls.crt" \
  -out "$scratch/tls.pfx" -passout "file:$scratch/tls-password"
kubectl -n "$namespace" create secret generic fixture-tls \
  --from-file="tls.pfx=$scratch/tls.pfx" --from-file="tls-password=$scratch/tls-password" >/dev/null
kubectl -n "$namespace" create secret generic fixture-credentials --from-file="kafka-ca.pem=$scratch/broker.crt" >/dev/null
kubectl -n "$namespace" create configmap fixture-config \
  --from-literal='appsettings.Production.json={"Kafdeck":{"Clusters":[{"Tls":{"CaCertificate":"file:/run/kafdeck-credentials/kafka-ca.pem"}}]}}' >/dev/null

jq -n --arg repo "$image_repo" --arg digest "$image_digest" --arg hostname "$hostname" --arg ns "$namespace" \
  --arg broker "$bootstrap" --arg approved "$approved/32" --arg outside "$outside/32" \
  '{image:{repository:$repo,digest:$digest},publicHost:$hostname,operatorConfigMap:"fixture-config",
    credentialsSecret:"fixture-credentials",tlsSecret:"fixture-tls",oidc:{issuer:"https://idp.example.test/realms/kafdeck",clientId:"fixture"},
    kafka:{clusterId:"fixture",bootstrapServers:[$broker],protocol:"Ssl"},persistence:{provider:"Sqlite",existingClaim:"fixture-state"},
    network:{ingressNamespaces:[$ns],egress:[{cidr:$approved,port:9092},{cidr:$outside,port:9092}]}}' >"$evidence/values-positive.json"
jq '.network.egress |= .[0:1]' "$evidence/values-positive.json" >"$evidence/values-restricted.json"
render() {
  "$helm" template "$release" "$(wslpath -w "$source/deploy/helm/kafdeck")" --namespace "$namespace" \
    --values "$(wslpath -w "$evidence/values-$1.json")" >"$evidence/rendered-$1.yaml"
}
render positive
render restricted
kubectl -n "$namespace" apply -f "$evidence/rendered-positive.yaml" >/dev/null
kubectl -n "$namespace" rollout status "deployment/$workload" --timeout=180s
pod=$(kubectl -n "$namespace" get pod -l "app.kubernetes.io/instance=$release" -o jsonpath='{.items[0].metadata.name}')
pod_uid=$(kubectl -n "$namespace" get pod "$pod" -o jsonpath='{.metadata.uid}')
pod_ip=$(kubectl -n "$namespace" get pod "$pod" -o jsonpath='{.status.podIP}')
[[ -n "$pod_uid" && "$pod_ip" == 10.197.* ]]
container_id=$(kubectl -n "$namespace" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].containerID}')
container_id=${container_id#containerd://}
[[ "$container_id" =~ ^[a-f0-9]{64}$ ]]
pod_exec() {
  ctr tasks exec --exec-id "kaf02-$BASHPID-$RANDOM" --user 1654:1654 "$container_id" "$@"
}

pod_exec mkdir -p /tmp/kaf02-probe
tar -C "$assets/portable-probe" -cf - Kaf02DeploymentProbe.dll Kaf02DeploymentProbe.deps.json \
  Kaf02DeploymentProbe.runtimeconfig.json Confluent.Kafka.dll runtimes/linux-x64 |
  pod_exec tar -xf - -C /tmp/kaf02-probe

client() {
  pod_exec dotnet /tmp/kaf02-probe/Kaf02DeploymentProbe.dll \
    "$1" "$bootstrap" "$topic" "$2" /run/kafdeck-credentials/kafka-ca.pem
}
offset() {
  broker_net bash "$kafka_home/bin/kafka-get-offsets.sh" --bootstrap-server 127.0.0.1:9094 --topic "$topic" --time latest |
    awk -F: -v topic="$topic" '$1 == topic && $2 == 0 { print $3 }'
}
trace() {
  nsenter --target "$broker_ns" --net -- tcpdump -p -i kaf02-outside -n -tt -l -Q in \
    'tcp dst port 9092 and tcp[tcpflags] & tcp-syn != 0' >"$evidence/$1-syn.txt" 2>"$scratch/$1-tcpdump.log" &
  trace_pid=$!
  for _ in {1..20}; do
    kill -0 "$trace_pid" 2>/dev/null || return 1
    if grep -qs 'listening on' "$scratch/$1-tcpdump.log"; then return; fi
    sleep .1
  done
  return 1
}

client metadata "$bootstrap" >"$evidence/registration-metadata.json"
[[ $(offset) == 0 ]]
stop_broker
start_broker "$changed"
trace positive
client produce-allowed "$changed" >"$evidence/positive-produce.json"
stop_trace
positive_syn=$(awk 'NF { count++ } END { print count+0 }' "$evidence/positive-syn.txt")
[[ "$positive_syn" -gt 0 ]]
before=$(offset)
printf '%s\n' "$before" >"$evidence/before-offset.txt"
[[ "$before" == 1 ]]

kubectl -n "$namespace" apply -f "$evidence/rendered-restricted.yaml" >/dev/null
kubectl -n "$namespace" get networkpolicy "$workload" -o json |
  jq 'del(.metadata.annotations,.metadata.managedFields)' >"$evidence/restricted-policy.json"
jq -e --arg approved "$approved/32" '[.spec.egress[] | .to[]? | .ipBlock.cidr? | select(. != null)] == [$approved]' \
  "$evidence/restricted-policy.json" >/dev/null
sleep 3
[[ $(kubectl -n "$namespace" get pod "$pod" -o jsonpath='{.metadata.uid}') == "$pod_uid" ]]
client metadata "$changed" >"$evidence/restricted-metadata.json"
trace restricted
client produce-denied "$changed" >"$evidence/restricted-produce.json"
stop_trace
restricted_syn=$(awk 'NF { count++ } END { print count+0 }' "$evidence/restricted-syn.txt")
after=$(offset)
printf '%s\n' "$after" >"$evidence/after-offset.txt"
[[ "$restricted_syn" == 0 && "$after" == "$before" ]]
node_net iptables-save -c | awk -v ip="$pod_ip" 'index($0,ip) && /KUBE-(POD-FW|NWPLCY)/' >"$evidence/cni-pod-rules.txt"
[[ -s "$evidence/cni-pod-rules.txt" ]]

jq -n --arg source "$source_sha" --arg archive "$archive_sha" --arg image "$image_digest" \
  --arg pod "$pod_uid" --arg ip "$pod_ip" --arg approved "$approved/32" --argjson before "$before" \
  --argjson after "$after" --argjson positiveSyn "$positive_syn" --argjson restrictedSyn "$restricted_syn" \
  --slurpfile initial "$evidence/registration-metadata.json" --slurpfile control "$evidence/positive-produce.json" \
  --slurpfile metadata "$evidence/restricted-metadata.json" --slurpfile result "$evidence/restricted-produce.json" \
  '{sourceSha:$source,imageArchiveSha256:$archive,imageDigest:$image,podUid:$pod,podIp:$ip,approvedCidr:$approved,
    cni:"K3s flannel host-gw with embedded kube-router NetworkPolicy controller",
    initialMetadata:$initial[0],positiveControl:$control[0],restrictedMetadata:$metadata[0],restrictedProduce:$result[0],
    positiveOutsideSyn:$positiveSyn,restrictedOutsideSyn:$restrictedSyn,beforeOffset:$before,finalOffset:$after,passed:true}' \
  >"$evidence/kaf02-deployment-result.json"
