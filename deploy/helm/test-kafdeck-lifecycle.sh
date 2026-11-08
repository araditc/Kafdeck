#!/usr/bin/env bash
set -euo pipefail

image="${1:?Pass the already-built amd64 candidate image}"
evidence="${2:?Pass the evidence output file}"
[[ "$(uname -sm)" == 'Linux x86_64' ]]
scratch="$(mktemp -d)"
cluster="w81-helm-${RANDOM}-$$"
registry="$cluster-registry"
namespace=w81-fixture
release=w81-fixture
workload="$release-kafdeck"
hostname=kafdeck.example.test
node_image='kindest/node:v1.34.3@sha256:08497ee19eace7b4b5348db5c6a1591d7752b164530a36f855cb0f2bdcbadd48'
export KUBECONFIG="$scratch/kubeconfig"
export PATH="$scratch:$PATH"
forward_pid=

cleanup() {
    if [[ -n "$forward_pid" ]]; then kill "$forward_pid" 2>/dev/null || true; fi
    if command -v kind >/dev/null; then kind delete cluster --name "$cluster" >/dev/null 2>&1 || true; fi
    docker rm -f "$registry" >/dev/null 2>&1 || true
    rm -rf -- "$scratch"
}
trap cleanup EXIT
trap 'kubectl -n "$namespace" get pods,pvc 2>/dev/null || true; kubectl -n "$namespace" get events --sort-by=.metadata.creationTimestamp 2>/dev/null || true' ERR

fetch() {
    curl --fail --silent --show-error --location --max-time 90 -o "$scratch/$1" "$2"
    printf '%s  %s\n' "$3" "$scratch/$1" | sha256sum --check -
}
fetch helm.tar.gz https://get.helm.sh/helm-v3.21.4-linux-amd64.tar.gz 61f88ab166748cb19604d7884cb100ae9ccb13804ddeb98e08af167eacbb6a14
tar -xzf "$scratch/helm.tar.gz" -C "$scratch" --strip-components=1 linux-amd64/helm
fetch kind https://github.com/kubernetes-sigs/kind/releases/download/v0.31.0/kind-linux-amd64 eb244cbafcc157dff60cf68693c14c9a75c4e6e6fedaf9cd71c58117cb93e3fa
fetch kubectl https://dl.k8s.io/release/v1.34.3/bin/linux/amd64/kubectl ab60ca5f0fd60c1eb81b52909e67060e3ba0bd27e55a8ac147cbc2172ff14212
chmod +x "$scratch/kind" "$scratch/kubectl"
helm package deploy/helm/kafdeck --destination "$scratch"
charts=("$scratch"/kafdeck-*.tgz)
[[ "${#charts[@]}" == 1 && -f "${charts[0]}" ]]
chart="${charts[0]}"

registry_image="$(jq -er .localTestRegistryImage .github/release/publication-tooling.json)"
[[ "$registry_image" =~ ^registry:3\.0\.0@sha256:[0-9a-f]{64}$ ]]
docker run -d --name "$registry" --publish 127.0.0.1::5000 "$registry_image" >/dev/null
endpoint="$(docker port "$registry" 5000/tcp)"
[[ "$endpoint" =~ ^127\.0\.0\.1:[0-9]+$ ]]
curl --fail --silent --show-error --retry 10 --retry-connrefused --retry-max-time 20 \
    --max-time 3 "http://$endpoint/v2/" >/dev/null
docker image tag "$image" "$endpoint/kafdeck:fixture"
docker image push --platform linux/amd64 "$endpoint/kafdeck:fixture"
curl --fail --silent --show-error --max-time 10 \
    --header 'Accept: application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json' \
    "http://$endpoint/v2/kafdeck/manifests/fixture" > "$scratch/manifest.json"
image_id="$(docker image inspect --format '{{.Id}}' "$image")"
[[ "$(jq -er .config.digest "$scratch/manifest.json")" == "$image_id" ]]
manifest_digest="sha256:$(sha256sum "$scratch/manifest.json" | cut -d' ' -f1)"

# The local HTTP registry is reachable only from the disposable host/Kind
# network. The chart still deploys the application by its exact digest.
kind create cluster --name "$cluster" --image "$node_image" --wait 120s
node="$(kind get nodes --name "$cluster")"
docker network connect kind "$registry"
registry_alias="localhost:${endpoint#*:}"
registry_config="/etc/containerd/certs.d/$registry_alias"
docker exec "$node" mkdir -p "$registry_config"
printf '[host."http://%s:5000"]\n' "$registry" | docker exec -i "$node" tee "$registry_config/hosts.toml" >/dev/null
docker exec "$node" install -d -m 0700 -o 1654 -g 1654 /var/local/kafdeck-w81-state

kubectl create namespace "$namespace"
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
  name: w81-fixture-state
spec:
  storageClassName: ""
  capacity: { storage: 128Mi }
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  claimRef: { namespace: $namespace, name: fixture-kept-state }
  hostPath: { path: /var/local/kafdeck-w81-state, type: Directory }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: fixture-kept-state, namespace: $namespace }
spec:
  storageClassName: ""
  volumeName: w81-fixture-state
  accessModes: [ReadWriteOnce]
  resources: { requests: { storage: 128Mi } }
EOF
openssl req -x509 -newkey rsa:2048 -noenc -days 1 -subj "/CN=$hostname" \
    -addext "subjectAltName=DNS:$hostname" -keyout "$scratch/tls.key" -out "$scratch/tls.crt" 2>/dev/null
openssl rand -hex 24 | tr -d '\n' > "$scratch/tls-password"
openssl pkcs12 -export -inkey "$scratch/tls.key" -in "$scratch/tls.crt" \
    -out "$scratch/tls.pfx" -passout "file:$scratch/tls-password"
kubectl -n "$namespace" create secret generic fixture-tls \
    --from-file="tls.pfx=$scratch/tls.pfx" --from-file="tls-password=$scratch/tls-password"
kubectl -n "$namespace" create secret generic fixture-credentials
kubectl -n "$namespace" create configmap fixture-config --from-literal='appsettings.Production.json={}'

jq -n --arg repo "$registry_alias/kafdeck" --arg digest "$manifest_digest" --arg hostname "$hostname" --arg namespace "$namespace" \
    '{image:{repository:$repo,digest:$digest},publicHost:$hostname,operatorConfigMap:"fixture-config",credentialsSecret:"fixture-credentials",tlsSecret:"fixture-tls",oidc:{issuer:"https://idp.example.test/realms/kafdeck",clientId:"fixture"},kafka:{clusterId:"fixture",bootstrapServers:["broker.example.test:9093"],protocol:"Ssl"},persistence:{provider:"Sqlite",existingClaim:"fixture-kept-state"},network:{ingressNamespaces:[$namespace],egress:[{cidr:"192.0.2.10/32",port:443},{cidr:"192.0.2.11/32",port:9093}]}}' > "$scratch/values.json"

probe_https() {
    local port=''
    kubectl -n "$namespace" port-forward --address 127.0.0.1 "service/$workload" :443 > "$scratch/forward.log" 2>&1 &
    forward_pid=$!
    for _ in {1..20}; do
        port="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) .*/\1/p' "$scratch/forward.log")"
        [[ -z "$port" ]] || break
        kill -0 "$forward_pid"
        sleep 1
    done
    [[ "$port" =~ ^[0-9]+$ ]]
    local -a tls=(--fail --silent --show-error --max-time 10 --noproxy '*' --cacert "$scratch/tls.crt" --resolve "$hostname:$port:127.0.0.1")
    curl "${tls[@]}" "https://$hostname:$port/healthz" | jq -e '.status == "ok"' >/dev/null
    curl "${tls[@]}" "https://$hostname:$port/" > "$scratch/index.html"
    grep -qi '<html' "$scratch/index.html"
    kill "$forward_pid"
    wait "$forward_pid" 2>/dev/null || true
    forward_pid=
}
pod_uid() {
    kubectl -n "$namespace" get pods -l "app.kubernetes.io/instance=$release" -o json \
        | jq -er '[.items[] | select(.metadata.deletionTimestamp == null)] | if length == 1 then .[0].metadata.uid else error("Expected one active pod") end'
}
claim_uid() {
    kubectl -n "$namespace" get pvc fixture-kept-state -o jsonpath='{.metadata.uid}'
}
helm_args=(--namespace "$namespace" --wait --timeout 180s --values "$scratch/values.json")
helm install "$release" "$chart" "${helm_args[@]}"
probe_https
installed_pod="$(pod_uid)"
original_claim="$(claim_uid)"
kubectl -n "$namespace" exec "deployment/$workload" -- sh -c 'id -u | grep -qx 1654 && printf %s w81-retained > /var/lib/kafdeck/w81-fixture'

# Upgrade the packaged chart with a changed pod template, forcing an actual
# replacement; this does not claim cross-version application migration.
helm upgrade "$release" "$chart" "${helm_args[@]}" --set 'resources.requests.memory=160Mi'
probe_https
upgraded_pod="$(pod_uid)"
[[ "$installed_pod" != "$upgraded_pod" && "$(claim_uid)" == "$original_claim" ]]
kubectl -n "$namespace" exec "deployment/$workload" -- grep -qx w81-retained /var/lib/kafdeck/w81-fixture
helm history "$release" --namespace "$namespace" --output json \
    | jq -e 'length == 2 and .[0].status == "superseded" and .[1].status == "deployed"' >/dev/null

helm uninstall "$release" --namespace "$namespace" --wait --timeout 120s
[[ "$(claim_uid)" == "$original_claim" ]]
helm install "$release" "$chart" "${helm_args[@]}"
probe_https
[[ "$(pod_uid)" != "$upgraded_pod" && "$(claim_uid)" == "$original_claim" ]]
kubectl -n "$namespace" exec "deployment/$workload" -- grep -qx w81-retained /var/lib/kafdeck/w81-fixture

jq -n --arg source "$(git rev-parse HEAD)" --arg kube "$(kubectl version -o json | jq -er .serverVersion.gitVersion)" \
    --arg kind "$(kind version)" --arg helm "$(helm version --short)" --arg digest "$manifest_digest" \
    --arg chart "$(sha256sum "$chart" | cut -d' ' -f1)" --arg image "$image_id" \
    '{sourceSha:$source,kubernetes:$kube,kind:$kind,helm:$helm,imageDigest:$digest,imageId:$image,chartSha256:$chart,install:true,upgrade:true,uninstall:true,reinstall:true,retainedClaimAndFile:true,verifiedHttps:true,scope:"standalone SQLite pod/PVC and packaged chart lifecycle; no broker, IdP, database restore, or NetworkPolicy enforcement proof",published:false}' > "$evidence"
