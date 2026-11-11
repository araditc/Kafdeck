#!/usr/bin/env bash
set -euo pipefail
private="$(realpath "$1")"
evidence="$(realpath "$2")"
root="$(pwd)"
source_sha=01c7553425fbc61ba4a7a21918265c562086ba02
scratch="$private/helm"
mkdir -m700 "$scratch"
cluster="w81-auth-${RANDOM}-$$"
registry="$cluster-registry"
namespace=w81-auth
release=w81-auth
workload="$release-kafdeck"
state=/var/local/kafdeck-w81-state
export KUBECONFIG="$scratch/kubeconfig"
export PATH="$scratch:$PATH"
fixture_pid=
forward_pid=
cleanup() {
  if [[ -n "$forward_pid" ]]; then kill "$forward_pid" 2>/dev/null || true; fi
  if [[ -n "$fixture_pid" ]]; then kill "$fixture_pid" 2>/dev/null || true; wait "$fixture_pid" 2>/dev/null || true; fi
  if command -v kind >/dev/null; then kind delete cluster --name "$cluster" >/dev/null 2>&1 || true; fi
  docker rm -f "$registry" >/dev/null 2>&1 || true
}
trap cleanup EXIT
fetch() {
  curl --fail --silent --show-error --location --max-time 120 -o "$scratch/$1" "$2"
  printf '%s  %s\n' "$3" "$scratch/$1" | sha256sum --check -
}
fetch helm.tar.gz https://get.helm.sh/helm-v3.21.4-linux-amd64.tar.gz 61f88ab166748cb19604d7884cb100ae9ccb13804ddeb98e08af167eacbb6a14
tar -xzf "$scratch/helm.tar.gz" -C "$scratch" --strip-components=1 linux-amd64/helm
fetch kind https://github.com/kubernetes-sigs/kind/releases/download/v0.31.0/kind-linux-amd64 eb244cbafcc157dff60cf68693c14c9a75c4e6e6fedaf9cd71c58117cb93e3fa
fetch kubectl https://dl.k8s.io/release/v1.34.3/bin/linux/amd64/kubectl ab60ca5f0fd60c1eb81b52909e67060e3ba0bd27e55a8ac147cbc2172ff14212
chmod +x "$scratch/kind" "$scratch/kubectl"
signed="$private/signed"
chart="$signed/media/distribution/w81-helm-$source_sha/kafdeck-0.9.0.tgz"
archive="$signed/media/distribution/w81-oci-amd64-$source_sha/kafdeck-linux-amd64.docker.tar.gz"
config_digest="$(jq -er '.content.imageConfigDigests.amd64' "$signed/manifest.json")"
docker load --input "$archive" >/dev/null
registry_image="$(jq -er .localTestRegistryImage .github/release/publication-tooling.json)"
docker run -d --name "$registry" --publish 127.0.0.1::5000 "$registry_image" >/dev/null
endpoint="$(docker port "$registry" 5000/tcp)"
curl --fail --silent --show-error --retry 10 --retry-connrefused --retry-max-time 20 --max-time 3 "http://$endpoint/v2/" >/dev/null
docker tag kafdeck:w81-fixture "$endpoint/kafdeck:fixture"
docker push --platform linux/amd64 "$endpoint/kafdeck:fixture" >/dev/null
curl --fail --silent --show-error --max-time 10 --header 'Accept: application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json' \
  "http://$endpoint/v2/kafdeck/manifests/fixture" > "$scratch/image-manifest.json"
[[ "$(jq -er .config.digest "$scratch/image-manifest.json")" == "$config_digest" ]]
manifest_digest="sha256:$(sha256sum "$scratch/image-manifest.json" | cut -d' ' -f1)"
kind create cluster --name "$cluster" --image 'kindest/node:v1.34.3@sha256:08497ee19eace7b4b5348db5c6a1591d7752b164530a36f855cb0f2bdcbadd48' --wait 120s
node="$(kind get nodes --name "$cluster")"
docker network connect kind "$registry"
registry_alias="localhost:${endpoint#*:}"
docker exec "$node" mkdir -p "/etc/containerd/certs.d/$registry_alias"
printf '[host."http://%s:5000"]\n' "$registry" | docker exec -i "$node" tee "/etc/containerd/certs.d/$registry_alias/hosts.toml" >/dev/null
host="$(docker exec "$node" ip -4 route show default | awk 'NR==1 {print $3}')"
[[ "$host" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]
python3 -B deploy/dev/w81-signed-helm/fixture.py serve "$private" "$host" > "$private/fixture-driver.log" 2>&1 &
fixture_pid=$!
for attempt in $(seq 1 180); do
  [[ ! -f "$private/fixture-ready.json" ]] || break
  if ! kill -0 "$fixture_pid" 2>/dev/null; then
    tail -n20 "$private/fixture-driver.log"
    exit 1
  fi
  sleep 1
done
test -f "$private/fixture-ready.json"
issuer="$(jq -er .issuer "$private/fixture-ready.json")"
client_id="$(jq -er .cliClientId "$private/identity/lab.json")"
generation="$(jq -er .generation "$private/fixture-ready.json")"
kubectl create namespace "$namespace"
docker exec "$node" install -d -m0700 -o1654 -g1654 "$state"
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolume
metadata: {name: w81-auth-state}
spec:
  storageClassName: ""
  capacity: {storage: 128Mi}
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  claimRef: {namespace: $namespace, name: retained-state}
  hostPath: {path: $state, type: Directory}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: retained-state, namespace: $namespace}
spec:
  storageClassName: ""
  volumeName: w81-auth-state
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 128Mi}}
EOF
kubectl -n "$namespace" create secret generic fixture-tls --from-file="tls.pfx=$private/app.p12" --from-file="tls-password=$private/identity/https-password.txt"
kubectl -n "$namespace" create secret generic fixture-credentials --from-file="ca.pem=$private/ca.pem" --from-file="material-digest=$private/material-digest"
kubectl -n "$namespace" create configmap fixture-config --from-file="appsettings.Production.json=$private/settings.json"
jq -n --arg repo "$registry_alias/kafdeck" --arg digest "$manifest_digest" --arg issuer "$issuer" --arg client "$client_id" --arg host "$host" \
  '{image:{repository:$repo,digest:$digest},publicHost:"localhost",operatorConfigMap:"fixture-config",credentialsSecret:"fixture-credentials",tlsSecret:"fixture-tls",oidc:{issuer:$issuer,clientId:$client},kafka:{clusterId:"fixture",bootstrapServers:[($host+":19092")],protocol:"Ssl"},persistence:{provider:"Sqlite",existingClaim:"retained-state"},network:{ingressNamespaces:["w81-auth"],egress:[{cidr:($host+"/32"),port:18444},{cidr:($host+"/32"),port:19092}]}}' > "$scratch/values.json"
mkdir "$private/state-probe"
cp deploy/dev/w81-signed-helm/HelmState.cs "$private/state-probe/Program.cs"
cat > "$private/state-probe/HelmState.csproj" <<EOF
<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><TargetFramework>net10.0</TargetFramework><OutputType>Exe</OutputType><ImplicitUsings>enable</ImplicitUsings><Nullable>enable</Nullable></PropertyGroup><ItemGroup>
<Reference Include="Kafdeck.Infrastructure.Persistence"><HintPath>$private/installed/api-ui/Kafdeck.Infrastructure.Persistence.dll</HintPath></Reference>
<Reference Include="Kafdeck.Modules.Administration"><HintPath>$private/installed/api-ui/Kafdeck.Modules.Administration.dll</HintPath></Reference>
<Reference Include="Kafdeck.Core"><HintPath>$private/installed/api-ui/Kafdeck.Core.dll</HintPath></Reference>
</ItemGroup></Project>
EOF
dotnet build "$private/state-probe/HelmState.csproj" -c Release --output "$private/probe" --nologo --verbosity quiet
cp "$private/probe/HelmState.dll" "$private/installed/api-ui/HelmState.dll"
state_probe() { dotnet exec --depsfile "$private/installed/api-ui/Kafdeck.Api.deps.json" --runtimeconfig "$private/installed/api-ui/Kafdeck.Api.runtimeconfig.json" "$private/installed/api-ui/HelmState.dll" "$@"; }
fence() {
  kubectl -n "$namespace" scale "deployment/$workload" --replicas=0
  kubectl -n "$namespace" wait --for=delete pod -l "app.kubernetes.io/instance=$release" --timeout=120s
  [[ "$(kubectl -n "$namespace" get pods -l "app.kubernetes.io/instance=$release" -o json | jq '.items|length')" == 0 ]]
}
resume() {
  kubectl -n "$namespace" scale "deployment/$workload" --replicas=1
  kubectl -n "$namespace" rollout status "deployment/$workload" --timeout=180s
}
snapshot() {
  fence
  docker cp "$node:$state/mutations.sqlite3" "$private/$1.sqlite3"
  state_probe observe "$private/$1.sqlite3" > "$evidence/state-$1.json"
  resume
}
probe() {
  kubectl -n "$namespace" port-forward --address 127.0.0.1 "service/$workload" :443 > "$scratch/forward.log" 2>&1 &
  forward_pid=$!
  port=
  for attempt in $(seq 1 20); do
    port="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) .*/\1/p' "$scratch/forward.log")"
    [[ -z "$port" ]] || break
    kill -0 "$forward_pid"; sleep 1
  done
  [[ "$port" =~ ^[0-9]+$ ]]
  status="$(curl --silent --show-error --cacert "$private/ca.pem" --max-time 10 --output /dev/null --write-out '%{http_code}' "https://localhost:$port/api/v1/system/info")"
  [[ "$status" == 401 ]]
  python3 -B deploy/dev/w81-signed-helm/fixture.py probe "$private" "$port" "$1" "$evidence/journeys.ndjson"
  kill "$forward_pid"; wait "$forward_pid" 2>/dev/null || true; forward_pid=
}
helm_args=(--namespace "$namespace" --wait --timeout 180s --values "$scratch/values.json")
helm install "$release" "$chart" "${helm_args[@]}"
claim_uid="$(kubectl -n "$namespace" get pvc retained-state -o jsonpath='{.metadata.uid}')"
probe install
snapshot initial
helm upgrade "$release" "$chart" "${helm_args[@]}" --set resources.requests.memory=160Mi
probe upgrade
snapshot upgraded
cmp "$evidence/state-initial.json" "$evidence/state-upgraded.json"
set +e
helm upgrade "$release" "$chart" "${helm_args[@]}" --atomic --timeout 45s --set "image.digest=sha256:$(printf '1%.0s' {1..64})" > "$private/failed-rollout.log" 2>&1
failed_rollout=$?
set -e
[[ "$failed_rollout" != 0 ]]
kubectl -n "$namespace" rollout status "deployment/$workload" --timeout=180s
probe rollback
snapshot rollback
cmp "$evidence/state-initial.json" "$evidence/state-rollback.json"
helm history "$release" -n "$namespace" -o json > "$evidence/helm-history.json"
helm uninstall "$release" --namespace "$namespace" --wait --timeout 120s
[[ "$(kubectl -n "$namespace" get pvc retained-state -o jsonpath='{.metadata.uid}')" == "$claim_uid" ]]
helm install "$release" "$chart" "${helm_args[@]}"
probe reinstall
snapshot reinstalled
cmp "$evidence/state-initial.json" "$evidence/state-reinstalled.json"
fence
cp "$private/initial.sqlite3" "$private/restored.sqlite3"
restore_hash="$(sha256sum "$private/restored.sqlite3" | cut -d' ' -f1)"
jq -n --arg claim "$claim_uid" --arg restore "$restore_hash" '{apiPods:0,fenced:true,retainedClaimUid:$claim,restoredDatabaseSha256:$restore}' > "$evidence/restore-fence.json"
fence_hash="$(sha256sum "$evidence/restore-fence.json" | cut -d' ' -f1)"
docker exec "$node" rm -f "$state/mutations.sqlite3" "$state/mutations.sqlite3-wal" "$state/mutations.sqlite3-shm"
docker cp "$private/restored.sqlite3" "$node:$state/mutations.sqlite3"
docker exec "$node" chown 1654:1654 "$state/mutations.sqlite3"
next_generation="$(cat /proc/sys/kernel/random/uuid)"
jq --arg next "$next_generation" '."Kafdeck:Governance:DeploymentGeneration"=$next' "$private/settings.json" > "$private/restored-settings.json"
kubectl -n "$namespace" create configmap fixture-config --from-file="appsettings.Production.json=$private/restored-settings.json" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$namespace" scale "deployment/$workload" --replicas=1
if kubectl -n "$namespace" rollout status "deployment/$workload" --timeout=45s; then
  probe restored-unadmitted
else
  kubectl -n "$namespace" logs "deployment/$workload" > "$private/unadmitted.log" 2>&1 || true
  grep -q 'Governance deployment generation is not admitted' "$private/unadmitted.log"
  printf '%s\n' '{"phase":"restored-unadmitted","httpReady":false,"governanceGenerationExceptionObserved":true}' >> "$evidence/journeys.ndjson"
fi
fence
docker cp "$node:$state/mutations.sqlite3" "$private/quarantined.sqlite3"
state_probe admit "$private/quarantined.sqlite3" "$generation" "$next_generation" "$fence_hash" "$restore_hash" > "$evidence/state-admitted.json"
docker exec "$node" rm -f "$state/mutations.sqlite3" "$state/mutations.sqlite3-wal" "$state/mutations.sqlite3-shm"
docker cp "$private/quarantined.sqlite3" "$node:$state/mutations.sqlite3"
docker exec "$node" chown 1654:1654 "$state/mutations.sqlite3"
resume
probe restored-admitted
set +e
helm template pg-profile "$chart" --values "$scratch/values.json" --set persistence.provider=PostgreSql --set replicas=2 > "$private/pg-two-app-rejection.log" 2>&1
two_app_rejected=$?
set -e
[[ "$two_app_rejected" != 0 ]]
jq -n --arg source "$source_sha" --arg driver "$(git rev-parse HEAD)" --arg config "$config_digest" --arg manifest "$manifest_digest" \
 --arg chart "$(sha256sum "$chart"|cut -d' ' -f1)" --arg helm "$(helm version --short)" --arg kube "$(kubectl version -o json|jq -r .serverVersion.gitVersion)" \
 --arg restore "$restore_hash" --arg fence "$fence_hash" \
 '{applicationSourceSha:$source,driverSha:$driver,signedChartSha256:$chart,imageConfigDigest:$config,runtimeImageManifestDigest:$manifest,helm:$helm,kubernetes:$kube,profile:"SQLite single API replica",actualIdentityAndKafkaTls:true,exactFixtureRecordEachStage:true,install:true,upgrade:true,failedRolloutRollback:true,uninstallReinstall:true,appOwnedGenerationAndSchemaStateRetained:true,restore:true,unadmittedGenerationDenied:true,owningGateAdmittedOffline:true,fencedApiPods:0,restoreSha256:$restore,fenceSha256:$fence,multiApiHaClaimed:false,networkPolicyEnforcementClaimed:false,published:false}' > "$evidence/sqlite-helm-lifecycle.json"
cleanup
trap - EXIT
jq '.+{ownedKindRegistryFixturesRemoved:true}' "$evidence/sqlite-helm-lifecycle.json" > "$evidence/final.tmp"
mv "$evidence/final.tmp" "$evidence/sqlite-helm-lifecycle.json"
