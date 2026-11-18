#!/usr/bin/env bash
# Disposable GitHub runner only: a real systemd broker, with Docker removed.
set -euo pipefail
test "${GITHUB_ACTIONS:-}" = true
test "$(ps -p 1 -o comm= | xargs)" = systemd
lab="$GITHUB_WORKSPACE/deploy/dev/.generated/v11-host-native"
mkdir -p "$lab"
sudo systemctl stop docker.service docker.socket 2>/dev/null || true
mapfile -t docker_packages < <(dpkg-query -W -f='${binary:Package} ${db:Status-Status}\n' 'docker*' 'moby*' 2>/dev/null | awk '$2=="installed" {print $1}')
if ((${#docker_packages[@]})); then sudo apt-get purge -y "${docker_packages[@]}"; fi
if command -v docker; then echo 'Docker is still present; native acceptance is invalid.' >&2; exit 1; fi
config_fixture="${KAFDECK_NATIVE_CONFIG_FIXTURE:-}"
if [[ "$config_fixture" != 1 ]]; then
  if ! test -x /usr/sbin/sshd; then sudo apt-get update -qq; sudo apt-get install -y openssh-server; fi
  sudo systemctl start ssh
fi
verified=0
for base in https://downloads.apache.org/kafka https://dlcdn.apache.org/kafka https://archive.apache.org/dist/kafka; do
  if curl --fail --location --retry 1 --connect-timeout 15 --max-time 180 "$base/4.3.1/kafka_2.13-4.3.1.tgz" -o "$lab/kafka.tgz"; then
    if printf '%s  %s\n' c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c "$lab/kafka.tgz" | sha512sum --check; then verified=1; break; fi
  fi
done
test "$verified" = 1
tar -xzf "$lab/kafka.tgz" -C "$lab"
export JAVA_HOME="$JAVA_HOME_21_X64"
export KAFKA_HEAP_OPTS='-Xms128m -Xmx384m'
kafka="$lab/kafka_2.13-4.3.1"
cat > "$lab/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:19093
listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093
advertised.listeners=PLAINTEXT://127.0.0.1:19092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
log.dirs=$lab/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF
cluster_id=$("$kafka/bin/kafka-storage.sh" random-uuid)
"$kafka/bin/kafka-storage.sh" format --cluster-id "$cluster_id" --config "$lab/server.properties"
launch() {
  unit=$1
  shift
  sudo systemd-run --unit="$unit" --property="User=$(id -un)" --property="WorkingDirectory=$lab" \
    --setenv="JAVA_HOME=$JAVA_HOME" --setenv="KAFKA_HEAP_OPTS=$KAFKA_HEAP_OPTS" "$@"
}
agent_fixture="${KAFDECK_NATIVE_AGENT_FIXTURE:-}"
if [[ "${KAFDECK_NATIVE_LOG_FIXTURE:-}" == 1 || "$agent_fixture" == 1 ]]; then
  sed -e 's/process.roles=broker,controller/process.roles=broker/' -e 's/node.id=1/node.id=2/' \
    -e 's/:19092/:19094/g' -e 's/,CONTROLLER:\/\/127.0.0.1:19093//' -e 's|/data$|/data-sibling|' \
    "$lab/server.properties" > "$lab/sibling.properties"
  "$kafka/bin/kafka-storage.sh" format --cluster-id "$cluster_id" --config "$lab/sibling.properties"
  if [[ "$agent_fixture" == 1 ]]; then
    agent=/var/lib/kafdeck-agent/native-broker-2
    sudo install -d -m 0755 "$agent"
    openssl rand -hex 32 > "$lab/agent-token"
    chmod 0600 "$lab/agent-token"
    printf '%s\n' 'endpoint=http://127.0.0.1:18090/monitoring/ingest' "tokenFile=$agent/agent-token" 'intervalSeconds=10' 'includeTopicPartitions=false' > "$lab/agent.properties"
    sudo install -m 0600 -o "$(id -un)" "$lab/agent-token" "$agent/agent-token"
    sudo install -m 0644 "$lab/agent.properties" "$agent/agent.properties"
    sudo install -m 0644 /dev/null "$agent/jvm.args"
    launch kafdeck-v11-kafka "$kafka/bin/kafka-server-start.sh" "$lab/server.properties"
    sudo systemd-run --unit=kafdeck-v11-kafka-sibling --property="User=$(id -un)" --property="WorkingDirectory=$lab" \
      --setenv="JAVA_HOME=$JAVA_HOME" --setenv="KAFKA_HEAP_OPTS=$KAFKA_HEAP_OPTS" \
      --setenv="JDK_JAVA_OPTIONS=-Dkafdeck.fixture.preserved=true @$agent/jvm.args" \
      "$kafka/bin/kafka-server-start.sh" "$lab/sibling.properties"
  else
    cat > "$lab/launch-logged" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
marker=$1
timefile=$2
trigger=$3
shift 3
(
  # Stay in the broker unit so journald attaches its trusted unit field.
  deadline=$((SECONDS + 1200))
  until test -e "$trigger"; do
    test "$SECONDS" -lt "$deadline"
    sleep 1
  done
  at=$(date +%s)
  printf 'INFO Kafdeck diagnostics: %s\n' "$marker"
  printf 'INFO Kafdeck diagnostics: password=v11-example-value\n'
  printf '%s\n' "$at" > "$timefile"
) &
exec "$@"
EOF
    chmod 0755 "$lab/launch-logged"
    launch kafdeck-v11-kafka "$lab/launch-logged" native-486-primary "$lab/time-1" "$lab/emit-logs" "$kafka/bin/kafka-server-start.sh" "$lab/server.properties"
    launch kafdeck-v11-kafka-sibling "$lab/launch-logged" native-486-sibling "$lab/time-2" "$lab/emit-logs" "$kafka/bin/kafka-server-start.sh" "$lab/sibling.properties"
  fi
else
  launch kafdeck-v11-kafka "$kafka/bin/kafka-server-start.sh" "$lab/server.properties"
fi
ready=0
for attempt in $(seq 1 60); do
  if timeout 3 bash -c 'echo >/dev/tcp/127.0.0.1/19092' 2>/dev/null; then ready=1; break; fi
  sleep 1
done
test "$ready" = 1
if [[ "$config_fixture" == 1 ]]; then
  {
    echo KAFDECK_RUN_BROKER_CONFIG_LIVE=1
    echo KAFDECK_BROKER_CONFIG_TEST_BOOTSTRAP=127.0.0.1:19092
    echo "KAFDECK_BROKER_CONFIG_TEST_CLUSTER_ID=$cluster_id"
    echo KAFDECK_BROKER_CONFIG_TEST_ENVIRONMENT=Linux-systemd
    echo "KAFDECK_BROKER_CONFIG_TEST_EVIDENCE=$GITHUB_WORKSPACE/artifacts/broker-config"
  } >> "$GITHUB_ENV"
  exit 0
fi
ssh-keygen -q -t ed25519 -N '' -f "$lab/probe-key"
printf '%s\n' '{"version":1,"targets":[{"id":"native-broker-1","adapter":"systemd","unit":"kafdeck-v11-kafka.service"}]}' > "$lab/targets.json"
if [[ "${KAFDECK_NATIVE_LOG_FIXTURE:-}" == 1 ]]; then
  deadline=$((SECONDS + 90))
  until timeout 3 bash -c 'echo >/dev/tcp/127.0.0.1/19094' 2>/dev/null; do
    test "$SECONDS" -lt "$deadline"
    sleep 1
  done
  printf '%s\n' '{"version":1,"targets":[{"id":"native-broker-1","adapter":"systemd","unit":"kafdeck-v11-kafka.service","logs":true},{"id":"native-broker-2","adapter":"systemd","unit":"kafdeck-v11-kafka-sibling.service","logs":true}]}' > "$lab/targets.json"
  {
    echo KAFDECK_RUN_BROKER_LOG_LIVE=1
    echo "KAFDECK_HOST_LOG_CLUSTER_ID=$cluster_id"
    echo KAFDECK_HOST_LOG_TARGET_PREFIX=native-broker
    echo KAFDECK_HOST_LOG_MARKER=native-486-primary
    echo KAFDECK_HOST_LOG_SIBLING=native-486-sibling
  } >> "$GITHUB_ENV"
elif [[ "$agent_fixture" == 1 ]]; then
  deadline=$((SECONDS + 90))
  until timeout 3 bash -c 'echo >/dev/tcp/127.0.0.1/19094' 2>/dev/null; do
    test "$SECONDS" -lt "$deadline"
    sleep 1
  done
  printf '%s\n' '{"version":1,"targets":[{"id":"native-broker-1","adapter":"systemd","unit":"kafdeck-v11-kafka.service"},{"id":"native-broker-2","adapter":"systemd","unit":"kafdeck-v11-kafka-sibling.service"}]}' > "$lab/targets.json"
fi
sudo sh tools/broker-host/install.sh "$lab/targets.json" "$lab/probe-key.pub"
pin=$(sudo ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub | awk '{print $2}')
if [[ "$agent_fixture" == 1 ]]; then
  "$kafka/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:19092,127.0.0.1:19094 \
    --create --topic kafdeck-agent-fixture --partitions 3 --replication-factor 2 --config min.insync.replicas=1
  bash deploy/dev/stage-broker-agent-fixture.sh "$lab" "$agent" "$cluster_id"
fi
{
  echo KAFDECK_RUN_HOST_LIVE=1
  echo KAFDECK_HOST_ADDRESS=127.0.0.1
  echo KAFDECK_HOST_PORT=22
  echo "KAFDECK_HOST_PIN=$pin"
  echo "KAFDECK_HOST_KEY=$lab/probe-key"
  echo KAFDECK_HOST_KAFKA=127.0.0.1:19092
  echo KAFDECK_HOST_COUNT=1
  echo KAFDECK_HOST_ADAPTER=systemd
  echo "KAFDECK_HOST_EVIDENCE=$GITHUB_WORKSPACE/artifacts/host-acceptance"
  echo "KAFDECK_SOURCE_SHA=$(git rev-parse HEAD)"
} >> "$GITHUB_ENV"
mkdir -p artifacts/host-acceptance
python3 - <<'PY'
import json,shutil,subprocess
from pathlib import Path
assert shutil.which('docker') is None
Path('artifacts/host-acceptance/native-environment.json').write_text(json.dumps({
 'pid1': subprocess.check_output(['ps','-p','1','-o','comm='],text=True).strip(),
 'dockerExecutablePresent':False,
 'unit':subprocess.check_output(['systemctl','show','kafdeck-v11-kafka.service','--property=ActiveState','--value'],text=True).strip()
},indent=2))
PY
