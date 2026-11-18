#!/usr/bin/env bash
# Disposable CI lab only. Two distinguishable packages of the existing agent.
set -euo pipefail
test "${GITHUB_ACTIONS:-}" = true
lab=$1
agent=$2
cluster=$3
ssh-keygen -q -t ed25519 -N '' -f "$lab/agent-key"
sudo sh tools/broker-host/install-agent.sh "$lab/agent-key.pub"
python3 - "$lab" "$agent" <<'PY'
import hashlib, json, sys, time, zipfile
from pathlib import Path
lab, target = sys.argv[1:]
lab = Path(lab)
packages = {}
origin = Path('tools/kafka-agent/kafdeck-kafka-agent.jar')
for version in ('1.1.0-test.1', '1.1.0-test.2'):
    file = lab / f'{version}.jar'
    with zipfile.ZipFile(origin) as source, zipfile.ZipFile(file, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for entry in source.infolist():
            content = source.read(entry)
            if entry.filename == 'META-INF/MANIFEST.MF':
                content = content.rstrip() + f'\r\nImplementation-Version: {version}\r\n\r\n'.encode()
            archive.writestr(entry, content)
    digest = hashlib.sha256(file.read_bytes()).hexdigest()
    file.rename(lab / f'{digest}.jar')
    packages[version] = digest
now = int(time.time())
(lab / 'agent-targets.json').write_text(json.dumps({'version': 1, 'packages': packages, 'targets': {
  'native-broker-2': {'hostDirectory':target, 'runtimeDirectory':target, 'maintenanceFrom':now-30, 'maintenanceTo':now+1400}
}}))
(lab / 'packages.json').write_text(json.dumps(packages))
PY
sudo install -m 0644 "$lab"/*.jar /var/lib/kafdeck-agent/packages/
sudo install -m 0644 "$lab/agent-targets.json" /etc/kafdeck/agent-targets.json
{
  echo KAFDECK_RUN_V11_AGENT_LIVE=1
  echo KAFDECK_AGENT_BROKER=2
  echo KAFDECK_AGENT_TARGET=native-broker-2
  echo KAFDECK_AGENT_ADAPTER=systemd
  echo "KAFDECK_AGENT_EXPECTED_CLUSTER=$cluster"
  echo KAFDECK_AGENT_BOOTSTRAP=127.0.0.1:19092,127.0.0.1:19094
  echo "KAFDECK_AGENT_KEY=$lab/agent-key"
  echo "KAFDECK_AGENT_INGEST_SECRET_FILE=$lab/agent-token"
  echo "KAFDECK_AGENT_PACKAGE_CATALOG=$lab/packages.json"
} >> "$GITHUB_ENV"
