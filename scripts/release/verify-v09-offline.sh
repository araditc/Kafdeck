#!/usr/bin/env bash
set -euo pipefail
media="$(realpath "$1")"
policy_sha="$2"
trusted_root_sha="$3"
[[ "$policy_sha" =~ ^[0-9a-f]{40}$ && "$trusted_root_sha" =~ ^[0-9a-f]{64}$ ]]
# This proof must have only loopback, no egress-capable interfaces or routes.
[[ "$(ip -o link show | wc -l)" == 1 ]]
ip -o link show | grep -Eq '^[0-9]+: lo:'
[[ -z "$(ip -4 route show default)" && -z "$(ip -6 route show default)" ]]
rm -f -- "$media/offline-verification.json"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
bash "$script_dir/verify-v09-signature.sh" "$media" "$policy_sha" "$trusted_root_sha"
python3 - "$media" "$policy_sha" "$script_dir" "$trusted_root_sha" <<'PY'
import json, pathlib, sys
sys.path.insert(0, sys.argv[3])
from v09_prepublication import digest, verify
from v09_offline_install import install
directory = pathlib.Path(sys.argv[1])
manifest = verify(directory)
assert manifest['workflowSourceSha'] == sys.argv[2], 'Signed manifest policy identity differs from expected certificate SHA'
installation = install(directory, manifest)
evidence = {'schemaVersion': 1, 'signatureVerified': True, 'mediaVerified': True,
            'networkNamespace': 'loopback-only', 'ipv4DefaultRoute': False, 'ipv6DefaultRoute': False,
            'scope': manifest['scope'], 'sourceSha': manifest['sourceSha'],
            'workflowSourceSha': sys.argv[2], 'manifestSha256': digest(directory / 'manifest.json'),
            'trustedRootSha256': sys.argv[4], 'releaseReady': False, 'installation': installation}
(directory / 'offline-verification.json').write_text(json.dumps(evidence, sort_keys=True, indent=2) + '\n')
PY
