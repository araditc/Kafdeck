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
echo "$trusted_root_sha  $media/trusted-root.json" | sha256sum --check --status
rm -f -- "$media/offline-verification.json"
"${COSIGN:-cosign}" verify-blob --bundle "$media/manifest.sigstore.json" \
  --trusted-root "$media/trusted-root.json" \
  --certificate-identity 'https://github.com/araditc/Kafdeck/.github/workflows/v09-prepublication-signing.yml@refs/heads/main' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  --certificate-github-workflow-repository 'araditc/Kafdeck' \
  --certificate-github-workflow-name 'v09-prepublication-signing' \
  --certificate-github-workflow-ref 'refs/heads/main' \
  --certificate-github-workflow-trigger 'workflow_dispatch' \
  --certificate-github-workflow-sha "$policy_sha" "$media/manifest.json"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
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
