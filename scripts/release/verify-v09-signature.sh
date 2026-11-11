#!/usr/bin/env bash
set -euo pipefail
media="$(realpath "$1")"
policy_sha="$2"
trusted_root_sha="$3"
[[ "$policy_sha" =~ ^[0-9a-f]{40}$ && "$trusted_root_sha" =~ ^[0-9a-f]{64}$ ]]
echo "$trusted_root_sha  $media/trusted-root.json" | sha256sum --check --status
"${COSIGN:-cosign}" verify-blob --bundle "$media/manifest.sigstore.json" \
  --trusted-root "$media/trusted-root.json" \
  --certificate-identity 'https://github.com/araditc/Kafdeck/.github/workflows/v09-prepublication-signing.yml@refs/heads/main' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  --certificate-github-workflow-repository 'araditc/Kafdeck' \
  --certificate-github-workflow-name 'v09-prepublication-signing' \
  --certificate-github-workflow-ref 'refs/heads/main' \
  --certificate-github-workflow-trigger 'workflow_dispatch' \
  --certificate-github-workflow-sha "$policy_sha" "$media/manifest.json"
