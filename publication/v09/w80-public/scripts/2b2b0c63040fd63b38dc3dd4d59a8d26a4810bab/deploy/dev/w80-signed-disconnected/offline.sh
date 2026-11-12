#!/usr/bin/env bash
set -euo pipefail
private="$1"
evidence="$2"
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
ip link set lo up
[[ "$(ip -o link show | wc -l)" == 1 ]]
ip -o link show | grep -Eq '^[0-9]+: lo:'
[[ -z "$(ip -4 route show default)" && -z "$(ip -6 route show default)" ]]
bash "$root/scripts/release/verify-v09-signature.sh" "$private/signed" \
  01c7553425fbc61ba4a7a21918265c562086ba02 \
  0ba58f6c09c271f664f0f8c1fa62e2065844ca89a0397c0178ed5188f6096724
python3 -B "$root/deploy/dev/w80-signed-disconnected/run.py" "$private" "$evidence"
