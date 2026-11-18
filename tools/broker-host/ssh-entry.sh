#!/bin/sh
set -eu
test "${SSH_ORIGINAL_COMMAND:-}" = kafdeck-host-probe-v1 || exit 126
exec /usr/bin/sudo -n -- /usr/local/libexec/kafdeck-host-probe
