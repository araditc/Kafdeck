#!/bin/sh
set -eu
case "${SSH_ORIGINAL_COMMAND:-}" in
    kafdeck-host-probe-v1) exec /usr/bin/sudo -n -- /usr/local/libexec/kafdeck-host-probe ;;
    kafdeck-host-logs-v1) exec /usr/bin/sudo -n -- /usr/local/libexec/kafdeck-host-probe --logs-v1 ;;
    *) exit 126 ;;
esac
