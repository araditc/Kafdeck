#!/bin/sh
# Run as root on a host with the v1.1 registration helper already installed.
set -eu
test "$(id -u)" = 0 || { echo 'Run as root.' >&2; exit 1; }
test "$#" = 0 || exit 1
id kafdeck-probe >/dev/null
test -f /etc/sudoers.d/kafdeck-probe
test -x /usr/local/libexec/kafdeck-host-probe
test -x /usr/local/libexec/kafdeck-host-entry
source_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
probe=$(mktemp /usr/local/libexec/.kafdeck-probe.XXXXXX)
entry=$(mktemp /usr/local/libexec/.kafdeck-entry.XXXXXX)
rule=$(mktemp /etc/sudoers.d/.kafdeck-logs.XXXXXX)
trap 'rm -f -- "$probe" "$entry" "$rule"' EXIT
install -m 0755 -o root -g root "$source_dir/probe.py" "$probe"
install -m 0755 -o root -g root "$source_dir/ssh-entry.sh" "$entry"
printf '%s\n' 'kafdeck-probe ALL=(root) NOPASSWD: /usr/local/libexec/kafdeck-host-probe --logs-v1' > "$rule"
chmod 0440 "$rule"
/usr/sbin/visudo -cf "$rule"
mv -fT -- "$probe" /usr/local/libexec/kafdeck-host-probe
mv -fT -- "$rule" /etc/sudoers.d/kafdeck-probe-logs
mv -fT -- "$entry" /usr/local/libexec/kafdeck-host-entry
echo 'Log reader installed. Only targets with logs enabled in the approved catalog can be queried.'
