#!/bin/sh
# Run locally as root after installing the read-only host registration helper.
set -eu
test "$(id -u)" = 0
test "$#" = 1 || { echo 'Usage: install-agent.sh public-key.pub' >&2; exit 1; }
test -x /usr/local/libexec/kafdeck-host-probe
source_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
/usr/bin/python3 -I - "$1" <<'PY'
import re, sys
from pathlib import Path
assert re.fullmatch(r'(ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp256) [A-Za-z0-9+/=]+(?: [^\r\n]*)?', Path(sys.argv[1]).read_text().strip())
PY
if id kafdeck-agent >/dev/null 2>&1; then
    echo 'kafdeck-agent already exists. Review the installed entry and key before updating.' >&2
    exit 1
fi
/usr/sbin/useradd --system --home-dir /var/lib/kafdeck-agent-ssh --shell /bin/sh kafdeck-agent
install -m 0755 -o root -g root "$source_dir/agent-lifecycle.py" /usr/local/libexec/kafdeck-agent-lifecycle
install -m 0755 -o root -g root "$source_dir/agent-entry.sh" /usr/local/libexec/kafdeck-agent-entry
install -d -m 0755 -o root -g root /var/lib/kafdeck-agent-ssh /var/lib/kafdeck-agent-ssh/.ssh /var/lib/kafdeck-agent/packages
printf 'restrict,command="/usr/local/libexec/kafdeck-agent-entry" %s\n' "$(cat "$1")" > /var/lib/kafdeck-agent-ssh/.ssh/authorized_keys
chmod 0644 /var/lib/kafdeck-agent-ssh/.ssh/authorized_keys
printf 'kafdeck-agent ALL=(root) NOPASSWD: /usr/local/libexec/kafdeck-agent-lifecycle ""\n' > /etc/sudoers.d/kafdeck-agent
chmod 0440 /etc/sudoers.d/kafdeck-agent
/usr/sbin/visudo -cf /etc/sudoers.d/kafdeck-agent
