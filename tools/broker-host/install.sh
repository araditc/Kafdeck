#!/bin/sh
# Run locally as root. Arguments: approved catalog JSON and public SSH key.
set -eu
test "$(id -u)" = 0 || { echo 'Run as root.' >&2; exit 1; }
test "$#" = 2 || { echo 'Usage: install.sh catalog.json public-key.pub' >&2; exit 1; }
source_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
test -x /usr/bin/python3 && test -x /usr/bin/sudo
/usr/bin/python3 -I - "$1" "$2" <<'PY'
import json,re,sys
from pathlib import Path
c=json.loads(Path(sys.argv[1]).read_text())
assert c['version']==1 and 1<=len(c['targets'])<=32
names=set()
for t in c['targets']:
    assert re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}',t['id']) and t['id'] not in names
    names.add(t['id'])
    fields=['unit'] if t['adapter']=='systemd' else ['project','service'] if t['adapter']=='compose' else []
    assert fields
    for field in fields: assert re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}',t[field])
    if t['adapter']=='systemd': assert t['unit'].endswith('.service')
key=Path(sys.argv[2]).read_text().strip()
assert re.fullmatch(r'(ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp256) [A-Za-z0-9+/=]+(?: [^\r\n]*)?',key)
PY
# Never take over an existing account or replace its authorized keys.
if id kafdeck-probe >/dev/null 2>&1; then
    echo 'kafdeck-probe already exists; review/update its approved catalog manually.' >&2
    exit 1
fi
/usr/sbin/useradd --system --create-home --home-dir /var/lib/kafdeck-probe --shell /bin/sh kafdeck-probe
install -d -m 0755 -o root -g root /usr/local/libexec /etc/kafdeck
install -m 0755 -o root -g root "$source_dir/probe.py" /usr/local/libexec/kafdeck-host-probe
install -m 0755 -o root -g root "$source_dir/ssh-entry.sh" /usr/local/libexec/kafdeck-host-entry
install -m 0644 -o root -g root "$1" /etc/kafdeck/host-targets.json
install -d -m 0755 -o root -g root /var/lib/kafdeck-probe/.ssh
printf 'restrict,command="/usr/local/libexec/kafdeck-host-entry" %s\n' "$(cat "$2")" > /var/lib/kafdeck-probe/.ssh/authorized_keys
chown root:root /var/lib/kafdeck-probe /var/lib/kafdeck-probe/.ssh/authorized_keys
chmod 0755 /var/lib/kafdeck-probe
chmod 0644 /var/lib/kafdeck-probe/.ssh/authorized_keys
printf 'kafdeck-probe ALL=(root) NOPASSWD: /usr/local/libexec/kafdeck-host-probe ""\n' > /etc/sudoers.d/kafdeck-probe
chmod 0440 /etc/sudoers.d/kafdeck-probe
/usr/sbin/visudo -cf /etc/sudoers.d/kafdeck-probe
echo 'Read-only host probe installed. No broker service was changed or restarted.'
