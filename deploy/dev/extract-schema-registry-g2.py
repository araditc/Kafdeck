"""Test-only official distribution extraction without Windows symlink privileges."""
import hashlib
import json
import pathlib
import posixpath
import shutil
import sys
import tarfile

archive, destination = map(pathlib.Path, sys.argv[1:3])
expected = 'c87e2ab78f17690769e40b9a1d58369df71afaeba43e6ac2e5af98699c0d804bfbca7f13c47b3aae1cfe749cce4094d7626d808d7144be735040e4685816fa7d'
with archive.open('rb') as stream:
    actual = hashlib.file_digest(stream, 'sha512').hexdigest()
if actual != expected:
    raise SystemExit('Official HTTPS-acquired pinned distribution digest mismatch.')
if destination.exists():
    raise SystemExit('Select a fresh destination; existing files are retained.')
prefix = 'confluent-8.3.2/'
libraries = ['confluent-security/schema-registry', 'schema-registry-plugins', 'confluent-common', 'confluent-telemetry', 'rest-utils', 'schema-registry']
with tarfile.open(archive, 'r:gz') as tar:
    members = {m.name: m for m in tar.getmembers()}
def resolve(member):
    seen = set()
    while member.issym() or member.islnk():
        if member.name in seen or len(seen) >= 16:
            raise ValueError('Archive link cycle refused.')
        seen.add(member.name)
        name = posixpath.normpath(posixpath.join(posixpath.dirname(member.name), member.linkname) if member.issym() else member.linkname)
        if not name.startswith(prefix):
            raise ValueError('Archive link escaped its fixed distribution.')
        member = members[name]
    if not member.isfile() or not 0 <= member.size <= 128 * 1024 * 1024:
        raise ValueError('Archive member type or size refused.')
    return member
selected = {}
for member in members.values():
    relative = member.name.removeprefix(prefix)
    if not member.name.startswith(prefix) or '..' in pathlib.PurePosixPath(relative).parts:
        continue
    if (member.name.endswith('.jar') and any(relative.startswith('share/java/' + lib + '/') for lib in libraries)) or relative == 'etc/schema-registry/log4j2.yaml':
        selected[relative] = resolve(member).name
needed = set(selected.values())
if sum(members[name].size for name in needed) > 768 * 1024 * 1024 or not selected:
    raise ValueError('Selected runtime budget refused.')
destination.mkdir(parents=True)
cache = destination / '.archive-members'
cache.mkdir()
materialized = {}
with tarfile.open(archive, 'r|gz') as tar:
    for member in tar:
        if member.name not in needed:
            continue
        if not member.isfile():
            raise ValueError('Resolved target must be a regular archive member.')
        target = cache / (hashlib.sha256(member.name.encode()).hexdigest() + '.bin')
        with tar.extractfile(member) as source, target.open('xb') as sink:
            shutil.copyfileobj(source, sink, 1024 * 1024)
        materialized[member.name] = target
for relative, source in selected.items():
    target = destination.joinpath(*pathlib.PurePosixPath(relative).parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(materialized[source], target)
print(json.dumps({'distribution': 'Confluent Community 8.3.2', 'sha512': actual, 'runtimeFiles': len(selected), 'symbolicLinksCreated': 0, 'testOnly': True}))
