"""Publish the separately checksummed Windows supplement from protected main."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import urllib.request
import zipfile

REPO = 'araditc/Kafdeck'
TAG = 'v1.0.0'
ADMITTED = 'd335d278729a9d5ca6e64728a81de74177aa9e86'
ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'artifacts/windows-supplement'


def run(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True, encoding='utf-8').strip()


def api(path):
    return json.loads(run('gh', 'api', f'repos/{REPO}/{path}'))


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def jar_payload(content):
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError('Duplicate JAR entry')
        return {name: hashlib.sha256(archive.read(name)).hexdigest() for name in sorted(names)}


def authority():
    owners = json.loads((ROOT / '.github/release/release-owners.json').read_text())['actors']
    assert os.environ['GITHUB_ACTOR'] in owners
    assert os.environ['GITHUB_TRIGGERING_ACTOR'] in owners
    assert os.environ['GITHUB_REPOSITORY'] == REPO
    assert os.environ['GITHUB_REF'] == 'refs/heads/main'
    assert os.environ['GITHUB_EVENT_NAME'] == 'workflow_dispatch'
    assert os.environ['GITHUB_WORKFLOW_REF'] == REPO + '/.github/workflows/windows-offline-publication.yml@refs/heads/main'
    assert os.environ['OWNER_APPROVAL'] == 'PUBLISH OFFLINE 1.0.0'
    assert run('git', 'rev-parse', 'HEAD') == os.environ['GITHUB_SHA']
    assert api('branches/main')['protected']
    run('git', 'merge-base', '--is-ancestor', ADMITTED, 'HEAD')
    # Compare reviewed installer content independently of subsequent documentation commits.
    run('git', 'diff', '--exit-code', ADMITTED, 'HEAD', '--', 'deploy/windows', 'tools/kafka-agent', 'docs/operator/windows-offline-install.md', 'docs/operator/windows-offline-install.fa.md', 'docs/operator/windows-offline-upgrade.md', 'docs/operator/windows-offline-upgrade.fa.md')


def download(row):
    target = OUT / 'vendor' / row['file']
    with urllib.request.urlopen(row['downloadUrl'], timeout=180) as source, target.open('wb') as destination:
        shutil.copyfileobj(source, destination)
    assert target.stat().st_size == row['bytes'] and sha(target) == row['sha256'], 'Vendor bytes changed: ' + row['file']
    print('Verified vendor ' + row['file'], flush=True)


def main():
    authority()
    OUT.mkdir(parents=True)
    (OUT / 'vendor').mkdir()
    rows = json.loads((ROOT / 'publication/v1-offline/vendors.json').read_text())
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(download, rows))
    (OUT / 'vendor/vendor-manifest.json').write_text(json.dumps(rows[:-1], indent=2), encoding='utf-8')
    with zipfile.ZipFile(OUT / 'vendor/java21.zip') as archive:
        archive.extractall(OUT / 'jdk')
    jdk = next((OUT / 'jdk').iterdir())
    native_name = 'media__native__win-x64__kafdeck-api-ui-1.0.0-win-x64.tar.gz'
    run('gh', 'release', 'download', TAG, '--repo', REPO, '--pattern', native_name, '--dir', str(OUT))
    run('python', '-X', 'utf8', 'scripts/package/build-windows-offline.py', '--vendor-dir', str(OUT / 'vendor'), '--native-archive', str(OUT / native_name), '--jdk', str(jdk), '--output', str(OUT / 'package'))
    package = OUT / 'package'
    run('powershell.exe', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', str(package / 'bundle/Install-Kafdeck.ps1'), '-VerifyOnly')
    original = json.loads((ROOT / 'docs/implementation/evidence/v1.0-release-published-20261117.json').read_text())
    release = api('releases/tags/' + TAG)
    assert not release['draft'] and not release['prerelease']
    assert api('git/ref/tags/' + TAG)['object']['sha'] == original['source']
    remote = {row['name']: row['digest'] for row in release['assets']}
    assert all(remote.get(name) == 'sha256:' + digest for name, digest in original['remoteAssetsVerified'].items())
    assets = [package / 'kafdeck-offline-1.0.0-win-x64.zip', package / 'kafdeck-offline-1.0.0-win-x64.zip.sha256']
    for name in ('windows-offline-install.md', 'windows-offline-install.fa.md', 'windows-offline-upgrade.md', 'windows-offline-upgrade.fa.md'):
        target = package / name
        shutil.copyfile(ROOT / 'docs/operator' / name, target)
        assets.append(target)
    manifest = json.loads((package / 'bundle/bundle-manifest.json').read_text())
    proof = {
        'release': TAG, 'kind': 'supplementary-offline-installer', 'installerSource': os.environ['GITHUB_SHA'],
        'admittedInstallerSource': ADMITTED, 'nativeSource': manifest['nativeRuntimeCommit'],
        'nativeManifestSha256': manifest['nativeManifestSha256'], 'nativeArchiveSha256': manifest['nativeArchiveSha256'],
        'signatureScope': 'original-native-distribution-only', 'offlineManifestSha256': sha(package / 'bundle/bundle-manifest.json'),
        'filesVerified': len(manifest['files']), 'vendorInventorySha256': sha(ROOT / 'publication/v1-offline/vendors.json'),
        'upgradeWindowsRun': 'https://github.com/' + REPO + '/actions/runs/44169318391',
        'publicationRun': 'https://github.com/' + REPO + '/actions/runs/' + os.environ['GITHUB_RUN_ID'],
        'assets': {path.name: sha(path) for path in assets},
    }
    # Preserve the already published agent bytes and verify them by download.
    agent_names = ('kafdeck-kafka-agent-java21.zip', 'kafdeck-kafka-agent-java21.zip.sha256')
    for name in agent_names:
        assert name in remote
        run('gh', 'release', 'download', TAG, '--repo', REPO, '--pattern', name, '--dir', str(package))
        proof['assets'][name] = sha(package / name)
        assert remote[name] == 'sha256:' + proof['assets'][name]
    with zipfile.ZipFile(package / agent_names[0]) as archive:
        published_jar = archive.read('kafdeck-kafka-agent.jar')
    rebuilt_jar = package / 'bundle/payload/agent/kafdeck-kafka-agent.jar'
    published_payload = jar_payload(published_jar)
    assert published_payload == jar_payload(rebuilt_jar.read_bytes()), 'Agent JAR content differs'
    proof['agentVerification'] = {
        'publishedJarSha256': hashlib.sha256(published_jar).hexdigest(),
        'rebuiltJarSha256': sha(rebuilt_jar),
        'verifiedEntries': published_payload,
    }
    provenance = package / 'windows-offline-1.0.0-provenance.json'
    provenance.write_text(json.dumps(proof, indent=2) + '\n', encoding='utf-8')
    assets.append(provenance)
    expected = {path.name: sha(path) for path in assets}
    authority()
    for path in assets:
        if path.name in remote:
            assert remote[path.name] == 'sha256:' + expected[path.name], 'Existing asset differs'
        else:
            run('gh', 'release', 'upload', TAG, '--repo', REPO, str(path))
        print('Published ' + path.name, flush=True)
    check = package / 'remote-verification'
    check.mkdir()
    for name, digest in expected.items():
        run('gh', 'release', 'download', TAG, '--repo', REPO, '--pattern', name, '--dir', str(check))
        assert sha(check / name) == digest, 'Remote download mismatch'
    release = api('releases/tags/' + TAG)
    remote = {row['name']: row['digest'] for row in release['assets']}
    for name, digest in {**original['remoteAssetsVerified'], **proof['assets'], **expected}.items():
        assert remote[name] == 'sha256:' + digest
    receipt = {'release': release['html_url'], 'originalAssetsPreserved': len(original['remoteAssetsVerified']), 'supplementaryAssetsVerified': {**proof['assets'], **expected}, 'totalAssets': len(release['assets']), 'publisherCommit': os.environ['GITHUB_SHA'], 'publicationRun': proof['publicationRun']}
    (OUT / 'publication-receipt.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(receipt), flush=True)


if __name__ == '__main__':
    main()
