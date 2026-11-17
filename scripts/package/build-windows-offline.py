"""Build Windows offline installation/upgrade media from the signed native release."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import urllib.parse
import zipfile

REPO = Path(__file__).resolve().parents[2]
SOURCE = '0617a87ddc176b8255ca1452fc53c097008d9a51'
VERSION = '1.0.0'
NATIVE = 'native/win-x64/kafdeck-api-ui-1.0.0-win-x64.tar.gz'


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def repack(source, destination, select=None):
    with zipfile.ZipFile(source) as archive, zipfile.ZipFile(destination, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as target:
        files = [row for row in archive.infolist() if not row.is_dir()]
        roots = {row.filename.split('/')[0] for row in files}
        if len(roots) != 1:
            raise ValueError('Unexpected vendor archive root')
        prefix = next(iter(roots)) + '/'
        for row in files:
            name = row.filename[len(prefix):]
            if not name or '..' in Path(name).parts or name.startswith('/'):
                raise ValueError('Unsafe vendor archive path')
            if select is None or select(name):
                target.writestr(name, archive.read(row))


def build(args):
    signature = REPO / 'docs/implementation/evidence/v1.0-signature'
    manifest = json.loads((signature / 'manifest.json').read_text())
    native = next(row for row in manifest['files'] if row['path'] == NATIVE)
    if sha(args.native_archive) != native['sha256']:
        raise ValueError('Native archive differs from the signed stable manifest')
    vendors = json.loads((args.vendor_dir / 'vendor-manifest.json').read_text())
    for row in vendors:
        row['resolvedUrl'] = urllib.parse.urlsplit(row['resolvedUrl'])._replace(query='', fragment='').geturl()
        if sha(args.vendor_dir / row['file']) != row['sha256']:
            raise ValueError('Vendor medium changed: ' + row['file'])
    bundle = args.output / 'bundle'
    if bundle.exists():
        raise ValueError('Select a fresh output directory to prevent stale package files')
    media = bundle / 'media'
    payload = bundle / 'payload'
    media.mkdir(parents=True)
    repack(args.vendor_dir / 'java21.zip', media / 'java21.zip')
    repack(args.vendor_dir / 'keycloak.zip', media / 'keycloak.zip')
    repack(args.vendor_dir / 'postgresql17.zip', media / 'postgresql17.zip',
           lambda name: name.startswith(('bin/', 'lib/', 'share/')) or name in ('server_license.txt', 'commandlinetools_3rd_party_licenses.txt'))
    for name in ('vc_redist.x64.exe', 'winsw.exe'):
        shutil.copyfile(args.vendor_dir / name, media / name)
    shutil.copyfile(args.vendor_dir / 'winsw-LICENSE.txt', bundle / 'WinSW-LICENSE.txt')
    (payload / 'app').mkdir(parents=True)
    with tarfile.open(args.native_archive) as archive:
        archive.extractall(payload / 'app', filter='data')
    if not (payload / 'app/Kafdeck.Api.exe').is_file():
        raise ValueError('Native archive layout changed')
    # Build the source-controlled agent reproducibly with the supplied Java 21 JDK.
    agent = payload / 'agent'
    agent.mkdir()
    subprocess.run(['python', str(REPO / 'tools/kafka-agent/build.py'), '--jdk', str(args.jdk), '--output', str(agent / 'kafdeck-kafka-agent.jar')], check=True)
    shutil.copyfile(REPO / 'tools/kafka-agent/agent.properties.example', agent / 'agent.properties.example')
    shutil.copytree(REPO / 'tools/kafka-agent', agent / 'source', ignore=shutil.ignore_patterns('__pycache__'))
    shutil.copytree(REPO / 'deploy/windows', payload / 'scripts')
    docs = payload / 'docs'
    docs.mkdir()
    for name in ('windows-offline-install.md', 'windows-offline-install.fa.md', 'windows-offline-upgrade.md', 'windows-offline-upgrade.fa.md', 'kafka-agent.md', 'kafka-monitoring.md', 'kafka-agent-java21.fa.md', 'windows-server-monitoring-advanced.fa.md'):
        shutil.copyfile(REPO / 'docs/operator' / name, docs / name)
    for source, target in (('Install-Offline.ps1', 'Install-Kafdeck.ps1'), ('Upgrade-Offline.ps1', 'Upgrade-Kafdeck.ps1')):
        shutil.copyfile(REPO / 'deploy/windows' / source, bundle / target)
    for language, name in (('', 'README.md'), ('.fa', 'README.fa.md')):
        text = (docs / ('windows-offline-install' + language + '.md')).read_text(encoding='utf-8-sig')
        for target in docs.glob('*.md'):
            text = text.replace('](' + target.name + ')', '](payload/docs/' + target.name + ')')
        (bundle / name).write_text(text, encoding='utf-8')
    shutil.copyfile(REPO / 'LICENSE', bundle / 'LICENSE')
    shutil.copytree(signature, bundle / 'native-signature')
    shutil.copyfile(args.native_archive, media / Path(NATIVE).name)
    notices = '''Included components
Microsoft OpenJDK 21.0.13.0.2, GPLv2 with Classpath Exception. License/legal files in media/java21.zip.
Source https://github.com/microsoft/openjdk-jdk21u/tree/release/jdk-21.0.13.0.2_1
PostgreSQL 17.12, license and third-party notices in media/postgresql17.zip.
Source https://www.postgresql.org/ftp/source/v17.12/
Keycloak 26.8.1, Apache-2.0 and included upstream notices in media/keycloak.zip.
Source https://github.com/keycloak/keycloak/tree/26.8.1
WinSW 2.12.0, MIT. Included WinSW-LICENSE.txt.
Source https://github.com/winsw/winsw/tree/v2.12.0
Microsoft Visual C++ Redistributable, original Microsoft-signed installer.
Terms https://visualstudio.microsoft.com/license-terms/
Kafdeck 1.0.0, Apache-2.0, included LICENSE and agent source.
The native archive is covered by native-signature/manifest.json and its existing Sigstore bundle.
This supplementary installer has a separate SHA256 inventory and is not covered by that native distribution signature.
'''
    (bundle / 'THIRD-PARTY-NOTICES.txt').write_text(notices, encoding='utf-8')
    files = sorted(path for path in bundle.rglob('*') if path.is_file())
    if any(path.suffix.lower() in ('.pfx', '.key', '.pem', '.cer', '.dpapi', '.sql', '.sqlite', '.db') for path in files):
        raise ValueError('Private deployment data in package')
    result = {
        'format': 1, 'version': VERSION, 'upgradeLayout': 'windows-services-v1',
        'installerCommit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
        'nativeRuntimeCommit': SOURCE, 'nativeArchiveSha256': native['sha256'],
        'nativeManifestSha256': sha(signature / 'manifest.json'),
        'deploymentMode': 'windows-services', 'signatureScope': 'native-distribution',
        'vendors': vendors,
        'files': [{'path': path.relative_to(bundle).as_posix(), 'bytes': path.stat().st_size, 'sha256': sha(path)} for path in files],
    }
    (bundle / 'bundle-manifest.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    target = args.output / 'kafdeck-offline-1.0.0-win-x64.zip'
    with zipfile.ZipFile(target, 'w') as archive:
        for path in sorted(bundle.rglob('*')):
            if path.is_file():
                archive.write(path, path.relative_to(bundle).as_posix(), compress_type=zipfile.ZIP_STORED if path.suffix in ('.zip', '.gz') else zipfile.ZIP_DEFLATED, compresslevel=6)
    with zipfile.ZipFile(target) as archive:
        for row in result['files']:
            if hashlib.sha256(archive.read(row['path'])).hexdigest() != row['sha256']:
                raise ValueError('Final archive integrity failure')
    target.with_suffix('.zip.sha256').write_text(sha(target) + '  ' + target.name + '\n', encoding='ascii')
    print(json.dumps({'archive': str(target), 'sha256': sha(target), 'bytes': target.stat().st_size, 'verifiedFiles': len(files)}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vendor-dir', type=Path, required=True)
    parser.add_argument('--native-archive', type=Path, required=True)
    parser.add_argument('--jdk', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    build(parser.parse_args())
