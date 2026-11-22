"""Build one self-contained Arad ITC EXE from explicitly hash-admitted offline media.

No network fetch of vendor binaries. dotnet restore is a build-host operation only.
The native distribution signature remains scoped to its original native files.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[2]


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def safe(name):
    if '\\' in name or name.startswith('/') or ':' in name:
        raise ValueError('Unsafe media path')
    parts = PurePosixPath(name).parts
    if not parts or name != '/'.join(parts) or any(p in ('.', '..') or p.endswith((' ', '.')) for p in parts):
        raise ValueError('Unsafe media path')


def verify(archive):
    seen = set()
    for entry in archive.infolist():
        safe(entry.filename)
        if entry.filename.casefold() in seen or (entry.external_attr >> 16) & 0o170000 == 0o120000:
            raise ValueError('Duplicate path or link in offline medium')
        seen.add(entry.filename.casefold())
    manifest = json.loads(archive.read('bundle-manifest.json'))
    if manifest['format'] != 1 or manifest['deploymentMode'] != 'windows-services' or not re.fullmatch(r'\d+\.\d+\.\d+', manifest['version']):
        raise ValueError('Unsupported offline media contract')
    expected = {'bundle-manifest.json'}
    for row in manifest['files']:
        safe(row['path'])
        if row['path'] in expected:
            raise ValueError('Duplicate inventory entry')
        expected.add(row['path'])
        info = archive.getinfo(row['path'])
        with archive.open(info) as stream:
            if info.file_size != row['bytes'] or hashlib.file_digest(stream, 'sha256').hexdigest() != row['sha256']:
                raise ValueError('Offline inventory mismatch: ' + row['path'])
    if set(archive.namelist()) != expected:
        raise ValueError('Unlisted files in offline medium')
    return manifest


def build(args):
    from PIL import Image  # Check the build-only prerequisite before creating any output.
    if not re.fullmatch('[0-9a-f]{64}', args.expected_sha256) or sha(args.media) != args.expected_sha256:
        raise ValueError('Offline medium differs from the explicitly admitted SHA256')
    output = args.output.resolve()
    if output.exists():
        raise ValueError('Use a fresh output directory')
    assets = output / 'assets'
    assets.mkdir(parents=True)
    # Refresh only the installer layer. Original runtime and native signature bytes are preserved.
    # This does not turn old native media into a new feature release.
    with zipfile.ZipFile(args.media) as original:
        manifest = verify(original)
        files = {name: original.read(name) for name in original.namelist()
                 if not name.startswith(('payload/scripts/', 'payload/docs/')) and name not in
                 ('bundle-manifest.json', 'Install-Kafdeck.ps1', 'Upgrade-Kafdeck.ps1', 'README.md', 'README.fa.md')}
    for source in (ROOT / 'deploy/windows').glob('*.ps1'):
        files['payload/scripts/' + source.name] = source.read_bytes()
    for source, target in [('Install-Offline.ps1', 'Install-Kafdeck.ps1'), ('Upgrade-Offline.ps1', 'Upgrade-Kafdeck.ps1')]:
        files[target] = (ROOT / 'deploy/windows' / source).read_bytes()
    for name in ('windows-offline-install', 'windows-offline-upgrade', 'windows-graphical-setup', 'kafka-agent', 'kafka-monitoring', 'kafka-agent-java21', 'windows-server-monitoring-advanced'):
        for suffix in ('.md', '.fa.md'):
            path = ROOT / 'docs/operator' / (name + suffix)
            if path.exists():
                files['payload/docs/' + path.name] = path.read_bytes()
    for suffix in ('.md', '.fa.md'):
        text = (ROOT / 'docs/operator' / ('windows-graphical-setup' + suffix)).read_text(encoding='utf-8-sig')
        for target in files:
            if target.startswith('payload/docs/'):
                text = text.replace('](' + Path(target).name + ')', '](' + target + ')')
        text = text.replace('](v0.9-dashboard-guide.md)', '](https://github.com/araditc/Kafdeck/blob/main/docs/operator/v0.9-dashboard-guide.md)')
        files['README' + suffix] = text.encode('utf-8')
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    manifest.update(installerCommit=commit, installerWorkingTreeModified=bool(subprocess.check_output(
        ['git', 'status', '--porcelain', '--', 'deploy/windows', 'scripts/package', 'docs/operator'], cwd=ROOT, text=True).strip()),
        guiPublisher='Arad ITC', guiWebsite='https://arad-itc.com', guiEmail='info@arad-itc.org',
        guiLanguages=['en', 'fa'], guiDefaultLanguage='en', inputArchiveSha256=args.expected_sha256,
        files=[{'path': name, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()} for name, data in sorted(files.items())])
    files['bundle-manifest.json'] = json.dumps(manifest, indent=2).encode()
    payload = assets / 'payload.zip'
    with zipfile.ZipFile(payload, 'w', compression=zipfile.ZIP_STORED) as archive:
        for name, data in sorted(files.items()):
            entry = zipfile.ZipInfo(name, (2000, 1, 1, 0, 0, 0))
            entry.external_attr = 0o100644 << 16
            archive.writestr(entry, data)
    with zipfile.ZipFile(payload) as archive:
        verify(archive)
    metadata = {'Version': manifest['version'], 'PayloadSha256': sha(payload), 'PayloadBytes': payload.stat().st_size}
    (assets / 'metadata.json').write_text(json.dumps(metadata), encoding='utf-8')
    (assets / 'agreement.txt').write_text('Kafdeck — Arad ITC\nhttps://arad-itc.com\ninfo@arad-itc.org\n\n'
        'Kafdeck is provided under Apache License 2.0, without warranty. Included components retain their own licenses. '
        'Accepting setup does not replace or restrict those licenses. See Third-party notices.\n\n'
        + (ROOT / 'LICENSE').read_text(encoding='utf-8'), encoding='utf-8')
    notices = files['THIRD-PARTY-NOTICES.txt'].decode('utf-8-sig')
    notices += '\n\nWinSW license\n' + files['WinSW-LICENSE.txt'].decode('utf-8-sig')
    for medium, entries in (
        ('java21.zip', ['legal/java.base/LICENSE', 'legal/java.base/ADDITIONAL_LICENSE_INFO']),
        ('keycloak.zip', ['LICENSE.txt']),
        ('postgresql17.zip', ['server_license.txt', 'commandlinetools_3rd_party_licenses.txt']),
    ):
        with zipfile.ZipFile(io.BytesIO(files['media/' + medium])) as vendor:
            for entry in entries:
                notices += '\n\n' + medium + ' — ' + entry + '\n' + vendor.read(entry).decode('utf-8-sig', errors='replace')
    project = ROOT / 'deploy/windows/gui/Kafdeck.Setup.csproj'
    subprocess.run(['dotnet', 'restore', str(project), '-r', 'win-x64', '-p:RestoreLockedMode=true'], check=True)
    resolved = json.loads((project.parent / 'obj/project.assets.json').read_text(encoding='utf-8-sig'))
    runtime_notices = []
    for framework in resolved['project']['frameworks'].values():
        for dependency in framework.get('downloadDependencies', []):
            name = dependency['name'].lower()
            if name not in ('microsoft.netcore.app.runtime.win-x64', 'microsoft.windowsdesktop.app.runtime.win-x64'):
                continue
            match = re.fullmatch(r'\[([^,]+),\s*([^\]]+)\]', dependency['version'])
            if not match or match[1] != match[2]:
                raise ValueError('Runtime pack version is not exact')
            package = next((Path(folder) / name / match[1] for folder in resolved['packageFolders']
                            if (Path(folder) / name / match[1]).is_dir()), None)
            if package is None:
                raise ValueError('Resolved runtime pack is missing')
            license_files = [p for p in package.iterdir() if p.name.upper() in ('LICENSE', 'LICENSE.TXT', 'THIRD-PARTY-NOTICES.TXT')]
            if not any(p.name.upper().startswith('LICENSE') for p in license_files):
                raise ValueError('Runtime pack license is missing')
            for path in sorted(license_files):
                notices += '\n\n' + name + ' ' + match[1] + ' — ' + path.name + '\n' + path.read_text(encoding='utf-8-sig')
            runtime_notices.append({'package': name, 'version': match[1], 'licenses': [{'file': p.name, 'sha256': sha(p)} for p in sorted(license_files)]})
    if len(runtime_notices) != 2:
        raise ValueError('Expected the self-contained .NET and Windows Desktop runtime licenses')
    (assets / 'notices.txt').write_text(notices, encoding='utf-8')
    with Image.open(ROOT / 'src/frontend/public/kafdeck-logo.webp') as logo:
        logo.convert('RGBA').save(assets / 'logo.png')
        logo.convert('RGBA').save(assets / 'logo.ico', sizes=[(16, 16), (32, 32), (48, 48), (256, 256)])
    subprocess.run(['dotnet', 'publish', str(ROOT / 'deploy/windows/gui/Kafdeck.Setup.csproj'), '-c', 'Release', '-r', 'win-x64',
                    '--self-contained', 'true', '-m:1', '/nr:false', '-p:SetupAssets=' + str(assets),
                    '-p:Version=' + manifest['version'], '-o', str(output / 'publish')], check=True)
    exe = output / 'publish/Kafdeck.Setup.exe'
    expected = {exe.name}
    if {p.name for p in exe.parent.iterdir()} != expected:
        raise ValueError('Published setup is not a single-file EXE')
    report = {'installerSource': commit, 'version': manifest['version'], 'nativeRuntimeCommit': manifest['nativeRuntimeCommit'],
              'nativeManifestSha256': manifest['nativeManifestSha256'], 'inputArchiveSha256': args.expected_sha256,
              'exeSha256': sha(exe), 'exeBytes': exe.stat().st_size, 'payload': metadata,
              'publisherMetadata': 'Arad ITC', 'authenticode': 'not-signed', 'languages': ['en', 'fa'], 'defaultLanguage': 'en', 'runtimeLicenses': runtime_notices}
    (output / 'build-evidence.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    exe.with_suffix('.exe.sha256').write_text(report['exeSha256'] + '  ' + exe.name + '\n', encoding='ascii')
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--media', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    build(parser.parse_args())
