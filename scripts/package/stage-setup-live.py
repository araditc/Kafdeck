"""Prepare public, hash-pinned release inputs for isolated Windows installer acceptance."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'artifacts/setup-live-inputs'


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    OUT.mkdir(parents=True, exist_ok=False)
    vendor = OUT / 'vendor'
    signature = OUT / 'signature'
    vendor.mkdir()
    signature.mkdir()
    rows = json.loads((ROOT / 'publication/v1-offline/vendors.json').read_text(encoding='utf-8'))

    def download(row):
        target = vendor / row['file']
        with urllib.request.urlopen(row['downloadUrl'], timeout=180) as source, target.open('wb') as destination:
            if not source.url.startswith('https://'):
                raise ValueError('Vendor transport is not HTTPS')
            shutil.copyfileobj(source, destination)
        if target.stat().st_size != row['bytes'] or sha(target) != row['sha256']:
            raise ValueError('Pinned vendor bytes changed: ' + row['file'])

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(download, rows))
    for name in ('manifest.json', 'manifest.sigstore.json', 'trusted-root.json'):
        subprocess.run(['gh', 'release', 'download', 'v1.1.0', '--repo', 'araditc/Kafdeck', '--pattern', name, '--dir', str(signature)], check=True)
    native = 'media__native__win-x64__kafdeck-api-ui-1.1.0-win-x64.tar.gz'
    subprocess.run(['gh', 'release', 'download', 'v1.1.0', '--repo', 'araditc/Kafdeck', '--pattern', native, '--dir', str(OUT)], check=True)
    with zipfile.ZipFile(vendor / 'java21.zip') as archive:
        # This exact archive's size and digest were admitted above.
        archive.extractall(OUT / 'jdk')
    jdk = next((OUT / 'jdk').iterdir())
    subprocess.run(['python', '-X', 'utf8', str(ROOT / 'scripts/package/build-windows-offline.py'), '--version', '1.1.0',
                    '--signature-dir', str(signature), '--vendor-dir', str(vendor), '--native-archive', str(OUT / native),
                    '--jdk', str(jdk), '--output', str(OUT / 'offline')], check=True)
    medium = OUT / 'offline/kafdeck-offline-1.1.0-win-x64.zip'
    subprocess.run(['python', '-X', 'utf8', str(ROOT / 'scripts/package/build-windows-setup.py'), '--media', str(medium),
                    '--expected-sha256', sha(medium), '--output', str(ROOT / 'artifacts/setup-live-candidate')], check=True)


if __name__ == '__main__':
    main()
