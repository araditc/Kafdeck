import importlib.util
import io
import hashlib
import json
from pathlib import Path
import unittest
import zipfile

spec = importlib.util.spec_from_file_location('setup_builder', Path(__file__).with_name('build-windows-setup.py'))
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class SetupMediaTests(unittest.TestCase):
    def archive(self, changed=False, extra=False):
        data = b'valid'
        manifest = {'format': 1, 'deploymentMode': 'windows-services', 'version': '1.1.0',
                    'files': [{'path': 'payload/app/app.exe', 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}]}
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, 'w') as output:
            output.writestr('bundle-manifest.json', json.dumps(manifest))
            output.writestr('payload/app/app.exe', b'evil!' if changed else data)
            if extra:
                output.writestr('extra.ps1', 'unexpected')
        stream.seek(0)
        return zipfile.ZipFile(stream)

    def test_exact_inventory(self):
        with self.archive() as archive:
            self.assertEqual('1.1.0', builder.verify(archive)['version'])

    def test_changed_and_unlisted_rejected(self):
        for kwargs in ({'changed': True}, {'extra': True}):
            with self.archive(**kwargs) as archive, self.assertRaises(ValueError):
                builder.verify(archive)

    def test_unsafe_media_names(self):
        for name in ('../escape', '/absolute', 'C:/x', 'a\\b', 'file:stream', 'a//b', 'a/./b', 'a/b. '):
            with self.subTest(name=name), self.assertRaises(ValueError):
                builder.safe(name)


if __name__ == '__main__':
    unittest.main()
