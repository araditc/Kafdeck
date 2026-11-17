"""Focused determinism checks for retrying supplementary Release uploads."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile

path = Path(__file__).resolve().parents[2] / 'scripts/package/build-windows-offline.py'
spec = importlib.util.spec_from_file_location('offline_builder', path)
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class ArchiveIdentityTests(unittest.TestCase):
    def test_outer_archive_has_fixed_metadata(self):
        with tempfile.TemporaryDirectory() as folder:
            files = [Path(folder) / 'a.zip', Path(folder) / 'b.zip']
            for file in files:
                with zipfile.ZipFile(file, 'w') as archive:
                    builder.add_zip_file(archive, 'payload/docs/README.md', b'fixed content')
            self.assertEqual(files[0].read_bytes(), files[1].read_bytes())
            with zipfile.ZipFile(files[0]) as archive:
                self.assertEqual((2000, 1, 1, 0, 0, 0), archive.getinfo('payload/docs/README.md').date_time)

    def test_vendor_repack_ignores_upstream_entry_timestamps(self):
        with tempfile.TemporaryDirectory() as folder:
            outputs = []
            for index, year in enumerate((2001, 2026)):
                source = Path(folder) / f'source-{index}.zip'
                target = Path(folder) / f'target-{index}.zip'
                with zipfile.ZipFile(source, 'w') as archive:
                    archive.writestr(zipfile.ZipInfo('vendor/bin/runtime', (year, 1, 1, 0, 0, 0)), b'fixed runtime')
                builder.repack(source, target)
                outputs.append(target.read_bytes())
            self.assertEqual(outputs[0], outputs[1])


if __name__ == '__main__':
    unittest.main()
