"""Focused determinism checks for retrying supplementary Release uploads."""
import ast
import importlib.util
import io
from pathlib import Path
import tempfile
import subprocess
import unittest
import zipfile

path = Path(__file__).resolve().parents[2] / 'scripts/package/build-windows-offline.py'
spec = importlib.util.spec_from_file_location('offline_builder', path)
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)

publisher_spec = importlib.util.spec_from_file_location('offline_publisher', Path(__file__).with_name('publish.py'))
publisher = importlib.util.module_from_spec(publisher_spec)
publisher_spec.loader.exec_module(publisher)


class ArchiveIdentityTests(unittest.TestCase):
    def test_agent_payload_verification_ignores_compression_but_detects_changed_entries(self):
        jars = []
        for compression, content in ((zipfile.ZIP_STORED, b'bytecode'), (zipfile.ZIP_DEFLATED, b'bytecode'), (zipfile.ZIP_DEFLATED, b'changed')):
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, 'w', compression=compression) as archive:
                archive.writestr('Agent.class', content)
                archive.writestr('META-INF/MANIFEST.MF', b'Premain-Class: Agent\r\n')
            jars.append(buffer.getvalue())
        self.assertNotEqual(jars[0], jars[1])
        self.assertEqual(publisher.jar_payload(jars[0]), publisher.jar_payload(jars[1]))
        self.assertNotEqual(publisher.jar_payload(jars[0]), publisher.jar_payload(jars[2]))

    def test_publisher_invokes_the_builder_entry_point(self):
        publisher = Path(__file__).with_name('publish.py')
        tree = ast.parse(publisher.read_text())
        commands = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name) and node.func.id == 'run'
                    and any(isinstance(arg, ast.Constant) and arg.value == '--vendor-dir' for arg in node.args)]
        self.assertEqual(1, len(commands))
        prefix = [ast.literal_eval(arg) for arg in commands[0].args[:4]]
        self.assertEqual('scripts/package/build-windows-offline.py', prefix[3])
        result = subprocess.run(prefix + ['--help'], cwd=path.parents[2], capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn('--native-archive', result.stdout)

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
