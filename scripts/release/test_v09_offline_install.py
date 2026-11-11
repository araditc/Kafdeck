import io
import json
from pathlib import Path
import stat
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import v09_offline_install as offline


class ExtractionTests(unittest.TestCase):
    def archive(self, directory, name, kind=tarfile.REGTYPE, mode=0o755):
        path = Path(directory) / "package.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            item = tarfile.TarInfo(name)
            item.type, item.mode = kind, mode
            if kind == tarfile.REGTYPE:
                item.size = 3
                archive.addfile(item, io.BytesIO(b"app"))
            else:
                item.linkname = "../escape"
                archive.addfile(item)
        return path

    def test_extract_retains_native_execution_without_setuid(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = self.archive(directory, "./kafdeck", mode=0o6755)
            output = Path(directory) / "installed"
            offline.extract_archive(archive, output)
            self.assertEqual(b"app", (output / "kafdeck").read_bytes())
            self.assertFalse((output / "kafdeck").stat().st_mode & (stat.S_ISUID | stat.S_ISGID))

    def test_traversal_links_and_devices_never_install(self):
        for name, kind in (("../escape", tarfile.REGTYPE), ("/escape", tarfile.REGTYPE),
                           ("C:/escape", tarfile.REGTYPE), ("link", tarfile.SYMTYPE),
                           ("link", tarfile.LNKTYPE), ("device", tarfile.CHRTYPE)):
            with self.subTest(name=name, kind=kind), tempfile.TemporaryDirectory() as directory:
                archive = self.archive(directory, name, kind)
                with self.assertRaises(ValueError):
                    offline.extract_archive(archive, Path(directory) / "installed")


class IsolationTests(unittest.TestCase):
    def test_egress_interface_or_default_route_rejected(self):
        for outputs in ((json.dumps([{"ifname": "lo"}, {"ifname": "eth0"}]),),
                        (json.dumps([{"ifname": "lo"}]), "default via 10.0.0.1"),
                        (json.dumps([{"ifname": "lo"}]), "", "default via fe80::1")):
            with self.subTest(outputs=outputs), patch.object(offline, "run", side_effect=outputs):
                with self.assertRaises(ValueError):
                    offline.isolated_network()

    def test_loopback_without_ipv4_or_ipv6_default_route_admitted(self):
        with patch.object(offline, "run", side_effect=(json.dumps([{"ifname": "lo"}]), "", "")):
            offline.isolated_network()

    def test_subset_never_claims_native_install_and_full_requires_it(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(offline.platform, "system", return_value="Linux"), \
                patch.object(offline.platform, "machine", return_value="x86_64"), \
                patch.object(offline, "isolated_network"), patch.object(offline, "run"), \
                patch.object(offline, "oci_install", return_value={"health": True}) as oci, \
                patch.object(offline, "native_install", return_value={"installedFromArchives": True}) as native:
            manifest = {"scope": "staging-distribution", "sourceSha": "a" * 40}
            report = offline.install(Path(directory), manifest)
            self.assertFalse(report["native"]["installedFromArchives"])
            native.assert_not_called()
            manifest["scope"] = "full-distribution"
            self.assertTrue(offline.install(Path(directory), manifest)["native"]["installedFromArchives"])
            native.assert_called_once()
            self.assertEqual(2, oci.call_count)

    def test_network_failure_prevents_load_or_execution(self):
        with patch.object(offline.platform, "system", return_value="Linux"), \
                patch.object(offline.platform, "machine", return_value="x86_64"), \
                patch.object(offline, "isolated_network", side_effect=ValueError("egress")), \
                patch.object(offline, "oci_install") as oci, patch.object(offline, "native_install") as native:
            with self.assertRaisesRegex(ValueError, "egress"):
                offline.install(Path("fixture"), {"scope": "full-distribution"})
            oci.assert_not_called()
            native.assert_not_called()


if __name__ == "__main__":
    unittest.main()
