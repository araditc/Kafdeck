"""Negative packet/identity tests. Controlled bytes never stand in for live installation."""
import io
import json
import pathlib
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import v1_rc as rc

SOURCE = "a" * 40


class CandidatePacketTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = pathlib.Path(self.temp.name)
        self.choice = rc.selection()
        self.inputs = {"src/reference.cs": "b" * 64}
        self.proof = {"sourceSha": SOURCE, "version": self.choice["version"], "rid": "win-x64", "installedFromArchives": True, "components": {}}
        for name, (host, dll) in rc.COMPONENTS.items():
            archive = f"kafdeck-{name}-{self.choice['version']}-win-x64.tar.gz"
            members = {host + ".exe": b"controlled apphost", dll: b"controlled assembly"}
            if name == "api-ui":
                members.update({"wwwroot/index.html": b"<html></html>", "wwwroot/assets/reference.js": b"fixture"})
            with tarfile.open(self.folder / archive, "w:gz") as package:
                for path, value in members.items():
                    info = tarfile.TarInfo(path); info.size = len(value)
                    package.addfile(info, io.BytesIO(value))
            self.proof["components"][name] = {"archive": archive, "sha256": rc.sha(self.folder / archive), "started": True,
                "authenticatedOperation": True, "productVersion": self.choice["version"] + "+" + SOURCE, "assemblyVersion": self.choice["assemblyVersion"]}
        self.save_proof()

    def save_proof(self):
        (self.folder / "native-evidence.json").write_text(json.dumps(self.proof), encoding="utf-8")

    def packet(self):
        return rc.packet(self.folder, "win-x64", SOURCE, self.inputs, self.choice)

    def test_valid_unsigned_packet_binds_all_bytes_and_never_claims_publication(self):
        value = self.packet()
        manifest = self.folder / "rc-manifest.json"
        manifest.write_text(json.dumps(value), encoding="utf-8")
        with patch.object(rc, "selection", return_value=self.choice):
            self.assertEqual(value, rc.verify(self.folder, SOURCE, value["inputSha256"], rc.sha(manifest)))
        self.assertFalse(value["signed"] or value["releaseReady"] or value["publicationAuthorized"])

    def test_foreign_source_or_rid_is_rejected(self):
        for field, foreign in (("sourceSha", "c" * 40), ("rid", "linux-x64")):
            original = self.proof[field]
            self.proof[field] = foreign; self.save_proof()
            with self.assertRaisesRegex(ValueError, "selection mismatch"): self.packet()
            self.proof[field] = original

    def test_old_binary_version_and_unverified_authentication_are_rejected(self):
        for field, foreign in (("productVersion", "0.9.0"), ("assemblyVersion", "0.9.0.0"), ("authenticatedOperation", False)):
            original = self.proof["components"]["api-ui"][field]
            self.proof["components"]["api-ui"][field] = foreign; self.save_proof()
            with self.assertRaises(ValueError): self.packet()
            self.proof["components"]["api-ui"][field] = original

    def test_changed_archive_and_missing_component_are_rejected(self):
        archive = self.folder / self.proof["components"]["api-ui"]["archive"]
        with archive.open("ab") as stream: stream.write(b"unadmitted trailing bytes")
        with self.assertRaisesRegex(ValueError, "hash differs"): self.packet()
        del self.proof["components"]["mcp"]; self.save_proof()
        with self.assertRaisesRegex(ValueError, "incomplete"): self.packet()

    def test_manifest_digest_and_input_identity_are_independently_pinned(self):
        value = self.packet()
        manifest = self.folder / "rc-manifest.json"; manifest.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "selected digest"):
            rc.verify(self.folder, SOURCE, value["inputSha256"], "f" * 64)
        with self.assertRaisesRegex(ValueError, "source/input identity"):
            rc.verify(self.folder, SOURCE, "e" * 64, rc.sha(manifest))
        value["inputs"]["src/reference.cs"] = "d" * 64
        manifest.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "input list"):
            rc.verify(self.folder, SOURCE, value["inputSha256"], rc.sha(manifest))

    def test_archive_traversal_symlink_and_duplicate_members_are_rejected(self):
        for name, link, duplicate in (("../escape", False, False), ("/absolute", False, False), ("C:/escape", False, False),
                                      ("link", True, False), ("duplicate", False, True)):
            archive = self.folder / "unsafe.tar.gz"
            with tarfile.open(archive, "w:gz") as package:
                info = tarfile.TarInfo(name); info.size = 1
                if link: info.type = tarfile.SYMTYPE; info.linkname = "outside"
                package.addfile(info, io.BytesIO(b"x") if not link else None)
                if duplicate: package.addfile(info, io.BytesIO(b"x"))
            with self.assertRaises(ValueError): rc.archive_files(archive)

    def test_fresh_install_checks_every_file_and_refuses_existing_destination(self):
        value = self.packet()
        destination = self.folder / "fresh-install"
        executable = rc.install_api(self.folder, destination, value)
        self.assertEqual(executable.read_bytes(), b"controlled apphost")
        self.assertEqual((destination / "wwwroot/index.html").read_bytes(), b"<html></html>")
        with self.assertRaisesRegex(ValueError, "fresh"): rc.install_api(self.folder, destination, value)

    def test_tampered_archive_cannot_create_an_install_directory(self):
        value = self.packet()
        archive = self.folder / value["components"]["api-ui"]["archive"]
        with archive.open("ab") as stream: stream.write(b"changed")
        destination = self.folder / "never-created"
        with self.assertRaisesRegex(ValueError, "changed before"): rc.install_api(self.folder, destination, value)
        self.assertFalse(destination.exists())

    def test_replaced_archive_is_revalidated_during_extraction(self):
        value = self.packet()
        archive = self.folder / value["components"]["api-ui"]["archive"]
        original_inventory = rc.archive_files
        def swap_after_inventory(path):
            result = original_inventory(path)
            with tarfile.open(archive, "w:gz") as package:
                info = tarfile.TarInfo("../escape"); info.size = 1
                package.addfile(info, io.BytesIO(b"x"))
            return result
        with patch.object(rc, "archive_files", side_effect=swap_after_inventory):
            with self.assertRaisesRegex(ValueError, "Unsafe archive path"):
                rc.install_api(self.folder, self.folder / "owned", value)
        self.assertFalse((self.folder / "escape").exists())


if __name__ == "__main__":
    unittest.main()
