import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import v09_prepublication as release

SOURCE = "a" * 40
POLICY = "b" * 40


def tar(path, entries):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as archive:
        for name, data in entries.items():
            data = data.encode() if isinstance(data, str) else data
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))


def fixture(root, scope="staging-distribution"):
    # Small synthetic producer assets validate admission behavior, not native startup.
    digests = {}
    for arch in ("amd64", "arm64"):
        directory = root / f"distribution/w81-oci-{arch}-{SOURCE}"
        config = json.dumps({"architecture": arch, "os": "linux"}).encode()
        config_name = hashlib.sha256(config).hexdigest() + ".json"
        image = directory / f"kafdeck-linux-{arch}.docker.tar.gz"
        tar(image, {"manifest.json": json.dumps([{"Config": config_name}]), config_name: config})
        release.write(directory / "sbom.spdx.json", {"spdxVersion": "SPDX-2.3"})
        release.write(directory / "trivy-results.sarif", {"runs": [{"results": []}]})
        digests[arch] = "sha256:" + hashlib.sha256(config).hexdigest()
        release.write(directory / "oci-native-startup.json", {
            "sourceSha": SOURCE, "architecture": arch, "uid": 1654, "readOnlyRoot": True, "network": "none",
            "startup": {"networkNone": True, "health": True, "staticFrontend": True},
            "restart": {"networkNone": True, "health": True, "staticFrontend": True},
            "configDigest": digests[arch], "archiveSha256": release.digest(image),
            "sbomSha256": release.digest(directory / "sbom.spdx.json"),
            "scanSha256": release.digest(directory / "trivy-results.sarif")})
    directory = root / f"distribution/w81-helm-lifecycle-{SOURCE}"
    tar(directory / "kafdeck-0.9.0.tgz", {"kafdeck/Chart.yaml": 'version: 0.9.0\nappVersion: "0.9.0"\n'})
    release.write(directory / "helm-lifecycle.json", {
        "sourceSha": SOURCE, "configDigest": digests["amd64"], "chartSha256": release.digest(directory / "kafdeck-0.9.0.tgz"),
        **{key: True for key in ("install", "upgrade", "uninstall", "reinstall", "retainedClaimAndFile", "verifiedHttps")}})
    mcp = root / "integrations/mcp/Kafdeck.Mcp.0.9.0.nupkg"
    mcp.parent.mkdir(parents=True)
    with zipfile.ZipFile(mcp, "w") as archive:
        archive.writestr("Kafdeck.Mcp.nuspec", "<package><metadata><version>0.9.0</version></metadata></package>")
    provider = root / "integrations/terraform"
    provider.mkdir()
    rows = []
    for name in release.selected_files("g4-external-integration-packages", SOURCE):
        if name.endswith(".zip"):
            path = provider / Path(name).name
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("terraform-provider-kafdeck", "fixture")
            rows.append(f"{release.digest(path)}  {path.name}\n")
    (provider / "terraform-provider-kafdeck_0.8.0_SHA256SUMS").write_text("".join(rows))
    if scope == "full-distribution":
        for rid in release.RIDS:
            directory = root / "native" / rid
            components = {}
            for component in ("api-ui", "cli", "mcp"):
                filename = f"kafdeck-{component}-{rid}.tar.gz"
                tar(directory / filename, {"apphost": "fixture"})
                components[component] = {"archive": filename, "sha256": release.digest(directory / filename),
                                         "started": True, "authenticatedOperation": True}
            release.write(directory / "native-evidence.json", {"schemaVersion": 1, "sourceSha": SOURCE,
                "version": "0.9.0", "rid": rid, "installedFromArchives": True, "components": components})


class ProducerTests(unittest.TestCase):
    def valid_run(self):
        return {"head_sha": SOURCE, "head_branch": "main", "event": "push", "status": "completed",
                "conclusion": "success", "name": release.PRODUCERS["distribution"][0],
                "path": release.PRODUCERS["distribution"][1],
                "repository": {"full_name": release.REPOSITORY}, "head_repository": {"full_name": release.REPOSITORY}}

    def test_exact_trusted_successful_main_run(self):
        self.assertEqual("distribution", release.validate_run(self.valid_run(), SOURCE))

    def test_rejects_wrong_source_branch_event_status_workflow_or_fork(self):
        bad = {"head_sha": POLICY, "head_branch": "feature", "event": "pull_request", "status": "in_progress",
               "conclusion": "failure", "name": "same-files-different-workflow", "path": ".github/workflows/fake.yml",
               "head_repository": {"full_name": "attacker/Kafdeck"}, "repository": {"full_name": "attacker/Kafdeck"}}
        for key, value in bad.items():
            with self.subTest(key=key):
                run = self.valid_run()
                run[key] = value
                with self.assertRaises(ValueError):
                    release.validate_run(run, SOURCE)

    def test_full_scope_requires_all_five_native_artifacts(self):
        full = release.expected_artifacts(SOURCE, "full-distribution")
        self.assertEqual(9, len(full))
        self.assertEqual(set(release.RIDS), {name[len("v09-native-"):-len(SOURCE)-1] for name in full if name.startswith("v09-native-")})
        self.assertEqual(4, len(release.expected_artifacts(SOURCE, "staging-distribution")))


class ZipTests(unittest.TestCase):
    def check_zip(self, entries, expected_error=False):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "artifact.zip"
            with zipfile.ZipFile(path, "w") as archive:
                for entry in entries:
                    archive.writestr(entry, "bytes")
            if expected_error:
                with self.assertRaises(ValueError):
                    release.extract(path, Path(directory) / "out", release.digest(path))
            else:
                release.extract(path, Path(directory) / "out", release.digest(path))
                self.assertEqual("bytes", (Path(directory) / "out/safe/file.txt").read_text())

    def test_matching_github_zip_digest_extracts(self):
        self.check_zip(["safe/file.txt"])

    def test_corrupted_zip_digest_rejected_before_open(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.zip"
            path.write_bytes(b"corrupt")
            with self.assertRaisesRegex(ValueError, "immutable digest"):
                release.extract(path, Path(directory) / "out", "0" * 64)

    def test_traversal_absolute_drive_backslash_rejected(self):
        for name in ("../escape", "/escape", "C:/escape", "safe/../escape", "safe//file"):
            with self.subTest(name=name):
                self.check_zip([name], True)
        # Windows zipfile normalizes backslashes while writing, so test the raw admission boundary.
        with self.assertRaises(ValueError):
            release.safe_relative("safe\\escape")

    def test_symlink_rejected(self):
        entry = zipfile.ZipInfo("link")
        entry.create_system = 3
        entry.external_attr = 0o120777 << 16
        self.check_zip([entry], True)


class ContentTests(unittest.TestCase):
    def test_prepare_and_verify_selected_producer_archives_end_to_end(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "fixtures"
            fixture(media)
            catalogs, archives = {"1": [], "2": []}, {}
            for number, (name, (kind, relative)) in enumerate(release.expected_artifacts(SOURCE, "staging-distribution").items(), 1):
                run_id = "1" if kind == "distribution" else "2"
                path = root / f"{number}.zip"
                with zipfile.ZipFile(path, "w") as archive:
                    for filename in release.selected_files(name, SOURCE):
                        archive.write(media / relative / filename, filename)
                catalogs[run_id].append({"id": number, "name": name, "expired": False,
                    "digest": "sha256:" + release.digest(path), "workflow_run": {"id": int(run_id), "head_sha": SOURCE}})
                archives[number] = path

            def api(endpoint):
                run_id = endpoint.split("/")[2]
                if "/artifacts?" in endpoint:
                    return {"total_count": len(catalogs[run_id]), "artifacts": catalogs[run_id]}
                kind = "distribution" if run_id == "1" else "integrations"
                return {"head_sha": SOURCE, "head_branch": "main", "event": "push", "status": "completed",
                        "conclusion": "success", "name": release.PRODUCERS[kind][0], "path": release.PRODUCERS[kind][1],
                        "run_attempt": 1, "repository": {"full_name": release.REPOSITORY},
                        "head_repository": {"full_name": release.REPOSITORY}}

            def download(args, stdout, **kwargs):
                artifact_id = int(args[-1].split("/")[-2])
                stdout.write(archives[artifact_id].read_bytes())
                return subprocess.CompletedProcess(args, 0)

            with patch.object(release, "remote_guard", return_value=POLICY), patch.object(release, "api", side_effect=api), \
                    patch.object(release.subprocess, "run", side_effect=download):
                release.prepare(SOURCE, "staging-distribution", "1,2", root / "output")
            manifest = release.verify(root / "output")
            self.assertFalse(manifest["releaseReady"])
            self.assertEqual(SOURCE, manifest["sourceSha"])
            self.assertEqual(POLICY, manifest["workflowSourceSha"])
            self.assertEqual(4, len(manifest["producers"]))
            release.write(root / "output/media/integrations/terraform/unexpected.json", {})
            with self.assertRaisesRegex(ValueError, "Unmanifested"):
                release.verify(root / "output")

    def test_staging_media_and_both_oci_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            result = release.content(root, SOURCE, "staging-distribution")
            self.assertEqual(17, len(result["files"]))
            self.assertEqual({"amd64", "arm64"}, set(result["imageConfigDigests"]))
            self.assertEqual([], result["nativeRids"])
            with self.assertRaises(ValueError):
                release.content(root, SOURCE, "full-distribution")

    def test_full_native_hashes_and_authenticated_operations_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root, "full-distribution")
            self.assertEqual(list(release.RIDS), release.content(root, SOURCE, "full-distribution")["nativeRids"])
            path = root / "native/osx-arm64/native-evidence.json"
            evidence = release.read(path)
            evidence["components"]["mcp"]["authenticatedOperation"] = False
            release.write(path, evidence)
            with self.assertRaisesRegex(ValueError, "authenticated operation"):
                release.content(root, SOURCE, "full-distribution")

    def test_image_metadata_tamper_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            path = root / f"distribution/w81-oci-amd64-{SOURCE}/oci-native-startup.json"
            report = release.read(path)
            report["archiveSha256"] = "0" * 64
            release.write(path, report)
            with self.assertRaisesRegex(ValueError, "OCI archive differs"):
                release.content(root, SOURCE, "staging-distribution")

    def test_unmanifested_media_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            (root / "unexpected.secret").write_text("do not sign")
            with self.assertRaisesRegex(ValueError, "Unmanifested"):
                release.content(root, SOURCE, "staging-distribution")


class AuthorizationTests(unittest.TestCase):
    def test_nonowner_rejected_before_network_or_git(self):
        with patch.dict(os.environ, {"GITHUB_ACTOR": "attacker", "GITHUB_TRIGGERING_ACTOR": "akhiabanchian"}), \
                patch.object(release, "api") as api, patch.object(release, "command") as command:
            with self.assertRaisesRegex(ValueError, "Both original"):
                release.local_guard()
            api.assert_not_called()
            command.assert_not_called()

    def test_wrong_manifest_hash_rejected_before_verify_or_remote(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "manifest.json").write_text("{}")
            with patch.object(release, "local_guard", return_value=POLICY), patch.object(release, "verify") as verify, \
                    patch.object(release, "remote_guard") as remote:
                with self.assertRaisesRegex(ValueError, "reviewed SHA256"):
                    release.authorize(root, "0" * 64, "anything")
                verify.assert_not_called()
                remote.assert_not_called()

    def test_approval_bound_to_exact_source_scope_and_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "manifest.json").write_text("{}")
            sha = release.digest(root / "manifest.json")
            manifest = {"sourceSha": SOURCE, "scope": "staging-distribution", "workflowSourceSha": POLICY}
            approval = f"SIGN v0.9 staging-distribution FROM {SOURCE} MANIFEST {sha}"
            with patch.object(release, "local_guard", return_value=POLICY), patch.object(release, "verify", return_value=manifest), \
                    patch.object(release, "remote_guard", return_value=POLICY) as remote:
                for bad in ("", approval.replace(SOURCE, POLICY), approval.replace("staging-distribution", "full-distribution"), approval + " "):
                    with self.subTest(approval=bad), self.assertRaisesRegex(ValueError, "exact owner authorization"):
                        release.authorize(root, sha, bad)
                remote.assert_not_called()
                release.authorize(root, sha, approval)
                remote.assert_called_once_with(SOURCE, "staging-distribution")

    def test_stale_main_and_ancestor_full_scope_rejected(self):
        with patch.object(release, "local_guard", return_value=POLICY), \
                patch.object(release, "api", return_value={"object": {"sha": SOURCE}}):
            with self.assertRaisesRegex(ValueError, "main advanced"):
                release.remote_guard(SOURCE, "staging-distribution")
        with patch.object(release, "local_guard", return_value=POLICY), \
                patch.object(release, "api", return_value={"object": {"sha": POLICY}}), \
                patch.object(release.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)):
            with self.assertRaisesRegex(ValueError, "exact current main"):
                release.remote_guard(SOURCE, "full-distribution")


if __name__ == "__main__":
    unittest.main()
