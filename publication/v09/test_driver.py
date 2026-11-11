import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import driver

PUBLISHER = "a" * 40
SELECTION_HASH = "b" * 64


class GuardTests(unittest.TestCase):
    def environment(self):
        return {"GITHUB_ACTOR": "ammarheidari", "GITHUB_TRIGGERING_ACTOR": "akhiabanchian", "GITHUB_ACTIONS": "true",
                "GITHUB_REPOSITORY": driver.REPOSITORY, "GITHUB_REF": "refs/heads/" + driver.BRANCH,
                "GITHUB_SHA": PUBLISHER, "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_RUN_ID": "123",
                "GITHUB_WORKFLOW_REF": f"{driver.REPOSITORY}/{driver.WORKFLOW}@refs/heads/{driver.BRANCH}"}

    def selection(self):
        return {"publisherCommit": PUBLISHER, "w80PacketSha256": driver.W80_PACKET}

    def test_original_or_triggering_nonowner_rejected_before_io(self):
        for key in ("GITHUB_ACTOR", "GITHUB_TRIGGERING_ACTOR"):
            env = self.environment()
            env[key] = "not-an-owner"
            with self.subTest(key=key), patch.dict(os.environ, env), patch.object(driver, "owners", return_value={"ammarheidari", "akhiabanchian"}), \
                    patch.object(driver, "repo_api") as network, patch.object(driver, "run") as command:
                with self.assertRaisesRegex(ValueError, "both be immutable release owners"):
                    driver.authority({}, "", "", write_intent=True)
                network.assert_not_called()
                command.assert_not_called()

    def test_wrong_exact_approval_rejected_before_network(self):
        with patch.dict(os.environ, self.environment()), patch.object(driver, "owners", return_value={"ammarheidari", "akhiabanchian"}), \
                patch.object(driver, "run", return_value=PUBLISHER), patch.object(driver, "digest", return_value=SELECTION_HASH), \
                patch.object(driver, "repo_api") as network:
            with self.assertRaisesRegex(ValueError, "exact conditional owner"):
                driver.authority(self.selection(), SELECTION_HASH, "PUBLISH v0.9", write_intent=True)
            network.assert_not_called()

    def test_unprotected_or_advanced_driver_is_never_admitted(self):
        approval = driver.approval_text(SELECTION_HASH, PUBLISHER, driver.W80_PACKET)
        for branch in ({"protected": False, "commit": {"sha": PUBLISHER}}, {"protected": True, "commit": {"sha": "c" * 40}}):
            with self.subTest(branch=branch), patch.dict(os.environ, self.environment()), \
                    patch.object(driver, "owners", return_value={"ammarheidari", "akhiabanchian"}), \
                    patch.object(driver, "run", return_value=PUBLISHER), patch.object(driver, "digest", return_value=SELECTION_HASH), \
                    patch.object(driver, "repo_api", return_value=branch):
                with self.assertRaises(ValueError):
                    driver.authority(self.selection(), SELECTION_HASH, approval, write_intent=True)

    def test_actual_run_actor_cannot_be_replaced_by_environment_actor(self):
        approval = driver.approval_text(SELECTION_HASH, PUBLISHER, driver.W80_PACKET)
        replies = [{"protected": True, "commit": {"sha": PUBLISHER}}, {"protected": True, "commit": {"sha": driver.SOURCE}},
                   {"head_sha": PUBLISHER, "head_branch": driver.BRANCH, "event": "workflow_dispatch", "path": driver.WORKFLOW,
                    "repository": {"full_name": driver.REPOSITORY}, "head_repository": {"full_name": driver.REPOSITORY},
                    "actor": {"login": "attacker"}, "triggering_actor": {"login": "akhiabanchian"}}]
        with patch.dict(os.environ, self.environment()), patch.object(driver, "owners", return_value={"ammarheidari", "akhiabanchian"}), \
                patch.object(driver, "run", return_value=PUBLISHER), patch.object(driver, "digest", return_value=SELECTION_HASH), \
                patch.object(driver, "repo_api", side_effect=replies):
            with self.assertRaisesRegex(ValueError, "Actual publisher run actors"):
                driver.authority(self.selection(), SELECTION_HASH, approval, write_intent=True)


class LayerTests(unittest.TestCase):
    def test_foreign_tag_content_cannot_change_even_if_name_stays_the_same(self):
        with self.assertRaisesRegex(ValueError, "tag bytes changed"):
            driver.preserve_tags({"v0.8": "old"}, {"v0.8": "new"})
        driver.preserve_tags({"v0.8": "old"}, {"v0.8": "old", "sha256-owned": "proof"}, {"sha256-owned"})
        with self.assertRaises(ValueError):
            driver.preserve_tags({"v0.8": "old"}, {"v0.8": "old", "latest": "unapproved"}, {"sha256-owned"})

    def fixture(self, root, changed=False):
        directory = root / "blobs/sha256"
        directory.mkdir(parents=True)
        def put(data):
            sha = hashlib.sha256(data).hexdigest()
            (directory / sha).write_bytes(data)
            return {"digest": "sha256:" + sha, "size": len(data)}
        original = b"signed uncompressed filesystem layer"
        config = put(json.dumps({"rootfs": {"diff_ids": ["sha256:" + hashlib.sha256(original).hexdigest()]}}).encode())
        layer = put(gzip.compress(b"tampered filesystem" if changed else original))
        layer["mediaType"] = "application/vnd.oci.image.layer.v1.tar+gzip"
        manifest = put(json.dumps({"config": config, "layers": [layer]}).encode())
        return manifest, config["digest"]

    def test_oci_transport_layers_remain_bound_to_signed_config(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            descriptor, expected = self.fixture(root)
            self.assertEqual(expected, driver.validate_arch(root, descriptor, expected)["configDigest"])

    def test_rehashing_tampered_compressed_layer_does_not_bypass_signed_diff_id(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            descriptor, expected = self.fixture(root, changed=True)
            with self.assertRaisesRegex(ValueError, "signed config"):
                driver.validate_arch(root, descriptor, expected)

    def test_asset_tamper_rejected_before_readiness_or_registry_io(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "assets").mkdir()
            (root / "assets/client.nupkg").write_bytes(b"changed")
            selection = {"sourceSha": driver.SOURCE, "signedManifestSha256": driver.MANIFEST,
                         "indexDigest": "sha256:" + "d" * 64, "assets": [{"file": "client.nupkg", "sha256": "0" * 64}]}
            driver.write(root / "publication-selection.json", selection)
            with patch.object(driver, "OUT", root), patch.object(driver, "authority"), \
                    patch.object(driver, "readiness") as readiness, patch.object(driver, "skopeo") as registry:
                with self.assertRaisesRegex(ValueError, "Approved asset changed"):
                    driver.publish()
                readiness.assert_not_called()
                registry.assert_not_called()

    def test_attestation_predicate_and_subject_must_both_match(self):
        import base64
        expected = {"releaseReady": True, "sourceSha": driver.SOURCE}
        statement = {"predicateType": "urn:kafdeck:test", "predicate": expected,
                     "subject": [{"digest": {"sha256": "d" * 64}}]}
        envelope = json.dumps([{"payload": base64.b64encode(json.dumps(statement).encode()).decode()}])
        self.assertTrue(driver.attestation_matches(envelope, expected, "urn:kafdeck:test", "sha256:" + "d" * 64))
        self.assertFalse(driver.attestation_matches(envelope, {"releaseReady": False}, "urn:kafdeck:test", "sha256:" + "d" * 64))
        self.assertFalse(driver.attestation_matches(envelope, expected, "urn:kafdeck:test", "sha256:" + "e" * 64))


if __name__ == "__main__":
    unittest.main()
