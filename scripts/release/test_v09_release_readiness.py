import base64
import hashlib
import json
import unittest
from unittest.mock import patch

import v09_release_readiness as gate

SOURCE = "a" * 40
MANIFEST_HASH = "b" * 64
ROOT_HASH = "c" * 64


class BaselineTests(unittest.TestCase):
    def test_frozen_sqlite_failure_accepts_only_its_successful_selected_job(self):
        policy = gate.read(".github/release/v0.9-baseline-producers.json")
        reference = policy["providers"]["sqlite"]
        run = {"id": reference["runId"], "head_sha": reference["driverSha"], "head_branch": policy["branch"],
               "event": "push", "status": "completed", "conclusion": "failure", "name": policy["workflowName"],
               "path": policy["workflowPath"], "repository": {"full_name": gate.REPOSITORY}, "head_repository": {"full_name": gate.REPOSITORY}}
        job = {"id": reference["jobId"], "run_id": reference["runId"], "name": reference["jobName"], "status": "completed", "conclusion": "success"}
        gate.historical_baseline(run, job, reference, policy)
        for key, wrong in (("id", reference["runId"] + 1), ("head_branch", "main"), ("head_sha", SOURCE), ("conclusion", "success")):
            with self.subTest(key=key), self.assertRaises(ValueError):
                gate.historical_baseline({**run, key: wrong}, job, reference, policy)
        with self.assertRaisesRegex(ValueError, "selected provider job"):
            gate.historical_baseline(run, {**job, "conclusion": "failure"}, reference, policy)


class ReceiptTests(unittest.TestCase):
    def fixture(self):
        manifest = {"sourceSha": SOURCE, "scope": "full-distribution", "content": {"imageConfigDigests": {"amd64": "sha256:" + "d" * 64}}}
        receipt = {"schemaVersion": 1, "signatureVerified": True, "mediaVerified": True, "sourceSha": SOURCE,
            "workflowSourceSha": SOURCE, "scope": "full-distribution", "manifestSha256": MANIFEST_HASH,
            "trustedRootSha256": ROOT_HASH, "networkNamespace": "loopback-only", "ipv4DefaultRoute": False,
            "ipv6DefaultRoute": False, "installation": {"native": {"rid": "linux-x64", "sourceSha": SOURCE,
            "installedFromArchives": True, "cliHelp": True, "mcpInitialize": True, "mcpToolCatalog": True,
            "apiUi": {"health": True, "staticFrontend": True}}, "oci": {"architecture": "amd64",
            "loadedFromVerifiedArchive": True, "configDigest": manifest["content"]["imageConfigDigests"]["amd64"],
            "network": "none", "uid": 1654, "readOnlyRoot": True, "health": True, "staticFrontend": True}}}
        return manifest, receipt

    def test_actual_install_receipt_is_bound_to_exact_signature_and_media(self):
        manifest, receipt = self.fixture()
        gate.receipt_binding(receipt, manifest, MANIFEST_HASH, ROOT_HASH)
        for key, wrong in (("scope", "staging-distribution"), ("manifestSha256", ROOT_HASH), ("ipv6DefaultRoute", True)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                gate.receipt_binding({**receipt, key: wrong}, manifest, MANIFEST_HASH, ROOT_HASH)
        receipt["installation"]["native"]["installedFromArchives"] = False
        with self.assertRaisesRegex(ValueError, "Actual signed native", msg="hash-only proof must not grant installation admission"):
            gate.receipt_binding(receipt, manifest, MANIFEST_HASH, ROOT_HASH)


class LocalW80Tests(unittest.TestCase):
    def fixture(self):
        manifest = {"sourceSha": SOURCE, "content": {"files": [{"path": "native/win-x64/kafdeck-api-ui-win-x64.tar.gz", "sha256": "d" * 64}]}}
        packet = {"schemaVersion": 1, "authority": "owner-reviewed-local", "sourceSha": SOURCE,
            "baselineSourceSha": gate.BASELINE, "signedManifestSha256": MANIFEST_HASH, "candidateWindowsApiArchiveSha256": "d" * 64,
            "testHost": {"id": "named-local-fixture", "os": "Windows", "architecture": "x64"},
            "scripts": [{"path": "w80/run-provider.ps1", "sha256": "e" * 64}],
            "evidence": [{"path": "w80/recovery-evidence.json", "sha256": "f" * 64}],
            "checks": {name: True for name in gate.W80_CHECKS}, "disconnectedFixtureOnly": True}
        return manifest, packet

    def encode(self, packet):
        raw = json.dumps(packet).encode()
        sha = hashlib.sha256(raw).hexdigest()
        return base64.b64encode(raw).decode(), f"ACCEPT W80 {sha} FOR {SOURCE} MANIFEST {MANIFEST_HASH}"

    def test_absent_local_packet_is_an_explicit_missing_gate(self):
        evidence, missing = gate.local_w80("", "", {}, MANIFEST_HASH)
        self.assertIsNone(evidence)
        self.assertTrue(missing)

    def test_reviewed_packet_requires_all_actual_journeys(self):
        manifest, packet = self.fixture()
        encoded, approval = self.encode(packet)
        with patch.object(gate.subprocess, "run") as owner_guard:
            evidence, missing = gate.local_w80(encoded, approval, manifest, MANIFEST_HASH)
            self.assertFalse(missing)
            self.assertEqual("owner-reviewed-local", evidence["authority"])
            owner_guard.assert_called_once()
            packet["checks"]["disconnectedAuthenticatedJourney"] = False
            encoded, approval = self.encode(packet)
            self.assertTrue(gate.local_w80(encoded, approval, manifest, MANIFEST_HASH)[1])

    def test_wrong_archive_or_owner_decision_never_grants_local_admission(self):
        manifest, packet = self.fixture()
        encoded, approval = self.encode(packet)
        with patch.object(gate.subprocess, "run"):
            with self.assertRaisesRegex(ValueError, "exact owner review"):
                gate.local_w80(encoded, approval + " ", manifest, MANIFEST_HASH)
            packet["candidateWindowsApiArchiveSha256"] = ROOT_HASH
            encoded, approval = self.encode(packet)
            with self.assertRaisesRegex(ValueError, "signed Windows media"):
                gate.local_w80(encoded, approval, manifest, MANIFEST_HASH)


if __name__ == "__main__":
    unittest.main()
