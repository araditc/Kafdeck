import importlib.util
import json
import os
from pathlib import Path
import unittest
import tempfile
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("stable_publication", Path(__file__).with_name("driver.py"))
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


class PublicationGuards(unittest.TestCase):
    def setUp(self):
        self.publisher = "a" * 40
        self.environment = {"GITHUB_ACTOR": "ammarheidari", "GITHUB_TRIGGERING_ACTOR": "akhiabanchian",
                            "OWNER_APPROVAL": driver.APPROVAL, "GITHUB_ACTIONS": "true",
                            "GITHUB_REPOSITORY": driver.REPOSITORY, "GITHUB_EVENT_NAME": "workflow_dispatch",
                            "GITHUB_REF": "refs/heads/main", "GITHUB_SHA": self.publisher,
                            "GITHUB_WORKFLOW_REF": f"{driver.REPOSITORY}/{driver.WORKFLOW}@refs/heads/main"}

    def command(self, *args):
        if args[:2] == ("git", "show"):
            return json.dumps({"actors": ["ammarheidari", "akhiabanchian"]})
        return self.publisher

    def test_unauthorized_context_fails_before_remote_io(self):
        cases = {"OWNER_APPROVAL": "PUBLISH v0.9", "GITHUB_REF": "refs/heads/other",
                 "GITHUB_TRIGGERING_ACTOR": "other", "GITHUB_ACTOR": "other", "GITHUB_EVENT_NAME": "pull_request",
                 "GITHUB_REPOSITORY": "fork/Kafdeck", "GITHUB_SHA": "b" * 40,
                 "GITHUB_WORKFLOW_REF": "araditc/Kafdeck/untrusted@refs/heads/main"}
        for name, value in cases.items():
            with self.subTest(name=name), patch.dict(os.environ, {**self.environment, name: value}, clear=True), \
                    patch.object(driver.transport, "run", side_effect=self.command), patch.object(driver.transport, "repo_api") as api:
                with self.assertRaises(ValueError):
                    driver.authority()
                api.assert_not_called()

    def test_protected_current_main_and_source_ancestry(self):
        for protected, sha in [(True, self.publisher), (False, self.publisher), (True, "b" * 40)]:
            with self.subTest(protected=protected, sha=sha), patch.dict(os.environ, self.environment, clear=True), \
                    patch.object(driver.transport, "run", side_effect=self.command) as command, \
                    patch.object(driver.transport, "repo_api", return_value={"protected": protected, "commit": {"sha": sha}}):
                if protected and sha == self.publisher:
                    self.assertEqual(driver.authority(), self.publisher)
                    command.assert_called_with("git", "merge-base", "--is-ancestor", driver.SOURCE, self.publisher)
                else:
                    with self.assertRaises(ValueError):
                        driver.authority()

    def test_retry_accepts_only_matching_existing_release(self):
        tag = {"object": {"type": "commit", "sha": driver.SOURCE}}
        release = {"tag_name": driver.TAG, "body": "body", "name": "Kafdeck 1.0.0", "prerelease": False,
                   "assets": [{"name": "manifest.json", "digest": "sha256:" + driver.MANIFEST}]}
        driver.verify_existing(None, None, {}, "body")
        driver.verify_existing(tag, release, {"manifest.json": driver.MANIFEST}, "body")
        for changed_tag, changed_release in [(None, release), ({"object": {"type": "commit", "sha": "b" * 40}}, release),
                                               (tag, {**release, "body": "foreign"}), (tag, {**release, "assets": [{"name": "foreign"}]}),
                                               (tag, {**release, "assets": [{"name": "manifest.json", "digest": "sha256:wrong"}]})]:
            with self.subTest(tag=changed_tag, release=changed_release), self.assertRaises(ValueError):
                driver.verify_existing(changed_tag, changed_release, {"manifest.json": driver.MANIFEST}, "body")

    def test_artifact_bound_to_signer_and_digest(self):
        item = {"digest": "sha256:" + driver.SIGN_ZIP, "expired": False,
                "workflow_run": {"head_sha": driver.SOURCE, "id": driver.SIGN_RUN}}
        with patch.object(driver.transport, "repo_api", return_value=item):
            self.assertEqual(driver.artifact(driver.SIGN_ARTIFACT, driver.SIGN_ZIP), item)
        for changed in [{**item, "expired": True}, {**item, "digest": "sha256:wrong"},
                        {**item, "workflow_run": {"head_sha": "b" * 40, "id": driver.SIGN_RUN}},
                        {**item, "workflow_run": {"head_sha": driver.SOURCE, "id": 1}}]:
            with self.subTest(item=changed), patch.object(driver.transport, "repo_api", return_value=changed), self.assertRaises(ValueError):
                driver.artifact(driver.SIGN_ARTIFACT, driver.SIGN_ZIP)

    def test_partial_release_inventory_survives_publisher_main_advancement(self):
        with tempfile.TemporaryDirectory() as temporary:
            assets = Path(temporary) / "assets"
            assets.mkdir()
            (assets / "manifest.json").write_text("fixed signed manifest", encoding="utf-8")
            inventories = []
            checksums = []
            for publisher in ("a" * 40, "b" * 40):
                with patch.dict(os.environ, {"GITHUB_SHA": publisher}):
                    inventory = driver.release_inventory(assets, "sha256:" + "c" * 64, {})
                    target = Path(temporary) / (publisher + ".json")
                    driver.write(target, inventory)
                    inventories.append(inventory)
                    checksums.append(driver.digest(target))
            self.assertEqual(inventories[0], inventories[1])
            self.assertEqual(checksums[0], checksums[1])
            self.assertNotIn("publisherCommit", inventories[0])
            tag = {"object": {"type": "commit", "sha": driver.SOURCE}}
            release = {"tag_name": driver.TAG, "body": "body", "name": "Kafdeck 1.0.0", "prerelease": False,
                       "assets": [{"name": "publication-selection.json", "digest": "sha256:" + checksums[0]}]}
            driver.verify_existing(tag, release, {"publication-selection.json": checksums[1]}, "body")


if __name__ == "__main__":
    unittest.main()
