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
                            "OWNER_APPROVAL": driver.STABLE.approval, "GITHUB_ACTIONS": "true",
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
                    driver.authority(driver.STABLE)
                api.assert_not_called()

    def test_protected_current_main_and_source_ancestry(self):
        for protected, sha in [(True, self.publisher), (False, self.publisher), (True, "b" * 40)]:
            with self.subTest(protected=protected, sha=sha), patch.dict(os.environ, self.environment, clear=True), \
                    patch.object(driver.transport, "run", side_effect=self.command) as command, \
                    patch.object(driver.transport, "repo_api", return_value={"protected": protected, "commit": {"sha": sha}}):
                if protected and sha == self.publisher:
                    self.assertEqual(driver.authority(driver.STABLE), self.publisher)
                    command.assert_called_with("git", "merge-base", "--is-ancestor", driver.STABLE.source, self.publisher)
                else:
                    with self.assertRaises(ValueError):
                        driver.authority(driver.STABLE)

    def test_retry_accepts_only_matching_existing_release(self):
        tag = {"object": {"type": "commit", "sha": driver.STABLE.source}}
        release = {"tag_name": driver.STABLE.tag, "body": "body", "name": "Kafdeck 1.0.0", "prerelease": False,
                   "assets": [{"name": "manifest.json", "digest": "sha256:" + driver.STABLE.manifest}]}
        driver.verify_existing(driver.STABLE, None, None, {}, "body")
        driver.verify_existing(driver.STABLE, tag, release, {"manifest.json": driver.STABLE.manifest}, "body")
        for changed_tag, changed_release in [(None, release), ({"object": {"type": "commit", "sha": "b" * 40}}, release),
                                               (tag, {**release, "body": "foreign"}), (tag, {**release, "assets": [{"name": "foreign"}]}),
                                               (tag, {**release, "assets": [{"name": "manifest.json", "digest": "sha256:wrong"}]})]:
            with self.subTest(tag=changed_tag, release=changed_release), self.assertRaises(ValueError):
                driver.verify_existing(driver.STABLE, changed_tag, changed_release, {"manifest.json": driver.STABLE.manifest}, "body")

    def test_artifact_bound_to_signer_and_digest(self):
        item = {"digest": "sha256:" + driver.STABLE.signature_zip, "expired": False,
                "workflow_run": {"head_sha": driver.STABLE.source, "id": driver.STABLE.signing_run}}
        with patch.object(driver.transport, "repo_api", return_value=item):
            self.assertEqual(driver.artifact(driver.STABLE, driver.STABLE.signature_artifact, driver.STABLE.signature_zip), item)
        for changed in [{**item, "expired": True}, {**item, "digest": "sha256:wrong"},
                        {**item, "workflow_run": {"head_sha": "b" * 40, "id": driver.STABLE.signing_run}},
                        {**item, "workflow_run": {"head_sha": driver.STABLE.source, "id": 1}}]:
            with self.subTest(item=changed), patch.object(driver.transport, "repo_api", return_value=changed), self.assertRaises(ValueError):
                driver.artifact(driver.STABLE, driver.STABLE.signature_artifact, driver.STABLE.signature_zip)

    def test_partial_release_inventory_survives_publisher_main_advancement(self):
        with tempfile.TemporaryDirectory() as temporary:
            assets = Path(temporary) / "assets"
            assets.mkdir()
            (assets / "manifest.json").write_text("fixed signed manifest", encoding="utf-8")
            inventories = []
            checksums = []
            for publisher in ("a" * 40, "b" * 40):
                with patch.dict(os.environ, {"GITHUB_SHA": publisher}):
                    inventory = driver.release_inventory(driver.STABLE, assets, "sha256:" + "c" * 64, {})
                    target = Path(temporary) / (publisher + ".json")
                    driver.write(target, inventory)
                    inventories.append(inventory)
                    checksums.append(driver.digest(target))
            self.assertEqual(inventories[0], inventories[1])
            self.assertEqual(checksums[0], checksums[1])
            self.assertNotIn("publisherCommit", inventories[0])
            tag = {"object": {"type": "commit", "sha": driver.STABLE.source}}
            release = {"tag_name": driver.STABLE.tag, "body": "body", "name": "Kafdeck 1.0.0", "prerelease": False,
                       "assets": [{"name": "publication-selection.json", "digest": "sha256:" + checksums[0]}]}
            driver.verify_existing(driver.STABLE, tag, release, {"publication-selection.json": checksums[1]}, "body")

    def test_release_lookup_finds_drafts_across_pages_and_rejects_duplicates(self):
        draft = {"id": 449072989, "tag_name": driver.STABLE.tag, "draft": True}
        cases = [([[{"tag_name": "v0.9"}], [draft]], draft), ([[{**draft, "draft": False}]], {**draft, "draft": False}),
                 ([[{"tag_name": "v0.9"}]], None)]
        for pages, expected in cases:
            with self.subTest(pages=pages), patch.object(driver.transport, "run", return_value=json.dumps(pages)) as command:
                self.assertEqual(driver.existing_release(driver.STABLE), expected)
                command.assert_called_once_with("gh", "api", f"repos/{driver.REPOSITORY}/releases?per_page=100", "--paginate", "--slurp")
        with patch.object(driver.transport, "run", return_value=json.dumps([[draft], [draft]])), self.assertRaises(ValueError):
            driver.existing_release(driver.STABLE)

    def test_approval_separates_the_pre_release_from_the_published_stable_selection(self):
        self.assertIs(driver.approved_publication(driver.STABLE.approval), driver.STABLE)
        self.assertIs(driver.approved_publication(driver.RC.approval), driver.RC)
        for approval in (None, driver.RC.approval.replace("rc.1", "rc.2"),
                         driver.RC.approval.replace(driver.RC.source, driver.RC.signing_commit)):
            with self.subTest(approval=approval), self.assertRaises(ValueError):
                driver.approved_publication(approval)

    def test_rc_artifact_and_release_have_distinct_signer_and_product_commits(self):
        plan = driver.RC
        item = {"digest": "sha256:" + plan.signature_zip, "expired": False,
                "workflow_run": {"head_sha": plan.signing_commit, "id": plan.signing_run}}
        with patch.object(driver.transport, "repo_api", return_value=item):
            self.assertEqual(driver.artifact(plan, plan.signature_artifact, plan.signature_zip), item)
        item["workflow_run"]["head_sha"] = plan.source
        with patch.object(driver.transport, "repo_api", return_value=item), self.assertRaises(ValueError):
            driver.artifact(plan, plan.signature_artifact, plan.signature_zip)
        tag = {"object": {"type": "commit", "sha": plan.source}}
        release = {"tag_name": plan.tag, "body": "body", "name": f"Kafdeck {plan.version}", "prerelease": True, "assets": []}
        driver.verify_existing(plan, tag, release, {}, "body")
        with self.assertRaises(ValueError):
            driver.verify_existing(plan, tag, {**release, "prerelease": False}, {}, "body")


if __name__ == "__main__":
    unittest.main()
