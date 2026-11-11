"""Regression checks for fail-closed numerical and evidence validation."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import w82_evidence as evidence
import w82_build_inputs as build_inputs

spec = importlib.util.spec_from_file_location("budget", Path(__file__).with_name("check-w82-budget.py"))
budget = importlib.util.module_from_spec(spec)
spec.loader.exec_module(budget)
ROOT = Path(__file__).resolve().parents[2]


class BudgetChecks(unittest.TestCase):
    def setUp(self):
        self.candidate = json.loads((ROOT / "docs/performance/v0.9-budget-candidate.json").read_text())
        self.reference = self.candidate["providers"]["sqlite"]
        self.actual = copy.deepcopy(self.reference)

    def test_current_candidate_bounds_pass_their_recorded_baselines(self):
        for provider, values in self.candidate["providers"].items():
            self.assertGreater(budget.compare(values, values, provider), 40)

    def test_candidate_cannot_grant_release_admission(self):
        self.candidate["status"] = "requires-review"
        with self.assertRaisesRegex(ValueError, "requires review"):
            budget.admission(self.candidate)

    def test_canonical_admission_requires_owner_and_review(self):
        budget.admission(self.candidate)
        self.candidate["admission"]["reviewReference"] = ""
        with self.assertRaisesRegex(ValueError, "Missing reviewed"):
            budget.admission(self.candidate)
        self.candidate["admission"]["ownerAccepted"] = False
        with self.assertRaisesRegex(ValueError, "Missing owner acceptance"):
            budget.admission(self.candidate)

    def test_admission_requires_explicit_soak_bounds(self):
        del self.reference["soak"]["limits"]
        with self.assertRaisesRegex(ValueError, "reviewed numeric soak"):
            budget.enforce_soak(self.actual, self.reference, "sqlite")

    def test_explicit_soak_bounds_are_enforced(self):
        self.actual["soak"]["resources"] = {"process": {
            "cpuPercentOfOneLogicalProcessor": {"p95": 10}, "rssBytes": {"median": 100, "max": 110}}}
        limits = {"allowedErrors": 0, "allowedHttp429": 0}
        for path, value, floor in budget.soak_metrics(self.actual, "sqlite"):
            target = limits
            parts = path.split(".")
            for part in parts[:-1]:
                target = target.setdefault(part, {})
            target[parts[-1]] = {"observed": {"median": value}, "allowedRelativeRegression": .1,
                                 "proposedFloor" if floor else "proposedCeiling": value}
        self.reference["soak"]["limits"] = limits
        self.assertEqual(13, budget.enforce_soak(self.actual, self.reference, "sqlite"))
        self.actual["soak"]["resources"]["process"]["rssBytes"]["max"] = 111
        with self.assertRaisesRegex(ValueError, "rssPeakBytes"):
            budget.enforce_soak(self.actual, self.reference, "sqlite")

    def test_infinite_and_null_bounds_fail(self):
        self.reference["steady"]["1"]["successfulOperationsPerSecond"]["proposedFloor"] = float("nan")
        with self.assertRaises(ValueError):
            budget.finite(self.candidate)
        self.reference["steady"]["1"]["successfulOperationsPerSecond"]["proposedFloor"] = None
        with self.assertRaises(ValueError):
            budget.compare(self.actual, self.reference, "sqlite")

    def test_relative_limit_is_enforced_even_inside_absolute_limit(self):
        reference = {"observed": {"median": 100}, "proposedCeiling": 200, "allowedRelativeRegression": .05}
        with self.assertRaisesRegex(ValueError, "relative"):
            budget.bound(106, reference, "latency")

    def test_absolute_limit_is_enforced_even_inside_relative_limit(self):
        reference = {"observed": {"median": 100}, "proposedCeiling": 110, "allowedRelativeRegression": .5}
        with self.assertRaisesRegex(ValueError, "absolute"):
            budget.bound(111, reference, "rss")

    def test_throughput_floor_is_enforced(self):
        reference = {"observed": {"median": 100}, "proposedFloor": 90, "allowedRelativeRegression": .1}
        with self.assertRaises(ValueError):
            budget.bound(89, reference, "throughput", floor=True)

    def test_each_cold_start_must_fit(self):
        self.actual["authenticatedColdStartMilliseconds"]["observed"]["max"] *= 2
        with self.assertRaisesRegex(ValueError, "authenticatedColdStart"):
            budget.compare(self.actual, self.reference, "sqlite")

    def test_missing_steady_scenario_fails(self):
        del self.actual["steady"]["10"]
        with self.assertRaises(KeyError):
            budget.compare(self.actual, self.reference, "sqlite")

    def test_soak_growth_and_idle_peak_are_enforced(self):
        self.actual["soak"]["process"]["process"]["handles"]["proposedGrowthCeilingPerHour"] = 1
        with self.assertRaisesRegex(ValueError, "growthPerHour"):
            budget.compare(self.actual, self.reference, "sqlite")
        self.actual = copy.deepcopy(self.reference)
        self.actual["soak"]["postLoadIdle"]["process"]["rssBytes"]["observed"]["max"] *= 2
        with self.assertRaisesRegex(ValueError, "idle"):
            budget.compare(self.actual, self.reference, "sqlite")

    def test_cpu_artifact_and_current_source_are_independent_identity_checks(self):
        with patch.object(budget, "source_tree", return_value=self.candidate["applicationTreeSha"]), \
             patch.object(budget.subprocess, "run"), patch.object(budget.subprocess, "check_output", return_value=""):
            for field in ("apiDllSha256", "checkpointSha256", "harnessSha", "environment"):
                actual = copy.deepcopy(self.actual)
                if field == "environment":
                    actual[field]["cpu"] = "different provider CPU"
                else:
                    actual[field] = "0" * 64
                with self.subTest(field=field), self.assertRaises(ValueError):
                    budget.identity(actual, self.reference, self.candidate, ROOT, "HEAD")
        with patch.object(budget, "source_tree", return_value="0" * 40):
            with self.assertRaisesRegex(ValueError, "application tree"):
                budget.identity(self.actual, self.reference, self.candidate, ROOT, "HEAD")

    def test_skipped_or_absent_raw_phase_fails(self):
        with self.assertRaisesRegex(ValueError, "skipped mandatory"):
            evidence.validate_raw(Path("unused"), {"completedPhases": []}, "sqlite")

    def test_failed_raw_operation_is_rejected(self):
        names = ["preflight", "warmup", "soak-50"] + [f"steady-{trial:02d}-{workers}" for trial in (1, 2, 3) for workers in (1, 10, 50)]
        checkpoint = {"completedPhases": [{"name": name, "workers": 1} for name in names]}
        sample = {"phase": "preflight", "item": {"batch": 0, "worker": 0, "kind": "read", "error": "http-429"}}
        with patch.object(evidence, "records", return_value=iter([sample])):
            with self.assertRaisesRegex(ValueError, "Failed operation"):
                evidence.validate_raw(Path("unused"), checkpoint, "sqlite")
        sample["item"].update(error=None, elapsedMs=1, http=[{"status": 429, "elapsedMs": 1}])
        with patch.object(evidence, "records", return_value=iter([sample])):
            with self.assertRaisesRegex(ValueError, "Raw HTTP error"):
                evidence.validate_raw(Path("unused"), checkpoint, "sqlite")


class BuildInputChecks(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.repository = Path(self.directory.name)
        self.git("init", "-q")
        (self.repository / "Directory.Packages.props").write_text("<Project>1.0</Project>\n")
        self.commit()
        self.baseline = self.git("rev-parse", "HEAD").strip()
        self.policy = {"protectedPerformanceSourceSha": self.baseline, "status": "requires-review",
                       "buildInputIdentity": {"baseline": build_inputs.snapshot(self.repository, self.baseline), "equivalentSnapshots": []}}

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.repository), *args], text=True)

    def commit(self):
        self.git("add", ".")
        self.git("-c", "user.name=W82 verifier test", "-c", "user.email=w82@example.invalid", "commit", "-qm", "fixture")

    def test_build_input_change_requires_exact_reviewed_snapshot(self):
        build_inputs.verify(self.repository, "HEAD", self.policy)
        (self.repository / "Directory.Packages.props").write_text("<Project>2.0</Project>\n")
        self.commit()
        with self.assertRaisesRegex(ValueError, "exact reviewed"):
            build_inputs.verify(self.repository, "HEAD", self.policy)
        equivalent = {"snapshot": build_inputs.snapshot(self.repository, "HEAD"), "defaultBuildEquivalent": True,
                      "rationale": "Synthetic comparison decision", "reviewReference": None}
        self.policy["buildInputIdentity"]["equivalentSnapshots"].append(equivalent)
        build_inputs.verify(self.repository, "HEAD", self.policy)
        self.policy["status"] = "admitted"
        with self.assertRaisesRegex(ValueError, "requires review"):
            build_inputs.verify(self.repository, "HEAD", self.policy)
        equivalent["reviewReference"] = "SYNTHETIC test only"
        build_inputs.verify(self.repository, "HEAD", self.policy)

    def test_dirty_and_ignored_new_inputs_are_rejected(self):
        (self.repository / "Directory.Packages.props").write_text("dirty")
        with self.assertRaisesRegex(ValueError, "Dirty"):
            build_inputs.verify(self.repository, "HEAD", self.policy)
        self.git("checkout", "--", "Directory.Packages.props")
        (self.repository / ".gitignore").write_text("Directory.Build.targets\n")
        (self.repository / "Directory.Build.targets").write_text("<Project />")
        with self.assertRaisesRegex(ValueError, "Untracked or ignored"):
            build_inputs.verify(self.repository, "HEAD", self.policy)

    def test_new_build_metadata_paths_change_the_snapshot(self):
        before = build_inputs.snapshot(self.repository, "HEAD")
        target = self.repository / "build/native-locks"
        target.mkdir(parents=True)
        (target / "runtime.json").write_text("{}")
        self.commit()
        self.assertNotEqual(before, build_inputs.snapshot(self.repository, "HEAD"))

    def test_ignored_input_directory_case_matches_tracked_selector(self):
        (self.repository / ".gitignore").write_text("Build/\n.Config/\n")
        for name in ("Build", ".Config"):
            target = self.repository / name
            target.mkdir()
            (target / "input.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "Untracked or ignored"):
            build_inputs.verify(self.repository, "HEAD", self.policy)


if __name__ == "__main__":
    unittest.main()
