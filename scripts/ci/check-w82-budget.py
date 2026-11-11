"""Enforce reviewed W82 limits, or audit a candidate without granting admission."""
import argparse
import json
import math
from pathlib import Path
import re
import subprocess

from w82_evidence import evaluate, read, require
from w82_build_inputs import verify as verify_build_inputs


def finite(value, path="budget"):
    if isinstance(value, (float, int)) and not isinstance(value, bool):
        require(math.isfinite(value), f"Non-finite metric: {path}")
    elif isinstance(value, dict):
        for key, item in value.items():
            finite(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            finite(item, f"{path}[{index}]")


def number(value, path):
    require(type(value) in (int, float) and math.isfinite(value) and value >= 0, f"Invalid numeric limit: {path}")
    return value


def at(value, path):
    for key in path.split("."):
        value = value[key]
    return value


def bound(actual, budget, label, *, floor=False):
    center = number(budget["observed"]["median"], label + ".reference")
    relative = number(budget["allowedRelativeRegression"], label + ".relative")
    require(not floor or relative <= 1, f"Invalid throughput tolerance: {label}")
    limit = number(budget["proposedFloor" if floor else "proposedCeiling"], label + ".absolute")
    number(actual, label + ".measured")
    relative_limit = center * (1 - relative if floor else 1 + relative)
    # Decimal JSON round-trips can differ by one final binary digit.
    for name, threshold in (("absolute", limit), ("relative", relative_limit)):
        tolerance = 4 * math.ulp(threshold)
        passed = actual >= threshold - tolerance if floor else actual <= threshold + tolerance
        require(passed, f"{label} breaches {name} limit ({actual} versus {threshold})")


def metrics(provider):
    yield "authenticatedColdStartMilliseconds", False
    for workers in (1, 10, 50):
        prefix = f"steady.{workers}."
        yield prefix + "successfulOperationsPerSecond", True
        for kind in ("read", "explain", "catalogCommit"):
            for percentile in ("p95", "p99"):
                yield prefix + f"latencyMilliseconds.{kind}.{percentile}", False
        for node in ("process", "peerProcess") if provider == "postgres" else ("process",):
            for metric in ("cpuP95PercentOfOneLogicalProcessor", "rssSteadyMedianBytes", "rssPeakBytes"):
                yield prefix + f"process.{node}.{metric}", False
        for metric in ("cpuP95PercentOfOneLogicalProcessor", "memorySteadyMedianBytes", "memoryPeakBytes"):
            yield prefix + "container." + metric, False


def compare(actual, budget, provider):
    finite(actual, provider + ".actual")
    checked = 0
    for path, floor in metrics(provider):
        measured, reference = at(actual, path), at(budget, path)
        # All five cold starts, and the median of three steady repetitions.
        sample = "max" if path == "authenticatedColdStartMilliseconds" else "median"
        bound(measured["observed"][sample], reference, provider + "." + path, floor=floor)
        checked += 1
    for node, values in budget["soak"]["process"].items():
        for metric, reference in values.items():
            label = f"{provider}.soak.{node}.{metric}.growthPerHour"
            limit = number(reference["proposedGrowthCeilingPerHour"], label)
            measured = at(actual, f"soak.process.{node}.{metric}.proposedGrowthCeilingPerHour")
            require(measured <= limit + 4 * math.ulp(limit), f"{label} breaches limit")
            checked += 1
    for node, values in budget["soak"]["postLoadIdle"].items():
        for metric, reference in values.items():
            measured = at(actual, f"soak.postLoadIdle.{node}.{metric}.observed.max")
            bound(measured, reference, f"{provider}.idle.{node}.{metric}")
            checked += 1
    return checked


def source_tree(repository, reference):
    return subprocess.check_output(["git", "-C", str(repository), "rev-parse", reference + ":src"], text=True).strip()


def soak_metrics(actual, provider):
    soak = actual["soak"]
    yield "successfulOperationsPerSecond", soak["successfulOperationsPerSecond"], True
    for kind in ("read", "explain", "catalogCommit"):
        for percentile in ("p95", "p99"):
            yield f"latencyMilliseconds.{kind}.{percentile}", soak["latencyMilliseconds"][kind][percentile], False
    for node in ("process", "peerProcess") if provider == "postgres" else ("process",):
        for name, metric, stat in (("cpuP95PercentOfOneLogicalProcessor", "cpuPercentOfOneLogicalProcessor", "p95"),
                                  ("rssSteadyMedianBytes", "rssBytes", "median"), ("rssPeakBytes", "rssBytes", "max")):
            yield f"process.{node}.{name}", soak["resources"][node][metric][stat], False
    for name, metric, stat in (("cpuP95PercentOfOneLogicalProcessor", "cpuPercentOfOneLogicalProcessor", "p95"),
                              ("memorySteadyMedianBytes", "memoryCurrentBytes", "median"), ("memoryPeakBytes", "memoryCurrentBytes", "max")):
        yield "container." + name, soak["container"][metric][stat], False


def enforce_soak(actual, budget, provider):
    require("limits" in budget["soak"], f"{provider} requires reviewed numeric soak CPU/RSS/latency/throughput limits")
    limits = budget["soak"]["limits"]
    require(limits["allowedErrors"] == limits["allowedHttp429"] == 0, "Missing zero-error soak quality limits")
    checked = 0
    for path, value, floor in soak_metrics(actual, provider):
        bound(value, at(limits, path), provider + ".soak." + path, floor=floor)
        checked += 1
    return checked


def identity(actual, budget, candidate, repository, source):
    require(re.fullmatch(r"[0-9a-f]{40}", candidate["applicationTreeSha"]) is not None, "Invalid application tree identity")
    require(source_tree(repository, source) == source_tree(repository, candidate["protectedPerformanceSourceSha"])
            == candidate["applicationTreeSha"], "Current application tree differs from measured protected source")
    subprocess.run(["git", "-C", str(repository), "diff", "--exit-code", source, "--", "src"],
                   check=True, stdout=subprocess.DEVNULL)
    require(not subprocess.check_output(["git", "-C", str(repository), "ls-files", "--others", "--exclude-standard", "--", "src"], text=True),
            "Untracked application source invalidates tree proof")
    require(actual["sourceSha"] == budget["sourceSha"] == candidate["protectedPerformanceSourceSha"], "Baseline source mismatch")
    require(actual["apiDllSha256"] == budget["apiDllSha256"] and re.fullmatch(r"[0-9a-f]{64}", actual["apiDllSha256"]), "Provider API artifact mismatch")
    require(actual["checkpointSha256"] == budget["checkpointSha256"], "Baseline checkpoint identity mismatch")
    require(actual["harnessSha"] == budget["harnessSha"] and re.fullmatch(r"[0-9a-f]{40}", actual["harnessSha"]), "Provider harness identity mismatch")
    require(actual["environment"] == budget["environment"], "Provider CPU/runtime/topology identity mismatch")
    require(actual["dataset"] == budget["dataset"], "Provider dataset mismatch")
    verify_build_inputs(repository, source, candidate)


def admission(candidate):
    require(candidate["status"] == "admitted", "W82 candidate requires review before release admission")
    review = candidate["admission"]
    require(review["artifactComparabilityReviewed"] is True and review["reviewReference"].strip(),
            "Missing reviewed per-provider artifact comparability decision")
    require(review["hardwarePolicy"] == "per-provider", "Missing per-provider hardware policy")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--budget", type=Path, default=Path(__file__).resolve().parents[2] / "docs/performance/v0.9-budget-candidate.json")
    parser.add_argument("--sqlite", required=True, type=Path)
    parser.add_argument("--postgres", required=True, type=Path)
    parser.add_argument("--source", default="HEAD")
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--candidate-audit", action="store_true")
    args = parser.parse_args()
    repository = args.repository.resolve()
    try:
        candidate = read(args.budget)
        finite(candidate)
        require(candidate["schemaVersion"] == 1 and set(candidate["providers"]) == {"sqlite", "postgres"}, "Missing provider budget")
        if not args.candidate_audit:
            admission(candidate)
        results = {}
        for provider, directory in (("sqlite", args.sqlite), ("postgres", args.postgres)):
            actual = evaluate(directory, provider)
            reference = candidate["providers"][provider]
            identity(actual, reference, candidate, repository, args.source)
            checked = compare(actual, reference, provider)
            if not args.candidate_audit:
                checked += enforce_soak(actual, reference, provider)
            results[provider] = {"checkedBounds": checked, "apiDllSha256": actual["apiDllSha256"],
                                 "cpu": actual["environment"]["cpu"], "githubRunId": reference["githubRunId"]}
        print(json.dumps({"status": "candidate-audit-passed" if args.candidate_audit else "admitted-budget-passed",
                          "releaseAdmission": not args.candidate_audit,
                          "protectedPerformanceSourceSha": candidate["protectedPerformanceSourceSha"],
                          "applicationTreeSha": candidate["applicationTreeSha"], "currentApplicationTreeVerified": True,
                          "sharedApiBinary": results["sqlite"]["apiDllSha256"] == results["postgres"]["apiDllSha256"],
                          "pendingReview": ["candidate status", "per-provider artifact comparability", "explicit soak limits"] if args.candidate_audit else [],
                          "providers": results}, indent=2, allow_nan=False))
    except (ValueError, KeyError, TypeError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"W82 budget rejected: {error}\n")


if __name__ == "__main__":
    main()
