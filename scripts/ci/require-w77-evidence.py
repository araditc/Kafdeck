"""Validate measured W77 producer artifacts. No absent, skipped or partial cell passes.

Run --hardening-dir DIR for H1/H2 and/or --profile-dir DIR for one H3 cell.
Use --profiles-root DIR to require all six H3 cells recursively after downloading
the workflow artifacts. This verifies evidence completeness, not W82 budgets.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import xml.etree.ElementTree as ET


CELLS = {("apache", v) for v in ("4.3.1", "4.2.1", "4.1.2", "3.9.2")} | {
    ("confluent-platform", "8.3.2"), ("redpanda", "25.3.11")}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read(path, sha):
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    require(value.get("sourceSha") == sha, f"Source mismatch: {path.name}")
    return value


def hardening(directory, sha):
    files = []
    for provider in ("sqlite", "postgres"):
        path = directory / f"{provider}-w77-h1-evidence.json"
        value = read(path, sha)
        require(value["acceptance"] == "H1", "H1 acceptance identity")
        w = value["workload"]
        require((w["warmSamples"], w["loadSamples"], w["coolSamples"], w["concurrency"], w["requests"], w["metadataRequests"], w["failed"], w["skipped"])
                == (10, 300, 10, 4, 1220, 10, 0, 0), "Fixed H1 workload changed/incomplete")
        require(value["dataset"]["approvedRequests"] == value["dataset"]["activePolicyVersions"] == value["dataset"]["measurementWindows"] == 1, "Missing populated governance fixture")
        require(value["dataset"]["controlledMeasuredBytes"] == 98304, "Missing real measured traffic")
        require(len(value["samples"]) == 320, "Missing resource samples")
        for name, count in (("warm", 10), ("load", 300), ("cool", 10)):
            samples = [sample for sample in value["samples"] if sample["phase"] == name]
            require(len(samples) == count and [s["tick"] for s in samples] == list(range(count)), "Sample phase mismatch")
        for process in ("api", "testhost"):
            samples = [s[process] for s in value["samples"]]
            # Monotonic observed CPU, positive memory and explicit handles/threads are mandatory.
            require(all(math.isfinite(s["CpuSeconds"]) and s["RssBytes"] > 0 and s["Threads"] > 0 and s["Handles"] >= 0 for s in samples), "Invalid process sample")
            require(all(a["CpuSeconds"] <= b["CpuSeconds"] and a["ElapsedSeconds"] < b["ElapsedSeconds"] for a, b in zip(samples, samples[1:])), "Nonmonotonic process series")
            require(samples[-1]["ElapsedSeconds"] - samples[0]["ElapsedSeconds"] >= 315, "Soak wall clock is shorter than fixed profile")
        require(value["privateDirectoryRemoved"] and value["apiProcessesStopped"] and value["durableRestart"], "Incomplete H1 cleanup/restart")
        require(not value["admittedPerformanceBudget"] and not value["leakFreedomClaimed"], "W77 cannot manufacture W82 admission")
        files.append(path)
        path = directory / f"{provider}-w77-h2-evidence.json"
        value = read(path, sha)
        require(value["acceptance"] == "H2", "H2 acceptance identity")
        faults = {row["fault"] for row in value["events"]}
        expected = {"owned-broker-process-killed", "owned-loopback-network-connections-cut", "owned-api-abrupt-process-kill-and-restart",
                    "postgres-storage-authority-exclusive-lock" if provider == "postgres" else "sqlite-storage-writer-lock"}
        if provider == "postgres":
            expected.add("postgres-physical-transport-cut")
        require(faults == expected, "Missing physical fault cell")
        require(value["duplicateProtectedEffects"] == 0 and value["durableTeamCount"] == 8 and value["finalRevision"] == 1, "Durable recovery mismatch")
        cleanup = value["cleanup"]
        require(cleanup["ownedApiStopped"] and cleanup["ownedBrokerStopped"] and cleanup["privateDirectoriesRemoved"] and cleanup["proxyConnections"] == 0, "Incomplete H2 cleanup")
        files.append(path)
    return files


def profiles(directory, sha, complete):
    found, files = set(), []
    for path in sorted(directory.rglob("w77-h3-*-evidence.json")):
        value = read(path, sha)
        cell = value["provider"], value["version"]
        require(cell in CELLS and cell not in found, "Unadmitted or duplicate provider cell")
        found.add(cell)
        require(value["acceptance"] == "H3" and value["actualBroker"] and value["topicCleanupVerified"], "Incomplete H3 real broker/cleanup")
        require({row["operation"] for row in value["transitions"]} == {"TopicCreate", "TopicAlter", "TopicIncreasePartitions", "TopicDelete"}, "Missing typed lifecycle operation")
        require(all(row["independentlyObserved"] for row in value["transitions"]), "Unverified provider effect")
        require(len(value["capabilities"]) == 7 and value["notClaimed"], "Capability inventory/limitations absent")
        cleanup = json.loads((path.parent / "cleanup.json").read_text())
        require(cleanup["ownedContainerRemoved"] and cleanup["testExitCode"] == 0, "Provider container cleanup failed")
        image = json.loads((path.parent / "image-identity.json").read_text())
        require(image["Id"].startswith("sha256:") and image["Architecture"] and (path.parent / "actual-version.txt").stat().st_size > 0, "Actual runtime image/version missing")
        # Recheck the actual result when aggregating downloaded artifacts, independently
        # of the producer shell's earlier require-complete-trx invocation.
        trx = ET.parse(path.parent / "profile.trx").getroot()
        ns = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}
        counters = trx.find("t:ResultSummary/t:Counters", ns)
        require(counters is not None, "Provider TRX counters missing")
        require(int(counters.get("total", "0")) == int(counters.get("passed", "0")) == 1 and
                int(counters.get("failed", "0")) == int(counters.get("notExecuted", "0")) == 0, "Provider TRX failed/skipped/incomplete")
        results = trx.findall("t:Results/t:UnitTestResult", ns)
        require(len(results) == 1 and results[0].get("outcome") == "Passed", "Provider TRX result absent or not passed")
        definitions = {test.get("id"): test.find("t:TestMethod", ns) for test in trx.findall("t:TestDefinitions/t:UnitTest", ns)}
        method = definitions.get(results[0].get("testId"))
        require(method is not None and "V09W77ProviderCapabilityLiveTests" in method.get("className", ""), "Provider TRX belongs to another producer")
        files.extend([path, path.parent / "cleanup.json", path.parent / "image-identity.json", path.parent / "actual-version.txt", path.parent / "profile.trx"])
    require(found == CELLS if complete else len(found) == 1, "Missing required actual provider/version profile")
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--hardening-dir", type=Path)
    parser.add_argument("--profile-dir", type=Path)
    parser.add_argument("--profiles-root", type=Path)
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    require(len(args.source_sha) == 40 and all(c in "0123456789abcdef" for c in args.source_sha), "Exact source SHA required")
    files = []
    if args.hardening_dir:
        files.extend(hardening(args.hardening_dir, args.source_sha))
    if args.profile_dir:
        files.extend(profiles(args.profile_dir, args.source_sha, False))
    if args.profiles_root:
        files.extend(profiles(args.profiles_root, args.source_sha, True))
    require(files, "No evidence family requested")
    if args.manifest:
        args.manifest.write_text(json.dumps({"sourceSha": args.source_sha, "performanceBudgetAdmitted": False,
            "files": [{"name": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in files]}, indent=2) + "\n")
    print(f"W77 completeness verified: {len(files)} exact-source artifacts; no W82 numerical budget admission.")


if __name__ == "__main__":
    main()
