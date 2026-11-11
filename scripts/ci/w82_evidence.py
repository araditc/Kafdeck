"""Recompute complete W82 reference evidence using the measured candidate method."""
from bisect import bisect_left
from collections import defaultdict
import csv
from datetime import datetime
import hashlib
import json
import math
import re
from math import ceil
from statistics import median

def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return None
    return {
        "samples": len(ordered),
        "median": median(ordered),
        "p95": ordered[ceil(len(ordered) * 0.95) - 1],
        "p99": ordered[ceil(len(ordered) * 0.99) - 1],
        "max": ordered[-1],
    }


def records(path):
    if not path.is_file():
        return
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            yield json.loads(line)


def resources(path):
    grouped = defaultdict(list)
    previous = {}
    for record in records(path):
        phase = record["phase"]
        for key in ("process", "peerProcess"):
            sample = record.get(key)
            if sample is None:
                continue
            require(all(type(sample[name]) in (int, float) and math.isfinite(sample[name]) and sample[name] >= 0
                        for name in ("elapsedSeconds", "cpuSeconds", "rssBytes", "privateBytes", "handles", "threads")),
                    "Invalid process resource sample")
            require(sample["rssBytes"] > 0 and sample["threads"] > 0, "Missing live-process telemetry")
            prior = previous.get(key)
            elapsed = sample["elapsedSeconds"] - prior["elapsedSeconds"] if prior else 0
            require(prior is None or (elapsed > 0 and sample["cpuSeconds"] >= prior["cpuSeconds"]), "Nonmonotonic process resource samples")
            cpu_percent = (sample["cpuSeconds"] - prior["cpuSeconds"]) / elapsed * 100 if elapsed > 0 else None
            grouped[(phase, key)].append((sample, cpu_percent, record.get("sqliteBytes"), record.get("walBytes")))
            previous[key] = sample
    summary = defaultdict(dict)
    for (phase, key), rows in grouped.items():
        summary[phase][key] = {
            "cpuPercentOfOneLogicalProcessor": distribution(row[1] for row in rows if row[1] is not None),
            "rssBytes": distribution(row[0]["rssBytes"] for row in rows),
            "privateBytes": distribution(row[0]["privateBytes"] for row in rows),
            "handles": distribution(row[0]["handles"] for row in rows),
            "threads": distribution(row[0]["threads"] for row in rows),
            "sqliteBytes": distribution(row[2] for row in rows if row[2] is not None),
            "sqliteWalBytes": distribution(row[3] for row in rows if row[3] is not None),
        }
    return summary


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def validate_raw(root, checkpoint, provider):
    """Require actual successful operations, ordered worker slots and telemetry."""
    phases = {row["name"]: row for row in checkpoint["completedPhases"]}
    require(len(phases) == len(checkpoint["completedPhases"]), "Duplicate completed phase")
    expected = {"preflight", "warmup", "soak-50"} | {
        f"steady-{trial:02d}-{workers}" for trial in (1, 2, 3) for workers in (1, 10, 50)}
    require(set(phases) == expected, "Missing, extra or skipped mandatory load phase")
    counts = defaultdict(int)
    http = defaultdict(int)
    commits = defaultdict(int)
    nodes = defaultdict(set)
    latency = defaultdict(lambda: defaultdict(list))
    for row in records(root / "operations.ndjson"):
        name, item = row["phase"], row["item"]
        require(name in phases, "Unexpected raw operation phase")
        workers = phases[name]["workers"]
        sequence = counts[name]
        require((item["batch"], item["worker"]) == divmod(sequence, workers), "Missing or duplicated raw worker operation")
        slot = sequence * 7 % 20
        kind = "read" if slot < 16 else "explain" if slot < 19 else "catalogCommit"
        require(item["kind"] == kind and item["error"] is None, "Failed operation or changed 80/15/5 workload")
        require(math.isfinite(item["elapsedMs"]) and item["elapsedMs"] > 0, "Invalid raw latency")
        require(item["http"] and all(200 <= wire["status"] < 300 and math.isfinite(wire["elapsedMs"]) for wire in item["http"]),
                "Raw HTTP error or missing request")
        counts[name] += 1
        http[name] += len(item["http"])
        nodes[name].add(row["node"])
        latency[name][kind].append(item["elapsedMs"])
        if kind == "catalogCommit":
            # The pinned producer sets catalogTeamId only after its verified-state
            # assertion. Success records contain a null mutationState field.
            require(item["catalogTeamId"] and item["http"][-1]["route"].endswith("/execute")
                    and item["http"][-1]["status"] == 200, "Unverified governed effect")
            commits[name] += 1
    for name, phase in phases.items():
        require(counts[name] == phase["scheduled"] == phase["batches"] * phase["workers"] and counts[name] > 0,
                f"{name} raw operation count mismatch")
        require(http[name] == phase["httpCount"] and commits[name] == phase["verifiedCatalogCommits"] > 0,
                f"{name} raw HTTP/commit count mismatch")
        require(nodes[name] == ({"primary", "peer"} if provider == "postgres" else {"primary"}), "Missing required API node traffic")
        observed = {item["kind"]: item for item in phase["latency"]}
        for kind, values in latency[name].items():
            measured = distribution(values)
            require(observed[kind]["count"] == len(values), "Raw latency sample count mismatch")
            for metric in ("p95", "p99", "max"):
                require(math.isclose(measured[metric], observed[kind]["ms"][metric], rel_tol=1e-12), "Raw latency/checkpoint mismatch")
    for node in ("primary", "peer") if provider == "postgres" else ("primary",):
        counters = defaultdict(int)
        with (root / f"{node}-runtime-counters.csv").open(encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream):
                value = float(row["Mean/Increment"])
                require(math.isfinite(value), "Non-finite runtime counter")
                name = row["Counter Name"].split(" (")[0]
                # Queue length is exported as signed UpDownCounter deltas.
                if name in ("dotnet.gc.heap.total_allocated", "dotnet.gc.pause.time"):
                    require(value >= 0, "Negative monotonic GC/allocation counter")
                counters[name] += 1
        for counter in ("dotnet.gc.heap.total_allocated", "dotnet.gc.pause.time", "dotnet.thread_pool.queue.length"):
            require(counters[counter] >= 14_820, f"Incomplete {node} GC/allocation/queue counter coverage")


def band(values, *, floor=False):
    center = median(values)
    bound = min(values) if floor else max(values)
    return {
        "observed": distribution(values),
        "proposedFloor" if floor else "proposedCeiling": bound,
        "allowedRelativeRegression": abs(bound - center) / abs(center) if center else 0,
    }


def soak_growth(path):
    samples = [row for row in records(path) if row["phase"] == "soak-50"]
    require(len(samples) >= 6_840, "Soak has less than 95% of the required one-second samples.")
    width = 900
    result = {}
    for key in ("process", "peerProcess"):
        if samples[0].get(key) is None:
            continue
        blocks = [[row[key] for row in samples[index:index + width]] for index in range(0, len(samples) - width + 1, width)]
        times = [median(row["elapsedSeconds"] for row in block) for block in blocks]
        elapsed_hours = (times[-1] - times[0]) / 3600
        node = {}
        for metric in ("rssBytes", "handles", "threads"):
            medians = [median(row[metric] for row in block) for block in blocks]
            slopes = [(medians[i] - medians[i - 1]) * 3600 / (times[i] - times[i - 1]) for i in range(1, len(times))]
            node[metric] = {
                "observedNetGrowthPerHour": (medians[-1] - medians[0]) / elapsed_hours,
                "segmentGrowthPerHour": distribution(slopes),
                "proposedGrowthCeilingPerHour": max(0, max(slopes)),
            }
        result[key] = node
    return result


def container_by_phase(csv_path, resource_path):
    with csv_path.open(newline="", encoding="utf-8") as source:
        samples = list(csv.DictReader(source))
    by_phase = {}
    times = []
    values = []
    for previous, current in zip(samples, samples[1:]):
        start = float(previous["unixSeconds"])
        end = float(current["unixSeconds"])
        usec = int(current["cpuUsageUsec"]) - int(previous["cpuUsageUsec"])
        if 0.5 <= end - start <= 1.5 and usec >= 0:
            times.append(end)
            values.append((usec / ((end - start) * 10_000), int(current["memoryCurrentBytes"])))
    matched = set()
    for row in records(resource_path):
        stamp = datetime.fromisoformat(row["observedAtUtc"]).timestamp()
        index = bisect_left(times, stamp)
        nearest = min((i for i in (index - 1, index) if 0 <= i < len(times)),
                      default=None, key=lambda i: abs(times[i] - stamp))
        if nearest is not None and nearest not in matched and abs(times[nearest] - stamp) <= 0.75:
            matched.add(nearest)
            by_phase.setdefault(row["phase"], []).append(values[nearest])

    def summarize(phase, minimum):
        observed = by_phase.get(phase, [])
        require(len(observed) >= minimum, f"{phase} has insufficient cgroup samples.")
        return {
            "cpuPercentOfOneLogicalProcessor": distribution([item[0] for item in observed]),
            "memoryCurrentBytes": distribution([item[1] for item in observed]),
        }

    return summarize


def evaluate(run, provider):
    root = run / f"{provider}-reference"
    checkpoint_file = root / "checkpoint.json"
    checkpoint = read(checkpoint_file)
    provenance = read(run / "run-provenance.json")
    dataset = checkpoint["dataset"] or {}
    require(checkpoint["completed"] and checkpoint["cleaned"] and checkpoint["errors"] == 0, f"{provider} run incomplete or failed.")
    require(checkpoint["schedule"] == "reference" and provenance["fullDataset"], f"{provider} is not a full reference run.")
    require(provenance["testExit"] == 0 and provenance["ownedInfrastructureStopped"], f"{provider} fixture shutdown failed.")
    require(checkpoint["sourceSha"] == provenance["sourceSha"], f"{provider} source identity mismatch.")
    validate_raw(root, checkpoint, provider)
    require(all(dataset.get(key) == value for key, value in {
        "physicalKafkaClusters": 10, "actualBrokerTopics": 1000, "initialTeams": 100,
        "applications": 200, "policyRules": 100, "historicalRequests": 10_000, "historicalAttributionWindows": 43_200,
    }.items()), f"{provider} corpus is incomplete.")
    if provider == "postgres":
        require(provenance["physicalReplicationVerifiedAfterLoad"], "PostgreSQL standby did not replay the post-load barrier.")
    require(re.fullmatch(r"[0-9a-f]{64}", provenance["countersSha256"]) is not None,
            f"{provider} counter collector identity is missing.")
    for node in ("primary", "peer") if provider == "postgres" else ("primary",):
        counters = root / f"{node}-runtime-counters.csv"
        require(counters.stat().st_size > 512, f"{provider} {node} runtime counter samples are missing.")
    cancellation = read(root / "cancellation.json")
    require(cancellation["cancelled"] > 0 and cancellation["status"] == 200, f"{provider} cancellation recovery is missing.")
    env = checkpoint["environment"]
    require(env["apiNodeCount"] == env["persistenceNodeCount"] == (2 if provider == "postgres" else 1), f"{provider} topology mismatch.")
    cold = checkpoint["cold"]
    require(len(cold) == 5 and all(sample["status"] == 200 for sample in cold), f"{provider} cold-start evidence is incomplete.")
    phases = {row["name"]: row for row in checkpoint["completedPhases"]}
    raw = root / "resources.ndjson"
    observations = resources(raw)
    require(provenance.get("runtime") == "linux-cgroup-v2", f"{provider} is missing isolated cgroup provenance.")
    container = container_by_phase(run / "container-samples.csv", raw)
    host_cpu = {}
    for sample in records(raw):
        if sample.get("hostBusyPercent") is not None:
            host_cpu.setdefault(sample["phase"], []).append(sample["hostBusyPercent"])

    def phase(name, seconds, workers):
        row = phases.get(name)
        require(row and row["workers"] == workers and row["elapsedSeconds"] >= seconds, f"{provider} {name} did not reach its duration.")
        require(row["rejected"] == 0 and not row["outcomes"] and row["verifiedCatalogCommits"] > 0, f"{provider} {name} had errors or missed writes.")
        return row

    phase("warmup", 300, 1)
    soak = phase("soak-50", 7200, 50)
    workloads = {}
    for workers in (1, 10, 50):
        repeated = [phase(f"steady-{trial:02d}-{workers}", 900, workers) for trial in (1, 2, 3)]
        rows = [{item["kind"]: item["ms"] for item in row["latency"]} for row in repeated]
        kinds = ("read", "explain", "catalogCommit")
        require(all(all(kind in row for kind in kinds) for row in rows), f"{provider} is missing a latency family.")
        process = {}
        for key in ("process", "peerProcess") if provider == "postgres" else ("process",):
            samples = [observations[row["name"]][key] for row in repeated]
            require(all(row["rssBytes"]["samples"] >= 855 for row in samples), f"{provider} has insufficient resource samples.")
            process[key] = {
                "cpuP95PercentOfOneLogicalProcessor": band([row["cpuPercentOfOneLogicalProcessor"]["p95"] for row in samples]),
                "rssSteadyMedianBytes": band([row["rssBytes"]["median"] for row in samples]),
                "rssPeakBytes": band([row["rssBytes"]["max"] for row in samples]),
            }
        container_rounds = [container(row["name"], 855) for row in repeated]
        workloads[str(workers)] = {
            "repetitions": [row["name"] for row in repeated],
            "successfulOperationsPerSecond": band([row["successfulOperationsPerSecond"] for row in repeated], floor=True),
            "latencyMilliseconds": {kind: {p: band([row[kind][p] for row in rows]) for p in ("p95", "p99")} for kind in kinds},
            "process": process,
            "container": {
                name: band([row[metric][stat] for row in container_rounds])
                for name, metric, stat in (
                    ("cpuP95PercentOfOneLogicalProcessor", "cpuPercentOfOneLogicalProcessor", "p95"),
                    ("memorySteadyMedianBytes", "memoryCurrentBytes", "median"),
                    ("memoryPeakBytes", "memoryCurrentBytes", "max"),
                )
            },
            "hostBusyPercentByRepeat": {row["name"]: distribution(host_cpu[row["name"]]) for row in repeated},
            "allowedErrors": 0,
            "allowedHttp429": 0,
        }
    return {
        "sourceSha": checkpoint["sourceSha"],
        "apiDllSha256": provenance["apiDllSha256"],
        "harnessSha": provenance["harnessSha"],
        "checkpointSha256": hashlib.sha256(checkpoint_file.read_bytes()).hexdigest(),
        "baselineRunId": run.name,
        "environment": env,
        "dataset": dataset,
        "authenticatedColdStartMilliseconds": band([sample["authenticatedReadyMs"] for sample in cold]),
        "steady": workloads,
        "soak": {
            "resources": observations["soak-50"],
            "durationSeconds": soak["elapsedSeconds"],
            "operations": soak["scheduled"],
            "verifiedCatalogCommits": soak["verifiedCatalogCommits"],
            "successfulOperationsPerSecond": soak["successfulOperationsPerSecond"],
            "process": soak_growth(raw),
            "container": container("soak-50", 6_840),
            "hostBusyPercent": distribution(host_cpu["soak-50"]),
            "latencyMilliseconds": {item["kind"]: item["ms"] for item in soak["latency"]},
            "postLoadIdle": {
                node: {name: band([metrics[name]["median"], metrics[name]["max"]]) for name in ("rssBytes", "handles", "threads")}
                for node, metrics in observations["postload-idle"].items()
            },
        },
    }


