"""Verify retained actual Helm rows and index the public evidence whitelist."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parents[1]
SOURCE = "01c7553425fbc61ba4a7a21918265c562086ba02"
DRIVER = "da6bfbd17084526f917e0f05a2eb5025d96c00cc"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text())


rows = {}
for provider in ("Sqlite", "PostgreSql"):
    folder = ROOT / provider
    lifecycle = read(folder / "helm-lifecycle.json")
    producer = read(folder / "producer.json")
    assert lifecycle["applicationSourceSha"] == SOURCE and lifecycle["driverSha"] == DRIVER
    assert lifecycle["apiReplicas"] == 1 and lifecycle["persistenceExecutionMode"] == "Standalone"
    assert all(lifecycle[key] for key in ("install", "upgrade", "failedRolloutRollback", "uninstallReinstall", "restore",
        "restoredGenerationQuarantined", "owningGateAdmittedOffline", "ownedKindRegistryFixturesRemoved"))
    assert lifecycle["externalDatabaseHa"] == (provider == "PostgreSql")
    assert not lifecycle["published"] and not lifecycle["multiApiHaClaimed"] and not lifecycle["networkPolicyEnforcementClaimed"]
    before = read(folder / "state-initial.json")
    for phase in ("upgraded", "rollback", "reinstalled", *(["failover"] if provider == "PostgreSql" else [])):
        assert read(folder / f"state-{phase}.json") == before
    admitted = read(folder / "state-admitted.json")
    for table, contents in before.items():
        if table.endswith("_schema"):
            assert admitted[table] == contents
    receipt = json.loads(admitted["governance_generation_admissions"]["rows"][0])
    old = json.loads(before["governance_deployment_generation"]["rows"][0])
    current = json.loads(admitted["governance_deployment_generation"]["rows"][0])
    assert receipt[1] == old[1] and receipt[0] == current[1] and old[1] != current[1] and current[2] == 2
    assert receipt[2] == lifecycle["fenceSha256"] == sha(folder / "restore-fence.json")
    assert receipt[3] == lifecycle["restoreSha256"]
    journeys = [json.loads(line) for line in (folder / "journeys.ndjson").read_text().splitlines()]
    ready = {row["phase"] for row in journeys if row.get("realKeycloakHttpsPrivateKeyJwt")}
    assert ready == {"install", "upgrade", "rollback", "reinstall", "restored-admitted"} | ({"physical-db-failover"} if provider == "PostgreSql" else set())
    assert all(row["realKafkaTlsMetadata"] and row["catalogGenerationReadAllowed"] for row in journeys if row["phase"] in ready)
    assert len({row["exactFixtureMessageSha256"] for row in journeys if row["phase"] in ready}) == 1
    quarantine = next(row for row in journeys if row["phase"] == "restored-unadmitted")
    assert not quarantine["httpReady"] and quarantine["restoreQuarantineObserved"]
    pods = [json.loads(line) for line in (folder / "runtime-pods.ndjson").read_text().splitlines()]
    assert {row["phase"] for row in pods} == ready and len({row["podUid"] for row in pods}) == len(pods)
    assert all(row["runtimeImageId"].endswith("@" + lifecycle["runtimeImageManifestDigest"]) for row in pods)
    history = read(folder / "helm-history.json")
    assert any(row["status"] == "failed" for row in history) and history[-1]["status"] == "deployed"
    signed = read(folder / "signed-input.json")
    assert signed["signatureVerified"] and signed["allMediaHashesVerified"] and signed["applicationSourceSha"] == SOURCE
    assert signed["signedManifestSha256"] == "b63864259a8a5e1c3c9d8dc3312d35a56e82749d62e2f6a05b88112f98af29ba"
    if provider == "PostgreSql":
        replication = read(folder / "pg-replication.json")
        fence = read(folder / "pg-primary-fence.json")
        assert replication["physicalBasebackupVerified"] and replication["standbyReplayedBarrier"] and replication["syncState"] == "sync"
        assert fence["oldPrimaryStopped"] and fence["oldPrimaryPortClosed"]
    rows[provider] = {"status": "passed", "producer": producer, "reportSha256": sha(folder / "helm-lifecycle.json"),
        "readyAuthenticatedMessageStages": len(ready), "runtimePods": len(pods), "restoreQuarantineProved": True,
        "owningGenerationReceiptVerified": True, "schemaAndGenerationStateEqualityVerified": True}

boundary = WORKSPACE / "artifacts/v09-admission-kaf02"
selected = ["kaf02-deployment-retest.md", "observed-final/kaf02-deployment-result.json", "observed-final/restricted-policy.json",
    "observed-final/positive-syn.txt", "observed-final/restricted-syn.txt", "observed-final/cni-pod-rules.txt",
    "observed-final/k3s-network-controller.txt", "observed-final/lab-cleanup.txt"]
for name in selected:
    target = ROOT / "conditional-network-boundary" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(boundary / name, target)
assert sha(boundary / selected[0]) == "c0fc9419167fb8c0c837e15bf5078af1ecaa1547af3de4b2c2e0f600e8ca6e8f"
assert sha(boundary / selected[1]) == "dc07ba6a6114b570b4a222a87115309586cf4e519a9c821d79c5cee4fdd02220"
repository = WORKSPACE / "artifacts/v09-w82-next/w81-helm-auth"
policy = "deploy/helm/kafdeck/templates/network-policy.yaml"
before = subprocess.check_output(["git", "-C", str(repository), "show", "8f4f8b228f91ef6b51e9d679b09a47463a40154c:" + policy])
after = subprocess.check_output(["git", "-C", str(repository), "show", SOURCE + ":" + policy])
assert before == after and hashlib.sha256(after).hexdigest() == "b4f57d1cfbe5e606bd318fffc9e4e3748dc62192dc3ace788906e23e2712b021"
(ROOT / "conditional-network-boundary/network-policy.yaml").write_bytes(after)
result = {"schemaVersion": 1, "applicationSourceSha": SOURCE, "fixtureDriverSha": DRIVER,
    "chartSha256": lifecycle["signedChartSha256"], "imageConfigDigest": lifecycle["imageConfigDigest"],
    "runtimeImageManifestDigest": lifecycle["runtimeImageManifestDigest"], "profiles": rows,
    "conditionalNetworkBoundary": {"testedSourceSha": "8f4f8b228f91ef6b51e9d679b09a47463a40154c",
        "testedRuntime": "K3s1.34.13+k3s1 flannel-host-gw / kube-router2.11.1-k3s2",
        "unchangedPolicySha256": hashlib.sha256(after).hexdigest(),
        "scope": "Transferred unchanged chart ipBlock boundary on direct-IP external routes only. Kind functional tests provide no new CNI enforcement certification; Service DNAT and other CNIs require separate admission."},
    "newProductBuilds": 0, "publicationPerformed": False}
(ROOT / "verification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8", newline="\n")
print(json.dumps(rows, indent=2))
