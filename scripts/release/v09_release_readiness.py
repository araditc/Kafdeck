"""Read-only final evidence decision. Never signs, installs, tags or publishes."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

from v09_prepublication import (HASH, SHA, REPOSITORY, api, command, digest, read, require,
                                retrieve_artifact, safe_relative, validate_run, verify, write)

BASELINE = "cbedd025ab97db10fb21407ea5188c08d6a4d8ce"
SIGNER = {"signed": ("v09-prepublication-signing", ".github/workflows/v09-prepublication-signing.yml")}
W80_CHECKS = ("sqliteReleasedV08Upgrade", "sqliteBackupRestoreRollback", "sqliteInterruptedMigration",
              "postgresReleasedV08Upgrade", "postgresPhysicalPitr", "postgresInterruptedMigration",
              "generationFenceUnknownResume", "disconnectedAuthenticatedJourney")


def catalog(run_id):
    response = api(f"actions/runs/{run_id}/artifacts?per_page=100")
    require(response["total_count"] <= 100 and response["total_count"] == len(response["artifacts"]), "Incomplete artifact inventory")
    return response["artifacts"]


def artifact(run_id, name):
    matches = [item for item in catalog(run_id) if item["name"] == name]
    require(len(matches) == 1, "Required artifact missing or ambiguous: " + name)
    return matches[0]


def historical_baseline(run, job, reference, policy):
    # This explicit historical exception is not the protected-main product policy.
    require(run["id"] == reference["runId"] and run["head_sha"] == reference["driverSha"]
            and run["head_branch"] == policy["branch"] and run["event"] == "push"
            and run["status"] == "completed" and run["conclusion"] == reference["runConclusion"]
            and run["name"] == policy["workflowName"] and run["path"] == policy["workflowPath"]
            and run["repository"]["full_name"] == run["head_repository"]["full_name"] == REPOSITORY,
            "Historical baseline driver identity differs from the explicit admitted policy")
    require(job["id"] == reference["jobId"] and job["run_id"] == reference["runId"]
            and job["name"] == reference["jobName"] and job["status"] == "completed" and job["conclusion"] == "success",
            "The explicitly selected provider job did not succeed")


def receipt_binding(receipt, manifest, manifest_hash, root_hash):
    require(receipt["schemaVersion"] == 1 and receipt["signatureVerified"] is True and receipt["mediaVerified"] is True
            and receipt["sourceSha"] == receipt["workflowSourceSha"] == manifest["sourceSha"]
            and receipt["scope"] == manifest["scope"] == "full-distribution"
            and receipt["manifestSha256"] == manifest_hash and receipt["trustedRootSha256"] == root_hash
            and receipt["networkNamespace"] == "loopback-only"
            and receipt["ipv4DefaultRoute"] is False and receipt["ipv6DefaultRoute"] is False,
            "Signed producer installation receipt identity/isolation differs")
    native, oci = receipt["installation"]["native"], receipt["installation"]["oci"]
    require(native["rid"] == "linux-x64" and native["sourceSha"] == manifest["sourceSha"]
            and all(native[key] is True for key in ("installedFromArchives", "cliHelp", "mcpInitialize", "mcpToolCatalog"))
            and all(native["apiUi"][key] is True for key in ("health", "staticFrontend")), "Actual signed native installation is missing")
    require(oci["architecture"] == "amd64" and oci["loadedFromVerifiedArchive"] is True
            and oci["configDigest"] == manifest["content"]["imageConfigDigests"]["amd64"]
            and oci["network"] == "none" and oci["uid"] == 1654
            and all(oci[key] is True for key in ("readOnlyRoot", "health", "staticFrontend")), "Actual signed OCI installation is missing")


def local_w80(encoded, approval, manifest, manifest_hash):
    if not encoded:
        return None, ["W80 owner-reviewed exact signed-media cross-version recovery and disconnected authenticated journey packet is missing"]
    require(len(encoded) <= 65536, "W80 packet input exceeds bound")
    raw = base64.b64decode(encoded, validate=True)
    packet_hash = hashlib.sha256(raw).hexdigest()
    packet = json.loads(raw)
    require(set(packet) == {"schemaVersion", "authority", "sourceSha", "baselineSourceSha", "signedManifestSha256",
                            "candidateWindowsApiArchiveSha256", "testHost", "scripts", "evidence", "checks", "disconnectedFixtureOnly"},
            "W80 packet must contain only the public evidence whitelist")
    # An owner-reviewed local packet is explicitly an attestation, not a forged
    # GitHub protected-main producer or a substitute for actual named test evidence.
    subprocess.run(["bash", "scripts/release/assert-v08-release-owner.sh"], check=True)
    expected = f'ACCEPT W80 {packet_hash} FOR {manifest["sourceSha"]} MANIFEST {manifest_hash}'
    require(approval == expected, "Missing exact owner review of the local W80 packet")
    require(packet["schemaVersion"] == 1 and packet["authority"] == "owner-reviewed-local"
            and packet["sourceSha"] == manifest["sourceSha"] and packet["baselineSourceSha"] == BASELINE
            and packet["signedManifestSha256"] == manifest_hash, "W80 packet source/signed candidate differs")
    archive = next(item for item in manifest["content"]["files"] if item["path"] == "native/win-x64/kafdeck-api-ui-win-x64.tar.gz")
    require(packet["candidateWindowsApiArchiveSha256"] == archive["sha256"], "W80 candidate archive differs from the signed Windows media")
    require(all(isinstance(packet["testHost"].get(key), str) and packet["testHost"][key].strip()
                for key in ("id", "os", "architecture")), "W80 actual test host identity is missing")
    require(set(packet["testHost"]) == {"id", "os", "architecture"} and set(packet["checks"]) <= set(W80_CHECKS),
            "Unexpected W80 host/check fields")
    for group in ("scripts", "evidence"):
        require(isinstance(packet[group], list) and packet[group], "W80 reproducible script/evidence hashes are missing")
        for item in packet[group]:
            require(set(item) == {"path", "sha256"}, "Unexpected private W80 evidence field")
            safe_relative(item["path"])
            require(HASH.fullmatch(item["sha256"]), "Invalid W80 evidence digest")
    missing = ["W80 actual test missing: " + check for check in W80_CHECKS if packet.get("checks", {}).get(check) is not True]
    if packet.get("disconnectedFixtureOnly") is not True:
        missing.append("W80 disconnected identity/broker fixture isolation is unproven")
    return {"sha256": packet_hash, "packet": packet, "authority": "owner-reviewed-local"}, missing


def assemble(source, signed_run_id, manifest_hash, root_hash, output):
    require(SHA.fullmatch(source) and HASH.fullmatch(manifest_hash) and HASH.fullmatch(root_hash)
            and re.fullmatch(r"[1-9][0-9]{0,19}", signed_run_id), "Invalid exact readiness input")
    require(os.environ.get("GITHUB_REPOSITORY") == REPOSITORY and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and os.environ.get("GITHUB_REF") == "refs/heads/main" and os.environ.get("GITHUB_SHA") == source
            and os.environ.get("GITHUB_WORKFLOW_REF") == f"{REPOSITORY}/.github/workflows/v09-release-readiness.yml@refs/heads/main"
            and command("git", "rev-parse", "HEAD") == source, "Readiness must execute the exact protected main workflow")
    require(api("git/ref/heads/main")["object"]["sha"] == source, "Current protected main differs from the requested source")
    require(not output.exists(), "Use a fresh readiness output directory")
    output.mkdir(parents=True)
    run = api(f"actions/runs/{signed_run_id}")
    validate_run(run, source, SIGNER)
    require(run["event"] == "workflow_dispatch", "Official signing must originate in owner-authorized dispatch")
    owners = read(".github/release/release-owners.json")["actors"]
    require(all(run[key]["login"].lower() in owners for key in ("actor", "triggering_actor")), "Signing producer actor is not a release owner")
    signed_artifact = artifact(signed_run_id, f"v09-prepublication-signed-{signed_run_id}")
    signed = output / "signed"
    retrieve_artifact(signed_artifact, signed_run_id, source, signed)
    require(digest(signed / "manifest.json") == manifest_hash, "Signed manifest differs from reviewed SHA256")
    manifest = verify(signed)
    require(manifest["sourceSha"] == manifest["workflowSourceSha"] == source and manifest["scope"] == "full-distribution",
            "Final decision requires the complete signed native/distribution matrix on exact protected main")
    subprocess.run(["sudo", "unshare", "--net", "env", "-i", "PATH=/usr/sbin:/usr/bin:/sbin:/bin",
                    "COSIGN=" + shutil.which("cosign"), "bash", str(Path("scripts/release/verify-v09-signature.sh").resolve()),
                    str(signed.resolve()), source, root_hash], check=True, timeout=180)
    receipt = read(signed / "offline-verification.json")
    receipt_binding(receipt, manifest, manifest_hash, root_hash)
    # Corroborate every immutable original producer; do not reexecute installations.
    run_cache, catalog_cache = {}, {}
    for producer in manifest["producers"]:
        run_id = str(producer["runId"])
        if run_id not in run_cache:
            run_cache[run_id] = api(f"actions/runs/{run_id}")
            catalog_cache[run_id] = catalog(run_id)
        validate_run(run_cache[run_id], source)
        require((run_cache[run_id]["name"], run_cache[run_id]["path"]) == (producer["workflowName"], producer["workflowPath"])
                and run_cache[run_id]["run_attempt"] == producer["runAttempt"], "Original producer workflow/attempt differs")
        matches = [item for item in catalog_cache[run_id] if item["id"] == producer["artifactId"]]
        require(len(matches) == 1 and matches[0]["name"] == producer["artifactName"]
                and matches[0]["digest"] == "sha256:" + producer["zipSha256"]
                and matches[0]["workflow_run"]["head_sha"] == source, "Signed media producer artifact identity differs")
    policy = read(".github/release/v0.9-baseline-producers.json")
    budget_path = Path("docs/performance/v0.9-budget-candidate.json")
    budget = read(budget_path)
    require(policy["schemaVersion"] == 1 and digest(budget_path) == policy["admittedBudgetSha256"]
            and budget["admission"]["ownerAccepted"] is True
            and budget["admission"]["acceptedProposalSha256"] == policy["acceptedProposalSha256"],
            "Budget differs from the exact owner-admitted canonical artifact")
    baselines = {}
    for provider, reference in policy["providers"].items():
        baseline_run = api(f'actions/runs/{reference["runId"]}')
        job = api(f'actions/jobs/{reference["jobId"]}')
        historical_baseline(baseline_run, job, reference, policy)
        provider_budget = budget["providers"][provider]
        require(provider_budget["githubRunId"] == str(reference["runId"]) and provider_budget["harnessSha"] == reference["driverSha"]
                and provider_budget["sourceSha"] == policy["applicationSourceSha"], "Admitted budget provenance differs from frozen provider selection")
        item = artifact(str(reference["runId"]), reference["artifactName"])
        require(item["id"] == reference["artifactId"] and item["digest"] == "sha256:" + reference["zipSha256"], "Frozen raw baseline artifact differs")
        destination = output / "raw" / provider
        retrieve_artifact(item, reference["runId"], reference["driverSha"], destination)
        provenance = read(destination / "w82-evidence/run-provenance.json")
        require(provenance["sourceSha"] == policy["applicationSourceSha"] and provenance["harnessSha"] == reference["driverSha"], "Measured application and frozen driver source differ")
        baselines[provider] = {**reference, "applicationSourceSha": provenance["sourceSha"], "wrapperBranch": baseline_run["head_branch"],
                               "selectedJobConclusion": job["conclusion"]}
    check = subprocess.run(["python3", "scripts/ci/check-w82-budget.py", "--budget", str(budget_path),
                            "--sqlite", str(output / "raw/sqlite/w82-evidence"), "--postgres", str(output / "raw/postgres/w82-evidence"),
                            "--source", source], capture_output=True, text=True, timeout=180)
    missing = []
    if check.returncode:
        missing.append("W82 admitted numeric/default-build enforcement failed: " + check.stderr.strip())
        budget_result = {"releaseAdmission": False}
    else:
        budget_result = json.loads(check.stdout)
        require(budget_result["status"] == "admitted-budget-passed" and budget_result["releaseAdmission"] is True, "W82 checker did not grant actual enforcement admission")
    w80, w80_missing = local_w80(os.environ.get("W80_PACKET_BASE64", ""), os.environ.get("W80_OWNER_APPROVAL", ""), manifest, manifest_hash)
    missing += w80_missing
    decision = {"schemaVersion": 1, "version": "0.9.0", "sourceSha": source, "publicationPerformed": False,
                "releaseReady": not missing, "missing": missing, "signedManifestSha256": manifest_hash,
                "trustedRootSha256": root_hash, "signedProducerRunId": int(signed_run_id),
                "signedArtifactId": signed_artifact["id"], "signedArtifactZipSha256": signed_artifact["digest"],
                "signedInstallationReceiptSha256": digest(signed / "offline-verification.json"),
                "nativeRids": manifest["content"]["nativeRids"], "w82BudgetSha256": digest(budget_path),
                "w82HistoricalBaselines": baselines, "w82Enforcement": budget_result, "w80": w80,
                "scope": "final signed distribution, accepted per-provider budgets and explicitly reviewed local W80 acceptance; no publication authorization"}
    write(output / "release-decision.json", decision)
    (output / "release-decision.sha256").write_text(digest(output / "release-decision.json") + "  release-decision.json\n")
    print(json.dumps({"releaseReady": decision["releaseReady"], "missing": missing, "decisionSha256": digest(output / "release-decision.json")}))
    return not missing


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--signed-run-id", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--trusted-root-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    raise SystemExit(0 if assemble(arguments.source, arguments.signed_run_id, arguments.manifest_sha256,
                                  arguments.trusted_root_sha256, arguments.output) else 1)
