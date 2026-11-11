"""Assemble authenticated CI media; authorize an exact nonpublishing signing decision.

No local-file import mode: production manifests originate in successful main workflows.
Offline verification never calls GitHub and never grants publication readiness.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
from xml.etree import ElementTree
from zipfile import ZipFile

REPOSITORY = "araditc/Kafdeck"
WORKFLOW = ".github/workflows/v09-prepublication-signing.yml"
RIDS = ("win-x64", "linux-x64", "linux-arm64", "osx-x64", "osx-arm64")
SCOPES = ("staging-distribution", "full-distribution")
SHA = re.compile(r"[0-9a-f]{40}")
HASH = re.compile(r"[0-9a-f]{64}")
PRODUCERS = {
    "distribution": ("v09-w81-distribution-candidate", ".github/workflows/v09-w81-distribution-candidate.yml"),
    "integrations": ("v08-g2-live-acceptance", ".github/workflows/v08-g2-live-acceptance.yml"),
    "native": ("v09-native-distribution", ".github/workflows/v09-native-distribution.yml"),
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def command(*args):
    return subprocess.check_output(args, text=True, timeout=120).strip()


def api(endpoint):
    return json.loads(command("gh", "api", f"repos/{REPOSITORY}/{endpoint}"))


def local_guard():
    """Reject identities/context before any network or artifact I/O."""
    policy = read(".github/release/release-owners.json")
    owners = set()
    for line in Path(".github/CODEOWNERS").read_text().splitlines():
        fields = line.split()
        if fields and fields[0] == "/.github/":
            owners.update(x[1:].lower() for x in fields[1:] if x.startswith("@"))
    require(policy["schemaVersion"] == 1 and set(policy["actors"]) == owners and owners,
            "Owner policy differs from protected CODEOWNERS")
    require(all(os.environ.get(k, "").lower() in owners for k in ("GITHUB_ACTOR", "GITHUB_TRIGGERING_ACTOR")),
            "Both original and triggering actors must be release owners")
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REPOSITORY") == REPOSITORY
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_WORKFLOW_REF") == f"{REPOSITORY}/{WORKFLOW}@refs/heads/main",
            "Signing preparation must run from the protected main workflow")
    policy_sha = os.environ.get("GITHUB_SHA", "")
    require(SHA.fullmatch(policy_sha) and command("git", "rev-parse", "HEAD") == policy_sha,
            "Checked policy/workflow revision differs from GITHUB_SHA")
    return policy_sha


def remote_guard(source, scope):
    policy_sha = local_guard()
    require(SHA.fullmatch(source) and scope in SCOPES, "Invalid source SHA or scope")
    require(api("git/ref/heads/main")["object"]["sha"] == policy_sha, "Protected main advanced; repeat preparation")
    require(subprocess.run(["git", "merge-base", "--is-ancestor", source, policy_sha], check=False).returncode == 0,
            "Producer source must be an ancestor of the protected workflow revision")
    require(scope != "full-distribution" or source == policy_sha,
            "Full distribution requires exact current main, not an earlier staging source")
    return policy_sha


def validate_run(run, source):
    require(run.get("head_sha") == source and run.get("head_branch") == "main"
            and run.get("event") in ("push", "workflow_dispatch")
            and run.get("status") == "completed" and run.get("conclusion") == "success",
            "Producer must be a successful completed main run of the exact source")
    require(run.get("repository", {}).get("full_name") == REPOSITORY
            and run.get("head_repository", {}).get("full_name") == REPOSITORY,
            "Foreign producer repository")
    kinds = [kind for kind, (name, path) in PRODUCERS.items()
             if run.get("name") == name and run.get("path", "").split("@")[0] == path]
    require(len(kinds) == 1, "Untrusted producer workflow name/path")
    return kinds[0]


def expected_artifacts(source, scope):
    result = {f"w81-oci-{arch}-{source}": ("distribution", f"distribution/w81-oci-{arch}-{source}")
              for arch in ("amd64", "arm64")}
    result[f"w81-helm-lifecycle-{source}"] = ("distribution", f"distribution/w81-helm-lifecycle-{source}")
    result["g4-external-integration-packages"] = ("integrations", "integrations")
    if scope == "full-distribution":
        result.update({f"v09-native-{rid}-{source}": ("native", f"native/{rid}") for rid in RIDS})
    return result


def safe_relative(name):
    require(name and "\\" not in name and "\x00" not in name, "Unsafe media path")
    path = PurePosixPath(name)
    require(not path.is_absolute() and all(part not in ("..", "") and ":" not in part for part in path.parts)
            and str(path) == name, "Unsafe or noncanonical media path")
    return path


def extract(zip_path, target, expected_hash):
    require(HASH.fullmatch(expected_hash) and digest(zip_path) == expected_hash,
            "Downloaded artifact ZIP differs from GitHub's immutable digest")
    with ZipFile(zip_path) as archive:
        entries = archive.infolist()
        require(len(entries) <= 10000 and sum(x.file_size for x in entries) <= 4 * 1024**3,
                "Artifact exceeds extraction bounds")
        seen = set()
        for item in entries:
            name = item.filename.rstrip("/")
            relative = safe_relative(name)
            require(name not in seen and not stat.S_ISLNK(item.external_attr >> 16),
                    "Duplicate or symlink ZIP entry")
            seen.add(name)
            output = target.joinpath(*relative.parts)
            if item.is_dir():
                output.mkdir(parents=True, exist_ok=True)
            else:
                output.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(item) as source, output.open("wb") as destination:
                    shutil.copyfileobj(source, destination)


def selected_files(name, source):
    if name.startswith("w81-oci-"):
        arch = name.split("-")[2]
        return [f"kafdeck-linux-{arch}.docker.tar.gz", "oci-native-startup.json", "sbom.spdx.json", "trivy-results.sarif"]
    if name.startswith("w81-helm-"):
        return ["kafdeck-0.9.0.tgz", "helm-lifecycle.json"]
    if name == "g4-external-integration-packages":
        prefix = "terraform/terraform-provider-kafdeck_0.8.0_"
        return ["mcp/Kafdeck.Mcp.0.9.0.nupkg", prefix + "SHA256SUMS"] + [prefix + x + ".zip" for x in
                ("darwin_amd64", "darwin_arm64", "linux_amd64", "linux_arm64", "windows_amd64")]
    rid = name.removeprefix("v09-native-").removesuffix("-" + source)
    require(rid in RIDS, "Unexpected native artifact")
    return ["native-evidence.json"] + [f"kafdeck-{component}-{rid}.tar.gz" for component in ("api-ui", "cli", "mcp")]


def content(root, source, scope):
    """Verify selected bytes and producer acceptance evidence, online or offline."""
    def entry(path):
        require(path.is_file() and not path.is_symlink(), f"Missing regular media file: {path.name}")
        return {"path": path.relative_to(root).as_posix(), "size": path.stat().st_size, "sha256": digest(path)}
    result = {"files": [], "imageConfigDigests": {}, "nativeRids": []}
    for name, (_, relative) in expected_artifacts(source, scope).items():
        for filename in selected_files(name, source):
            result["files"].append(entry(root / relative / filename))
    result["files"].sort(key=lambda x: x["path"])
    tree = list(root.rglob("*"))
    require(not any(path.is_symlink() for path in tree), "Symlink media tree is prohibited")
    require({path.relative_to(root).as_posix() for path in tree if path.is_file()}
            == {item["path"] for item in result["files"]}, "Unmanifested media files are prohibited")
    for arch in ("amd64", "arm64"):
        folder = root / f"distribution/w81-oci-{arch}-{source}"
        report = read(folder / "oci-native-startup.json")
        image = folder / f"kafdeck-linux-{arch}.docker.tar.gz"
        require(report["sourceSha"] == source and report["architecture"] == arch, "OCI source/platform mismatch")
        require(report["uid"] == 1654 and report["readOnlyRoot"] is True and report["network"] == "none",
                "OCI native isolation evidence is incomplete")
        require(all(report[phase][key] is True for phase in ("startup", "restart")
                    for key in ("networkNone", "health", "staticFrontend")), "OCI startup/restart did not pass")
        with tarfile.open(image, "r:gz") as archive:
            metadata = json.load(archive.extractfile("manifest.json"))
            require(len(metadata) == 1, "Expected one archived OCI image")
            config_name = metadata[0]["Config"]
            require(re.fullmatch(r"([0-9a-f]{64}\.json|blobs/sha256/[0-9a-f]{64})", config_name), "Unsafe OCI config path")
            config_data = archive.extractfile(config_name).read(4 * 1024**2 + 1)
            require(len(config_data) <= 4 * 1024**2, "OCI config exceeds bound")
        config = json.loads(config_data)
        config_digest = "sha256:" + hashlib.sha256(config_data).hexdigest()
        require(config_digest == report["configDigest"] and config["architecture"] == arch and config["os"] == "linux",
                "OCI config identity differs from native evidence")
        require(digest(image) == report["archiveSha256"], "OCI archive differs from native evidence")
        sbom, scan = folder / "sbom.spdx.json", folder / "trivy-results.sarif"
        require(digest(sbom) == report["sbomSha256"] and read(sbom)["spdxVersion"].startswith("SPDX-"), "SBOM identity mismatch")
        runs = read(scan)["runs"]
        require(digest(scan) == report["scanSha256"] and runs and all(not run.get("results") for run in runs),
                "High/Critical scan differs or contains findings")
        result["imageConfigDigests"][arch] = config_digest
    folder = root / f"distribution/w81-helm-lifecycle-{source}"
    lifecycle = read(folder / "helm-lifecycle.json")
    require(lifecycle["sourceSha"] == source and lifecycle["configDigest"] == result["imageConfigDigests"]["amd64"]
            and digest(folder / "kafdeck-0.9.0.tgz") == lifecycle["chartSha256"], "Helm identity mismatch")
    require(all(lifecycle[key] is True for key in ("install", "upgrade", "uninstall", "reinstall", "retainedClaimAndFile", "verifiedHttps")),
            "Helm lifecycle evidence is incomplete")
    with tarfile.open(folder / "kafdeck-0.9.0.tgz", "r:gz") as chart:
        chart_yaml = chart.extractfile("kafdeck/Chart.yaml").read().decode()
        require(re.search(r"(?m)^version:\s*[\"']?0\.9\.0[\"']?\s*$", chart_yaml)
                and re.search(r"(?m)^appVersion:\s*[\"']?0\.9\.0[\"']?\s*$", chart_yaml), "Helm version mismatch")
    with ZipFile(root / "integrations/mcp/Kafdeck.Mcp.0.9.0.nupkg") as package:
        nuspec = ElementTree.fromstring(package.read("Kafdeck.Mcp.nuspec"))
        require(nuspec.findtext(".//{*}version") == "0.9.0", "MCP version mismatch")
    provider = root / "integrations/terraform"
    rows = [line.split() for line in (provider / "terraform-provider-kafdeck_0.8.0_SHA256SUMS").read_text().splitlines()]
    expected = {Path(x).name for x in selected_files("g4-external-integration-packages", source) if x.endswith(".zip")}
    require(len(rows) == 5 and all(len(row) == 2 for row in rows) and {x[1] for x in rows} == expected,
            "Terraform platform checksum set differs")
    require(all(HASH.fullmatch(value) and digest(provider / name) == value for value, name in rows), "Terraform checksum mismatch")
    if scope == "full-distribution":
        for rid in RIDS:
            folder = root / "native" / rid
            evidence = read(folder / "native-evidence.json")
            require(evidence["schemaVersion"] == 1 and evidence["sourceSha"] == source and evidence["version"] == "0.9.0"
                    and evidence["rid"] == rid and evidence["installedFromArchives"] is True,
                    "Native archive install/source evidence mismatch")
            require(set(evidence["components"]) == {"api-ui", "cli", "mcp"}, "Incomplete native component matrix")
            for component, test in evidence["components"].items():
                filename = f"kafdeck-{component}-{rid}.tar.gz"
                require(test["archive"] == filename and digest(folder / filename) == test["sha256"]
                        and test["started"] is True and test["authenticatedOperation"] is True,
                        "Native component archive/startup/authenticated operation mismatch")
            result["nativeRids"].append(rid)
    return result


def prepare(source, scope, run_ids, output):
    policy_sha = remote_guard(source, scope)
    ids = run_ids.split(",")
    require(1 <= len(ids) <= 8 and len(set(ids)) == len(ids) and all(re.fullmatch(r"[1-9][0-9]{0,19}", x) for x in ids),
            "Expected distinct numeric producer run IDs")
    require(not output.exists(), "Output already exists; use a fresh directory")
    output.mkdir(parents=True)
    expected, producers, found = expected_artifacts(source, scope), [], set()
    with tempfile.TemporaryDirectory() as temporary:
        temp = Path(temporary)
        for run_id in sorted(ids, key=int):
            run = api(f"actions/runs/{run_id}")
            kind = validate_run(run, source)
            catalog = api(f"actions/runs/{run_id}/artifacts?per_page=100")
            require(catalog["total_count"] <= 100 and catalog["total_count"] == len(catalog["artifacts"]), "Incomplete artifact inventory")
            used = False
            for artifact in catalog["artifacts"]:
                name = artifact["name"]
                if name not in expected:
                    continue
                require(expected[name][0] == kind and name not in found and artifact.get("expired") is False,
                        "Duplicate, expired or wrong-workflow artifact")
                binding = artifact.get("workflow_run", {})
                require(str(binding.get("id")) == run_id and binding.get("head_sha") == source,
                        "Artifact source/run binding differs")
                artifact_digest = artifact.get("digest", "")
                require(re.fullmatch(r"sha256:[0-9a-f]{64}", artifact_digest), "GitHub artifact has no immutable SHA256 digest")
                zip_path = temp / f'{artifact["id"]}.zip'
                with zip_path.open("wb") as stream:
                    subprocess.run(["gh", "api", f'repos/{REPOSITORY}/actions/artifacts/{artifact["id"]}/zip'],
                                   stdout=stream, check=True, timeout=900)
                extracted = temp / str(artifact["id"])
                extract(zip_path, extracted, artifact_digest.removeprefix("sha256:"))
                target = output / "media" / expected[name][1]
                target.mkdir(parents=True)
                for filename in selected_files(name, source):
                    source_path = extracted / filename
                    require(source_path.is_file() and not source_path.is_symlink(), f"Missing selected asset {filename}")
                    destination = target / filename
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source_path, destination)
                producers.append({"runId": int(run_id), "runAttempt": run["run_attempt"], "workflowPath": PRODUCERS[kind][1],
                                  "workflowName": PRODUCERS[kind][0], "artifactId": artifact["id"], "artifactName": name,
                                  "zipSha256": artifact_digest.removeprefix("sha256:")})
                used = True
                found.add(name)
            require(used, "Selected producer run supplies no required artifact")
    require(found == set(expected), "Missing required artifacts: " + ", ".join(sorted(set(expected) - found)))
    manifest = {"schemaVersion": 1, "version": "0.9.0", "repository": REPOSITORY, "sourceSha": source,
                "workflowSourceSha": policy_sha, "scope": scope, "releaseReady": False,
                "producers": sorted(producers, key=lambda x: x["artifactName"]),
                "content": content(output / "media", source, scope)}
    write(output / "manifest.json", manifest)
    print(digest(output / "manifest.json"))


def verify(directory):
    manifest = read(directory / "manifest.json")
    require(manifest["schemaVersion"] == 1 and manifest["version"] == "0.9.0" and manifest["repository"] == REPOSITORY
            and manifest["releaseReady"] is False and manifest["scope"] in SCOPES
            and SHA.fullmatch(manifest["sourceSha"]) and SHA.fullmatch(manifest["workflowSourceSha"]), "Invalid manifest identity/scope")
    expected = expected_artifacts(manifest["sourceSha"], manifest["scope"])
    require(manifest["scope"] != "full-distribution" or manifest["sourceSha"] == manifest["workflowSourceSha"],
            "Full manifest producer and protected workflow sources differ")
    producers = manifest["producers"]
    require(len(producers) == len(expected) and {x["artifactName"] for x in producers} == set(expected)
            and len({x["artifactId"] for x in producers}) == len(producers), "Incomplete or duplicate producer manifest")
    for producer in producers:
        kind = expected[producer["artifactName"]][0]
        require((producer["workflowName"], producer["workflowPath"]) == PRODUCERS[kind]
                and HASH.fullmatch(producer["zipSha256"]) and type(producer["runId"]) is int and producer["runId"] > 0
                and type(producer["artifactId"]) is int and producer["artifactId"] > 0
                and type(producer["runAttempt"]) is int and producer["runAttempt"] > 0, "Invalid producer provenance")
    for entry in manifest["content"]["files"]:
        safe_relative(entry["path"])
    require(content(directory / "media", manifest["sourceSha"], manifest["scope"]) == manifest["content"],
            "Media content or acceptance evidence differs from the signed manifest")
    return manifest


def authorize(directory, expected_hash, approval):
    # Never acquire OIDC/sign or install signers before this returns successfully.
    local_guard()
    require(HASH.fullmatch(expected_hash) and digest(directory / "manifest.json") == expected_hash,
            "Manifest differs from the owner's reviewed SHA256")
    manifest = verify(directory)
    expected = f'SIGN v0.9 {manifest["scope"]} FROM {manifest["sourceSha"]} MANIFEST {expected_hash}'
    require(approval == expected, "Missing exact owner authorization for this source, scope and manifest")
    require(remote_guard(manifest["sourceSha"], manifest["scope"]) == manifest["workflowSourceSha"],
            "Prepared manifest policy revision differs from signing workflow revision")
    print("Exact owner-approved nonpublishing manifest authorized")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    assembly = commands.add_parser("prepare")
    assembly.add_argument("--source", required=True)
    assembly.add_argument("--scope", choices=SCOPES, required=True)
    assembly.add_argument("--run-ids", required=True)
    assembly.add_argument("--output", type=Path, required=True)
    check = commands.add_parser("verify")
    check.add_argument("directory", type=Path)
    authorize_parser = commands.add_parser("authorize")
    authorize_parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.source, args.scope, args.run_ids, args.output)
    elif args.command == "verify":
        verify(args.directory)
        print("Selected media and acceptance evidence verified; publication readiness remains false")
    else:
        authorize(args.directory, os.environ.get("MANIFEST_SHA256", ""), os.environ.get("OWNER_APPROVAL", ""))


if __name__ == "__main__":
    main()
