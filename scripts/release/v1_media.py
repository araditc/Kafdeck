"""Assemble unsigned v1 candidate media from successful main runs and verify offline."""
import argparse
import hashlib
import json
import re
import subprocess
import tarfile
from pathlib import Path
from xml.etree import ElementTree
from zipfile import ZipFile

import v1_rc
from v09_prepublication import (
    REPOSITORY, api, digest, read, require, retrieve_artifact, validate_run, write,
)

RUNS = {
    "native": ("v1-rc-native", ".github/workflows/v1-rc-native.yml"),
    "distribution": ("v09-w81-distribution-candidate", ".github/workflows/v09-w81-distribution-candidate.yml"),
    "integrations": ("v08-g2-live-acceptance", ".github/workflows/v08-g2-live-acceptance.yml"),
}
SELECTION = Path(__file__).with_name("v1_media_selection.json")


def targets(source, rids):
    native = {f"v1-rc-native-{rid}-{source}": ("native", f"native/{rid}") for rid in rids}
    images = {f"w81-oci-{arch}-{source}": ("distribution", f"oci/{arch}") for arch in ("amd64", "arm64")}
    return {**native, **images, f"w81-helm-lifecycle-{source}": ("distribution", "helm"),
            "g4-external-integration-packages": ("integrations", "integrations")}


def check_image(folder, arch, source):
    report = read(folder / "oci-native-startup.json")
    archive = folder / f"kafdeck-linux-{arch}.docker.tar.gz"
    require(report["sourceSha"] == source and report["architecture"] == arch
            and digest(archive) == report["archiveSha256"], "Image archive or producer mismatch")
    with tarfile.open(archive, "r:gz") as tar:
        entries = json.load(tar.extractfile("manifest.json"))
        require(len(entries) == 1, "Expected one image")
        name = entries[0]["Config"]
        require(re.fullmatch(r"(?:blobs/sha256/[a-f0-9]{64}|[a-f0-9]{64}\.json)", name), "Invalid image config")
        config = tar.extractfile(name).read(4 * 1024**2 + 1)
    config_id = "sha256:" + hashlib.sha256(config).hexdigest()
    metadata = json.loads(config)
    require(config_id == report["configDigest"] and metadata["architecture"] == arch
            and metadata["os"] == "linux", "Image configuration mismatch")
    require(report["uid"] == 1654 and report["readOnlyRoot"] is True and report["network"] == "none"
            and all(report[p][k] is True for p in ("startup", "restart")
                    for k in ("networkNone", "health", "staticFrontend")), "Missing isolated startup evidence")
    sbom, sarif = folder / "sbom.spdx.json", folder / "trivy-results.sarif"
    require(digest(sbom) == report["sbomSha256"] and read(sbom)["spdxVersion"].startswith("SPDX-"), "Invalid SBOM")
    scans = read(sarif)["runs"]
    require(digest(sarif) == report["scanSha256"] and scans and all(s["results"] == [] for s in scans),
            "Image scan contains findings or differs from producer")
    return config_id


def check_media(media, policy, version):
    source = policy["source"]
    chosen = v1_rc.selection()
    expected = set()

    def include(folder, names):
        expected.update(folder / name for name in names)

    for rid in chosen["nativeRids"]:
        folder = media / "native" / rid
        manifest = folder / "rc-manifest.json"
        packet = v1_rc.verify(folder, source, policy["nativeInputSha256"], digest(manifest))
        require(packet["rid"] == rid and packet["version"] == version, "Native package mismatch")
        include(folder, ["rc-manifest.json", "native-evidence.json", *(c["archive"] for c in packet["components"].values())])

    configs = {}
    for arch in ("amd64", "arm64"):
        folder = media / "oci" / arch
        configs[arch] = check_image(folder, arch, source)
        include(folder, [f"kafdeck-linux-{arch}.docker.tar.gz", "oci-native-startup.json", "sbom.spdx.json", "trivy-results.sarif"])

    folder = media / "helm"
    chart = folder / f"kafdeck-{version}.tgz"
    lifecycle = read(folder / "helm-lifecycle.json")
    require(lifecycle["sourceSha"] == source and lifecycle["configDigest"] == configs["amd64"]
            and lifecycle["chartSha256"] == digest(chart), "Chart differs from installed artifact")
    require(all(lifecycle[k] is True for k in ("install", "upgrade", "uninstall", "reinstall", "retainedClaimAndFile", "verifiedHttps")),
            "Incomplete Helm lifecycle")
    with tarfile.open(chart) as tar:
        chart_yaml = tar.extractfile("kafdeck/Chart.yaml").read().decode()
        for key in ("version", "appVersion"):
            require(re.search(rf"(?m)^{key}:\s*[\"']?{re.escape(version)}[\"']?\s*$", chart_yaml), "Chart version mismatch")
    include(folder, [chart.name, "helm-lifecycle.json"])

    folder = media / "integrations"
    mcp = folder / "mcp" / f"Kafdeck.Mcp.{version}.nupkg"
    with ZipFile(mcp) as package:
        spec = ElementTree.fromstring(package.read("Kafdeck.Mcp.nuspec"))
        require(spec.findtext(".//{*}version") == version, "MCP version mismatch")
    expected.add(mcp)
    provider = folder / "terraform"
    prefix = "terraform-provider-kafdeck_" + chosen["terraformVersion"] + "_"
    sums = provider / (prefix + "SHA256SUMS")
    platforms = ["darwin_amd64", "darwin_arm64", "linux_amd64", "linux_arm64", "windows_amd64"]
    archives = {prefix + platform + ".zip" for platform in platforms}
    rows = [line.split() for line in sums.read_text().splitlines()]
    require(len(rows) == len(archives) and {name for _, name in rows} == archives
            and all(digest(provider / name) == value for value, name in rows), "Terraform checksum mismatch")
    include(provider, [*archives, sums.name, "build-modules.txt"])
    actual = list(media.rglob("*"))
    require(not any(p.is_symlink() for p in actual) and {p for p in actual if p.is_file()} == expected,
            "Unexpected media inventory")
    return [{"path": p.relative_to(media).as_posix(), "size": p.stat().st_size, "sha256": digest(p)}
            for p in sorted(expected, key=lambda path: path.relative_to(media).as_posix())]


def prepare(output, policy):
    require(not output.exists(), "Use a fresh output directory")
    source = policy["source"]
    subprocess.run(["git", "merge-base", "--is-ancestor", source, "HEAD"], check=True)
    version = v1_rc.selection()["version"]
    expected = targets(source, v1_rc.selection()["nativeRids"])
    receipts = []
    output.mkdir(parents=True)
    for kind, run_id in policy["runs"].items():
        run = api(f"actions/runs/{run_id}")
        require(validate_run(run, source, RUNS) == kind, "Unexpected producer")
        catalog = api(f"actions/runs/{run_id}/artifacts?per_page=100")
        require(catalog["total_count"] == len(catalog["artifacts"]), "Incomplete catalog")
        for item in catalog["artifacts"]:
            name = item["name"]
            if name not in expected:
                continue
            producer, relative = expected.pop(name)
            require(producer == kind, "Wrong workflow for archive")
            artifact = api(f"actions/artifacts/{item['id']}")
            require(artifact["name"] == name and artifact["digest"] == item["digest"], "Artifact metadata mismatch")
            retrieve_artifact(artifact, run_id, source, output / "media" / relative)
            receipts.append({"artifact": name, "id": artifact["id"], "zipDigest": artifact["digest"],
                             "run": run_id, "attempt": run["run_attempt"], "workflow": RUNS[kind][1]})
    require(not expected, f"Missing artifacts: {list(expected)}")
    write(output / "manifest.json", {"schemaVersion": 1, "repository": REPOSITORY, "version": version,
          "source": source, "scope": "full-distribution", "producers": sorted(receipts, key=lambda x: x["artifact"]),
          "files": check_media(output / "media", policy, version)})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["prepare", "verify"])
    parser.add_argument("directory", type=Path)
    parser.add_argument("--manifest-sha256")
    args = parser.parse_args()
    policy = read(SELECTION)
    manifest = args.directory / "manifest.json"
    if args.operation == "prepare":
        prepare(args.directory, policy)
    else:
        require(digest(manifest) == args.manifest_sha256, "Manifest differs from independently pinned SHA256")
        value = read(manifest)
        version = v1_rc.selection()["version"]
        require(value["source"] == policy["source"] and value["version"] == version and value["repository"] == REPOSITORY
                and value["scope"] == "full-distribution" and value["files"] == check_media(args.directory / "media", policy, version),
                "Selected media differs from manifest")
    print(digest(manifest))


if __name__ == "__main__":
    main()
