"""Publish the owner-authorized stable bytes, retaining their signed source identity."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/release"))
from v09_prepublication import digest, extract, read, require, retrieve_artifact, validate_run, write

# Reuse the admitted OCI config/layer verifier and pinned transport implementation.
spec = importlib.util.spec_from_file_location("publication_transport", ROOT / "publication/v09/driver.py")
transport = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transport)
SOURCE = "0617a87ddc176b8255ca1452fc53c097008d9a51"
MANIFEST = "0cefda57cdef623f96739439b426a60321727ffebff2c1e4509feccc5a3f75bc"
BUNDLE = "8f228dc679edc4217a6ae5206d3781834ee69ab4be98e4a266b1d57c4c337590"
TRUST = "0ba58f6c09c271f664f0f8c1fa62e2065844ca89a0397c0178ed5188f6096724"
SIGN_RUN = 44134707888
SIGN_ARTIFACT = 14351412646
SIGN_ZIP = "67dd560bfbbd6b58bd7403e672394c33c10efce7f858f92d08d1494fdb3eff14"
MEDIA_ARTIFACT = 14350304738
MEDIA_ZIP = "2562a7650fa4c2b4fdc56b392ab0f16077af53d8869cb03231d38f47c7ab5698"
REPOSITORY = "araditc/Kafdeck"
WORKFLOW = ".github/workflows/v1-publication.yml"
TAG = "v1.0.0"
IMAGE = "ghcr.io/araditc/kafdeck"
OUT = Path("artifacts/v1-publication")
APPROVAL = f"PUBLISH {TAG} FROM {SOURCE} MANIFEST {MANIFEST}"
SIGN_IDENTITY = f"https://github.com/{REPOSITORY}/.github/workflows/v1-candidate-media.yml@refs/heads/main"


def authority():
    policy = json.loads(transport.run("git", "show", SOURCE + ":.github/release/release-owners.json"))
    require(all(os.environ.get(key, "").lower() in policy["actors"]
                for key in ("GITHUB_ACTOR", "GITHUB_TRIGGERING_ACTOR")), "Unapproved release actor")
    publisher = transport.run("git", "rev-parse", "HEAD")
    require(os.environ.get("OWNER_APPROVAL") == APPROVAL
            and os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REPOSITORY") == REPOSITORY
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_SHA") == publisher
            and os.environ.get("GITHUB_WORKFLOW_REF") == f"{REPOSITORY}/{WORKFLOW}@refs/heads/main",
            "Exact owner publication decision and protected workflow context required")
    branch = transport.repo_api("branches/main")
    require(branch["protected"] is True and branch["commit"]["sha"] == publisher, "Protected publisher main advanced")
    transport.run("git", "merge-base", "--is-ancestor", SOURCE, publisher)
    return publisher


def artifact(artifact_id, expected):
    item = transport.repo_api(f"actions/artifacts/{artifact_id}")
    require(item["digest"] == "sha256:" + expected and not item["expired"]
            and item["workflow_run"]["id"] == SIGN_RUN
            and item["workflow_run"]["head_sha"] == SOURCE, "Signed artifact identity mismatch")
    return item


def signature():
    folder = OUT / "signed"
    retrieve_artifact(artifact(SIGN_ARTIFACT, SIGN_ZIP), SIGN_RUN, SOURCE, folder)
    require(digest(folder / "manifest.json") == MANIFEST
            and digest(folder / "manifest.sigstore.json") == BUNDLE
            and digest(folder / "trusted-root.json") == TRUST, "Signed decision inputs differ")
    subprocess.run(["sudo", "unshare", "--net", shutil.which("cosign"), "verify-blob", "--offline",
                    "--bundle", str(folder / "manifest.sigstore.json"), "--trusted-root", str(folder / "trusted-root.json"),
                    "--certificate-identity", SIGN_IDENTITY, "--certificate-oidc-issuer", "https://token.actions.githubusercontent.com",
                    "--certificate-github-workflow-sha", SOURCE, str(folder / "manifest.json")], check=True)
    return folder


def image_layout(media, manifest):
    combined = OUT / "oci"
    blobs = combined / "blobs/sha256"
    blobs.mkdir(parents=True)
    platforms, descriptors = {}, []
    for arch in ("amd64", "arm64"):
        archive = media / "oci" / arch / f"kafdeck-linux-{arch}.docker.tar.gz"
        converted = OUT / ("converted-" + arch)
        expected = manifest["imageConfigDigests"][arch]
        transport.run("bash", "scripts/release/convert-v08-publication-image.sh", str(archive), expected, str(converted))
        original = read(converted / "image/manifest.json")
        require(original["config"]["digest"] == expected, "Conversion changed signed image config")
        for path in (converted / "image").iterdir():
            if re.fullmatch(r"[a-f0-9]{64}", path.name):
                require(digest(path) == path.name, "Converted blob hash differs")
                target = blobs / path.name
                if not target.exists():
                    os.link(path, target)
        config = {**original["config"], "mediaType": "application/vnd.oci.image.config.v1+json"}
        layers = [{**layer, "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip"} for layer in original["layers"]]
        encoded = json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                              "config": config, "layers": layers}, separators=(",", ":")).encode()
        image_hash = hashlib.sha256(encoded).hexdigest()
        (blobs / image_hash).write_bytes(encoded)
        descriptor = {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "sha256:" + image_hash,
                      "size": len(encoded), "platform": {"os": "linux", "architecture": arch}}
        platforms[arch] = transport.validate_arch(combined, descriptor, expected)
        descriptors.append(descriptor)
    encoded = json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json",
                          "manifests": descriptors}, separators=(",", ":")).encode()
    index_hash = hashlib.sha256(encoded).hexdigest()
    (blobs / index_hash).write_bytes(encoded)
    write(combined / "oci-layout", {"imageLayoutVersion": "1.0.0"})
    write(combined / "index.json", {"schemaVersion": 2, "manifests": [{"mediaType": "application/vnd.oci.image.index.v1+json",
          "digest": "sha256:" + index_hash, "size": len(encoded), "annotations": {"org.opencontainers.image.ref.name": "release"}}]})
    return "sha256:" + index_hash, platforms


def prepare():
    authority()
    require(not OUT.exists(), "Use a fresh publication working directory")
    producer = transport.repo_api(f"actions/runs/{SIGN_RUN}")
    validate_run(producer, SOURCE, {"signed": ("v1-candidate-media", ".github/workflows/v1-candidate-media.yml")})
    folder = signature()
    artifact(MEDIA_ARTIFACT, MEDIA_ZIP)
    archive = OUT / "kafdeck-1.0.0-full-distribution.zip"
    with archive.open("wb") as stream:
        subprocess.run(["gh", "api", f"repos/{REPOSITORY}/actions/artifacts/{MEDIA_ARTIFACT}/zip"], stdout=stream, check=True, timeout=900)
    extract(archive, OUT / "distribution", MEDIA_ZIP)
    require(digest(OUT / "distribution/manifest.json") == MANIFEST, "Distribution manifest differs")
    transport.run(sys.executable, "scripts/release/v1_media.py", "verify", str(OUT / "distribution"), "--manifest-sha256", MANIFEST)
    manifest = read(folder / "manifest.json")
    media = OUT / "distribution/media"
    index, platforms = image_layout(media, manifest)
    assets = OUT / "assets"
    assets.mkdir()
    selected = [(archive, archive.name)]
    selected += [(folder / name, name) for name in ("manifest.json", "manifest.sigstore.json", "trusted-root.json")]
    selected += [(media / entry["path"], "media__" + entry["path"].replace("/", "__")) for entry in manifest["files"]]
    selected += [(Path("publication/v1/release-notes.md"), "release-notes.md")]
    for path, name in selected:
        require(re.fullmatch(r"[A-Za-z0-9_.-]+", name) and path.stat().st_size < 2 * 1024**3, "Invalid asset")
        os.link(path, assets / name)
    selection = release_inventory(assets, index, platforms)
    write(assets / "publication-selection.json", selection)
    (assets / "SHA256SUMS").write_text("".join(digest(p) + "  " + p.name + "\n" for p in sorted(assets.iterdir())), encoding="utf-8")
    write(OUT / "publication-selection.json", selection)
    return selection


def release_inventory(assets, index, platforms):
    # Release assets retain their identity across retries from a newer protected main.
    # The attempt's publisher commit belongs in publication-result.json.
    return {"version": "1.0.0", "source": SOURCE, "manifestSha256": MANIFEST, "signatureBundleSha256": BUNDLE,
            "signingRun": SIGN_RUN, "image": IMAGE, "indexDigest": index, "platforms": platforms,
            "assets": [{"name": p.name, "sha256": digest(p), "bytes": p.stat().st_size} for p in sorted(assets.iterdir())]}


def verify_existing(tag, release, expected, body):
    require(tag is None or (tag["object"]["type"] == "commit" and tag["object"]["sha"] == SOURCE), "Existing tag points elsewhere")
    if release is not None:
        require(tag is not None and release["tag_name"] == TAG and release["body"] == body
                and release["name"] == "Kafdeck 1.0.0" and not release["prerelease"], "Existing release differs")
        require(all(a["name"] in expected and a.get("digest") == "sha256:" + expected[a["name"]]
                    for a in release["assets"]), "Existing asset bytes differ")


def publish(selection):
    publisher = authority()
    assets = OUT / "assets"
    expected = {p.name: digest(p) for p in assets.iterdir()}
    body = Path("publication/v1/release-notes.md").read_text(encoding="utf-8")
    body += f"\n\nSigned manifest SHA256 `{MANIFEST}`.\n\nOCI multi-architecture image `{IMAGE}@{selection['indexDigest']}`.\n"
    body += f"\n[Publication inventory](https://github.com/{REPOSITORY}/releases/download/{TAG}/publication-selection.json).\n"
    tag = transport.repo_api("git/ref/tags/" + TAG, optional=True)
    release = transport.repo_api("releases/tags/" + TAG, optional=True)
    verify_existing(tag, release, expected, body)
    privacy = transport.privacy()
    subprocess.run(["docker", "login", "ghcr.io", "--username", os.environ["GITHUB_ACTOR"], "--password-stdin"],
                   input=os.environ["REGISTRY_TOKEN"], text=True, check=True)
    before = transport.tag_snapshot(IMAGE)
    authority()
    transport.skopeo("copy", "--all", "--preserve-digests", "oci:/work/" + str(OUT / "oci") + ":release",
                     "docker://" + IMAGE + "@" + selection["indexDigest"])
    transport.OUT = OUT
    transport.verify_transport(selection, IMAGE)
    require(before == transport.tag_snapshot(IMAGE) and transport.privacy() == privacy, "Existing registry tags or visibility changed")
    authority()
    if tag is None:
        transport.repo_api("git/refs", data={"ref": "refs/tags/" + TAG, "sha": SOURCE})
    if release is None:
        release = transport.repo_api("releases", data={"tag_name": TAG, "target_commitish": SOURCE, "name": "Kafdeck 1.0.0",
                                                      "body": body, "draft": True, "prerelease": False})
    existing = {a["name"] for a in release["assets"]}
    for name in sorted(expected):
        if name not in existing:
            transport.run("gh", "release", "upload", TAG, "--repo", REPOSITORY, str(assets / name))
    downloaded = OUT / "remote-assets"
    transport.run("gh", "release", "download", TAG, "--repo", REPOSITORY, "--dir", str(downloaded))
    require({p.name: digest(p) for p in downloaded.iterdir()} == expected, "Downloaded Release assets differ")
    tag = transport.repo_api("git/ref/tags/" + TAG)
    release = transport.repo_api("releases/tags/" + TAG)
    verify_existing(tag, release, expected, body)
    require({a["name"] for a in release["assets"]} == set(expected), "Release inventory incomplete")
    authority()
    transport.run("gh", "release", "edit", TAG, "--repo", REPOSITORY, "--draft=false", "--latest")
    public = transport.repo_api("releases/tags/" + TAG)
    require(not public["draft"] and not public["prerelease"] and transport.privacy() == privacy, "Final publication state differs")
    write(OUT / "publication-result.json", {"published": True, "tag": TAG, "source": SOURCE, "manifestSha256": MANIFEST,
          "release": public["html_url"], "publisherCommit": publisher, "indexDigest": selection["indexDigest"],
          "platforms": selection["platforms"], "remoteAssetsVerified": expected, "registryVisibility": privacy,
          "registryTagSnapshotPreserved": True, "remoteImageConfigsAndLayersVerified": True})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "publish"))
    args = parser.parse_args()
    if args.command == "preflight":
        authority()
    else:
        publish(prepare())
