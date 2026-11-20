"""Publish an exact owner-authorized selection of signed v1 media."""
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
from typing import NamedTuple

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/release"))
from v09_prepublication import digest, extract, read, require, retrieve_artifact, validate_run, write

# Reuse the admitted OCI config/layer verifier and pinned transport implementation.
spec = importlib.util.spec_from_file_location("publication_transport", ROOT / "publication/v09/driver.py")
transport = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transport)
REPOSITORY = "araditc/Kafdeck"
WORKFLOW = ".github/workflows/v1-publication.yml"
IMAGE = "ghcr.io/araditc/kafdeck"
OUT = Path("artifacts/v1-publication")
SIGN_IDENTITY = f"https://github.com/{REPOSITORY}/.github/workflows/v1-candidate-media.yml@refs/heads/main"
TRUST = "0ba58f6c09c271f664f0f8c1fa62e2065844ca89a0397c0178ed5188f6096724"


class Publication(NamedTuple):
    version: str
    source: str
    signing_commit: str
    manifest: str
    bundle: str
    signing_run: int
    signature_artifact: int
    signature_zip: str
    media_artifact: int
    media_zip: str
    notes: Path
    prerelease: bool = False

    @property
    def tag(self):
        return "v" + self.version

    @property
    def approval(self):
        return f"PUBLISH {self.tag} FROM {self.source} MANIFEST {self.manifest}"


STABLE = Publication(
    version="1.0.0",
    source="0617a87ddc176b8255ca1452fc53c097008d9a51",
    signing_commit="0617a87ddc176b8255ca1452fc53c097008d9a51",
    manifest="0cefda57cdef623f96739439b426a60321727ffebff2c1e4509feccc5a3f75bc",
    bundle="8f228dc679edc4217a6ae5206d3781834ee69ab4be98e4a266b1d57c4c337590",
    signing_run=44134707888,
    signature_artifact=14351412646,
    signature_zip="67dd560bfbbd6b58bd7403e672394c33c10efce7f858f92d08d1494fdb3eff14",
    media_artifact=14350304738,
    media_zip="2562a7650fa4c2b4fdc56b392ab0f16077af53d8869cb03231d38f47c7ab5698",
    notes=Path("publication/v1/release-notes.md"),
)
RC = Publication(
    version="1.1.0-rc.1",
    source="ba60c2b0bd53d3928040e351673a8c07ffd2e658",
    signing_commit="459e9d7c055e7def6a52dc803d853cbb97a41885",
    manifest="3926c04165ea3ddffad6f576da0cc4475ad82c564ac8fe6ac9fc95a74d8591d4",
    bundle="409cfdeaa23f928de5c0e1821019ff0fdd4d55f886f46c913c0bb27997d6965a",
    signing_run=44766054199,
    signature_artifact=14646215657,
    signature_zip="c0a13ce8e1d860e50fd137b35d12416f87c7e03ad770d1f1fc215e9cb164df16",
    media_artifact=14646655305,
    media_zip="12742f02d106a1adf4107b6139bb42c070587f869f6cf33ae480928354a2ee1e",
    notes=Path("publication/v1/release-notes-1.1.0-rc.1.md"),
    prerelease=True,
)


STABLE_11 = Publication(
    version="1.1.0",
    source="13e297d2e98e414b6e3e8c7ef520f611bf992ad4",
    signing_commit="13e297d2e98e414b6e3e8c7ef520f611bf992ad4",
    manifest="fbde2e6f65e7e4559fbe59a6d1f228abbc2355c1d6435140153326cfd37a85e5",
    bundle="8c8da5acb40d9df2621c232497057bc2f0b34b6552c96411e5b7bb78f05ad88d",
    signing_run=44796139117,
    signature_artifact=14658478200,
    signature_zip="e08df94b4cf7b585973e2fe84d6d2dce3665319c6cc5f12725c8f473d012e62c",
    media_artifact=14658458052,
    media_zip="25b45f186932652527cef613b40c422513c01040c9453199401473d848c3e154",
    notes=Path("publication/v1/release-notes-1.1.0.md"),
)


def approved_publication(approval):
    options = {plan.approval: plan for plan in (STABLE, RC, STABLE_11)}
    require(approval in options, "Exact publication approval must select a signed manifest")
    return options[approval]


def authority(plan):
    policy = json.loads(transport.run("git", "show", plan.source + ":.github/release/release-owners.json"))
    require(all(os.environ.get(key, "").lower() in policy["actors"]
                for key in ("GITHUB_ACTOR", "GITHUB_TRIGGERING_ACTOR")), "Unapproved release actor")
    publisher = transport.run("git", "rev-parse", "HEAD")
    require(os.environ.get("OWNER_APPROVAL") == plan.approval
            and os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REPOSITORY") == REPOSITORY
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_SHA") == publisher
            and os.environ.get("GITHUB_WORKFLOW_REF") == f"{REPOSITORY}/{WORKFLOW}@refs/heads/main",
            "Exact owner publication decision and protected workflow context required")
    branch = transport.repo_api("branches/main")
    require(branch["protected"] is True and branch["commit"]["sha"] == publisher, "Protected publisher main advanced")
    transport.run("git", "merge-base", "--is-ancestor", plan.source, publisher)
    if plan.signing_commit != plan.source:
        transport.run("git", "merge-base", "--is-ancestor", plan.signing_commit, publisher)
    return publisher


def artifact(plan, artifact_id, expected):
    item = transport.repo_api(f"actions/artifacts/{artifact_id}")
    require(item["digest"] == "sha256:" + expected and not item["expired"]
            and item["workflow_run"]["id"] == plan.signing_run
            and item["workflow_run"]["head_sha"] == plan.signing_commit, "Signed artifact identity mismatch")
    return item


def signature(plan):
    folder = OUT / "signed"
    retrieve_artifact(artifact(plan, plan.signature_artifact, plan.signature_zip), plan.signing_run, plan.signing_commit, folder)
    require(digest(folder / "manifest.json") == plan.manifest
            and digest(folder / "manifest.sigstore.json") == plan.bundle
            and digest(folder / "trusted-root.json") == TRUST, "Signed decision inputs differ")
    subprocess.run(["sudo", "unshare", "--net", shutil.which("cosign"), "verify-blob", "--offline",
                    "--bundle", str(folder / "manifest.sigstore.json"), "--trusted-root", str(folder / "trusted-root.json"),
                    "--certificate-identity", SIGN_IDENTITY, "--certificate-oidc-issuer", "https://token.actions.githubusercontent.com",
                    "--certificate-github-workflow-sha", plan.signing_commit, str(folder / "manifest.json")], check=True)
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


def prepare(plan):
    authority(plan)
    require(not OUT.exists(), "Use a fresh publication working directory")
    producer = transport.repo_api(f"actions/runs/{plan.signing_run}")
    validate_run(producer, plan.signing_commit, {"signed": ("v1-candidate-media", ".github/workflows/v1-candidate-media.yml")})
    folder = signature(plan)
    artifact(plan, plan.media_artifact, plan.media_zip)
    archive = OUT / f"kafdeck-{plan.version}-full-distribution.zip"
    with archive.open("wb") as stream:
        subprocess.run(["gh", "api", f"repos/{REPOSITORY}/actions/artifacts/{plan.media_artifact}/zip"], stdout=stream, check=True, timeout=900)
    extract(archive, OUT / "distribution", plan.media_zip)
    require(digest(OUT / "distribution/manifest.json") == plan.manifest, "Distribution manifest differs")
    transport.run(sys.executable, "scripts/release/v1_media.py", "verify", str(OUT / "distribution"), "--manifest-sha256", plan.manifest)
    manifest = read(folder / "manifest.json")
    require(manifest["source"] == plan.source and manifest["version"] == plan.version, "Unexpected signed product identity")
    media = OUT / "distribution/media"
    index, platforms = image_layout(media, manifest)
    assets = OUT / "assets"
    assets.mkdir()
    selected = [(archive, archive.name)]
    selected += [(folder / name, name) for name in ("manifest.json", "manifest.sigstore.json", "trusted-root.json")]
    selected += [(media / entry["path"], "media__" + entry["path"].replace("/", "__")) for entry in manifest["files"]]
    selected += [(plan.notes, "release-notes.md")]
    for path, name in selected:
        require(re.fullmatch(r"[A-Za-z0-9_.-]+", name) and path.stat().st_size < 2 * 1024**3, "Invalid asset")
        os.link(path, assets / name)
    selection = release_inventory(plan, assets, index, platforms)
    write(assets / "publication-selection.json", selection)
    (assets / "SHA256SUMS").write_text("".join(digest(p) + "  " + p.name + "\n" for p in sorted(assets.iterdir())), encoding="utf-8")
    write(OUT / "publication-selection.json", selection)
    return selection


def release_inventory(plan, assets, index, platforms):
    # Release assets retain their identity across retries from a newer protected main.
    # The attempt's publisher commit belongs in publication-result.json.
    return {"version": plan.version, "source": plan.source, "manifestSha256": plan.manifest, "signatureBundleSha256": plan.bundle,
            "signingRun": plan.signing_run, "image": IMAGE, "indexDigest": index, "platforms": platforms,
            "assets": [{"name": p.name, "sha256": digest(p), "bytes": p.stat().st_size} for p in sorted(assets.iterdir())]}


def verify_existing(plan, tag, release, expected, body):
    require(tag is None or (tag["object"]["type"] == "commit" and tag["object"]["sha"] == plan.source), "Existing tag points elsewhere")
    if release is not None:
        require(tag is not None and release["tag_name"] == plan.tag and release["body"] == body
                and release["name"] == f"Kafdeck {plan.version}" and release["prerelease"] is plan.prerelease, "Existing release differs")
        require(all(a["name"] in expected and a.get("digest") == "sha256:" + expected[a["name"]]
                    for a in release["assets"]), "Existing asset bytes differ")


def existing_release(plan):
    # GitHub's by-tag endpoint returns 404 for drafts. The authenticated inventory
    # includes drafts, allowing a failed upload or finalization to resume safely.
    pages = json.loads(transport.run("gh", "api", f"repos/{REPOSITORY}/releases?per_page=100", "--paginate", "--slurp"))
    matches = [release for page in pages for release in page if release["tag_name"] == plan.tag]
    require(len(matches) <= 1, "Multiple release records target the selected tag")
    return matches[0] if matches else None


def release_body(plan, index_digest=None):
    body = plan.notes.read_text(encoding="utf-8")
    body += f"\n\nSigned manifest SHA256 `{plan.manifest}`.\n"
    if index_digest is not None:
        body += f"\nOCI multi-architecture image `{IMAGE}@{index_digest}`.\n"
    return body + f"\n[Publication inventory](https://github.com/{REPOSITORY}/releases/download/{plan.tag}/publication-selection.json).\n"


def owner_staged_draft(plan, release, actor):
    return bool(release and release["draft"] and not release["assets"]
                and release["author"]["login"].lower() == actor.lower()
                and release["body"] == release_body(plan))


def publish(plan, selection):
    publisher = authority(plan)
    assets = OUT / "assets"
    expected = {p.name: digest(p) for p in assets.iterdir()}
    body = release_body(plan, selection["indexDigest"])
    tag = transport.repo_api("git/ref/tags/" + plan.tag, optional=True)
    release = existing_release(plan)
    staged = owner_staged_draft(plan, release, os.environ["GITHUB_ACTOR"])
    verify_existing(plan, tag, release, expected, release_body(plan) if staged else body)
    privacy = transport.privacy()
    previous_latest = transport.repo_api("releases/latest")["tag_name"] if plan.prerelease else None
    subprocess.run(["docker", "login", "ghcr.io", "--username", os.environ["GITHUB_ACTOR"], "--password-stdin"],
                   input=os.environ["REGISTRY_TOKEN"], text=True, check=True)
    before = transport.tag_snapshot(IMAGE)
    authority(plan)
    transport.skopeo("copy", "--all", "--preserve-digests", "oci:/work/" + str(OUT / "oci") + ":release",
                     "docker://" + IMAGE + "@" + selection["indexDigest"])
    transport.OUT = OUT
    transport.verify_transport(selection, IMAGE)
    require(before == transport.tag_snapshot(IMAGE) and transport.privacy() == privacy, "Existing registry tags or visibility changed")
    authority(plan)
    if tag is None:
        transport.repo_api("git/refs", data={"ref": "refs/tags/" + plan.tag, "sha": plan.source})
    if release is None:
        release = transport.repo_api("releases", data={"tag_name": plan.tag, "target_commitish": plan.source, "name": f"Kafdeck {plan.version}",
                                                      "body": body, "draft": True, "prerelease": plan.prerelease,
                                                      "make_latest": "false"})
    if staged:
        # The owner can reserve an empty draft before the workflow computes the OCI index.
        endpoint = "releases/" + str(release["id"])
        release = transport.repo_api(endpoint)
        require(owner_staged_draft(plan, release, os.environ["GITHUB_ACTOR"]), "Owner-staged draft changed before upload")
        verify_existing(plan, tag, release, expected, release_body(plan))
        # GitHub can detach a draft when a PATCH omits its tag name.
        update = {"tag_name": plan.tag, "target_commitish": plan.source, "body": body}
        release = json.loads(transport.run("gh", "api", f"repos/{REPOSITORY}/{endpoint}", "--method", "PATCH",
                                            "--input", "-", input=json.dumps(update)))
        require(release["draft"] and not release["assets"], "Owner-staged draft changed during finalization")
        verify_existing(plan, tag, release, expected, body)
    existing = {a["name"] for a in release["assets"]}
    for name in sorted(expected):
        if name not in existing:
            transport.run("gh", "release", "upload", plan.tag, "--repo", REPOSITORY, str(assets / name))
    downloaded = OUT / "remote-assets"
    transport.run("gh", "release", "download", plan.tag, "--repo", REPOSITORY, "--dir", str(downloaded))
    require({p.name: digest(p) for p in downloaded.iterdir()} == expected, "Downloaded Release assets differ")
    tag = transport.repo_api("git/ref/tags/" + plan.tag)
    release = transport.repo_api("releases/" + str(release["id"]))
    verify_existing(plan, tag, release, expected, body)
    require({a["name"] for a in release["assets"]} == set(expected), "Release inventory incomplete")
    authority(plan)
    if plan.prerelease:
        require(transport.repo_api("releases/latest")["tag_name"] == previous_latest, "Latest stable release changed before finalization")
    latest_flag = "--latest=false" if plan.prerelease else "--latest"
    transport.run("gh", "release", "edit", plan.tag, "--repo", REPOSITORY, "--draft=false", latest_flag)
    public = transport.repo_api("releases/tags/" + plan.tag)
    latest = transport.repo_api("releases/latest")["tag_name"]
    require(not public["draft"] and public["prerelease"] is plan.prerelease
            and latest == (previous_latest if plan.prerelease else plan.tag)
            and transport.privacy() == privacy, "Final publication state differs")
    write(OUT / "publication-result.json", {"published": True, "tag": plan.tag, "source": plan.source, "manifestSha256": plan.manifest,
          "prerelease": plan.prerelease, "latestStableTag": latest,
          "release": public["html_url"], "publisherCommit": publisher, "indexDigest": selection["indexDigest"],
          "platforms": selection["platforms"], "remoteAssetsVerified": expected, "registryVisibility": privacy,
          "registryTagSnapshotPreserved": True, "remoteImageConfigsAndLayersVerified": True})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "publish"))
    args = parser.parse_args()
    plan = approved_publication(os.environ.get("OWNER_APPROVAL"))
    if args.command == "preflight":
        authority(plan)
    else:
        publish(plan, prepare(plan))
