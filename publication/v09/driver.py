"""Owner-guarded v0.9 publication driver; prepare/local-transport are read-only externally."""
import argparse
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import zipfile

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts/release"))
from v09_prepublication import (HASH, SHA, REPOSITORY, digest, extract, read, require, retrieve_artifact, safe_relative, validate_run, verify, write)

SOURCE = "01c7553425fbc61ba4a7a21918265c562086ba02"
MANIFEST = "b63864259a8a5e1c3c9d8dc3312d35a56e82749d62e2f6a05b88112f98af29ba"
TRUST = "0ba58f6c09c271f664f0f8c1fa62e2065844ca89a0397c0178ed5188f6096724"
SIGNED_RUN, SIGNED_ARTIFACT = 42984770583, 13839715488
SIGNED_ZIP = "7100754a28ebc405a8ca9739412341a3e13587f6e9514882ab547f2e62b8c295"
BRANCH = "codex/v09-publication"
WORKFLOW = ".github/workflows/v09-publication.yml"
IMAGE = "ghcr.io/araditc/kafdeck"
W80_PACKET = "5e86572f5983aca73cbdd28136194267db6ef323c8e95907d4fdeb45f7a1b01f"
W81_INDEX = "e9e6510c2f170773f9cb6e81ceb9525eba0d0ff5426e813b048591a34867064c"
OUT = Path("artifacts/v09-publication")


def run(*args, **kwargs):
    return subprocess.check_output(args, text=True, timeout=900, **kwargs).strip()


def gh(endpoint, *, data=None, optional=False):
    args = ["gh", "api", endpoint]
    if data is not None:
        args += ["--method", "POST", "--input", "-"]
    result = subprocess.run(args, input=json.dumps(data) if data is not None else None, capture_output=True, text=True, timeout=120)
    if result.returncode:
        if optional and "HTTP 404" in result.stderr:
            return None
        raise ValueError("GitHub API operation failed: " + endpoint)
    return json.loads(result.stdout) if result.stdout.strip() else None


def repo_api(path, **kwargs):
    return gh(f"repos/{REPOSITORY}" + ("/" + path if path else ""), **kwargs)


def skopeo(*args):
    return run("bash", "-c", 'source scripts/release/skopeo-v08-tool.sh; skopeo_pinned "$@"', "skopeo", *args)


def tag_names(raw):
    return sorted(json.loads(raw).get("Tags") or [])


def tag_snapshot(reference, local=False):
    flags = ["--tls-verify=false"] if local else []
    result = {}
    for tag in tag_names(skopeo("list-tags", *flags, "docker://" + reference)):
        data = subprocess.check_output(["bash", "-c", 'source scripts/release/skopeo-v08-tool.sh; skopeo_pinned "$@"', "skopeo",
                                       "inspect", *flags, "--raw", "docker://" + reference + ":" + tag], timeout=120)
        result[tag] = hashlib.sha256(data).hexdigest()
    return result


def preserve_tags(before, after, proof_tags=()):
    allowed = set(proof_tags)
    # A reserved spelling is not proof of ownership/type. Existing discoveries
    # require separate review; fail closed instead of overwriting any old tag.
    require(not set(before) & allowed, "Pre-existing reserved proof tag collision requires explicit review")
    require(all(after.get(tag) == value for tag, value in before.items())
            and set(after) - set(before) <= allowed, "Foreign/application registry tag bytes changed")


def proof_tag_names(selection):
    return {"sha256-" + value.removeprefix("sha256:") + suffix
            for value in [selection["indexDigest"], *[item["manifestDigest"] for item in selection["platforms"].values()]]
            for suffix in ("", ".sig", ".att")}


def guard_unused_proof_tags(snapshot, selection):
    require(not set(snapshot) & proof_tag_names(selection),
            "Pre-existing reserved proof tag collision requires explicit review before any registry mutation")


def owners():
    # Authority comes from accepted immutable protected source, not mutable driver policy.
    policy = json.loads(run("git", "show", SOURCE + ":.github/release/release-owners.json"))
    codeowners = run("git", "show", SOURCE + ":.github/CODEOWNERS")
    expected = {actor[1:].lower() for line in codeowners.splitlines() if line.startswith("/.github/ ") for actor in line.split()[1:] if actor.startswith("@")}
    require(policy["schemaVersion"] == 1 and expected == set(policy["actors"]) and expected, "Immutable owner policy mismatch")
    return expected


def local_actor_guard():
    approved = owners()
    require(all(re.fullmatch(r"[A-Za-z0-9-]+", os.environ.get(key, "")) and os.environ[key].lower() in approved
                for key in ("GITHUB_ACTOR", "GITHUB_TRIGGERING_ACTOR")), "Original and triggering actors must both be immutable release owners")
    return approved


def approval_text(selection_hash, publisher, w80_hash):
    return f'PUBLISH v0.9 SOURCE {SOURCE} MANIFEST {MANIFEST} W80 {w80_hash} PUBLISHER {publisher} SELECTION {selection_hash} AND SIGN REGISTRY INDEX AND ARCHITECTURES WITH LOCKED PUBLISHER IDENTITY ONLY AFTER VERIFIED MAIN READINESS'


def authority(selection, expected_hash, approval, *, write_intent):
    # Reject unauthorized actors/context/decision before any GitHub/registry I/O.
    approved = local_actor_guard()
    publisher = run("git", "rev-parse", "HEAD")
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REPOSITORY") == REPOSITORY
            and os.environ.get("GITHUB_REF") == "refs/heads/" + BRANCH and os.environ.get("GITHUB_SHA") == publisher
            and os.environ.get("GITHUB_WORKFLOW_REF") == f"{REPOSITORY}/{WORKFLOW}@refs/heads/{BRANCH}", "Wrong publisher workflow context")
    if write_intent:
        require(os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch" and HASH.fullmatch(expected_hash)
                and digest(OUT / "publication-selection.json") == expected_hash and selection["publisherCommit"] == publisher,
                "Publication requires the exact reviewed selection and publisher commit")
        require(HASH.fullmatch(selection.get("w80PacketSha256") or ""), "Final W80 packet hash is required")
        expected = approval_text(expected_hash, publisher, selection["w80PacketSha256"])
        require(approval == expected, "Missing exact conditional owner publication authorization")
    branch = repo_api("branches/" + urllib.parse.quote(BRANCH, safe=""))
    require(branch["commit"]["sha"] == publisher, "Publisher branch advanced from the checked revision")
    if write_intent:
        require(branch["protected"] is True, "Reviewed publisher branch must actually be protected before publication")
    main = repo_api("branches/main")
    require(main["protected"] is True and main["commit"]["sha"] == SOURCE, "Accepted protected main source changed")
    if write_intent:
        active = repo_api("actions/runs/" + os.environ["GITHUB_RUN_ID"])
        require(active["head_sha"] == publisher and active["head_branch"] == BRANCH and active["event"] == "workflow_dispatch"
                and active["path"] == WORKFLOW and active["repository"]["full_name"] == active["head_repository"]["full_name"] == REPOSITORY
                and all(active[key]["login"].lower() in approved for key in ("actor", "triggering_actor")), "Actual publisher run actors/source differ")
    return branch["protected"]


def preflight():
    local_actor_guard()
    publisher = run("git", "rev-parse", "HEAD")
    selection_hash = os.environ.get("SELECTION_SHA256", "")
    require(os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch" and os.environ.get("PUBLISHER_COMMIT") == publisher
            and HASH.fullmatch(selection_hash) and os.environ.get("OWNER_APPROVAL") == approval_text(selection_hash, publisher, W80_PACKET),
            "Write job requires the exact owner decision and reviewed publisher commit before preparation")
    require(authority({}, "", "", write_intent=False) is True, "Actual publisher branch must already be protected")


def privacy():
    owner = repo_api("")["owner"]
    prefix = "orgs" if owner["type"] == "Organization" else "users"
    package = gh(f'{prefix}/{owner["login"]}/packages/container/kafdeck')
    require(package["visibility"] == "private" and package["name"] == "kafdeck", "Existing GHCR package must remain private")
    return {"packageId": package["id"], "visibility": package["visibility"]}


def blob(layout, descriptor):
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", descriptor["digest"]), "Unsupported OCI digest")
    path = layout / "blobs/sha256" / descriptor["digest"].split(":")[1]
    require(path.is_file() and path.stat().st_size == descriptor["size"] and "sha256:" + digest(path) == descriptor["digest"], "OCI blob identity differs")
    return path


def validate_arch(layout, descriptor, expected_config):
    image = json.loads(blob(layout, descriptor).read_bytes())
    require(image["config"]["digest"] == expected_config, "Converted OCI config differs from signed archive config")
    config = json.loads(blob(layout, image["config"]).read_bytes())
    require(len(image["layers"]) == len(config["rootfs"]["diff_ids"]), "OCI layer/config cardinality mismatch")
    for layer, expected in zip(image["layers"], config["rootfs"]["diff_ids"]):
        path = blob(layout, layer)
        opener = gzip.open if layer["mediaType"].endswith("+gzip") else open
        with opener(path, "rb") as stream:
            actual = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
        require(actual == expected, "Converted OCI layer no longer matches the signed config's uncompressed diff ID")
    return {"manifestDigest": descriptor["digest"], "manifestSize": descriptor["size"], "configDigest": expected_config,
            "layers": image["layers"], "signedUncompressedLayerDiffIds": config["rootfs"]["diff_ids"]}


def w81_supplement(public=Path("publication/v09/w81-public"), output=None):
    index_file = public / "index.json"
    require(index_file.is_file(), "Frozen public W81 supplement is required; missing evidence is not admission")
    require(digest(index_file) == W81_INDEX, "W81 index differs from the frozen owner-review proposal")
    index = read(index_file)
    require(index["schemaVersion"] == 1 and index["sourceSha"] == SOURCE and index["signedManifestSha256"] == MANIFEST,
            "W81 supplement source/manifest differs from the accepted media")
    require(index["acceptedW80ProposalSha256"] == W80_PACKET and index["allAdmittedCellsPassed"] is True
            and index["mandatoryMissing"] == [] and index["status"] == "technical-complete-pending-owner-adoption",
            "W81 technical evidence is incomplete or not the exact pending owner-adoption proposal")
    require(isinstance(index["files"], list) and index["files"], "W81 public raw evidence inventory is missing")
    expected = {"index.json", "index.sha256"}
    for entry in index["files"]:
        relative = str(safe_relative(entry["path"]))
        require(relative not in expected and HASH.fullmatch(entry["sha256"]), "Duplicate/invalid W81 public evidence identity")
        path = public / relative
        require(path.is_file() and not path.is_symlink() and digest(path) == entry["sha256"]
                and path.stat().st_size == entry["size"], "W81 public evidence bytes differ")
        expected.add(relative)
    actual = {path.relative_to(public).as_posix() for path in public.rglob("*") if path.is_file()}
    require(not any(path.is_symlink() for path in public.rglob("*")) and actual <= expected, "Unlisted W81 files are prohibited")
    if (public / "index.sha256").exists():
        require((public / "index.sha256").read_text().split()[0] == digest(index_file), "Frozen W81 index SHA256 differs")
    result = {"sourceSha": SOURCE, "signedManifestSha256": MANIFEST, "indexSha256": digest(index_file),
              "indexedPublicFiles": len(index["files"]), "ownerAdoptionRequired": True}
    if output is not None:
        archive_path = output / "v0.9-w81-public-evidence.zip"
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
            for path in sorted(public.rglob("*")):
                if path.is_file():
                    info = zipfile.ZipInfo(path.relative_to(public).as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
                    info.compress_type, info.external_attr = zipfile.ZIP_DEFLATED, 0o100644 << 16
                    archive.writestr(info, path.read_bytes(), compresslevel=6)
        result["archiveSha256"] = digest(archive_path)
        result["assets"] = [(index_file, "v0.9-w81-supplement-index.json"), (archive_path, archive_path.name)]
    return result


def prepare(w80_hash):
    w80_hash = w80_hash or W80_PACKET
    require(w80_hash == W80_PACKET, "Only the exact frozen public W80 proposal is admitted for selection")
    local_actor_guard()
    w81_supplement()  # Local frozen-evidence check before heavy media/network preparation.
    require(not OUT.exists(), "Use a fresh publisher workspace")
    OUT.mkdir(parents=True)
    publisher = run("git", "rev-parse", "HEAD")
    protected = authority({}, "", "", write_intent=False)
    producer = repo_api(f"actions/runs/{SIGNED_RUN}")
    validate_run(producer, SOURCE, {"signed": ("v09-prepublication-signing", ".github/workflows/v09-prepublication-signing.yml")})
    item = repo_api(f"actions/artifacts/{SIGNED_ARTIFACT}")
    require(item["digest"] == "sha256:" + SIGNED_ZIP and item["workflow_run"]["head_sha"] == SOURCE and not item["expired"], "Official signed artifact differs")
    mirror = OUT / "kafdeck-v0.9-signed-offline-mirror.zip"
    with mirror.open("wb") as stream:
        subprocess.run(["gh", "api", f"repos/{REPOSITORY}/actions/artifacts/{SIGNED_ARTIFACT}/zip"], stdout=stream, check=True, timeout=900)
    signed = OUT / "signed"
    extract(mirror, signed, SIGNED_ZIP)
    require(digest(signed / "manifest.json") == MANIFEST and digest(signed / "trusted-root.json") == TRUST, "Reviewed signed manifest/root changed")
    subprocess.run(["bash", "scripts/release/verify-v09-signature.sh", str(signed), SOURCE, TRUST], check=True)
    manifest = verify(signed)
    require(manifest["scope"] == "full-distribution" and manifest["sourceSha"] == manifest["workflowSourceSha"] == SOURCE, "Full exact accepted media required")
    combined = OUT / "oci"
    (combined / "blobs/sha256").mkdir(parents=True)
    platforms, descriptors = {}, []
    for arch in ("amd64", "arm64"):
        layout = OUT / ("oci-" + arch)
        archive = signed / f"media/distribution/w81-oci-{arch}-{SOURCE}/kafdeck-linux-{arch}.docker.tar.gz"
        converted = OUT / ("converted-" + arch)
        expected_config = manifest["content"]["imageConfigDigests"][arch]
        # Force-format conversion may normalize config JSON. The immutable main
        # helper instead preserves exact config bytes while compressing layers.
        run("bash", "scripts/release/convert-v08-publication-image.sh", str(archive), expected_config, str(converted))
        docker_manifest = read(converted / "image/manifest.json")
        require(docker_manifest["config"]["digest"] == expected_config, "Signed config changed during compressed-dir conversion")
        (layout / "blobs/sha256").mkdir(parents=True)
        for path in (converted / "image").iterdir():
            if HASH.fullmatch(path.name):
                require(digest(path) == path.name, "Converted blob name/hash differs")
                os.link(path, layout / "blobs/sha256" / path.name)
        # OCI descriptor wrapping changes media-type declarations only; config
        # JSON and compressed layer bytes are retained unchanged and checked.
        config = {**docker_manifest["config"], "mediaType": "application/vnd.oci.image.config.v1+json"}
        layers = [{**item, "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip"} for item in docker_manifest["layers"]]
        image_bytes = json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                                  "config": config, "layers": layers}, separators=(",", ":")).encode()
        image_hash = hashlib.sha256(image_bytes).hexdigest()
        (layout / "blobs/sha256" / image_hash).write_bytes(image_bytes)
        descriptor = {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "sha256:" + image_hash, "size": len(image_bytes)}
        write(layout / "oci-layout", {"imageLayoutVersion": "1.0.0"})
        write(layout / "index.json", {"schemaVersion": 2, "manifests": [{**descriptor, "annotations": {"org.opencontainers.image.ref.name": "image"}}]})
        (converted / "candidate-image.tar").unlink()
        platforms[arch] = validate_arch(layout, descriptor, manifest["content"]["imageConfigDigests"][arch])
        for path in (layout / "blobs/sha256").iterdir():
            target = combined / "blobs/sha256" / path.name
            if not target.exists():
                os.link(path, target)
            else:
                require(digest(path) == digest(target), "OCI blob name collision")
        descriptors.append({"mediaType": descriptor["mediaType"], "digest": descriptor["digest"], "size": descriptor["size"],
                            "platform": {"os": "linux", "architecture": arch}})
    index = json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": descriptors}, separators=(",", ":")).encode()
    index_hash = hashlib.sha256(index).hexdigest()
    (combined / "blobs/sha256" / index_hash).write_bytes(index)
    write(combined / "oci-layout", {"imageLayoutVersion": "1.0.0"})
    write(combined / "index.json", {"schemaVersion": 2, "manifests": [{"mediaType": "application/vnd.oci.image.index.v1+json",
        "digest": "sha256:" + index_hash, "size": len(index), "annotations": {"org.opencontainers.image.ref.name": "release"}}]})
    assets = OUT / "assets"
    assets.mkdir()
    public = Path("publication/v09/w80-public")
    require(digest(public / "owner-w80-packet.json") == w80_hash, "Public W80 proposal changed")
    packet = read(public / "owner-w80-packet.json")
    prefix = "artifacts/v09-w82-next/w80-final-01c7553/"
    for item in packet["scripts"] + packet["evidence"]:
        require(item["path"].startswith(prefix) and digest(public / item["path"].removeprefix(prefix)) == item["sha256"], "Public W80 script/evidence bytes differ")
    w80_archive = OUT / "v0.9-w80-public-evidence.zip"
    with zipfile.ZipFile(w80_archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in sorted(public.rglob("*")):
            if path.is_file():
                info = zipfile.ZipInfo(prefix + str(path.relative_to(public)), date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o100644 << 16
                archive.writestr(info, path.read_bytes(), compresslevel=6)
    fixed = [(mirror, mirror.name)] + [(path, "media__" + str(path.relative_to(signed / "media")).replace("/", "__")) for path in sorted((signed / "media").rglob("*")) if path.is_file()]
    fixed += [(signed / name, name) for name in ("manifest.json", "manifest.sigstore.json", "trusted-root.json", "offline-verification.json")]
    fixed += [(Path("publication/v09/release-notes.md"), "v0.9-release-notes.md")]
    fixed += [(w80_archive, w80_archive.name), (public / "owner-w80-packet.json", "v0.9-owner-w80-packet.json")]
    entries = []
    for path, name in fixed:
        require(re.fullmatch(r"[A-Za-z0-9_.-]+", name) and path.stat().st_size < 2 * 1024**3, "Release asset name/size invalid")
        os.link(path, assets / name)
        entries.append({"file": name, "sha256": digest(path), "size": path.stat().st_size})
    baseline = read("publication/v09/fixed-assets.json")
    require(baseline["sourceSha"] == SOURCE and baseline["signedManifestSha256"] == MANIFEST
            and sorted(entries, key=lambda item: item["file"]) == baseline["assets"], "The original 45 accepted fixed asset bytes must not change")
    supplement = w81_supplement(output=OUT)
    review_assets = []
    for path, name in supplement.pop("assets"):
        os.link(path, assets / name)
        review_assets.append({"file": name, "sha256": digest(path), "size": path.stat().st_size})
    selection = {"schemaVersion": 1, "version": "v0.9", "sourceSha": SOURCE, "signedManifestSha256": MANIFEST,
        "trustedRootSha256": TRUST, "officialSignedArtifactZipSha256": SIGNED_ZIP, "w80PacketSha256": w80_hash or None,
        "publisherCommit": publisher, "publisherBranch": BRANCH, "publisherBranchProtectedAtPreparation": protected,
        "releaseReady": False, "published": False, "image": IMAGE, "indexDigest": "sha256:" + index_hash,
        "platforms": platforms, "assets": sorted(entries, key=lambda item: item["file"]), "privacy": privacy(),
        "reviewEvidence": {"w81": supplement}, "reviewAssets": review_assets,
        "fixedAssetBaselineSha256": digest(Path("publication/v09/fixed-assets.json")),
        "transportTooling": read(".github/release/publication-tooling.json"),
        "conditionalReadiness": {"workflow": ".github/workflows/v09-release-readiness.yml", "sourceSha": SOURCE,
            "signedManifestSha256": MANIFEST, "w80PacketSha256": w80_hash or None, "requiresSuccessfulOwnerMainDispatch": True,
            "conditionalReleaseAssets": ["v0.9-readiness-decision.json", "v0.9-readiness-decision.sha256"]},
        "registrySigning": {"identity": f"https://github.com/{REPOSITORY}/{WORKFLOW}@refs/heads/{BRANCH}",
            "workflowSha": publisher, "indexAndArchitectures": True, "perArchitectureSpdx": True,
            "readinessPredicateType": "https://kafdeck.io/attestations/v0.9-release-readiness/v1",
            "newOwnerAuthorizationRequired": True, "originalOfflineManifestResigned": False},
        "trustMethod": "unchanged official main01c GitHub-OIDC signed offline manifest; source/config/layer-bound OCI conversion; newly owner-authorized registry image signatures and readiness/SPDX attestations under separately adopted protected publisher identity"}
    write(OUT / "publication-selection.json", selection)
    existing_tag()
    prior = repo_api("releases/tags/v0.9", optional=True)
    require(prior is None or prior["body"] == release_body(selection, digest(OUT / "publication-selection.json")),
            "Existing v0.9 release body is foreign to this exact selection; never overwrite")
    print("Prepared selection SHA256: " + digest(OUT / "publication-selection.json"))


def verify_transport(selection, destination, local=False):
    roundtrip = OUT / ("local-roundtrip" if local else "remote-roundtrip")
    flags = ["--src-tls-verify=false"] if local else []
    skopeo("copy", "--all", "--preserve-digests", *flags, "docker://" + destination + "@" + selection["indexDigest"], "oci:/work/" + str(roundtrip) + ":release")
    root = read(roundtrip / "index.json")["manifests"][0]
    require(root["digest"] == selection["indexDigest"], "Transport changed the exact multiarch index digest")
    index = json.loads(blob(roundtrip, root).read_bytes())
    require(len(index["manifests"]) == 2, "Transport lost an architecture")
    for descriptor in index["manifests"]:
        arch = descriptor["platform"]["architecture"]
        require(validate_arch(roundtrip, descriptor, selection["platforms"][arch]["configDigest"]) == selection["platforms"][arch], "Remote config/layers differ")


def local_transport():
    selection = read(OUT / "publication-selection.json")
    pin = read(".github/release/publication-tooling.json")["localTestRegistryImage"]
    require(re.fullmatch(r"registry:3\.0\.0@sha256:[0-9a-f]{64}", pin), "Registry fixture tooling must remain immutable")
    name = "kafdeck-v09-local-" + os.environ["GITHUB_RUN_ID"]
    run("docker", "run", "--detach", "--name", name, "--publish", "127.0.0.1::5000", pin)
    try:
        endpoint = run("docker", "port", name, "5000/tcp").splitlines()[0]
        require(re.fullmatch(r"127\.0\.0\.1:[0-9]+", endpoint), "Registry fixture is not loopback-bound")
        for attempt in range(30):
            if subprocess.run(["curl", "--fail", "--silent", "http://" + endpoint + "/v2/"], stdout=subprocess.DEVNULL).returncode == 0:
                break
            time.sleep(1)
        else:
            raise ValueError("Owned local registry did not become healthy")
        target = endpoint + "/fixture"
        # Local sentinel tag tests preservation; no external registry reference enters this path.
        skopeo("copy", "--dest-tls-verify=false", "--preserve-digests", "oci:/work/" + str(OUT / "oci-amd64") + ":image", "docker://" + target + ":foreign-sentinel")
        before = tag_snapshot(target, local=True)
        skopeo("copy", "--all", "--dest-tls-verify=false", "--preserve-digests", "oci:/work/" + str(OUT / "oci") + ":release", "docker://" + target + "@" + selection["indexDigest"])
        verify_transport(selection, target, local=True)
        preserve_tags(before, tag_snapshot(target, local=True))
        write(OUT / "local-transport.json", {"externalPublication": False, "localRegistry": True, "indexDigest": selection["indexDigest"],
            "bothArchitecturesVerified": True, "configAndCompressedLayersPreserved": True, "signedUncompressedLayersVerified": True,
            "foreignSentinelTagUnchanged": True, "foreignSentinelManifestBytesUnchanged": True, "noProductBuildOrStartup": True})
    finally:
        run("docker", "rm", "--force", name)


def readiness(run_id, selection):
    require(re.fullmatch(r"[1-9][0-9]{0,19}", run_id), "Actual successful readiness run is mandatory")
    producer = repo_api("actions/runs/" + run_id)
    validate_run(producer, SOURCE, {"readiness": ("v09-release-readiness", ".github/workflows/v09-release-readiness.yml")})
    require(producer["event"] == "workflow_dispatch" and all(producer[key]["login"].lower() in owners() for key in ("actor", "triggering_actor")), "Readiness must be an actual successful owner main dispatch")
    items = repo_api("actions/runs/" + run_id + "/artifacts?per_page=100")
    require(items["total_count"] <= 100, "Readiness artifact inventory exceeds bound")
    matches = [item for item in items["artifacts"] if item["name"] == f"v09-release-readiness-{SOURCE}-{run_id}"]
    require(len(matches) == 1, "Missing unique successful readiness artifact")
    directory = OUT / "readiness"
    retrieve_artifact(matches[0], run_id, SOURCE, directory)
    decision = read(directory / "release-decision.json")
    require(decision["sourceSha"] == SOURCE and decision["signedManifestSha256"] == MANIFEST and decision["trustedRootSha256"] == TRUST
            and decision["releaseReady"] is True and not decision["missing"] and decision["w80"]["sha256"] == selection["w80PacketSha256"],
            "Successful readiness packet does not admit the exact authorized source/manifest/W80 input")
    return directory / "release-decision.json"


def existing_tag():
    tag = repo_api("git/ref/tags/v0.9", optional=True)
    require(tag is None or tag["object"]["type"] == "commit" and tag["object"]["sha"] == SOURCE, "Existing v0.9 tag is foreign or ambiguous; never retarget")
    return tag


def release_body(selection, selection_hash):
    return (Path("publication/v09/release-notes.md").read_text() + "\n\nPublication selection SHA256: " + selection_hash
            + "\n\nProtected publisher workflow commit: `" + selection["publisherCommit"] + "`\n"
            + "\nImage: `" + IMAGE + "@" + selection["indexDigest"] + "`\n")


def attestation_matches(text, expected, kind, digest_value):
    def visit(value):
        if isinstance(value, list):
            return any(visit(item) for item in value)
        if not isinstance(value, dict):
            return False
        payload = value.get("payload")
        if isinstance(payload, str):
            try:
                statement = json.loads(base64.b64decode(payload, validate=True))
                if (statement.get("predicateType") == kind and statement.get("predicate") == expected
                        and any(subject.get("digest", {}).get("sha256") == digest_value.removeprefix("sha256:") for subject in statement.get("subject", []))):
                    return True
            except (ValueError, UnicodeDecodeError):
                pass
        return any(visit(item) for item in value.values() if isinstance(item, (dict, list)))
    decoder, position = json.JSONDecoder(), 0
    while position < len(text):
        position += len(text[position:]) - len(text[position:].lstrip())
        if position == len(text):
            break
        value, position = decoder.raw_decode(text, position)
        if visit(value):
            return True
    return False


def registry_signatures(selection, decision, expected_hash, approval):
    guard_unused_proof_tags(tag_snapshot(IMAGE), selection)
    identity = selection["registrySigning"]["identity"]
    certificate = ["--certificate-identity", identity, "--certificate-oidc-issuer", "https://token.actions.githubusercontent.com",
        "--certificate-github-workflow-repository", REPOSITORY, "--certificate-github-workflow-name", "v09-publication",
        "--certificate-github-workflow-ref", "refs/heads/" + BRANCH, "--certificate-github-workflow-trigger", "workflow_dispatch",
        "--certificate-github-workflow-sha", selection["publisherCommit"], "--trusted-root", str(OUT / "signed/trusted-root.json")]
    annotations = ["-a", "sourceSha=" + SOURCE, "-a", "signedManifestSha256=" + MANIFEST,
                   "-a", "w80PacketSha256=" + selection["w80PacketSha256"], "-a", "selectionSha256=" + expected_hash]
    targets = {"index": selection["indexDigest"], **{arch: item["manifestDigest"] for arch, item in selection["platforms"].items()}}
    proofs = []
    for name, target in targets.items():
        authority(selection, expected_hash, approval, write_intent=True)
        ref = IMAGE + "@" + target
        run("cosign", "sign", "--yes", "--new-bundle-format=true", "--oidc-provider=github-actions", *annotations, ref)
        path = OUT / ("registry-signature-" + name + ".json")
        path.write_text(run("cosign", "verify", *certificate, *annotations, ref) + "\n")
        proofs.append({"file": path.name, "sha256": digest(path), "subjectDigest": target, "publisherIdentity": identity})
    predicates = [("index-readiness", selection["indexDigest"], decision, selection["registrySigning"]["readinessPredicateType"], selection["registrySigning"]["readinessPredicateType"])]
    for arch in ("amd64", "arm64"):
        sbom = OUT / f"signed/media/distribution/w81-oci-{arch}-{SOURCE}/sbom.spdx.json"
        predicates.append((arch + "-spdx", selection["platforms"][arch]["manifestDigest"], sbom, "spdxjson", "https://spdx.dev/Document"))
    for name, target, predicate, cli_type, predicate_type in predicates:
        authority(selection, expected_hash, approval, write_intent=True)
        ref = IMAGE + "@" + target
        run("cosign", "attest", "--yes", "--new-bundle-format=true", "--oidc-provider=github-actions",
            "--type", cli_type, "--predicate", str(predicate), ref)
        result = run("cosign", "verify-attestation", *certificate, "--type", cli_type, ref)
        require(attestation_matches(result, read(predicate), predicate_type, target), "Verified registry attestation predicate/subject differs")
        path = OUT / ("registry-attestation-" + name + ".json")
        path.write_text(result + "\n")
        proofs.append({"file": path.name, "sha256": digest(path), "subjectDigest": target, "publisherIdentity": identity})
    return proofs


def admission():
    selection = read(OUT / "publication-selection.json")
    authority(selection, os.environ.get("SELECTION_SHA256", ""), os.environ.get("OWNER_APPROVAL", ""), write_intent=True)
    require(w81_supplement()["indexSha256"] == selection["reviewEvidence"]["w81"]["indexSha256"], "W81 supplement differs from owner-reviewed selection")
    readiness(os.environ.get("READINESS_RUN_ID", ""), selection)
    print("Exact conditional owner decision, protected publisher and actual matching main readiness admitted; no write performed")


def publish():
    selection = read(OUT / "publication-selection.json")
    expected, approval = os.environ.get("SELECTION_SHA256", ""), os.environ.get("OWNER_APPROVAL", "")
    authority(selection, expected, approval, write_intent=True)
    require(selection["sourceSha"] == SOURCE and selection["signedManifestSha256"] == MANIFEST and selection["indexDigest"].startswith("sha256:"), "Selection source differs")
    for item in selection["assets"] + selection["reviewAssets"]:
        require(digest(OUT / "assets" / item["file"]) == item["sha256"], "Approved asset changed")
    decision = readiness(os.environ.get("READINESS_RUN_ID", ""), selection)
    assets = OUT / "assets"
    shutil.copyfile(decision, assets / "v0.9-readiness-decision.json")
    (assets / "v0.9-readiness-decision.sha256").write_text(digest(decision) + "  v0.9-readiness-decision.json\n")
    shutil.copyfile(OUT / "publication-selection.json", assets / "publication-selection.json")
    (assets / "SHA256SUMS").write_text("".join(digest(path) + "  " + path.name + "\n" for path in sorted(assets.iterdir()) if path.name != "SHA256SUMS"))
    expected_assets = {path.name: digest(path) for path in assets.iterdir()}
    tag = existing_tag()
    release = repo_api("releases/tags/v0.9", optional=True)
    marker = "Publication selection SHA256: " + expected
    if release:
        require(tag is not None and release["tag_name"] == "v0.9" and release["name"] == "Kafdeck v0.9"
                and release.get("body") == release_body(selection, expected), "Existing release is foreign; never overwrite")
        require(all(item["name"] in expected_assets and item.get("digest") == "sha256:" + expected_assets[item["name"]] for item in release["assets"]), "Existing release assets differ or are unverifiable")
    require(privacy() == selection["privacy"], "GHCR privacy/identity changed before publication")
    token = os.environ.get("REGISTRY_TOKEN", "")
    require(token, "Missing scoped registry credential")
    subprocess.run(["docker", "login", "ghcr.io", "--username", os.environ["GITHUB_ACTOR"], "--password-stdin"], input=token, text=True, check=True)
    tags_before = tag_snapshot(IMAGE)
    guard_unused_proof_tags(tags_before, selection)
    authority(selection, expected, approval, write_intent=True)
    skopeo("copy", "--all", "--preserve-digests", "oci:/work/" + str(OUT / "oci") + ":release", "docker://" + IMAGE + "@" + selection["indexDigest"])
    verify_transport(selection, IMAGE)
    preserve_tags(tags_before, tag_snapshot(IMAGE))
    require(privacy() == selection["privacy"], "Registry privacy changed")
    signatures = registry_signatures(selection, decision, expected, approval)
    proof_tags = proof_tag_names(selection)
    preserve_tags(tags_before, tag_snapshot(IMAGE), proof_tags)
    require(privacy() == selection["privacy"], "Registry privacy changed during signature publication")
    authority(selection, expected, approval, write_intent=True)
    if existing_tag() is None:
        repo_api("git/refs", data={"ref": "refs/tags/v0.9", "sha": SOURCE})
    require(existing_tag() is not None, "Exact source tag was not created")
    if release is None:
        body = release_body(selection, expected)
        release = repo_api("releases", data={"tag_name": "v0.9", "target_commitish": SOURCE, "name": "Kafdeck v0.9", "body": body, "draft": True, "prerelease": False})
    existing = {item["name"] for item in release["assets"]}
    for name in sorted(expected_assets):
        if name not in existing:
            authority(selection, expected, approval, write_intent=True)
            run("gh", "release", "upload", "v0.9", "--repo", REPOSITORY, str(assets / name))
    # Independently download and hash every release asset; never rely only on upload metadata.
    downloaded = OUT / "remote-assets"
    downloaded.mkdir()
    run("gh", "release", "download", "v0.9", "--repo", REPOSITORY, "--dir", str(downloaded))
    require({path.name: digest(path) for path in downloaded.iterdir()} == expected_assets, "Remote release asset bytes differ from the exact approved/conditional assets")
    authority(selection, expected, approval, write_intent=True)
    run("gh", "release", "edit", "v0.9", "--repo", REPOSITORY, "--draft=false")
    require(privacy() == selection["privacy"] and existing_tag() is not None, "Final source/privacy check failed")
    write(OUT / "publication-result.json", {"published": True, "sourceSha": SOURCE, "selectionSha256": expected,
        "signedManifestSha256": MANIFEST, "readinessDecisionSha256": digest(decision), "indexDigest": selection["indexDigest"],
        "digestOnly": True, "privateGhcrPreserved": True, "remoteAssetsVerified": expected_assets,
        "individualRegistryCosignSignaturesVerified": True, "registrySigningProofs": signatures,
        "registryPublisherIdentity": selection["registrySigning"], "officialFullManifestSignatureUnchanged": True})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "local-transport", "preflight", "admission", "publish"))
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(os.environ.get("W80_PACKET_SHA256", ""))
    elif args.command == "local-transport":
        local_transport()
    elif args.command == "preflight":
        preflight()
    elif args.command == "admission":
        admission()
    else:
        publish()
