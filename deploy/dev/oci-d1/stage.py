"""Stage immutable signed media and fixture dependencies before network isolation."""
import hashlib
import os
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts/release"))
from v09_prepublication import api, digest, read, require, retrieve_artifact, validate_run, verify, write
from v09_offline_install import extract_archive

SOURCE = "01c7553425fbc61ba4a7a21918265c562086ba02"
MANIFEST = "b63864259a8a5e1c3c9d8dc3312d35a56e82749d62e2f6a05b88112f98af29ba"
SIGNED_RUN = 42984770583
ARTIFACT = 13839715488
ZIP_HASH = "7100754a28ebc405a8ca9739412341a3e13587f6e9514882ab547f2e62b8c295"


if __name__ == "__main__":
    private, evidence, root_hash = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
    evidence.mkdir(parents=True, exist_ok=True)
    print("Authenticating official signed producer and immutable artifact ZIP", flush=True)
    producer = api(f"actions/runs/{SIGNED_RUN}")
    validate_run(producer, SOURCE, {"signed": ("v09-prepublication-signing", ".github/workflows/v09-prepublication-signing.yml")})
    item = api(f"actions/artifacts/{ARTIFACT}")
    require(item["name"] == f"v09-prepublication-signed-{SIGNED_RUN}" and item["digest"] == "sha256:" + ZIP_HASH,
            "Official signed artifact identity differs")
    signed = private / "signed"
    retrieve_artifact(item, SIGNED_RUN, SOURCE, signed)
    require(digest(signed / "manifest.json") == MANIFEST and digest(signed / "trusted-root.json") == root_hash,
            "Independent reviewed manifest/root pin differs")
    subprocess.run(["cosign", "trusted-root", "create", "--with-default-services", "--out", str(private / "independent-root.json")], check=True)
    require(digest(private / "independent-root.json") == root_hash, "Current independent Sigstore trust material differs from the reviewed root")
    subprocess.run(["bash", str(ROOT / "scripts/release/verify-v09-signature.sh"), str(signed), SOURCE, root_hash], check=True)
    manifest = verify(signed)
    require(manifest["sourceSha"] == manifest["workflowSourceSha"] == SOURCE and manifest["scope"] == "full-distribution",
            "Require exact official full-distribution signed source")
    rid = os.environ["D1_RID"]
    for component in ("api-ui", "cli", "mcp"):
        extract_archive(signed / f"media/native/{rid}/kafdeck-{component}-{rid}.tar.gz", private / "installed" / component)
    print("Official signature, independent Sigstore root and signed installed apphost hashes verified", flush=True)
    write(evidence / "signed-input.json", {"schemaVersion": 1, "applicationSourceSha": SOURCE, "signedManifestSha256": MANIFEST,
        "trustedRootSha256": root_hash, "officialSignedRunId": SIGNED_RUN, "officialArtifactId": ARTIFACT,
        "independentDefaultSigstoreRootCompared": True,
        "officialArtifactZipSha256": ZIP_HASH, "signatureVerified": True, "allMediaHashesVerified": True,
        "installedFromOfficialSignedArchives": True,
        "apphosts": {component: digest(private / "installed" / component / name) for component, name in
                     (("api-ui", "Kafdeck.Api"), ("cli", "kafdeck"), ("mcp", "kafdeck-mcp"))},
        "driverSha": subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip(),
        "driverFiles": {str(path.relative_to(ROOT)): digest(path) for path in sorted((ROOT / "deploy/dev/w80-signed-disconnected").glob("*.py"))}})
