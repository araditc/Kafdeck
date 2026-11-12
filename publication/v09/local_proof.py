"""LOCAL-only compatibility fixture: no product image, OIDC, tlog or external write."""
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile

import driver


def checked(*args, **kwargs):
    return subprocess.check_output(args, timeout=90, **kwargs)


def inner(private, reference, subject):
    from v09_offline_install import isolated_network
    isolated_network()
    driver.require(reference.startswith("127.0.0.1:5000/compat@sha256:"), "Local proof target must be the isolated loopback registry")
    env = {**os.environ, "COSIGN_PASSWORD": (private / "password").read_text()}
    cosign = os.environ["COSIGN"]
    common = ["--allow-http-registry", "--new-bundle-format=true"]
    signing = ["--key", str(private / "test.key"), "--yes", "--tlog-upload=false", "--use-signing-config=false", "--oidc-disable-ambient-providers=true"]
    checked(cosign, "sign", *common, *signing, reference, env=env)
    verifying = ["--key", str(private / "test.pub"), "--private-infrastructure=true", "--trusted-root", str(private / "root.json")]
    signature = checked(cosign, "verify", *common, *verifying, reference, env=env).decode()
    predicate = {"localOnly": True, "noOIDC": True, "noTransparencyLog": True}
    driver.write(private / "predicate.json", predicate)
    kind = "https://kafdeck.io/attestations/local-compatibility/v1"
    checked(cosign, "attest", *common, *signing, "--type", kind, "--predicate", str(private / "predicate.json"), reference, env=env)
    attestation = checked(cosign, "verify-attestation", *common, *verifying, "--type", kind, reference, env=env).decode()
    driver.require(driver.attestation_matches(attestation, predicate, kind, subject), "Actual Cosign attestation output shape/subject/predicate failed")
    driver.require(not driver.attestation_matches(attestation, {"localOnly": False}, kind, subject), "Wrong predicate was accepted")
    isolated_network()
    driver.write(private / "inner.json", {"localSignatureVerified": bool(signature), "localAttestationVerified": True,
        "actualPredicateShapeValidated": True, "wrongPredicateRejected": True, "interfaces": ["lo"],
        "ipv4DefaultRoute": False, "ipv6DefaultRoute": False, "oidcUsed": False, "tlogUpload": False,
        "externalPublication": False, "testTrustOnly": True})


def main():
    proof = Path("artifacts/v09-publication/local-proof.json")
    proof.parent.mkdir(parents=True, exist_ok=True)
    policy = driver.read(".github/release/publication-tooling.json")
    for image in (policy["skopeoImage"], policy["localTestRegistryImage"]):
        subprocess.run(["docker", "pull", image], check=True)
    with tempfile.TemporaryDirectory(prefix="kafdeck-local-proof-", dir=os.environ["RUNNER_TEMP"]) as temporary:
        private = Path(temporary)
        (private / "password").write_text(secrets.token_hex(24))
        env = {**os.environ, "COSIGN_PASSWORD": (private / "password").read_text()}
        cosign = driver.run("which", "cosign")
        subprocess.run([cosign, "generate-key-pair", "--output-key-prefix", str(private / "test")], env=env, check=True)
        subprocess.run([cosign, "trusted-root", "create", "--with-default-services", "--out", str(private / "root.json")], check=True)
        layout = Path("artifacts/v09-publication/local-proof-fixture")
        blobs = layout / "blobs/sha256"
        blobs.mkdir(parents=True)
        def put(value):
            data = json.dumps(value, separators=(",", ":")).encode()
            sha = hashlib.sha256(data).hexdigest()
            (blobs / sha).write_bytes(data)
            return {"digest": "sha256:" + sha, "size": len(data)}
        config = put({"architecture": "amd64", "os": "linux", "rootfs": {"type": "layers", "diff_ids": []}, "config": {}})
        image = put({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                     "config": {**config, "mediaType": "application/vnd.oci.image.config.v1+json"}, "layers": []})
        driver.write(layout / "oci-layout", {"imageLayoutVersion": "1.0.0"})
        driver.write(layout / "index.json", {"schemaVersion": 2, "manifests": [{**image,
            "mediaType": "application/vnd.oci.image.manifest.v1+json", "annotations": {"org.opencontainers.image.ref.name": "fixture"}}]})
        name = "kafdeck-local-proof-" + os.environ["GITHUB_RUN_ID"]
        container = checked("docker", "run", "--detach", "--name", name, "--network=none", policy["localTestRegistryImage"], text=True).strip()
        try:
            def sk(*args):
                return checked("docker", "run", "--rm", "--network=container:" + container, "--volume", str(Path.cwd()) + ":/work",
                               "--workdir=/work", "--entrypoint=/usr/bin/skopeo", policy["skopeoImage"], *args)
            ref = "127.0.0.1:5000/"
            bare = "sha256-" + image["digest"].removeprefix("sha256:")
            sk("copy", "--dest-tls-verify=false", "--preserve-digests", "oci:/work/" + str(layout) + ":fixture", "docker://" + ref + "collision:" + bare)
            before = sk("inspect", "--tls-verify=false", "--raw", "docker://" + ref + "collision:" + bare)
            try:
                driver.guard_unused_proof_tags({bare: hashlib.sha256(before).hexdigest()}, {"indexDigest": image["digest"], "platforms": {}})
            except ValueError:
                collision_rejected = True
            else:
                raise ValueError("Actual reserved-name runtime image collision was accepted")
            driver.require(before == sk("inspect", "--tls-verify=false", "--raw", "docker://" + ref + "collision:" + bare), "Foreign runtime tag changed")
            sk("copy", "--dest-tls-verify=false", "--preserve-digests", "oci:/work/" + str(layout) + ":fixture", "docker://" + ref + "compat@" + image["digest"])
            pid = json.loads(checked("docker", "inspect", container))[0]["State"]["Pid"]
            subprocess.run(["sudo", "nsenter", "--target", str(pid), "--net", "env", "-i", "PATH=/usr/sbin:/usr/bin:/sbin:/bin",
                "HOME=" + str(private), "COSIGN=" + cosign, "/usr/bin/python3", "-B", str(Path(__file__).resolve()),
                "--inner", str(private), ref + "compat@" + image["digest"], image["digest"]], check=True, timeout=180)
            result = driver.read(private / "inner.json")
            result.update({"cosignVersion": driver.run(cosign, "version", "--json"), "reservedRuntimeCollisionRejectedBeforeCosign": collision_rejected,
                           "foreignRuntimeManifestBytesPreserved": True, "productImageUsed": False,
                           "tooling": policy, "scope": "ephemeral-key loopback registry compatibility only; private infrastructure/tlog opt-outs never apply to production publisher"})
            driver.write(proof, result)
        finally:
            subprocess.run(["docker", "rm", "--force", container], check=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--inner":
        inner(Path(sys.argv[2]), sys.argv[3], sys.argv[4])
    else:
        main()
