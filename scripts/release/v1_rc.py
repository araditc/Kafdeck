"""Prepare/verify an unsigned native RC packet. Never sign, publish or claim release readiness."""
from __future__ import annotations
import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import tarfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
COMPONENTS = {"api-ui": ("Kafdeck.Api", "Kafdeck.Api.dll"), "cli": ("kafdeck", "kafdeck.dll"), "mcp": ("kafdeck-mcp", "kafdeck-mcp.dll")}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def selection(root=ROOT):
    value = json.loads((root / ".github/release/v1.0-rc.json").read_text(encoding="utf-8-sig"))
    require(value["schemaVersion"] == 1 and re.fullmatch(r"1\.0\.0-rc\.[1-9][0-9]*", value["version"]), "Unexpected RC version")
    require(value["candidate"] == "v" + value["version"] and value["signed"] is False and value["releaseReady"] is False
            and value["publicationAuthorized"] is False, "RC preparation cannot authorize signing/publication")
    require(value["nativeProducer"] == ".github/workflows/v1-rc-native.yml", "Unexpected native producer")
    require(value["nativeRids"] == ["win-x64", "linux-x64", "linux-arm64", "osx-x64", "osx-arm64"], "Native selection changed")
    return value


def inputs(root=ROOT):
    """Source projection includes runtime embedded documents and exact build/tool producers.

    Tests, evidence and ordinary documentation do not become runtime build inputs.
    Exact producer source SHA is still retained; matching this projection never silently
    transfers an authenticated producer run or a signature to another commit.
    """
    files = subprocess.check_output(["git", "ls-files", "src", "build", "docs/api", "Directory.Build.props", "Directory.Build.targets",
                                     "Directory.Packages.props", "Kafdeck.slnx", "global.json", ".github/release/v1.0-rc.json",
                                     ".github/workflows/v1-rc-native.yml", "scripts/release/test-v1-rc-native.ps1", "scripts/release/v1_rc.py"], cwd=root, text=True).splitlines()
    selected = []
    for name in sorted(files):
        if name.startswith("docs/api/") and not re.fullmatch(r"docs/api/openapi-[^/]+\.json", name):
            continue
        selected.append(name)
    # Git blobs already apply the repository's text/binary attributes. Never
    # normalize arbitrary binary assets as though CRLF/LF were interchangeable.
    batch = subprocess.run(["git", "cat-file", "--batch"], cwd=root,
                           input="".join("HEAD:" + name + "\n" for name in selected).encode(), capture_output=True, check=True)
    result, offset = {}, 0
    for name in selected:
        end = batch.stdout.index(b"\n", offset)
        header = batch.stdout[offset:end].decode().split()
        require(len(header) == 3 and header[1] == "blob", "Build input is not a committed blob")
        size = int(header[2]); data = batch.stdout[end + 1:end + 1 + size]
        require(len(data) == size, "Incomplete committed input")
        result[name] = hashlib.sha256(data).hexdigest()
        offset = end + size + 2
    return result


def member_name(member):
    raw = member.name
    while raw.startswith("./"):
        raw = raw[2:]
    if raw in ("", ".") and member.isdir():
        return "."
    name = pathlib.PurePosixPath(raw)
    require(raw and not name.is_absolute() and ".." not in name.parts and "\\" not in raw and ":" not in raw,
            "Unsafe archive path")
    require(all(part == part.rstrip(" .") and part.split(".")[0].upper() not in
                {"CON", "PRN", "AUX", "NUL", *["COM" + str(n) for n in range(1, 10)], *["LPT" + str(n) for n in range(1, 10)]}
                for part in name.parts), "Nonportable archive path")
    require(member.isdir() or member.isfile(), "Archive links/devices are forbidden")
    return str(name)


def archive_files(archive):
    """Bounded inventory before extraction; reject links, aliases and duplicate names."""
    result, aliases, total = {}, set(), 0
    with tarfile.open(archive, "r:gz") as package:
        for member in package:
            name = member_name(member)
            if member.isdir():
                continue
            require(str(name).casefold() not in aliases and member.size <= 256 * 1024 * 1024, "Duplicate/oversized archive member")
            aliases.add(str(name).casefold())
            total += member.size
            require(len(result) < 20000 and total <= 2 * 1024**3, "Archive bounds exceeded")
            stream = package.extractfile(member)
            require(stream is not None, "Missing archive member data")
            value = hashlib.sha256()
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                value.update(block)
            result[str(name)] = value.hexdigest()
    return result


def packet(directory, rid, source, source_inputs, chosen):
    require(rid in chosen["nativeRids"] and re.fullmatch(r"[0-9a-f]{40}", source), "Invalid source/RID")
    proof = json.loads((directory / "native-evidence.json").read_text(encoding="utf-8-sig"))
    require(proof["sourceSha"] == source and proof["version"] == chosen["version"] and proof["rid"] == rid
            and proof["installedFromArchives"] is True, "Native evidence/source selection mismatch")
    require(set(proof["components"]) == set(COMPONENTS), "Native component selection incomplete")
    components = {}
    for key, (host, dll) in COMPONENTS.items():
        expected = f"kafdeck-{key}-{chosen['version']}-{rid}.tar.gz"
        evidence = proof["components"][key]
        require(evidence["archive"] == expected and evidence["started"] is True and evidence["authenticatedOperation"] is True,
                "Native archive installation/authentication incomplete")
        require(evidence["productVersion"] == chosen["version"] + "+" + source
                and evidence["assemblyVersion"] == chosen["assemblyVersion"], "Installed binary version/source metadata mismatch")
        archive = directory / expected
        require(sha(archive) == evidence["sha256"], "Native archive hash differs from installed media")
        files = archive_files(archive)
        executable = host + (".exe" if rid.startswith("win-") else "")
        require(executable in files and dll in files, "Native apphost/managed assembly missing")
        if key == "api-ui":
            require("wwwroot/index.html" in files and any(p.startswith("wwwroot/assets/") for p in files), "Packaged UI absent")
        components[key] = {"archive": expected, "sha256": sha(archive), "executable": executable, "managedAssembly": dll,
                           "files": files, "installedAndAuthenticated": True}
    return {"schemaVersion": 1, "kind": "unsigned-v1-native-candidate", "version": chosen["version"], "rid": rid,
            "sourceSha": source, "producer": chosen["nativeProducer"], "inputSha256": hashlib.sha256(canonical(source_inputs)).hexdigest(),
            "inputs": source_inputs, "selectionSha256": hashlib.sha256(canonical(chosen)).hexdigest(), "components": components,
            "nativeEvidenceSha256": sha(directory / "native-evidence.json"), "signed": False, "releaseReady": False,
            "publicationAuthorized": False,
            "limits": "Local hashes are not a signature or authenticated GitHub producer attestation. Full media/security/owner admission remains separate."}


def verify(directory, expected_source, expected_input_sha, expected_manifest_sha):
    manifest = directory / "rc-manifest.json"
    require(sha(manifest) == expected_manifest_sha, "Manifest does not match independently selected digest")
    value = json.loads(manifest.read_text(encoding="utf-8-sig"))
    require(value["sourceSha"] == expected_source and value["inputSha256"] == expected_input_sha, "RC source/input identity mismatch")
    require(hashlib.sha256(canonical(value["inputs"])).hexdigest() == expected_input_sha, "RC input list is inconsistent")
    chosen = selection()
    rebuilt = packet(directory, value["rid"], expected_source, value["inputs"], chosen)
    require(rebuilt == value, "RC manifest/media/native proof mismatch")
    return value


def install_api(directory, destination, value):
    require(not destination.exists(), "Install destination must be fresh")
    component = value["components"]["api-ui"]
    # Verification must finish before creating any installation directory.
    require(sha(directory / component["archive"]) == component["sha256"], "API archive changed before installation")
    archive_files(directory / component["archive"])
    destination.mkdir(parents=True)
    with tarfile.open(directory / component["archive"], "r:gz") as package:
        for member in package:
            # Revalidate the actual extraction pass too, including a concurrent
            # replacement after inventory. Exclusive writes cannot escape here.
            name = member_name(member)
            if not member.isfile():
                continue
            require(name in component["files"] and member.size <= 256 * 1024 * 1024, "Unselected extraction member")
            output = destination / name
            output.parent.mkdir(parents=True, exist_ok=True)
            with package.extractfile(member) as source, output.open("xb") as target:
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    target.write(block)
            output.chmod(member.mode & 0o777)
    actual = {p.relative_to(destination).as_posix(): sha(p) for p in destination.rglob("*") if p.is_file()}
    require(actual == component["files"], "Installed API files differ from selected archive")
    return destination / component["executable"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare-native")
    prepare.add_argument("--directory", required=True, type=pathlib.Path)
    prepare.add_argument("--rid", required=True)
    for command in ("verify-native", "install-api"):
        check = sub.add_parser(command)
        check.add_argument("--directory", required=True, type=pathlib.Path)
        check.add_argument("--source", required=True)
        check.add_argument("--input-sha256", required=True)
        check.add_argument("--manifest-sha256", required=True)
        if command == "install-api":
            check.add_argument("--destination", required=True, type=pathlib.Path)
    args = parser.parse_args()
    if args.command == "prepare-native":
        require(not subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=all"], cwd=ROOT, text=True).strip(), "RC producer requires clean tracked and untracked source")
        source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        value = packet(args.directory, args.rid, source, inputs(), selection())
        output = args.directory / "rc-manifest.json"
        output.write_bytes((json.dumps(value, indent=2) + "\n").encode())
        print(json.dumps({"source": source, "rid": args.rid, "manifestSha256": sha(output), "inputSha256": value["inputSha256"], "releaseReady": False}))
    else:
        value = verify(args.directory, args.source, args.input_sha256, args.manifest_sha256)
        installed = str(install_api(args.directory, args.destination, value)) if args.command == "install-api" else None
        print(json.dumps({"verifiedUnsignedNativeBytes": True, "source": value["sourceSha"], "rid": value["rid"], "signatureVerified": False, "installedApi": installed}))


if __name__ == "__main__":
    main()
