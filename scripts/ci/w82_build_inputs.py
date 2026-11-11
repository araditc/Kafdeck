"""Bind default build configuration in addition to the complete src tree."""
import hashlib
import json
import os
from pathlib import PurePosixPath
import subprocess

from w82_evidence import require

ALGORITHM = "git-path-mode-blob-sha256-v1"
PATHS = [":(top)build", ":(top).config"] + [
    ":(top,glob,icase)" + pattern for pattern in (
        "Directory.Build.*", "Directory.Packages.*", "global.json", "NuGet.Config", "*.props", "*.targets",
        "*.proj", "*.sln", "*.slnx", "*.rsp", "Dockerfile", ".dockerignore", ".gitattributes")]


def selected(path):
    parts = PurePosixPath(path).parts
    name = parts[0].lower()
    if name in ("build", ".config"):
        return True
    return len(parts) == 1 and (name.startswith(("directory.build.", "directory.packages.")) or
        name in ("global.json", "nuget.config", "dockerfile", ".dockerignore", ".gitattributes") or
        name.endswith((".props", ".targets", ".proj", ".sln", ".slnx", ".rsp")))


def git(repository, *arguments):
    return subprocess.check_output(["git", "-C", str(repository), *arguments])


def snapshot(repository, reference):
    entries = []
    for row in git(repository, "ls-tree", "-r", "-z", reference).decode().split("\0"):
        if not row:
            continue
        metadata, path = row.split("\t", 1)
        if selected(path):
            mode, kind, blob = metadata.split()
            require(kind == "blob" and mode in ("100644", "100755"), "Unsupported build input file type")
            entries.append({"path": path, "mode": mode, "blob": blob})
    entries.sort(key=lambda row: row["path"])
    encoded = json.dumps(entries, separators=(",", ":"), ensure_ascii=True).encode()
    return {"algorithm": ALGORITHM, "sha256": hashlib.sha256(encoded).hexdigest(), "entries": entries}


def verify(repository, reference, candidate):
    require(not any(key.lower() == "kafdecknativerid" and value for key, value in os.environ.items()),
            "Default-build proof requires KafdeckNativeRid unset")
    require("buildInputIdentity" in candidate, "Missing exact default-build input identity")
    policy = candidate["buildInputIdentity"]
    baseline = snapshot(repository, candidate["protectedPerformanceSourceSha"])
    require(baseline == policy["baseline"], "Measured baseline build inputs mismatch")
    actual = snapshot(repository, reference)
    changed = git(repository, "diff", "--name-only", "-z", reference, "--").decode().split("\0")
    require(not any(selected(path) for path in changed if path), "Dirty default-build input")
    untracked = git(repository, "ls-files", "--others", "-z", "--", *PATHS).decode().split("\0")
    require(not any(selected(path) for path in untracked if path), "Untracked or ignored default-build input")
    if actual == baseline:
        return actual["sha256"]
    matches = [entry for entry in policy["equivalentSnapshots"] if entry["snapshot"] == actual]
    require(len(matches) == 1, "Default-build inputs differ without an exact reviewed equivalent snapshot")
    entry = matches[0]
    require(entry["defaultBuildEquivalent"] is True and entry["rationale"].strip(), "Missing build-equivalence rationale")
    if candidate["status"] == "admitted":
        require(entry["reviewReference"] and entry["reviewReference"].strip(), "Build-input equivalence requires review")
    return actual["sha256"]
