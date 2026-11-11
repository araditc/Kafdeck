"""Execute already signature-verified media. Called only after verify-v09-offline.sh.

This module reports installation/startup, never authenticates a signature itself.
Offline fixture initialization does not prove a disconnected authenticated read.
"""
import json
from pathlib import Path, PurePosixPath
import platform
import shutil
import socket
import stat
import subprocess
import tarfile
import tempfile
import time
import urllib.request

from v09_prepublication import require


def run(*args, timeout=180, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=timeout, **kwargs).stdout


def isolated_network():
    links = run("ip", "-j", "link", timeout=10)
    require([link["ifname"] for link in json.loads(links)] == ["lo"], "Offline installation requires loopback-only networking")
    for family in ("-4", "-6"):
        require(not run("ip", family, "route", "show", "default", timeout=10).strip(), "Offline installation has a default route")


def extract_archive(archive, destination):
    """No links/devices/traversal; retain executable bits without setuid/setgid."""
    destination.mkdir(parents=True)
    with tarfile.open(archive, "r:gz") as package:
        members = package.getmembers()
        require(len(members) <= 20000 and sum(item.size for item in members) <= 4 * 1024**3,
                "Native archive exceeds installation bounds")
        seen = set()
        for item in members:
            path = PurePosixPath(item.name)
            require(not path.is_absolute() and ".." not in path.parts and "\\" not in item.name
                    and not any(":" in part for part in path.parts), "Unsafe native archive path")
            require(item.isfile() or item.isdir(), "Native archive links and special files are prohibited")
            canonical = path.as_posix()
            require(canonical not in seen, "Duplicate native archive member")
            seen.add(canonical)
            output = destination.joinpath(*path.parts)
            if item.isdir():
                output.mkdir(parents=True, exist_ok=True)
            else:
                output.parent.mkdir(parents=True, exist_ok=True)
                with package.extractfile(item) as source, output.open("xb") as target:
                    shutil.copyfileobj(source, target)
                output.chmod(item.mode & 0o755)


def http_probe(origin, deadline=90):
    # No proxy inheritance; the only address is the owned loopback listener.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    until = time.monotonic() + deadline
    while time.monotonic() < until:
        try:
            with opener.open(origin + "/healthz", timeout=2) as response:
                require(response.status == 200 and json.loads(response.read(65536))["status"] == "ok", "API health response failed")
            with opener.open(origin + "/", timeout=3) as response:
                require(response.status == 200 and b"<html" in response.read(2 * 1024**2).lower(), "Packaged frontend is missing")
            return {"health": True, "staticFrontend": True}
        except (OSError, ValueError, KeyError):
            time.sleep(0.25)
    raise ValueError("Installed offline API/frontend did not become ready")


def native_install(media, source, private):
    directory = media / "native/linux-x64"
    installed = private / "installed"
    for component in ("api-ui", "cli", "mcp"):
        extract_archive(directory / f"kafdeck-{component}-linux-x64.tar.gz", installed / component)
    executables = {"api-ui": installed / "api-ui/Kafdeck.Api", "cli": installed / "cli/kafdeck", "mcp": installed / "mcp/kafdeck-mcp"}
    require(all(path.is_file() and path.stat().st_mode & stat.S_IXUSR for path in executables.values()), "Installed native apphost is missing or not executable")
    runtime = private / "runtime"
    runtime.mkdir()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    origin = f"http://127.0.0.1:{port}"
    settings = {"Kafdeck": {"Deployment": {"AccessMode": "Local", "ListenUrls": [origin]}, "Clusters": []},
                "Logging": {"LogLevel": {"Default": "Warning"}}}
    (runtime / "appsettings.json").write_text(json.dumps(settings))
    profile = runtime / "fixture-oidc.json"
    profile.write_text(json.dumps({"issuer": "https://offline.invalid", "audience": "offline-fixture", "clientId": "offline-fixture"}))
    environment = {"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "HOME": str(runtime), "ASPNETCORE_ENVIRONMENT": "Production",
                   "DOTNET_CLI_TELEMETRY_OPTOUT": "1", "DOTNET_BUNDLE_EXTRACT_BASE_DIR": str(runtime / "bundle")}
    with (runtime / "api.log").open("w") as log:
        api = subprocess.Popen([str(executables["api-ui"]), "--contentRoot", str(runtime), "--webroot", str(installed / "api-ui/wwwroot")],
                               cwd=runtime, env=environment, stdout=log, stderr=log)
        try:
            api_proof = http_probe(origin)
            require(api.poll() is None, "Installed native API exited")
            help_text = run(str(executables["cli"]), "--help", timeout=30, cwd=runtime, env=environment)
            require("kafdeck" in help_text.lower() and "--help" in help_text, "Installed CLI help failed")
            requests = [
                {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18",
                 "capabilities": {}, "clientInfo": {"name": "signed-offline-install", "version": "0.9.0"}}},
                {"jsonrpc": "2.0", "method": "notifications/initialized"},
                {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}]
            output = run(str(executables["mcp"]), "--base-url", origin, "--oidc-profile", str(profile), timeout=30,
                         cwd=runtime, env=environment, input="".join(json.dumps(request) + "\n" for request in requests))
            require(len(output) < 262144, "Installed MCP response exceeds bound")
            replies = {reply["id"]: reply for reply in (json.loads(line) for line in output.splitlines())}
            require(replies[1].get("result", {}).get("serverInfo", {}).get("version") == "0.9.0"
                    and replies[1]["result"].get("protocolVersion") == "2025-06-18"
                    and replies[2].get("result", {}).get("tools"), "Installed MCP initialize/tool catalog failed")
            return {"rid": "linux-x64", "sourceSha": source, "installedFromArchives": True,
                    "apiUi": api_proof, "cliHelp": True, "mcpInitialize": True, "mcpToolCatalog": True,
                    "authenticatedOperation": False, "identityFixtureStaged": False,
                    "scope": "signed archive extraction and native startup only; fixture profile identifiers, no authenticated backend call"}
        finally:
            if api.poll() is None:
                api.terminate()
                try:
                    api.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    api.kill()
                    api.wait(timeout=10)


def oci_install(media, manifest):
    """Load verified local bytes; never pull/build/tag/push or publish a port."""
    source = manifest["sourceSha"]
    archive = media / f"distribution/w81-oci-amd64-{source}/kafdeck-linux-amd64.docker.tar.gz"
    expected = manifest["content"]["imageConfigDigests"]["amd64"]
    run("docker", "image", "load", "--input", str(archive), timeout=300)
    image = json.loads(run("docker", "image", "inspect", expected))[0]
    require(image["Id"] == expected and image["Architecture"] == "amd64" and image["Os"] == "linux", "Loaded OCI config/platform identity differs")
    container = run("docker", "create", "--pull=never", "--network=none", "--read-only", "--user=1654:1654",
                    "--cap-drop=ALL", "--security-opt=no-new-privileges", "--tmpfs=/tmp:rw,nosuid,nodev,size=64m",
                    "--tmpfs=/state:rw,nosuid,nodev,uid=1654,gid=1654,mode=0700,size=64m", "--env=HOME=/state", expected).strip()
    require(len(container) == 64 and all(c in "0123456789abcdef" for c in container), "Unexpected owned container identity")
    try:
        run("docker", "start", container)
        details = json.loads(run("docker", "inspect", container))[0]
        require(details["HostConfig"]["NetworkMode"] == "none" and details["HostConfig"]["ReadonlyRootfs"] is True
                and details["Config"]["User"] == "1654:1654" and details["State"]["Pid"] > 0,
                "Installed OCI isolation differs")
        # Join only the container's network namespace; use the runner's Python,
        # never compile, fetch, or mount an unsigned probe into the signed image.
        probe = ("import sys; sys.path.insert(0,sys.argv[1]); from v09_offline_install import isolated_network,http_probe; "
                 "isolated_network(); import json; print(json.dumps(http_probe('http://127.0.0.1:8080')))")
        proof = json.loads(run("nsenter", "--target", str(details["State"]["Pid"]), "--net", "/usr/bin/python3", "-c", probe,
                               str(Path(__file__).resolve().parent), timeout=100))
        return {"architecture": "amd64", "loadedFromVerifiedArchive": True, "configDigest": expected,
                "network": "none", "readOnlyRoot": True, "uid": 1654, **proof,
                "authenticatedOperation": False, "scope": "signed OCI load and isolated startup; external identity/broker operations are separate"}
    finally:
        run("docker", "rm", "--force", container)


def install(directory, manifest):
    require(platform.system() == "Linux" and platform.machine().lower() in ("x86_64", "amd64"),
            "Signed offline installation acceptance requires a native Linux x64 runner")
    isolated_network()
    run("ip", "link", "set", "lo", "up", timeout=10)
    media = directory / "media"
    with tempfile.TemporaryDirectory(prefix="kafdeck-signed-offline-") as temporary:
        result = {"oci": oci_install(media, manifest)}
        if manifest["scope"] == "full-distribution":
            result["native"] = native_install(media, manifest["sourceSha"], Path(temporary))
        else:
            result["native"] = {"status": "not-in-staging-subset", "installedFromArchives": False}
    return result
