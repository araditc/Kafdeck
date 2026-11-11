"""Actual published v0.8 -> signed v0.9 SQLite and PostgreSQL, no external network."""
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts/release"))
from v09_offline_install import http_probe, isolated_network
from v09_prepublication import digest, read, require, write

private, evidence = Path(sys.argv[1]), Path(sys.argv[2])
isolated_network()
environment = {**os.environ, "DOTNET_CLI_TELEMETRY_OPTOUT": "1"}
dotnet = private / "old-dotnet/dotnet"
probe = private / "probe/UpgradeProbe.dll"
key = private / "upgrade-material-key.txt"
key.write_text(secrets.token_hex(32))
password = (private / "postgres-password.txt").read_text().strip()
connection = private / "postgres-connection.txt"
connection.write_text("Host=127.0.0.1;Port=5432;Database=offline_fixture;Username=offline_fixture;Password=" + password + ";Pooling=false")
owned, logs = [], []


def fixture(action, provider, location, *extra):
    result = subprocess.run([str(dotnet), str(probe), action, provider, str(location), *map(str, extra)],
        capture_output=True, text=True, env=environment, timeout=60)
    if result.returncode:
        diagnostic = result.stderr.replace(password, "[fixture-password]").replace(str(private), "[fixture-root]")
        diagnostic = diagnostic.replace(key.read_text(), "[fixture-key]")
        write(evidence / "fixture-diagnostic.json", {"provider": provider, "action": action, "exitCode": result.returncode,
                                                    "redactedFixtureToolError": diagnostic[-4096:]})
        raise ValueError("Published-v0.8 fixture tool failed: " + diagnostic[-2048:])
    return json.loads(result.stdout) if action != "backup" else None


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=15)


def start(provider, location, current, generation, label):
    runtime = private / (provider + "-" + label)
    runtime.mkdir()
    port = 18101 if provider == "sqlite" else 18102
    origin = f"http://127.0.0.1:{port}"
    settings = {"Kafdeck:Deployment:ListenUrls:0": origin, "Kafdeck:Deployment:AccessMode": "Oidc",
        "Kafdeck:Deployment:Oidc:Issuer": "https://localhost:18444/realms/offline-upgrade-fixture",
        "Kafdeck:Deployment:Oidc:ClientId": "offline-upgrade-fixture", "Kafdeck:Administration:Mutations:Enabled": "true",
        "Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled": "false",
        "Kafdeck:Administration:Mutations:MaterialDigestKey": "file:" + str(key),
        "Kafdeck:Administration:Mutations:Persistence:Provider": "Sqlite" if provider == "sqlite" else "PostgreSql",
        "Kafdeck:Administration:Mutations:Persistence:ExecutionMode": "Standalone", "Logging:LogLevel:Default": "Warning"}
    if provider == "sqlite":
        settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"] = str(location)
    else:
        settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"] = "file:" + str(location)
    if current:
        settings["Kafdeck:Governance:DeploymentGeneration"] = generation
    write(runtime / "appsettings.json", settings)
    arguments = [str(private / "installed/api-ui/Kafdeck.Api")] if current else [str(dotnet), str(private / "released-v08/Kafdeck.Api.dll")]
    webroot = private / ("installed/api-ui/wwwroot" if current else "released-v08/wwwroot")
    log = (runtime / "api.log").open("w")
    logs.append(log)
    process = subprocess.Popen([*arguments, "--contentRoot", str(runtime), "--webroot", str(webroot)], cwd=runtime, env=environment, stdout=log, stderr=log)
    owned.append(process)
    http_probe(origin)
    require(process.poll() is None, "Published/signed API exited after offline startup")
    return process


def assert_states(expected, actual):
    require(len(expected["States"]) == 3 and expected["States"] == actual["States"], "Legacy mutation snapshots changed during offline upgrade")


try:
    providers = {}
    for provider in ("sqlite", "postgres"):
        location = private / "old-sqlite.db" if provider == "sqlite" else connection
        generation = str(uuid.uuid4())
        old = start(provider, location, False, generation, "released-open")
        stop(old)
        original = fixture("seed", provider, location)
        old = start(provider, location, False, generation, "released-seeded-open")
        stop(old)
        assert_states(original, fixture("observe", provider, location))
        backup = private / "verified-v08-backup.db"
        if provider == "sqlite":
            fixture("backup", provider, location, backup)
        candidate = start(provider, location, True, generation, "signed-upgrade")
        stop(candidate)
        upgraded = fixture("observe", provider, location)
        assert_states(original, upgraded)
        require({"catalog_owned_schema", "governance_generation_schema"} <= set(upgraded["Tables"]), "Signed candidate did not install v0.9 schemas")
        candidate = start(provider, location, True, generation, "signed-reopen")
        stop(candidate)
        repeated = fixture("observe", provider, location)
        assert_states(original, repeated)
        require(upgraded["Tables"] == repeated["Tables"], "Signed schema was not stable on offline re-entry")
        rollback = False
        if provider == "sqlite":
            restored = private / "restored-v08.db"
            fixture("backup", provider, backup, restored)
            old = start(provider, restored, False, generation, "released-restored-backup")
            stop(old)
            restored_data = fixture("observe", provider, restored)
            assert_states(original, restored_data)
            require(original["Tables"] == restored_data["Tables"], "Verified v0.8 restore changed schema")
            rollback = True
        providers[provider] = {"officialReleasedV08OpenedActualDatabase": True, "threeLegacySnapshotsPreserved": True,
            "legacySnapshotHashes": original["States"], "signedCandidateMigratedSchema": True, "signedCandidateReopened": True,
            "schemaStableOnReentry": True, "verifiedPreUpgradeBackupRollback": rollback,
            "actualPhysicalPitrClaimed": False, "interruptionClaimed": False, "syntheticApprovalFixture": True}
    isolated_network()
    write(evidence / "offline-upgrade.json", {"schemaVersion": 1, "applicationSourceSha": read(evidence / "signed-input.json")["applicationSourceSha"],
        "signedManifestSha256": read(evidence / "signed-input.json")["signedManifestSha256"], "baseline": read(evidence / "upgrade-input.json"),
        "namespaceInterfaces": ["lo"], "ipv4DefaultRoute": False, "ipv6DefaultRoute": False, "providers": providers,
        "actualOfficialV08ToSignedV09OfflineUpgrade": True, "productMutationDispatched": False,
        "scope": "actual official published image binaries open v0.8 data and final signed native app migrates/reopens both providers inside network-none namespace; synthetic pending/approved/unknown migration fixtures; SQLite verified backup rollback; physical PITR/fencing/interruption and authenticated journey are separate proofs"})
finally:
    for process in reversed(owned):
        stop(process)
    for log in logs:
        log.close()
