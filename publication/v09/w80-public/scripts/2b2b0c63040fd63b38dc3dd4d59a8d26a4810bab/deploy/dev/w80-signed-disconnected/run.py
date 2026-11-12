"""Actual signed native app, genuine local Keycloak and Kafka, zero external egress."""
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts/release"))
from v09_offline_install import http_probe, isolated_network
from v09_prepublication import digest, read, require, write

private, evidence = Path(sys.argv[1]), Path(sys.argv[2])
isolated_network()
subprocess.run(["ip", "link", "set", "lo", "up"], check=True)
lab = read(private / "identity/lab.json")
owned, handles = [], []
environment = {**os.environ, "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m", "DOTNET_CLI_TELEMETRY_OPTOUT": "1"}


def launch(args, name, extra=None):
    log = (private / (name + ".log")).open("w")
    handles.append(log)
    process = subprocess.Popen([str(arg) for arg in args], cwd=private, env={**environment, **(extra or {})}, stdout=log, stderr=log)
    owned.append(process)
    return process


def execute(args, *, data=None, timeout=90, allow_failure=False):
    process = subprocess.run([str(arg) for arg in args], cwd=private, env=environment, input=data,
                             capture_output=True, text=True, timeout=timeout)
    if not allow_failure:
        require(process.returncode == 0, "Bounded fixture/native command failed: " + Path(str(args[0])).name)
    return process


try:
    # Staged Java/Keycloak and Kafka distributions: no download or dependency restore here.
    idp = launch([private / "keycloak-26.8.1/bin/kc.sh", "start", "--http-host=127.0.0.1", "--http-enabled=false",
        "--db=dev-file", "--https-port=18444", "--hostname=https://localhost:18444",
        "--https-key-store-file=" + str(private / "identity/https.p12"), "--cache=local", "--import-realm"], "keycloak",
        {"KC_HTTPS_KEY_STORE_PASSWORD": (private / "identity/https-password.txt").read_text().strip(), "KC_DB_URL": lab["databaseUrl"]})
    identity_ready = False
    for _ in range(150):
        response = execute(["curl", "--silent", "--fail", "--cacert", lab["caPemFile"], "--connect-timeout", "1", "--max-time", "2",
                            lab["issuer"] + "/.well-known/openid-configuration"], allow_failure=True, timeout=5)
        if response.returncode == 0 and json.loads(response.stdout)["issuer"] == lab["issuer"]:
            identity_ready = True
            break
        require(idp.poll() is None, "Staged Keycloak exited before genuine HTTPS discovery")
        time.sleep(1)
    require(identity_ready, "Staged Keycloak HTTPS discovery failed")
    kafka = private / "kafka"
    configuration = private / "kafka.properties"
    configuration.write_text("\n".join([
        "process.roles=broker,controller", "node.id=1", "controller.quorum.voters=1@127.0.0.1:19093",
        "listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093", "advertised.listeners=PLAINTEXT://127.0.0.1:19092",
        "controller.listener.names=CONTROLLER", "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "inter.broker.listener.name=PLAINTEXT", "offsets.topic.replication.factor=1", "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1", "group.initial.rebalance.delay.ms=0", "num.partitions=1", "log.dirs=" + str(private / "kafka-data")]) + "\n")
    cluster_id = execute([kafka / "bin/kafka-storage.sh", "random-uuid"]).stdout.strip()
    execute([kafka / "bin/kafka-storage.sh", "format", "--cluster-id", cluster_id, "--config", configuration])
    broker = launch([kafka / "bin/kafka-server-start.sh", configuration], "kafka")
    ready = False
    for _ in range(60):
        with socket.socket() as connection:
            connection.settimeout(1)
            if connection.connect_ex(("127.0.0.1", 19092)) == 0:
                ready = True
                break
        require(broker.poll() is None, "Staged real Kafka exited")
        time.sleep(1)
    require(ready, "Staged Kafka listener failed")
    topic, marker = "w80-signed-offline-01c7553", "signed-disconnected-auth-01c7553"
    execute([kafka / "bin/kafka-topics.sh", "--bootstrap-server", "127.0.0.1:19092", "--create", "--topic", topic,
             "--partitions", "1", "--replication-factor", "1"])
    message = json.dumps({"probe": marker})
    execute([kafka / "bin/kafka-console-producer.sh", "--bootstrap-server", "127.0.0.1:19092", "--topic", topic], data=message + "\n")
    origin = "http://127.0.0.1:18080"
    actions = ["SystemRead", "ClusterRead", "BrokerRead", "TopicList", "TopicRead", "RecordRead"]
    settings = {"Kafdeck:Deployment:ListenUrls:0": origin, "Kafdeck:Deployment:AccessMode": "Oidc",
        "Kafdeck:Deployment:Oidc:Issuer": lab["issuer"], "Kafdeck:Deployment:Oidc:ClientId": lab["cliClientId"],
        "Kafdeck:Deployment:Oidc:TrustedCaPemFile": lab["caPemFile"], "Kafdeck:NonBrowserRead:Enabled": "true",
        "Kafdeck:NonBrowserRead:Issuer": lab["issuer"], "Kafdeck:NonBrowserRead:ApiAudience": lab["audience"],
        "Kafdeck:NonBrowserRead:TrustedCaPemFile": lab["caPemFile"], "Kafdeck:NonBrowserRead:Registrations:0:Id": "offline-service",
        "Kafdeck:NonBrowserRead:Registrations:0:OwnerId": "offline-fixture", "Kafdeck:NonBrowserRead:Registrations:0:ClientId": lab["clientId"],
        "Kafdeck:NonBrowserRead:Registrations:0:Subject": lab["subject"], "Kafdeck:NonBrowserRead:Registrations:0:Enabled": "true",
        "Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 1200)),
        "Kafdeck:Authorization:Roles:0:Id": "offline-reader", "Kafdeck:Authorization:SubjectBindings:0:Issuer": lab["issuer"],
        "Kafdeck:Authorization:SubjectBindings:0:Subject": lab["subject"], "Kafdeck:Authorization:SubjectBindings:0:RoleIds:0": "offline-reader",
        "Kafdeck:Clusters:0:Id": "offline", "Kafdeck:Clusters:0:BootstrapServers:0": "127.0.0.1:19092",
        "Logging:LogLevel:Default": "Warning"}
    for index, action in enumerate(actions):
        settings[f"Kafdeck:NonBrowserRead:Registrations:0:ReadActions:{index}"] = action
        settings[f"Kafdeck:Authorization:Roles:0:Permissions:{index}:Action"] = action
    write(private / "appsettings.json", settings)
    profile = private / "service.json"
    write(profile, {"issuer": lab["issuer"], "audience": lab["audience"], "clientId": lab["clientId"],
                    "expectedSubject": lab["subject"], "privateKeyFile": lab["clientPrivateKeyFile"], "trustedCaPemFile": lab["caPemFile"]})
    installed = private / "installed"
    api = launch([installed / "api-ui/Kafdeck.Api", "--contentRoot", private, "--webroot", installed / "api-ui/wwwroot"], "api")
    startup = http_probe(origin)
    unsigned = execute(["curl", "--silent", "--output", "/dev/null", "--write-out", "%{http_code}", origin + "/api/v1/system/info"])
    require(unsigned.stdout == "401", "Unauthenticated operator read was not rejected")
    cli = [installed / "cli/kafdeck", "--url", origin, "--service-profile", profile]
    capabilities = execute([*cli, "views", "v08-bearer-read-capabilities"])
    require(json.loads(capabilities.stdout)["contracts"], "Genuine Keycloak CLI read returned no contracts")
    topics = execute([*cli, "topics", "list", "offline"])
    require(topic in topics.stdout, "Installed signed CLI did not observe real Kafka topic metadata")
    query = private / "record-query.json"
    write(query, {"offset": 0, "maxRecords": 1, "maxBytes": 65536})
    records = execute([*cli, "views", "v03-records-browse", "offline", topic, "0", "--request-file", query])
    page = json.loads(records.stdout)
    # The bounded safe projection retains raw bytes as base64; no decoder or masking bypass.
    values = [base64.b64decode(item["rawValue"]).decode() for item in page["records"] if item.get("rawValue")]
    require(message in values, "Installed signed authenticated record read did not return the exact fixture message")
    rpc = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "offline-auth", "version": "0.9.0"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "read_v01_topics_list", "arguments": {"clusterId": "offline"}}}]
    result = execute([installed / "mcp/kafdeck-mcp", "--base-url", origin, "--service-profile", profile], data="".join(json.dumps(item) + "\n" for item in rpc))
    replies = {item["id"]: item for item in map(json.loads, result.stdout.splitlines())}
    require(replies[1]["result"]["serverInfo"]["version"] == "0.9.0" and not replies[2]["result"].get("isError")
            and replies[2]["result"]["structuredContent"]["httpStatus"] == 200 and topic in json.dumps(replies[2]),
            "Installed signed MCP did not authenticate and observe genuine broker metadata")
    isolated_network()
    write(evidence / "journey.json", {"schemaVersion": 1, "applicationSourceSha": read(evidence / "signed-input.json")["applicationSourceSha"],
        "signedManifestSha256": read(evidence / "signed-input.json")["signedManifestSha256"], "namespaceInterfaces": ["lo"],
        "ipv4DefaultRoute": False, "ipv6DefaultRoute": False, "genuineKeycloak": True, "identity": "Keycloak26.8.1 HTTPS private_key_jwt",
        "actualSignedInstalledApiUi": startup, "cliAuthenticatedRead": True, "mcpAuthenticatedRead": True, "unauthenticatedReadRejected": True,
        "realKafka": True, "realTopicObservedThroughSignedApi": True, "actualFixtureMessageReadThroughSignedApi": True,
        "topic": topic, "fixtureMessageSha256": hashlib.sha256(message.encode()).hexdigest(), "authenticatedJourney": True,
        "disconnectedFixtureOnly": True, "productMutationExecuted": False, "humanBusinessApprovalClaimed": False,
        "scope": "real service-authenticated operator reads and exact fixture Kafka record through installed signed API/CLI/MCP; fixtures seeded directly; no product mutation/approval or browser-human session claimed"})
finally:
    for process in reversed(owned):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=15)
    for log in handles:
        log.close()
