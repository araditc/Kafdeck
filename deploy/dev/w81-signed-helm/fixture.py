"""Owned real HTTPS identity and TLS Kafka fixtures for signed Helm acceptance."""
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import ssl
import subprocess
import sys
import time
import urllib.request
import uuid

private = Path(sys.argv[2])


def read(name):
    return json.loads((private / name).read_text())


def write(name, value):
    (private / name).write_text(json.dumps(value, indent=2) + "\n")


def run(args, data=None, check=True):
    result = subprocess.run(list(map(str, args)), input=data, capture_output=True, text=True, timeout=90)
    if check and result.returncode:
        detail = result.stderr[-1200:] if Path(str(args[0])).name == "openssl" else ""
        raise RuntimeError("Fixture command failed: " + Path(str(args[0])).name + " " + detail)
    return result


if sys.argv[1] == "serve":
    host = sys.argv[3]
    lab = read("identity/lab.json")
    ca = private / "ca.pem"
    run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-noenc", "-days", "1", "-subj", "/CN=W81 private fixture CA",
         "-addext", "basicConstraints=critical,CA:true", "-keyout", private / "ca.key", "-out", ca])
    password = (private / "identity/https-password.txt").read_text().strip()
    for name, san in (("idp", f"IP:{host},IP:127.0.0.1"), ("app", "DNS:localhost,IP:127.0.0.1")):
        run(["openssl", "req", "-newkey", "rsa:2048", "-noenc", "-subj", "/CN=" + (host if name == "idp" else "localhost"),
             "-keyout", private / f"{name}.key", "-out", private / f"{name}.csr"])
        (private / f"{name}.ext").write_text("subjectAltName=" + san + "\nbasicConstraints=CA:false\nextendedKeyUsage=serverAuth\n")
        run(["openssl", "x509", "-req", "-in", private / f"{name}.csr", "-CA", ca, "-CAkey", private / "ca.key",
             "-CAcreateserial", "-days", "1", "-extfile", private / f"{name}.ext", "-out", private / f"{name}.crt"])
        run(["openssl", "pkcs12", "-export", "-inkey", private / f"{name}.key", "-in", private / f"{name}.crt",
             "-certfile", ca, "-out", private / f"{name}.p12", "-passout", "file:" + str(private / "identity/https-password.txt")])
    lab["issuer"] = f"https://{host}:18444/realms/kafdeck-g2"
    lab["caPemFile"] = str(ca)
    write("identity/lab.json", lab)
    owned, logs = [], []
    pg = None
    def launch(args, name, env=None):
        output = (private / (name + ".log")).open("w")
        logs.append(output)
        process = subprocess.Popen(list(map(str, args)), env={**os.environ, **(env or {})}, stdout=output, stderr=output)
        owned.append(process)
        return process
    def stop(*_):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, stop)
    try:
        idp = launch([private / "keycloak-26.8.1/bin/kc.sh", "start", "--http-host=0.0.0.0", "--http-enabled=false", "--db=dev-file",
            "--https-port=18444", "--hostname=" + lab["issuer"].split("/realms/")[0], "--https-key-store-file=" + str(private / "idp.p12"),
            "--cache=local", "--import-realm"], "keycloak", {"KC_HTTPS_KEY_STORE_PASSWORD": password, "KC_DB_URL": lab["databaseUrl"]})
        context = ssl.create_default_context(cafile=ca)
        for attempt in range(150):
            try:
                with urllib.request.urlopen(lab["issuer"] + "/.well-known/openid-configuration", context=context, timeout=2) as response:
                    assert json.load(response)["issuer"] == lab["issuer"]
                break
            except Exception:
                assert idp.poll() is None
                time.sleep(1)
        else:
            raise RuntimeError("Real Keycloak HTTPS discovery failed")
        kafka = private / "kafka"
        properties = {"process.roles": "broker,controller", "node.id": "1", "controller.quorum.voters": "1@127.0.0.1:19093",
            "listeners": "SSL://0.0.0.0:19092,CONTROLLER://127.0.0.1:19093", "advertised.listeners": f"SSL://{host}:19092",
            "controller.listener.names": "CONTROLLER", "listener.security.protocol.map": "SSL:SSL,CONTROLLER:PLAINTEXT",
            "inter.broker.listener.name": "SSL", "offsets.topic.replication.factor": "1", "transaction.state.log.replication.factor": "1",
            "transaction.state.log.min.isr": "1", "group.initial.rebalance.delay.ms": "0", "num.partitions": "1",
            "log.dirs": str(private / "kafka-data"), "ssl.keystore.type": "PKCS12", "ssl.keystore.location": str(private / "idp.p12"),
            "ssl.keystore.password": password, "ssl.key.password": password, "ssl.truststore.type": "PEM", "ssl.truststore.location": str(ca)}
        config = private / "kafka.properties"
        config.write_text("".join(key + "=" + value + "\n" for key, value in properties.items()))
        cluster = run([kafka / "bin/kafka-storage.sh", "random-uuid"]).stdout.strip()
        run([kafka / "bin/kafka-storage.sh", "format", "--cluster-id", cluster, "--config", config])
        broker = launch([kafka / "bin/kafka-server-start.sh", config], "kafka", {"KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m"})
        client = private / "kafka-client.properties"
        client.write_text(f"security.protocol=SSL\nssl.truststore.type=PEM\nssl.truststore.location={ca}\nrequest.timeout.ms=5000\ndefault.api.timeout.ms=5000\n")
        for attempt in range(40):
            listed = run([kafka / "bin/kafka-topics.sh", "--bootstrap-server", f"{host}:19092", "--command-config", client, "--list"], check=False)
            if listed.returncode == 0:
                break
            assert broker.poll() is None
            time.sleep(1)
        else:
            raise RuntimeError("Real TLS Kafka failed")
        topic = "w81-helm-" + uuid.uuid4().hex[:12]
        marker = json.dumps({"probe": "signed-helm-" + uuid.uuid4().hex})
        run([kafka / "bin/kafka-topics.sh", "--bootstrap-server", f"{host}:19092", "--command-config", client,
             "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1"])
        run([kafka / "bin/kafka-console-producer.sh", "--bootstrap-server", f"{host}:19092", "--producer.config", client, "--topic", topic], marker + "\n")
        if os.environ.get("W81_PROVIDER") == "PostgreSql":
            from postgres import PostgresFixture
            pg = PostgresFixture(private, host)
            pg.start()
        generation = str(uuid.uuid4())
        settings = {"Kafdeck:Deployment:Oidc:TrustedCaPemFile": "/run/kafdeck-credentials/ca.pem",
            "Kafdeck:Clusters:0:Tls:CaCertificate": "file:/run/kafdeck-credentials/ca.pem", "Kafdeck:NonBrowserRead:Enabled": "true",
            "Kafdeck:NonBrowserRead:Issuer": lab["issuer"], "Kafdeck:NonBrowserRead:ApiAudience": lab["audience"],
            "Kafdeck:NonBrowserRead:TrustedCaPemFile": "/run/kafdeck-credentials/ca.pem",
            "Kafdeck:NonBrowserRead:Registrations:0:Id": "helm-reader", "Kafdeck:NonBrowserRead:Registrations:0:OwnerId": "owned-fixture",
            "Kafdeck:NonBrowserRead:Registrations:0:ClientId": lab["clientId"], "Kafdeck:NonBrowserRead:Registrations:0:Subject": lab["subject"],
            "Kafdeck:NonBrowserRead:Registrations:0:Enabled": "true", "Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 2400)),
            "Kafdeck:Authorization:Roles:0:Id": "helm-reader", "Kafdeck:Authorization:SubjectBindings:0:Issuer": lab["issuer"],
            "Kafdeck:Authorization:SubjectBindings:0:Subject": lab["subject"], "Kafdeck:Authorization:SubjectBindings:0:RoleIds:0": "helm-reader",
            "Kafdeck:Administration:Mutations:Enabled": "true", "Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled": "false",
            "Kafdeck:Administration:Mutations:MaterialDigestKey": "file:/run/kafdeck-credentials/material-digest",
            "Kafdeck:Governance:CatalogEnabled": "true", "Kafdeck:Governance:DeploymentGeneration": generation, "Logging:LogLevel:Default": "Warning"}
        for index, action in enumerate(("SystemRead", "ClusterRead", "TopicList", "TopicRead", "RecordRead", "CatalogRead")):
            settings[f"Kafdeck:NonBrowserRead:Registrations:0:ReadActions:{index}"] = action
            settings[f"Kafdeck:Authorization:Roles:0:Permissions:{index}:Action"] = action
        write("settings.json", settings)
        write("service.json", {"issuer": lab["issuer"], "audience": lab["audience"], "clientId": lab["clientId"],
             "expectedSubject": lab["subject"], "privateKeyFile": lab["clientPrivateKeyFile"], "trustedCaPemFile": str(ca)})
        (private / "material-digest").write_text(secrets.token_hex(32))
        write("fixture-ready.json", {"issuer": lab["issuer"], "generation": generation, "topic": topic, "message": marker,
            "messageSha256": hashlib.sha256(marker.encode()).hexdigest(), "clusterId": cluster})
        while True:
            assert all(process.poll() is None for process in owned)
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        if pg is not None:
            pg.stop()
        for process in reversed(owned):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=15)
        for output in logs:
            output.close()
elif sys.argv[1] == "probe":
    port, label, evidence = sys.argv[3], sys.argv[4], Path(sys.argv[5])
    fixture = read("fixture-ready.json")
    cli = [private / "installed/cli/kafdeck", "--url", "https://localhost:" + port, "--service-profile", private / "service.json"]
    topic = run([*cli, "topics", "list", "fixture"])
    assert fixture["topic"] in topic.stdout
    write("record-query.json", {"offset": 0, "maxRecords": 1, "maxBytes": 65536})
    result = run([*cli, "views", "v03-records-browse", "fixture", fixture["topic"], "0", "--request-file", private / "record-query.json"])
    values = [base64.b64decode(row["rawValue"]).decode() for row in json.loads(result.stdout)["records"] if row.get("rawValue")]
    assert fixture["message"] in values
    catalog = run([*cli, "catalog", "teams", "fixture"], check=False)
    write("last-catalog.json", {"exit": catalog.returncode})
    if label != "restored-unadmitted":
        assert catalog.returncode == 0
    else:
        assert catalog.returncode != 0
    record = {"phase": label, "realKeycloakHttpsPrivateKeyJwt": True, "realKafkaTlsMetadata": True,
              "exactFixtureMessageSha256": fixture["messageSha256"], "catalogGenerationReadAllowed": catalog.returncode == 0}
    with evidence.open("a") as stream:
        stream.write(json.dumps(record) + "\n")
