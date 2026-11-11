"""Reuse the bounded physical recovery laboratory behind a local TLS endpoint."""
import json
from pathlib import Path
import select
import socket
import subprocess
import threading


class PostgresFixture:
    def __init__(self, private, host):
        self.private, self.host = private, host
        self.root = private / "pg"
        self.control = Path(__file__).resolve().parents[1] / "prepare-postgres-g2-recovery.ps1"
        self.bin = subprocess.check_output(["pg_config", "--bindir"], text=True).strip()

    def action(self, action):
        log = self.private / ("pg-" + action.lower() + ".log")
        with log.open("w") as output:
            subprocess.run(["pwsh", "-NoProfile", "-File", str(self.control), "-Action", action,
                "-OutputDirectory", str(self.root), "-BinDirectory", self.bin], stdout=output, stderr=output, check=True, timeout=90)

    def start(self):
        self.action("Prepare")
        lab = json.loads((self.root / "lab.json").read_text())
        for node in ("primary", "standby"):
            directory = Path(lab[node]["dataDirectory"])
            with (directory / "postgresql.auto.conf").open("a") as config:
                config.write(f"\nssl=on\nssl_cert_file='{self.private}/idp.crt'\nssl_key_file='{self.private}/idp.key'\n")
            subprocess.run([str(Path(self.bin) / "pg_ctl"), "-D", str(directory), "reload"], check=True, capture_output=True)
        password = Path(lab["adminPasswordFile"]).read_text().strip()
        conn = f"Host={self.host};Port=15440;Username=g2_recovery_owner;Password={password};Database=postgres;SSL Mode=VerifyFull;Root Certificate=/run/kafdeck-credentials/ca.pem;Pooling=false;Timeout=5;Command Timeout=15"
        (self.private / "postgres-connection").write_text(conn)
        (self.private / "host.pgconn").write_text(conn.replace("/run/kafdeck-credentials/ca.pem", str(self.private / "ca.pem")))
        server = socket.socket()
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, 15440))
        server.listen()
        self.server = server
        def relay(client):
            state = json.loads((self.root / "lab.json").read_text())
            port = state[state.get("activeEndpoint", "primary")]["port"]
            try:
                with client, socket.create_connection(("127.0.0.1", port), timeout=5) as backend:
                    while True:
                        ready, _, _ = select.select([client, backend], [], [], 15)
                        if not ready:
                            continue
                        for source in ready:
                            data = source.recv(65536)
                            if not data:
                                return
                            (backend if source is client else client).sendall(data)
            except OSError:
                client.close()
        def accept():
            while True:
                try:
                    client, _ = server.accept()
                except OSError:
                    return
                threading.Thread(target=relay, args=(client,), daemon=True).start()
        threading.Thread(target=accept, daemon=True).start()

    def stop(self):
        if hasattr(self, "server"):
            self.server.close()
        if (self.root / "lab.json").exists():
            self.action("Stop")
