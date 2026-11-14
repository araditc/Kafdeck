"""Exercise the distributed JAR on a target JVM, using a local authenticated receiver."""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from queue import Empty, Queue
import secrets
import subprocess
from tempfile import TemporaryDirectory
import threading
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--java", default="java")
    parser.add_argument("--javac", default="javac", help="JDK 11+ compiler for the test fixture")
    args = parser.parse_args()
    directory = Path(__file__).resolve().parent
    jar = directory.parent / "kafdeck-kafka-agent.jar"
    captures = Queue()
    token = secrets.token_hex(32)

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/monitoring/ingest" or self.headers.get("X-Kafdeck-Agent-Token") != token:
                self.send_error(403)
                return
            batch = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            captures.put((time.monotonic(), batch))
            self.send_response(204)
            self.send_header("X-Kafdeck-Interval-Seconds", "10")
            self.send_header("X-Kafdeck-Topics", "v1:orders")
            self.end_headers()

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Receiver) as server, TemporaryDirectory() as tmp:
        threading.Thread(target=server.serve_forever, daemon=True).start()
        root = Path(tmp)
        secret = root / "token"
        secret.write_text(token)
        secret.chmod(0o600)
        config = root / "agent.properties"
        config.write_text(f"endpoint=http://127.0.0.1:{server.server_port}/monitoring/ingest\n"
                          f"tokenFile={secret.as_posix()}\nintervalSeconds=60\nincludeTopicPartitions=false\n")
        subprocess.run([args.javac, "--release", "11", "-d", tmp, str(directory / "AgentSmoke.java")], check=True)
        version = subprocess.check_output([args.java, "-version"], stderr=subprocess.STDOUT, text=True).splitlines()[0]
        with subprocess.Popen([args.java, f"-javaagent:{jar}={config}", "-cp", tmp, "AgentSmoke"]) as process:
            try:
                first_time, first = captures.get(timeout=20)
                second_time, second = captures.get(timeout=21)
                for batch in (first, second):
                    assert batch["last"] and batch["part"] == 0
                    values = {row["metric"]: row["value"] for row in batch["samples"]}
                    assert values["kafka.test:Probe/Compatibility/Value"] == 73
                    assert "java.lang:Memory/HeapMemoryUsage/used" in values
                assert first["cycle"] != second["cycle"]
                assert not any("topic" in row["labels"] for row in first["samples"])
                scoped = [row["labels"] for row in second["samples"] if "topic" in row["labels"]]
                assert scoped == [{"partition":"0","topic":"orders"}]
                assert 0 <= values["kafdeck.jvm/cpu_percent"] <= 100
                assert values["kafdeck.jvm/gc_time_percent"] >= 0
                assert 9 <= second_time - first_time <= 21
                print(f"{version}: agent, JMX, CPU, GC, interval OK; SHA256 {hashlib.sha256(jar.read_bytes()).hexdigest()}")
            except Empty as error:
                raise RuntimeError("Timed out waiting for authenticated agent samples") from error
            finally:
                process.terminate()
                process.wait(timeout=10)
                server.shutdown()


if __name__ == "__main__":
    main()
