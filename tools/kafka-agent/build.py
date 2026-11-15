from pathlib import Path
import argparse
import shutil
import subprocess
import tempfile
import zipfile

parser = argparse.ArgumentParser(description="Build the Kafdeck Kafka monitoring agent using JDK 11 or newer.")
parser.add_argument("--jdk", type=Path, help="JDK directory (defaults to javac on PATH)")
parser.add_argument("--output", type=Path, default=Path("artifacts/kafdeck-kafka-agent.jar"))
args = parser.parse_args()
base = Path(__file__).resolve().parent
def tool(name):
    return str(args.jdk / "bin" / name) if args.jdk else shutil.which(name) or name

args.output.parent.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory() as classes:
    subprocess.run([tool("javac"), "--release", "11", "-d", classes, str(base/"src/org/kafdeck/monitoring/KafkaMonitoringAgent.java")], check=True)
    with zipfile.ZipFile(args.output, "w") as jar:
        def add(path, content):
            entry = zipfile.ZipInfo(path, (2000, 1, 1, 0, 0, 0))
            entry.create_system = 3
            entry.external_attr = 0o100644 << 16
            jar.writestr(entry, content, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
        manifest = (base / "MANIFEST.MF").read_text().strip().replace("\n", "\r\n") + "\r\n\r\n"
        add("META-INF/MANIFEST.MF", manifest.encode("ascii"))
        for file in sorted(Path(classes).rglob("*.class")):
            add(file.relative_to(classes).as_posix(), file.read_bytes())
print(args.output.resolve())
