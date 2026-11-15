"""Package the already-tested agent, its source and operator guide; no recompilation."""
from pathlib import Path
import hashlib,zipfile
repo=Path(__file__).resolve().parents[2]
out=repo/'artifacts/windows-offline';out.mkdir(parents=True,exist_ok=True)
archive=out/'kafdeck-kafka-agent-java21.zip'
files=[p for p in (repo/'tools/kafka-agent').rglob('*') if p.is_file() and p.suffix in ('.java','.py','.MF','.jar','.md','.example')]
files += [repo/'LICENSE',repo/'docs/operator/kafka-agent.md',repo/'docs/operator/windows-offline-install.md',repo/'docs/operator/kafka-monitoring.md',repo/'docs/operator/kafka-agent-java21.fa.md',repo/'docs/operator/windows-offline-install.fa.md']
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
    z.writestr('README.md','# Kafdeck Kafka agent for Java 21\n\nStart with [the agent guide](docs/operator/kafka-agent.md). The prebuilt JAR and full source are under `tools/kafka-agent`. No tokens or private keys are included. Build commands run from this archive root.\n')
    for p in sorted(files): z.write(p,p.relative_to(repo).as_posix())
with zipfile.ZipFile(archive) as z: assert z.testzip() is None
digest=hashlib.file_digest(archive.open('rb'),'sha256').hexdigest()
archive.with_suffix('.zip.sha256').write_text(digest+'  '+archive.name+'\n',encoding='ascii')
print(f'{archive}: {archive.stat().st_size} bytes, SHA256 {digest}')
