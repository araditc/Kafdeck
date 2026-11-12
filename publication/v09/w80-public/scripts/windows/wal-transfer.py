"""Local-only WAL transfer helper for the disposable PostgreSQL PITR fixture."""
import os
from pathlib import Path
import re
import shutil
import sys
import uuid


def main():
    action, archive, name, pg_path = sys.argv[1:]
    if not re.fullmatch(r"(?:[0-9A-F]{24}(?:\.[0-9A-F]{8}\.backup)?|[0-9A-F]{8}\.history)", name):
        raise ValueError("unexpected WAL file name")
    archived = Path(archive) / name
    provider = Path(pg_path)
    source, target = (provider, archived) if action == "archive" else (archived, provider)
    if not source.is_file():
        return 1
    if action == "archive" and target.exists():
        return 0 if target.read_bytes() == source.read_bytes() else 1
    temporary = target.with_name(target.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with source.open("rb") as reader, temporary.open("xb") as writer:
            shutil.copyfileobj(reader, writer)
            writer.flush()
            os.fsync(writer.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
