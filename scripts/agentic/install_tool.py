#!/usr/bin/env python3
"""Install the pinned Linux x86-64 Copilot release after checking its digest."""

import argparse
import hashlib
import io
import platform
import tarfile
import urllib.request
from pathlib import Path

from copilot_policy import CLI_ARCHIVE_SHA256, CLI_ARCHIVE_URL


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tool", choices=["copilot"])
    parser.add_argument("--directory", required=True)
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() not in {"x86_64", "AMD64"}:
        parser.error("This pinned archive is for Linux x86-64; use the official installer for your platform")
    url = CLI_ARCHIVE_URL
    expected = CLI_ARCHIVE_SHA256
    with urllib.request.urlopen(url, timeout=120) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != expected:
        raise SystemExit("Copilot release checksum mismatch; installation refused")
    destination = Path(args.directory).expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / "copilot"
    if target.exists() or target.is_symlink():
        raise SystemExit("Destination exists; inspect it before replacing a tool")
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        member = next(m for m in archive.getmembers() if m.name.lstrip("./") == "copilot" and m.isfile())
        with archive.extractfile(member) as stream:
            target.write_bytes(stream.read())
    target.chmod(0o755)
    print(target)


if __name__ == "__main__":
    main()
