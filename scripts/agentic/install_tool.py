#!/usr/bin/env python3
"""Download explicitly selected pinned reviewer artifacts; never upgrade during review."""

import argparse
import hashlib
import io
import shutil
import tarfile
import tempfile
import urllib.request
from pathlib import Path

import review_cli
from copilot_policy import CLI_ARCHIVE_SHA256, CLI_ARCHIVE_URL
from review_policy import PROVIDERS
from tasks import plain_path
from workflow import WorkflowError


def download(url, limit):
    with urllib.request.urlopen(url, timeout=120) as response:
        if not response.geturl().startswith("https://"):
            raise WorkflowError("Reviewer artifacts require HTTPS")
        data = response.read(limit + 1)
    if len(data) > limit:
        raise WorkflowError("Reviewer artifact exceeds its download bound")
    return data


def install(tool, directory):
    review_cli.supported_platform()
    destination = plain_path(Path(directory).expanduser())
    destination.mkdir(parents=True, exist_ok=True)
    names = (
        ["copilot", "copilot.tar.gz"]
        if tool == "copilot"
        else ["claude", "manifest.json", "manifest.json.sig", "signing-key.asc"]
    )
    if any((destination / name).exists() or (destination / name).is_symlink() for name in names):
        raise WorkflowError("Destination exists; inspect it before replacing a reviewer")
    with tempfile.TemporaryDirectory(prefix=".reviewer-install-", dir=destination) as temporary:
        staged = Path(temporary)
        if tool == "copilot":
            data = download(CLI_ARCHIVE_URL, review_cli.MAX_BINARY)
            if hashlib.sha256(data).hexdigest() != CLI_ARCHIVE_SHA256:
                raise WorkflowError("Copilot release checksum mismatch")
            (staged / "copilot.tar.gz").write_bytes(data)
            with tarfile.open(fileobj=io.BytesIO(data)) as archive:
                members = [m for m in archive.getmembers() if m.name in {"copilot", "./copilot"}]
                if len(members) != 1 or not members[0].isfile() or members[0].size > review_cli.MAX_BINARY:
                    raise WorkflowError("Unsupported Copilot archive contents")
                with archive.extractfile(members[0]) as stream:
                    (staged / "copilot").write_bytes(stream.read())
            review_cli.verify_copilot(staged / "copilot", staged / "copilot.tar.gz")
        else:
            spec = PROVIDERS["claude-code"]["cli"]
            base = "https://downloads.claude.ai/claude-code-releases/" + spec["version"]
            for name, url, limit in [
                ("manifest.json", base + "/manifest.json", 1_000_000),
                ("manifest.json.sig", base + "/manifest.json.sig", 100_000),
                ("signing-key.asc", "https://downloads.claude.ai/keys/claude-code.asc", 100_000),
                ("claude", base + "/linux-x64/claude", review_cli.MAX_BINARY),
            ]:
                (staged / name).write_bytes(download(url, limit))
            review_cli.verify_claude(
                staged / "claude",
                staged / "manifest.json",
                staged / "manifest.json.sig",
                staged / "signing-key.asc",
            )
        binary = "copilot" if tool == "copilot" else "claude"
        (staged / binary).chmod(0o755)
        for name in names:
            with (destination / name).open("xb") as output, (staged / name).open("rb") as source:
                shutil.copyfileobj(source, output)
            (destination / name).chmod(0o755 if name == binary else 0o644)
    return str(destination / binary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tool", choices=["copilot", "claude-code"])
    parser.add_argument("--directory", required=True)
    args = parser.parse_args()
    try:
        print(install(args.tool, args.directory))
    except (WorkflowError, OSError, ValueError, tarfile.TarError):
        raise SystemExit("Pinned reviewer installation failed; no inference or automatic fallback") from None


if __name__ == "__main__":
    main()
