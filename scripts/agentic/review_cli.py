"""Offline verification/registration of exact native reviewer binaries; never upgrades."""

from __future__ import annotations

import hashlib
import os
import platform
import shutil
import stat
import subprocess
import tarfile
import tempfile
from pathlib import Path

from review_policy import PROVIDERS
from tasks import plain_path, private_directory
from workflow import WorkflowError

MAX_BINARY = 500_000_000


def regular(path, limit):
    path = plain_path(path)
    info = path.stat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_size > limit
        or info.st_uid not in {0, os.getuid()}
        or info.st_mode & 0o022
    ):
        raise WorkflowError("Unsafe reviewer binary or verification material")
    return path


def checksum(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def supported_platform():
    if (
        platform.system() != "Linux"
        or platform.machine() not in {"x86_64", "AMD64"}
        or platform.libc_ver()[0] != "glibc"
    ):
        raise WorkflowError("Only the verified Linux x64 glibc reviewer artifacts are supported")


def verify_claude(binary, manifest, signature, key):
    supported_platform()
    spec = PROVIDERS["claude-code"]["cli"]
    binary = regular(binary, MAX_BINARY)
    manifest = regular(manifest, 1_000_000)
    signature = regular(signature, 100_000)
    key = regular(key, 100_000)
    if checksum(manifest) != spec["manifest_sha256"] or checksum(binary) != spec["binary_sha256"]:
        raise WorkflowError("Pinned Claude manifest or binary digest mismatch")
    from review_coverage_v2 import strict_json

    try:
        value = strict_json(manifest.read_text(encoding="utf-8"))
        if (
            value["version"] != spec["version"]
            or value["platforms"]["linux-x64"]["checksum"] != spec["binary_sha256"]
        ):
            raise ValueError
    except (ValueError, KeyError, TypeError):
        raise WorkflowError("Claude manifest has unsupported version/platform metadata") from None
    gpg = shutil.which("gpg")
    if not gpg:
        raise WorkflowError("GPG is required for the official Claude signed manifest")
    with tempfile.TemporaryDirectory(prefix="agentic-signature-") as temporary:
        args = [gpg, "--batch", "--no-options", "--homedir", temporary]
        env = {"PATH": "/usr/bin:/bin", "HOME": temporary, "LC_ALL": "C"}
        try:
            imported = subprocess.run([*args, "--import", str(key)], env=env, capture_output=True, timeout=20)
            verified = subprocess.run(
                [*args, "--status-fd", "1", "--verify", str(signature), str(manifest)],
                env=env,
                capture_output=True,
                timeout=20,
            )
        except (OSError, subprocess.SubprocessError):
            raise WorkflowError("Claude release signature verification unavailable") from None
        signatures = [
            line.split()
            for line in verified.stdout.decode("ascii", errors="replace").splitlines()
            if line.startswith("[GNUPG:] VALIDSIG ")
        ]
        if (
            imported.returncode
            or verified.returncode
            or len(signatures) != 1
            or signatures[0][2] != spec["signing_fingerprint"]
        ):
            raise WorkflowError(
                "Claude release signature does not match the pinned official signing identity"
            )
    return str(binary)


def verify_copilot(binary, archive):
    supported_platform()
    spec = PROVIDERS["copilot"]["cli"]
    binary, archive = regular(binary, MAX_BINARY), regular(archive, MAX_BINARY)
    if checksum(archive) != spec["archive_sha256"]:
        raise WorkflowError("Pinned Copilot archive digest mismatch")
    try:
        with tarfile.open(archive, "r:gz") as bundle:
            matches = [item for item in bundle.getmembers() if item.name in {"copilot", "./copilot"}]
            if len(matches) != 1 or not matches[0].isfile() or matches[0].size > MAX_BINARY:
                raise ValueError
            with bundle.extractfile(matches[0]) as stream:
                expected = hashlib.file_digest(stream, "sha256").hexdigest()
        if expected != checksum(binary):
            raise ValueError
    except (tarfile.TarError, ValueError, OSError):
        raise WorkflowError("Copilot executable does not match its pinned archive") from None
    return str(binary)


def bundle_path(repo, provider):
    return plain_path(
        repo.main / ".agentic-local/provider-clis" / provider / PROVIDERS[provider]["cli"]["version"]
    )


def executable(repo, provider):
    folder = bundle_path(repo, provider)
    try:
        if provider == "claude-code":
            return verify_claude(
                folder / "claude",
                folder / "manifest.json",
                folder / "manifest.json.sig",
                folder / "signing-key.asc",
            )
        return verify_copilot(folder / "copilot", folder / "copilot.tar.gz")
    except OSError:
        raise WorkflowError(
            "Verified reviewer installation unavailable; register the pinned artifact first"
        ) from None


def register(repo, provider, binary, proof_directory):
    repo.assert_main()
    if provider not in PROVIDERS:
        raise WorkflowError("Unsupported reviewer provider")
    proof = plain_path(proof_directory)
    binary = regular(binary, MAX_BINARY)
    if provider == "claude-code":
        verify_claude(binary, proof / "manifest.json", proof / "manifest.json.sig", proof / "signing-key.asc")
        materials = {
            "claude": binary,
            **{name: proof / name for name in ("manifest.json", "manifest.json.sig", "signing-key.asc")},
        }
    else:
        verify_copilot(binary, proof / "copilot.tar.gz")
        materials = {"copilot": binary, "copilot.tar.gz": proof / "copilot.tar.gz"}
    folder = bundle_path(repo, provider)
    if folder.exists():
        executable(repo, provider)
        return {"provider": provider, "registered": True, "existing": True}
    private_directory(folder.parent)
    with tempfile.TemporaryDirectory(prefix=".register-", dir=folder.parent) as temporary:
        staged = Path(temporary) / "bundle"
        staged.mkdir(mode=0o700)
        for name, source in materials.items():
            shutil.copyfile(source, staged / name)
            (staged / name).chmod(0o700 if name in {"claude", "copilot"} else 0o600)
        if provider == "claude-code":
            verify_claude(
                staged / "claude",
                staged / "manifest.json",
                staged / "manifest.json.sig",
                staged / "signing-key.asc",
            )
        else:
            verify_copilot(staged / "copilot", staged / "copilot.tar.gz")
        staged.rename(folder)
    return {"provider": provider, "registered": True, "existing": False}
