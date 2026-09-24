"""Read bounded experiment source snapshots from local committed Git objects.

This module never reads source bytes from the working tree or contacts a remote.
Selected source remains operator-trusted code; hashes establish provenance only.
"""

import hashlib
import os
import re
import subprocess
from pathlib import Path

SOURCE_PATHS = (
    "bin/TaskvineFLOWDC.py",
    "bin/download_batch.py",
    "bin/single_download.py",
)
MAX_SOURCE_BYTES = 4 * 1024 * 1024
GIT_TIMEOUT_SECONDS = 10


class SourceError(ValueError):
    """Fixed diagnostic suitable for a public experiment outcome."""


def _git(repository, *arguments):
    # Inherited Git routing/configuration must not substitute another repository,
    # replacement object, or an automatic promisor-remote fetch.
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_NO_REPLACE_OBJECTS="1",
        GIT_NO_LAZY_FETCH="1",
        GIT_ALLOW_PROTOCOL="",
        GIT_TERMINAL_PROMPT="0",
        GIT_CONFIG_COUNT="1",
        GIT_CONFIG_KEY_0="protocol.allow",
        GIT_CONFIG_VALUE_0="never",
    )
    try:
        result = subprocess.run(
            ["git", "--no-pager", "-C", str(repository), *arguments],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
            timeout=GIT_TIMEOUT_SECONDS,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        raise SourceError("source_git_unavailable") from None
    return result.stdout


def read_source(repository, revision):
    """Return (manifest, files) for an explicit local commit ID.

    Accept abbreviated or full hexadecimal commit IDs, never moving names such as
    HEAD or tags. Only regular blobs at the maintained entrypoints are admitted.
    Each blob is sized before reading; no archives, filters or checkout are used.
    The caller owns private durable staging of the returned bytes.
    """
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-fA-F]{7,64}", revision):
        raise SourceError("source_commit_required")
    repository = Path(repository)
    if not repository.is_absolute() or not repository.is_dir():
        raise SourceError("source_repository_invalid")
    # Disambiguate objects directly: rev-parse --verify gives hex-named refs
    # precedence over abbreviated IDs, even when followed by ^{commit}.
    candidates = _git(repository, "rev-parse", "--disambiguate=" + revision.lower()).decode().splitlines()
    if not candidates:
        raise SourceError("source_git_unavailable")
    if len(candidates) != 1:
        raise SourceError("source_commit_invalid")
    commit = candidates[0]
    if not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", commit):
        raise SourceError("source_commit_invalid")
    if _git(repository, "cat-file", "-t", commit) != b"commit\n":
        raise SourceError("source_commit_required")
    files = {}
    entries = {}
    total = 0
    for path in SOURCE_PATHS:
        tree = _git(repository, "ls-tree", "-z", commit, "--", path)
        try:
            metadata, name = tree.removesuffix(b"\0").split(b"\t")
            mode, kind, object_id = metadata.decode("ascii").split(" ")
            if name.decode("ascii") != path or mode not in ("100644", "100755") or kind != "blob":
                raise ValueError
            if not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", object_id):
                raise ValueError
        except (ValueError, UnicodeError):
            raise SourceError("source_entrypoint_invalid") from None
        size = int(_git(repository, "cat-file", "-s", object_id))
        total += size
        if size < 1 or total > MAX_SOURCE_BYTES:
            raise SourceError("source_size_limit")
        content = _git(repository, "cat-file", "blob", object_id)
        if len(content) != size:
            raise SourceError("source_size_changed")
        files[path] = content
        entries[path] = {"bytes": size, "sha256": hashlib.sha256(content).hexdigest()}
    return {"schema_version": 1, "commit": commit, "files": entries}, files
