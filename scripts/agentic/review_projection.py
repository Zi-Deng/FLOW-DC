"""Versioned lossless navigation for long inert UTF-8 lines; never tool credit.

Rows are JSON [absolute UTF-8 start byte, exclusive end byte, exact text].
Canonical reproduction binds every projected byte to the unchanged raw snapshot.
Only actual returned projection lines can satisfy their inventory obligations.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

VERSION = 1
CHUNK_CHARACTERS = 128
MAX_LINE_BYTES = 65536
MAX_PACKET_BYTES = 2_000_000


def render(line, start_byte):
    """Bound expansion, preserve line endings and escape controls without loss."""
    if not line or len(line.encode("utf-8")) > MAX_LINE_BYTES:
        raise ValueError("Line exceeds bounded projection byte limit")
    offset, rows = start_byte, []
    for start in range(0, len(line), CHUNK_CHARACTERS):
        text = line[start : start + CHUNK_CHARACTERS]
        end = offset + len(text.encode("utf-8"))
        rows.append(json.dumps([offset, end, text], ensure_ascii=True, separators=(",", ":")))
        offset = end
    return ("\n".join(rows) + "\n").encode("ascii")


def safe_bytes(packet, artifact):
    if not isinstance(artifact, str) or Path(artifact).is_absolute():
        raise ValueError("Invalid projection artifact")
    target = packet / artifact
    if ".." in Path(artifact).parts or not target.resolve().is_relative_to(packet.resolve()):
        raise ValueError("Invalid projection artifact")
    return target.read_bytes()


def validate(packet, item):
    """Reject changed/missing/raw or rendered bytes even with positive tool spans."""
    packet = Path(packet)
    try:
        binding = item["projection"]
        if not isinstance(binding, dict) or set(binding) != {
            "schema_version",
            "source_artifact",
            "source_sha256",
            "source_line",
            "start_byte",
            "end_byte",
            "sha256",
        }:
            return False
        if type(binding["schema_version"]) is not int or binding["schema_version"] != VERSION:
            return False
        if any(type(binding[key]) is not int for key in ("source_line", "start_byte", "end_byte")):
            return False
        raw = safe_bytes(packet, binding["source_artifact"])
        if hashlib.sha256(raw).hexdigest() != binding["source_sha256"]:
            return False
        lines = raw.decode("utf-8").splitlines(keepends=True)
        number = binding["source_line"]
        if not 1 <= number <= len(lines):
            return False
        start = sum(len(line.encode("utf-8")) for line in lines[: number - 1])
        end = start + len(lines[number - 1].encode("utf-8"))
        if (start, end) != (binding["start_byte"], binding["end_byte"]):
            return False
        expected = render(lines[number - 1], start)
        actual = safe_bytes(packet, item["artifact"])
        return actual == expected and hashlib.sha256(actual).hexdigest() == binding["sha256"]
    except (OSError, ValueError, KeyError, TypeError):
        return False
