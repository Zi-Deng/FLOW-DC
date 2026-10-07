"""Lossless whole-report material for batch9; never review qualification.

Raw model bytes remain the publication artifact. This separate representation
provides bounded native Read lines without changing legacy projection semantics.
Callers must independently qualify the dependency execution and publication.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import PurePosixPath

from claude_reporting import _report
from review_coverage import report_document, strict_json
from tasks import digest

VERSION = 1
CHUNK_CHARACTERS = 128
MAX_REPORT_BYTES = 10_000
MAX_PROJECTION_BYTES = 31_343
MAX_PROJECTION_LINES = 79
MAX_PACKET_PROJECTION_BYTES = 2_000_000
DEPENDENCY_FIELDS = frozenset(
    {
        "review_sha256",
        "diagnostics_sha256",
        "coverage_sha256",
        "reporting_sha256",
        "terminal_sha256",
        "execution_sha256",
        "publication_sha256",
    }
)


def _hex(value):
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _source(raw):
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_REPORT_BYTES:
        raise ValueError("Whole report exceeds exact byte bound")
    text = raw.decode("utf-8")
    document = report_document(text)
    if type(document) is not dict or not _hex(document.get("inventory_sha256")):
        raise ValueError("Whole report lacks inventory binding")
    _report(document, document["inventory_sha256"])
    return text


def render(raw):
    """Project every character, including JSON whitespace and original endings."""
    text = _source(raw)
    offset, rows = 0, []
    for start in range(0, len(text), CHUNK_CHARACTERS):
        chunk = text[start : start + CHUNK_CHARACTERS]
        end = offset + len(chunk.encode("utf-8"))
        row = json.dumps([offset, end, chunk], ensure_ascii=False, separators=(",", ":"))
        for character in ("\u0085", "\u2028", "\u2029"):
            row = row.replace(character, f"\\u{ord(character):04x}")
        rows.append(row)
        offset = end
    projection = ("\n".join(rows) + "\n").encode("utf-8")
    if len(projection) > MAX_PROJECTION_BYTES or len(rows) > MAX_PROJECTION_LINES:
        raise ValueError("Whole report projection exceeds bound")
    return projection


def reconstruct(projection):
    """Require canonical rows and contiguous UTF-8 offsets; refuse normalization."""
    if type(projection) is not bytes or not 0 < len(projection) <= MAX_PROJECTION_BYTES:
        raise ValueError("Invalid whole report projection size")
    if not projection.endswith(b"\n"):
        raise ValueError("Incomplete whole report projection")
    rows = projection[:-1].split(b"\n")
    if not 0 < len(rows) <= MAX_PROJECTION_LINES:
        raise ValueError("Invalid whole report projection lines")
    offset, chunks = 0, []
    for encoded in rows:
        row = strict_json(encoded.decode("utf-8"))
        if (
            type(row) is not list
            or len(row) != 3
            or type(row[0]) is not int
            or type(row[1]) is not int
            or type(row[2]) is not str
            or not 0 < len(row[2]) <= CHUNK_CHARACTERS
        ):
            raise ValueError("Invalid whole report projection row")
        chunk = row[2].encode("utf-8")
        if row[0] != offset or row[1] != offset + len(chunk):
            raise ValueError("Noncontiguous whole report projection")
        offset = row[1]
        if offset > MAX_REPORT_BYTES:
            raise ValueError("Reconstructed report exceeds byte bound")
        chunks.append(chunk)
    raw = b"".join(chunks)
    if render(raw) != projection:
        raise ValueError("Noncanonical whole report projection")
    return raw


def material(raw, artifact, dependency):
    """Return one inventory obligation and projected bytes, without writing files.

    Dependency hashes bind identity only. They do not attest a successful child;
    aggregate admission must recompute execution/publication and source evidence.
    """
    path = PurePosixPath(artifact) if type(artifact) is str else None
    if (
        path is None
        or path.is_absolute()
        or len(path.parts) != 2
        or path.parts[0] != "component-reports"
        or path.suffix != ".txt"
        or str(path) != artifact
        or "\\" in artifact
        or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}\.txt", path.name) is None
    ):
        raise ValueError("Invalid whole report artifact")
    projection = render(raw)
    if (
        type(dependency) is not dict
        or set(dependency) != DEPENDENCY_FIELDS
        or not all(_hex(value) for value in dependency.values())
        or dependency["review_sha256"] != hashlib.sha256(raw).hexdigest()
    ):
        raise ValueError("Invalid whole report dependency binding")
    binding = {
        "schema_version": VERSION,
        "source_artifact": artifact,
        "source_sha256": dependency["review_sha256"],
        "start_byte": 0,
        "end_byte": len(raw),
        "sha256": hashlib.sha256(projection).hexdigest(),
        "dependency": dict(dependency),
    }
    identifier = digest({"whole_report_material": VERSION, **binding})[:24]
    return {
        "id": identifier,
        "kind": "component-report",
        "path": artifact,
        "revision": "packet",
        "artifact": f"whole-report-projections/{identifier}.txt",
        "start_line": 1,
        "end_line": projection.count(b"\n"),
        "bytes": len(projection),
        "links": [],
        "whole_report_projection": binding,
    }, projection


def verify(raw, projection, item, dependency):
    """Compare with independently supplied dependency identity, including ALL rows."""
    if type(item) is not dict:
        raise ValueError("Invalid whole report material")
    expected, rendered = material(raw, item.get("path"), dependency)
    # Canonical digest comparison also distinguishes booleans from integers.
    if digest(item) != digest(expected) or projection != rendered or reconstruct(projection) != raw:
        raise ValueError("Whole report material changed")


def packet_budget(projections, existing_bytes):
    """Account for legacy projections as well; storage fit is not token fit."""
    if type(existing_bytes) is not int or not 0 <= existing_bytes <= MAX_PACKET_PROJECTION_BYTES:
        raise ValueError("Invalid existing projection volume")
    if type(projections) is not list or not 1 <= len(projections) <= 48:
        raise ValueError("Invalid component report count")
    for projection in projections:
        reconstruct(projection)
    total = existing_bytes + sum(map(len, projections))
    if total > MAX_PACKET_PROJECTION_BYTES:
        raise ValueError("Complete projection packet exceeds unchanged storage bound")
    return total
