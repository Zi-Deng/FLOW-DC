"""Bounded, reproducible context navigation. Copies never earn source credit."""

import hashlib
import json
from pathlib import Path

import review_projection
from workflow import WorkflowError

PAGE_BYTES = 16000
PAGE_LINES = 120
MAX_NAVIGATION_BYTES = 16_000_000


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(row):
    return (json.dumps(row, ensure_ascii=True, separators=(",", ":")) + "\n").encode("ascii")


def pages(rows):
    page = b""
    count = 0
    for row in rows:
        raw = encoded(row)
        if len(raw) > PAGE_BYTES:
            raise WorkflowError("Navigation record exceeds bounded page size")
        if count == PAGE_LINES or len(page) + len(raw) > PAGE_BYTES:
            yield page
            page, count = b"", 0
        page += raw
        count += 1
    if page:
        yield page


def artifact_windows(packet, artifact):
    """LF-based renderer windows, preserving CRLF and final unterminated bytes."""
    raw = review_projection.safe_bytes(packet, artifact)
    raw.decode("utf-8", errors="strict")
    parts = raw.split(b"\n")
    lines = [part + b"\n" for part in parts[:-1]]
    if parts[-1]:
        lines.append(parts[-1])
    rows, copies = [], {}
    start, size, first, count = 0, 0, 1, 0

    def window(end, read, representation="original"):
        rows.append(
            {
                "artifact": artifact,
                "sha256": sha(raw),
                "start_byte": start,
                "end_byte": end,
                "range_sha256": sha(raw[start:end]),
                "representation": representation,
                "read": read,
            }
        )

    for number, line in enumerate(lines, 1):
        if count and (count == PAGE_LINES or size + len(line) > PAGE_BYTES):
            window(start + size, {"file_path": artifact, "offset": first, "limit": count})
            start += size
            size, count = 0, 0
        if len(line) > PAGE_BYTES:
            rendered = review_projection.render(line.decode("utf-8"), start)
            chunks = [json.loads(row) for row in rendered.splitlines()]
            for index, body in enumerate(pages(chunks)):
                name = f"navigation/long-lines/{sha(artifact.encode())[:24]}-{number}-{index}.jsonl"
                copies[name] = body
                last = json.loads(body.splitlines()[-1])[1]
                window(
                    last,
                    {"file_path": name, "offset": 1, "limit": len(body.splitlines())},
                    "context-json-byte-chunks-v1",
                )
                start = last
        else:
            if count == 0:
                first = number
            size += len(line)
            count += 1
    if count:
        window(start + size, {"file_path": artifact, "offset": first, "limit": count})
    return rows, copies


def windows(packet, artifact):
    return artifact_windows(Path(packet), artifact)[0]


def render(packet, unit, report_limit):
    """Pure deterministic materialization; validation recomputes from originals."""
    packet = Path(packet)
    files = {}
    total = 0

    def save(name, body):
        nonlocal total
        total += len(body)
        if len(body) > PAGE_BYTES or total > MAX_NAVIGATION_BYTES:
            raise WorkflowError("Navigation exceeds finite storage envelope")
        files[name] = body
        return {"file_path": name, "offset": 1, "limit": len(body.splitlines()), "sha256": sha(body)}

    def tree(prefix, records):
        level = 0
        while True:
            refs = [
                save(f"navigation/{prefix}/{level}-{i:05d}.jsonl", body)
                for i, body in enumerate(pages(records))
            ]
            if len(refs) <= 1:
                return refs
            records = refs
            level += 1

    inventory = json.loads((packet / "required-material.json").read_bytes())["required"]
    lookup = {row["id"]: row for row in inventory}
    from review_batch import inspection_suggestions

    selected = [lookup[key] for key in unit["required_ids"]]
    suggestions = {row["id"]: row for row in inspection_suggestions(packet, selected, version=6)}
    required = tree(
        "required", [{**row, "inspection_suggestions": suggestions.get(row["id"])} for row in selected]
    )
    preferred = unit.get("navigation_ids", [])
    related_ids = preferred + [key for key in unit["context_ids"] if key not in set(preferred)]
    context = tree("related", [lookup[key] for key in related_ids])
    catalog = []
    originals = sorted(
        p
        for p in packet.rglob("*")
        if p.is_file()
        and p.relative_to(packet).parts[0] != "navigation"
        and p.relative_to(packet).as_posix() != "assignment.json"
    )
    for index, path in enumerate(originals):
        name = path.relative_to(packet).as_posix()
        rows, copies = artifact_windows(packet, name)
        for target, body in copies.items():
            save(target, body)
        catalog.append(
            {
                "artifact": name,
                "bytes": path.stat().st_size,
                "sha256": sha(path.read_bytes()),
                "windows": tree(f"windows/{index:05d}", rows),
            }
        )
    artifacts = tree("artifacts", catalog)
    body = (
        "Bounded unit navigation v1. All original packet files remain available unchanged.\n"
        f"Keep the exact final report under {report_limit} UTF-8 bytes.\n"
        "Follow every required page; inspect actual assigned artifact ranges, their callers, tests, linked criteria and relevant findings.\n"
        "Related pages contain conservative source/test context links, not proven test adequacy. All public findings retain their own primary obligations.\n"
        "Artifact pages index every original byte, including findings-and-dispositions.txt, source-index.json and base-source-index.json.\n"
        "Use listed finite Read offset/limit windows (view: inclusive [offset,offset+limit-1]) or actual numbered Grep matches. Never request whole large files.\n"
        "Context JSON byte chunks preserve exact text/CRLF/Unicode; concatenate decoded text without separators. They grant no original source-line credit.\n"
        "For assigned inventory projections use their original required artifact, not context copies. Missing/truncated/masked evidence remains incomplete.\n"
        + json.dumps(
            {"required": required, "related": context, "all_artifacts": artifacts}, separators=(",", ":")
        )
        + "\n"
    ).encode()
    save("navigation/START.txt", body)
    return files


def materialize(packet, unit, report_limit):
    try:
        files = render(packet, unit, report_limit)
        for name, body in files.items():
            target = Path(packet) / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(body)
        return {"version": 1, "entry": "navigation/START.txt", "files": {k: sha(v) for k, v in files.items()}}
    except (OSError, ValueError, KeyError) as exc:
        raise WorkflowError("Unsupported or missing navigation material") from exc


def validate(packet, unit, report_limit, binding):
    try:
        files = render(packet, unit, report_limit)
        expected = {
            "version": 1,
            "entry": "navigation/START.txt",
            "files": {k: sha(v) for k, v in files.items()},
        }
        actual = {
            p.relative_to(packet).as_posix(): p.read_bytes()
            for p in (Path(packet) / "navigation").rglob("*")
            if p.is_file()
        }
        if binding != expected or actual != files:
            raise WorkflowError("Navigation or original context bytes changed")
    except (OSError, ValueError, KeyError) as exc:
        raise WorkflowError("Unsupported or missing navigation material") from exc
