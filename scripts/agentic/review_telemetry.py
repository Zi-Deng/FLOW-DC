"""Read one fresh CLI session's bounded events before its temporary home is removed.

The wrapper supplies --session-id. CLI stdout is only completion framing; SDK-shaped
session events are the sole tool evidence. Neither raw source is retained.
"""

import json
import os
import stat
from pathlib import Path

import review_coverage as coverage
from workflow import WorkflowError

KNOWN_TYPES = coverage.IGNORED_EVENTS | {
    "tool.execution_start",
    "tool.execution_complete",
    "assistant.message",
    "session.idle",
    "session.shutdown",
    "result",
    "session.error",
}
SHAPE_KEYS = {"count", "call_id", "arguments", "content"}


def decode(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > coverage.MAX_STREAM_BYTES:
        raise ValueError("stream limit")
    events = []
    for line in raw.split("\n"):
        if line.strip():
            event = coverage.strict_json(line)
            if not isinstance(event, dict) or not isinstance(event.get("type"), str):
                raise ValueError("event envelope")
            events.append(event)
            if len(events) > coverage.MAX_EVENTS:
                raise ValueError("event limit")
    return events


def shapes(events):
    result = {}
    for event in events:
        kind = event["type"] if event["type"] in KNOWN_TYPES else "unknown"
        row = result.setdefault(kind, dict.fromkeys(sorted(SHAPE_KEYS), 0))
        row["count"] += 1
        data = event.get("data")
        if isinstance(data, dict):
            row["call_id"] += isinstance(data.get("toolCallId"), str)
            row["arguments"] += isinstance(data.get("arguments"), dict)
            row["content"] += isinstance(data.get("content"), str) or (
                isinstance(data.get("result"), dict) and isinstance(data["result"].get("content"), str)
            )
    return result


def session_events(state, session_id):
    root = Path(state) / "session-state"
    if Path(state).is_symlink() or root.is_symlink() or not root.is_dir():
        raise WorkflowError("missing_session_events")
    children = list(root.iterdir())
    if len(children) != 1 or children[0].name != session_id or children[0].is_symlink():
        raise WorkflowError("ambiguous_session_events")
    path = children[0] / "events.jsonl"
    # O_NOFOLLOW plus fstat refuses links, directories, FIFOs and oversized files.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > coverage.MAX_STREAM_BYTES:
            raise WorkflowError("unsafe_or_oversized_session_events")
        events = decode(source.read(coverage.MAX_STREAM_BYTES + 1))
    starts = [event for event in events if event["type"] == "session.start"]
    if len(starts) != 1 or starts[0].get("data", {}).get("sessionId") != session_id:
        raise WorkflowError("session_identity_mismatch")
    return events


def capture(stdout, state, session_id, packet, workspace, **kwargs):
    reasons, observed, framing = set(), [], []
    try:
        observed = decode(stdout)
    except (ValueError, UnicodeError):
        reasons.add("malformed_stdout_framing")
    terminals = [event for event in observed if event["type"] == "result"]
    if len(terminals) != 1 or not observed or observed[-1].get("type") != "result":
        reasons.add("missing_or_ambiguous_cli_completion")
    else:
        framing = terminals
    for event in observed:
        kind, data = event["type"], event.get("data") or {}
        if kind not in KNOWN_TYPES or kind == "session.error":
            reasons.add("unsupported_stdout_event")
        if not isinstance(data, dict):
            reasons.add("malformed_stdout_framing")
            continue
        if data.get("parentToolCallId") or data.get("mcpServerName") or data.get("mcpToolName"):
            reasons.add("delegated_or_mcp_event")
        if kind == "tool.execution_start" and data.get("toolName") not in coverage.TOOLS:
            reasons.add("forbidden_or_unsupported_tool")
        if kind == "session.start" and data.get("sessionId", session_id) != session_id:
            reasons.add("session_identity_mismatch")
    events = []
    try:
        events = session_events(state, session_id)
    except WorkflowError as exc:
        reasons.add(str(exc))
    except (OSError, ValueError, TypeError, AttributeError):
        reasons.add("unavailable_or_malformed_session_events")
    # Never fabricate correlation by adjacency. If stdout shows calls missing
    # from the full session event source, that source cannot establish coverage.
    for kind in ("tool.execution_start", "tool.execution_complete"):
        if sum(e["type"] == kind for e in observed) > sum(e["type"] == kind for e in events):
            reasons.add("session_tool_telemetry_incomplete")
    combined = "\n".join(json.dumps(event, ensure_ascii=False) for event in events + framing)
    report, diagnostics = coverage.parse_events(combined, packet, workspace, **kwargs)
    diagnostics["reasons"] = sorted(set(diagnostics["reasons"]) | reasons)
    diagnostics["telemetry"] = {
        "source": "session-state" if events else "unavailable",
        "stdout_shapes": shapes(observed),
        "session_shapes": shapes(events),
    }
    return report, diagnostics


def validate_summary(value):
    if (
        not isinstance(value, dict)
        or set(value) != {"source", "stdout_shapes", "session_shapes"}
        or value["source"] not in {"stdout", "session-state", "unavailable"}
    ):
        raise WorkflowError("Invalid telemetry summary")
    for name in ("stdout_shapes", "session_shapes"):
        rows = value[name]
        if not isinstance(rows, dict) or set(rows) - KNOWN_TYPES - {"unknown"}:
            raise WorkflowError("Unsafe telemetry event type")
        for row in rows.values():
            if (
                not isinstance(row, dict)
                or set(row) != SHAPE_KEYS
                or any(type(n) is not int or not 0 <= n <= coverage.MAX_EVENTS for n in row.values())
            ):
                raise WorkflowError("Invalid telemetry shape counts")
