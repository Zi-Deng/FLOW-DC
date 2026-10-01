"""Read one fresh CLI session's bounded events before its temporary home is removed.

The wrapper supplies --session-id. CLI stdout is only completion framing; SDK-shaped
session events are the sole tool evidence. Neither raw source is retained.
"""

import json
import os
import re
import stat
from datetime import datetime
from pathlib import Path
from uuid import UUID

import review_coverage as coverage
from workflow import WorkflowError

KNOWN_TYPES = (
    coverage.IGNORED_EVENTS
    | coverage.ROOT_EVENTS
    | {
        "tool.execution_start",
        "tool.execution_complete",
        "assistant.message",
        "session.idle",
        "session.shutdown",
        "result",
        "session.error",
    }
)
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


MAX_UNKNOWN_TYPES = 64


def unknown_types(events):
    """At most MAX_UNKNOWN_TYPES digests plus one fixed overflow counter."""
    result = {}
    for event in events:
        if event["type"] not in KNOWN_TYPES:
            key = coverage.checksum(event["type"])
            if key not in result and len(result) >= MAX_UNKNOWN_TYPES:
                key = "overflow"
            result[key] = result.get(key, 0) + 1
    return result


# Pinned SDK WarningData has an open string category, not a benign-category enum.
# See COVERAGE.md for source provenance. These labels are diagnostic examples only.
WARNING_CATEGORIES = {"subscription", "policy", "mcp", "other", "missing_or_invalid"}
WARNING_COUNTS = {
    "count",
    "data_object",
    "warning_type_string",
    "message_string",
    "url_present",
    "url_string",
    "remediation_present",
    "extra_fields",
}


def warning_summary(events):
    """Fixed-size shape/category counters; never retain messages, URLs or arbitrary labels.

    This projection grants no support/credit: warnings still pass through the existing
    unsupported-event gates. Remediation contents are deliberately not inspected.
    """
    row = dict.fromkeys(sorted(WARNING_COUNTS), 0)
    row["categories"] = dict.fromkeys(sorted(WARNING_CATEGORIES), 0)
    for event in events:
        if event["type"] != "session.warning":
            continue
        row["count"] += 1
        data = event.get("data")
        category = "missing_or_invalid"
        if isinstance(data, dict):
            row["data_object"] += 1
            value = data.get("warningType")
            row["warning_type_string"] += isinstance(value, str)
            if isinstance(value, str) and value:
                category = value if value in {"subscription", "policy", "mcp"} else "other"
            row["message_string"] += isinstance(data.get("message"), str)
            row["url_present"] += "url" in data
            row["url_string"] += isinstance(data.get("url"), str)
            row["remediation_present"] += "remediation" in data
            row["extra_fields"] += bool(set(data) - {"warningType", "message", "url", "remediation"})
        row["categories"][category] += 1
    return row


def validate_warnings(value):
    if not isinstance(value, dict) or set(value) != {"stdout", "session"}:
        raise WorkflowError("Invalid warning summary")
    for row in value.values():
        if not isinstance(row, dict) or set(row) != WARNING_COUNTS | {"categories"}:
            raise WorkflowError("Unsafe warning summary")
        counts = {key: row[key] for key in WARNING_COUNTS}
        categories = row["categories"]
        if (
            any(type(n) is not int or not 0 <= n <= coverage.MAX_EVENTS for n in counts.values())
            or any(n > row["count"] for n in counts.values())
            or not isinstance(categories, dict)
            or set(categories) != WARNING_CATEGORIES
            or any(type(n) is not int or not 0 <= n <= row["count"] for n in categories.values())
            or sum(categories.values()) != row["count"]
            or row["url_string"] > row["url_present"]
        ):
            raise WorkflowError("Invalid warning counts")


MANAGED_COUNTS = {
    "count",
    "root_ephemeral",
    "data_object",
    "known_source",
    "source_none",
    "indeterminate",
    "settings_present",
    "managed_keys_present",
}


def managed_settings_summary(events):
    """Diagnostic-only projection of the pinned experimental policy snapshot.

    No payload establishes benign policy here. Unsupported-event/isolation gates
    remain unchanged, including for enforcement and indeterminate resolution.
    """
    row = dict.fromkeys(sorted(MANAGED_COUNTS), 0)
    for event in events:
        if event["type"] != "session.managed_settings_resolved":
            continue
        row["count"] += 1
        row["root_ephemeral"] += event.get("ephemeral") is True and "agentId" not in event
        data = event.get("data")
        if not isinstance(data, dict):
            continue
        row["data_object"] += 1
        source = data.get("source")
        row["known_source"] += isinstance(source, str) and source in {
            "none",
            "server",
            "device",
            "client",
            "policyHelper",
            "mixed",
        }
        row["source_none"] += source == "none"
        row["indeterminate"] += (
            data.get("failClosed") is True or data.get("sandboxEnabledByUndeterminedPolicy") is True
        )
        row["settings_present"] += "settings" in data
        row["managed_keys_present"] += "managedKeys" in data
    return row


def validate_managed_settings(value):
    if not isinstance(value, dict) or set(value) != {"stdout", "session"}:
        raise WorkflowError("Invalid managed settings summary")
    for row in value.values():
        if (
            not isinstance(row, dict)
            or set(row) != MANAGED_COUNTS
            or any(type(n) is not int or not 0 <= n <= coverage.MAX_EVENTS for n in row.values())
            or any(n > row["count"] for n in row.values())
        ):
            raise WorkflowError("Unsafe managed settings summary")


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


def unmanaged_stdout_event(event):
    """Pinned SDK live-only no-policy snapshot; never tool or inspection evidence."""
    if set(event) != {"type", "ephemeral", "id", "parentId", "timestamp", "data"}:
        return False
    if event["type"] != "session.managed_settings_resolved" or event["ephemeral"] is not True:
        return False
    try:
        for key in ("id", "parentId"):
            value = event[key]
            if key == "parentId" and value is None:
                continue
            if not isinstance(value, str) or UUID(value).version != 4:
                return False
        if not isinstance(event["id"], str) or not isinstance(event["timestamp"], str):
            return False
        if datetime.fromisoformat(event["timestamp"]).tzinfo is None:
            return False
    except ValueError:
        return False
    data = event["data"]
    required = {
        "source",
        "failClosed",
        "managedKeys",
        "deviceManaged",
        "serverManaged",
        "bypassPermissionsDisabled",
    }
    optional = {
        "clientManaged",
        "policyHelperManaged",
        "permissionsAllowIntersected",
        "sandboxEnabledByUndeterminedPolicy",
    }
    return (
        isinstance(data, dict)
        and required <= data.keys() <= required | optional
        and data["source"] == "none"
        and data["managedKeys"] == []
        and isinstance(data["bypassPermissionsDisabled"], bool)
        and all(data[key] is False for key in {"failClosed", "deviceManaged", "serverManaged"})
        and all(data[key] is False for key in optional & data.keys())
    )


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
        kind, data = event["type"], event.get("data", {})
        restriction = coverage.event_restriction(event)
        if restriction:
            reasons.add(restriction)
        if (kind not in KNOWN_TYPES or kind == "session.error") and not unmanaged_stdout_event(event):
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
        "unknown_types": {"stdout": unknown_types(observed), "session": unknown_types(events)},
    }
    warnings = {"stdout": warning_summary(observed), "session": warning_summary(events)}
    # Optional additive diagnostics keep historical records and warning-free captures unchanged.
    if any(row["count"] for row in warnings.values()):
        diagnostics["telemetry"]["warnings"] = warnings
    managed = {"stdout": managed_settings_summary(observed), "session": managed_settings_summary(events)}
    if any(row["count"] for row in managed.values()):
        diagnostics["telemetry"]["managed_settings"] = managed
    return report, diagnostics


def validate_summary(value):
    if (
        not isinstance(value, dict)
        or set(value) - {"warnings", "managed_settings"}
        != {"source", "stdout_shapes", "session_shapes", "unknown_types"}
        or value["source"] not in {"stdout", "session-state", "unavailable"}
    ):
        raise WorkflowError("Invalid telemetry summary")
    if "managed_settings" in value:
        validate_managed_settings(value["managed_settings"])
    if "warnings" in value:
        validate_warnings(value["warnings"])
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

    unknown = value["unknown_types"]
    if not isinstance(unknown, dict) or set(unknown) != {"stdout", "session"}:
        raise WorkflowError("Invalid unknown event summary")
    for names in unknown.values():
        if (
            not isinstance(names, dict)
            or len(names) > MAX_UNKNOWN_TYPES + 1
            or any(
                not isinstance(key, str) or not re.fullmatch(r"[a-f0-9]{64}|overflow", key) for key in names
            )
            or any(type(n) is not int or not 1 <= n <= coverage.MAX_EVENTS for n in names.values())
            or sum(names.values()) > coverage.MAX_EVENTS
        ):
            raise WorkflowError("Unsafe unknown event summary")
