"""Bounded lexical report proof, independent of tool inspection and activation.

This is a prospective transport primitive, not a native stream adapter. A caller
must project actual native events, preserving their identities and order, and
independently validate the full stream, policy, tools, usage and packet. No caller
uses this module for readiness yet. A successful proof alone earns no coverage.

Proof events contain only reporting fragments, their correlation fields, completed
report objects and auxiliary terminal text. They never contain inspection inputs,
source results, thinking, credentials or a raw provider session. Parsed objects
are comparison evidence; only fragment text supplies the report bytes.
"""

from __future__ import annotations

import json
import math
import re

FORMAT = "claude-report-fragments-v1"
TOOL_ACK = "Structured output provided successfully"
MAXIMA = {
    "report_bytes": 60000,
    "fragment_bytes": 60000,
    "terminal_bytes": 60000,
    "proof_bytes": 2000000,
    "events": 20000,
}
FIELDS = {
    "message_start": {"message_id", "model"},
    "block_start": {"index", "tool_id", "input"},
    "fragment": {"index", "text"},
    "assistant": {"message_id", "tool_id", "input"},
    "block_stop": {"index"},
    "message_stop": set(),
    "tool_result": {"tool_id", "success", "content"},
    "terminal": {"success", "structured_output", "text"},
}
OMISSIONS = {"proof_limit_exceeded", "event_limit_exceeded"}


def _text(value, limit):
    if not isinstance(value, str) or len(value) > limit:
        raise ValueError("text_limit_or_type")
    if len(value.encode("utf-8", errors="strict")) > limit:
        raise ValueError("text_limit_or_type")
    return value


def _json_bytes(value, limit):
    # Count the exact UTF-8 JSON size before allocating the serialized proof,
    # including escaping. A short string of control characters expands 6x.
    remaining = limit

    def spend(count):
        nonlocal remaining
        remaining -= count
        if remaining < 0:
            raise ValueError("object_limit_exceeded")

    def visit(item, depth=0):
        if depth > 32:
            raise ValueError("object_limit_exceeded")
        if isinstance(item, str):
            spend(2 + len(_text(item, remaining).encode("utf-8")))
            for match in re.finditer(r'["\\\x00-\x1f]', item):
                spend(1 if match.group() in '"\\\b\f\n\r\t' else 5)
        elif type(item) is dict:
            spend(2)
            for index, (key, child) in enumerate(item.items()):
                if not isinstance(key, str):
                    raise ValueError("invalid_object_key")
                spend(1 + bool(index))  # Colon and optional comma.
                visit(key, depth + 1)
                visit(child, depth + 1)
        elif type(item) is list:
            spend(2)
            for index, child in enumerate(item):
                spend(bool(index))
                visit(child, depth + 1)
        elif item is None:
            spend(4)
        elif type(item) is bool:
            spend(4 if item else 5)
        elif type(item) is int:
            if item.bit_length() > 4 * remaining:
                raise ValueError("object_limit_exceeded")
            spend(len(str(item)))
        elif type(item) is float:
            if not math.isfinite(item):
                raise ValueError("nonfinite_number")
            spend(len(repr(item)))
        else:
            raise ValueError("invalid_object_value")

    visit(value)
    encoded = json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode()
    if len(encoded) != limit - remaining:
        raise ValueError("unsupported_json_size")
    return encoded


def _strict_json(text):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate_key")
            value[key] = item
        return value

    def invalid(_):
        raise ValueError("nonfinite_number")

    return json.loads(text, object_pairs_hook=unique, parse_constant=invalid)


def _identifier(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,256}", value) is not None


def _hex(value, length):
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{" + str(length) + "}", value) is not None


def _report(value, digest):
    """Schema-2 syntax plus local non-vacuity checks; no source/readiness claims."""
    if (
        type(value) is not dict
        or set(value)
        != {"schema_version", "inventory_sha256", "findings", "reviewed", "incomplete", "limitations"}
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 2
        or value["inventory_sha256"] != digest
        or any(type(value[key]) is not list for key in ("findings", "reviewed", "incomplete", "limitations"))
        or not all(isinstance(item, str) for item in value["limitations"])
    ):
        raise ValueError("invalid_report_contract")
    claimed = set()

    def claim(items):
        if any(not _hex(item, 24) for item in items):
            raise ValueError("invalid_report_id")
        if len(set(items)) != len(items) or claimed.intersection(items):
            raise ValueError("duplicate_report_id")
        claimed.update(items)

    claim(value["reviewed"])
    for item in value["incomplete"]:
        if (
            type(item) is not dict
            or set(item) != {"ids", "state", "reason"}
            or type(item["ids"]) is not list
            or not item["ids"]
            or item["state"] not in ("unread", "unsupported")
            or not isinstance(item["reason"], str)
            or not item["reason"].strip()
        ):
            raise ValueError("invalid_incomplete_group")
        claim(item["ids"])
    strings = {"id", "path", "claim", "trigger", "impact", "evidence", "fix"}
    for finding in value["findings"]:
        if (
            type(finding) is not dict
            or set(finding) != strings | {"severity", "line"}
            or any(not isinstance(finding[key], str) or not finding[key].strip() for key in strings)
            or finding["severity"] not in ("P0", "P1", "P2", "P3")
            or type(finding["line"]) is not int
            or finding["line"] < 1
        ):
            raise ValueError("invalid_finding")


def _configuration(model, digest, limits):
    if not _identifier(model) or not _hex(digest, 64):
        raise ValueError("invalid_reporting_binding")
    if type(limits) is not dict or set(limits) != set(MAXIMA):
        raise ValueError("invalid_reporting_limits")
    if any(type(limits[key]) is not int or not 0 < limits[key] <= maximum for key, maximum in MAXIMA.items()):
        raise ValueError("invalid_reporting_limits")
    if limits["proof_bytes"] < 2:  # Even an empty JSON proof array needs two bytes.
        raise ValueError("invalid_reporting_limits")


def _projection(event, limits):
    """Refuse extra payloads; caller supplies an explicit bounded projection."""
    if type(event) is not dict or not isinstance(event.get("kind"), str):
        return {"kind": "invalid_event"}
    kind = event["kind"]
    if event == {"kind": "invalid_or_over_limit_event"}:
        return dict(event)
    if kind not in FIELDS or set(event) != FIELDS[kind] | {"kind", "event_id"}:
        return {"kind": "invalid_event"}
    if not _identifier(event["event_id"]):
        return {"kind": "invalid_event"}
    try:
        for key in ("message_id", "tool_id", "model"):
            if key in event and not _identifier(event[key]):
                raise ValueError("invalid_identity")
        if "index" in event and (type(event["index"]) is not int or not 0 <= event["index"] < 20000):
            raise ValueError("invalid_index")
        if "success" in event and type(event["success"]) is not bool:
            raise ValueError("invalid_success")
        if kind == "fragment":
            _text(event["text"], limits["fragment_bytes"])
        if kind == "terminal":
            _text(event["text"], limits["terminal_bytes"])
        if kind == "tool_result":
            _text(event["content"], 256)
        raw = _json_bytes(event, limits["proof_bytes"])
        # Detach from the caller's mutable objects. This serializes proof only,
        # never supplies a substitute for model report text.
        return json.loads(raw)
    except (ValueError, UnicodeError, RecursionError):
        return {"kind": "invalid_or_over_limit_event"}


def capture(events, *, model, inventory_sha256, limits):
    """Capture bounded projected reporting events. No tools or inference run here.

    Event IDs must be actual unique transport identities, not generated counters.
    The forthcoming native adapter must establish that mapping before adoption.
    An omitted/over-limit payload stays incomplete; retained text is never labeled
    a complete report in that case. Auxiliary terminal text cannot replace it.
    """
    _configuration(model, inventory_sha256, limits)
    proof, omissions = [], []
    size = 2
    for count, event in enumerate(events):
        if count >= limits["events"]:
            omissions.append("event_limit_exceeded")
            break
        projected = _projection(event, limits)
        size += len(_json_bytes(projected, MAXIMA["proof_bytes"])) + 1
        if size > limits["proof_bytes"]:
            omissions.append("proof_limit_exceeded")
            break
        proof.append(projected)
    return _evaluate(proof, omissions, model, inventory_sha256, dict(limits))


def _evaluate(proof, omissions, model, digest, limits):
    reasons = list(omissions)
    seen = set()
    message = tool = index = None
    started = block_open = assistant = block_stopped = message_stopped = success = terminal = False
    fragments, report_size, terminal_text = [], 0, ""
    assistant_input = terminal_input = None
    report_complete = not omissions

    def refuse(reason):
        reasons.append(reason)

    for event in proof:
        kind = event["kind"]
        if kind in {"invalid_event", "invalid_or_over_limit_event"}:
            refuse(kind)
            report_complete = False
            continue
        if event["event_id"] in seen:
            refuse("duplicate_event")
        seen.add(event["event_id"])
        if terminal:
            refuse("event_after_terminal")
        if kind == "message_start":
            if started or event["model"] != model:
                refuse("unexpected_message")
            started, message = True, event["message_id"]
        elif kind == "block_start":
            if not started or tool is not None or event["input"] != {} or message_stopped:
                refuse("unexpected_reporting_call")
            tool, index, block_open = event["tool_id"], event["index"], True
        elif kind == "fragment":
            if not block_open or assistant or event["index"] != index:
                refuse("uncorrelated_fragment")
            fragment = event["text"]
            report_size += len(fragment.encode())
            if report_size > limits["report_bytes"]:
                refuse("report_limit_exceeded")
                report_complete = False
            else:
                fragments.append(fragment)
        elif kind == "assistant":
            if not block_open or assistant or event["message_id"] != message or event["tool_id"] != tool:
                refuse("uncorrelated_assistant")
            assistant, assistant_input = True, event["input"]
        elif kind == "block_stop":
            if not block_open or not assistant or event["index"] != index:
                refuse("uncorrelated_block_stop")
            block_open, block_stopped = False, True
        elif kind == "message_stop":
            if not block_stopped or block_open or message_stopped:
                refuse("uncorrelated_message_stop")
            message_stopped = True
        elif kind == "tool_result":
            if (
                not assistant
                or success
                or event["tool_id"] != tool
                or not event["success"]
                or event["content"] != TOOL_ACK
            ):
                refuse("uncorrelated_tool_success")
            success = True
        elif kind == "terminal":
            if not message_stopped or not success or terminal or not event["success"]:
                refuse("uncorrelated_terminal")
            terminal, terminal_input, terminal_text = True, event["structured_output"], event["text"]
    report = "".join(fragments)
    if not (
        started
        and tool is not None
        and assistant
        and block_stopped
        and message_stopped
        and success
        and terminal
    ):
        refuse("missing_reporting_evidence")
        report_complete = False
    try:
        value = _strict_json(report)
        _report(value, digest)
        # Canonical comparison is internal only. It also avoids True == 1.
        encoded = _json_bytes(value, limits["proof_bytes"])
        if encoded != _json_bytes(assistant_input, limits["proof_bytes"]) or encoded != _json_bytes(
            terminal_input, limits["proof_bytes"]
        ):
            refuse("report_object_disagreement")
    except (ValueError, TypeError, RecursionError, UnicodeError):
        refuse("invalid_report_contract")
    return {
        "format": FORMAT,
        "model": model,
        "inventory_sha256": digest,
        "limits": limits,
        "proof": proof,
        "omissions": omissions,
        "report": report,
        "report_complete": report_complete,
        "terminal_text": terminal_text,
        "accepted": not reasons,
        "reasons": sorted(set(reasons)),
    }


def replay(record):
    """Storage-only recomputation; integrity is bound externally by capture hashes."""
    fields = {
        "format",
        "model",
        "inventory_sha256",
        "limits",
        "proof",
        "omissions",
        "report",
        "report_complete",
        "terminal_text",
        "accepted",
        "reasons",
    }
    if type(record) is not dict or set(record) != fields or record["format"] != FORMAT:
        raise ValueError("invalid_reporting_record")
    _configuration(record["model"], record["inventory_sha256"], record["limits"])
    proof, omissions, limits = record["proof"], record["omissions"], record["limits"]
    if (
        type(proof) is not list
        or len(proof) > limits["events"]
        or type(omissions) is not list
        or any(not isinstance(item, str) or item not in OMISSIONS for item in omissions)
    ):
        raise ValueError("invalid_reporting_proof")
    _json_bytes(proof, limits["proof_bytes"])
    for event in proof:
        if event in ({"kind": "invalid_event"}, {"kind": "invalid_or_over_limit_event"}):
            continue
        if _projection(event, limits) != event:
            raise ValueError("invalid_reporting_projection")
    expected = _evaluate(proof, omissions, record["model"], record["inventory_sha256"], limits)
    if _json_bytes(record, MAXIMA["proof_bytes"] + 200000) != _json_bytes(
        expected, MAXIMA["proof_bytes"] + 200000
    ):
        raise ValueError("changed_reporting_record")
    return expected


def native_projection(events, *, session_id):
    """Project decoded, bounded native envelopes without retaining other content.

    The native adapter must still validate *every* original event (including
    non-reporting tools, errors, delegated traffic, init controls and usage).
    This function establishes only the reporting subset's origin/correlation.
    Pinned engine envelopes supply UUIDs; missing identities are unsupported.
    """
    start = None
    reporting_index = None
    reporting_tool = None
    reporting_message = None
    active = False
    seen = set()
    for count, envelope in enumerate(events):
        if count >= MAXIMA["events"]:
            yield {"kind": "invalid_or_over_limit_event"}
            break
        if type(envelope) is not dict:
            yield {"kind": "invalid_event"}
            continue
        identity = envelope.get("uuid")
        if (
            envelope.get("session_id") != session_id
            or envelope.get("parent_tool_use_id") is not None
            or "agent_id" in envelope
            or not _identifier(identity)
            or identity in seen
        ):
            yield {"kind": "invalid_event"}
            continue

        seen.add(identity)

        def row(kind, event_id=identity, **fields):
            return {"kind": kind, "event_id": event_id, **fields}

        kind = envelope.get("type")
        if kind == "stream_event":
            event = envelope.get("event")
            if type(event) is not dict:
                yield {"kind": "invalid_event"}
                continue
            subkind = event.get("type")
            if subkind == "message_start":
                if start is not None:
                    yield {"kind": "invalid_event"}
                message = event.get("message")
                if type(message) is not dict:
                    start = None
                    yield {"kind": "invalid_event"}
                else:
                    start = row("message_start", message_id=message.get("id"), model=message.get("model"))
            elif subkind == "content_block_start":
                block = event.get("content_block")
                if type(block) is dict and block.get("name") == "StructuredOutput":
                    if start is None or block.get("type") != "tool_use" or reporting_tool is not None:
                        yield {"kind": "invalid_event"}
                    if start is not None:
                        yield start
                        reporting_message = start["message_id"]
                    active = True
                    reporting_index, reporting_tool = event.get("index"), block.get("id")
                    yield row(
                        "block_start", index=reporting_index, tool_id=reporting_tool, input=block.get("input")
                    )
            elif subkind == "content_block_delta" and active:
                if event.get("index") == reporting_index:
                    delta = event.get("delta")
                    if (
                        type(delta) is not dict
                        or set(delta) != {"type", "partial_json"}
                        or delta["type"] != "input_json_delta"
                    ):
                        yield {"kind": "invalid_event"}
                    else:
                        yield row("fragment", index=reporting_index, text=delta["partial_json"])
            elif subkind == "content_block_stop" and active:
                if event.get("index") == reporting_index:
                    yield row("block_stop", index=reporting_index)
            elif subkind == "message_stop":
                if active:
                    yield row("message_stop")
                    active = False
                start = None
        elif kind in {"assistant", "user"}:
            message = envelope.get("message")
            if type(message) is not dict or type(message.get("content")) is not list:
                continue  # The full native adapter, not this projection, validates other shapes.
            for block in message["content"]:
                if type(block) is not dict:
                    continue
                if kind == "assistant" and block.get("name") == "StructuredOutput":
                    if (
                        block.get("type") != "tool_use"
                        or message.get("id") != reporting_message
                        or not start
                        or message.get("model") != start["model"]
                        or envelope.get("error") is not None
                    ):
                        yield {"kind": "invalid_event"}
                    yield row(
                        "assistant",
                        message_id=message.get("id"),
                        tool_id=block.get("id"),
                        input=block.get("input"),
                    )
                elif (
                    kind == "user"
                    and block.get("tool_use_id") == reporting_tool
                    and reporting_tool is not None
                ):
                    if block.get("type") != "tool_result" or type(block.get("is_error", False)) is not bool:
                        yield {"kind": "invalid_event"}
                    yield row(
                        "tool_result",
                        tool_id=reporting_tool,
                        success=block.get("is_error", False) is False,
                        content=block.get("content"),
                    )
        elif kind == "result":
            yield row(
                "terminal",
                success=envelope.get("subtype") == "success" and envelope.get("is_error") is False,
                structured_output=envelope.get("structured_output"),
                text=envelope.get("result"),
            )
        elif kind == "tombstone":
            # The pinned public stream normally filters these; never credit an
            # unexpected explicit retraction, even when objects happen to agree.
            yield {"kind": "invalid_event"}
