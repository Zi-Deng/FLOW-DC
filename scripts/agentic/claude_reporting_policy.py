"""Explicit prospective v7 bindings. Validation does not grant activation."""

from __future__ import annotations

import copy
import hashlib
from pathlib import Path

import claude_reporting
from workflow import WorkflowError

ADAPTER = "claude-stream-json-2.1.282-v7"
REPORT_SCHEMA_SHA256 = "0c2b71cc9d004931a6fed6ff2234f4b8f61251cd923592ecfd155aa2f0a25cbf"
SCHEMA_SHA256 = "67c4666ee2d1eb2dc5b74c71512e5bf8538845b116930f570847546239058af4"
TOOLS = ["Read", "Grep", "Glob", "StructuredOutput"]


def binding(max_turns, limits, schema_text):
    if type(max_turns) is not int or not 0 < max_turns <= 4000:
        raise WorkflowError("Reporting requires an explicit positive turn limit at most 4000")
    try:
        claude_reporting._configuration("claude-opus-5-5", "0" * 64, limits)
        if hashlib.sha256(schema_text.encode("utf-8")).hexdigest() != SCHEMA_SHA256:
            raise ValueError
    except (ValueError, TypeError, AttributeError, UnicodeError):
        raise WorkflowError("Unsupported reporting schema or retention limits") from None
    return {
        "format": claude_reporting.FORMAT,
        "capture_bytes": 8000000,
        "stream_bytes": 16000000,
        "stream_events": 20000,
        "diagnostic_bytes": 2000000,
        "schema_text": schema_text,
        "schema_sha256": SCHEMA_SHA256,
        "report_schema_sha256": REPORT_SCHEMA_SHA256,
        "tool": "StructuredOutput",
        "tools": TOOLS.copy(),
        "output_format": "stream-json",
        "include_partial_messages": True,
        "retry_environment": "MAX_STRUCTURED_OUTPUT_RETRIES",
        "retry_limit": 1,
        "max_turns": max_turns,
        "limits": copy.deepcopy(limits),
    }


def build(base, *, max_turns, limits):
    import review_policy

    review_policy.validate_policy(base)
    if (
        base["schema_version"] != 1
        or base["provider"] != "claude-code"
        or base["adapter"] != "claude-stream-json-2.1.282-v6"
        or base["model"] != "claude-opus-5-5"
        or base["effort"] != "medium"
        or "authentication" not in base
        or "model_compatibility" in base
    ):
        raise WorkflowError("Structured reporting requires the approved pinned native policy")
    schema_text = (
        (Path(__file__).resolve().parents[2] / ".agentic/schemas/review-report.json")
        .read_bytes()
        .decode("utf-8")
    )
    if hashlib.sha256(schema_text.encode()).hexdigest() != REPORT_SCHEMA_SHA256:
        raise WorkflowError("Repository report schema changed")
    # The pinned reporting tool instantiates Ajv draft-07. Every keyword in this
    # report contract is supported by draft-07; only the dialect declaration
    # differs. Bind both texts independently; never edit the packet's schema or
    # use object serialization to replace model report bytes.
    schema_text = schema_text.replace(
        "https://json-schema.org/draft/2020-12/schema", "http://json-schema.org/draft-07/schema#"
    )
    return {
        **copy.deepcopy(base),
        "schema_version": 2,
        "adapter": ADAPTER,
        "reporting": binding(max_turns, limits, schema_text),
    }


def validate(value):
    import review_policy

    if (
        type(value) is not dict
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 2
        or value.get("adapter") != ADAPTER
        or value.get("provider") != "claude-code"
        or value.get("model") != "claude-opus-5-5"
        or value.get("effort") != "medium"
        or "authentication" not in value
        or "model_compatibility" in value
    ):
        raise WorkflowError("Unsupported prospective reporting policy")
    report = value.get("reporting")
    if type(report) is not dict:
        raise WorkflowError("Missing reporting policy")
    expected = binding(report.get("max_turns"), report.get("limits"), report.get("schema_text"))
    # Typed canonical comparison avoids bool/int equality in immutable policy.
    try:
        if claude_reporting._json_bytes(report, 100000) != claude_reporting._json_bytes(expected, 100000):
            raise ValueError
    except (ValueError, UnicodeError, RecursionError):
        raise WorkflowError("Reporting policy changed or exceeds its bound") from None
    base = {k: v for k, v in value.items() if k != "reporting"}
    base.update(schema_version=1, adapter="claude-stream-json-2.1.282-v6")
    review_policy.validate_policy(base)
    return value


def validate_controls(data, policy):
    """Pinned source identity, not live admission or successful reporting."""
    validate(policy)
    if hashlib.sha256(data).hexdigest() != policy["cli"]["binary_sha256"]:
        raise WorkflowError("Reporting executable differs from the approved identity")
    ranges = (
        (210761700, 210766100, "e41c12c7d6b2e2b51532f2f620bb6035c38779aa3bb85e4bbe451a6b9bd52e79"),
        (194338900, 194339850, "3f03bc29f95de2d35abd6c0220a77d0cc2550e31ec5753d7f8ed734eed02ffe4"),
        (219228850, 219230050, "da64f1d9b76442ea26f56ceb80dfbe8d8f2540118c4d62aacf1c58c51dbc6d87"),
        (210183400, 210185000, "aa4f322a943f5bdc2614872b19324a9f3213bfe0c5a8d74596cbe422b1190847"),
        (209787920, 209788190, "6d96724089d1cf7b618a02b0ad517a341d63973f4b0612fb405d8b1d858fb933"),
    )
    if any(hashlib.sha256(data[start:end]).hexdigest() != expected for start, end, expected in ranges):
        raise WorkflowError("Pinned reporting controls differ from their source audit")
