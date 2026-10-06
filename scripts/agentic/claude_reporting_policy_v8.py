"""Exact v8 acceptance identity; unchanged v7 reporting controls remain frozen."""

import copy

import claude_reporting_policy as legacy
from workflow import WorkflowError

ADAPTER = "claude-stream-json-2.1.282-v8"
TOOLS = legacy.TOOLS
PARTIAL_CONTRACT = {
    "schema_version": 1,
    "adapter": ADAPTER,
    "block": "thinking",
    "delta": "thinking_delta",
    "extra_field": "estimated_tokens",
    "accepted_classes": ["null", "integer_safe"],
    "minimum": 0,
    "maximum": 9007199254740991,
}


def historical_controls(value):
    """Private validation projection only; never dispatch or relabel evidence."""
    if type(value) is not dict or value.get("adapter") != ADAPTER:
        raise WorkflowError("Unsupported v8 reporting policy")
    projected = copy.deepcopy(value)
    projected["adapter"] = legacy.ADAPTER
    legacy.validate(projected)
    return projected


def validate(value):
    historical_controls(value)
    return value


def build(base, *, max_turns, limits):
    value = legacy.build(base, max_turns=max_turns, limits=limits)
    value["adapter"] = ADAPTER
    return validate(value)


def validate_controls(data, policy):
    legacy.validate_controls(data, historical_controls(policy))
