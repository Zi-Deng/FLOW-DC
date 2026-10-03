"""Closed v6 diagnostic canary input. Old packets keep their absent-contract meaning."""

import copy
import json

from workflow import WorkflowError

ADAPTER = "claude-stream-json-2.1.282-v6"
GREP = {
    "pattern": "CLAUDE_NATIVE_CANARY",
    "path": ".",
    "glob": "capability/fixture.txt",
    "output_mode": "content",
    "-n": True,
    "head_limit": 10,
}


def exact(value, expected):
    # JSON's boolean and number types are distinct (unlike Python equality).
    return type(value) is type(expected) and (
        set(value) == set(expected) and all(exact(value[k], expected[k]) for k in expected)
        if isinstance(expected, dict)
        else value == expected
    )


def contract():
    return {"schema_version": 1, "grep_canary": copy.deepcopy(GREP)}


def validate(value):
    if not exact(value, contract()):
        raise WorkflowError("Invalid diagnostic tool contract")


def validate_meta(meta):
    if meta["review_policy"]["adapter"] == ADAPTER:
        validate(meta.get("diagnostic_tool_contract"))
    elif "diagnostic_tool_contract" in meta:
        raise WorkflowError("Historical diagnostic cannot acquire a new tool contract")


def instruction():
    return "Use exactly one Grep with this exact input: " + json.dumps(GREP, separators=(",", ":")) + ". "
