"""Pinned restricted Read correlation. Only a bound diagnostic canary may qualify.

Raw provider values live only in this transient parser state; callers persist fixed
reasons and bounded field shapes/hashes. A denied canary earns no source coverage.
"""

import re
from pathlib import Path

REASON = "--restricted: path outside the working directory"
DELEGATION = {
    "agent_id",
    "agentId",
    "subagent_type",
    "task_description",
    "attributionAgent",
    "attributionSkill",
    "attributionPlugin",
    "attributionMcpServer",
    "attributionMcpTool",
}
FIELDS = {
    "type",
    "subtype",
    "session_id",
    "uuid",
    "tool_name",
    "tool_use_id",
    "decision_reason_type",
    "decision_reason",
    "message",
}
OBSERVATIONS = FIELDS | {"agent_id", "decision_reason_code", "tool_input", "permission_denials", "keys"}


def bounded(value, limit):
    try:
        return isinstance(value, str) and 0 < len(value.encode("utf-8")) <= limit
    except UnicodeError:
        return False


def direct_path(value):
    return (
        bounded(value, 4096)
        and Path(value).is_absolute()
        and str(Path(value)) == value
        and ".." not in Path(value).parts
    )


class Correlation:
    def __init__(self, workspace, path, purpose, session, reasons, observe):
        self.reasons, self.observe, self.session = reasons, observe, session
        self.path = str(path) if path is not None else None
        self.enabled = purpose == "isolation-refusal" and self.path is not None
        self.stage, self.identifier, self.invalid = 0, None, False
        self.message = None
        if self.enabled:
            workspace = str(workspace)
            if (
                not direct_path(workspace)
                or not direct_path(self.path)
                or Path(self.path).is_relative_to(workspace)
            ):
                self.fail("controlled_refusal_path_mismatch")
            self.message = f"{self.path} is outside {workspace}; --restricted confines the file tools to the working directory."
            if not bounded(self.message, 16384):
                self.fail("controlled_refusal_message_mismatch")

    def fail(self, reason):
        self.invalid = True
        self.reasons.add(reason)

    def identity(self, event):
        return (
            event.get("session_id") == self.session
            and not any(k in event for k in DELEGATION)
            and event.get("parent_tool_use_id") is None
        )

    def call(self, event, block, initialized, terminal):
        predicates = call_predicates(event, block, self.session, initialized, terminal)
        observe_call(self.observe, event, block, predicates)
        args = block.get("input")
        if (
            not self.enabled
            or not isinstance(args, dict)
            or not bounded(args.get("file_path"), 4096)
            or args.get("file_path") != self.path
        ):
            return
        predicates.update(
            tool_name=block.get("name") == "Read",
            input_keys=set(args) == {"file_path"},
            input_path=direct_path(args.get("file_path")) and args["file_path"] == self.path,
        )
        for name, valid in predicates.items():
            self.observe("call." + name, {"predicate": valid}, "predicate")
            if not valid:
                self.fail("controlled_refusal_call_" + name)
        if self.stage != 0:
            self.fail("controlled_refusal_call_order")
        self.identifier, self.stage = block.get("id"), 1

    def advisory(self, event, initialized, terminal):
        for key in sorted(OBSERVATIONS - {"keys", "tool_input", "permission_denials"}):
            self.observe("refusal." + key, event, key)
        self.observe("refusal.keys", {"keys": sorted(event)}, "keys")
        if not self.enabled:
            self.reasons.add("permission_denied")
            return
        if self.stage != 1 or not initialized or terminal:
            self.fail("controlled_refusal_advisory_order")
        if set(event) != FIELDS:
            self.fail("controlled_refusal_advisory_envelope")
        if (
            not self.identity(event)
            or event.get("type") != "system"
            or event.get("tool_name") != "Read"
            or not bounded(event.get("tool_use_id"), 256)
            or event.get("tool_use_id") != self.identifier
            or not isinstance(event.get("uuid"), str)
            or re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", event["uuid"]) is None
        ):
            self.fail("controlled_refusal_advisory_identity")
        if event.get("decision_reason_type") != "other" or event.get("decision_reason") != REASON:
            self.fail("controlled_refusal_category")
        if not bounded(event.get("message"), 16384) or event.get("message") != self.message:
            self.fail("controlled_refusal_message_mismatch")
        self.stage = 2

    def result(self, event, block, initialized, terminal):
        if (
            not self.enabled
            or self.identifier is None
            or not bounded(block.get("tool_use_id"), 256)
            or block.get("tool_use_id") != self.identifier
        ):
            return False
        if self.stage != 2 or not initialized or terminal:
            self.fail("controlled_refusal_result_order")
        if (
            not self.identity(event)
            or set(block) != {"type", "tool_use_id", "is_error", "content"}
            or block.get("is_error") is not True
            or not bounded(block.get("content"), 16384)
            or block.get("content") != self.message
        ):
            self.fail("controlled_refusal_result_mismatch")
        self.stage = 3
        return not self.invalid

    def terminal(self, event):
        denials = event.get("permission_denials")
        self.observe("refusal.permission_denials", event, "permission_denials")
        expected = [
            {"tool_name": "Read", "tool_use_id": self.identifier, "tool_input": {"file_path": self.path}}
        ]
        if not self.enabled:
            return False
        if self.stage != 3:
            self.fail("controlled_refusal_terminal_order")
        valid_row = (
            isinstance(denials, list)
            and len(denials) == 1
            and isinstance(denials[0], dict)
            and bounded(denials[0].get("tool_use_id"), 256)
            and isinstance(denials[0].get("tool_input"), dict)
            and bounded(denials[0]["tool_input"].get("file_path"), 4096)
        )
        if (
            not self.identity(event)
            or not valid_row
            or denials != expected
            or self.identifier is None
            or event.get("subtype") != "success"
            or event.get("is_error") is not False
        ):
            self.fail("controlled_refusal_terminal_mismatch")
        self.stage = 4
        return not self.invalid

    def complete(self):
        if self.enabled and (self.stage != 4 or self.invalid):
            self.reasons.add("controlled_refusal_not_observed")
        return self.enabled and self.stage == 4 and not self.invalid


CALL_FIELDS = {
    "block_keys",
    "input",
    "caller",
    "session_id",
    "parent_tool_use_id",
    "agent_id",
    "agentId",
    "id",
    "name",
}
CALL_PREDICATES = {
    "envelope",
    "caller_category",
    "identity",
    "tool_name",
    "use_id",
    "input_keys",
    "input_path",
    "framing",
}


def call_predicates(event, block, session, initialized, terminal):
    return {
        "envelope": set(block) in ({"type", "id", "name", "input"}, {"type", "id", "name", "input", "caller"})
        and block.get("type") == "tool_use",
        "caller_category": "caller" not in block or block["caller"] == {"type": "direct"},
        "identity": event.get("session_id") == session
        and not any(
            k in event or k in (event.get("message") if isinstance(event.get("message"), dict) else {})
            for k in DELEGATION
        )
        and event.get("parent_tool_use_id") is None,
        "tool_name": block.get("name") in ("Read", "Grep", "Glob"),
        "use_id": bounded(block.get("id"), 256),
        "input_keys": isinstance(block.get("input"), dict),
        "input_path": not isinstance(block.get("input"), dict)
        or "file_path" not in block["input"]
        or bounded(block["input"]["file_path"], 4096),
        "framing": initialized and not terminal,
    }


def observe_call(observe, event, block, predicates):
    observe("call.block_keys", {"keys": sorted(block)}, "keys")
    for key in CALL_FIELDS - {"block_keys"}:
        observe(
            "call." + key,
            event if key in {"session_id", "parent_tool_use_id", "agent_id", "agentId"} else block,
            key,
        )
    for name, value in predicates.items():
        observe("call." + name, {"predicate": value}, "predicate")
