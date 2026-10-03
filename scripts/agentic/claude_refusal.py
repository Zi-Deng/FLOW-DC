"""Pinned restricted Read correlation. Only a bound diagnostic canary may qualify.

Raw provider values live only in this transient parser state; callers persist fixed
reasons and bounded field shapes/hashes. A denied canary earns no source coverage.
"""

import re
from pathlib import Path

REASON = "--restricted: path outside the working directory"
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
            and not any(k in event for k in ("agent_id", "agentId"))
            and event.get("parent_tool_use_id") is None
        )

    def call(self, event, block, initialized, terminal):
        args = block.get("input")
        if not self.enabled or not isinstance(args, dict) or args.get("file_path") != self.path:
            return
        if self.stage != 0 or not initialized or terminal:
            self.fail("controlled_refusal_call_order")
        if (
            not self.identity(event)
            or block.get("name") != "Read"
            or set(block) != {"type", "id", "name", "input"}
            or not bounded(block.get("id"), 256)
            or args != {"file_path": self.path}
        ):
            self.fail("controlled_refusal_call_mismatch")
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
            or event.get("tool_use_id") != self.identifier
            or not bounded(event.get("tool_use_id"), 256)
            or not isinstance(event.get("uuid"), str)
            or re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", event["uuid"]) is None
        ):
            self.fail("controlled_refusal_advisory_identity")
        if event.get("decision_reason_type") != "other" or event.get("decision_reason") != REASON:
            self.fail("controlled_refusal_category")
        if event.get("message") != self.message:
            self.fail("controlled_refusal_message_mismatch")
        self.stage = 2

    def result(self, event, block, initialized, terminal):
        if not self.enabled or self.identifier is None or block.get("tool_use_id") != self.identifier:
            return False
        if self.stage != 2 or not initialized or terminal:
            self.fail("controlled_refusal_result_order")
        if (
            not self.identity(event)
            or set(block) != {"type", "tool_use_id", "is_error", "content"}
            or block.get("is_error") is not True
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
        if (
            not self.identity(event)
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
