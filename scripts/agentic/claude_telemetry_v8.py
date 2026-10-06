"""Prospective v8 native evaluation: full controls plus exact reporting proof.

No activation is implied. Reporting events earn no inspection credit. Only the
bounded reporting projection survives; all other native payloads are discarded.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path

import claude_partial_observation_v2 as partial_observation
import claude_refusal_v6 as claude_refusal
import claude_reporting
import claude_reporting_policy_v8 as reporting_policy
import claude_telemetry_v6 as frozen
import claude_telemetry_v7 as original
import diagnostic_tool_contract as tool_contract
import review_coverage as coverage
from claude_telemetry_v6 import (
    BUILTIN_AGENTS,
    TOOLS,
    count_hash,
    empty_catalog,
    numerical,
    observation,
    observe_control,
    request_start,
    thinking_tokens,
    usage_projection,
)
from tasks import digest
from workflow import WorkflowError

ADAPTER = reporting_policy.ADAPTER
KINDS = frozen.KINDS | {"stream_event"}


def reporting_summary(proof):
    return {
        "proof_sha256": digest(proof),
        "report_sha256": coverage.checksum(proof["report"]),
        "terminal_sha256": coverage.checksum(proof["terminal_text"]),
        "accepted": proof["accepted"],
        "report_complete": proof["report_complete"],
    }


def estimate_allowed(value):
    if value is None:
        return True
    if type(value) not in (int, float):
        return False
    if type(value) is float and (not math.isfinite(value) or not value.is_integer()):
        return False
    return 0 <= value <= 9007199254740991


def validate_summary(value):
    if type(value) is not dict or "partial_stream" not in value:
        raise WorkflowError("Missing v8 partial-stream observation")
    original.validate_summary({k: v for k, v in value.items() if k != "partial_stream"})
    record = value["partial_stream"]
    partial_observation.validate(record, value["types"].get("stream_event", 0))
    if record["schema_version"] != 2:
        raise WorkflowError("V8 requires observation schema 2")
    for row in record["samples"]:
        shape = row["delta_shape"]
        if (
            row["predicate"] == "delta_fields"
            and shape["block_kind"] == "thinking"
            and shape["delta_kind"] == "thinking_delta"
            and shape["payload_present"]
            and shape["estimated_tokens_present"]
            and shape["other_fields"] == 0
            and not shape["other_fields_overflow"]
            and shape["estimated_tokens_class"] in {"null", "integer_safe"}
        ):
            raise WorkflowError("V8 rejected-shape observation describes an accepted layout")


def call_predicates(event, block, session, initialized, terminal):
    result = claude_refusal.call_predicates(event, block, session, initialized, terminal)
    result["tool_name"] = block.get("name") in reporting_policy.TOOLS
    return result


class PartialStream:
    """Validate partial-message framing without treating partial reads as evidence."""

    def __init__(self, model, reasons):
        self.model, self.reasons = model, reasons
        self.active = None
        self._observation = partial_observation.Observation()
        self.messages, self.blocks = set(), {}

    def observation(self):
        return self._observation.record()

    def observe(self, event):
        # Set the fixed predicate BEFORE evaluating it: TypeError/KeyError must
        # identify the evaluation boundary without rendering the offending value.
        guard = "event_object"
        try:
            if type(event) is not dict:
                raise ValueError
            kind = event.get("type")
            if kind == "message_start":
                message = event.get("message")
                guard = "message_inactive"
                if self.active is not None:
                    raise ValueError
                guard = "message_object"
                if type(message) is not dict:
                    raise ValueError
                guard = "message_id"
                if not claude_reporting._identifier(message.get("id")):
                    raise ValueError
                guard = "message_unique"
                if message["id"] in self.messages:
                    raise ValueError
                guard = "message_model"
                if message.get("model") != self.model:
                    raise ValueError
                self.active = message["id"]
                self.messages.add(self.active)
                self.blocks = {}
                return
            guard = "message_active"
            if self.active is None:
                raise ValueError
            guard = "event_kind"
            if kind in {"content_block_start", "content_block_delta", "content_block_stop"}:
                index = event.get("index")
                guard = "block_index"
                if type(index) is not int or not 0 <= index < coverage.MAX_EVENTS:
                    raise ValueError
                if kind == "content_block_start":
                    block = event.get("content_block")
                    guard = "block_unique"
                    if index in self.blocks:
                        raise ValueError
                    guard = "block_object"
                    if type(block) is not dict:
                        raise ValueError
                    block_type = block.get("type")
                    if block_type == "tool_use":
                        guard = "tool_name"
                        if block.get("name") not in reporting_policy.TOOLS:
                            raise ValueError
                        guard = "tool_id"
                        if not claude_reporting._identifier(block.get("id")):
                            raise ValueError
                        guard = "tool_input"
                        if block.get("input") != {}:
                            raise ValueError
                    else:
                        guard = "block_kind"
                        if block_type not in {"text", "thinking", "redacted_thinking"}:
                            raise ValueError
                    self.blocks[index] = block_type
                else:
                    guard = "block_open"
                    if index not in self.blocks or self.blocks[index] is None:
                        raise ValueError
                    if kind == "content_block_stop":
                        self.blocks[index] = None
                    else:
                        delta = event.get("delta")
                        guard = "delta_object"
                        if type(delta) is not dict:
                            raise ValueError
                        allowed = {
                            "tool_use": {"input_json_delta": "partial_json"},
                            "text": {"text_delta": "text"},
                            "thinking": {"thinking_delta": "thinking", "signature_delta": "signature"},
                            "redacted_thinking": {},
                        }[self.blocks[index]]
                        guard = "delta_kind"
                        field = allowed.get(delta.get("type"))
                        if field is None:
                            raise ValueError
                        guard = "delta_fields"
                        if set(delta) != {"type", field} and not (
                            self.blocks[index] == "thinking"
                            and delta.get("type") == "thinking_delta"
                            and set(delta) == {"type", "thinking", "estimated_tokens"}
                            and estimate_allowed(delta["estimated_tokens"])
                        ):
                            raise ValueError
                        guard = "delta_text"
                        if not isinstance(delta[field], str):
                            raise ValueError
            elif kind == "message_delta":
                guard = "message_delta_object"
                if type(event.get("delta")) is not dict:
                    raise ValueError
                guard = "message_usage_object"
                if type(event.get("usage")) is not dict:
                    raise ValueError
            elif kind == "message_stop":
                guard = "message_blocks_closed"
                if any(v is not None for v in self.blocks.values()):
                    raise ValueError
                self.active = None
            else:
                raise ValueError
        except (ValueError, TypeError, KeyError) as exc:
            self.reasons.add("unsupported_partial_stream")
            self._observation.reject(
                guard, type(exc) in {TypeError, KeyError}, self.active, self.blocks, event
            )


def capture(
    raw,
    packet,
    workspace,
    policy,
    session_id,
    *,
    exit_code=0,
    failure=None,
    refusal_path=None,
    diagnostic_purpose=None,
    diagnostic_tool_contract=None,
):
    reporting_policy.validate(policy)
    if diagnostic_purpose is not None:
        if diagnostic_purpose not in {"native-tools-and-source", "isolation-refusal"}:
            raise WorkflowError("Unsupported reporting diagnostic purpose")
        tool_contract.validate(diagnostic_tool_contract)
        if (diagnostic_purpose == "isolation-refusal") != (refusal_path is not None):
            raise WorkflowError("Reporting refusal path differs from diagnostic purpose")
    elif refusal_path is not None or diagnostic_tool_contract is not None:
        raise WorkflowError("Reporting diagnostic inputs require an explicit purpose")
    files, reasons = {}, set()
    grep_calls = 0
    for path in Path(packet).rglob("*"):
        if path.is_file() and not path.is_symlink():
            try:
                files[path.relative_to(packet).as_posix()] = path.read_bytes().decode("utf-8")
            except (OSError, UnicodeError):
                reasons.add("unreadable_packet_artifact")
    if failure:
        reasons.add(failure)
    if exit_code != 0:
        reasons.add("provider_exit_failure")
    summary = {
        "control_fields": {},
        "system_subtypes": {},
        "system_payloads": {},
        "unknown_agents": {},
        "types": {},
        "unknown_types": {},
        "terminal_count": 0,
        "init_count": 0,
        "model_verified": False,
        "session_verified": True,
        "controlled_refusals": 0,
    }
    if diagnostic_purpose is not None:
        summary["diagnostic"] = {
            "purpose": diagnostic_purpose,
            "tool_contract_digest": digest(tool_contract.contract()),
        }
    pending, seen, records = {}, set(), []
    decoded = []
    stream = PartialStream(policy["model"], reasons)
    message_usage, assistant_envelopes = {}, set()
    step_usage_unknown = False
    valid_initialization = False
    refusal = claude_refusal.Correlation(
        workspace,
        refusal_path,
        diagnostic_purpose,
        session_id,
        reasons,
        lambda field, event, key: observe_control(summary, field, event, key),
    )
    count, usage = 0, {"status": "unknown", "counters": {}, "models": {}}
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if not isinstance(raw, str) or len(raw.encode("utf-8")) > coverage.MAX_STREAM_BYTES:
            raise ValueError
    except (UnicodeError, ValueError):
        raw = ""
        reasons.add("invalid_or_oversized_stream")
    for line in raw.split("\n"):
        if not line.strip():
            continue
        if count >= coverage.MAX_EVENTS:
            reasons.add("event_limit_exceeded")
            break
        count += 1
        try:
            event = coverage.strict_json(line)
            if not isinstance(event, dict) or not isinstance(event.get("type"), str):
                raise ValueError
            decoded.append(event)
            if refusal.enabled:
                stack = [event]
                while stack:
                    value = stack.pop()
                    if isinstance(value, str) and "HARMLESS_OUTSIDE_CANARY_" + session_id in value:
                        refusal.fail("restricted_workspace_canary_exposed")
                    elif isinstance(value, dict):
                        stack.extend(value.values())
                    elif isinstance(value, list):
                        stack.extend(value)
            kind = event["type"]
            if kind not in KINDS:
                key = coverage.checksum(kind)
                unknown = summary["unknown_types"]
                if key not in unknown and len(unknown) >= 64:
                    key = "overflow"
                unknown[key] = unknown.get(key, 0) + 1
                reasons.add("unsupported_event_type")
                continue
            summary["types"][kind] = summary["types"].get(kind, 0) + 1
            if summary["terminal_count"]:
                reasons.add("events_after_terminal")
            if not summary["init_count"] and kind != "system":
                reasons.add("event_before_initialization")
            if event.get("session_id") != session_id:
                summary["session_verified"] = False
                reasons.add("unexpected_session_identity")
            if event.get("parent_tool_use_id") is not None or any(
                k in event for k in claude_refusal.DELEGATION
            ):
                reasons.add("delegated_or_mcp_event")
            if kind == "stream_event":
                stream.observe(event.get("event"))
                continue
            if kind == "system":
                count_hash(summary["system_subtypes"], event.get("subtype"))
                if event.get("subtype") == "permission_denied":
                    refusal.advisory(
                        event,
                        valid_initialization and summary["init_count"] == 1,
                        summary["terminal_count"] != 0,
                    )
                    continue
                if event.get("subtype") != "init":
                    observe_control(summary, "system.commands", event, "commands")
                    observe_control(summary, "system.status", event, "status")
                    observe_control(summary, "system.keys", {"keys": sorted(event)}, "keys")
                    if event.get("subtype") == "thinking_tokens":
                        for key in ("estimated_tokens", "estimated_tokens_delta"):
                            observe_control(summary, "system." + key, event, key)
                    if (
                        not (request_start(event) or empty_catalog(event) or thinking_tokens(event))
                        or summary["init_count"] != 1
                    ):
                        reasons.add("unsupported_system_event")
                        count_hash(summary["system_payloads"], event)
                    continue
                summary["init_count"] += 1
                if event.get("model") != policy["model"]:
                    reasons.add("unexpected_model_identity")
                if event.get("tools") != reporting_policy.TOOLS and (
                    not isinstance(event.get("tools"), list)
                    or set(event["tools"]) != set(reporting_policy.TOOLS)
                    or len(event["tools"]) != 4
                ):
                    reasons.add("unexpected_tools")
                agents = event.get("agents", [])
                if isinstance(agents, list):
                    for agent in agents[: coverage.MAX_EVENTS]:
                        if not isinstance(agent, str) or agent not in BUILTIN_AGENTS:
                            count_hash(summary["unknown_agents"], agent)
                required = {"mcp_servers", "plugins", "skills", "agents", "slash_commands"}
                optional = {
                    "terminal_slash_commands",
                    "plugin_errors",
                    "plugin_warnings",
                    "mcp_server_errors",
                }
                for field in sorted(required | optional):
                    observe_control(summary, "init." + field, event, field)
                    allowed_agents = (
                        field == "agents"
                        and isinstance(agents, list)
                        and all(isinstance(agent, str) and agent in BUILTIN_AGENTS for agent in agents)
                    )
                    if (field in required and field not in event) or (
                        field in event and event[field] != [] and not allowed_agents
                    ):
                        reasons.add("unexpected_init_" + field)
                        reasons.add("customization_or_mcp_loaded")
                if (
                    event.get("permissionMode") != "dontAsk"
                    or event.get("cwd") != str(workspace)
                    or event.get("claude_code_version") != policy["cli"]["version"]
                ):
                    reasons.add("unexpected_initialization_controls")
                valid_initialization = summary["init_count"] == 1 and not reasons
            elif kind == "assistant":
                message = event.get("message")
                invalid_call = False
                if isinstance(message, dict) and isinstance(message.get("content"), list):
                    for candidate in message["content"]:
                        if not isinstance(candidate, dict):
                            observe_control(summary, "call.block", {"block": candidate}, "block")
                            reasons.add("tool_call_envelope")
                            invalid_call = True
                        if isinstance(candidate, dict) and candidate.get("type") not in {
                            "text",
                            "thinking",
                            "redacted_thinking",
                        }:
                            predicates = call_predicates(
                                event,
                                candidate,
                                session_id,
                                valid_initialization and summary["init_count"] == 1,
                                summary["terminal_count"] != 0,
                            )
                            claude_refusal.observe_call(
                                lambda field, obj, key: observe_control(summary, field, obj, key),
                                event,
                                candidate,
                                predicates,
                            )
                            for name, valid in predicates.items():
                                if not valid:
                                    reasons.add("tool_call_" + name)
                                    invalid_call = True
                if (
                    not isinstance(message, dict)
                    or message.get("model") != policy["model"]
                    or event.get("error")
                ):
                    reasons.add("unexpected_model_or_assistant_error")
                    continue
                summary["model_verified"] = True
                fingerprint = (
                    None
                    if invalid_call
                    else coverage.checksum(json.dumps(message, sort_keys=True, ensure_ascii=False))
                )
                if fingerprint in assistant_envelopes:
                    if (
                        diagnostic_purpose is not None
                        and isinstance(message.get("content"), list)
                        and any(isinstance(b, dict) and b.get("name") == "Grep" for b in message["content"])
                    ):
                        reasons.add("diagnostic_grep_duplicate_call")
                    if refusal.enabled and any(
                        isinstance(b, dict) and b.get("id") == refusal.identifier
                        for b in message.get("content", [])
                        if isinstance(message.get("content"), list)
                    ):
                        refusal.fail("controlled_refusal_duplicate_call")
                    continue
                if fingerprint is not None:
                    assistant_envelopes.add(fingerprint)
                identifier, step = message.get("id"), message.get("usage")
                input_fields = ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens")
                if (
                    not isinstance(identifier, str)
                    or not identifier
                    or not isinstance(step, dict)
                    or any(not numerical(step.get(key)) for key in input_fields)
                ):
                    step_usage_unknown = True
                else:
                    values = {key: step[key] for key in input_fields}
                    if identifier in message_usage and message_usage[identifier] != values:
                        reasons.add("inconsistent_assistant_usage")
                    message_usage[identifier] = values
                blocks = message.get("content")
                if not isinstance(blocks, list):
                    raise ValueError
                for block in blocks:
                    if not isinstance(block, dict):
                        raise ValueError
                    if block.get("type") in {"text", "thinking", "redacted_thinking"}:
                        continue  # Never concatenate assistant text or retain reasoning.
                    if block.get("type") != "tool_use":
                        reasons.add("unsupported_assistant_block")
                        continue
                    refusal.call(
                        event,
                        block,
                        valid_initialization and summary["init_count"] == 1,
                        summary["terminal_count"] != 0,
                    )
                    predicates = call_predicates(
                        event,
                        block,
                        session_id,
                        valid_initialization and summary["init_count"] == 1,
                        summary["terminal_count"] != 0,
                    )
                    for name, valid in predicates.items():
                        if not valid:
                            reasons.add("tool_call_" + name)
                    identifier, tool, args = block.get("id"), block.get("name"), block.get("input")
                    if tool == "Grep":
                        observe_control(summary, "grep.input_shape", block, "input")
                        grep_args = args if isinstance(args, dict) else {}
                        checks = {
                            "mode": grep_args.get("output_mode") == "content",
                            "line_numbers": grep_args.get("-n") is True,
                            "path": grep_args.get("path") == ".",
                            "glob": grep_args.get("glob") == "capability/fixture.txt",
                            "head_limit": type(grep_args.get("head_limit")) is int
                            and grep_args["head_limit"] == 10,
                            "input": tool_contract.exact(args, tool_contract.GREP),
                        }
                        for key, value in checks.items():
                            observe_control(summary, "grep." + key, {"predicate": value}, "predicate")
                        if diagnostic_purpose is not None:
                            grep_calls += 1
                            if not checks["input"]:
                                reasons.add("diagnostic_grep_input_mismatch")
                    if not all(predicates.values()):
                        continue
                    if not isinstance(identifier, str) or identifier in seen or not isinstance(args, dict):
                        raise ValueError
                    seen.add(identifier)
                    if tool not in reporting_policy.TOOLS:
                        reasons.add("forbidden_tool")
                        continue
                    if len(pending) + len(records) >= coverage.MAX_TOOL_RECORDS:
                        reasons.add("tool_record_limit_exceeded")
                        continue
                    if summary["terminal_count"]:
                        reasons.add("events_after_terminal")
                    pending[identifier] = (tool, args)
            elif kind == "user":
                message = event.get("message")
                if not isinstance(message, dict) or not isinstance(message.get("content"), list):
                    raise ValueError
                for block in message["content"]:
                    if not isinstance(block, dict) or block.get("type") != "tool_result":
                        raise ValueError
                    identifier = block.get("tool_use_id")
                    if not isinstance(identifier, str) or identifier not in pending:
                        raise ValueError
                    tool, args = pending.pop(identifier)
                    content = block.get("content")
                    if tool == "StructuredOutput":
                        if block.get("is_error", False) is not False or content != claude_reporting.TOOL_ACK:
                            reasons.add("reporting_tool_failed")
                        continue  # Pure reporting produces no inspection record or probe.
                    if refusal.result(
                        event,
                        block,
                        valid_initialization and summary["init_count"] == 1,
                        summary["terminal_count"] != 0,
                    ):
                        continue  # Provisional: counted only after the terminal binding.
                    success = block.get("is_error", False) is False and isinstance(content, str)
                    spans, paths, reason = [], [], "tool_failed_or_unsupported_content"
                    if success:
                        spans, paths, reason = observation(tool, args, content, workspace, files)
                    if tool == "Grep":
                        rendering = {
                            "path_line_text": isinstance(content, str)
                            and re.search(r"(?m)^[^\n:]+:[1-9][0-9]*:", content) is not None,
                            "line_text": isinstance(content, str)
                            and re.search(r"(?m)^[1-9][0-9]*:", content) is not None,
                            "empty_result": content == "",
                        }
                        for key, value in rendering.items():
                            observe_control(summary, "grep." + key, {"predicate": value}, "predicate")
                        observe_control(
                            summary, "grep.rendered_lines", {"predicate": bool(spans)}, "predicate"
                        )
                        if diagnostic_purpose is not None and success and not spans:
                            reasons.add("grep_no_qualifying_lines")
                    if not success:
                        reasons.add("tool_execution_failed")
                    if reason not in {None, "unrecognized_or_empty_tool_result"}:
                        reasons.add(reason)
                    record = {
                        "id": f"event-{len(records) + 1:06d}",
                        "tool": TOOLS[tool],
                        "success": success,
                        "reason": reason,
                        "result_sha256": coverage.checksum(content) if isinstance(content, str) else None,
                        "spans": spans,
                        "paths": paths,
                    }
                    if len(json.dumps(records + [record]).encode("utf-8")) > coverage.MAX_DIAGNOSTIC_BYTES:
                        reasons.add("diagnostic_limit_exceeded")
                    else:
                        records.append(record)
            elif kind == "result":
                summary["terminal_count"] += 1
                if event.get("subtype") != "success" or event.get("is_error") is not False:
                    reasons.add("provider_terminal_failure")
                    if event.get("subtype") == "error_max_budget_usd":
                        reasons.add("reference_cost_limit_reached")
                denials = event.get("permission_denials")
                expected_denials = refusal.terminal(event)
                if denials and not expected_denials:
                    reasons.add("permission_denied")
                turns = event.get("num_turns")
                if type(turns) is not int or not 0 < turns <= policy["reporting"]["max_turns"]:
                    reasons.add("invalid_or_exhausted_turn_limit")
                models = event.get("modelUsage")
                if not isinstance(models, dict) or set(models) != {policy["model"]}:
                    reasons.add("unexpected_usage_models")
                usage = usage_projection(event, policy)
                if any(
                    not numerical(event.get(key)) for key in ("total_cost_usd", "duration_ms", "num_turns")
                ):
                    reasons.add("unsupported_usage")
                token_usage = event.get("usage")
                if not isinstance(token_usage, dict) or any(
                    not numerical(token_usage.get(key)) for key in ("input_tokens", "output_tokens")
                ):
                    reasons.add("unsupported_usage")
            else:
                info = event.get("rate_limit_info")
                if not isinstance(info, dict) or info.get("status") not in {
                    "allowed",
                    "allowed_warning",
                    "rejected",
                }:
                    reasons.add("unsupported_quota_telemetry")
                elif info["status"] == "rejected":
                    reasons.add("subscription_quota_exhausted")
        except (ValueError, TypeError, KeyError, IndexError):
            reasons.add("malformed_or_uncorrelated_event")
    if diagnostic_purpose is not None and grep_calls != 1:
        reasons.add("diagnostic_grep_call_count")
    if refusal.complete():
        summary["controlled_refusals"] = 1
        reasons.add("controlled_refusal_diagnostic_only")
    if pending:
        reasons.add("missing_tool_completion")
    if summary["init_count"] != 1 or summary["terminal_count"] != 1 or not summary["model_verified"]:
        reasons.add("missing_or_ambiguous_terminal_or_identity")
    if usage["status"] == "unknown":
        reasons.add("unsupported_usage")
    elif usage["counters"]["estimated_usd"] > policy["budget"]["estimated_usd"]:
        reasons.add("reference_cost_limit_exceeded")
    if message_usage and not step_usage_unknown:
        usage["counters"]["model_steps_observed"] = len(message_usage)
        for field in ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"):
            usage["counters"]["assistant_" + field + "_observed"] = sum(
                step[field] for step in message_usage.values()
            )
    # Missing step counters mean unknown. Native num_turns is not exact API-call
    # accounting; output tokens come only from terminal usage, never placeholders.
    if stream.active is not None:
        reasons.add("incomplete_partial_stream")
    proof = claude_reporting.capture(
        claude_reporting.native_projection(decoded, session_id=session_id),
        model=policy["model"],
        inventory_sha256=hashlib.sha256((Path(packet) / "required-material.json").read_bytes()).hexdigest(),
        limits=policy["reporting"]["limits"],
    )
    reasons.update("reporting_" + reason for reason in proof["reasons"])
    summary["reporting"] = reporting_summary(proof)
    summary["partial_stream"] = stream.observation()
    diagnostics = {
        "schema_version": 10,
        "adapter": policy["adapter"],
        "cli_version": policy["cli"]["version"],
        "exit_code": exit_code,
        "event_count": count,
        "reasons": sorted(reasons),
        "capability": coverage.capability(records, packet),
        "events": records,
        "usage": usage,
        "telemetry": summary,
    }
    return proof["report"], diagnostics, proof
