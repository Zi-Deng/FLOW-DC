"""Strict native stream-json adapter. UI metadata and assistant assertions earn no reads.

Fixtures test the pinned renderer contract; only an actual successful invocation can
establish live capability. Unknown shapes preserve incomplete, sanitized diagnostics.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path

import claude_telemetry_v1 as legacy
import claude_telemetry_v2 as previous
import review_coverage as coverage
from workflow import WorkflowError

TOOLS = {"Read": "view", "Grep": "grep", "Glob": "glob"}
KINDS = {"system", "assistant", "user", "result", "rate_limit_event"}
# Pinned built-in declarations are not delegation permission. Agent is absent
# from the tool list and actual delegated messages remain forbidden.
BUILTIN_AGENTS = {*legacy.BUILTIN_AGENTS, "claude-code-guide"}
ADAPTER = "claude-stream-json-2.1.282-v3"
LEGACY_ADAPTER = "claude-stream-json-2.1.282-v1"


CONTROL_FIELDS = {
    "init." + name
    for name in (
        "mcp_servers",
        "plugins",
        "skills",
        "agents",
        "slash_commands",
        "terminal_slash_commands",
        "plugin_errors",
        "plugin_warnings",
        "mcp_server_errors",
    )
} | {"system.commands", "system.status", "system.keys"}
FIELD_TYPES = {"missing", "null", "boolean", "number", "string", "array", "object"}


def validate_summary(value):
    if not isinstance(value, dict) or "control_fields" not in value:
        raise WorkflowError("Invalid Claude v3 control summary")
    previous.validate_summary({k: v for k, v in value.items() if k != "control_fields"})
    fields = value["control_fields"]
    if not isinstance(fields, dict) or set(fields) - CONTROL_FIELDS:
        raise WorkflowError("Unsafe control field names")
    for field in fields.values():
        if (
            not isinstance(field, dict)
            or set(field) != {"observations", "overflow"}
            or type(field["overflow"]) is not int
            or not 0 <= field["overflow"] <= coverage.MAX_EVENTS
            or not isinstance(field["observations"], list)
            or len(field["observations"]) > 8
        ):
            raise WorkflowError("Unsafe control field observations")
        for item in field["observations"]:
            if (
                not isinstance(item, dict)
                or set(item) != {"present", "type", "length", "value_hashes", "name_hashes", "count"}
                or type(item["present"]) is not bool
                or not isinstance(item["type"], str)
                or item["type"] not in FIELD_TYPES
                or item["present"] != (item["type"] != "missing")
                or (
                    item["length"] is not None
                    and (
                        type(item["length"]) is not int
                        or not 0 <= item["length"] <= coverage.MAX_STREAM_BYTES
                    )
                )
                or type(item["count"]) is not int
                or not 0 < item["count"] <= coverage.MAX_EVENTS
            ):
                raise WorkflowError("Unsafe control field shape")
            for name in ("value_hashes", "name_hashes"):
                counts = item[name]
                if (
                    not isinstance(counts, dict)
                    or len(counts) > 65
                    or any(
                        not isinstance(key, str)
                        or (key != "overflow" and re.fullmatch(r"[a-f0-9]{64}", key) is None)
                        or type(count) is not int
                        or not 0 < count <= coverage.MAX_EVENTS
                        for key, count in counts.items()
                    )
                ):
                    raise WorkflowError("Unsafe control field hashes")


def observe_control(summary, field, event, key):
    """Retain bounded shapes and hashes, never raw customization/account/path data."""
    present = key in event
    value = event.get(key)
    kind = (
        "missing"
        if not present
        else "null"
        if value is None
        else "boolean"
        if type(value) is bool
        else "number"
        if type(value) in {int, float}
        else "string"
        if isinstance(value, str)
        else "array"
        if isinstance(value, list)
        else "object"
    )
    item = {
        "present": present,
        "type": kind,
        "length": len(value) if isinstance(value, (str, list, dict)) else None,
        "value_hashes": {},
        "name_hashes": {},
    }
    values = value if isinstance(value, list) else [value] if present else []
    for entry in values[:64]:
        count_hash(item["value_hashes"], entry)
        name = entry.get("name") if isinstance(entry, dict) else entry if isinstance(entry, str) else None
        if name is not None:
            count_hash(item["name_hashes"], name)
    if len(values) > 64:
        item["value_hashes"]["overflow"] = min(len(values) - 64, coverage.MAX_EVENTS)
    target = summary["control_fields"].setdefault(field, {"observations": [], "overflow": 0})
    for old in target["observations"]:
        if {k: v for k, v in old.items() if k != "count"} == item:
            old["count"] = min(old["count"] + 1, coverage.MAX_EVENTS)
            return
    if len(target["observations"]) < 8:
        target["observations"].append({**item, "count": 1})
    else:
        target["overflow"] = min(target["overflow"] + 1, coverage.MAX_EVENTS)


def empty_catalog(event):
    return (
        set(event) == {"type", "subtype", "commands", "session_id", "uuid"}
        and event.get("subtype") == "commands_changed"
        and event.get("commands") == []
        and isinstance(event.get("uuid"), str)
        and re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", event["uuid"]) is not None
    )


def count_hash(counts, value):
    # Never retain arbitrary provider subtype, agent name or payload strings.
    key = coverage.checksum(json.dumps(value, sort_keys=True, ensure_ascii=False))
    if key not in counts and len(counts) >= 64:
        key = "overflow"
    counts[key] = min(counts.get(key, 0) + 1, coverage.MAX_EVENTS)


def request_start(event):
    return (
        set(event) == {"type", "subtype", "status", "session_id", "uuid"}
        and event.get("subtype") == "status"
        and event.get("status") == "requesting"
        and isinstance(event.get("uuid"), str)
        and re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", event["uuid"]) is not None
    )


def numerical(value):
    return type(value) in {int, float} and math.isfinite(value) and value >= 0


def usage_projection(event, policy):
    result = {}
    for field, name in (
        ("total_cost_usd", "estimated_usd"),
        ("duration_ms", "duration_ms"),
        ("num_turns", "num_turns"),
    ):
        if numerical(event.get(field)):
            result[name] = event[field]
    usage = event.get("usage")
    if isinstance(usage, dict):
        for name in (
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
        ):
            if numerical(usage.get(name)):
                result[name] = usage[name]
    models = {}
    native_models = event.get("modelUsage")
    if isinstance(native_models, dict) and isinstance(native_models.get(policy["model"]), dict):
        mapping = {
            "inputTokens": "input_tokens",
            "outputTokens": "output_tokens",
            "cacheReadInputTokens": "cache_read_input_tokens",
            "cacheCreationInputTokens": "cache_creation_input_tokens",
            "costUSD": "estimated_usd",
        }
        models[policy["model"]] = {
            target: native_models[policy["model"]][source]
            for source, target in mapping.items()
            if numerical(native_models[policy["model"]].get(source))
        }
    return {
        "status": "observed" if "estimated_usd" in result else "unknown",
        "counters": result,
        "models": models,
    }


def observation(tool, args, content, workspace, files):
    if tool == "Read":
        path = coverage.packet_path(args.get("file_path"), workspace, files)
        if path is None:
            return [], [], "unsafe_or_unknown_path"
        source = files[path].splitlines()
        # Lcn splits only LF; h2n removes one terminal CR. Refuse text whose
        # native numbering differs from the inventory's splitlines convention.
        native = [line.removesuffix("\r") for line in files[path].split("\n")]
        eof = bool(files[path]) and files[path].endswith("\n")
        if (native[:-1] if eof else native) != source:
            return [], [], "unsupported_native_line_boundaries"
        start = args.get("offset", 1)
        limit = args.get("limit", len(native))
        if type(start) is not int or type(limit) is not int or start < 1 or limit < 1:
            return [], [], "unsupported_range"
        end = min(start + limit - 1, len(native))
        numbers, seen = [], set()
        for line in content.split("\n"):
            match = re.fullmatch(r"([1-9][0-9]*)(?:\t|:)(.*)", line)
            if not match:
                return [], [], "unsupported_native_read_rendering"
            number = int(match[1])
            if not start <= number <= end or number in seen or match[2] != native[number - 1]:
                return [], [], "native_read_differs_from_source"
            seen.add(number)
            if number <= len(source):
                numbers.append(number)
            elif not eof:
                return [], [], "native_read_differs_from_source"
        spans = [
            {"artifact": path, "start_line": a, "end_line": b, "sha256": coverage.line_digest(source, a, b)}
            for a, b in coverage.ranges(numbers)
        ]
        return spans, [], None if spans else "unrecognized_or_empty_tool_result"
    if tool == "Grep":
        if args.get("output_mode") != "content" or args.get("-n") is not True:
            return [], [], "unrecognized_or_empty_tool_result"
        return coverage.tool_observation("grep", args, content, workspace, files)
    return coverage.tool_observation("glob", args, content, workspace, files)


def capture(raw, packet, workspace, policy, session_id, *, exit_code=0, failure=None, refusal_path=None):
    if policy["adapter"] in {LEGACY_ADAPTER, previous.ADAPTER}:
        frozen = legacy if policy["adapter"] == LEGACY_ADAPTER else previous
        return frozen.capture(
            raw,
            packet,
            workspace,
            policy,
            session_id,
            exit_code=exit_code,
            failure=failure,
            refusal_path=refusal_path,
        )
    if policy["adapter"] != ADAPTER:
        raise WorkflowError("Unsupported Claude stream adapter")
    files, reasons = {}, set()
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
    pending, seen, records = {}, set(), []
    message_usage, assistant_envelopes = {}, set()
    step_usage_unknown = False
    refused_ids = set()
    count, report, usage = 0, "", {"status": "unknown", "counters": {}, "models": {}}
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
            if event.get("parent_tool_use_id") is not None or event.get("agent_id") or event.get("agentId"):
                reasons.add("delegated_or_mcp_event")
            if kind == "system":
                count_hash(summary["system_subtypes"], event.get("subtype"))
                if event.get("subtype") != "init":
                    observe_control(summary, "system.commands", event, "commands")
                    observe_control(summary, "system.status", event, "status")
                    observe_control(summary, "system.keys", {"keys": sorted(event)}, "keys")
                    if not (request_start(event) or empty_catalog(event)) or not summary["init_count"]:
                        reasons.add("unsupported_system_event")
                        count_hash(summary["system_payloads"], event)
                    continue
                summary["init_count"] += 1
                if event.get("model") != policy["model"]:
                    reasons.add("unexpected_model_identity")
                if event.get("tools") != ["Read", "Grep", "Glob"] and (
                    not isinstance(event.get("tools"), list)
                    or set(event["tools"]) != set(TOOLS)
                    or len(event["tools"]) != 3
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
            elif kind == "assistant":
                message = event.get("message")
                if (
                    not isinstance(message, dict)
                    or message.get("model") != policy["model"]
                    or event.get("error")
                ):
                    reasons.add("unexpected_model_or_assistant_error")
                    continue
                summary["model_verified"] = True
                fingerprint = coverage.checksum(json.dumps(message, sort_keys=True, ensure_ascii=False))
                if fingerprint in assistant_envelopes:
                    continue
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
                    identifier, tool, args = block.get("id"), block.get("name"), block.get("input")
                    if not isinstance(identifier, str) or identifier in seen or not isinstance(args, dict):
                        raise ValueError
                    seen.add(identifier)
                    if tool not in TOOLS:
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
                    if (
                        refusal_path is not None
                        and tool == "Read"
                        and args.get("file_path") == str(refusal_path)
                    ):
                        if (
                            block.get("is_error") is True
                            and isinstance(content, str)
                            and "Permission to use Read has been denied" in content
                        ):
                            summary["controlled_refusals"] += 1
                            refused_ids.add(identifier)
                            reasons.add("controlled_refusal_diagnostic_only")
                            continue
                        reasons.add("controlled_refusal_not_observed")
                    success = block.get("is_error", False) is False and isinstance(content, str)
                    spans, paths, reason = [], [], "tool_failed_or_unsupported_content"
                    if success:
                        spans, paths, reason = observation(tool, args, content, workspace, files)
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
                if summary["terminal_count"] == 1 and isinstance(event.get("result"), str):
                    report = event["result"]  # Exact terminal text, even for a partial report.
                if event.get("subtype") != "success" or event.get("is_error") is not False:
                    reasons.add("provider_terminal_failure")
                    if event.get("subtype") == "error_max_budget_usd":
                        reasons.add("reference_cost_limit_reached")
                denials = event.get("permission_denials")
                expected_denials = (
                    refusal_path is not None
                    and isinstance(denials, list)
                    and all(
                        isinstance(item, dict)
                        and item.get("tool_name") == "Read"
                        and item.get("tool_use_id") in refused_ids
                        and isinstance(item.get("tool_input"), dict)
                        and item["tool_input"].get("file_path") == str(refusal_path)
                        for item in denials
                    )
                )
                if denials and not expected_denials:
                    reasons.add("permission_denied")
                if event.get("structured_output") is not None:
                    reasons.add("unsupported_structured_output")
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
    diagnostics = {
        "schema_version": 5,
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
    return report, diagnostics
