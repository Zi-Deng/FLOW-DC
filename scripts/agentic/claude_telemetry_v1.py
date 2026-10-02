"""Strict native stream-json adapter. UI metadata and assistant assertions earn no reads.

Fixtures test the pinned renderer contract; only an actual successful invocation can
establish live capability. Unknown shapes preserve incomplete, sanitized diagnostics.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path

import review_coverage as coverage
from workflow import WorkflowError

TOOLS = {"Read": "view", "Grep": "grep", "Glob": "glob"}
KINDS = {"system", "assistant", "user", "result", "rate_limit_event"}
# Pinned built-in declarations are not delegation permission. Agent is absent
# from the tool list and actual delegated messages remain forbidden.
BUILTIN_AGENTS = {"Explore", "Plan", "general-purpose", "statusline-setup"}


def validate_summary(value):
    if not isinstance(value, dict) or set(value) != {
        "types",
        "unknown_types",
        "terminal_count",
        "init_count",
        "model_verified",
        "session_verified",
        "controlled_refusals",
    }:
        raise WorkflowError("Invalid Claude telemetry summary")
    for key in ("types", "unknown_types"):
        if not isinstance(value[key], dict) or len(value[key]) > 65:
            raise WorkflowError("Unbounded Claude telemetry summary")
        for name, count in value[key].items():
            if not isinstance(name, str) or type(count) is not int or not 0 < count <= coverage.MAX_EVENTS:
                raise WorkflowError("Unsafe Claude telemetry count")
            if (key == "types" and name not in KINDS) or (
                key == "unknown_types" and name != "overflow" and not re.fullmatch(r"[a-f0-9]{64}", name)
            ):
                raise WorkflowError("Unsafe Claude telemetry name")
    if any(
        type(value[key]) is not int or not 0 <= value[key] <= coverage.MAX_EVENTS
        for key in ("terminal_count", "init_count", "controlled_refusals")
    ) or any(type(value[key]) is not bool for key in ("model_verified", "session_verified")):
        raise WorkflowError("Invalid Claude identity summary")


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
        start = args.get("offset", 1)
        limit = args.get("limit", len(source))
        if type(start) is not int or type(limit) is not int or start < 1 or limit < 1:
            return [], [], "unsupported_range"
        end = min(start + limit - 1, len(source))
        numbers = []
        for line in content.splitlines():
            match = re.fullmatch(r"([1-9][0-9]*)(?:\t|:)(.*)", line)
            if not match:
                return [], [], "unsupported_native_read_rendering"
            number = int(match[1])
            if not start <= number <= end or number in numbers or match[2] != source[number - 1]:
                return [], [], "native_read_differs_from_source"
            numbers.append(number)
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
                if event.get("subtype") != "init":
                    reasons.add("unsupported_system_event")
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
                if (
                    event.get("mcp_servers") != []
                    or event.get("plugins", []) != []
                    or event.get("skills", []) != []
                    or not isinstance(agents, list)
                    or any(not isinstance(agent, str) or agent not in BUILTIN_AGENTS for agent in agents)
                ):
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
        "schema_version": 2,
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
