"""Conservative Copilot session-event adapter and required-material coverage gate.

Only model-facing tool result content is evidence. Provider reasoning, environments,
errors, arbitrary arguments and detailed/UI-only results are never persisted here.
See docs/agent-workflow/COVERAGE.md for the versioned contract and its limits.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path, PurePosixPath

from copilot_policy import CLI_VERSION
from workflow import WorkflowError

SCHEMA = 2
ADAPTER = "copilot-session-events-v2"
MAX_EVENTS = 20000
MAX_TOOL_RECORDS = 4000
MAX_STREAM_BYTES = 16000000
MAX_DIAGNOSTIC_BYTES = 2000000
TOOLS = {"view", "grep", "glob"}
# Unknown event types are not silently treated as benign. Payloads of these known
# bookkeeping events are discarded, including all reasoning and user/prompt text.
IGNORED_EVENTS = {
    "session.start",
    "session.info",
    "session.title_changed",
    "session.context_changed",
    "session.usage_info",
    "session.usage_checkpoint",
    "user.message",
    "assistant.turn_start",
    "assistant.turn_end",
    "assistant.intent",
    "assistant.reasoning",
    "assistant.reasoning_delta",
    "assistant.message_delta",
    "assistant.message_start",
    "assistant.streaming_delta",
    "assistant.tool_call_delta",
    "tool.execution_progress",
    "tool.execution_partial_result",
    "session.tools_updated",
    "session.compaction_start",
    "session.compaction_complete",
    "session.context_cleared",
    "assistant.usage",
    "assistant.idle",
    "model.call_start",
    "model.call_finished",
    "session.model_change",
    "session.mode_changed",
    "session.mode_notice_delivered",
    "session.skills_loaded",
    "session.custom_agents_updated",
    "session.mcp_servers_loaded",
}


# Public SDK root-agent bookkeeping; payloads are never retained. Supporting a
# named type does not imply these were the unidentified events in the first run.
ROOT_EVENTS = {"system.message", "subagent.selected"}


def event_restriction(event):
    data = event.get("data") or {}
    if "agentId" in event:
        return "delegated_or_mcp_event"
    if not isinstance(data, dict):
        return "malformed_or_uncorrelated_event"
    if any(data.get(key) for key in ("parentToolCallId", "mcpServerName", "mcpToolName")):
        return "delegated_or_mcp_event"
    if event["type"] == "subagent.selected":
        selected = data.get("tools")
        if (
            data.get("agentName") != "independent-reviewer"
            or not isinstance(selected, list)
            or len(selected) != len(TOOLS)
            or any(not isinstance(tool, str) for tool in selected)
            or set(selected) != TOOLS
            or "toolCallId" in data
        ):
            return "unsupported_agent_selection"
    if event["type"] == "system.message" and (
        data.get("role") not in {"system", "developer"} or not isinstance(data.get("content"), str)
    ):
        return "unsupported_system_message"
    return None


def checksum(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def strict_json(text):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value

    def invalid(value):
        raise ValueError("nonfinite JSON value")

    return json.loads(text, object_pairs_hook=unique, parse_constant=invalid)


def read_json(path):
    try:
        return strict_json(Path(path).read_bytes().decode("utf-8"))
    except (OSError, ValueError) as exc:
        raise WorkflowError("Missing or malformed review evidence") from exc


def packet_path(value, workspace, files):
    if not isinstance(value, str) or "\x00" in value:
        return None
    path = PurePosixPath(value)
    if ".." in path.parts:
        return None
    if path.is_absolute():
        try:
            path = path.relative_to(str(workspace))
        except ValueError:
            return None
    relative = path.as_posix()
    return relative if relative in files else None


def ranges(numbers):
    result = []
    for number in sorted(set(numbers)):
        if result and result[-1][1] + 1 == number:
            result[-1][1] = number
        else:
            result.append([number, number])
    return result


def valid_range(start, end, length):
    return type(start) is int and type(end) is int and 1 <= start <= end <= length


def line_digest(lines, start, end):
    return checksum(json.dumps(lines[start - 1 : end], ensure_ascii=False))


def view_text_matches(expected, returned):
    """Exact source bytes, optionally without one complete final LF/CRLF separator.

    Empty output cannot prove an empty line. Omitting a final blank line's separator
    would also be indistinguishable from omitting that line, so refuse that case.
    Internal separators, whitespace, Unicode and report bytes are never normalized.
    """
    if not returned:
        return False
    if expected == returned:
        return True
    separator = "\r\n" if expected.endswith("\r\n") else "\n"
    return (
        expected.endswith(separator)
        and expected.splitlines(keepends=True)[-1] != separator
        and returned == expected[: -len(separator)]
    )


def tool_observation(name, arguments, content, workspace, files):
    """Credit exact model-facing content against immutable packet text.

    Supported renderings: exact unnumbered view text or N. / N: / N<TAB> lines, grep's
    packet/path:N:text and glob's newline-separated packet paths. Other formats
    retain a result digest but receive no inspected-range credit.
    """
    observed = {}
    if name == "view":
        path = packet_path(arguments.get("path"), workspace, files)
        if path is None:
            return [], [], "unsafe_or_unknown_path"
        text = files[path]
        chunks = text.splitlines(keepends=True)
        lines = text.splitlines()
        requested = arguments.get("view_range")
        if requested is None:
            requested = [1, len(lines)]
        if not isinstance(requested, list) or len(requested) != 2:
            return [], [], "unsupported_range"
        start, end = requested
        if end == -1:
            end = len(lines)
        if not valid_range(start, end, len(lines)):
            return [], [], "unsupported_range"
        # Prefer actual source text over a possible numbered rendering. Numeric-
        # looking raw lines (or prefixes of them) must not credit a different line.
        if view_text_matches(text, content):
            observed[path] = set(range(1, len(lines) + 1))
        else:
            width = len(content.splitlines())
            candidates, raw_prefix = [], False
            if width:
                for offset in range(len(chunks) - width + 1):
                    candidate = "".join(chunks[offset : offset + width])
                    if view_text_matches(candidate, content):
                        candidates.append([offset + 1, offset + width])
                    raw_prefix |= candidate.startswith(content)
            if candidates:
                if candidates != [[start, end]]:
                    return [], [], "ambiguous_or_out_of_range_view"
                observed[path] = set(range(start, end + 1))
            elif raw_prefix:
                return [], [], "partial_raw_view"
            else:
                for line in content.splitlines():
                    match = re.fullmatch(r"\s*([1-9][0-9]*)(?:\. |: |\t)(.*)", line)
                    if match:
                        number = int(match[1])
                        if start <= number <= end and match[2] == lines[number - 1]:
                            observed.setdefault(path, set()).add(number)
    elif name == "grep":
        # Search results are evidence of returned matching lines, never the entire
        # file or the requested search range. The model must view missing context.
        for line in content.splitlines():
            match = re.fullmatch(r"(.+?):([1-9][0-9]*):(.*)", line)
            if match:
                path = packet_path(match[1], workspace, files)
                number = int(match[2])
                if (
                    path
                    and number <= len(files[path].splitlines())
                    and match[3] == files[path].splitlines()[number - 1]
                ):
                    observed.setdefault(path, set()).add(number)
    elif name == "glob":
        paths = [packet_path(line, workspace, files) for line in content.splitlines()]
        return [], sorted({path for path in paths if path}), None
    spans = []
    for path, numbers in sorted(observed.items()):
        for start, end in ranges(numbers):
            spans.append(
                {
                    "artifact": path,
                    "start_line": start,
                    "end_line": end,
                    "sha256": line_digest(files[path].splitlines(), start, end),
                }
            )
    return spans, [], None if spans else "unrecognized_or_empty_tool_result"


def parse_events(
    raw, packet, workspace, *, exit_code=0, failure=None, version="unknown", usage=None, model="claude-opus-5"
):
    """Normalize public-shaped JSONL events; never persist the raw provider stream."""
    packet = Path(packet)
    files, reasons = {}, set()
    try:
        for path in packet.rglob("*"):
            if path.is_file() and not path.is_symlink():
                try:
                    files[path.relative_to(packet).as_posix()] = path.read_bytes().decode("utf-8")
                except UnicodeError:
                    reasons.add("undecodable_packet_artifact")
                except OSError:
                    reasons.add("unreadable_packet_artifact")
    except OSError:
        reasons.add("unreadable_packet_artifact")
    if failure:
        reasons.add(failure)
    if exit_code != 0:
        reasons.add("provider_exit_failure")
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeError:
            raw = ""
            reasons.add("invalid_utf8_stream")
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_STREAM_BYTES:
        raw = ""
        reasons.add("stream_limit_exceeded")
    pending, seen, records = {}, set(), []
    unknown = {}
    report, final_seen, terminal = "", False, False
    event_count = 0
    for line in raw.split("\n"):
        if not line.strip():
            continue
        event_count += 1
        if event_count > MAX_EVENTS:
            reasons.add("event_limit_exceeded")
            break
        try:
            event = strict_json(line)
            if not isinstance(event, dict) or not isinstance(event.get("type"), str):
                raise ValueError("unsupported envelope")
            kind, data = event["type"], event.get("data", {})
            restriction = event_restriction(event)
            if restriction:
                reasons.add(restriction)
            if kind == "result":
                # CLI stdout has a terminal envelope in addition to SDK events.
                # It can supply completion/usage, never fabricate tool evidence.
                terminal = True
                if (
                    event.get("is_error") is True
                    or event.get("error")
                    or event.get("subtype") not in {None, "success"}
                    or (
                        "exitCode" in event and (type(event["exitCode"]) is not int or event["exitCode"] != 0)
                    )
                ):
                    reasons.add("provider_terminal_failure")
                if usage is None:
                    usage = event.get("usage")
                final = event.get("result")
                if isinstance(final, str):
                    if final_seen and report != final:
                        reasons.add("conflicting_final_report")
                    report, final_seen = final, True
                continue
            if data is None and kind in IGNORED_EVENTS | {"session.idle", "session.shutdown"}:
                data = {}
            if not isinstance(data, dict):
                raise ValueError("unsupported event data")
            if kind in {"session.start", "session.model_change", "model.call_start", "model.call_finished"}:
                for field in ("model", "selectedModel"):
                    if field in data and data[field] != model:
                        reasons.add("unexpected_model_identity")
            if data.get("parentToolCallId") or data.get("mcpServerName") or data.get("mcpToolName"):
                reasons.add("delegated_or_mcp_event")
            if kind == "tool.execution_start":
                terminal, final_seen = False, False
                identifier = data.get("toolCallId")
                name, args = data.get("toolName"), data.get("arguments")
                if not isinstance(identifier, str) or not identifier or identifier in seen:
                    raise ValueError("invalid call id")
                seen.add(identifier)
                if name not in TOOLS or not isinstance(args, dict):
                    reasons.add("forbidden_or_unsupported_tool")
                    pending[identifier] = None
                else:
                    target = args.get("path", ".")
                    if not isinstance(target, str):
                        reasons.add("inspection_outside_packet")
                    else:
                        selected = PurePosixPath(target)
                        if ".." in selected.parts or (
                            selected.is_absolute() and not selected.is_relative_to(str(workspace))
                        ):
                            reasons.add("inspection_outside_packet")
                    if name == "glob":
                        pattern = args.get("pattern")
                        if (
                            not isinstance(pattern, str)
                            or ".." in PurePosixPath(pattern).parts
                            or (
                                PurePosixPath(pattern).is_absolute()
                                and not PurePosixPath(pattern).is_relative_to(str(workspace))
                            )
                        ):
                            reasons.add("inspection_outside_packet")
                    pending[identifier] = (name, args)
            elif kind == "tool.execution_complete":
                identifier = data.get("toolCallId")
                if not isinstance(identifier, str) or identifier not in pending:
                    raise ValueError("uncorrelated result")
                call = pending.pop(identifier)
                if call is None:
                    continue
                name, args = call
                result = data.get("result")
                success = data.get("success") is True and not data.get("error")
                content = result.get("content") if isinstance(result, dict) else None
                reason = None
                spans, paths = [], []
                if not success:
                    reason = "tool_failed"
                elif not isinstance(content, str):
                    reason = "missing_model_facing_result"
                elif (
                    data.get("truncated")
                    or result.get("truncated")
                    or re.search(r"(?im)^\s*(?:\[|<|\.\.\.).*(?:truncat|omitted|more lines)", content)
                ):
                    reason = "truncated_tool_result"
                else:
                    spans, paths, reason = tool_observation(name, args, content, workspace, files)
                # Empty searches, directory listings and truncated exploratory
                # reads earn no range credit. A later complete read can recover
                # that material. Missing protocol fields still fail closed.
                if reason == "missing_model_facing_result" or type(data.get("success")) is not bool:
                    reasons.add("malformed_tool_completion")
                if len(records) >= MAX_TOOL_RECORDS:
                    reasons.add("tool_record_limit_exceeded")
                    continue
                # Do not retain the provider-supplied ID or arbitrary arguments.
                records.append(
                    {
                        "id": f"event-{event_count:06d}",
                        "tool": name,
                        "success": success,
                        "reason": reason,
                        "result_sha256": checksum(content) if isinstance(content, str) else None,
                        "spans": spans,
                        "paths": paths,
                    }
                )
            elif kind == "assistant.message":
                terminal = False
                content = data.get("content")
                if not isinstance(content, str):
                    raise ValueError("missing report")
                if data.get("toolRequests"):
                    final_seen = False
                else:
                    # Keep exact decoded UTF-8 message bytes, including CRLF and
                    # leading/trailing whitespace. Do not concatenate reasoning.
                    report, final_seen = content, True
                    if data.get("chunkCount", 1) != 1:
                        reasons.add("unsupported_chunked_report")
            elif kind in {"session.idle", "session.shutdown"}:
                terminal = True
            elif kind not in IGNORED_EVENTS | ROOT_EVENTS:
                reasons.add("unsupported_provider_event")
                key = checksum(kind)
                if key not in unknown and len(unknown) >= 64:
                    key = "overflow"
                unknown[key] = unknown.get(key, 0) + 1
        except (ValueError, TypeError, KeyError):
            reasons.add("malformed_or_uncorrelated_event")
    if not terminal or not final_seen or pending:
        reasons.add("incomplete_event_stream")
    if not records:
        reasons.add("missing_tool_telemetry")
    if not report.strip():
        reasons.add("missing_final_report")
    retained, size = [], 0
    for record in records:
        size += len(json.dumps(record).encode("utf-8"))
        if size > MAX_DIAGNOSTIC_BYTES:
            reasons.add("diagnostic_limit_exceeded")
            break
        retained.append(record)
    records = retained
    try:
        canary = capability(records, packet)
    except (WorkflowError, KeyError, TypeError, OSError, UnicodeError):
        canary = dict.fromkeys(sorted(TOOLS), False)
        reasons.add("unavailable_capability_artifact")
    if not all(canary.values()):
        reasons.add("capability_probe_incomplete")
    if isinstance(usage, dict):
        for field in ("modelUsage", "modelMetrics"):
            if isinstance(usage.get(field), dict) and set(usage[field]) - {model}:
                reasons.add("unexpected_usage_models")
    diagnostics = {
        "schema_version": SCHEMA,
        "adapter": ADAPTER,
        "cli_version": version,
        "exit_code": exit_code,
        "event_count": min(event_count, MAX_EVENTS),
        "reasons": sorted(reasons),
        "capability": canary,
        "events": records,
        "usage": sanitize_usage(usage, model),
        "telemetry": {
            "source": "stdout",
            "stdout_shapes": {},
            "session_shapes": {},
            "unknown_types": {"stdout": unknown, "session": {}},
        },
    }
    return report, diagnostics


def capability(records, packet):
    probe = read_json(Path(packet) / "capability.json")
    fixture = probe["artifact"]
    canary = {tool: False for tool in sorted(TOOLS)}
    for record in records:
        if not record["success"] or record["reason"]:
            continue
        if record["tool"] == "glob" and fixture in record["paths"]:
            canary["glob"] = True
        if record["tool"] in {"view", "grep"} and any(
            span["artifact"] == fixture and span["start_line"] <= probe["line"] <= span["end_line"]
            for span in record["spans"]
        ):
            canary[record["tool"]] = True
    return canary


USAGE_COUNTERS = {
    "inputTokens",
    "outputTokens",
    "reasoningTokens",
    "cacheReadTokens",
    "cacheWriteTokens",
    "cacheReadInputTokens",
    "cacheCreationInputTokens",
    "requestCount",
    "requests",
    "totalTokens",
    "totalApiDurationMs",
    "totalAiCredits",
    "aiCreditsUsed",
    "totalNanoAiu",
    "totalPremiumRequestCost",
    "premiumRequests",
    "sessionDurationMs",
    "totalUserRequests",
    "lastCallInputTokens",
    "lastCallOutputTokens",
    "requests_count",
    "requests_cost",
    "token_input",
    "token_output",
    "token_cache_read",
    "token_cache_write",
}


def sanitize_usage(value, model="claude-opus-5"):
    """Explicit numerical projection of CLI 1.0.83 usage and terminal usage."""

    def number(value):
        return type(value) in {int, float} and math.isfinite(value) and value >= 0

    def counters(item):
        if not isinstance(item, dict):
            return {}
        result = {key: item[key] for key in sorted(USAGE_COUNTERS) if key in item and number(item[key])}
        for key in ("inputTokens", "outputTokens", "reasoningTokens", "cacheReadTokens", "cacheWriteTokens"):
            if isinstance(item.get("usage"), dict) and number(item["usage"].get(key)):
                result[key] = item["usage"][key]
        for key in ("count", "cost"):
            if isinstance(item.get("requests"), dict) and number(item["requests"].get(key)):
                result["requests_" + key] = item["requests"][key]
        for key in ("input", "output", "cache_read", "cache_write"):
            token = (
                item.get("tokenDetails", {}).get(key) if isinstance(item.get("tokenDetails"), dict) else None
            )
            if isinstance(token, dict) and number(token.get("tokenCount")):
                result["token_" + key] = token["tokenCount"]
        return result

    result, models = counters(value), {}
    if isinstance(value, dict):
        for field in ("modelUsage", "modelMetrics"):
            source = value.get(field)
            if isinstance(source, dict) and isinstance(source.get(model), dict):
                models[model] = counters(source[model])
    return {"status": "observed" if result or models else "unknown", "counters": result, "models": models}


def validate_diagnostics(diagnostics, packet, policy=None):
    """Check persisted allowlist and ranges again at every qualification gate."""
    if (
        not isinstance(diagnostics, dict)
        or set(diagnostics)
        != {
            "schema_version",
            "adapter",
            "cli_version",
            "exit_code",
            "event_count",
            "reasons",
            "capability",
            "events",
            "usage",
            "telemetry",
        }
        or type(diagnostics["schema_version"]) is not int
        or diagnostics["schema_version"]
        != (
            {
                "claude-stream-json-2.1.282-v2": 3,
                "claude-stream-json-2.1.282-v3": 5,
                "claude-stream-json-2.1.282-v4": 6,
            }.get(policy.get("adapter"), SCHEMA)
            if policy
            else SCHEMA
        )
    ):
        raise WorkflowError("Unsupported coverage diagnostics")
    if (
        diagnostics["adapter"] != (policy["adapter"] if policy else ADAPTER)
        or not isinstance(diagnostics["reasons"], list)
        or not isinstance(diagnostics["events"], list)
        or len(diagnostics["events"]) > MAX_TOOL_RECORDS
        or not isinstance(diagnostics["capability"], dict)
        or set(diagnostics["capability"]) != TOOLS
        or any(type(v) is not bool for v in diagnostics["capability"].values())
    ):
        raise WorkflowError("Invalid coverage diagnostics")
    if (
        not isinstance(diagnostics["cli_version"], str)
        or not re.fullmatch(r"(?:unknown|[0-9]+\.[0-9]+\.[0-9]+)", diagnostics["cli_version"])
        or type(diagnostics["event_count"]) is not int
        or not 0 <= diagnostics["event_count"] <= MAX_EVENTS
        or (diagnostics["exit_code"] is not None and type(diagnostics["exit_code"]) is not int)
        or any(
            not isinstance(reason, str) or not re.fullmatch(r"[a-z_]{1,80}", reason)
            for reason in diagnostics["reasons"]
        )
    ):
        raise WorkflowError("Invalid diagnostic fields")
    if policy and policy["provider"] == "claude-code":
        if policy["adapter"] == "claude-stream-json-2.1.282-v1":
            from claude_telemetry_v1 import validate_summary
        elif policy["adapter"] == "claude-stream-json-2.1.282-v2":
            from claude_telemetry_v2 import validate_summary
        elif policy["adapter"] == "claude-stream-json-2.1.282-v3":
            from claude_telemetry_v3 import validate_summary
        else:
            from claude_telemetry import validate_summary
    else:
        from review_telemetry import validate_summary
    validate_summary(diagnostics["telemetry"])
    if policy and diagnostics["cli_version"] != policy["cli"]["version"]:
        raise WorkflowError("Diagnostic CLI identity differs from the packet")
    ids = set()
    usage = diagnostics["usage"]
    if (
        not isinstance(usage, dict)
        or set(usage) != {"status", "counters", "models"}
        or usage["status"] not in {"observed", "unknown"}
        or not isinstance(usage["counters"], dict)
        or not isinstance(usage["models"], dict)
        or set(usage["models"]) - {policy["model"] if policy else "claude-opus-5"}
    ):
        raise WorkflowError("Invalid usage projection")
    for counters in [usage["counters"], *usage["models"].values()]:
        if (
            not isinstance(counters, dict)
            or set(counters)
            - (CLAUDE_USAGE_COUNTERS if policy and policy["provider"] == "claude-code" else USAGE_COUNTERS)
            or any(type(v) not in {int, float} or not math.isfinite(v) or v < 0 for v in counters.values())
        ):
            raise WorkflowError("Unsafe usage projection")
    if len(json.dumps(diagnostics["events"]).encode("utf-8")) > MAX_DIAGNOSTIC_BYTES + 2 * MAX_TOOL_RECORDS:
        raise WorkflowError("Diagnostic evidence exceeds its bound")
    for event in diagnostics["events"]:
        if not isinstance(event, dict) or set(event) != {
            "id",
            "tool",
            "success",
            "reason",
            "result_sha256",
            "spans",
            "paths",
        }:
            raise WorkflowError("Invalid tool evidence")
        if (
            not isinstance(event["id"], str)
            or not re.fullmatch(r"event-[0-9]{6}", event["id"])
            or event["id"] in ids
            or event["tool"] not in TOOLS
            or type(event["success"]) is not bool
            or not isinstance(event["spans"], list)
            or not isinstance(event["paths"], list)
        ):
            raise WorkflowError("Invalid tool evidence identity")
        ids.add(event["id"])
        if (
            event["reason"] is not None
            and (not isinstance(event["reason"], str) or not re.fullmatch(r"[a-z_]{1,80}", event["reason"]))
            or event["result_sha256"] is not None
            and (
                not isinstance(event["result_sha256"], str)
                or not re.fullmatch(r"[a-f0-9]{64}", event["result_sha256"])
            )
            or event["success"]
            and event["reason"] is None
            and event["result_sha256"] is None
        ):
            raise WorkflowError("Invalid tool result metadata")
        for artifact in event["paths"]:
            if (
                not isinstance(artifact, str)
                or PurePosixPath(artifact).is_absolute()
                or ".." in PurePosixPath(artifact).parts
                or not (Path(packet) / artifact).is_file()
            ):
                raise WorkflowError("Invalid glob evidence path")
        for span in event["spans"]:
            if not isinstance(span, dict) or set(span) != {"artifact", "start_line", "end_line", "sha256"}:
                raise WorkflowError("Invalid inspected span")
            artifact = span["artifact"]
            if (
                not isinstance(artifact, str)
                or PurePosixPath(artifact).is_absolute()
                or ".." in PurePosixPath(artifact).parts
            ):
                raise WorkflowError("Invalid evidence path")
            path = Path(packet) / artifact
            if not path.is_file() or path.is_symlink():
                raise WorkflowError("Missing evidence artifact")
            try:
                lines = path.read_bytes().decode("utf-8").splitlines()
            except (OSError, UnicodeError) as exc:
                raise WorkflowError("Unreadable evidence artifact") from exc
            start, end = span["start_line"], span["end_line"]
            if not valid_range(start, end, len(lines)) or span["sha256"] != line_digest(lines, start, end):
                raise WorkflowError("Inspected span differs from packet")
    if diagnostics["capability"] != capability(diagnostics["events"], packet):
        raise WorkflowError("Capability assertions differ from observed probe evidence")


CLAUDE_USAGE_COUNTERS = {
    "model_steps_observed",
    "assistant_input_tokens_observed",
    "assistant_cache_read_input_tokens_observed",
    "assistant_cache_creation_input_tokens_observed",
    "estimated_usd",
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "duration_ms",
    "num_turns",
}


def assess(packet, body, diagnostics, policy=None):
    """Model claims are checked against required ranges and observed returned lines."""
    packet = Path(packet)
    validate_diagnostics(diagnostics, packet, policy)
    inventory = read_json(packet / "required-material.json")
    required = inventory["required"]
    reasons = list(diagnostics["reasons"])
    if diagnostics["exit_code"] != 0 or not all(diagnostics["capability"].values()):
        reasons.append("capability_or_execution_incomplete")
    if diagnostics["cli_version"] != (policy["cli"]["version"] if policy else CLI_VERSION):
        reasons.append("unsupported_cli_version")
    claims = {}
    try:
        document = report_document(body)
        if (
            not isinstance(document, dict)
            or set(document)
            != {"schema_version", "inventory_sha256", "findings", "reviewed", "incomplete", "limitations"}
            or type(document["schema_version"]) is not int
            or document["schema_version"] != SCHEMA
            or not isinstance(document["findings"], list)
            or not isinstance(document["reviewed"], list)
            or not isinstance(document["incomplete"], list)
            or document["inventory_sha256"]
            != checksum((packet / "required-material.json").read_bytes().decode("utf-8"))
            or not isinstance(document["limitations"], list)
            or not all(isinstance(v, str) for v in document["limitations"])
        ):
            raise ValueError("invalid report schema")
        for finding in document["findings"]:
            if not isinstance(finding, dict) or set(finding) != {
                "id",
                "severity",
                "path",
                "line",
                "claim",
                "trigger",
                "impact",
                "evidence",
                "fix",
            }:
                raise ValueError("invalid finding")
            if (
                finding["severity"] not in {"P0", "P1", "P2", "P3"}
                or type(finding["line"]) is not int
                or finding["line"] < 1
                or any(
                    not isinstance(finding[key], str) or not finding[key].strip()
                    for key in ("id", "path", "claim", "trigger", "impact", "evidence", "fix")
                )
            ):
                raise ValueError("invalid finding fields")
        lookup = {item["id"]: item for item in required}
        for identifier in document["reviewed"]:
            if not isinstance(identifier, str) or identifier in claims or identifier not in lookup:
                raise ValueError("invalid positive inspection ID")
            item = lookup[identifier]
            claims[identifier] = {
                "id": identifier,
                "state": "reviewed",
                "reason": "",
                "locations": [{key: item[key] for key in ("artifact", "start_line", "end_line")}]
                if not item.get("omitted")
                else [],
            }
        for group in document["incomplete"]:
            if (
                not isinstance(group, dict)
                or set(group) != {"ids", "state", "reason"}
                or not isinstance(group["ids"], list)
                or not group["ids"]
                or group["state"] not in {"unread", "unsupported"}
                or not isinstance(group["reason"], str)
                or not group["reason"].strip()
            ):
                raise ValueError("invalid incomplete group")
            for identifier in group["ids"]:
                if not isinstance(identifier, str) or identifier in claims or identifier not in lookup:
                    raise ValueError("invalid incomplete ID")
                claims[identifier] = {"state": group["state"], "reason": group["reason"]}
        # The immutable inventory, not the model's output length, defines scope.
        # Every absent ID remains explicitly unread in the materialized assessment.
    except (ValueError, TypeError, KeyError):
        reasons.append("malformed_report_contract")
        claims = {}
    rows = []
    for item in required:
        claim = claims.get(item["id"])
        row = {"id": item["id"], "state": "unread", "reason": "not_claimed_by_reviewer", "evidence": []}
        row["location"] = {
            key: item.get(key) for key in ("artifact", "start_line", "end_line", "path", "revision", "kind")
        }
        if item.get("omitted"):
            row.update(state="unsupported", reason="source_omitted")
        elif claim:
            row["state"] = claim["state"]
            if claim["state"] != "reviewed":
                row["reported_reason"] = claim["reason"]
                row["reason"] = (
                    "explicit_incomplete" if claim["reason"].strip() else "missing_incomplete_reason"
                )
            else:
                needed = set(range(item["start_line"], item["end_line"] + 1))
                claimed, observed = set(), set()
                invalid = False
                for location in claim["locations"]:
                    if (
                        not isinstance(location, dict)
                        or set(location) != {"artifact", "start_line", "end_line"}
                        or location["artifact"] != item["artifact"]
                        or not valid_range(location["start_line"], location["end_line"], item["end_line"])
                        or location["start_line"] < item["start_line"]
                    ):
                        invalid = True
                        continue
                    claimed.update(range(location["start_line"], location["end_line"] + 1))
                for event in diagnostics["events"]:
                    if not event["success"] or event["reason"] or event["tool"] not in {"view", "grep"}:
                        continue
                    for span in event["spans"]:
                        if span["artifact"] == item["artifact"]:
                            intersect = needed.intersection(range(span["start_line"], span["end_line"] + 1))
                            if intersect:
                                observed.update(intersect)
                                row["evidence"].append(event["id"])
                row["evidence"] = sorted(set(row["evidence"]))
                if invalid or claimed != needed or observed != needed:
                    row.update(state="unsupported", reason="locations_not_supported_by_tool_results")
                else:
                    row["reason"] = None
        rows.append(row)
    qualified = bool(rows) and not reasons and all(row["state"] == "reviewed" for row in rows)
    return {
        "schema_version": SCHEMA,
        "qualified": qualified,
        "reasons": sorted(set(reasons)),
        "required_count": len(rows),
        "inspected_count": sum(row["state"] == "reviewed" for row in rows),
        "material": rows,
        "limit": "Observed returned lines do not prove understanding, correctness or scientific validity.",
    }


def report_document(body):
    """Decode bare JSON or one complete outer json fence; never rewrite the body."""
    text = body.strip(" \t\r\n")
    if text.startswith("```json\n") or text.startswith("```json\r\n"):
        opening = text.index("\n") + 1
        if not text.endswith("\n```"):
            raise ValueError("incomplete JSON fence")
        text = text[opening:-4]
    return strict_json(text)
