"""Synthetic native stream shapes; not live 2.1.282 capability evidence."""

import json
from pathlib import Path

from review_fixtures import events


def native_events(packet, workspace, session, model="claude-opus-5-5", body=None):
    packet = Path(packet)
    rows = events(packet, body)
    output = [
        {
            "type": "system",
            "subtype": "init",
            "session_id": session,
            "model": model,
            "tools": ["Read", "Grep", "Glob"],
            "mcp_servers": [],
            "plugins": [],
            "skills": [],
            "agents": [],
            "slash_commands": [],
            "permissionMode": "dontAsk",
            "cwd": str(workspace),
            "claude_code_version": "2.1.282",
        }
    ]
    pending = {}
    for row in rows:
        data = row["data"]
        if row["type"] == "tool.execution_start":
            tool = {"view": "Read", "grep": "Grep", "glob": "Glob"}[data["toolName"]]
            args = data["arguments"]
            if tool == "Read":
                start, end = args["view_range"]
                args = {"file_path": args["path"], "offset": start, "limit": end - start + 1}
            pending[data["toolCallId"]] = (tool, args)
            output.append(
                {
                    "type": "assistant",
                    "session_id": session,
                    "parent_tool_use_id": None,
                    "message": {
                        "model": model,
                        "content": [
                            {"type": "tool_use", "id": data["toolCallId"], "name": tool, "input": args}
                        ],
                    },
                }
            )
        elif row["type"] == "tool.execution_complete":
            tool, args = pending.pop(data["toolCallId"])
            content = data["result"]["content"]
            if tool == "Read":
                lines = (packet / args["file_path"]).read_bytes().decode("utf-8").splitlines()
                content = "\n".join(
                    f"{number}\t{lines[number - 1]}"
                    for number in range(args["offset"], args["offset"] + args["limit"])
                )
            output.append(
                {
                    "type": "user",
                    "session_id": session,
                    "parent_tool_use_id": None,
                    "message": {
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": data["toolCallId"],
                                "content": content,
                                "is_error": False,
                            }
                        ]
                    },
                }
            )
    output.append(
        {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "session_id": session,
            "result": rows[-2]["data"]["content"],
            "total_cost_usd": 0.01,
            "duration_ms": 42,
            "num_turns": 1,
            "modelUsage": {model: {"inputTokens": 100, "outputTokens": 20}},
            "usage": {"input_tokens": 100, "output_tokens": 20},
            "permission_denials": [],
        }
    )
    return output


def native_stream(*args, **kwargs):
    return "\n".join(json.dumps(row, ensure_ascii=False) for row in native_events(*args, **kwargs)).encode(
        "utf-8"
    )


# Synthetic binding for parser/lifecycle tests; not a native registration or live proof.
AUTHENTICATION = {
    "schema_version": 2,
    "setup_provenance": "human-interactive-native-v1",
    "mode": "native-max-access-only-v1",
    "registration_id": "11111111-1111-4111-8111-111111111111",
    "generation_id": "22222222-2222-4222-8222-222222222222",
}
