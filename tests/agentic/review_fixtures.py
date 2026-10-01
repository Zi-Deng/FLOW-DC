"""Synthetic session events based on the public SDK schema, NOT live CLI evidence.

Schema reference (2026-09-29): github/copilot-sdk/nodejs/src/generated/session-events.ts.
The content renderings are representative fixtures for the explicitly supported
adapter, not proof that pinned Copilot 1.0.83 emits them for this account.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts/agentic"))
import review  # noqa: E402
import review_coverage as coverage  # noqa: E402

HELP = (
    "--model --available-tools --no-custom-instructions --disable-builtin-mcps "
    "--no-remote-export --no-ask-user --usage-output-file --max-ai-credits --output-format --session-id --effort"
)


def events(packet, body=None, *, omit=()):
    packet = Path(packet)
    required = coverage.read_json(packet / "required-material.json")["required"]
    output = [{"type": "session.start", "data": {"sessionId": "synthetic-session"}}]

    def call(name, args, result):
        identifier = f"synthetic-call-{len(output)}"
        output.extend(
            [
                {
                    "type": "tool.execution_start",
                    "data": {"toolCallId": identifier, "toolName": name, "arguments": args},
                },
                {
                    "type": "tool.execution_complete",
                    "data": {"toolCallId": identifier, "success": True, "result": {"content": result}},
                },
            ]
        )

    def view(path, start, end):
        lines = (packet / path).read_bytes().decode("utf-8").splitlines()
        call(
            "view",
            {"path": path, "view_range": [start, end]},
            "\n".join(f"{i}. {lines[i - 1]}" for i in range(start, end + 1)),
        )

    probe = coverage.read_json(packet / "capability.json")
    view(probe["artifact"], 1, 2)
    call(
        "grep",
        {"path": probe["artifact"], "pattern": probe["token"], "output_mode": "content", "-n": True},
        f"{probe['artifact']}:2:{probe['token']}",
    )
    call("glob", {"pattern": "capability/*.txt"}, probe["artifact"])
    claims = []
    for item in required:
        if item.get("omitted"):
            claims.append(
                {"id": item["id"], "state": "unsupported", "locations": [], "reason": item["omitted"]}
            )
        else:
            location = {key: item[key] for key in ("artifact", "start_line", "end_line")}
            claims.append({"id": item["id"], "state": "reviewed", "locations": [location], "reason": ""})
            if item["id"] not in omit:
                view(item["artifact"], item["start_line"], item["end_line"])
    if body is None:
        body = json.dumps(
            {
                "schema_version": 2,
                "inventory_sha256": review.digest(packet / "required-material.json"),
                "findings": [],
                "reviewed": [claim["id"] for claim in claims if claim["state"] == "reviewed"],
                "incomplete": [
                    {"ids": [claim["id"]], "state": claim["state"], "reason": claim["reason"]}
                    for claim in claims
                    if claim["state"] != "reviewed"
                ],
                "limitations": ["Synthetic fixture. Reviewer ran no tests."],
            },
            ensure_ascii=False,
        )
    output.extend(
        [
            {"type": "assistant.message", "data": {"messageId": "synthetic-final", "content": body}},
            {"type": "session.idle", "data": {}},
        ]
    )
    return output


def stream(packet, body=None, **kwargs):
    return "\n".join(json.dumps(item, ensure_ascii=False) for item in events(packet, body, **kwargs)) + "\n"


def store(repo, directory, body=None, **kwargs):
    directory = Path(directory)
    meta = review.verify_packet(directory)
    report, diagnostics = coverage.parse_events(
        stream(directory / "packet", body, **kwargs),
        directory / "packet",
        directory / "packet",
        version="1.0.83",
    )
    review.save_result(directory, meta, report, diagnostics, "1.0.83")
    return review.recover_review(repo, directory)


def provider_response(args, kwargs, packet, body=None):
    """Synthetic stdout without call IDs, plus correlated transient session file."""
    session_id = args[args.index("--session-id") + 1]
    rows = events(packet, body)
    rows[0]["data"]["sessionId"] = session_id
    target = Path(kwargs["env"]["COPILOT_HOME"]) / "session-state" / session_id / "events.jsonl"
    target.parent.mkdir(parents=True)
    target.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    return json.dumps({"type": "result", "exitCode": 0, "result": rows[-2]["data"]["content"]}) + "\n"
