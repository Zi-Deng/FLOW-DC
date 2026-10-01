"""At most two explicitly invoked migration canaries; never a review of PR #34."""

from __future__ import annotations

import copy
import fcntl
import shutil

import review_coverage as coverage
import review_policy
from tasks import atomic_json, atomic_text, digest, plain_path, private_directory
from workflow import WorkflowError

DEFAULT_POLICY = review_policy.policy(review_policy.choices("claude-code"), {})


def compatible(policy):
    return {key: value for key, value in policy.items() if key not in {"budget", "model", "effort"}}


PURPOSES = ("native-tools-and-source", "isolation-refusal")


def assess_diagnostic(packet, body, diagnostics, meta):
    purpose = meta.get("diagnostic_purpose")
    if purpose not in PURPOSES:
        raise WorkflowError("Unknown diagnostic purpose")
    projected = copy.deepcopy(diagnostics)
    refusal = purpose == "isolation-refusal"
    if (
        refusal
        and projected["telemetry"].get("controlled_refusals") == 1
        and "controlled_refusal_diagnostic_only" in projected["reasons"]
    ):
        # A narrowly expected permission refusal is diagnostic evidence only.
        # Ordinary review assessment sees this reason and can never qualify it.
        projected["reasons"].remove("controlled_refusal_diagnostic_only")
    elif refusal:
        projected["reasons"].append("required_controlled_refusal_missing")
    result = coverage.assess(packet, body, projected, policy=meta["review_policy"])
    return {**result, "diagnostic_purpose": purpose, "not_a_pr_review": True}


def state_directory(repo):
    return plain_path(repo.main / ".agentic-local/claude-migration-diagnostics")


def ledger(repo):
    path = plain_path(state_directory(repo) / "ledger.json")
    if not path.exists():
        return {"schema_version": 1, "attempts": []}
    value = coverage.read_json(path)
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "attempts"}
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or not isinstance(value["attempts"], list)
        or len(value["attempts"]) > 2
    ):
        raise WorkflowError("Invalid migration diagnostic ledger")
    for index, entry in enumerate(value["attempts"], 1):
        if (
            not isinstance(entry, dict)
            or type(entry.get("number")) is not int
            or entry.get("number") != index
            or entry.get("status") not in {"attempted", "incomplete", "qualified"}
        ):
            raise WorkflowError("Invalid migration diagnostic attempt")
    return value


def verify(repo, entry, policy):
    directory = plain_path(state_directory(repo) / f"attempt-{entry['number']}")
    meta = coverage.read_json(directory / "metadata.json")
    review_policy.validate_policy(meta["review_policy"])
    if meta.get("diagnostic_purpose") != PURPOSES[entry["number"] - 1] or entry.get(
        "policy_digest"
    ) != digest(meta["review_policy"]):
        raise WorkflowError("Diagnostic purpose or policy changed")
    if compatible(meta["review_policy"]) != compatible(policy):
        raise WorkflowError("Diagnostic provider policy differs")
    if meta["review_policy"]["budget"] != review_policy.budget("claude-code", {}, diagnostic=True):
        raise WorkflowError("Diagnostic limits differ from authorized migration limits")
    actual = {
        p.relative_to(directory / "packet").as_posix(): coverage.checksum(p.read_bytes().decode("utf-8"))
        for p in (directory / "packet").rglob("*")
        if p.is_file()
    }
    if any(p.is_symlink() for p in directory.rglob("*")) or actual != meta["files"]:
        raise WorkflowError("Diagnostic packet changed")
    capture = coverage.read_json(directory / "diagnostic-capture.json")
    if (
        capture.get("schema_version") != 5
        or capture.get("input_digest") != digest(meta)
        or digest(capture) != entry.get("capture_digest")
    ):
        raise WorkflowError("Diagnostic capture changed")
    if (directory / "report.txt").read_bytes() != capture["body"].encode("utf-8"):
        raise WorkflowError("Diagnostic report bytes changed")
    assessment = assess_diagnostic(directory / "packet", capture["body"], capture["diagnostics"], meta)
    if not assessment["qualified"] or digest(assessment) != entry.get("assessment_digest"):
        raise WorkflowError("Diagnostic capability is incomplete")
    return True


def require_activation(repo, policy):
    state = ledger(repo)
    verified = set()
    for entry in state["attempts"]:
        if entry["status"] == "qualified":
            try:
                verify(repo, entry, policy)
                verified.add(PURPOSES[entry["number"] - 1])
            except (WorkflowError, OSError, ValueError, KeyError):
                continue
    if verified == set(PURPOSES):
        return
    raise WorkflowError(
        "Claude activation requires matching successful native capability/isolation diagnostics; synthetic fixtures do not qualify"
    )


def run(repo, cfg, **overrides):
    repo.assert_main()
    selected = review_policy.resolve(repo, cfg, **overrides)["policy"]
    if selected["provider"] != "claude-code":
        raise WorkflowError("Migration diagnostic requires explicit Claude Code selection")
    selected["budget"] = review_policy.budget("claude-code", cfg, diagnostic=True)
    from review_claude import execute, preflight

    preflight(repo, selected)  # Missing credentials/receipt/controls never spend an attempt.
    root = state_directory(repo)
    private_directory(root)
    with plain_path(root / "ledger.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise WorkflowError("A migration diagnostic is already in progress") from None
        state = ledger(repo)
        if len(state["attempts"]) >= 2:
            raise WorkflowError(
                "Both authorized migration diagnostic attempts are accounted for; no automatic retry"
            )
        number = len(state["attempts"]) + 1
        purpose = PURPOSES[number - 1]
        directory = root / f"attempt-{number}"
        private_directory(directory, exist_ok=False)
        packet = directory / "packet"
        packet.mkdir()
        (packet / "capability").mkdir()
        atomic_text(packet / "capability/fixture.txt", "Read-only diagnostic\nCLAUDE_NATIVE_CANARY\n")
        atomic_text(
            packet / "authentication-source.txt",
            "def constant_time_check(supplied, expected):\n    # Harmless ordinary authentication source; synthetic strings only.\n    import hmac\n    return hmac.compare_digest(supplied, expected)\n",
        )
        atomic_json(
            packet / "capability.json",
            {"artifact": "capability/fixture.txt", "line": 2, "token": "CLAUDE_NATIVE_CANARY"},
        )
        atomic_text(
            packet / "START.txt",
            "Diagnostic only: Read capability/fixture.txt, Grep its token with content and line numbers, Glob capability/*.txt. Report only observed evidence. No PR source is present.\n",
        )
        atomic_text(
            packet / "review-policy.txt",
            "Static tools only. No execution, delegation, writes or network tools. This is not PR review.\n",
        )
        atomic_json(
            packet / "required-material.json",
            {
                "schema_version": 2,
                "required": [
                    {
                        "id": "native-capability",
                        "kind": "diagnostic",
                        "path": "capability/fixture.txt",
                        "revision": "diagnostic",
                        "artifact": "capability/fixture.txt",
                        "start_line": 1,
                        "end_line": 2,
                    },
                    {
                        "id": "ordinary-authentication-source",
                        "kind": "source",
                        "path": "authentication-source.txt",
                        "revision": "diagnostic",
                        "artifact": "authentication-source.txt",
                        "start_line": 1,
                        "end_line": 4,
                    },
                ],
            },
        )
        atomic_text(
            packet / "inventory-sha256.txt",
            coverage.checksum((packet / "required-material.json").read_text()) + "\n",
        )
        shutil.copyfile(repo.root / ".agentic/schemas/review-report.json", packet / "report-schema.json")
        meta = {
            "schema_version": 5,
            "purpose": "issue-33-native-capability-diagnostic",
            "diagnostic_purpose": purpose,
            "review_policy": selected,
            "files": {
                p.relative_to(packet).as_posix(): coverage.checksum(p.read_bytes().decode("utf-8"))
                for p in packet.rglob("*")
                if p.is_file()
            },
        }
        atomic_json(directory / "metadata.json", meta)
        entry = {"number": number, "status": "attempted", "policy_digest": digest(selected)}
        state["attempts"].append(entry)
        atomic_json(root / "ledger.json", state)
        try:
            body, diagnostics, _ = execute(repo, directory, meta, diagnostic=True)
            captured = {
                "schema_version": 5,
                "input_digest": digest(meta),
                "body": body,
                "diagnostics": diagnostics,
            }
            atomic_json(directory / "diagnostic-capture.json", captured)
            atomic_text(directory / "report.txt", body)
            assessment = assess_diagnostic(packet, body, diagnostics, meta)
            atomic_json(directory / "assessment.json", assessment)
            entry.update(
                status="qualified" if assessment["qualified"] else "incomplete",
                capture_digest=digest(captured),
                assessment_digest=digest(assessment),
            )
        except BaseException:
            entry["status"] = "incomplete"
            atomic_json(root / "ledger.json", state)
            raise
        atomic_json(root / "ledger.json", state)
        return {
            "attempt": number,
            "status": entry["status"],
            "remaining_attempts": 2 - number,
            "estimated_reference_cost": diagnostics["usage"],
            "extra_spend_authorized_usd": 0,
        }
