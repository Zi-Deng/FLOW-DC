"""Issue-33-only approved prospective recovery. Local receipts are not attestations."""

from __future__ import annotations

import copy
import hashlib

import review_coverage as coverage
import review_policy
from tasks import atomic_json, digest, plain_path
from workflow import WorkflowError

CONTRACT = {
    "issue": 33,
    "plan_comment": 5963470903,
    "issue_digest": "ffb1d1c3836e54cb7a801b6e9049bd263e8fb8d4e3159641fd25a29c25f72629",
    "plan_digest": "7ef98e371d3b358c71dfb6058decbc9ad463f6e62fec07bc0b3476ddc4d12bc1",
}
CONTRACT_DIGEST = "dbbe4f2dccf83acd2a5fab4b6b031c96673516f4e63820df1036a198fd1c81c1"
SEQUENCE = {2: "native-tools-and-source", 3: "isolation-refusal"}
FILENAME = "recovery-ledger.json"
# Revision 4 is frozen, including its consumed/stopped v2 grant. New adapters
# cannot reuse it; changing software does not create another diagnostic slot.
REV4_ADAPTER = "claude-stream-json-2.1.282-v2"


def authorization(repo, *, historical=False, contract=CONTRACT, contract_digest=CONTRACT_DIGEST):
    """Execution uses current approval; frozen reads may use one exact historical receipt."""
    path = plain_path(repo.main / ".agentic-local/tasks/issue-33.json")
    state = coverage.read_json(path)
    if (
        not isinstance(state, dict)
        or repo.name != "Zi-Deng/FLOW-DC"
        or state.get("repository") != repo.name
        or state.get("key") != "issue-33"
        or type(state.get("schema_version")) is not int
        or state.get("schema_version") != 1
        or digest(contract) != contract_digest
    ):
        raise WorkflowError("Invalid issue 33 approval record")
    candidates = [state.get("approval")]
    if historical:
        history = state.get("approval_history", [])
        if not isinstance(history, list) or any(not isinstance(item, dict) for item in history):
            raise WorkflowError("Invalid historical approval records")
        candidates += history
    matches = [item for item in candidates if isinstance(item, dict) and item.get("contract") == contract]
    if len(matches) != 1:
        raise WorkflowError("Recovery requires one exact recorded approval; missing or ambiguous history")
    approval = matches[0]
    if (
        type(approval.get("issue")) is not int
        or type(approval.get("plan_comment")) is not int
        or approval.get("issue") != 33
        or approval.get("plan_comment") != contract["plan_comment"]
        or digest(approval.get("contract")) != contract_digest
        or not isinstance(approval.get("source"), str)
        or not approval["source"].strip()
        or not isinstance(approval.get("recorded_at"), str)
        or not approval["recorded_at"].strip()
    ):
        raise WorkflowError("Recovery requires the exact recorded approval source and contract")
    return {"contract": contract, "contract_digest": contract_digest, "approval_digest": digest(approval)}


def historical(repo):
    import review_diagnostics as diagnostics

    root = diagnostics.state_directory(repo)
    state = diagnostics.legacy_ledger(repo)
    if len(state["attempts"]) != 1 or state["attempts"][0]["status"] != "incomplete":
        raise WorkflowError("Recovery requires exactly the original incomplete attempt 1")
    directory = plain_path(root / "attempt-1")
    diagnostics.validate_files(directory)
    meta = coverage.read_json(directory / "metadata.json")
    if meta["review_policy"].get("adapter") != "claude-stream-json-2.1.282-v1":
        raise WorkflowError("Recovery cannot relabel a different historical adapter")
    diagnostics.verify(
        repo, state["attempts"][0], meta["review_policy"], require_qualified=False, strict_evidence=True
    )
    files = [root / "ledger.json", *directory.rglob("*")]
    hashes = {}
    for path in files:
        plain_path(path)
        if path.is_file():
            hashes[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
        elif not path.is_dir():
            raise WorkflowError("Historical diagnostic contains an unsafe file")
    return {
        "ledger_text": (root / "ledger.json").read_bytes().decode("utf-8"),
        "files": hashes,
        "entry": copy.deepcopy(state["attempts"][0]),
    }


def validate_policy(policy):
    import claude_native_auth

    review_policy.validate_policy(policy)
    claude_native_auth.validate_binding(policy.get("authentication"))
    expected = review_policy.policy(
        review_policy.choices("claude-code", "claude-opus-5-5", "medium"), {}, diagnostic=True
    )
    expected["adapter"] = REV4_ADAPTER
    expected["authentication"] = policy["authentication"]
    if policy != expected:
        raise WorkflowError(
            "Revision-4 recovery is bound to its original v2 Claude model/effort/CLI/budget policy"
        )


def match_recorded_policy(observed, current):
    """Compare durable bindings without credentials; this does not authorize lineage."""
    import claude_native_auth

    for policy in (observed, current):
        review_policy.validate_policy(policy)
        claude_native_auth.validate_binding(policy.get("authentication"))
    left = {k: v for k, v in observed.items() if k not in {"budget", "authentication"}}
    right = {k: v for k, v in current.items() if k not in {"budget", "authentication"}}
    left_auth = {k: v for k, v in observed["authentication"].items() if k != "generation_id"}
    right_auth = {k: v for k, v in current["authentication"].items() if k != "generation_id"}
    if left != right or left_auth != right_auth:
        raise WorkflowError("Recorded recovery policy or registration changed")


def match_policy(observed, current):
    """Execution/activation additionally require explicit verified renewal lineage."""
    import claude_native_auth

    match_recorded_policy(observed, current)
    if not claude_native_auth.capability_lineage(
        observed.get("authentication"), current.get("authentication"), current["budget"]["timeout_seconds"]
    ):
        raise WorkflowError("Recovery policy or verified authentication lineage changed")


def grant(repo, policy, *, historical_authority=False):
    validate_policy(policy)
    return {
        "schema_version": 1,
        "authorization": authorization(repo, historical=historical_authority),
        "historical": historical(repo),
        "policy": copy.deepcopy(policy),
        "slots": [{"number": n, "purpose": p} for n, p in SEQUENCE.items()],
        "max_total_attempts": 3,
        "stop_on_future_failure": True,
    }


def load(repo, *, later_attempts=False):
    import review_diagnostics as diagnostics

    root = diagnostics.state_directory(repo)
    path = plain_path(root / FILENAME)
    if not path.exists():
        return None
    value = coverage.read_json(path)
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "grant", "grant_digest", "attempts"}
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != 2
    ):
        raise WorkflowError("Invalid prospective recovery ledger")
    saved = value["grant"]
    if (
        not isinstance(saved, dict)
        or type(saved.get("schema_version")) is not int
        or digest(saved) != digest(grant(repo, saved.get("policy", {}), historical_authority=True))
        or value["grant_digest"] != digest(saved)
    ):
        raise WorkflowError("Recovery approval, policy or historical snapshot changed")
    attempts = value["attempts"]
    if (
        not isinstance(attempts, list)
        or not 1 <= len(attempts) <= 3
        or attempts[0] != saved["historical"]["entry"]
    ):
        raise WorkflowError("Recovery attempt count or original identity changed")
    for number, entry in enumerate(attempts[1:], 2):
        fields = {"number", "purpose", "status", "policy_digest", "grant_digest"}
        if (
            not isinstance(entry, dict)
            or set(entry) not in (fields, fields | {"capture_digest", "assessment_digest"})
            or entry.get("number") != number
            or type(entry.get("number")) is not int
            or entry.get("purpose") != SEQUENCE[number]
            or entry.get("grant_digest") != value["grant_digest"]
            or entry.get("status") not in {"attempted", "incomplete", "qualified"}
        ):
            raise WorkflowError("Invalid prospective diagnostic identity or purpose")
        meta = coverage.read_json(plain_path(root / f"attempt-{number}" / "metadata.json"))
        validate_policy(meta["review_policy"])
        match_recorded_policy(saved["policy"], meta["review_policy"])
        if entry["policy_digest"] != digest(meta["review_policy"]) or meta.get("recovery") != {
            "grant_digest": value["grant_digest"],
            "number": number,
            "purpose": SEQUENCE[number],
        }:
            raise WorkflowError("Prospective packet policy or grant binding changed")
    present = {p.name for p in root.glob("attempt-*")}
    expected = {f"attempt-{n}" for n in range(1, len(attempts) + 1)}
    allowed = (
        expected | {"attempt-3", "attempt-4", "attempt-5", "attempt-6", "attempt-7"}
        if later_attempts
        else expected
    )
    if not expected <= present <= allowed:
        raise WorkflowError("Conflicting or partial diagnostic migration/attempt state")
    if len(attempts) == 3:
        if attempts[1]["status"] != "qualified":
            raise WorkflowError("Recovery sequence continued after a failed attempt")
        # Validate retained evidence in its observed generation. Comparing it back
        # to the older grant generation would reverse permitted renewal lineage.
        observed = coverage.read_json(root / "attempt-2/metadata.json")["review_policy"]
        diagnostics.verify(repo, attempts[1], observed, recovery=value)
    return value


def next_slot(repo, state, policy):
    import review_diagnostics as diagnostics

    review_policy.require_current_adapter(policy)
    validate_policy(policy)
    match_policy(state["grant"]["policy"], policy)
    attempts = state["attempts"]
    if len(attempts) >= 3:
        raise WorkflowError("All three counted diagnostic attempts are exhausted; no fourth call")
    if len(attempts) == 2:
        if attempts[1]["status"] != "qualified":
            raise WorkflowError("Recovery stops after a failed or interrupted future attempt")
        diagnostics.verify(repo, attempts[1], policy, recovery=state)
    return len(attempts) + 1, SEQUENCE[len(attempts) + 1]


def prepare(repo, cfg, *, apply=False, preview_digest=None):
    """Explicit preview/application; never invoked by status or automatic recovery."""
    import claude_native_auth
    import review_diagnostics as diagnostics

    repo.assert_main()
    if review_policy.PROVIDERS["claude-code"]["adapter"] == "claude-stream-json-2.1.282-v5":
        import diagnostic_recovery_v7

        return diagnostic_recovery_v7.prepare(repo, cfg, apply=apply, preview_digest=preview_digest)
    # V5 is a separate grant/file; historical v4 callers retain their own policy.
    if review_policy.PROVIDERS["claude-code"]["adapter"] == "claude-stream-json-2.1.282-v4":
        import diagnostic_recovery_v6

        return diagnostic_recovery_v6.prepare(repo, cfg, apply=apply, preview_digest=preview_digest)
    if review_policy.PROVIDERS["claude-code"]["adapter"] == "claude-stream-json-2.1.282-v3":
        import diagnostic_recovery_v5

        return diagnostic_recovery_v5.prepare(repo, cfg, apply=apply, preview_digest=preview_digest)
    selected = review_policy.resolve(repo, cfg, review_provider="claude-code")["policy"]
    selected["budget"] = review_policy.budget("claude-code", {}, diagnostic=True)
    if selected["adapter"] != REV4_ADAPTER:
        raise WorkflowError("Revision-4 grant is recovery-only for its original v2 adapter; no new allowance")
    selected = claude_native_auth.bind(selected)
    with diagnostics.locked(repo):
        current = load(repo)
        if current is not None:
            proposed = current["grant"]
            match_policy(proposed["policy"], selected)
        else:
            proposed = grant(repo, selected)
            root = diagnostics.state_directory(repo)
            if {p.name for p in root.glob("attempt-*")} != {"attempt-1"}:
                raise WorkflowError("Conflicting or partial diagnostic state")
        identifier = digest(proposed)
        if apply:
            if preview_digest != identifier:
                raise WorkflowError("Recovery application requires the unchanged preview digest")
            if current is None:
                atomic_json(
                    diagnostics.state_directory(repo) / FILENAME,
                    {
                        "schema_version": 2,
                        "grant": proposed,
                        "grant_digest": identifier,
                        "attempts": [copy.deepcopy(proposed["historical"]["entry"])],
                    },
                )
        return {
            "status": "applied" if apply else "preview",
            "preview_digest": identifier,
            "contract_digest": CONTRACT_DIGEST,
            "policy": proposed["policy"],
            "slots": proposed["slots"],
            "max_total_attempts": 3,
            "original_attempt_preserved": True,
            "note": "Local operator approval receipt is not cryptographic attestation; no inference performed.",
        }
