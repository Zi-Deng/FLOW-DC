"""Exactly bound revision-8 prospective slots; preserve the stopped revision-7 grant."""

from __future__ import annotations

import copy
import hashlib

import diagnostic_recovery as prior
import diagnostic_recovery_v5 as older
import diagnostic_recovery_v6 as previous
import diagnostic_recovery_v7 as stopped
import diagnostic_tool_contract as tool_contract
import review_coverage as coverage
import review_policy
from tasks import atomic_json, digest, plain_path
from workflow import WorkflowError

CONTRACT = {
    "issue": 33,
    "plan_comment": 5965755308,
    "issue_digest": "2cb85f8a0db7f58d5929e49acbeec955f395f4f9f05c91f1848fc4f0c7af8207",
    "plan_digest": "d744a1e290ce55b459c75f9edb0466d59dc0f6e91173572091cb8bd458b3d9b1",
}
CONTRACT_DIGEST = "dd3a54615f7db9cd099b083df98c33ca3a525552a5a591d20e7dc68b6977191e"
ADAPTER = "claude-stream-json-2.1.282-v6"
FILENAME = "recovery-v8-ledger.json"
MAX_HISTORY_FILES = 256
MAX_HISTORY_BYTES = 64_000_000
MAX_LEDGER_BYTES = 16_000_000
SEQUENCE = {8: "native-tools-and-source", 9: "isolation-refusal"}


def authorization(repo, *, historical=False):
    return prior.authorization(
        repo, historical=historical, contract=CONTRACT, contract_digest=CONTRACT_DIGEST
    )


def validate_policy(policy):
    import claude_native_auth

    review_policy.validate_policy(policy)
    claude_native_auth.validate_binding(policy.get("authentication"))
    expected = review_policy.policy(
        review_policy.choices("claude-code", "claude-opus-5-5", "medium"), {}, diagnostic=True
    )
    expected["adapter"] = ADAPTER
    expected["authentication"] = policy["authentication"]
    if policy != expected:
        raise WorkflowError("Revision-8 recovery requires its exact v6 model/effort/CLI/auth/budget policy")


def historical_files(root):
    # Check bounded history before frozen readers open any of its contents.
    files = [
        root / "ledger.json",
        root / prior.FILENAME,
        root / older.FILENAME,
        root / previous.FILENAME,
        root / stopped.FILENAME,
    ]
    total = 0
    for number in range(1, 8):
        directory = plain_path(root / f"attempt-{number}")
        if not directory.is_dir():
            raise WorkflowError("Missing historical diagnostic directory")
        for path in directory.rglob("*"):
            plain_path(path)
            if not path.is_file() and not path.is_dir():
                raise WorkflowError("Unsafe historical recovery evidence")
            files.append(path)
            if len(files) > MAX_HISTORY_FILES:
                raise WorkflowError("Historical recovery file count exceeds its bound")
    for path in files:
        plain_path(path)
        if path.is_file():
            size = path.stat().st_size
            total += size
            if total > MAX_HISTORY_BYTES or (path.parent == root and size > MAX_LEDGER_BYTES):
                raise WorkflowError("Historical recovery evidence exceeds its byte bound")
    return files


def historical(repo):
    import review_diagnostics as diagnostics

    root = diagnostics.state_directory(repo)
    files = historical_files(root)
    old = stopped.load(repo, later_attempts=True)
    if old is None or [e["status"] for e in old["attempts"]] != [
        "incomplete",
        "incomplete",
        "incomplete",
        "qualified",
        "incomplete",
        "qualified",
        "incomplete",
    ]:
        raise WorkflowError(
            "Revision 8 requires seven original-status trials and the stopped revision-7 grant"
        )
    for number in (6, 7):
        meta = coverage.read_json(plain_path(root / f"attempt-{number}/metadata.json"))
        diagnostics.verify(
            repo,
            old["attempts"][number - 1],
            meta["review_policy"],
            recovery=old,
            require_qualified=number == 6,
            strict_evidence=True,
        )
    hashes = {}
    for path in files:
        plain_path(path)
        if path.is_file():
            hashes[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
        elif not path.is_dir():
            raise WorkflowError("Unsafe historical recovery evidence")
    return {
        "ledger_text": (root / "ledger.json").read_bytes().decode("utf-8"),
        "recovery_ledger_text": (root / prior.FILENAME).read_bytes().decode("utf-8"),
        "recovery_v5_ledger_text": (root / older.FILENAME).read_bytes().decode("utf-8"),
        "recovery_v6_ledger_text": (root / previous.FILENAME).read_bytes().decode("utf-8"),
        "recovery_v7_ledger_text": (root / stopped.FILENAME).read_bytes().decode("utf-8"),
        "files": hashes,
        "entries": copy.deepcopy(old["attempts"]),
    }


def grant(repo, policy, *, historical_authority=False):
    validate_policy(policy)
    return {
        "schema_version": 6,
        "authorization": authorization(repo, historical=historical_authority),
        "historical": historical(repo),
        "policy": copy.deepcopy(policy),
        "diagnostic_tool_contract": tool_contract.contract(),
        "slots": [{"number": n, "purpose": p} for n, p in SEQUENCE.items()],
        "max_total_attempts": 9,
        "max_total_timeout_seconds": 2700,
        "max_total_estimated_usd": 18,
        "max_prospective_timeout_seconds": 600,
        "max_prospective_estimated_usd": 4,
        "extra_spend_authorized_usd": 0,
        "stop_on_future_failure": True,
    }


def load(repo):
    import review_diagnostics as diagnostics

    root = diagnostics.state_directory(repo)
    path = plain_path(root / FILENAME)
    if not path.exists():
        return None
    if path.stat().st_size > MAX_LEDGER_BYTES:
        raise WorkflowError("Revision-8 recovery ledger exceeds its byte bound")
    value = coverage.read_json(path)
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "grant", "grant_digest", "attempts"}
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 7
    ):
        raise WorkflowError("Invalid revision-8 recovery ledger")
    saved = value["grant"]
    if (
        not isinstance(saved, dict)
        or type(saved.get("schema_version")) is not int
        or digest(saved) != digest(grant(repo, saved.get("policy", {}), historical_authority=True))
        or value["grant_digest"] != digest(saved)
    ):
        raise WorkflowError("Revision-8 approval, policy or historical snapshot changed")
    attempts = value["attempts"]
    if (
        not isinstance(attempts, list)
        or not 7 <= len(attempts) <= 9
        or attempts[:7] != saved["historical"]["entries"]
    ):
        raise WorkflowError("Revision-8 count or historical identity changed")
    for number, entry in enumerate(attempts[7:], 8):
        fields = {"number", "purpose", "status", "policy_digest", "grant_digest"}
        if (
            not isinstance(entry, dict)
            or set(entry) not in (fields, fields | {"capture_digest", "assessment_digest"})
            or type(entry.get("number")) is not int
            or entry["number"] != number
            or entry.get("purpose") != SEQUENCE[number]
            or entry.get("grant_digest") != value["grant_digest"]
            or entry.get("status") not in {"attempted", "incomplete", "qualified"}
        ):
            raise WorkflowError("Invalid revision-8 diagnostic identity or purpose")
        directory = plain_path(root / f"attempt-{number}")
        diagnostics.validate_files(directory)
        meta = coverage.read_json(directory / "metadata.json")
        validate_policy(meta["review_policy"])
        tool_contract.validate_meta(meta)
        prior.match_recorded_policy(saved["policy"], meta["review_policy"])
        if entry["policy_digest"] != digest(meta["review_policy"]) or meta.get("recovery") != {
            "grant_digest": value["grant_digest"],
            "number": number,
            "purpose": SEQUENCE[number],
        }:
            raise WorkflowError("Revision-8 packet policy or grant binding changed")
    expected = {f"attempt-{n}" for n in range(1, len(attempts) + 1)}
    actual = {p.name for p in root.glob("attempt-*")}
    allowed = expected
    if not expected <= actual <= allowed:
        raise WorkflowError("Conflicting or partial revision-8 attempt state")
    if len(attempts) == 9 and attempts[7]["status"] != "qualified":
        raise WorkflowError("Revision-8 sequence continued after failure")
    if len(attempts) >= 8 and attempts[7]["status"] == "qualified":
        observed = coverage.read_json(root / "attempt-8/metadata.json")["review_policy"]
        diagnostics.verify(repo, attempts[7], observed, recovery=value)
    return value


def next_slot(repo, state, policy):
    import review_diagnostics as diagnostics

    if authorization(repo) != state["grant"]["authorization"]:
        raise WorkflowError("Current approval cannot authorize this revision-8 grant")
    review_policy.require_current_adapter(policy)
    validate_policy(policy)
    prior.match_policy(state["grant"]["policy"], policy)
    attempts = state["attempts"]
    if len(attempts) >= 9:
        raise WorkflowError("All nine counted attempts exhausted; no tenth call")
    if len(attempts) == 8:
        if attempts[7]["status"] != "qualified":
            raise WorkflowError("Revision-8 recovery stops after future failure or interruption")
        diagnostics.verify(repo, attempts[7], policy, recovery=state)
    return len(attempts) + 1, SEQUENCE[len(attempts) + 1]


def prepare(repo, cfg, *, apply=False, preview_digest=None):
    import claude_native_auth
    import review_diagnostics as diagnostics

    repo.assert_main()
    authorization(repo)  # Historical authority must never authorize a fresh grant.
    selected = review_policy.resolve(repo, cfg, review_provider="claude-code")["policy"]
    review_policy.require_current_adapter(selected)
    selected["budget"] = review_policy.budget("claude-code", {}, diagnostic=True)
    with diagnostics.locked(repo):
        current = load(repo)
        if current is None:
            historical(repo)  # Reject unbound/missing prior evidence before auth reads.
            if {p.name for p in diagnostics.state_directory(repo).glob("attempt-*")} != {
                "attempt-1",
                "attempt-2",
                "attempt-3",
                "attempt-4",
                "attempt-5",
                "attempt-6",
                "attempt-7",
            }:
                raise WorkflowError("Conflicting or partial revision-8 migration")
        selected = claude_native_auth.bind(selected)
        validate_policy(selected)
        if current is not None:
            proposed = current["grant"]
            if proposed["authorization"] != authorization(repo):
                raise WorkflowError("Current approval differs from the existing revision-8 grant")
            prior.match_policy(proposed["policy"], selected)
        else:
            proposed = grant(repo, selected)
        identifier = digest(proposed)
        if apply:
            if preview_digest != identifier:
                raise WorkflowError("Revision-8 application requires the unchanged preview digest")
            if current is None:
                atomic_json(
                    diagnostics.state_directory(repo) / FILENAME,
                    {
                        "schema_version": 7,
                        "grant": proposed,
                        "grant_digest": identifier,
                        "attempts": copy.deepcopy(proposed["historical"]["entries"]),
                    },
                )
        return {
            "status": "applied" if apply else "preview",
            "preview_digest": identifier,
            "contract_digest": CONTRACT_DIGEST,
            "policy": proposed["policy"],
            "slots": proposed["slots"],
            "max_total_attempts": 9,
            "max_total_timeout_seconds": 2700,
            "max_total_estimated_usd": 18,
            "max_prospective_timeout_seconds": 600,
            "max_prospective_estimated_usd": 4,
            "extra_spend_authorized_usd": 0,
            "original_attempts_preserved": True,
            "note": "Owner-writable operator receipt; not attestation or billing guarantee. No inference performed.",
        }
