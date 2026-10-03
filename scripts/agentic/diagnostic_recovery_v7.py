"""Exactly bound revision-7 prospective slots; preserve the stopped revision-6 grant."""

from __future__ import annotations

import copy
import hashlib

import diagnostic_recovery as prior
import diagnostic_recovery_v5 as older
import diagnostic_recovery_v6 as stopped
import review_coverage as coverage
import review_policy
from tasks import atomic_json, digest, plain_path
from workflow import WorkflowError

CONTRACT = {
    "issue": 33,
    "plan_comment": 5965161662,
    "issue_digest": "61045bd2bf6b675c9629ca511a19122aee659a6bfc8764b46391ad1d04d92d3d",
    "plan_digest": "2b1b9437bd1d665c930c079cc3e30655bca3c68a1f332b43b66142696b0b6137",
}
CONTRACT_DIGEST = "8c342fdbb9093d5b310bfe10854910dbf1e66e66527ac1a2514b7a90d615589b"
ADAPTER = "claude-stream-json-2.1.282-v5"
FILENAME = "recovery-v7-ledger.json"
MAX_HISTORY_FILES = 256
MAX_HISTORY_BYTES = 64_000_000
MAX_LEDGER_BYTES = 16_000_000
SEQUENCE = {6: "native-tools-and-source", 7: "isolation-refusal"}


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
        raise WorkflowError("Revision-7 recovery requires its exact v5 model/effort/CLI/auth/budget policy")


def historical_files(root):
    # Check bounded history before frozen readers open any of its contents.
    files = [root / "ledger.json", root / prior.FILENAME, root / older.FILENAME, root / stopped.FILENAME]
    total = 0
    for number in (1, 2, 3, 4, 5):
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
    ]:
        raise WorkflowError(
            "Revision 7 requires five original-status trials and the stopped revision-6 grant"
        )
    for number in (4, 5):
        meta = coverage.read_json(plain_path(root / f"attempt-{number}/metadata.json"))
        diagnostics.verify(
            repo,
            old["attempts"][number - 1],
            meta["review_policy"],
            recovery=old,
            require_qualified=number == 4,
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
        "recovery_v6_ledger_text": (root / stopped.FILENAME).read_bytes().decode("utf-8"),
        "files": hashes,
        "entries": copy.deepcopy(old["attempts"]),
    }


def grant(repo, policy, *, historical_authority=False):
    validate_policy(policy)
    return {
        "schema_version": 5,
        "authorization": authorization(repo, historical=historical_authority),
        "historical": historical(repo),
        "policy": copy.deepcopy(policy),
        "slots": [{"number": n, "purpose": p} for n, p in SEQUENCE.items()],
        "max_total_attempts": 7,
        "max_total_timeout_seconds": 2100,
        "max_total_estimated_usd": 14,
        "max_prospective_timeout_seconds": 600,
        "max_prospective_estimated_usd": 4,
        "extra_spend_authorized_usd": 0,
        "stop_on_future_failure": True,
    }


def load(repo, *, later_attempts=False):
    import review_diagnostics as diagnostics

    root = diagnostics.state_directory(repo)
    path = plain_path(root / FILENAME)
    if not path.exists():
        return None
    if path.stat().st_size > MAX_LEDGER_BYTES:
        raise WorkflowError("Revision-7 recovery ledger exceeds its byte bound")
    value = coverage.read_json(path)
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "grant", "grant_digest", "attempts"}
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 6
    ):
        raise WorkflowError("Invalid revision-7 recovery ledger")
    saved = value["grant"]
    if (
        not isinstance(saved, dict)
        or type(saved.get("schema_version")) is not int
        or digest(saved) != digest(grant(repo, saved.get("policy", {}), historical_authority=True))
        or value["grant_digest"] != digest(saved)
    ):
        raise WorkflowError("Revision-7 approval, policy or historical snapshot changed")
    attempts = value["attempts"]
    if (
        not isinstance(attempts, list)
        or not 5 <= len(attempts) <= 7
        or attempts[:5] != saved["historical"]["entries"]
    ):
        raise WorkflowError("Revision-7 count or historical identity changed")
    for number, entry in enumerate(attempts[5:], 6):
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
            raise WorkflowError("Invalid revision-7 diagnostic identity or purpose")
        directory = plain_path(root / f"attempt-{number}")
        diagnostics.validate_files(directory)
        meta = coverage.read_json(directory / "metadata.json")
        validate_policy(meta["review_policy"])
        prior.match_recorded_policy(saved["policy"], meta["review_policy"])
        if entry["policy_digest"] != digest(meta["review_policy"]) or meta.get("recovery") != {
            "grant_digest": value["grant_digest"],
            "number": number,
            "purpose": SEQUENCE[number],
        }:
            raise WorkflowError("Revision-7 packet policy or grant binding changed")
    expected = {f"attempt-{n}" for n in range(1, len(attempts) + 1)}
    actual = {p.name for p in root.glob("attempt-*")}
    allowed = expected | {"attempt-8", "attempt-9"} if later_attempts else expected
    if not expected <= actual <= allowed:
        raise WorkflowError("Conflicting or partial revision-7 attempt state")
    if len(attempts) == 7:
        if attempts[5]["status"] != "qualified":
            raise WorkflowError("Revision-7 sequence continued after failure")
        observed = coverage.read_json(root / "attempt-6/metadata.json")["review_policy"]
        diagnostics.verify(repo, attempts[5], observed, recovery=value)
    return value


def next_slot(repo, state, policy):
    import review_diagnostics as diagnostics

    if authorization(repo) != state["grant"]["authorization"]:
        raise WorkflowError("Current approval cannot authorize this revision-7 grant")
    review_policy.require_current_adapter(policy)
    validate_policy(policy)
    prior.match_policy(state["grant"]["policy"], policy)
    attempts = state["attempts"]
    if len(attempts) >= 7:
        raise WorkflowError("All seven counted attempts exhausted; no eighth call")
    if len(attempts) == 6:
        if attempts[5]["status"] != "qualified":
            raise WorkflowError("Revision-7 recovery stops after future failure or interruption")
        diagnostics.verify(repo, attempts[5], policy, recovery=state)
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
            }:
                raise WorkflowError("Conflicting or partial revision-7 migration")
        selected = claude_native_auth.bind(selected)
        validate_policy(selected)
        if current is not None:
            proposed = current["grant"]
            if proposed["authorization"] != authorization(repo):
                raise WorkflowError("Current approval differs from the existing revision-7 grant")
            prior.match_policy(proposed["policy"], selected)
        else:
            proposed = grant(repo, selected)
        identifier = digest(proposed)
        if apply:
            if preview_digest != identifier:
                raise WorkflowError("Revision-7 application requires the unchanged preview digest")
            if current is None:
                atomic_json(
                    diagnostics.state_directory(repo) / FILENAME,
                    {
                        "schema_version": 6,
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
            "max_total_attempts": 7,
            "max_total_timeout_seconds": 2100,
            "max_total_estimated_usd": 14,
            "max_prospective_timeout_seconds": 600,
            "max_prospective_estimated_usd": 4,
            "extra_spend_authorized_usd": 0,
            "original_attempts_preserved": True,
            "note": "Owner-writable operator receipt; not attestation or billing guarantee. No inference performed.",
        }
