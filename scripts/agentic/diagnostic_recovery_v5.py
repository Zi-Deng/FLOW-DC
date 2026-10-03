"""Exactly bound revision-5 prospective slots; preserve the stopped revision-4 grant."""

from __future__ import annotations

import copy
import hashlib

import diagnostic_recovery as prior
import review_coverage as coverage
import review_policy
from tasks import atomic_json, digest, plain_path
from workflow import WorkflowError

CONTRACT = {
    "issue": 33,
    "plan_comment": 5964179523,
    "issue_digest": "5040edb6c15ef83527e801e3712a7693b59a75e8a7b96bd773064e76c75e1621",
    "plan_digest": "b82c3f9610e13929529c1840468c4484eebdc81efd63315be3b175f70922a06f",
}
CONTRACT_DIGEST = "5c8c8c8bc87c2cd02229ea1c0f74b98a9748542fa770fc93178234d73501c45f"
ADAPTER = "claude-stream-json-2.1.282-v3"
FILENAME = "recovery-v5-ledger.json"
SEQUENCE = {3: "native-tools-and-source", 4: "isolation-refusal"}


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
        raise WorkflowError("Revision-5 recovery requires its exact v3 model/effort/CLI/auth/budget policy")


def historical(repo):
    import review_diagnostics as diagnostics

    root = diagnostics.state_directory(repo)
    old = prior.load(repo, later_attempts=True)
    if old is None or len(old["attempts"]) != 2 or any(e["status"] != "incomplete" for e in old["attempts"]):
        raise WorkflowError(
            "Revision 5 requires both original incomplete trials and the stopped revision-4 grant"
        )
    meta = coverage.read_json(plain_path(root / "attempt-2/metadata.json"))
    diagnostics.verify(
        repo,
        old["attempts"][1],
        meta["review_policy"],
        recovery=old,
        require_qualified=False,
        strict_evidence=True,
    )
    files = [root / "ledger.json", root / prior.FILENAME]
    for number in (1, 2):
        directory = plain_path(root / f"attempt-{number}")
        diagnostics.validate_files(directory)
        files.extend(directory.rglob("*"))
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
        "files": hashes,
        "entries": copy.deepcopy(old["attempts"]),
    }


def grant(repo, policy, *, historical_authority=False):
    validate_policy(policy)
    return {
        "schema_version": 2,
        "authorization": authorization(repo, historical=historical_authority),
        "historical": historical(repo),
        "policy": copy.deepcopy(policy),
        "slots": [{"number": n, "purpose": p} for n, p in SEQUENCE.items()],
        "max_total_attempts": 4,
        "max_total_timeout_seconds": 1200,
        "max_total_estimated_usd": 8,
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
    value = coverage.read_json(path)
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "grant", "grant_digest", "attempts"}
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 3
    ):
        raise WorkflowError("Invalid revision-5 recovery ledger")
    saved = value["grant"]
    if (
        not isinstance(saved, dict)
        or type(saved.get("schema_version")) is not int
        or digest(saved) != digest(grant(repo, saved.get("policy", {}), historical_authority=True))
        or value["grant_digest"] != digest(saved)
    ):
        raise WorkflowError("Revision-5 approval, policy or historical snapshot changed")
    attempts = value["attempts"]
    if (
        not isinstance(attempts, list)
        or not 2 <= len(attempts) <= 4
        or attempts[:2] != saved["historical"]["entries"]
    ):
        raise WorkflowError("Revision-5 count or historical identity changed")
    for number, entry in enumerate(attempts[2:], 3):
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
            raise WorkflowError("Invalid revision-5 diagnostic identity or purpose")
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
            raise WorkflowError("Revision-5 packet policy or grant binding changed")
    if {p.name for p in root.glob("attempt-*")} != {f"attempt-{n}" for n in range(1, len(attempts) + 1)}:
        raise WorkflowError("Conflicting or partial revision-5 attempt state")
    if len(attempts) == 4:
        if attempts[2]["status"] != "qualified":
            raise WorkflowError("Revision-5 sequence continued after failure")
        observed = coverage.read_json(root / "attempt-3/metadata.json")["review_policy"]
        diagnostics.verify(repo, attempts[2], observed, recovery=value)
    return value


def next_slot(repo, state, policy):
    import review_diagnostics as diagnostics

    if authorization(repo) != state["grant"]["authorization"]:
        raise WorkflowError("Current approval cannot authorize this revision-5 grant")
    review_policy.require_current_adapter(policy)
    validate_policy(policy)
    prior.match_policy(state["grant"]["policy"], policy)
    attempts = state["attempts"]
    if len(attempts) >= 4:
        raise WorkflowError("All four counted attempts exhausted; no fifth call")
    if len(attempts) == 3:
        if attempts[2]["status"] != "qualified":
            raise WorkflowError("Revision-5 recovery stops after future failure or interruption")
        diagnostics.verify(repo, attempts[2], policy, recovery=state)
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
            }:
                raise WorkflowError("Conflicting or partial revision-5 migration")
        selected = claude_native_auth.bind(selected)
        validate_policy(selected)
        if current is not None:
            proposed = current["grant"]
            if proposed["authorization"] != authorization(repo):
                raise WorkflowError("Current approval differs from the existing revision-5 grant")
            prior.match_policy(proposed["policy"], selected)
        else:
            proposed = grant(repo, selected)
        identifier = digest(proposed)
        if apply:
            if preview_digest != identifier:
                raise WorkflowError("Revision-5 application requires the unchanged preview digest")
            if current is None:
                atomic_json(
                    diagnostics.state_directory(repo) / FILENAME,
                    {
                        "schema_version": 3,
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
            "max_total_attempts": 4,
            "max_total_timeout_seconds": 1200,
            "max_total_estimated_usd": 8,
            "max_prospective_timeout_seconds": 600,
            "max_prospective_estimated_usd": 4,
            "extra_spend_authorized_usd": 0,
            "original_attempts_preserved": True,
            "note": "Owner-writable operator receipt; not attestation or billing guarantee. No inference performed.",
        }
