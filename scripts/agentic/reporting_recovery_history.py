"""Read-only identity of the stopped v1 authority; unknown usage is never zero."""

import copy
import hashlib

import claude_reporting_execution as execution
import packet_tree
import reporting_activation as old
import reporting_diagnostic as diagnostic
import review
from tasks import digest
from workflow import WorkflowError


def semantics(policy):
    value = copy.deepcopy(policy)
    value.pop("authentication")
    return digest(value)


def stopped(repo, *, original=None):
    grant, application = old.load(repo)  # No current approval or credentials needed.
    original = old.historical(repo) if original is None else original
    if digest(original) != digest(grant["binding"]["history"]):
        raise WorkflowError("Original diagnostic history differs from the stopped grant")
    state = old.read(repo.main / ".agentic-local/tasks/issue-31.json")
    history = state.get("approval_history")
    if type(history) is not list:
        raise WorkflowError("Missing historical reporting approval identity")
    matches = [
        approval
        for approval in history
        if type(approval) is dict
        and approval.get("issue") == 31
        and approval.get("plan_comment") == old.CONTRACT["plan_comment"]
        and digest(approval.get("contract")) == old.CONTRACT_DIGEST
        and digest(approval) == grant["binding"]["authorization"].get("approval_digest")
    ]
    if state.get("repository") != repo.name or state.get("key") != "issue-31" or len(matches) != 1:
        raise WorkflowError("Stopped reporting grant lacks its exact historical approval")
    directory = old.root(repo) / "evidence-10"
    meta = review.verify_packet(directory)
    diagnostic.identity(repo, directory, meta)
    reservation = old.read(old.root(repo) / "attempt-10.json")
    started = old.clock(reservation.get("started"))
    expected = {
        "schema_version": 1,
        "number": 10,
        "purpose": old.SEQUENCE[10],
        "grant_digest": digest(grant),
        "input_digest": digest(meta),
        "started": started,
        "deadline": started + 300,
        "reserved_seconds": 300,
        "reserved_reference_usd": 2,
        "wrapper_processes": 1,
    }
    record = execution.retained(directory, meta)
    attempt = old.read(directory / "attempt.json")
    if (
        digest(reservation) != digest(expected)
        or not application["applied_at"] <= started <= grant["expires_at"] - 300
        or record is None
        or record.get("schema_version") != 2
        or record.get("reservation_digest") != digest(reservation)
        or digest(attempt)
        != digest(
            {
                "schema_version": 7,
                "input_digest": digest(meta),
                "policy_digest": digest(meta["review_policy"]),
                "requests": 1,
                "status": "started",
                "execution_sha256": digest(record),
            }
        )
    ):
        raise WorkflowError("Stopped reporting reservation or execution identity changed")
    expected_files = {
        "grant.json",
        "application.json",
        "attempt-10.json",
        "evidence-10/metadata.json",
        "evidence-10/attempt.json",
        "evidence-10/reporting-execution.json",
        *("evidence-10/packet/" + name for name in meta["files"]),
    }
    files = {}
    total = 0
    for name, path in packet_tree.files(old.root(repo)):
        # This amendment binds the exact uncertain first call, not a completed
        # or substituted predecessor. Unexpected outcomes/second calls refuse.
        if name not in expected_files:
            raise WorkflowError("Stopped reporting history has additional or completed evidence")
        raw = review.exact_reporting_bytes(path, old.MAX_RECORD_BYTES)
        total += len(raw)
        if total > 16_000_000:
            raise WorkflowError("Stopped reporting history byte bound exceeded")
        files[name] = hashlib.sha256(raw).hexdigest()
    if set(files) != expected_files:
        raise WorkflowError("Stopped reporting history is missing evidence")
    return {
        "grant_digest": digest(grant),
        "approval_digest": digest(matches[0]),
        "files": files,
        "policy_semantics": semantics(grant["binding"]["policy"]),
        "attempted": 10,
        "unavailable": 11,
        "reserved_seconds": 300,
        "reserved_reference_usd": 2,
        "reserved_wrapper_processes": 1,
        "usage": "unknown",
        "state": "consumed-uncertain-no-retry",
    }
