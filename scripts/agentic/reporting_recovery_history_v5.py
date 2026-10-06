"""Pinned uncertain v4 reservation; storage only, never inferred zero usage."""

import hashlib

import reporting_activation_v4 as old
import reporting_diagnostic_v4 as diagnostic
import review
from reporting_recovery_history import semantics
from reporting_recovery_history_v3 import approval, tree
from tasks import digest
from workflow import WorkflowError

V4_GRANT = "2da166b2d0158a3806743a8130483d6e45511ca883c726f2c671fa14f07fa588"
V4_APPROVAL = "65ea7d798151f777a1e6682d7cda05f14b6722bd0c36f455febaabe8d7f2023d"
V4_TREE = {
    "files_digest": "c692fad8fd149597d3ca86ad03704c2357567f4bc3168b8869ffd2ad48ccac9e",
    "file_count": 16,
    "byte_count": 67578,
}
PUBLIC_SNAPSHOT = (
    "issue31-provider-reconciliation/oct06-reporting-phase25-planning-current-public-feedback.json"
)
PUBLIC_COMMENT = 6010351406
PUBLIC_BODY_SHA256 = "387b826ce51da85cf1727711667dedd7d99628cf0a733586fb5035532cd2d6a7"


def stopped(repo):
    # Pin the original closed state before allowing a new preview to bind it.
    directory = old.root(repo)
    closure = tree(directory, max_files=16, max_bytes=2_000_000)
    if digest(closure) != digest(V4_TREE):
        raise WorkflowError("Stopped v4 original history closure changed")
    original = old.historical(repo)
    grant, application = old.load(repo)
    if digest(grant) != V4_GRANT or digest(grant["binding"]["history"]) != digest(original):
        raise WorkflowError("Stopped v4 grant or original history changed")
    historical_approval = approval(repo, old.CONTRACT_DIGEST, old.CONTRACT["plan_comment"], V4_APPROVAL)
    approvals = old.read(repo.main / ".agentic-local/tasks/issue-31.json")["approval_history"]
    if (
        sum(
            type(row) is dict and row.get("plan_comment") == old.CONTRACT["plan_comment"] for row in approvals
        )
        != 1
    ):
        raise WorkflowError("Stopped v4 historical approval is ambiguous")
    if grant["binding"]["authorization"]["approval_digest"] != historical_approval:
        raise WorkflowError("Stopped v4 original approval changed")
    snapshot = old.read(repo.main / ".agentic-local" / PUBLIC_SNAPSHOT)
    rows = snapshot.get("conversation")
    if type(rows) is not list or len(rows) > 1000:
        raise WorkflowError("Missing stopped v4 public response")
    matches = [row for row in rows if type(row) is dict and row.get("id") == PUBLIC_COMMENT]
    if (
        len(matches) != 1
        or type(matches[0].get("body")) is not str
        or hashlib.sha256(matches[0]["body"].encode()).hexdigest() != PUBLIC_BODY_SHA256
    ):
        raise WorkflowError("Stopped v4 whole public response changed")
    evidence = directory / "evidence-16"
    meta = review.verify_packet(evidence)
    diagnostic.identity(repo, evidence, meta)
    input_digest = digest({k: v for k, v in meta.items() if k not in review.RESULT_FIELDS})
    reservation = old.read(directory / "attempt-16.json")
    started = old.clock(reservation.get("started"))
    expected = {
        "schema_version": 4,
        "number": 16,
        "purpose": "isolation-refusal",
        "grant_digest": digest(grant),
        "input_digest": input_digest,
        "started": started,
        "deadline": started + 300,
        "reserved_seconds": 300,
        "reserved_reference_usd": 2,
        "wrapper_processes": 1,
    }
    if (
        digest(reservation) != digest(expected)
        or not application["applied_at"] <= started <= grant["expires_at"] - 300
    ):
        raise WorkflowError("Stopped v4 uncertain reservation changed")
    expected_paths = {"grant.json", "application.json", "attempt-16.json", "evidence-16/metadata.json"}
    expected_paths.update("evidence-16/packet/" + name for name in meta["files"])
    # No execution/capture/completion/outcome or purpose17 may be synthesized.
    if tree(directory, expected=expected_paths, max_files=16, max_bytes=2_000_000) != closure:
        raise WorkflowError("Stopped v4 closure changed while reading")
    return {
        **original,
        "stopped_v4": {
            **closure,
            "grant_digest": digest(grant),
            "application_digest": digest(application),
            "approval_digest": historical_approval,
            "reservation_digest": digest(reservation),
            "input_digest": input_digest,
            "public_comment": PUBLIC_COMMENT,
            "public_body_sha256": PUBLIC_BODY_SHA256,
            "policy_semantics": semantics(grant["binding"]["policy"]),
            "state": "consumed-uncertain-no-retry",
            "usage": "unknown",
            "unavailable": [17],
        },
    }
