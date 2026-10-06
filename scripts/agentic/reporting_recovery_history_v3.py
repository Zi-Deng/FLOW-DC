"""Closed stopped-history bindings for v3; no authentication or inference."""

import hashlib

import packet_tree
import reporting_activation_v2 as old
import reporting_diagnostic_v2 as diagnostic
import review
import review_batch
from reporting_recovery_history import semantics
from tasks import digest
from workflow import WorkflowError

C303_DIRECTORY = "reviews/pr-32-c303c46a8675-05c013fb"
C303_HEAD = "c303c46a8675442a3d7270c00c40ad849d51bb5c"
C303_CONTRACT = "e715ef7690c2b69830491740c2081bbeca88f0e2dd9b5cbc03a873ca4de7fc43"
PUBLIC_SNAPSHOT = "issue31-provider-reconciliation/oct05-report-audit-current-public-feedback.json"
V2_GRANT = "980e8aa6a6fd56ccbec5876d83b31f965b4cf177e78a8d31c08d7af49e662e34"


def tree(directory, *, expected=None, max_files=50000, max_bytes=512_000_000):
    """Hash complete closure with bounded individual reads and manifest allocation."""
    files, total = {}, 0
    for name, path in packet_tree.files(directory):
        if len(files) >= max_files or (expected is not None and name not in expected):
            raise WorkflowError("Stopped history has unexpected material")
        raw = review.exact_reporting_bytes(path, 8_000_000)
        total += len(raw)
        if total > max_bytes:
            raise WorkflowError("Stopped history exceeds its byte bound")
        files[name] = hashlib.sha256(raw).hexdigest()
    if expected is not None and files.keys() != expected:
        raise WorkflowError("Stopped history is missing material")
    return {"files_digest": digest(files), "file_count": len(files), "byte_count": total}


def approval(repo, contract, comment, expected=None):
    state = old.read(repo.main / ".agentic-local/tasks/issue-31.json")
    rows = state.get("approval_history")
    if type(rows) is not list:
        raise WorkflowError("Missing historical approval identity")
    matches = [
        row
        for row in rows
        if type(row) is dict
        and row.get("issue") == 31
        and row.get("plan_comment") == comment
        and digest(row.get("contract")) == contract
        and (expected is None or digest(row) == expected)
    ]
    if len(matches) != 1:
        raise WorkflowError("Historical approval identity changed or is ambiguous")
    return digest(matches[0])


def c303(repo):
    directory = repo.main / ".agentic-local" / C303_DIRECTORY
    meta = review.verify_packet(directory)
    batch = review_batch.load(directory)
    state = review_batch.state_for(directory, batch)
    assessment = review.qualification(directory)
    if (
        meta.get("repository") != repo.name
        or meta.get("pr") != 32
        or meta.get("head_sha") != C303_HEAD
        or meta.get("plan_comment") != 5966428269
        or batch["contract_digest"] != C303_CONTRACT
        or batch["schema_version"] != 6
        or state is None
        or state["stop_reason"] != "execution_incomplete_or_interrupted"
        or len(state["reservations"]) != 20
        or len(batch["units"]) != 139
        or assessment["qualified"]
        or assessment["inspected_count"] != 60
        or assessment["required_count"] != 776
    ):
        raise WorkflowError("Stopped c303 identity or original incomplete result changed")
    historical_approval = approval(repo, C303_CONTRACT, 5966428269)
    snapshot_path = repo.main / ".agentic-local" / PUBLIC_SNAPSHOT
    snapshot = old.read(snapshot_path)
    public = snapshot.get("reviews")
    if type(public) is not list or len(public) > 1000:
        raise WorkflowError("Invalid retained historical publication snapshot")
    bodies = [
        (unit["id"], review.publication_body(directory / "units" / unit["id"]))
        for unit in assessment["units"]
        if "review_sha256" in unit
    ]
    if len(bodies) != 20 or sum(unit["state"] == "complete" for unit in assessment["units"]) != 19:
        raise WorkflowError("Stopped c303 component accounting changed")
    bodies.append(("aggregate", review_batch.publication_body(directory)))
    publications = {}
    for unit, body in bodies:
        hits = [
            row
            for row in public
            if type(row) is dict
            and row.get("body") == body
            and row.get("commit_id") == C303_HEAD
            and row.get("state") == "COMMENTED"
        ]
        if len(hits) != 1:
            raise WorkflowError("Stopped c303 exact publication is missing or ambiguous")
        publications[unit] = {"id": hits[0]["id"], "body_sha256": hashlib.sha256(body.encode()).hexdigest()}
    return {
        **tree(directory),
        "approval_digest": historical_approval,
        "head": C303_HEAD,
        "batch_digest": digest(batch),
        "ledger_digest": digest(state),
        "assessment_digest": digest(assessment),
        "publications": publications,
        "public_snapshot_sha256": review.digest(snapshot_path),
        "qualified": False,
    }


def stopped(repo):
    original = old.historical(repo)
    grant, application = old.load(repo)
    if digest(grant) != V2_GRANT or digest(grant["binding"]["history"]) != digest(original):
        raise WorkflowError("Stopped v2 grant or bound original history changed")
    historical_approval = approval(
        repo,
        old.CONTRACT_DIGEST,
        old.CONTRACT["plan_comment"],
        grant["binding"]["authorization"]["approval_digest"],
    )
    expected = {
        "grant.json",
        "application.json",
        "attempt-12.json",
        "attempt-13.json",
        "outcome-12.json",
        "outcome-13.json",
    }
    outcomes = {}
    for number in (12, 13):
        directory = old.root(repo) / f"evidence-{number}"
        meta = review.verify_packet(directory)
        diagnostic.identity(repo, directory, meta)
        result = old.outcome(repo, number)
        if result["qualified"] != (number == 12):
            raise WorkflowError("Stopped v2 outcome changed")
        capture = review.read_result_artifact(directory, "review-capture.json", meta)
        if number == 13 and "unsupported_partial_stream" not in capture["diagnostics"]["reasons"]:
            raise WorkflowError("Stopped v2 failure evidence changed")
        expected.update(f"evidence-{number}/packet/" + name for name in meta["files"])
        expected.update(
            f"evidence-{number}/" + name
            for name in (
                "metadata.json",
                "attempt.json",
                "reporting-execution.json",
                "reporting-finished.json",
                "review-capture.json",
                "review-result.json",
                "review.md",
                "terminal.txt",
                "diagnostics.json",
                "reporting-proof.json",
                "coverage.json",
            )
        )
        outcomes[str(number)] = result
    return {
        **original,
        "stopped_v2": {
            **tree(old.root(repo), expected=expected, max_files=52, max_bytes=32_000_000),
            "grant_digest": digest(grant),
            "application_digest": digest(application),
            "approval_digest": historical_approval,
            "outcomes": outcomes,
            "policy_semantics": semantics(grant["binding"]["policy"]),
            "state": "stopped-no-retry",
        },
        "stopped_c303": c303(repo),
    }
