"""Pinned stopped v3 closure and original evaluations; no auth or inference."""

import hashlib

import reporting_activation_v3 as old
import reporting_diagnostic_v3 as diagnostic
import review
from reporting_recovery_history_v3 import approval, tree
from tasks import digest
from workflow import WorkflowError

V3_GRANT = "e3c9b3c2c6146643dd8305b0f7d16e365ceaaf3e03e2a5eb371308cda4c63746"
V3_TREE = {
    "files_digest": "7cc3946df754dc134edad35bcce68e5b13bd093a59a40dddb55b12ae62b2b0a5",
    "file_count": 52,
    "byte_count": 390735,
}
PUBLIC_SNAPSHOT = "issue31-provider-reconciliation/oct06-reporting-phase18-current-public-feedback.json"
PUBLIC_COMMENT = 6009644511
PUBLIC_BODY_SHA256 = "72de3a611f7b25695116c164259fa9388be19ed6fcb60b0329af52c133bc52af"


def stopped(repo):
    # Compare original closed history BEFORE a preview can bind a changed tree.
    closure = tree(old.root(repo), max_files=52, max_bytes=32_000_000)
    if digest(closure) != digest(V3_TREE):
        raise WorkflowError("Stopped v3 original history closure changed")
    original = old.historical(repo)
    grant, application = old.load(repo)
    if digest(grant) != V3_GRANT or digest(grant["binding"]["history"]) != digest(original):
        raise WorkflowError("Stopped v3 grant or original history changed")
    historical_approval = approval(
        repo,
        old.CONTRACT_DIGEST,
        old.CONTRACT["plan_comment"],
        grant["binding"]["authorization"]["approval_digest"],
    )
    snapshot = old.read(repo.main / ".agentic-local" / PUBLIC_SNAPSHOT)
    rows = snapshot.get("conversation")
    if type(rows) is not list or len(rows) > 1000:
        raise WorkflowError("Missing stopped v3 public response")
    matches = [row for row in rows if type(row) is dict and row.get("id") == PUBLIC_COMMENT]
    if (
        len(matches) != 1
        or type(matches[0].get("body")) is not str
        or hashlib.sha256(matches[0]["body"].encode()).hexdigest() != PUBLIC_BODY_SHA256
    ):
        raise WorkflowError("Stopped v3 whole public response changed")
    outcomes = {}
    for number in (14, 15):
        directory = old.root(repo) / f"evidence-{number}"
        meta = review.verify_packet(directory)
        diagnostic.identity(repo, directory, meta)
        value = old.outcome(repo, number)
        if value["qualified"] != (number == 14):
            raise WorkflowError("Stopped v3 original outcome changed")
        capture = review.read_result_artifact(directory, "review-capture.json", meta)
        if number == 15 and "unsupported_partial_stream" not in capture["diagnostics"]["reasons"]:
            raise WorkflowError("Stopped v3 original failure changed")
        outcomes[str(number)] = value
    return {
        **original,
        "stopped_v3": {
            **closure,
            "grant_digest": digest(grant),
            "application_digest": digest(application),
            "approval_digest": historical_approval,
            "outcomes": outcomes,
            "public_comment": PUBLIC_COMMENT,
            "public_body_sha256": PUBLIC_BODY_SHA256,
            "state": "stopped-no-retry",
        },
    }
