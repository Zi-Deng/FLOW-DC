"""Current reporting admission, separate from immutable historical assessment.

This replays retained native execution evidence. Owner-writable local records are
not attestations; neither a synthetic fixture nor a saved success flag is proof
of an actual provider call. No inference or credential renewal occurs here.
"""

from __future__ import annotations

import copy
from pathlib import Path

import claude_native_auth
import claude_reporting_policy
import reporting_activation as activation
import reporting_diagnostic as diagnostic
from tasks import digest
from workflow import WorkflowError, run

FILENAME = "reporting-admission.json"


def source_continuity(repo, original, current):
    """A human-merged harness may move commits, never change qualified bytes."""
    if digest(original) == digest(current):
        return
    if digest(original.get("files")) != digest(current.get("files")):
        raise WorkflowError("Reporting harness source changed; new qualification is required")
    # A coincidentally identical tree on an unrelated branch is insufficient.
    # This exception is for adoption of this exact implementation after merge.
    pr = repo.api("pulls/32")
    merged = pr.get("merge_commit_sha")
    if (
        pr.get("merged") is not True
        or pr.get("head", {}).get("sha") != original["head"]
        or pr.get("base", {}).get("repo", {}).get("full_name") != repo.name
        or not isinstance(merged, str)
        or len(merged) != 40
        or any(c not in "0123456789abcdef" for c in merged)
        or run(
            ["git", "-C", repo.root, "merge-base", "--is-ancestor", merged, current["head"]],
            check=False,
        ).returncode
    ):
        raise WorkflowError("Reporting harness commit lacks verified merged-source continuity")


def check(repo, policy, *, owned_auth=None):
    """Re-evaluate both purposes and all immutable bindings without dispatch."""
    import review

    claude_reporting_policy.validate(policy)
    if not activation.root(repo).exists():
        raise WorkflowError("Missing separate reporting activation; both actual purposes are required")
    claude_native_auth.validate_binding(policy["authentication"])
    grant, _ = activation.load(repo)
    bound = grant["binding"]
    if digest(activation.authorization(repo)) != digest(bound["authorization"]):
        raise WorkflowError("Reporting activation approval changed")
    if digest(activation.historical(repo)) != digest(bound["history"]):
        raise WorkflowError("Original diagnostic history changed")
    source_continuity(repo, bound["harness"], activation.harness())
    observed, requested = copy.deepcopy(bound["policy"]), copy.deepcopy(policy)
    old_auth, new_auth = observed.pop("authentication"), requested.pop("authentication")
    # Per-review time/reference allocations are independently enforced by the
    # immutable review policy and batch grant. Reporting/retention/turn limits,
    # model, effort, tool surface and every other policy field must match.
    observed.pop("budget")
    requested.pop("budget")
    lineage = claude_native_auth.capability_lineage
    if owned_auth is not None:
        from claude_owned_auth import require

        lineage = require(owned_auth).capability_lineage
    if digest(observed) != digest(requested) or not lineage(
        old_auth, new_auth, policy["budget"]["timeout_seconds"]
    ):
        raise WorkflowError("Reporting policy or verified same-account capability lineage differs")
    outcomes = {}
    for number in (10, 11):
        directory = activation.root(repo) / f"evidence-{number}"
        meta = review.verify_packet(directory)
        diagnostic.identity(repo, directory, meta)
        capture = review.read_result_artifact(directory, "review-capture.json", meta)
        reservation = activation.read(activation.root(repo) / f"attempt-{number}.json")
        _, timing = diagnostic.completion(directory, capture, reservation)
        result = activation.outcome(repo, number)
        if not timing or not result["qualified"]:
            raise WorkflowError("Both actual reporting purposes must qualify independently")
        outcomes[str(number)] = digest(result)
    return {
        "schema_version": 1,
        "grant_digest": digest(grant),
        "policy_digest": digest(policy),
        "harness": copy.deepcopy(bound["harness"]),
        "outcomes": outcomes,
        "observed_authentication": old_auth,
        "current_authentication": new_auth,
        "current_generation_live_tested": digest(old_auth) == digest(new_auth),
    }


def retain(repo, directory, meta):
    from claude_reporting_execution import exclusive

    record = check(repo, meta["review_policy"])
    exclusive(Path(directory) / FILENAME, record, limit=activation.MAX_RECORD_BYTES)
    return record


def require_packet(directory, meta, *, repo=None, owned_auth=None):
    """Current readiness needs dispatch admission, not just storage qualification."""
    from claude_reporting_execution import retained
    from workflow import Repo

    if "reporting_activation" in meta:
        raise WorkflowError("Reporting diagnostics cannot establish PR readiness")
    if not (Path(directory) / FILENAME).exists():
        raise WorkflowError("Missing reporting dispatch admission; storage-only evidence is not ready")
    record = activation.read(Path(directory) / FILENAME)
    repo = repo or Repo(directory)
    if repo.name != meta["repository"] or digest(record) != digest(
        check(repo, meta["review_policy"], **({"owned_auth": owned_auth} if owned_auth is not None else {}))
    ):
        raise WorkflowError("Reporting admission or current evidence changed")
    execution = retained(directory, meta)
    if execution is None or execution.get("admission_digest") != digest(record):
        raise WorkflowError("Reporting execution lacks its admitted activation binding")
    return record
