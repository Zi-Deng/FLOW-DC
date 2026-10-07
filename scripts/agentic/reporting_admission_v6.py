"""V6 capability and empirical evidence; no ordinary or batch consumer yet."""

import copy

import claude_owned_auth
import reporting_activation_v6 as activation
import reporting_diagnostic_v6 as diagnostic
import review
import review_capacity_native_v1 as capacity
from tasks import digest
from workflow import WorkflowError


def check(repo, *, owned_auth, capacity_required=False):
    """Recompute actual outcomes and current same-generation source binding.

    This is qualification evidence only. Batch9 readiness requires its separate
    complete-catalog consumer; neither this record nor a child grants readiness.
    """
    if type(capacity_required) is not bool:
        raise WorkflowError("V6 admission requires a typed capability selection")
    owned = claude_owned_auth.require(owned_auth)
    grant, _ = activation.load(repo)
    current = activation.context(repo, grant["binding"]["policy"], owned_auth=owned)
    if digest(current) != digest(grant["binding"]):
        raise WorkflowError("V6 current source, authority, fixtures or generation differs")
    outcomes, cases, bindings = {}, {}, {}
    for number in (20, 21, 22, 23) if capacity_required else (20, 21):
        outcome = activation.outcome(repo, number)
        allocation = activation.slot(number)
        if outcome.get("qualified") is not True or not diagnostic.known_usage(
            outcome.get("usage"), allocation["native_seconds"], allocation["reference_usd"]
        ):
            raise WorkflowError("V6 actual predecessor qualification is incomplete")
        outcomes[str(number)] = digest(outcome)
        if number >= 22:
            directory = activation.root(repo) / f"evidence-{number}"
            meta = review.verify_packet(directory)
            diagnostic.identity(repo, directory, meta)
            capture = review.read_result_artifact(directory, "review-capture.json", meta)
            cases[number] = diagnostic.owned_capture(directory, meta, capture)
            bindings[number] = diagnostic.observation_bindings(grant, meta, capture)
    owned.recheck()
    return {
        "schema_version": 6,
        "grant_digest": digest(grant),
        "binding_digest": digest(current),
        "outcomes": outcomes,
        "empirical_receipt": capacity.empirical_receipt(cases, bindings) if capacity_required else None,
        "authentication": copy.deepcopy(current["policy"]["authentication"]),
    }


def require_packet(directory, meta, *, repo=None, owned_auth=None):
    raise WorkflowError("V6 has no standalone ordinary route; batch9 admission consumer is unavailable")
