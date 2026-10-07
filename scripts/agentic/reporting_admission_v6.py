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


def check_batch(repo, directory, *, owned_auth):
    """Same-generation first-window consumer; original diagnostic check is unchanged."""
    import review_batch_windows_v1 as windows
    from reporting_activation_v2 import harness
    from reporting_recovery_history import semantics

    owned = claude_owned_auth.require(owned_auth)
    batch = windows.load_preparation(directory)
    timer = windows.PrefixClock(directory)
    source = windows.catalog(repo, batch_directory=directory)
    timer.check()
    grant, _ = activation.load(repo)
    policy = grant["binding"]["policy"]
    activation.validate_policy(policy)
    current = {"policy": copy.deepcopy(policy)}
    current["authorization"] = activation.authorization(repo)
    timer.check()
    current["history"] = activation.historical(repo)
    timer.check()
    current["harness"] = harness()
    timer.check()
    current["fixtures"] = {
        n: diagnostic.fixture_binding(files) for n, files in diagnostic.packets(source).items()
    }
    timer.check()
    activation._binding(current)
    if (
        digest(current) != digest(grant["binding"])
        or semantics(policy) != current["history"]["stopped_v4"]["policy_semantics"]
    ):
        raise WorkflowError("Batch9 original V6 source/fixtures/authority/history differs")
    if (
        owned.current_binding(900, batch["plan"]["schedule"]["window_seconds"][0] - 360)
        != policy["authentication"]
    ):
        raise WorkflowError("Batch9 first window requires the original V6 generation")
    timer.check()
    outcomes, cases, bindings = {}, {}, {}
    for number in (20, 21, 22, 23):
        outcome = activation.outcome(repo, number)
        allocation = activation.slot(number)
        if outcome.get("qualified") is not True or not diagnostic.known_usage(
            outcome.get("usage"), allocation["native_seconds"], allocation["reference_usd"]
        ):
            raise WorkflowError("Batch9 actual V6 predecessor qualification incomplete")
        outcomes[str(number)] = digest(outcome)
        if number >= 22:
            path = activation.root(repo) / f"evidence-{number}"
            meta = review.verify_packet(path)
            diagnostic.identity(repo, path, meta)
            capture = review.read_result_artifact(path, "review-capture.json", meta)
            cases[number] = diagnostic.owned_capture(path, meta, capture)
            bindings[number] = diagnostic.observation_bindings(grant, meta, capture)
        timer.check()
    actual = {
        "schema_version": 6,
        "grant_digest": digest(grant),
        "binding_digest": digest(current),
        "outcomes": outcomes,
        "empirical_receipt": capacity.empirical_receipt(cases, bindings),
        "authentication": copy.deepcopy(policy["authentication"]),
    }
    if actual != batch["admission"]:
        raise WorkflowError("Batch9 original admission/capacity evidence changed")
    owned.recheck()
    timer.check()
    return actual
