"""Read-only V5 closure, preserving every earlier historical outcome and uncertainty.

Pins derive from the retained phase34 root-verified map, not a new live capture.
This reader never substitutes historical qualification for a changed harness.
"""

import reporting_activation_v5 as old
import reporting_diagnostic_v5 as diagnostic
import review
from reporting_recovery_history_v3 import approval, tree
from tasks import digest
from workflow import WorkflowError

V5_TREE = {
    "files_digest": "6d11493a7a0475b8f90a0ecd5542d8d38b2896a8578339033da6bf01aacda4a0",
    "file_count": 52,
    "byte_count": 384209,
}
V5_APPROVAL = "111069f7b7474052c5055b2b9f69cf73b2df8b5aba2f5c719550fb20bad37a71"


def known_usage(value, seconds, reference_usd):
    """Observed cost and duration, never a status-only or unknown receipt."""
    if type(value) is not dict or value.get("status") != "observed":
        return False
    counters = value.get("counters")
    if type(counters) is not dict:
        return False
    for key, maximum in (("duration_ms", seconds * 1000), ("estimated_usd", reference_usd)):
        number = counters.get(key)
        if type(number) not in (int, float) or not 0 <= number <= maximum:
            return False
    return True


def validate_records(closure, grant, original, outcomes, historical_approval):
    """Pure cross-record validation; tree and frozen readers remain mandatory."""
    if digest(closure) != digest(V5_TREE):
        raise WorkflowError("V5 original history closure changed")
    if (
        type(grant) is not dict
        or type(grant.get("schema_version")) is not int
        or grant["schema_version"] != 5
        or grant.get("kind") != "reporting-recovery-v5"
        or type(grant.get("binding")) is not dict
        or digest(grant["binding"].get("history")) != digest(original)
        or grant["binding"].get("authorization")
        != {"contract_digest": old.CONTRACT_DIGEST, "approval_digest": V5_APPROVAL}
        or historical_approval != V5_APPROVAL
    ):
        raise WorkflowError("V5 original authority or inherited history changed")
    if type(outcomes) is not dict or set(outcomes) != {"18", "19"}:
        raise WorkflowError("V5 complete original outcomes are required")
    for number in (18, 19):
        value = outcomes[str(number)]
        if (
            type(value) is not dict
            or type(value.get("schema_version")) is not int
            or value["schema_version"] != 5
            or type(value.get("number")) is not int
            or value["number"] != number
            or value.get("grant_digest") != digest(grant)
            or value.get("qualified") is not True
            or not known_usage(value.get("usage"), 300, 2)
        ):
            raise WorkflowError("V5 original outcome or known usage changed")


def stopped(repo):
    directory = old.root(repo)
    closure = tree(directory, max_files=52, max_bytes=2_000_000)
    if digest(closure) != digest(V5_TREE):
        raise WorkflowError("V5 original history closure changed")
    original = old.historical(repo)
    grant, application = old.load(repo)
    historical_approval = approval(repo, old.CONTRACT_DIGEST, 6014789492, V5_APPROVAL)
    import reporting_activation_v6 as current

    state = old.read(repo.main / ".agentic-local/tasks/issue-31.json")
    rows = state.get("approval_history")
    next_generation = type(state.get("contract_generation")) is int and state["contract_generation"] == 13
    g14 = type(state.get("contract_generation")) is int and state["contract_generation"] == 14
    g15 = type(state.get("contract_generation")) is int and state["contract_generation"] == 15
    g16 = type(state.get("contract_generation")) is int and state["contract_generation"] == 16
    g17 = type(state.get("contract_generation")) is int and state["contract_generation"] == 17
    g18 = type(state.get("contract_generation")) is int and state["contract_generation"] == 18
    if g18:
        current.authorization(repo)
        current._g18_history(state)
    if g17:
        current.authorization(repo)
        current._g17_history(state)
    if g16:
        current.authorization(repo)
        current._g16_history(state)
    if g15:
        current.authorization(repo)
        current._g15_history(state)
    if g14:
        current.authorization(repo)
        current._g14_history(state)
    if next_generation:
        current.authorization(repo)
        current._next_history(state)
    if (
        type(rows) is not list
        or len(rows)
        != (
            17
            if g18
            else 16
            if g17
            else 15
            if g16
            else 14
            if g15
            else 13
            if g14
            else 12
            if next_generation
            else 11
        )
        or sum(type(row) is dict and row.get("plan_comment") == 6014789492 for row in rows) != 1
    ):
        raise WorkflowError("V5 historical approval is ambiguous")
    outcomes = {}
    for number in (18, 19):
        evidence = directory / f"evidence-{number}"
        diagnostic.identity(repo, evidence, review.verify_packet(evidence))
        outcomes[str(number)] = old.outcome(repo, number)
    validate_records(closure, grant, original, outcomes, historical_approval)
    if digest(tree(directory, max_files=52, max_bytes=2_000_000)) != digest(closure):
        raise WorkflowError("V5 closure changed during replay")
    return {
        **original,
        "stopped_v5": {
            **closure,
            "grant_digest": digest(grant),
            "application_digest": digest(application),
            "approval_digest": historical_approval,
            "approval_history_digest": digest(rows),
            "outcomes": outcomes,
            "state": "closed-no-retry-no20",
        },
    }
