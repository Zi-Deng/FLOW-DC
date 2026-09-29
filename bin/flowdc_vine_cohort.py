"""Finite owned native worker cohorts, distinct from HTTP-attempt admission.

Only the local process owner and prepared guest service launcher construct these
contracts. A feature is scheduling eligibility, never a credential or identity.
"""

import re
from uuid import uuid4

from flowdc_vine_protocol import require

SCHEMA = "flowdc-owned-vine-cohort-v1"
NATIVE_RETRIES = 1  # 7.17.2 can redispatch exhaustion once at try_count == 1.
CATEGORY = "flowdc-bounded-v1"


def cohort(count, *, owner="local-process-v1", identifier=None, replacements=()):
    require(type(count) is int and count in (1, 2, 4), "invalid owned worker count")
    identifier = uuid4().hex if identifier is None else identifier
    require(isinstance(identifier, str) and re.fullmatch(r"[0-9a-f]{32}", identifier), "invalid cohort ID")
    value = {
        "schema": SCHEMA,
        "owner": owner,
        "slots": [
            {"feature": f"flowdc-{identifier}-{i}", "launch_limit": 2 if i in replacements else 1}
            for i in range(count)
        ],
    }
    return validate(value, count)


def validate(value, count):
    require(
        isinstance(value, dict)
        and set(value) == {"schema", "owner", "slots"}
        and value["schema"] == SCHEMA
        and value["owner"] in ("local-process-v1", "prepared-guest-service-v1"),
        "an owned single-shot worker cohort is required",
    )
    slots = value["slots"]
    require(isinstance(slots, list) and len(slots) == count, "cohort size mismatch")
    features = []
    for slot in slots:
        require(
            isinstance(slot, dict)
            and set(slot) == {"feature", "launch_limit"}
            and isinstance(slot["feature"], str)
            and re.fullmatch(r"flowdc-[0-9a-f]{32}-[0-3]", slot["feature"])
            and type(slot["launch_limit"]) is int
            and slot["launch_limit"] in (1, 2),
            "invalid cohort slot",
        )
        require(
            value["owner"] != "prepared-guest-service-v1" or slot["launch_limit"] == 1,
            "guest replacement requires a new prepared run",
        )
        features.append(slot["feature"])
    require(len(set(features)) == count, "cohort features must be distinct")
    return value


def dispatch_audit(value, tasks, dispatches):
    """Check observed transactions against the separately enforced launch contract.

    K accepted single-shot worker connections allow at most K loss/final dispatches;
    the native retry counter permits at most R additional exhaustion dispatches.
    Forsaken retries and fast abort must be disabled. This audit is not enforcement.
    """
    counts = {identifier: 0 for identifier in tasks}
    for event in dispatches:
        require(event["task_id"] in counts, "dispatch for unknown task")
        counts[event["task_id"]] += 1
    limits = {
        identifier: value["slots"][index]["launch_limit"] + NATIVE_RETRIES
        for index, identifier in enumerate(tasks)
    }
    return {
        "cohort": value,
        "native_retries": NATIVE_RETRIES,
        "max_forsaken": 0,
        "slow_worker_abort": False,
        "allocation": "fixed",
        "tasks": [
            {"task_id": key, "dispatches": counts[key], "dispatch_limit": limits[key]} for key in tasks
        ],
        "within_bound": all(counts[key] <= limits[key] for key in tasks),
        "validity_domain": "owned finite single-shot cohort; exclusive native endpoint containment",
    }
