"""Finite V6 authority and durable accounting; provider dispatch remains disabled.

The final exact catalog/check adapter remains a deliberately closed dependency.
No caller-supplied success or context can activate this module. Synthetic tests replace those seams only in disposable
repositories; they provide no native qualification. Historical APIs are unchanged.
"""

from __future__ import annotations

import copy
import os
import re
import time

from claude_reporting_execution import exclusive
from reporting_activation_v2 import clock, read
from reporting_activation_v4 import validate_policy
from tasks import digest, plain_path, private_directory
from workflow import WorkflowError

CONTRACT = {
    "issue": 31,
    "plan_comment": 6035844223,
    "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
    "plan_digest": "494abcda1fa95d346ae5f8a84b638202701e4756c3ceb24fc4b4f0a9f9d114e5",
}
CONTRACT_DIGEST = "d124f792051c909f1cf77c8717393888141ff64c2c7cf5a38411f48cce7c5467"
PURPOSE = "issue-31-reporting-recovery-v6"
SEQUENCE = {
    20: "isolation-refusal",
    21: "native-tools-and-source",
    22: "capacity-largest-component",
    23: "capacity-integration48-projected",
}
LIMITS = {
    "processes": 4,
    "seconds": 2400,
    "reference_usd": 24,
    "paid_extra_usd": 0,
    "api_usd": 0,
    "setup_seconds": 900,
    "local_seconds_per_slot": 840,
    "replay_seconds_per_slot": 180,
    "wall_seconds": 7380,
    "expiry_seconds": 10800,
    "credential_margin_seconds": 360,
}
MAX_RECORD_BYTES = 2_000_000


def root(repo):
    return plain_path(repo.main / ".agentic-local/claude-reporting-activation-v6")


def _hex(value, length=64):
    return type(value) is str and re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is not None


def slot(number):
    if type(number) is not int or number not in SEQUENCE:
        raise WorkflowError("Only V6 slots 20 through 23 exist; no retry or slot24")
    return {
        "number": number,
        "purpose": SEQUENCE[number],
        "native_seconds": 300 if number < 22 else 900,
        "reference_usd": 2 if number < 22 else 10,
    }


def remaining(number):
    slot(number)
    return sum(slot(n)["native_seconds"] + 840 + 180 for n in SEQUENCE if n >= number)


def authorization(repo):
    state = read(repo.main / ".agentic-local/tasks/issue-31.json")
    approval = state.get("approval")
    if (
        repo.name != "Zi-Deng/FLOW-DC"
        or state.get("repository") != repo.name
        or state.get("key") != "issue-31"
        or type(state.get("contract_generation")) is not int
        or state["contract_generation"] != 12
        or digest(CONTRACT) != CONTRACT_DIGEST
        or type(approval) is not dict
        or digest(approval.get("contract")) != CONTRACT_DIGEST
        or type(approval.get("issue")) is not int
        or approval["issue"] != 31
        or type(approval.get("plan_comment")) is not int
        or approval["plan_comment"] != 6035844223
        or type(approval.get("source")) is not str
        or not approval["source"].strip()
        or type(approval.get("recorded_at")) is not str
        or not approval["recorded_at"].strip()
    ):
        raise WorkflowError("V6 requires the current exact issue-31 approval")
    return {"contract_digest": CONTRACT_DIGEST, "approval_digest": digest(approval)}


def historical(repo):
    from reporting_recovery_history_v6 import stopped

    return stopped(repo)


def context(repo, policy, *, remaining_seconds=7380, owned_auth=None):
    """Closed until final-source fixtures and owned full-window checks are wired.

    The later implementation must recheck exact source/history/authority/policy,
    all four fixture descriptors and unchanged owned authentication generation;
    credential AND paid-receipt lifetime must exceed remaining_seconds + 360.
    The default catalog refusal precedes all credential/history access. Once that
    adapter exists, only an already-owned snapshot may verify the full window;
    there is no unowned fallback or caller-supplied qualification.
    """
    from claude_owned_auth import require
    from reporting_activation_v2 import harness
    from reporting_diagnostic_v6 import catalog, fixture_binding, packets
    from reporting_recovery_history import semantics

    current = authorization(repo)
    validate_policy(policy)
    if type(remaining_seconds) not in {int, float} or not 0 < remaining_seconds <= 7380:
        raise WorkflowError("Invalid V6 full remaining window")
    # The actual final catalog/check adapter is deliberately not implemented yet.
    source = catalog(repo)
    if current["approval_digest"] != "ae8e4b2ff106d44908e471d1f36be74b5d9f632f1d0b92b32e5261193a245cd8":
        raise WorkflowError("V6 exact standing approval differs")
    actual_harness = harness()
    fixtures = {n: fixture_binding(files) for n, files in packets(source).items()}
    history = historical(repo)
    if semantics(policy) != history["stopped_v4"]["policy_semantics"]:
        raise WorkflowError("V6 frozen policy semantics differ")
    # This existing owned verifier checks BOTH lifetimes, adds300+60 margins,
    # and holds the original lock. No unowned fallback or lineage renewal.
    actual_auth = require(owned_auth).current_binding(900, remaining_seconds)
    if actual_auth != policy["authentication"]:
        raise WorkflowError("V6 authentication generation changed")
    result = {
        "authorization": current,
        "history": history,
        "harness": actual_harness,
        "policy": copy.deepcopy(policy),
        "fixtures": fixtures,
    }
    _binding(result)
    return result


def _binding(value):
    if type(value) is not dict or set(value) != {"authorization", "history", "harness", "policy", "fixtures"}:
        raise WorkflowError("Invalid V6 context shape")
    auth = value["authorization"]
    if (
        type(auth) is not dict
        or set(auth) != {"contract_digest", "approval_digest"}
        or auth["contract_digest"] != CONTRACT_DIGEST
        or not _hex(auth["approval_digest"])
    ):
        raise WorkflowError("Invalid V6 authority binding")
    harness = value["harness"]
    if (
        type(harness) is not dict
        or set(harness) != {"head", "files"}
        or not _hex(harness["head"], 40)
        or type(harness["files"]) is not dict
        or not harness["files"]
        or not all(type(k) is str and k and _hex(v) for k, v in harness["files"].items())
    ):
        raise WorkflowError("Invalid V6 source binding")
    if type(value["history"]) is not dict or not value["history"]:
        raise WorkflowError("Missing V6 inherited history")
    validate_policy(value["policy"])
    fixtures = value["fixtures"]
    if type(fixtures) is not dict or set(fixtures) != {str(n) for n in SEQUENCE}:
        raise WorkflowError("V6 requires the whole fixed four-fixture inventory")
    for item in fixtures.values():
        if (
            type(item) is not dict
            or set(item) != {"fixture_sha256", "descriptor_sha256"}
            or not all(_hex(v) for v in item.values())
        ):
            raise WorkflowError("Invalid V6 fixture binding")


def preview(repo, policy, *, name, tested_head, now=None, owned_auth=None):
    if root(repo).exists():
        raise WorkflowError("Prior V6 application cannot be repeated")
    start = clock(time.time() if now is None else now)
    binding = context(repo, policy, owned_auth=owned_auth)
    if tested_head != binding["harness"]["head"]:
        raise WorkflowError("V6 tested source differs")
    grant = {
        "schema_version": 6,
        "kind": "reporting-recovery-v6",
        "purpose": PURPOSE,
        "name": name,
        "not_before": start,
        "expires_at": start + 10800,
        "binding": copy.deepcopy(binding),
        "slots": [slot(n) for n in SEQUENCE],
        "limits": LIMITS.copy(),
        "stop_on_failure": True,
    }
    validate_grant(grant)
    return {"status": "preview", "grant": grant, "preview_digest": digest(grant)}


def validate_grant(grant):
    if type(grant) is not dict or set(grant) != {
        "schema_version",
        "kind",
        "purpose",
        "name",
        "not_before",
        "expires_at",
        "binding",
        "slots",
        "limits",
        "stop_on_failure",
    }:
        raise WorkflowError("Invalid V6 grant shape")
    _binding(grant["binding"])
    start, end = clock(grant["not_before"]), clock(grant["expires_at"])
    expected = {
        **grant,
        "schema_version": 6,
        "kind": "reporting-recovery-v6",
        "purpose": PURPOSE,
        "slots": [slot(n) for n in SEQUENCE],
        "limits": LIMITS.copy(),
        "stop_on_failure": True,
    }
    if (
        digest(expected) != digest(grant)
        or end != start + 10800
        or type(grant["name"]) is not str
        or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", grant["name"]) is None
    ):
        raise WorkflowError("V6 bounds or identity changed")


def apply(repo, proposal, *, preview_digest, now=None, owned_auth=None):
    started = clock(time.time() if now is None else now)
    # Even an empty or torn namespace is not silently adopted or reset.
    if root(repo).exists():
        raise WorkflowError("Prior V6 application cannot be repeated")
    if type(proposal) is not dict or set(proposal) != {"status", "grant", "preview_digest"}:
        raise WorkflowError("Invalid V6 preview")
    grant = proposal["grant"]
    validate_grant(grant)
    if (
        proposal["status"] != "preview"
        or proposal["preview_digest"] != preview_digest
        or digest(grant) != preview_digest
        or not grant["not_before"] <= started <= grant["expires_at"] - 7380
        or digest(context(repo, grant["binding"]["policy"], owned_auth=owned_auth))
        != digest(grant["binding"])
    ):
        raise WorkflowError("Stale V6 preview or context")
    checked = started if now is not None else clock(time.time())
    if not started <= checked <= min(grant["expires_at"] - 7380, started + 900):
        raise WorkflowError("V6 application checks exhausted the window")
    # Exclusive directory claim closes concurrent applications and torn writes.
    private_directory(root(repo).parent)
    try:
        root(repo).mkdir(mode=0o700)
    except FileExistsError:
        raise WorkflowError("Prior V6 application cannot be repeated") from None
    parent = os.open(root(repo).parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)
    exclusive(
        root(repo) / "application.json",
        {
            "schema_version": 6,
            "grant_digest": digest(grant),
            "applied_at": started,
            "deadline": min(grant["expires_at"], started + 7380),
        },
    )
    exclusive(root(repo) / "grant.json", grant, limit=MAX_RECORD_BYTES)
    return {"status": "applied", "grant_digest": digest(grant)}


def load(repo):
    allowed = {"grant.json", "application.json"}
    allowed.update(f"{kind}-{n}.json" for n in SEQUENCE for kind in ("attempt", "outcome"))
    allowed.update(f"evidence-{n}" for n in SEQUENCE)
    if any(path.name not in allowed or path.is_symlink() for path in root(repo).iterdir()):
        raise WorkflowError("Unexpected V6 namespace material")
    grant, application = read(root(repo) / "grant.json"), read(root(repo) / "application.json")
    validate_grant(grant)
    applied = clock(application.get("applied_at"))
    expected = {
        "schema_version": 6,
        "grant_digest": digest(grant),
        "applied_at": applied,
        "deadline": min(grant["expires_at"], applied + 7380),
    }
    if (
        digest(expected) != digest(application)
        or not grant["not_before"] <= applied <= grant["expires_at"] - 7380
    ):
        raise WorkflowError("V6 application is torn or changed")
    return grant, application


def _reservation(grant, number, input_digest, started):
    allocation = slot(number)
    if not _hex(input_digest):
        raise WorkflowError("V6 requires an exact prepared input digest")
    return {
        "schema_version": 6,
        "number": number,
        "purpose": allocation["purpose"],
        "grant_digest": digest(grant),
        "input_digest": input_digest,
        "fixture": copy.deepcopy(grant["binding"]["fixtures"][str(number)]),
        "started": started,
        "deadline": started + allocation["native_seconds"] + 840,
        "replay_deadline": started + allocation["native_seconds"] + 840 + 180,
        "reserved_seconds": allocation["native_seconds"],
        "reserved_reference_usd": allocation["reference_usd"],
        "wrapper_processes": 1,
    }


def reserve(repo, *, number, input_digest, now=None, owned_auth=None):
    from reporting_recovery_history_v6 import known_usage

    slot(number)
    if not _hex(input_digest):
        raise WorkflowError("V6 requires an exact prepared input digest")
    started = clock(time.time() if now is None else now)
    grant, application = load(repo)
    if any(
        (root(repo) / f"{kind}-{n}.json").exists()
        for n in SEQUENCE
        if n >= number
        for kind in ("attempt", "outcome")
    ):
        raise WorkflowError("V6 replay or out-of-order reservation")
    previous_finished = application["applied_at"]
    for n in SEQUENCE:
        if n < number:
            previous = outcome(repo, n)  # Independently replay; a saved bool is insufficient.
            allocation = slot(n)
            if previous.get("qualified") is not True or not known_usage(
                previous.get("usage"), allocation["native_seconds"], allocation["reference_usd"]
            ):
                raise WorkflowError("V6 predecessor is incomplete or usage unknown")
            previous_finished = clock(previous["finished"])
    if not previous_finished <= started <= application["deadline"] - remaining(number):
        raise WorkflowError("V6 remaining allocation does not fit or clock rolled back")
    if digest(
        context(
            repo,
            grant["binding"]["policy"],
            remaining_seconds=application["deadline"] - started,
            owned_auth=owned_auth,
        )
    ) != digest(grant["binding"]):
        raise WorkflowError("V6 source, authority, history, fixtures or generation changed")
    checked = started if now is not None else clock(time.time())
    if not started <= checked <= application["deadline"] - remaining(number):
        raise WorkflowError("V6 reservation checks exhausted the window")
    # Fresh prerequisite checks consume this slot's local allocation too.
    record = _reservation(grant, number, input_digest, started)
    exclusive(root(repo) / f"attempt-{number}.json", record)
    return record


def evaluate(repo, grant, reservation, finished):
    """Recompute exact V6 diagnostic/capacity evidence within immutable bounds."""
    validate_grant(grant)
    number = reservation.get("number")
    slot(number)
    started, finished = clock(reservation.get("started")), clock(finished)
    loaded, application = load(repo)
    if (
        digest(loaded) != digest(grant)
        or digest(reservation)
        != digest(_reservation(grant, number, reservation.get("input_digest"), started))
        or not application["applied_at"]
        <= started
        <= finished
        <= min(application["deadline"], reservation["replay_deadline"])
    ):
        raise WorkflowError("V6 reservation, completion window or grant changed")
    from reporting_diagnostic_v6 import replay

    return replay(repo, grant, reservation, finished)


def outcome(repo, number):
    slot(number)
    grant, _ = load(repo)
    saved = read(root(repo) / f"outcome-{number}.json")
    expected = evaluate(repo, grant, read(root(repo) / f"attempt-{number}.json"), saved.get("finished"))
    if digest(expected) != digest(saved):
        raise WorkflowError("V6 saved outcome differs from independent replay")
    return expected


def complete(repo, *, number, now=None):
    slot(number)
    grant, _ = load(repo)
    record = evaluate(
        repo, grant, read(root(repo) / f"attempt-{number}.json"), clock(time.time() if now is None else now)
    )
    exclusive(root(repo) / f"outcome-{number}.json", record)
    return record
