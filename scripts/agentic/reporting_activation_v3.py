"""Finite recovery v3 (14/15), separate from the immutable v1 allowance.

No provider is run here. Local records are owner-writable accounting, not
attestations; final-head CI, inspection and fresh actual prerequisites precede apply.
"""

from __future__ import annotations

import copy
import re
import time

import claude_native_auth
import claude_partial_observation_v2 as observation
import diagnostic_tool_contract
from claude_reporting_execution import exclusive
from reporting_activation_v2 import clock, harness, read, validate_policy
from tasks import digest, plain_path, private_directory
from workflow import WorkflowError

CONTRACT = {
    "issue": 31,
    "plan_comment": 6009076812,
    "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
    "plan_digest": "88af4787bfab768b81ff05ea7b09ab20bdefbb6e87572666b38c8b890ce1d2d0",
}
CONTRACT_DIGEST = "e2c7d17ff002678c6c2e6b87e2a0d1f898f285c1ad9fb9f22c467644ab1aa73f"
SEQUENCE = {14: "isolation-refusal", 15: "native-tools-and-source"}
LIMITS = {"processes": 2, "seconds": 600, "reference_usd": 4, "paid_extra_usd": 0, "api_usd": 0}
MAX_RECORD_BYTES = 2_000_000


def root(repo):
    return plain_path(repo.main / ".agentic-local/claude-reporting-activation-v3")


def authorization(repo):
    state = read(repo.main / ".agentic-local/tasks/issue-31.json")
    approval = state.get("approval", {})
    if type(approval) is not dict:
        raise WorkflowError("Invalid current reporting approval")
    if (
        repo.name != "Zi-Deng/FLOW-DC"
        or state.get("repository") != repo.name
        or state.get("key") != "issue-31"
        or digest(CONTRACT) != CONTRACT_DIGEST
        or digest(approval.get("contract")) != CONTRACT_DIGEST
        or type(approval.get("issue")) is not int
        or approval["issue"] != 31
        or type(approval.get("plan_comment")) is not int
        or approval["plan_comment"] != 6009076812
        or not isinstance(approval.get("source"), str)
        or not approval["source"].strip()
        or not isinstance(approval.get("recorded_at"), str)
        or not approval["recorded_at"].strip()
    ):
        raise WorkflowError("Reporting activation requires the current exact issue-31 approval")
    return {"contract_digest": CONTRACT_DIGEST, "approval_digest": digest(approval)}


def historical(repo):
    from reporting_recovery_history_v3 import stopped

    return stopped(repo)


def context(repo, policy, *, owned_auth=None):
    validate_policy(policy)
    # No historical approval or auth lineage substitution for this new grant.
    observed = {"authorization": authorization(repo), "history": historical(repo), "harness": harness()}
    from reporting_recovery_history import semantics

    if semantics(policy) != observed["history"]["stopped_v1"]["policy_semantics"]:
        raise WorkflowError("Recovery reporting policy differs from the stopped approved profile")
    if owned_auth is None:
        binding = claude_native_auth.current_binding(600)
    else:
        from claude_owned_auth import require

        binding = require(owned_auth).current_binding(600)
    if binding != policy["authentication"]:
        raise WorkflowError("Reporting activation authentication generation changed")
    return {
        **observed,
        "policy": copy.deepcopy(policy),
        "tool_contract": diagnostic_tool_contract.contract(),
        "observation": observation.DESCRIPTOR.copy(),
    }


def preview(repo, policy, *, name, tested_head, expires_at, now=None):
    now, expires_at = clock(time.time() if now is None else now), clock(expires_at)
    if (
        type(name) is not str
        or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", name) is None
        or not now + 600 <= expires_at <= now + 86400
    ):
        raise WorkflowError("Reporting activation needs a named finite window of 600 seconds to one day")
    binding = context(repo, policy)
    if tested_head != binding["harness"]["head"]:
        raise WorkflowError("Tested head differs from the actual reporting harness")
    value = {
        "schema_version": 3,
        "kind": "reporting-recovery-v3",
        "name": name,
        "not_before": now,
        "expires_at": expires_at,
        "binding": binding,
        "slots": [{"number": n, "purpose": p} for n, p in SEQUENCE.items()],
        "limits": LIMITS.copy(),
        "stop_on_failure": True,
    }
    return {"status": "preview", "grant": value, "preview_digest": digest(value)}


def validate_grant(grant):
    if type(grant) is not dict or set(grant) != {
        "schema_version",
        "kind",
        "name",
        "not_before",
        "expires_at",
        "binding",
        "slots",
        "limits",
        "stop_on_failure",
    }:
        raise WorkflowError("Invalid reporting grant shape")
    binding = grant["binding"]
    if type(binding) is not dict or set(binding) != {
        "authorization",
        "history",
        "harness",
        "policy",
        "tool_contract",
        "observation",
    }:
        raise WorkflowError("Invalid reporting grant binding")
    if digest(binding["observation"]) != digest(observation.DESCRIPTOR):
        raise WorkflowError("Reporting observation contract changed")
    validate_policy(binding["policy"])
    diagnostic_tool_contract.validate(binding["tool_contract"])
    start, end = clock(grant["not_before"]), clock(grant["expires_at"])
    expected = {
        **grant,
        "schema_version": 3,
        "kind": "reporting-recovery-v3",
        "slots": [{"number": n, "purpose": p} for n, p in SEQUENCE.items()],
        "limits": LIMITS.copy(),
        "stop_on_failure": True,
    }
    if (
        digest(expected) != digest(grant)
        or not start + 600 <= end <= start + 86400
        or type(grant["name"]) is not str
        or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", grant["name"])
        or binding["authorization"].get("contract_digest") != CONTRACT_DIGEST
    ):
        raise WorkflowError("Reporting grant bounds or authority changed")


def apply(repo, proposal, *, preview_digest, now=None):
    """Explicit coordinator operation; synthetic tests use disposable repositories."""
    directory = root(repo)
    if directory.exists() and any(directory.iterdir()):
        raise WorkflowError("Prior reporting application cannot be repeated, including uncertain writes")
    if type(proposal) is not dict:
        raise WorkflowError("Invalid reporting preview")
    grant = proposal.get("grant")
    validate_grant(grant)
    supplied_clock = now is not None
    now = clock(time.time() if now is None else now)
    if (
        proposal.get("status") != "preview"
        or digest(grant) != preview_digest
        or proposal.get("preview_digest") != preview_digest
        or not grant["not_before"] <= now <= grant["expires_at"] - 600
        or digest(context(repo, grant["binding"]["policy"])) != digest(grant["binding"])
    ):
        raise WorkflowError("Reporting preview is stale or differs from application")
    checked_at = now if supplied_clock else clock(time.time())
    if not now <= checked_at <= grant["expires_at"] - 600:
        raise WorkflowError("Reporting application window expired during final checks")
    private_directory(directory)
    # A torn second write remains consumed/uncertain; never retry application.
    exclusive(directory / "application.json", {"grant_digest": preview_digest, "applied_at": checked_at})
    exclusive(directory / "grant.json", grant, limit=MAX_RECORD_BYTES)
    return {"status": "applied", "grant_digest": preview_digest}


def load(repo):
    directory = root(repo)
    grant, application = read(directory / "grant.json"), read(directory / "application.json")
    validate_grant(grant)
    if (
        type(application) is not dict
        or set(application) != {"grant_digest", "applied_at"}
        or application["grant_digest"] != digest(grant)
        or not grant["not_before"] <= clock(application["applied_at"]) <= grant["expires_at"] - 600
    ):
        raise WorkflowError("Reporting application is missing, uncertain or changed")
    return grant, application


def reserve(repo, *, number, input_digest, now=None):
    """Consume a slot before any setup/dispatch; no uncertain reservation retry."""
    if type(number) is not int or number not in SEQUENCE:
        raise WorkflowError("Only reporting attempts 14 and 15 are authorized; no sixteenth slot")
    if type(input_digest) is not str or re.fullmatch(r"[0-9a-f]{64}", input_digest) is None:
        raise WorkflowError("Reporting reservation requires its exact prepared input digest")
    if plain_path(root(repo) / f"attempt-{number}.json").exists():
        raise WorkflowError("Prior reporting reservation cannot be repeated")
    grant, application = load(repo)
    supplied_clock = now is not None
    now = clock(time.time() if now is None else now)
    deadline = grant["expires_at"]
    if number == 15:
        previous = outcome(repo, 14)
        if not previous["qualified"]:
            raise WorkflowError("Reporting sequence stopped after failure")
        first = read(root(repo) / "attempt-14.json")
        deadline = min(deadline, first["started"] + 600)
        if now < previous["finished"]:
            raise WorkflowError("Reporting clock moved backwards")
    elif any((root(repo) / name).exists() for name in ("attempt-15.json", "outcome-15.json")):
        raise WorkflowError("Reporting sequence is inconsistent")
    if not application["applied_at"] <= now <= deadline - 300:
        raise WorkflowError("Insufficient reporting grant time or clock rollback")
    if digest(context(repo, grant["binding"]["policy"])) != digest(grant["binding"]):
        raise WorkflowError("Reporting grant context changed")
    checked_at = now if supplied_clock else clock(time.time())
    if not now <= checked_at <= deadline - 300:
        raise WorkflowError("Reporting reservation window expired during final checks")
    now = checked_at
    record = {
        "schema_version": 3,
        "number": number,
        "purpose": SEQUENCE[number],
        "grant_digest": digest(grant),
        "input_digest": input_digest,
        "started": now,
        "deadline": min(now + 300, deadline),
        "reserved_seconds": 300,
        "reserved_reference_usd": 2,
        "wrapper_processes": 1,
    }
    exclusive(root(repo) / f"attempt-{number}.json", record)
    return record


def outcome(repo, number):
    """Recompute exact diagnostic evidence on every read; never trust saved success."""
    if type(number) is not int or number not in SEQUENCE:
        raise WorkflowError("Unknown reporting outcome")
    grant, _ = load(repo)
    reservation = read(root(repo) / f"attempt-{number}.json")
    saved = read(root(repo) / f"outcome-{number}.json")
    expected = evaluate(repo, grant, reservation, saved.get("finished"))
    if digest(expected) != digest(saved):
        raise WorkflowError("Reporting outcome or its exact evidence changed")
    return expected


def evaluate(repo, grant, reservation, finished):
    """Offline evidence only. The diagnostic runner must retain the bound capture."""
    import review
    import review_coverage

    number = reservation.get("number")
    if type(number) is not int or number not in SEQUENCE:
        raise WorkflowError("Invalid reporting reservation number")
    started, finished = clock(reservation.get("started")), clock(finished)
    _, application = load(repo)
    if not application["applied_at"] <= started <= grant["expires_at"] - 300:
        raise WorkflowError("Reporting reservation is outside its finite grant")
    if number == 15:
        first = outcome(repo, 14)
        first_reservation = read(root(repo) / "attempt-14.json")
        if (
            not first["qualified"]
            or started < first["finished"]
            or started + 300 > first_reservation["started"] + 600
        ):
            raise WorkflowError("Reporting successor lacks a complete prior success or aggregate time")
    expected = {
        "schema_version": 3,
        "number": number,
        "purpose": SEQUENCE[number],
        "grant_digest": digest(grant),
        "input_digest": reservation.get("input_digest"),
        "started": started,
        "deadline": started + 300,
        "reserved_seconds": 300,
        "reserved_reference_usd": 2,
        "wrapper_processes": 1,
    }
    if digest(expected) != digest(reservation) or finished < started:
        raise WorkflowError("Reporting reservation changed or clock moved backwards")
    directory = root(repo) / f"evidence-{number}"
    meta = review.verify_packet(directory)
    input_value = {k: v for k, v in meta.items() if k not in review.RESULT_FIELDS}
    if (
        digest(input_value) != reservation["input_digest"]
        or digest(meta["review_policy"]) != digest(grant["binding"]["policy"])
        or meta.get("reporting_activation")
        != {
            "grant_digest": digest(grant),
            "number": number,
            "purpose": SEQUENCE[number],
        }
    ):
        raise WorkflowError("Reporting evidence differs from its reserved input")
    review.qualification(directory)  # independently binds all retained capture/proof/report bytes
    capture = review.read_result_artifact(directory, "review-capture.json", meta)
    if "execution" not in capture:
        raise WorkflowError("Reporting diagnostic lacks its actual execution binding")
    from reporting_diagnostic_v3 import PURPOSE, completion, identity

    if meta.get("purpose") != PURPOSE:
        raise WorkflowError("Recovery v3 requires actual fixed diagnostic execution and completion")
    identity(repo, directory, meta)
    if number == 15:
        previous_directory = root(repo) / "evidence-14"
        identity(repo, previous_directory, review.verify_packet(previous_directory))
    timing, timing_qualified = completion(directory, capture, reservation)
    if finished != timing["finished"]:
        raise WorkflowError("Reporting outcome completion time differs from execution")
    diagnostics = copy.deepcopy(capture["diagnostics"])
    if diagnostics["telemetry"].get("diagnostic") != {
        "purpose": SEQUENCE[number],
        "tool_contract_digest": digest(diagnostic_tool_contract.contract()),
    }:
        raise WorkflowError("Ordinary review telemetry cannot establish reporting diagnostics")
    if SEQUENCE[number] == "isolation-refusal":
        if (
            diagnostics["telemetry"].get("controlled_refusals") != 1
            or diagnostics["reasons"].count("controlled_refusal_diagnostic_only") != 1
        ):
            diagnostics["reasons"].append("required_controlled_refusal_missing")
        else:
            diagnostics["reasons"].remove("controlled_refusal_diagnostic_only")
    partial = diagnostics["telemetry"].get("partial_stream")
    observation.validate(partial, diagnostics["telemetry"]["types"].get("stream_event", 0))
    if partial["schema_version"] != 2:
        raise WorkflowError("Recovery v3 requires structural observation schema 2")
    assessment = review_coverage.assess(
        directory / "packet", capture["body"], diagnostics, policy=meta["review_policy"]
    )
    return {
        "schema_version": 3,
        "number": number,
        "grant_digest": digest(grant),
        "reservation_digest": digest(reservation),
        "capture_digest": digest(capture),
        "assessment_digest": digest(assessment),
        "finished": finished,
        "qualified": (
            timing_qualified
            and assessment["qualified"]
            and capture["reporting"]["accepted"]
            and finished <= reservation["deadline"]
            and diagnostics["usage"]["counters"].get("duration_ms", float("inf")) <= 300000
        ),
        "usage": copy.deepcopy(diagnostics["usage"]),
    }


def complete(repo, *, number, now=None):
    grant, _ = load(repo)
    if type(number) is not int or number not in SEQUENCE:
        raise WorkflowError("Unknown reporting attempt")
    record = evaluate(
        repo, grant, read(root(repo) / f"attempt-{number}.json"), clock(time.time() if now is None else now)
    )
    exclusive(root(repo) / f"outcome-{number}.json", record)
    return record
