#!/usr/bin/env python3
"""Real installed grant CLI on the disposable fake-only upgrade service.

Uses the upgrade harness's unique regular unit, immutable six-module releases,
real actor lock and heartbeat, cleanup guards and retained evidence. Never reads
live credentials. Also verifies that subsequent upgrade preserves the grant.
"""

import copy
import json
import subprocess
from uuid import uuid4

import pilot_upgrade_smoke as smoke
from flowdc_pilot_journal import allowance_binding_sha256, grant_receipts


def exercise_grant(journal, root, service):
    before = journal.read()
    value = {
        "schema_version": 1,
        "grant_id": str(uuid4()),
        "registration_id": before["registration_id"],
        "expected_binding_sha256": allowance_binding_sha256(before),
        "expected_limits_seconds": {vm["role"]: vm["active_seconds"] for vm in before["spec"]["vms"]},
        "additional_seconds": 600,
    }
    path = root / "grant.json"
    command = [
        service["interpreter"],
        "-E",
        "-s",
        "-B",
        service["release"] + "/flowdc_ops.py",
        "pilot",
        "extend-allowance",
        "--state-root",
        str(journal.root),
        "--grant",
        str(path),
    ]
    # Failure before commit adds no receipt and preserves all accounting.
    path.write_text(json.dumps(dict(value, expected_binding_sha256="0" * 64)))
    refused = subprocess.run(command, capture_output=True, timeout=30)
    if refused.returncode != 3 or grant_receipts(journal.read()):
        raise RuntimeError("grant_refusal_failed")
    path.write_text(json.dumps(value))
    applied = subprocess.run(command, capture_output=True, timeout=30)
    if applied.returncode or applied.stderr:
        raise RuntimeError("grant_application_failed")
    first = json.loads(applied.stdout)["data"]
    # Retry the exact private file, as after a lost acknowledgement.
    replay = subprocess.run(command, capture_output=True, timeout=30)
    if replay.returncode or replay.stderr:
        raise RuntimeError("grant_replay_failed")
    second = json.loads(replay.stdout)["data"]
    if (
        first["result"] != "applied"
        or second["result"] != "already_applied"
        or first["receipt"] != second["receipt"]
        or first["current_readiness_verified"]
        or second["current_readiness_verified"]
    ):
        raise RuntimeError("grant_receipt_changed")
    expected = copy.deepcopy(before)
    for vm in expected["spec"]["vms"]:
        vm["active_seconds"] += 600
        expected["vms"][vm["id"]]["account"]["limit"] += 600
    expected["events"].append({"kind": "allowance_granted", "data": first["receipt"]})
    after = journal.read()
    if dict(after, heartbeat=None) != dict(expected, heartbeat=None):
        raise RuntimeError("grant_history_changed")
    (root / "grant-result.json").write_text(json.dumps(first, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(smoke.main(grant_check=exercise_grant))
