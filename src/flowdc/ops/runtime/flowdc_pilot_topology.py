"""Offline topology migration/registration requests with immutable UUID accounts.

No provider calls, installations, grants or activation. Applying to a production
journal is a separate human checkpoint; this implementation is tested on fixtures.
"""

import copy
import hashlib
import os
from dataclasses import asdict
from pathlib import Path

import flowdc_ops as ops
from flowdc_pilot import Allowance
from flowdc_pilot_journal import (
    Journal,
    allowance_binding_sha256,
    encode,
    failure,
    require_grant_idle,
    validate_record,
)
from flowdc_pilot_provider import validate_access_value
from flowdc_topology import JOURNAL_SCHEMA, roles


def sha(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def source_digest():
    # Complete installed supervisor closure, including this migration module.
    from flowdc_pilot_cli import MODULES

    source = Path(__file__).parent
    return hashlib.sha256(b"".join((source / name).read_bytes() for name in MODULES)).hexdigest()


def validate_request(request):
    try:
        ops.fields(
            request,
            (
                "schema_version",
                "migration_id",
                "registration_id",
                "expected_binding_sha256",
                "expected_state_sha256",
                "expected_source_sha256",
                "spec",
                "access",
                "new_worker_ids",
            ),
        )
        ops.version(request)
        for key in ("migration_id", "registration_id"):
            if ops.uuid_value(request[key]) != request[key]:
                raise ValueError
        import re

        for key in ("expected_binding_sha256", "expected_state_sha256", "expected_source_sha256"):
            if not isinstance(request[key], str) or not re.fullmatch(r"[0-9a-f]{64}", request[key]):
                raise ValueError
        ops.validate_spec_value(request["spec"])
        validate_access_value(request["access"])
        if request["spec"]["schema_version"] != 2 or request["access"]["schema_version"] != 2:
            raise ValueError
        if set(request["access"]["interfaces"]) != set(roles(request["spec"])):
            raise ValueError
        ids = request["new_worker_ids"]
        if not isinstance(ids, list) or len(ids) > 3 or len(set(ids)) != len(ids):
            raise ValueError
        for identifier in ids:
            if ops.uuid_value(identifier) != identifier:
                raise ValueError
    except (KeyError, TypeError, ValueError, ops.OpsError):
        raise failure("invalid_topology_request", invalid=True) from None
    return request


def transition(record, request):
    """Pure preview. Preserve every old account/history/grant and all identities."""
    validate_request(request)
    validate_record(record)
    for event in record["events"]:
        if event["kind"] == "topology_migrated" and event["data"]["migration_id"] == request["migration_id"]:
            if event["data"]["request_sha256"] != sha(request):
                raise failure("topology_replay_conflict")
            return copy.deepcopy(record), copy.deepcopy(event["data"]), False
    require_grant_idle(record)
    if (
        record["registration_id"] != request["registration_id"]
        or allowance_binding_sha256(record) != request["expected_binding_sha256"]
        or sha(record) != request["expected_state_sha256"]
        or source_digest() != request["expected_source_sha256"]
    ):
        raise failure("topology_expectation_changed")
    old, new = record["spec"], request["spec"]
    if old["context"] != new["context"]:
        raise failure("immutable_topology_context")
    old_vms = {vm["id"]: vm for vm in old["vms"]}
    new_vms = {vm["id"]: vm for vm in new["vms"]}
    added = set(new_vms) - set(old_vms)
    if (
        not set(old_vms) <= set(new_vms)
        or added != set(request["new_worker_ids"])
        or any(new_vms[key] != value for key, value in old_vms.items())
        or any(not new_vms[key]["role"].startswith("worker-") for key in added)
    ):
        raise failure("immutable_topology_accounts")
    for key in ("operator_cidr", "route"):
        if record["access"][key] != request["access"][key]:
            raise failure("immutable_topology_access")
    for role, interface in record["access"]["interfaces"].items():
        if request["access"]["interfaces"].get(role) != interface:
            raise failure("immutable_topology_access")
    candidate = copy.deepcopy(record)
    candidate.update(
        schema_version=JOURNAL_SCHEMA,
        spec=copy.deepcopy(new),
        access=copy.deepcopy(request["access"]),
        heartbeat=None,
    )
    for identifier in sorted(added):
        vm = new_vms[identifier]
        candidate["vms"][identifier] = {
            "role": vm["role"],
            "account": asdict(Allowance(limit=vm["active_seconds"])),
            "phase": "offloaded",
            "observed": None,
        }
    receipt = {
        "migration_id": request["migration_id"],
        "request_sha256": sha(request),
        "previous_state_sha256": sha(record),
        "previous_schema_version": record["schema_version"],
        "new_worker_ids": sorted(added),
        "preserved_accounts_sha256": sha(record["vms"]),
        "backup": "topology-backup-" + request["migration_id"] + ".json",
        "rollback": "no automatic downgrade after commit; retain backup and new journal",
    }
    candidate["events"].append({"kind": "topology_migrated", "data": receipt})
    validate_record(candidate)
    # This inverse allowlist makes accidental accounting/history modifications fatal.
    restored = copy.deepcopy(candidate)
    restored["events"].pop()
    for identifier in added:
        del restored["vms"][identifier]
    for key in ("schema_version", "spec", "access", "heartbeat"):
        restored[key] = record[key]
    if restored != record:
        raise failure("invalid_topology_transition")
    return candidate, receipt, True


def preview(journal, request):
    candidate, receipt, changed = transition(journal.read(), request)
    return {
        "would_change": changed,
        "receipt": receipt,
        "candidate_sha256": sha(candidate),
        "accounts": {
            identifier: {
                "role": vm["role"],
                "limit": vm["account"]["limit"],
                "consumed": vm["account"]["consumed"],
                "obligation": vm["account"]["obligation"],
            }
            for identifier, vm in candidate["vms"].items()
        },
        "activation_ready": False,
        "required": "fresh provider offload, installed compatible supervisor, and separate live approval",
    }


def apply(journal, request, *, fault=lambda point: None):
    """Atomic fixture-tested publication. No rollback can reset later consumption."""
    validate_request(request)
    # An old/live supervisor cannot race migration. No service is stopped here.
    with journal.supervisor_lock(), journal.connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute("SELECT body FROM pilot WHERE id=1").fetchone()
        if row is None:
            raise failure("missing_journal_history")
        current = validate_record(ops.parse_json(row[0]))
        candidate, receipt, changed = transition(current, request)
        if not changed:
            return receipt, False
        # Verify an existing crash-cut backup or durably create an exclusive one.
        backup = {
            "schema_version": 1,
            "journal_schema": current["schema_version"],
            "record": current,
            "record_sha256": sha(current),
            "request_sha256": sha(request),
        }
        with ops.private_directory(journal.root) as parent:
            name = receipt["backup"]
            if ops.inspect_child(parent, name):
                with ops.open_private_at(parent, name) as fd:
                    if ops.parse_json(ops.read_bounded_file(fd)) != backup:
                        raise failure("topology_backup_conflict")
            else:
                fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
                with os.fdopen(fd, "w") as stream:
                    stream.write(encode(backup))
                    stream.flush()
                    os.fsync(stream.fileno())
                os.fsync(parent)
            with ops.open_private_at(parent, name) as fd:
                if ops.parse_json(ops.read_bounded_file(fd)) != backup:
                    raise failure("topology_backup_verification_failed")
        fault("backup_durable")
        connection.execute("UPDATE pilot SET body=? WHERE id=1", (encode(candidate),))
        connection.execute(f"PRAGMA user_version={JOURNAL_SCHEMA}")
        fault("before_commit")
        connection.commit()
        fault("after_commit")
        return receipt, True


def run(args):
    request = validate_request(ops.read_document(args.request))
    journal = Journal(args.state_root)
    if args.pilot_command == "topology-preview":
        return ops.outcome("pilot topology-preview", "ok", data=preview(journal, request)), 0
    receipt, changed = apply(journal, request)
    return ops.outcome(
        "pilot topology-apply", "ok", data={"receipt": receipt, "applied": changed, "activation_ready": False}
    ), 0
