#!/usr/bin/env python3
"""Generate offline topology examples and conservative sizing; never opens a journal/provider."""

import argparse
import copy
import hashlib
import json
import sys
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
from flowdc_ops import validate_spec_value  # noqa: E402
from flowdc_pilot import SHUTDOWN_RESERVE_SECONDS  # noqa: E402
from flowdc_pilot_provider import validate_access_value  # noqa: E402
from flowdc_topology import action_lead_seconds, role_names, selected_ids, selection  # noqa: E402


def encode(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def synthetic_id(name):
    return str(uuid5(NAMESPACE_URL, "https://flowdc-fixture.invalid/" + name))


def example(workers):
    names = role_names(workers)
    ids = {role: synthetic_id(role) for role in names}
    spec = {
        "schema_version": 2,
        "context": {
            "project_id": synthetic_id("project"),
            "region": "EXAMPLE-OFFLINE",
            "auth_url": "https://identity.fixture.invalid/v3",
        },
        "topology": {
            "manager": ids["manager"],
            "origin": ids["origin"],
            "workers": [ids[role] for role in names if role.startswith("worker")],
        },
        "vms": [{"role": role, "id": ids[role], "active_seconds": 1800, "rate": None} for role in names],
    }
    # Documentation-network addresses and deterministic fixture UUIDs are shape
    # examples, never discovered live identities or a real route attestation.
    access = {
        "schema_version": 2,
        "operator_cidr": "192.0.2.200/32",
        "route": {
            "mode": "private",
            "external_network_id": None,
            "router_id": None,
            "operator_route_verified": True,
        },
        "interfaces": {
            role: {
                "port_id": synthetic_id(role + "-port"),
                "network_id": synthetic_id("network"),
                "subnet_id": synthetic_id("subnet"),
                "fixed_ip": f"192.0.2.{10 + role_names(4).index(role)}",
            }
            for role in names
        },
    }
    return validate_spec_value(spec), validate_access_value(access)


def sizing(spec, worker_ids=None, window_seconds=1800):
    """Calculate scheduling bounds without treating initial limits as remaining allowance."""
    spec = validate_spec_value(copy.deepcopy(spec))
    if type(window_seconds) is not int or not 600 < window_seconds <= 1800:
        raise ValueError("existing experiment window must be 601..1800 seconds")
    chosen = selection(spec, worker_ids)
    record = {"spec": spec, "schema_version": 3, "selection": chosen}
    ids = selected_ids(record)
    lead = action_lead_seconds(len(ids))
    stop = window_seconds - SHUTDOWN_RESERVE_SECONDS - lead
    if stop <= 0:
        raise ValueError("window has no work/collection time after stop and cleanup reserves")
    budgets = []
    for vm in spec["vms"]:
        if vm["id"] not in ids:
            continue
        if window_seconds > vm["active_seconds"]:
            raise ValueError("window exceeds a selected VM's configured lifetime limit")
        rate = vm["rate"]
        budgets.append(
            {
                "id": vm["id"],
                "role": vm["role"],
                "configured_limit_seconds": vm["active_seconds"],
                "remaining_allowance_seconds": None,
                "required_remaining_seconds": window_seconds,
                "conditional_su": rate["su_per_hour"] * window_seconds / 3600
                if rate is not None and rate["verified"]
                else None,
                "rate_evidence": rate,
            }
        )
    return {
        "schema": "flowdc-offline-topology-plan-v1",
        "activation_ready": False,
        "spec_sha256": hashlib.sha256(encode(spec)).hexdigest(),
        "selection": chosen,
        "selected_ids": list(ids),
        "window_seconds": window_seconds,
        "maximum_stop_after_seconds": stop,
        "stop_scheduling_lead_seconds": lead,
        "cleanup_reserve_seconds": SHUTDOWN_RESERVE_SECONDS,
        "per_vm": budgets,
        "conditional_total_su": sum(v["conditional_su"] for v in budgets)
        if all(v["conditional_su"] is not None for v in budgets)
        else None,
        "cost_domain": "full configured window at supplied rates, conditional on timely offload; provider outages can exceed it",
        "pending": [
            "fresh allocation/expiry, inventory and flavor/rate evidence",
            "current per-UUID remaining allowance and idle obligations",
            "verified SSH/TLS trust and exclusive native endpoint containment",
            "installation/migration preview and explicit concrete human approval",
        ],
    }


def write_new(path, value):
    with path.open("xb") as stream:
        stream.write(encode(value))


def generate(output):
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    for count in (1, 2, 4):
        spec, access = example(count)
        write_new(output / f"{count + 2}-vm-spec.json", spec)
        write_new(output / f"{count + 2}-vm-access.fixture.json", access)
        write_new(output / f"{count + 2}-vm-plan.json", sizing(spec))
    spec, _ = example(4)
    for count, roles in ((1, ["worker-3"]), (2, ["worker", "worker-3"]), (4, list(role_names(4)[1:-1]))):
        chosen = [synthetic_id(role) for role in roles]
        write_new(output / f"6-enrolled-{count}-selected-plan.json", sizing(spec, chosen))
    write_new(
        output / "README.json",
        {
            "fixture_only": True,
            "live_ids_discovered": False,
            "notice": "Synthetic UUIDs, .invalid identity endpoint and documentation addresses. Route verified=true is fixture input only. Never register these examples against a real journal.",
            "instructions": "docs/BOUNDED-TOPOLOGY.md and docs/PRODUCTION-CHECKPOINT.md",
        },
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    examples = commands.add_parser("examples", help="Write validated synthetic 3/4/6-VM examples")
    examples.add_argument("--output", required=True, type=Path)
    plan = commands.add_parser(
        "plan", help="Size a supplied specification offline; does not authorize activation"
    )
    plan.add_argument("--spec", required=True, type=Path)
    plan.add_argument("--worker-id", action="append")
    plan.add_argument("--window-seconds", type=int, default=1800)
    plan.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "examples":
        generate(args.output)
    else:
        value = sizing(json.loads(args.spec.read_bytes()), args.worker_id, args.window_seconds)
        write_new(args.output, value)
    print(json.dumps({"output": str(args.output), "activation_ready": False}))


if __name__ == "__main__":
    main()
