"""Versioned bounded topology identities; no provider, journal or permission mutation."""

from uuid import UUID

LEGACY_ROLES = ("manager", "worker", "origin")
TOPOLOGY_SCHEMA = 2
JOURNAL_SCHEMA = 3  # 2 is the historical supervisor-upgrade fence, never a topology.
UPGRADE_FENCE = 4


def worker_roles(count):
    if type(count) is not int or count not in (1, 2, 4):
        raise ValueError("topology_requires_1_2_4_workers")
    # Keep the first worker's historical role label and every UUID binding intact.
    return ("worker",) + tuple(f"worker-{i}" for i in range(2, count + 1))


def role_names(count):
    return ("manager", *worker_roles(count), "origin")


def validate_roles(names, version):
    names = list(names)
    if type(version) is not int or version not in (1, TOPOLOGY_SCHEMA):
        raise ValueError("unsupported_topology_schema")
    expected = LEGACY_ROLES if version == 1 else role_names(len(names) - 2)
    if len(names) != len(expected) or set(names) != set(expected):
        raise ValueError("invalid_topology_roles")
    return expected


def roles(spec):
    version = spec["schema_version"]
    expected = validate_roles((vm["role"] for vm in spec["vms"]), version)
    ids = [vm["id"] for vm in spec["vms"]]
    if any(not isinstance(value, str) or str(UUID(value)) != value for value in ids) or len(set(ids)) != len(
        ids
    ):
        raise ValueError("invalid_topology_uuid")
    if version == TOPOLOGY_SCHEMA:
        assigned = {vm["role"]: vm["id"] for vm in spec["vms"]}
        topology = {
            "manager": assigned["manager"],
            "origin": assigned["origin"],
            "workers": [assigned[role] for role in worker_roles(len(ids) - 2)],
        }
        if spec.get("topology") != topology:
            raise ValueError("topology_identity_mismatch")
    elif "topology" in spec:
        raise ValueError("legacy_topology_has_new_fields")
    return expected


def from_legacy(spec):
    """An explicit new-schema copy. The caller must preserve the old snapshot."""
    import copy

    roles(spec)
    value = copy.deepcopy(spec)
    value["schema_version"] = TOPOLOGY_SCHEMA
    assigned = {vm["role"]: vm["id"] for vm in value["vms"]}
    value["topology"] = {
        "manager": assigned["manager"],
        "origin": assigned["origin"],
        "workers": [assigned[role] for role in worker_roles(len(assigned) - 2)],
    }
    return value


def record_roles(record):
    return roles(record["spec"])


def is_worker(role):
    return role in worker_roles(4)
