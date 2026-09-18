"""Private, singleton pilot journal. No transaction spans a provider call."""

import fcntl
import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict
from uuid import uuid4

import flowdc_ops as ops
from flowdc_pilot import AccountingError, Allowance, ClockSample

JOURNAL_VERSION = 1
DB_NAME = "pilot.sqlite3"
BINDING_NAME = "pilot-binding.json"
EMERGENCY = (
    "Keep supervision running. In the verified allocation/region, inspect each registered VM in Horizon; "
    "shelve it and confirm SHELVED_OFFLOADED (SHUTOFF/SHELVED is insufficient). "
    "If offload fails, contact the allocation/cloud operator immediately. Preserve the journal, "
    "then run pilot reconcile; restore only recorded pilot-owned network changes."
)


def failure(code, *, invalid=False):
    return ops.OpsError(
        code, "Pilot prerequisite or recovery checkpoint requires attention.", EMERGENCY, 2 if invalid else 3
    )


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def allowance(value):
    try:
        value = dict(value)
        if value["sample"] is not None:
            value["sample"] = ClockSample(**value["sample"])
        return Allowance(**value)
    except (KeyError, TypeError, ValueError):
        raise failure("invalid_account_history") from None


def validate_record(record):
    try:
        ops.fields(
            record,
            (
                "schema_version",
                "registration_id",
                "profile_path",
                "spec",
                "access",
                "desired",
                "window",
                "heartbeat",
                "service",
                "checkpoint",
                "events",
                "network",
                "vms",
            ),
            optional=("fake_actions",),
        )
        if record["schema_version"] != JOURNAL_VERSION or len(record["vms"]) != 3:
            raise ValueError
        from flowdc_pilot_provider import validate_access_value

        validate_access_value(record["access"])
        ops.uuid_value(record["registration_id"])
        ops.absolute_path(record["profile_path"])
        spec = ops.fields(record["spec"], ("schema_version", "context", "vms"))
        ops.version(spec)
        ops.validate_context(spec["context"])
        if len(spec["vms"]) != 3 or {vm["role"] for vm in spec["vms"]} != {"manager", "worker", "origin"}:
            raise ValueError
        if set(record["vms"]) != {vm["id"] for vm in spec["vms"]}:
            raise ValueError
        for selected in spec["vms"]:
            ops.uuid_value(selected["id"])
            account = allowance(record["vms"][selected["id"]]["account"])
            if (
                account.limit != selected["active_seconds"]
                or record["vms"][selected["id"]]["role"] != selected["role"]
            ):
                raise ValueError
        for vm in record["vms"].values():
            account = allowance(vm["account"])
            if (vm["phase"] in ("requested", "unshelve_intent") and not account.obligation) or (
                vm["phase"] == "offloaded" and account.obligation
            ):
                raise ValueError
            if vm["phase"] not in ("pending", "offloaded", "unshelve_intent", "requested", "verify_offload"):
                raise ValueError
            if "cleanup_attempt" in vm:
                ClockSample(**vm["cleanup_attempt"]["clock"])
                ops.integer(vm["cleanup_attempt"]["order"], 1)
            if vm["observed"] is not None:
                ClockSample(**vm["observed"]["clock"])
                if vm["observed"]["state"] not in ops.STATES:
                    raise ValueError
        service = record["service"]
        if service is not None:
            if not isinstance(service, dict) or "unit" not in service:
                raise ValueError
            if service["unit"] != "fake-only":
                ops.fields(service, ("unit", "release", "digest", "interpreter", "interpreter_digest"))
                ops.absolute_path(service["release"])
                ops.absolute_path(service["interpreter"])
                if not all(
                    isinstance(service[key], str) and len(service[key]) == 64
                    for key in ("digest", "interpreter_digest")
                ):
                    raise ValueError
        if record["heartbeat"] is not None:
            ClockSample(**record["heartbeat"])
        if record["desired"] not in ("idle", "run", "stop") or not isinstance(record["events"], list):
            raise ValueError
        if record["desired"] == "run" and record["window"] is None:
            raise ValueError
        if record["window"] is not None:
            ops.fields(record["window"], ("seconds", "inspection"))
            ops.integer(record["window"]["seconds"], 601, 7200)
            if type(record["window"]["inspection"]) is not bool:
                raise ValueError
        network = record["network"]
        ops.fields(
            network,
            ("intents", "original", "ready", "rolled_back", "generation", "seen_groups", "seen_floating"),
            optional=("configured", "route_checked", "floating_id"),
        )
        ops.uuid_value(network["generation"])
        for key in ("ready", "rolled_back", "seen_floating"):
            if type(network[key]) is not bool:
                raise ValueError
        for role, groups in network["original"].items():
            if role not in ("manager", "worker", "origin"):
                raise ValueError
            for group in groups:
                ops.uuid_value(group)
        if not isinstance(network["intents"], dict) or not isinstance(network["seen_groups"], dict):
            raise ValueError
        return record
    except (KeyError, TypeError, ValueError, AttributeError, AccountingError, ops.OpsError):
        raise failure("invalid_journal_history") from None


@contextmanager
def private_lock(parent, name, *, blocking=True):
    fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=parent)
    try:
        ops.private_metadata(os.fstat(fd))
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        except BlockingIOError:
            raise failure("supervisor_already_running") from None
        yield fd
    finally:
        os.close(fd)


class Journal:
    """Open only an existing registered journal; never recreate lost history."""

    def __init__(self, root):
        self.root = ops.absolute_path(str(root))

    @contextmanager
    def connection(self):
        with ops.private_directory(self.root) as parent:
            for name in (DB_NAME, DB_NAME + "-journal", DB_NAME + "-wal", DB_NAME + "-shm"):
                ops.inspect_child(parent, name, required=name == DB_NAME)
            # The descriptor anchors the private directory for SQLite's auxiliary files.
            connection = None
            try:
                connection = sqlite3.connect(
                    f"file:/proc/self/fd/{parent}/{DB_NAME}?mode=rw", uri=True, timeout=2
                )
                connection.execute("PRAGMA synchronous=FULL")
                connection.execute("PRAGMA trusted_schema=OFF")
                if connection.execute("PRAGMA user_version").fetchone()[0] != JOURNAL_VERSION:
                    raise failure("unsupported_journal")
                yield connection
            except sqlite3.Error:
                raise failure("journal_unavailable_or_corrupt") from None
            finally:
                if connection is not None:
                    connection.close()

    def read(self):
        with self.connection() as connection:
            row = connection.execute("SELECT body FROM pilot WHERE id=1").fetchone()
            if row is None:
                raise failure("missing_journal_history")
            return validate_record(ops.parse_json(row[0]))

    def change(self, update):
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT body FROM pilot WHERE id=1").fetchone()
            if row is None:
                raise failure("missing_journal_history")
            record = validate_record(ops.parse_json(row[0]))
            binding = encode(
                {
                    key: record[key]
                    for key in ("schema_version", "registration_id", "profile_path", "spec", "access")
                }
            )
            previous = {key: allowance(vm["account"]) for key, vm in record["vms"].items()}
            update(record)
            validate_record(record)
            if binding != encode(
                {
                    key: record[key]
                    for key in ("schema_version", "registration_id", "profile_path", "spec", "access")
                }
            ):
                raise failure("immutable_journal_binding")
            for key, old in previous.items():
                new = allowance(record["vms"][key]["account"])
                if new.consumed < old.consumed or (old.uncertain and not new.uncertain):
                    raise failure("account_cannot_be_replenished")
            connection.execute("UPDATE pilot SET body=? WHERE id=1", (encode(record),))
            connection.commit()
            return record

    def event(self, record, kind, data):
        # Events are embedded in the same atomic record as the state transition.
        # Never store raw provider text. Retain all intents; the pilot has bounded work.
        if record["events"] and record["events"][-1]["kind"] == kind and record["events"][-1]["data"] == data:
            record["events"][-1]["count"] = record["events"][-1].get("count", 1) + 1
        else:
            record["events"].append({"kind": kind, "data": data})

    @contextmanager
    def supervisor_lock(self):
        with ops.private_directory(self.root) as parent:
            with private_lock(parent, "supervisor.lock", blocking=False):
                yield

    def supervisor_locked(self):
        try:
            with self.supervisor_lock():
                return False
        except ops.OpsError as exc:
            if exc.code == "supervisor_already_running":
                return True
            raise


def fresh_network():
    return {
        "intents": {},
        "original": {},
        "ready": False,
        "rolled_back": True,
        "generation": str(uuid4()),
        "seen_groups": {},
        "seen_floating": False,
    }


def register(profile_path, root, spec, access):
    """Bind once, before creating a database. Partial creation requires recovery.

    A marker adjacent to the existing profile prevents a new state-root/run name
    from silently minting another allowance. Manual removal of both marker and
    journal cannot be detected locally and is explicitly prohibited operationally.
    """
    profile_path = ops.absolute_path(str(profile_path))
    root = ops.absolute_path(str(root))
    with ops.private_directory(profile_path.parent) as config:
        with private_lock(config, "pilot-registration.lock"):
            try:
                with ops.open_private_at(config, BINDING_NAME) as fd:
                    binding = ops.parse_json(ops.read_bounded_file(fd))
            except FileNotFoundError:
                binding = None
            if binding is not None:
                if binding != {"schema_version": 1, "state_root": str(root)}:
                    raise failure("allowance_already_bound")
                existing = Journal(root).read()
                if (
                    existing["spec"] != spec
                    or existing["access"] != access
                    or existing["profile_path"] != str(profile_path)
                ):
                    raise failure("immutable_pilot_binding")
                return Journal(root)
            with ops.private_directory(root, create=True) as parent:
                for name in (DB_NAME, DB_NAME + "-journal", DB_NAME + "-wal", DB_NAME + "-shm"):
                    if ops.inspect_child(parent, name):
                        raise failure("unbound_existing_history")
                fd = os.open(
                    BINDING_NAME, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=config
                )
                with os.fdopen(fd, "w") as stream:
                    stream.write(encode({"schema_version": 1, "state_root": str(root)}))
                    stream.flush()
                    os.fsync(stream.fileno())
                os.fsync(config)
                fd = os.open(
                    DB_NAME, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent
                )
                os.close(fd)
                record = {
                    "schema_version": 1,
                    "registration_id": str(uuid4()),
                    "profile_path": str(profile_path),
                    "spec": spec,
                    "access": access,
                    "desired": "idle",
                    "window": None,
                    "heartbeat": None,
                    "service": None,
                    "checkpoint": None,
                    "events": [],
                    "network": fresh_network(),
                    "vms": {
                        vm["id"]: {
                            "role": vm["role"],
                            "account": asdict(Allowance(limit=vm["active_seconds"])),
                            "phase": "offloaded",
                            "observed": None,
                        }
                        for vm in spec["vms"]
                    },
                }
                try:
                    with sqlite3.connect(f"/proc/self/fd/{parent}/{DB_NAME}") as connection:
                        connection.execute("PRAGMA synchronous=FULL")
                        connection.execute("PRAGMA journal_mode=DELETE")
                        connection.execute(f"PRAGMA user_version={JOURNAL_VERSION}")
                        connection.execute(
                            "CREATE TABLE pilot (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL)"
                        )
                        connection.execute("INSERT INTO pilot VALUES (1, ?)", (encode(record),))
                        connection.commit()
                except sqlite3.Error:
                    raise failure("journal_initialization_incomplete") from None
                os.fsync(parent)
    return Journal(root)
