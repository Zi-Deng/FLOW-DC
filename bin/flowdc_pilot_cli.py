"""Additive pilot CLI and explicit immutable user-service installation."""

import errno
import hashlib
import os
import re
import signal
import stat
import sys
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import flowdc_ops as ops
from flowdc_pilot_journal import (
    EMERGENCY,
    Journal,
    allowance,
    allowance_binding_sha256,
    encode,
    failure,
    find_grant_receipt,
    grant_receipts,
    load_grant_request,
    private_lock,
    register,
    require_grant_expectation,
    require_grant_idle,
    validate_record,
)
from flowdc_pilot_provider import Provider, validate_access
from flowdc_pilot_supervisor import (
    OBSERVATION_SECONDS,
    Supervisor,
    cleanup_diagnostics,
    heartbeat_fresh,
    request,
    sample_clock,
)

UNIT = "flowdc-pilot.service"
FATAL_SERVICE_EXIT = 78
PERMANENT_VERIFICATION_FAILURES = {
    "service_not_installed",
    "immutable_release_required",
    "installed_release_changed",
    "installed_interpreter_changed",
    "installed_service_unreadable",
    "user_systemd_invocation_required",
    "user_systemd_identity_mismatch",
    "unsafe_path",
    "unsafe_permissions",
}
MODULES = (
    "flowdc_ops.py",
    "flowdc_pilot.py",
    "flowdc_pilot_journal.py",
    "flowdc_pilot_provider.py",
    "flowdc_pilot_supervisor.py",
    "flowdc_pilot_cli.py",
)


def arguments(commands):
    pilot = commands.add_parser("pilot", help="Prepare and control the bounded pilot.", allow_abbrev=False)
    actions = pilot.add_subparsers(dest="pilot_command", required=True)
    for action in (
        "prepare",
        "start",
        "status",
        "stop",
        "reconcile",
        "supervise",
        "upgrade-supervisor",
        "extend-allowance",
        "runtime-check",
    ):
        parser = actions.add_parser(action, allow_abbrev=False)
        if action == "runtime-check":
            parser.add_argument("--profile", required=True)
        else:
            parser.add_argument("--state-root", default=str(Path.home() / ".local/share/flowdc-ops"))
        if action == "prepare":
            parser.add_argument("--profile", required=True)
            parser.add_argument("--spec", required=True)
            parser.add_argument("--inventory", required=True)
            parser.add_argument("--access", required=True)
            parser.add_argument("--install-supervisor", action="store_true")
        if action == "upgrade-supervisor":
            parser.add_argument("--expected-current-digest", required=True)
            parser.add_argument("--expected-candidate-digest", required=True)
            parser.add_argument(
                "--candidate-source", help="Absolute directory of the six reviewed candidate modules."
            )
            parser.add_argument("--recover", choices=("complete", "rollback"))
        if action == "extend-allowance":
            parser.add_argument("--grant", required=True)
        if action == "start":
            parser.add_argument("--window-seconds", type=int, default=1800)
            parser.add_argument(
                "--full-window",
                action="store_true",
                help="Allow over 1800 seconds, within cumulative balance.",
            )


def systemctl(*args):
    code, raw = ops.run_bounded(["/usr/bin/systemctl", "--user", *args], timeout=5, local=True)
    if code:
        raise failure("user_systemd_unavailable")
    return raw


def require_persistent_session():
    try:
        code, raw = ops.run_bounded(
            ["/usr/bin/loginctl", "show-user", str(os.getuid()), "--property=Linger", "--value"],
            timeout=5,
            local=True,
        )
    except (ops.OpsError, OSError):
        raise failure("user_lingering_probe_failed") from None
    if code or raw.strip() not in (b"yes", b"no"):
        raise failure("user_lingering_probe_failed")
    if raw.strip() == b"no":
        raise failure("user_lingering_required")


def quote_unit(value):
    # systemd specifier and environment expansion are distinct from shell quoting.
    if any(char in value for char in "\n\r\x00"):
        raise failure("unsafe_unit_path", invalid=True)
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%").replace("$", "$$") + '"'


def write_new(parent, name, content, *, mode=0o600):
    try:
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode, dir_fd=parent)
    except FileExistsError:
        with ops.open_private_at(parent, name) as fd:
            if ops.read_bounded_file(fd) != content:
                raise failure("installed_file_conflict") from None
        return
    with os.fdopen(fd, "wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.fsync(parent)


def service_unit(service, root):
    return (
        "[Unit]\nDescription=FLOW-DC bounded pilot supervisor\nStartLimitIntervalSec=0\n"
        "[Service]\nType=simple\nRestart=always\nRestartSec=2\nRestartPreventExitStatus=78\nTimeoutStopSec=infinity\nUMask=0077\n"
        "NoNewPrivileges=yes\nStandardOutput=null\nStandardError=journal\nLogRateLimitIntervalSec=30s\nLogRateLimitBurst=3\n"
        f"ExecStart={quote_unit(service['interpreter'])} -E -s -B {quote_unit(str(Path(service['release']) / 'flowdc_ops.py'))} "
        f"pilot supervise --state-root {quote_unit(str(root))}\n"
        "[Install]\nWantedBy=default.target\n"
    ).encode()


def install(journal):
    source = Path(__file__).resolve().parent
    content = {name: (source / name).read_bytes() for name in MODULES}
    digest = hashlib.sha256(b"".join(content[name] for name in MODULES)).hexdigest()
    release = journal.root / "releases" / ("pilot-" + digest)
    with ops.private_directory(release, create=True) as parent:
        for name, raw in content.items():
            write_new(parent, name, raw)
    interpreter = str(Path(sys.executable).resolve())
    with ops.private_directory(Path(interpreter).parent, private=False) as parent:
        info = os.stat(Path(interpreter).name, dir_fd=parent, follow_symlinks=False)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid not in (0, os.geteuid(), os.stat("/").st_uid)
            or info.st_mode & 0o022
            or not info.st_mode & 0o111
        ):
            raise failure("unsafe_interpreter", invalid=True)
    interpreter_digest = hashlib.sha256(Path(interpreter).read_bytes()).hexdigest()
    service = {
        "unit": UNIT,
        "release": str(release),
        "digest": digest,
        "interpreter": interpreter,
        "interpreter_digest": interpreter_digest,
    }
    unit = service_unit(service, journal.root)
    unit_root = Path.home() / ".config/systemd/user"
    with ops.private_directory(unit_root, create=True, private=False) as parent:
        write_new(parent, UNIT, unit)

    def save(current):
        if current["service"] not in (None, service):
            raise failure("service_binding_conflict")
        current["service"] = service

    journal.change(save)
    systemctl("daemon-reload")
    systemctl("enable", "--now", UNIT)


def trusted_bytes(path, *, executable=False):
    """Read pinned source/interpreter through a checked, non-symlink descriptor."""
    path = ops.absolute_path(str(path))
    with ops.private_directory(path.parent, private=False) as parent:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid not in (0, os.geteuid(), os.stat("/").st_uid)
                or info.st_mode & 0o022
                or (executable and not info.st_mode & 0o111)
            ):
                raise failure("unsafe_release_file")
            with os.fdopen(os.dup(fd), "rb") as stream:
                raw = stream.read(64 * 1024 * 1024 + 1)
                if len(raw) > 64 * 1024 * 1024:
                    raise failure("release_file_limit")
                return raw
        finally:
            os.close(fd)


def verify_release(service, root):
    if (
        service["unit"] != UNIT
        or not re.fullmatch(r"[0-9a-f]{64}", service["digest"])
        or service["release"] != str(root / "releases" / ("pilot-" + service["digest"]))
    ):
        raise failure("unexpected_release_provenance")
    with ops.private_directory(Path(service["release"])) as parent:
        content = []
        for name in MODULES:
            with ops.open_private_at(parent, name) as fd:
                content.append(ops.read_bounded_file(fd))
    if hashlib.sha256(b"".join(content)).hexdigest() != service["digest"]:
        raise failure("installed_release_changed")
    interpreter = trusted_bytes(service["interpreter"], executable=True)
    if hashlib.sha256(interpreter).hexdigest() != service["interpreter_digest"]:
        raise failure("installed_interpreter_changed")


def stage_candidate(journal, expected, source=None):
    source = ops.absolute_path(source) if source is not None else Path(__file__).resolve().parent
    content = {name: trusted_bytes(source / name) for name in MODULES}
    digest = hashlib.sha256(b"".join(content.values())).hexdigest()
    if digest != expected:
        raise failure("candidate_release_mismatch")
    for name, raw in content.items():
        try:
            compile(raw, name, "exec")
        except (SyntaxError, ValueError):
            raise failure("invalid_candidate_source") from None
    interpreter = str(Path(sys.executable).resolve())
    service = {
        "unit": UNIT,
        "release": str(journal.root / "releases" / ("pilot-" + digest)),
        "digest": digest,
        "interpreter": interpreter,
        "interpreter_digest": hashlib.sha256(trusted_bytes(interpreter, executable=True)).hexdigest(),
    }
    with ops.private_directory(Path(service["release"]), create=True) as parent:
        for name, raw in content.items():
            write_new(parent, name, raw)
    verify_release(service, journal.root)
    return service


def unit_bytes():
    path = Path.home() / ".config/systemd/user" / UNIT
    with ops.private_directory(path.parent, private=False) as parent:
        with ops.open_private_at(parent, path.name) as fd:
            return ops.read_bounded_file(fd)


def verify_unit_origin(*, stopped=False, reloaded=False, running=False):
    raw = systemctl(
        "show",
        UNIT,
        "--property=FragmentPath",
        "--property=DropInPaths",
        "--property=NeedDaemonReload",
        "--property=MainPID",
        "--property=ActiveState",
    )
    lines = raw.decode("utf-8", errors="replace").splitlines()
    expected = {
        "FragmentPath=" + str(Path.home() / ".config/systemd/user" / UNIT),
        "DropInPaths=",
    }
    if reloaded:
        expected.add("NeedDaemonReload=no")
    if running and (
        "ActiveState=active" not in lines
        or not any(re.fullmatch(r"MainPID=[1-9][0-9]*", line) for line in lines)
    ):
        raise failure("supervisor_not_ready")
    if stopped:
        expected.add("MainPID=0")
        if not ({"ActiveState=inactive", "ActiveState=failed"} & set(lines)):
            raise failure("maintenance_service_not_stopped")
    if not expected.issubset(lines):
        raise failure("unexpected_unit_provenance")


def require_idle(record):
    if (
        record["desired"] != "idle"
        or not record["network"]["rolled_back"]
        or record["network"]["ready"]
        or any(
            vm["phase"] != "offloaded"
            or allowance(vm["account"]).obligation
            or vm["observed"] is None
            or vm["observed"]["state"] != "SHELVED_OFFLOADED"
            for vm in record["vms"].values()
        )
    ):
        raise failure("maintenance_idle_required")


def maintenance_state(connection):
    row = connection.execute("SELECT body FROM pilot_maintenance ORDER BY id DESC LIMIT 1").fetchone()
    if row is None:
        raise failure("maintenance_history_required")
    value = ops.parse_json(row[0])
    ops.fields(value, ("old", "candidate", "phase"))
    validate_record(value["old"])
    require_idle(value["old"])
    if value["old"]["service"] is None or value["candidate"] is None:
        raise failure("maintenance_history_required")
    if value["phase"] not in ("blocked", "unit", "binding", "published_complete", "published_rollback"):
        raise failure("maintenance_history_required")
    # Validate the candidate binding using the unchanged journal schema.
    validate_record(dict(value["old"], service=value["candidate"]))
    return value


def maintenance_record(connection):
    row = connection.execute("SELECT body FROM pilot WHERE id=1").fetchone()
    if row is None:
        raise failure("missing_journal_history")
    return validate_record(ops.parse_json(row[0]))


def maintenance_update(journal, phase, *, service=None, publish=False):
    with journal.connection(maintenance=True) as connection:
        connection.execute("BEGIN IMMEDIATE")
        value = maintenance_state(connection)
        current = maintenance_record(connection)
        old = value["old"]
        if current not in (old, dict(old, service=value["candidate"])):
            raise failure("maintenance_history_changed")
        if service is not None:
            if service not in (old["service"], value["candidate"]):
                raise failure("maintenance_history_changed")
            current = dict(old, service=service)
        if publish:
            # A legacy start checks heartbeat even while we still own the actor lock.
            current["heartbeat"] = None
        connection.execute("UPDATE pilot SET body=? WHERE id=1", (encode(current),))
        value["phase"] = phase
        connection.execute(
            "UPDATE pilot_maintenance SET body=? WHERE id=(SELECT max(id) FROM pilot_maintenance)",
            (encode(value),),
        )
        if publish:
            connection.execute("PRAGMA user_version=1")
        connection.commit()


def replace_unit(expected, target):
    from uuid import uuid4

    path = Path.home() / ".config/systemd/user" / UNIT
    with ops.private_directory(path.parent, private=False) as parent:
        with ops.open_private_at(parent, UNIT) as fd:
            if ops.read_bounded_file(fd) not in expected:
                raise failure("unexpected_unit_content")
        temporary = ".flowdc-upgrade-" + uuid4().hex
        write_new(parent, temporary, target)
        os.replace(temporary, UNIT, src_dir_fd=parent, dst_dir_fd=parent)
        os.fsync(parent)


def upgrade_supervisor(journal, args):
    """Durably exclude even legacy actors before stopping or replacing a service.

    Journal version 2 exists only during maintenance. Both old and new ordinary
    clients refuse it. The snapshot and version transition share one transaction;
    no external backup is substituted for the live account history.
    """
    for digest in (args.expected_current_digest, args.expected_candidate_digest):
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise failure("expected_release_digest_required", invalid=True)
    with (
        ops.private_directory(journal.root) as parent,
        private_lock(parent, "maintenance.lock", blocking=False),
    ):
        if args.recover:
            with journal.connection(maintenance=True) as connection:
                value = maintenance_state(connection)
                version = connection.execute("PRAGMA user_version").fetchone()[0]
            old, candidate = value["old"], value["candidate"]
        else:
            old = journal.read()
            require_idle(old)
            if old["service"] is None or old["service"]["digest"] != args.expected_current_digest:
                raise failure("current_release_mismatch")
            verify_release(old["service"], journal.root)
            if unit_bytes() != service_unit(old["service"], journal.root):
                raise failure("unexpected_unit_content")
            verify_unit_origin(reloaded=True)
            candidate = stage_candidate(
                journal, args.expected_candidate_digest, getattr(args, "candidate_source", None)
            )
            # Re-read under the legacy journal lock: an intervening start wins
            # and makes maintenance refuse, never overwrites its obligations.
            with journal.connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                current = maintenance_record(connection)
                require_idle(current)
                if current["service"] != old["service"]:
                    raise failure("current_release_mismatch")
                old = current
                value = {"old": old, "candidate": candidate, "phase": "blocked"}
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS pilot_maintenance (id INTEGER PRIMARY KEY, body TEXT NOT NULL)"
                )
                connection.execute("INSERT INTO pilot_maintenance(body) VALUES (?)", (encode(value),))
                connection.execute("PRAGMA user_version=2")
                connection.commit()
            version = 2
        if (
            old["service"]["digest"] != args.expected_current_digest
            or candidate["digest"] != args.expected_candidate_digest
        ):
            raise failure("maintenance_release_mismatch")
        verify_release(old["service"], journal.root)
        verify_release(candidate, journal.root)
        target = old["service"] if args.recover == "rollback" else candidate
        phase = "published_rollback" if args.recover == "rollback" else "published_complete"
        if version == 1:
            # Publication already committed. Do not restore a snapshot over any
            # later activity. An idempotent completion may only start its service.
            if value["phase"] != phase or journal.read() != dict(old, service=target, heartbeat=None):
                raise failure("maintenance_already_published")
            if unit_bytes() != service_unit(target, journal.root):
                raise failure("unexpected_unit_content")
            verify_unit_origin(reloaded=True)
        else:
            if unit_bytes() not in (
                service_unit(old["service"], journal.root),
                service_unit(candidate, journal.root),
            ):
                raise failure("unexpected_unit_content")
            verify_unit_origin()
            systemctl("stop", UNIT)
            verify_unit_origin(stopped=True)
            with journal.supervisor_lock():
                # Read-only fresh cloud verification. No accounting/state rewrite
                # and no provider mutation is allowed in this maintenance path.
                Provider(ops.load_profile(old["profile_path"])).verify_idle(old)
                verify_release(old["service"], journal.root)
                verify_release(candidate, journal.root)
                replace_unit(
                    (service_unit(old["service"], journal.root), service_unit(candidate, journal.root)),
                    service_unit(target, journal.root),
                )
                maintenance_update(journal, "unit")
                systemctl("daemon-reload")
                verify_unit_origin(stopped=True, reloaded=True)
                maintenance_update(journal, "binding", service=target)
                if unit_bytes() != service_unit(target, journal.root):
                    raise failure("unexpected_unit_content")
                verify_release(target, journal.root)
                maintenance_update(journal, phase, service=target, publish=True)
        systemctl("start", UNIT)
    return ops.outcome(
        "pilot upgrade-supervisor",
        "ok",
        data={"release_digest": target["digest"], "accounts_preserved": True},
        next_actions=[
            "Verify supervisor heartbeat and provenance with pilot status, then reconcile before any start."
        ],
    ), 0


def verify_grant_controller(journal, record, *, clock=sample_clock):
    """Probe the local session/manager only outside the journal transaction."""
    verify_grant_local(journal, record, clock=clock)
    require_persistent_session()
    verify_unit_origin(reloaded=True, running=True)


def verify_grant_local(journal, record, *, clock=sample_clock):
    """Recheck pinned files, heartbeat and actor lock without subprocesses."""
    service = record["service"]
    if service is None:
        raise failure("service_not_installed")
    if (
        str(Path(__file__).resolve().parent) != service["release"]
        or str(Path(sys.executable).resolve()) != service["interpreter"]
    ):
        raise failure("immutable_release_required")
    verify_release(service, journal.root)
    if unit_bytes() != service_unit(service, journal.root):
        raise failure("unexpected_unit_content")
    if not heartbeat_fresh(record, clock()) or not journal.supervisor_locked():
        raise failure("supervisor_not_ready")


def grant_outcome(receipt, applied, record):
    return ops.outcome(
        "pilot extend-allowance",
        "ok",
        data={
            "result": "applied" if applied else "already_applied",
            "receipt": receipt,
            "current_balances": {
                vm["role"]: asdict(allowance(vm["account"])) for vm in record["vms"].values()
            },
            "current_readiness_verified": False,
        },
        next_actions=[
            "Receipt confirms historical application only. Inspect pilot status; any later start "
            "requires its own readiness checks. Re-prepare experiments against the new binding."
        ],
    ), 0


def extend_allowance(journal, path, *, clock=sample_clock):
    request_value = load_grant_request(path)
    record = journal.read()
    receipt = find_grant_receipt(record, request_value)
    if receipt is not None:
        return grant_outcome(receipt, False, record)
    # Match the experiment owner protocol without importing uninstalled modules.
    # Fixed lock order: experiment, maintenance, then the short journal I/O lock.
    with (
        ops.private_directory(journal.root / "runs", create=True) as runs,
        private_lock(runs, "experiment.lock", blocking=False),
        ops.private_directory(journal.root) as parent,
        private_lock(parent, "maintenance.lock", blocking=False),
    ):
        record = journal.read()
        receipt = find_grant_receipt(record, request_value)
        if receipt is not None:
            return grant_outcome(receipt, False, record)
        try:
            with ops.open_private_at(runs, "active-experiment.json") as fd:
                owner = ops.fields(ops.parse_json(ops.read_bounded_file(fd)), ("run_id",))
            if owner["run_id"] is not None:
                raise failure("active_experiment_owner")
        except FileNotFoundError:
            pass
        require_grant_expectation(record, request_value)
        require_grant_idle(record)
        verify_grant_controller(journal, record, clock=clock)
        start = clock()
        Provider(ops.load_profile(record["profile_path"])).verify_idle(record)
        # Manager/session probes may wait for D-Bus. Keep them out of the I/O
        # lock so idle supervision can publish heartbeats while they run. Read
        # the latest heartbeat here; commit still compares to the original proof
        # snapshot and refuses every other intervening state change.
        verify_grant_controller(journal, journal.read(), clock=clock)
        receipt, applied, current = journal.extend_allowance(
            request_value,
            record,
            start,
            clock=clock,
            recheck=lambda current: verify_grant_local(journal, current, clock=clock),
        )
        return grant_outcome(receipt, applied, current)


def prepare(args):
    # Offline validated facts gate registration; supervisor performs fresh cloud
    # identity/state validation before any activation or network mutation.
    result, code = ops.plan(args)
    if code:
        result["operation"] = "pilot prepare"
        return result, code
    spec = ops.validate_spec(args.spec)
    saved = ops.validate_snapshot(args.inventory)
    age = (datetime.now(UTC) - ops.timestamp(saved["observed_at"])).total_seconds()
    if not 0 <= age <= 300:
        raise failure("fresh_inventory_required")
    servers = {server["id"]: server for server in saved["servers"]}
    if any(servers[vm["id"]]["status"] != "SHELVED_OFFLOADED" for vm in spec["vms"]):
        raise failure("initial_offload_required")
    profile = ops.load_profile(args.profile)
    context = {
        "project_id": profile["expected_project_id"],
        "region": profile["region"],
        "auth_url": profile["auth_url"],
    }
    if context != spec["context"] or not {vm["id"] for vm in spec["vms"]}.issubset(
        profile["intended_server_ids"]
    ):
        raise failure("context_binding_mismatch")
    if any(vm["active_seconds"] <= 600 for vm in spec["vms"]):
        raise failure("insufficient_allowance")
    access = validate_access(args.access)
    journal = register(args.profile, args.state_root, spec, access)
    if args.install_supervisor:
        install(journal)
    return status(journal, "pilot prepare")


def status(journal, operation="pilot status"):
    record = journal.read()
    now = sample_clock()
    ready = heartbeat_fresh(record, now) and journal.supervisor_locked()
    vms = []
    for vm_id, vm in record["vms"].items():
        account = allowance(vm["account"]).account(now)
        observed = vm["observed"]
        observation_fresh = observed is not None and heartbeat_fresh(
            {"heartbeat": observed["clock"]}, now, maximum_age=OBSERVATION_SECONDS
        )
        vms.append(
            {
                "id": vm_id,
                "role": vm["role"],
                "account": asdict(account),
                "phase": vm["phase"],
                "provider_state": observed["state"] if observation_fresh else "UNKNOWN",
                "observation_fresh": observation_fresh,
                "last_cleanup_request": vm.get("cleanup_intent"),
            }
        )
    pending = (
        bool(record["checkpoint"])
        or not ready
        or record["desired"] == "stop"
        or any(not vm["observation_fresh"] for vm in vms)
        or (record["desired"] == "run" and any(vm["provider_state"] != "ACTIVE" for vm in vms))
    )
    return ops.outcome(
        operation,
        "pending" if pending else "ok",
        data={
            "registration_id": record["registration_id"],
            "allowance_binding_sha256": allowance_binding_sha256(record),
            "allowance_grants": [event["data"] for event in grant_receipts(record)],
            "context": record["spec"]["context"],
            "desired": record["desired"],
            "supervisor_ready": ready,
            "supervisor_release_digest": record["service"]["digest"] if record["service"] else None,
            "checkpoint": record["checkpoint"],
            "cleanup_diagnostics": cleanup_diagnostics(record),
            "vms": vms,
            "network_ready": record["network"]["ready"],
            "network_rolled_back": record["network"]["rolled_back"],
            "request_acceptance_is_completion": False,
        },
        next_actions=[EMERGENCY]
        if pending
        else ["Keep workstation powered, awake and online. Verify provider observations with pilot status."],
    ), 3 if pending else 0


def verify_service(record):
    service = record["service"]
    if service is None or service["unit"] != UNIT:
        raise failure("service_not_installed")
    if str(Path(__file__).resolve().parent) != service["release"]:
        raise failure("immutable_release_required")
    with ops.private_directory(ops.absolute_path(service["release"])) as parent:
        content = []
        for name in MODULES:
            with ops.open_private_at(parent, name) as fd:
                content.append(ops.read_bounded_file(fd))
    if hashlib.sha256(b"".join(content)).hexdigest() != service["digest"]:
        raise failure("installed_release_changed")
    if (
        str(Path(sys.executable).resolve()) != service["interpreter"]
        or hashlib.sha256(Path(service["interpreter"]).read_bytes()).hexdigest()
        != service["interpreter_digest"]
    ):
        raise failure("installed_interpreter_changed")
    invocation = os.environ.get("INVOCATION_ID", "")
    if not re.fullmatch(r"[0-9a-f]{32}", invocation):
        raise failure("user_systemd_invocation_required")
    raw = systemctl("show", UNIT, "--property=MainPID", "--property=InvocationID")
    lines = raw.decode("ascii", errors="replace").splitlines()
    if f"MainPID={os.getpid()}" not in lines or f"InvocationID={invocation}" not in lines:
        raise failure("user_systemd_identity_mismatch")


def supervise(journal):
    record = journal.read()  # A busy journal is transient, not an integrity verdict.
    try:
        verify_service(record)
    except (ops.OpsError, OSError) as original:
        if isinstance(original, OSError):
            if original.errno not in (errno.ENOENT, errno.EACCES, errno.ENOTDIR, errno.ELOOP):
                raise  # I/O outages remain retryable, without using an unverified service.
            exc = failure("installed_service_unreadable")
        else:
            exc = original
        try:
            journal.change(lambda current, code=exc.code: current.update(desired="stop", checkpoint=code))
        except (ops.OpsError, OSError):
            # Preserve the original verification cause even if evidence storage is busy.
            pass
        if exc.code in PERMANENT_VERIFICATION_FAILURES:
            # Only a fixed code is logged. Never run provider code after failed integrity checks.
            print("FLOW-DC supervisor verification failed: " + exc.code, file=sys.stderr)
            raise ops.OpsError(exc.code, exc.message, EMERGENCY, FATAL_SERVICE_EXIT) from None
        raise exc from None
    with journal.supervisor_lock():
        record = journal.read()
        provider = Provider(ops.load_profile(record["profile_path"]))
        supervisor = Supervisor(journal, provider)
        provider.before_activation = supervisor.before_activation
        supervisor.recover()
        stopping = False

        def stop(signum, frame):
            nonlocal stopping
            stopping = True

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        while True:
            try:
                if stopping:
                    request(journal, "stop")
                supervisor.tick()
                # A stopping systemd unit will not restart an exited process.
                if stopping and journal.read()["desired"] == "idle":
                    return ops.outcome("pilot supervise", "ok"), 0
            except ops.OpsError as exc:
                if exc.code != "pilot_state_busy":
                    raise
                # Retain the verified actor lock and the in-memory stop latch.
                # Do not bypass journal serialization or retry corruption errors.
            time.sleep(2)


def run(args):
    mask = os.umask(0o077)
    try:
        if args.pilot_command == "prepare":
            return prepare(args)
        if args.pilot_command == "runtime-check":
            try:
                Provider(ops.load_profile(args.profile)).runtime_check()
            except ops.OpsError as exc:
                if exc.code != "offload_runtime_unsupported":
                    raise
                return ops.outcome(
                    "pilot runtime-check",
                    "pending",
                    errors=[{"code": exc.code, "message": exc.message}],
                    next_actions=[
                        "Have the administrator provide a supported, trusted administration runtime "
                        "as described in docs/jetstream2/README.md#explicit-offload-runtime-prerequisite, "
                        "then rerun pilot runtime-check. Cloud readiness has not been assessed."
                    ],
                    data={
                        "offload_runtime": "unsupported",
                        "cloud_readiness": "not_assessed",
                        "request_acceptance_is_completion": False,
                    },
                ), exc.exit_code
            return ops.outcome(
                "pilot runtime-check",
                "ok",
                data={
                    "offload_runtime": "supported",
                    "cloud_readiness": "not_assessed",
                    "request_acceptance_is_completion": False,
                },
            ), 0
        journal = Journal(args.state_root)
        if args.pilot_command == "extend-allowance":
            return extend_allowance(journal, args.grant)
        if args.pilot_command == "upgrade-supervisor":
            return upgrade_supervisor(journal, args)
        if args.pilot_command == "supervise":
            return supervise(journal)
        if args.pilot_command in ("start", "stop", "reconcile"):
            if args.pilot_command == "start":
                require_persistent_session()
            request(
                journal,
                args.pilot_command,
                window=getattr(args, "window_seconds", 1800),
                inspection=not getattr(args, "full_window", False),
            )
            value, code = status(journal, "pilot " + args.pilot_command)
            value["data"]["request_accepted"] = True
            # Pending reflects actual readiness/cleanup, not request failure.
            return value, code
        return status(journal)
    finally:
        os.umask(mask)
