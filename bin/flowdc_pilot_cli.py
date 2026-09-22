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
from flowdc_pilot_journal import EMERGENCY, Journal, allowance, failure, register
from flowdc_pilot_provider import Provider, validate_access
from flowdc_pilot_supervisor import OBSERVATION_SECONDS, Supervisor, heartbeat_fresh, request, sample_clock

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
    for action in ("prepare", "start", "status", "stop", "reconcile", "supervise"):
        parser = actions.add_parser(action, allow_abbrev=False)
        parser.add_argument("--state-root", default=str(Path.home() / ".local/share/flowdc-ops"))
        if action == "prepare":
            parser.add_argument("--profile", required=True)
            parser.add_argument("--spec", required=True)
            parser.add_argument("--inventory", required=True)
            parser.add_argument("--access", required=True)
            parser.add_argument("--install-supervisor", action="store_true")
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
    unit = (
        "[Unit]\nDescription=FLOW-DC bounded pilot supervisor\nStartLimitIntervalSec=0\n"
        "[Service]\nType=simple\nRestart=always\nRestartSec=2\nRestartPreventExitStatus=78\nTimeoutStopSec=infinity\nUMask=0077\n"
        "NoNewPrivileges=yes\nStandardOutput=null\nStandardError=journal\nLogRateLimitIntervalSec=30s\nLogRateLimitBurst=3\n"
        "Environment=PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1\n"
        f"ExecStart={quote_unit(interpreter)} -E -s {quote_unit(str(release / 'flowdc_ops.py'))} "
        f"pilot supervise --state-root {quote_unit(str(journal.root))}\n"
        "[Install]\nWantedBy=default.target\n"
    ).encode()
    unit_root = Path.home() / ".config/systemd/user"
    with ops.private_directory(unit_root, create=True, private=False) as parent:
        write_new(parent, UNIT, unit)
    service = {
        "unit": UNIT,
        "release": str(release),
        "digest": digest,
        "interpreter": interpreter,
        "interpreter_digest": interpreter_digest,
    }

    def save(current):
        if current["service"] not in (None, service):
            raise failure("service_binding_conflict")
        current["service"] = service

    journal.change(save)
    systemctl("daemon-reload")
    systemctl("enable", "--now", UNIT)


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
            "context": record["spec"]["context"],
            "desired": record["desired"],
            "supervisor_ready": ready,
            "checkpoint": record["checkpoint"],
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
        journal = Journal(args.state_root)
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
