#!/usr/bin/env python3
"""Real user-systemd supervision with only fake resources; never loads a live profile.

Run explicitly, outside unittest discovery: python3 tests/pilot_systemd_smoke.py.
The disposable unit is always named flowdc-pilot-smoke-<random>. Evidence remains
in the printed temporary root. Production flowdc-pilot.service is never touched.
"""

import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))

from flowdc_ops import read_document
from flowdc_pilot import ClockSample
from flowdc_pilot_journal import Journal, register
from flowdc_pilot_supervisor import Supervisor, heartbeat_fresh, request
from test_flowdc_pilot_lifecycle import VM_IDS, FakeProvider, access, spec, synthetic_service


def accelerated_clock():
    return ClockSample(
        Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        time.clock_gettime(time.CLOCK_BOOTTIME) * 20,
        time.time() * 20,
    )


def service_fixture(journal, unit):
    from flowdc_pilot_cli import MODULES

    source = Path(__file__).resolve().parents[1] / "bin"
    return {
        "unit": unit,
        "release": str(journal.root.parent / "release"),
        "digest": hashlib.sha256(b"".join((source / name).read_bytes() for name in MODULES)).hexdigest(),
        "interpreter": str(Path(sys.executable).resolve()),
        "interpreter_digest": hashlib.sha256(Path(sys.executable).resolve().read_bytes()).hexdigest(),
    }


def valid_service_fixture(journal, service):
    if service == synthetic_service():
        return True
    unit = service.get("unit", "") if isinstance(service, dict) else ""
    return bool(
        re.fullmatch(r"flowdc-pilot-smoke-[0-9a-f]{32}\.service", unit)
    ) and service == service_fixture(journal, unit)


def serve_service(root):
    journal = Journal(root)
    validate_fake(journal)  # Before any patched adapter or journal write.
    service = journal.read()["service"]
    if service == synthetic_service():
        raise RuntimeError("installed_fake_fixture_required")
    # Execute the copied, hashed CLI module. Only this test harness substitutes
    # its unit name and provider; production has no test-mode bypass.
    module_spec = importlib.util.spec_from_file_location(
        "flowdc_pilot_cli", Path(service["release"]) / "flowdc_pilot_cli.py"
    )
    cli = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(cli)

    class ObservedSupervisor(Supervisor):
        def __init__(self, journal, provider):
            super().__init__(journal, provider, clock=accelerated_clock)

        def tick(self):
            super().tick()
            journal.change(lambda record: record.update(fake_actions=list(self.provider.actions)))

    real_sleep = time.sleep
    with (
        patch.object(cli, "UNIT", service["unit"]),
        patch.object(cli, "Provider", side_effect=lambda profile: FakeProvider()),
        patch.object(cli.ops, "load_profile", return_value={"fake_only": True}),
        patch.object(cli, "Supervisor", ObservedSupervisor),
        patch.object(
            cli, "request", side_effect=lambda j, command: request(j, command, clock=accelerated_clock)
        ),
        patch.object(cli.time, "sleep", side_effect=lambda seconds: real_sleep(min(seconds, 0.1))),
    ):
        # verify_service is real: release/interpreter hashes and systemd MainPID /
        # InvocationID must match. The provider cannot reach any real cloud.
        return cli.supervise(journal)[1]


def validate_fake(journal):
    record = journal.read()
    if (
        not valid_service_fixture(journal, record["service"])
        or set(record["vms"]) != set(VM_IDS)
        or record["spec"]["context"] != spec()["context"]
        or read_document(record["profile_path"]) != {"fake_only": True}
    ):
        raise RuntimeError("fake_fixture_required")


def serve(root):
    journal = Journal(root)
    validate_fake(journal)
    with journal.supervisor_lock():
        # Only FakeProvider is instantiated; no live-profile/provider selection.
        provider = FakeProvider()
        supervisor = Supervisor(journal, provider, clock=accelerated_clock)
        supervisor.recover()
        while True:
            supervisor.tick()
            journal.change(lambda record: record.update(fake_actions=list(provider.actions)))
            time.sleep(0.1)


def main():
    os.umask(0o077)
    if len(sys.argv) == 3 and sys.argv[1] == "--serve-service":
        return serve_service(sys.argv[2])
    if len(sys.argv) == 3 and sys.argv[1] == "--serve":
        serve(sys.argv[2])
        return 0
    if len(sys.argv) == 3 and sys.argv[1] == "--request":
        journal = Journal(sys.argv[2])
        validate_fake(journal)
        request(journal, "start", window=1000, clock=accelerated_clock)
        return 0
    terminate_active = sys.argv[1:] == ["--sigterm"]
    if len(sys.argv) != 1 and not terminate_active:
        return 2
    root = Path(tempfile.mkdtemp(prefix="flowdc-pilot-systemd-fake-"))
    config = root / "config"
    config.mkdir(mode=0o700)
    profile = config / "profile.json"
    profile.write_text('{"fake_only":true}')
    journal = register(profile, root / "state", spec(), access())
    unit = "flowdc-pilot-smoke-" + uuid4().hex + ".service"
    service = service_fixture(journal, unit)
    from flowdc_pilot_cli import MODULES

    release = Path(service["release"])
    release.mkdir(mode=0o700)
    for name in MODULES:
        (release / name).write_bytes((Path(__file__).resolve().parents[1] / "bin" / name).read_bytes())
    journal.change(lambda record: record.update(service=service))
    launched = False
    result = {
        "schema_version": 1,
        "status": "blocked",
        "fake_only": True,
        "evidence_root": str(root),
        "unit": unit,
    }
    try:
        command = [
            "systemd-run",
            "--user",
            "--unit=" + unit,
            "--property=Type=exec",
            "--property=Restart=on-failure",
            "--property=RemainAfterExit=yes",
            "--property=UMask=0077",
            "--property=TimeoutStopSec=infinity",
            "--property=RestartPreventExitStatus=78",
            "--property=RuntimeMaxSec=45",
            "--property=StandardOutput=null",
            "--property=StandardError=null",
            str(Path(sys.executable).resolve()),
            str(Path(__file__).resolve()),
            "--serve-service",
            str(journal.root),
        ]
        child = subprocess.run(command, capture_output=True, timeout=10)
        if child.returncode:
            result["checkpoint"] = "user_systemd_unavailable"
            return 3
        launched = True
        deadline = time.monotonic() + 25
        while not heartbeat_fresh(journal.read(), accelerated_clock()):
            if time.monotonic() >= deadline:
                raise RuntimeError("supervisor_not_ready")
            time.sleep(0.1)
        # A separate foreground requester exits before cleanup occurs.
        requester = subprocess.run(
            [
                str(Path(sys.executable).resolve()),
                str(Path(__file__).resolve()),
                "--request",
                str(journal.root),
            ],
            capture_output=True,
            timeout=5,
        )
        if requester.returncode:
            raise RuntimeError("requester_failed")
        result["foreground_requester_exit"] = requester.returncode
        signal_sent = False
        while time.monotonic() < deadline:
            record = journal.read()
            actions = record.get("fake_actions", [])
            if (
                terminate_active
                and not signal_sent
                and sum(action == "unshelve" for action, _ in actions) == 3
            ):
                if (
                    record["desired"] != "run"
                    or not all(vm["account"]["obligation"] for vm in record["vms"].values())
                    or any(action in ("shelve", "offload") for action, _ in actions)
                ):
                    raise RuntimeError("signal_missed_active_obligations")
                killed = subprocess.run(
                    ["systemctl", "--user", "kill", "--kill-whom=main", "--signal=SIGTERM", unit],
                    capture_output=True,
                    timeout=5,
                )
                if killed.returncode:
                    raise RuntimeError("signal_failed")
                signal_sent = True
            if record["desired"] == "idle" and any(action == "unshelve" for action, _ in actions):
                if sum(action == "unshelve" for action, _ in actions) != 3:
                    raise RuntimeError("activation_incomplete")
                if not all(
                    not vm["account"]["obligation"] and vm["observed"]["state"] == "SHELVED_OFFLOADED"
                    for vm in record["vms"].values()
                ):
                    raise RuntimeError("offload_incomplete")
                if terminate_active:
                    exited = subprocess.run(
                        [
                            "systemctl",
                            "--user",
                            "show",
                            unit,
                            "--property=SubState",
                            "--property=ExecMainStatus",
                        ],
                        capture_output=True,
                        timeout=5,
                    )
                    if (
                        exited.returncode
                        or b"SubState=exited" not in exited.stdout
                        or b"ExecMainStatus=0" not in exited.stdout
                    ):
                        time.sleep(0.1)
                        continue
                result.update(
                    status="completed",
                    confirmed_offloaded=3,
                    deadline_cleanup_after_foreground_exit=not terminate_active,
                    sigterm_cleanup_with_obligations=signal_sent,
                    production_supervise_and_verify_service=True,
                )
                return 0
            time.sleep(0.1)
        raise RuntimeError("deadline_cleanup_timeout")
    except (OSError, subprocess.SubprocessError, RuntimeError):
        result["checkpoint"] = "fake_systemd_smoke_failed"
        return 1
    finally:
        if launched:
            subprocess.run(["systemctl", "--user", "stop", unit], capture_output=True, timeout=10)
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result))


if __name__ == "__main__":
    raise SystemExit(main())
