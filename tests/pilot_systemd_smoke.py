#!/usr/bin/env python3
"""Real user-systemd supervision with only fake resources; never loads a live profile.

Run explicitly, outside unittest discovery: python3 tests/pilot_systemd_smoke.py.
The disposable unit is always named flowdc-pilot-smoke-<random>. Evidence remains
in the printed temporary root. Production flowdc-pilot.service is never touched.
"""

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))

from flowdc_ops import read_document
from flowdc_pilot import ClockSample
from flowdc_pilot_journal import Journal, register
from flowdc_pilot_supervisor import Supervisor, heartbeat_fresh, request
from test_flowdc_pilot_lifecycle import VM_IDS, FakeProvider, access, spec


def accelerated_clock():
    return ClockSample(
        Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        time.clock_gettime(time.CLOCK_BOOTTIME) * 20,
        time.time() * 20,
    )


def validate_fake(journal):
    record = journal.read()
    if (
        record["service"] != {"unit": "fake-only"}
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
    if len(sys.argv) == 3 and sys.argv[1] == "--serve":
        serve(sys.argv[2])
        return 0
    if len(sys.argv) == 3 and sys.argv[1] == "--request":
        journal = Journal(sys.argv[2])
        validate_fake(journal)
        request(journal, "start", window=1000, clock=accelerated_clock)
        return 0
    if len(sys.argv) != 1:
        return 2
    root = Path(tempfile.mkdtemp(prefix="flowdc-pilot-systemd-fake-"))
    config = root / "config"
    config.mkdir(mode=0o700)
    profile = config / "profile.json"
    profile.write_text('{"fake_only":true}')
    journal = register(profile, root / "state", spec(), access())
    journal.change(lambda record: record.update(service={"unit": "fake-only"}))
    unit = "flowdc-pilot-smoke-" + uuid4().hex
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
            "--property=UMask=0077",
            "--property=RuntimeMaxSec=45",
            "--property=StandardOutput=null",
            "--property=StandardError=null",
            str(Path(sys.executable).resolve()),
            str(Path(__file__).resolve()),
            "--serve",
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
        while time.monotonic() < deadline:
            record = journal.read()
            actions = record.get("fake_actions", [])
            if record["desired"] == "idle" and any(action == "unshelve" for action, _ in actions):
                if sum(action == "unshelve" for action, _ in actions) != 3:
                    raise RuntimeError("activation_incomplete")
                if not all(
                    not vm["account"]["obligation"] and vm["observed"]["state"] == "SHELVED_OFFLOADED"
                    for vm in record["vms"].values()
                ):
                    raise RuntimeError("offload_incomplete")
                result.update(
                    status="completed", confirmed_offloaded=3, deadline_cleanup_after_foreground_exit=True
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
