#!/usr/bin/env python3
"""Explicit user-systemd upgrade/recovery test with only temporary fake resources.

Creates a uniquely named linked user unit, never the production service. Source
injection is confined to temporary test releases; no production test mode exists.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))

import flowdc_ops as ops
import flowdc_pilot_cli as cli
from flowdc_pilot_journal import register
from flowdc_pilot_supervisor import heartbeat_fresh, sample_clock
from test_flowdc_pilot_lifecycle import access, spec


def wait_ready(journal):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if heartbeat_fresh(journal.read(), sample_clock()) and journal.supervisor_locked():
            return
        time.sleep(0.1)
    raise RuntimeError("fake_service_not_ready")


def main():
    os.umask(0o077)
    if sys.argv[1:] not in ([], ["--rollback"]):
        return 2
    root = Path(tempfile.mkdtemp(prefix="flowdc-upgrade-systemd-fake-"))
    unit = "flowdc-upgrade-smoke-" + uuid4().hex + ".service"
    result = {"status": "blocked", "fake_only": True, "evidence_root": str(root), "unit": unit}
    linked = False
    try:
        # No filesystem or service-manager mutation outside the temporary tree
        # until the existing user manager is confirmed reachable.
        bus = subprocess.run(
            ["systemctl", "--user", "show", "--property=Version"], capture_output=True, timeout=5
        )
        if bus.returncode:
            result["checkpoint"] = "user_systemd_unavailable"
            return 3
        config = root / "config"
        config.mkdir()
        profile = config / "profile.json"
        profile.write_text('{"fake_only":true}')
        journal = register(profile, root / "state", spec(), access())
        source = Path(cli.__file__).resolve().parent
        fake_source = root / "source"
        fake_source.mkdir()
        for name in cli.MODULES:
            (fake_source / name).write_bytes((source / name).read_bytes())
        # Loaded by the copied ops entrypoint, before the actual supervise call.
        # Provider construction and profile loading are permanently fake in these
        # hashed test releases. Every unexpected operation fails without cloud I/O.
        injected = """

def _fake_profile(path):
    if ops.read_document(path) != {"fake_only": True}:
        raise RuntimeError("fake_fixture_required")
    return {"fake_only": True}

class _FakeIdleProvider:
    def __init__(self, profile):
        if profile != {"fake_only": True}:
            raise RuntimeError("fake_fixture_required")
    def verify_idle(self, record):
        require_idle(record)

ops.load_profile = _fake_profile
Provider = _FakeIdleProvider
"""
        fake_cli = fake_source / "flowdc_pilot_cli.py"
        fake_cli.write_text(fake_cli.read_text() + injected + f"\nUNIT = {unit!r}\n")
        real_systemctl = cli.systemctl
        interpreter = root / "pinned-python"
        shutil.copyfile(Path(sys.executable).resolve(), interpreter)
        interpreter.chmod(0o700)
        with (
            patch.object(Path, "home", return_value=root),
            patch.object(sys, "executable", str(interpreter)),
            patch.object(cli, "UNIT", unit),
            patch.object(cli, "__file__", str(fake_cli)),
        ):
            # Offline installation only; explicitly link this unique temporary
            # unit into the real manager, without enabling it for later logins.
            with patch.object(cli, "systemctl", return_value=b""):
                cli.install(journal)
            unit_path = root / ".config/systemd/user" / unit
            linked = True  # Clean up our unique link even if its response is lost.
            real_systemctl("link", str(unit_path))
            real_systemctl("daemon-reload")
            real_systemctl("start", unit)
            wait_ready(journal)

            def settled(record):
                for index, vm in enumerate(record["vms"].values()):
                    vm["account"]["consumed"] = 234.5 + index
                    vm["observed"] = {"state": "SHELVED_OFFLOADED", "clock": asdict(sample_clock())}

            journal.change(settled)
            before = journal.read()
            fake_cli.write_text(fake_cli.read_text() + "\n# synthetic next release\n")
            digest = hashlib.sha256(
                b"".join((fake_source / name).read_bytes() for name in cli.MODULES)
            ).hexdigest()
            args = SimpleNamespace(
                expected_current_digest=before["service"]["digest"],
                expected_candidate_digest=digest,
                recover=None,
            )
            real_update = cli.maintenance_update

            def interrupt(journal, phase, **kwargs):
                real_update(journal, phase, **kwargs)
                if phase == "binding":
                    raise RuntimeError("synthetic interruption")

            with (
                patch.object(cli, "Provider") as fake,
                patch.object(ops, "load_profile", return_value={"fake_only": True}),
            ):
                fake.return_value.verify_idle.side_effect = cli.require_idle
                with patch.object(cli, "maintenance_update", side_effect=interrupt):
                    try:
                        cli.upgrade_supervisor(journal, args)
                    except RuntimeError as exc:
                        if str(exc) != "synthetic interruption":
                            raise
                    else:
                        raise RuntimeError("interruption_not_reached")
                try:
                    journal.read()
                except ops.OpsError as exc:
                    if exc.code != "unsupported_journal":
                        raise
                else:
                    raise RuntimeError("maintenance_not_exclusive")
                args.recover = "rollback" if sys.argv[1:] else "complete"
                cli.upgrade_supervisor(journal, args)
            wait_ready(journal)
            after = journal.read()
            expected = before["service"]["digest"] if args.recover == "rollback" else digest
            if after["service"]["digest"] != expected:
                raise RuntimeError("wrong_release")
            if {k: v for k, v in after.items() if k not in ("service", "heartbeat")} != {
                k: v for k, v in before.items() if k not in ("service", "heartbeat")
            }:
                raise RuntimeError("history_changed")
            result.update(
                status="completed", recovered=args.recover, accounts_preserved=True, heartbeat_verified=True
            )
            return 0
    except (ops.OpsError, OSError, RuntimeError, subprocess.SubprocessError):
        result["checkpoint"] = "fake_upgrade_smoke_failed"
        return 1
    finally:
        if linked:
            for action in ("stop", "disable"):
                subprocess.run(["systemctl", "--user", action, unit], capture_output=True, timeout=10)
            subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True, timeout=5)
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result))


if __name__ == "__main__":
    raise SystemExit(main())
