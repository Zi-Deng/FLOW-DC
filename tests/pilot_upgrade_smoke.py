#!/usr/bin/env python3
"""Explicit user-systemd upgrade/recovery test with only temporary fake resources.

Creates a uniquely named regular user unit, never the production service. Source
injection is confined to temporary test releases; no production test mode exists.
"""

import hashlib
import json
import os
import re
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


def cleanup_unit(unit, path, expected):
    """Remove only our regular fake unit, after a verified stop; preserve drift."""
    if (
        not re.fullmatch(r"flowdc-upgrade-smoke-[0-9a-f]{32}\.service", unit)
        or path != Path.home() / ".config/systemd/user" / unit
    ):
        raise RuntimeError("fake_unit_identity_changed")
    with ops.private_directory(path.parent, private=False) as parent:
        with ops.open_private_at(parent, unit) as fd:
            if ops.read_bounded_file(fd) not in expected:
                raise RuntimeError("fake_unit_changed")
        cli.systemctl("stop", unit)
        lines = cli.systemctl("show", unit, "--property=MainPID", "--property=ActiveState").splitlines()
        if b"MainPID=0" not in lines or not ({b"ActiveState=inactive", b"ActiveState=failed"} & set(lines)):
            raise RuntimeError("fake_unit_not_stopped")
        # A changed/replaced file during stop is not ours to unlink.
        with ops.open_private_at(parent, unit) as fd:
            if ops.read_bounded_file(fd) not in expected:
                raise RuntimeError("fake_unit_changed")
        os.unlink(unit, dir_fd=parent)
        os.fsync(parent)
    cli.systemctl("daemon-reload")


def main(*, grant_check=None):
    os.umask(0o077)
    if sys.argv[1:] not in ([], ["--rollback"]):
        return 2
    root = Path(tempfile.mkdtemp(prefix="flowdc-upgrade-systemd-fake-"))
    unit = "flowdc-upgrade-smoke-" + uuid4().hex + ".service"
    result = {"status": "blocked", "fake_only": True, "evidence_root": str(root), "unit": unit}
    created = False
    unit_path = None
    expected_units = []
    exit_code = 1
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
        selected = spec()
        if grant_check is not None:
            for vm in selected["vms"]:
                vm["active_seconds"] = 3600
        journal = register(profile, root / "state", selected, access())
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
            patch.object(sys, "executable", str(interpreter)),
            patch.object(cli, "UNIT", unit),
            patch.object(cli, "__file__", str(fake_cli)),
        ):
            # Reserve a regular file in the real managed-unit directory. A
            # systemctl link reports the user's link path, not its temp target.
            # O_EXCL refuses preexisting units, even if their bytes happen to match.
            original_digest = hashlib.sha256(
                b"".join((fake_source / name).read_bytes() for name in cli.MODULES)
            ).hexdigest()
            original_service = cli.stage_candidate(journal, original_digest)
            expected_units.append(cli.service_unit(original_service, journal.root))
            unit_path = Path.home() / ".config/systemd/user" / unit
            with ops.private_directory(unit_path.parent, create=True, private=False) as parent:
                fd = os.open(unit, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
                created = True
                with os.fdopen(fd, "wb") as stream:
                    stream.write(expected_units[0])
                    stream.flush()
                    os.fsync(stream.fileno())
                os.fsync(parent)
            # The normal installer verifies the reserved bytes and binds the
            # journal. Never enable this disposable unit for subsequent logins.
            with patch.object(cli, "systemctl", return_value=b""):
                cli.install(journal)
            real_systemctl("daemon-reload")
            real_systemctl("start", unit)
            wait_ready(journal)

            def settled(record):
                for index, vm in enumerate(record["vms"].values()):
                    vm["account"]["consumed"] = 234.5 + index
                    vm["observed"] = {"state": "SHELVED_OFFLOADED", "clock": asdict(sample_clock())}

            journal.change(settled)
            if grant_check is not None:
                grant_check(journal, root, original_service)
                result["grant_application_replay_refusal_verified"] = True
            before = journal.read()
            fake_cli.write_text(fake_cli.read_text() + "\n# synthetic next release\n")
            digest = hashlib.sha256(
                b"".join((fake_source / name).read_bytes() for name in cli.MODULES)
            ).hexdigest()
            expected_units.append(
                cli.service_unit(
                    dict(
                        original_service,
                        digest=digest,
                        release=str(journal.root / "releases" / ("pilot-" + digest)),
                    ),
                    journal.root,
                )
            )
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
            exit_code = 0
    except (ops.OpsError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
        result["checkpoint"] = "fake_upgrade_smoke_failed"
        result["error_code"] = exc.code if isinstance(exc, ops.OpsError) else "fake_fixture_error"
        exit_code = 1
    finally:
        cleanup_failed = False
        if created:
            try:
                cleanup_unit(unit, unit_path, expected_units)
                result["unit_removed"] = True
            except (ops.OpsError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
                cleanup_failed = True
                result.update(status="blocked", cleanup_checkpoint="fake_unit_cleanup_failed")
                result["cleanup_error_code"] = (
                    exc.code if isinstance(exc, ops.OpsError) else "fake_fixture_error"
                )
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result))
        if cleanup_failed:
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
