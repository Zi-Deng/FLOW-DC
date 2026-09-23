"""Exercise the smoke fixture against systemd's canonical user-unit layout."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import asdict
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pilot_upgrade_smoke as smoke
from flowdc_pilot_supervisor import sample_clock


class UpgradeSmokeTests(unittest.TestCase):
    def test_recovery_uses_canonical_regular_unit_and_cleans_only_its_fixture(self):
        for direction, bad_origin, cleanup_stuck in (
            ([], False, False),
            (["--rollback"], False, False),
            ([], True, False),
            ([], False, True),
        ):
            with (
                self.subTest(direction=direction, bad_origin=bad_origin, cleanup_stuck=cleanup_stuck),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                home = root / "home"
                units = home / ".config/systemd/user"
                units.mkdir(parents=True, mode=0o700)
                production = units / "flowdc-pilot.service"
                production.write_bytes(b"unrelated production unit")
                production.chmod(0o600)
                evidence = root / "evidence"
                evidence.mkdir(mode=0o700)
                calls = []

                def manager(
                    *args, calls=calls, units=units, bad_origin=bad_origin, cleanup_stuck=cleanup_stuck
                ):
                    calls.append(args)
                    if args[0] == "link":
                        target = Path(args[1])
                        (units / target.name).symlink_to(target)
                    if args[0] == "show" and cleanup_stuck and len(args) == 4:
                        return b"MainPID=123\nActiveState=active\n"
                    if args[0] == "show":
                        # systemd reports the user-unit name, even for link targets.
                        unit = units / ("unexpected.service" if bad_origin else args[1])
                        return (
                            f"FragmentPath={unit}\nDropInPaths=\nNeedDaemonReload=no\n"
                            "MainPID=0\nActiveState=inactive\n"
                        ).encode()
                    if args[0] == "disable":
                        unit = units / args[1]
                        if unit.is_symlink():
                            unit.unlink()
                    return b""

                def run(argv, manager=manager, **kwargs):
                    self.assertEqual(argv[:2], ["systemctl", "--user"])
                    if argv[2:] == ["show", "--property=Version"]:
                        return subprocess.CompletedProcess(argv, 0, b"Version=synthetic\n", b"")
                    return subprocess.CompletedProcess(argv, 0, manager(*argv[2:]), b"")

                def ready(journal):
                    journal.change(lambda r: r.update(heartbeat=asdict(sample_clock())))

                mask = os.umask(0o077)
                try:
                    with (
                        patch.object(Path, "home", return_value=home),
                        patch.object(smoke.tempfile, "mkdtemp", return_value=str(evidence)),
                        patch.object(smoke.cli, "systemctl", side_effect=manager),
                        patch.object(smoke.subprocess, "run", side_effect=run),
                        patch.object(smoke, "wait_ready", side_effect=ready),
                        patch.object(sys, "argv", ["pilot_upgrade_smoke.py", *direction]),
                        redirect_stdout(StringIO()),
                    ):
                        code = smoke.main()
                finally:
                    os.umask(mask)
                result = json.loads((evidence / "result.json").read_text())
                self.assertEqual(code, 1 if bad_origin or cleanup_stuck else 0, result)
                if cleanup_stuck:
                    self.assertEqual(result["status"], "blocked")
                    self.assertEqual(result["cleanup_checkpoint"], "fake_unit_cleanup_failed")
                elif bad_origin:
                    self.assertEqual(result["error_code"], "unexpected_unit_provenance")
                else:
                    self.assertEqual(result["status"], "completed")
                    self.assertEqual(result["recovered"], "rollback" if direction else "complete")
                    self.assertTrue(result["accounts_preserved"])
                self.assertEqual(result.get("unit_removed", False), not cleanup_stuck)
                self.assertNotIn("link", [call[0] for call in calls])
                self.assertEqual(
                    set(units.iterdir()),
                    {production, units / result["unit"]} if cleanup_stuck else {production},
                )
                self.assertEqual(production.read_bytes(), b"unrelated production unit")

    def test_cleanup_preserves_changed_linked_or_still_running_unit(self):
        for mode in ("changed", "symlink", "running"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                home = Path(directory)
                units = home / ".config/systemd/user"
                units.mkdir(parents=True, mode=0o700)
                unit = "flowdc-upgrade-smoke-" + "a" * 32 + ".service"
                path = units / unit
                target = home / "target"
                target.write_bytes(b"expected")
                target.chmod(0o600)
                if mode == "symlink":
                    path.symlink_to(target)
                else:
                    path.write_bytes(b"changed" if mode == "changed" else b"expected")
                    path.chmod(0o600)
                with (
                    patch.object(Path, "home", return_value=home),
                    patch.object(
                        smoke.cli, "systemctl", return_value=b"MainPID=123\nActiveState=active\n"
                    ) as manager,
                    self.assertRaises((smoke.ops.OpsError, OSError, RuntimeError)),
                ):
                    smoke.cleanup_unit(unit, path, [b"expected"])
                self.assertTrue(path.exists())
                self.assertEqual(target.read_bytes(), b"expected")
                if mode != "running":
                    manager.assert_not_called()

    def test_cleanup_rejects_production_service_name(self):
        with patch.object(smoke.cli, "systemctl") as manager:
            with self.assertRaisesRegex(RuntimeError, "fake_unit_identity_changed"):
                smoke.cleanup_unit(
                    "flowdc-pilot.service", Path.home() / ".config/systemd/user/flowdc-pilot.service", []
                )
        manager.assert_not_called()
