"""Private fake-service maintenance, exact accounting and crash boundary tests."""

import copy
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))

import flowdc_ops as ops
import flowdc_pilot_cli as cli
from flowdc_pilot_journal import private_lock, register
from flowdc_pilot_supervisor import request, sample_clock
from test_flowdc_pilot_lifecycle import access, spec


class UpgradeTests(unittest.TestCase):
    def setUp(self):
        mask = os.umask(0o077)
        self.addCleanup(os.umask, mask)
        temporary = tempfile.TemporaryDirectory(prefix="flowdc-upgrade-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        interpreter = self.root / "pinned-python"
        shutil.copyfile(Path(sys.executable).resolve(), interpreter)
        interpreter.chmod(0o700)
        runtime = patch.object(sys, "executable", str(interpreter))
        runtime.start()
        self.addCleanup(runtime.stop)
        config = self.root / "config"
        config.mkdir()
        profile = config / "profile.json"
        profile.write_text("{}")
        self.journal = register(profile, self.root / "state", spec(), access())
        self.calls = []
        self.home = patch.object(Path, "home", return_value=self.root)
        self.home.start()
        self.addCleanup(self.home.stop)
        self.systemctl = patch.object(cli, "systemctl", side_effect=self.service)
        self.systemctl.start()
        self.addCleanup(self.systemctl.stop)
        cli.install(self.journal)

        def settled(record):
            for index, vm in enumerate(record["vms"].values()):
                vm["observed"] = {"state": "SHELVED_OFFLOADED", "clock": asdict(sample_clock())}
                vm["account"]["consumed"] = 123.5 + index
                vm["account"]["uncertain"] = index == 1
            record["heartbeat"] = asdict(sample_clock())

        self.journal.change(settled)
        self.before = self.journal.read()
        source = Path(cli.__file__).resolve().parent
        candidate = self.root / "candidate"
        candidate.mkdir()
        for name in cli.MODULES:
            (candidate / name).write_bytes((source / name).read_bytes() + b"\n# synthetic candidate\n")
        self.candidate_path = candidate
        self.digest = hashlib.sha256(
            b"".join((candidate / name).read_bytes() for name in cli.MODULES)
        ).hexdigest()
        source_patch = patch.object(cli, "__file__", str(candidate / "flowdc_pilot_cli.py"))
        source_patch.start()
        self.addCleanup(source_patch.stop)
        self.args = SimpleNamespace(
            expected_current_digest=self.before["service"]["digest"],
            expected_candidate_digest=self.digest,
            recover=None,
        )
        self.provider = patch.object(cli, "Provider")
        self.fake_provider = self.provider.start()
        self.addCleanup(self.provider.stop)
        profile_patch = patch.object(ops, "load_profile", return_value={})
        profile_patch.start()
        self.addCleanup(profile_patch.stop)
        self.calls.clear()

    def service(self, *args):
        self.calls.append(args)
        if args[0] == "show":
            path = self.root / ".config/systemd/user" / cli.UNIT
            return f"FragmentPath={path}\nDropInPaths=\nNeedDaemonReload=no\nMainPID=0\nActiveState=inactive\n".encode()
        return b""

    def assert_preserved(self, service):
        self.assertEqual(self.journal.read(), dict(self.before, service=service, heartbeat=None))
        self.assertEqual(cli.unit_bytes(), cli.service_unit(service, self.journal.root))
        cli.verify_release(service, self.journal.root)
        cli.verify_release(self.before["service"], self.journal.root)

    def test_upgrade_preserves_complete_history_and_old_release(self):
        result, code = cli.upgrade_supervisor(self.journal, self.args)
        self.assertEqual(code, 0)
        self.assertTrue(result["data"]["accounts_preserved"])
        service = self.journal.read()["service"]
        self.assertEqual(service["digest"], self.digest)
        self.assert_preserved(service)
        self.fake_provider.return_value.verify_idle.assert_called_once_with(self.before)
        self.assertEqual(
            [args[0] for args in self.calls if args[0] != "show"], ["stop", "daemon-reload", "start"]
        )
        # The maintenance snapshot stays in the journal, including after publication.
        with self.journal.connection() as connection:
            snapshot = cli.maintenance_state(connection)
        self.assertEqual(snapshot["old"], self.before)
        self.assertEqual(snapshot["phase"], "published_complete")

    def test_explicit_candidate_can_reinstall_preserved_old_source(self):
        cli.upgrade_supervisor(self.journal, self.args)
        self.args.expected_current_digest = self.digest
        self.args.expected_candidate_digest = self.before["service"]["digest"]
        self.args.candidate_source = self.before["service"]["release"]
        cli.upgrade_supervisor(self.journal, self.args)
        self.assert_preserved(self.before["service"])
        with self.journal.connection() as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM pilot_maintenance").fetchone()[0], 2)

    def test_unsafe_state_refused_before_unit_or_journal_changes(self):
        cases = [
            lambda r: r.update(desired="stop"),
            lambda r: r["network"].update(rolled_back=False),
            lambda r: r["network"].update(ready=True),
            lambda r: next(iter(r["vms"].values())).update(observed=None),
            lambda r: next(iter(r["vms"].values()))["observed"].update(state="ACTIVE"),
            lambda r: next(iter(r["vms"].values())).update(phase="verify_offload"),
        ]
        for change in cases:
            with self.subTest(change=change):
                bad = copy.deepcopy(self.before)
                change(bad)
                with patch.object(self.journal, "read", return_value=bad):
                    with self.assertRaises(ops.OpsError) as caught:
                        cli.upgrade_supervisor(self.journal, self.args)
                self.assertEqual(caught.exception.code, "maintenance_idle_required")
                self.assertEqual(self.journal.read(), self.before)
                self.assertEqual(
                    cli.unit_bytes(), cli.service_unit(self.before["service"], self.journal.root)
                )
        self.assertFalse(self.calls)
        self.fake_provider.assert_not_called()

    def test_provenance_failures_precede_maintenance(self):
        for kind in ("current", "candidate", "source", "interpreter", "unit", "dropin"):
            with self.subTest(kind=kind):
                args = copy.copy(self.args)
                if kind == "current":
                    args.expected_current_digest = "0" * 64
                if kind == "candidate":
                    args.expected_candidate_digest = "0" * 64
                from contextlib import nullcontext

                contexts = {
                    "source": patch.object(
                        cli, "trusted_bytes", side_effect=cli.failure("unsafe_release_file")
                    ),
                    "interpreter": patch.object(
                        cli, "verify_release", side_effect=cli.failure("installed_interpreter_changed")
                    ),
                    "unit": patch.object(cli, "unit_bytes", return_value=b"unexpected"),
                    "dropin": patch.object(cli, "systemctl", return_value=b"DropInPaths=unexpected\n"),
                }
                with contexts.get(kind, nullcontext()), self.assertRaises(ops.OpsError):
                    cli.upgrade_supervisor(self.journal, args)
                self.assertEqual(self.journal.read(), self.before)
        self.fake_provider.assert_not_called()

    def test_fault_boundaries_recover_both_directions(self):
        # Separate fixture per boundary/direction to preserve immutable accounts.
        for boundary in ("stop", "replace", "unit", "reload", "binding", "publish", "start"):
            for direction in ("complete", "rollback"):
                if boundary == "start" and direction == "rollback":
                    continue  # Publication is final; rollback becomes a new guarded upgrade.
                with self.subTest(boundary=boundary, direction=direction):
                    case = UpgradeTests()
                    case.setUp()
                    try:
                        case.exercise_fault(boundary, direction)
                    finally:
                        case.doCleanups()

    def exercise_fault(self, boundary, direction):
        real_update = cli.maintenance_update
        real_replace = cli.replace_unit

        def update(journal, phase, **kwargs):
            real_update(journal, phase, **kwargs)
            if phase == boundary or (boundary == "publish" and phase.startswith("published")):
                raise RuntimeError("synthetic crash")

        def replace(*args):
            real_replace(*args)
            if boundary == "replace":
                raise RuntimeError("synthetic crash")

        def service(*args):
            if args[0] == {"reload": "daemon-reload"}.get(boundary, boundary):
                raise RuntimeError("synthetic crash")
            return self.service(*args)

        with (
            patch.object(cli, "maintenance_update", side_effect=update),
            patch.object(cli, "replace_unit", side_effect=replace),
            patch.object(cli, "systemctl", side_effect=service),
            self.assertRaisesRegex(RuntimeError, "synthetic crash"),
        ):
            cli.upgrade_supervisor(self.journal, self.args)
        published = boundary in ("publish", "start")
        if not published:
            with self.assertRaises(ops.OpsError) as caught:
                self.journal.read()
            self.assertEqual(caught.exception.code, "unsupported_journal")
            self.assert_legacy_refuses()
        else:
            before_start = self.journal.read()
            self.assertIsNone(before_start["heartbeat"])
            with self.journal.supervisor_lock(), self.assertRaises(ops.OpsError):
                request(self.journal, "start")
            # Publication is complete but the replacement service has not started.
            self.assertFalse(self.journal.supervisor_locked())
            with self.assertRaises(ops.OpsError) as caught:
                request(self.journal, "start")
            self.assertEqual(caught.exception.code, "supervisor_not_ready")
            self.assertEqual(self.journal.read(), before_start)
        self.args.recover = "complete" if published else direction
        _, code = cli.upgrade_supervisor(self.journal, self.args)
        self.assertEqual(code, 0)
        target = self.before["service"] if self.args.recover == "rollback" else self.journal.read()["service"]
        self.assert_preserved(target)

    def assert_legacy_refuses(self):
        # Run the frozen old connection guard in separate client processes.
        fixtures = Path(__file__).resolve().parent / "fixtures"
        source = Path(__file__).resolve().parents[1] / "bin"
        for command in ("start", "stop", "reconcile", "supervise"):
            with self.journal.supervisor_lock():
                result = subprocess.run(
                    [
                        sys.executable,
                        "-B",
                        "-c",
                        "import sys; sys.path[:0]=sys.argv[1:3]; "
                        "from flowdc_pilot_journal import Journal; "
                        "from pilot_v1_connection import connection; "
                        "Journal.connection=connection; "
                        "from flowdc_pilot_supervisor import request; "
                        "j=Journal(sys.argv[3]); "
                        "from flowdc_ops import OpsError; "
                        "exec(\"try:\\n request(j, sys.argv[4]) if sys.argv[4] != 'supervise' else j.read()\\nexcept OpsError as e:\\n print(e.code); sys.exit(3)\")",
                        str(source),
                        str(fixtures),
                        str(self.journal.root),
                        command,
                    ],
                    capture_output=True,
                    timeout=5,
                )
            self.assertEqual(result.returncode, 3, result.stderr)
            self.assertEqual(result.stdout.strip(), b"unsupported_journal")

    def test_concurrent_maintenance_refused(self):
        with (
            ops.private_directory(self.journal.root) as parent,
            private_lock(parent, "maintenance.lock", blocking=False),
        ):
            with self.assertRaises(ops.OpsError):
                cli.upgrade_supervisor(self.journal, self.args)
        self.assertEqual(self.journal.read(), self.before)
        self.assertFalse(self.calls)

    def test_start_winning_before_maintenance_is_preserved(self):
        real_stage = cli.stage_candidate

        def stage(*args):
            result = real_stage(*args)
            self.journal.change(
                lambda r: r.update(desired="run", window={"seconds": 1800, "inspection": True})
            )
            return result

        with patch.object(cli, "stage_candidate", side_effect=stage):
            with self.assertRaises(ops.OpsError) as caught:
                cli.upgrade_supervisor(self.journal, self.args)
        self.assertEqual(caught.exception.code, "maintenance_idle_required")
        self.assertEqual(self.journal.read()["desired"], "run")
        self.assertNotIn(("stop", cli.UNIT), self.calls)

    def test_account_obligation_refused(self):
        from flowdc_pilot_journal import allowance

        def obligation(r):
            vm = next(iter(r["vms"].values()))
            vm["account"] = asdict(
                allowance(vm["account"]).activation_intent(sample_clock(), window_seconds=1800)
            )
            vm["phase"] = "pending"

        self.journal.change(obligation)
        before = self.journal.read()
        with self.assertRaises(ops.OpsError) as caught:
            cli.upgrade_supervisor(self.journal, self.args)
        self.assertEqual(caught.exception.code, "maintenance_idle_required")
        self.assertEqual(self.journal.read(), before)
        self.assertFalse(self.calls)

    def test_tampered_release_and_candidate_link_are_refused(self):
        module = Path(self.before["service"]["release"]) / "flowdc_pilot.py"
        saved = module.read_bytes()
        module.write_bytes(saved + b"\n# unexpected\n")
        with self.assertRaises(ops.OpsError) as caught:
            cli.upgrade_supervisor(self.journal, self.args)
        self.assertEqual(caught.exception.code, "installed_release_changed")
        module.write_bytes(saved)
        candidate = self.candidate_path / "flowdc_pilot.py"
        candidate.unlink()
        candidate.symlink_to(module)
        with self.assertRaises((ops.OpsError, OSError)):
            cli.upgrade_supervisor(self.journal, self.args)
        self.assertEqual(self.journal.read(), self.before)
        self.assertNotIn(("stop", cli.UNIT), self.calls)

    def test_recovery_refuses_unit_edits_and_preserves_block(self):
        self.fake_provider.return_value.verify_idle.side_effect = cli.failure("probe_timeout")
        with self.assertRaises(ops.OpsError):
            cli.upgrade_supervisor(self.journal, self.args)
        unit = self.root / ".config/systemd/user" / cli.UNIT
        unit.write_bytes(unit.read_bytes() + b"\n# unexpected edit\n")
        self.fake_provider.return_value.verify_idle.side_effect = None
        self.args.recover = "complete"
        with self.assertRaises(ops.OpsError) as caught:
            cli.upgrade_supervisor(self.journal, self.args)
        self.assertEqual(caught.exception.code, "unexpected_unit_content")
        with self.assertRaises(ops.OpsError):
            self.journal.read()
        self.assertNotIn(("start", cli.UNIT), self.calls)

    def test_still_owned_supervisor_prevents_unit_replacement(self):
        with self.journal.supervisor_lock():
            with self.assertRaises(ops.OpsError) as caught:
                cli.upgrade_supervisor(self.journal, self.args)
        self.assertEqual(caught.exception.code, "supervisor_already_running")
        self.assertEqual(cli.unit_bytes(), cli.service_unit(self.before["service"], self.journal.root))
        self.fake_provider.assert_not_called()
        with self.assertRaises(ops.OpsError):
            self.journal.read()
        self.assertNotIn(("start", cli.UNIT), self.calls)

    def test_fresh_provider_uncertainty_leaves_recovery_checkpoint(self):
        self.fake_provider.return_value.verify_idle.side_effect = cli.failure("initial_offload_required")
        with self.assertRaises(ops.OpsError):
            cli.upgrade_supervisor(self.journal, self.args)
        with self.assertRaises(ops.OpsError):
            self.journal.read()
        with self.journal.connection(maintenance=True) as connection:
            self.assertEqual(cli.maintenance_state(connection)["old"], self.before)
        self.assertNotIn(("start", cli.UNIT), self.calls)
        self.assertEqual(cli.unit_bytes(), cli.service_unit(self.before["service"], self.journal.root))
