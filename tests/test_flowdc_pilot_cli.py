"""CLI compatibility, installation and sanitized local subprocess boundaries."""

import json
import os
import shutil
import signal
import subprocess
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))

import flowdc_ops as ops
import flowdc_pilot_cli as cli
import test_flowdc_ops as bootstrap
from flowdc_pilot_journal import Journal
from flowdc_pilot_provider import LIFECYCLE_WRAPPER, Provider
from flowdc_pilot_supervisor import Supervisor, request
from test_flowdc_pilot_lifecycle import VM_IDS, FakeClock, FakeProvider, access, synthetic_service


class PilotCliTests(unittest.TestCase):
    def setUp(self):
        mask = os.umask(0o077)
        self.addCleanup(os.umask, mask)
        self.fixture = bootstrap.PreflightTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.capture(save=True)
        self.root = self.fixture.root
        self.state = self.fixture.state
        self.access = self.fixture.config / "access.json"
        self.access.write_text(json.dumps(access()))
        self.args = [
            "pilot",
            "prepare",
            "--state-root",
            str(self.state),
            "--profile",
            str(self.fixture.profile_path),
            "--spec",
            str(self.fixture.spec_path),
            "--inventory",
            str(self.fixture.inventory_path),
            "--access",
            str(self.access),
        ]

    def invoke(self, args):
        result = subprocess.run(
            [sys.executable, str(bootstrap.SCRIPT), *args], capture_output=True, text=True, timeout=10
        )
        if result.returncode == 78:
            self.assertIn("FLOW-DC supervisor verification failed:", result.stderr)
        else:
            self.assertEqual(result.stderr, "")
        self.assertNotIn("secret-marker", result.stdout)
        value = json.loads(result.stdout)
        self.assertEqual(value["schema_version"], 1)
        return result.returncode, value

    def test_prepare_has_no_provider_calls_and_preserves_read_only_wrapper(self):
        before = (self.root / "calls.jsonl").read_bytes()
        wrapper = (self.fixture.config / "js2").read_bytes()
        code, value = self.invoke(self.args)
        self.assertEqual(code, 3)  # Explicit installation has not been requested.
        self.assertEqual(value["data"]["desired"], "idle")
        self.assertEqual((self.root / "calls.jsonl").read_bytes(), before)
        self.assertEqual((self.fixture.config / "js2").read_bytes(), wrapper)
        self.assertEqual((self.state / "pilot.sqlite3").stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.invoke(self.args)[0], 3)

    def test_versioned_start_status_stop_reconcile_and_unknown_options(self):
        self.invoke(self.args)
        for action in ("start", "status", "stop", "reconcile"):
            code, value = self.invoke(["pilot", action, "--state-root", str(self.state)])
            self.assertEqual(code, 3)
            self.assertEqual(value["schema_version"], 1)
        code, value = self.invoke(["pilot", "start", "--secret=secret-marker"])
        self.assertEqual(code, 2)
        self.assertEqual(value["errors"][0]["code"], "invalid_arguments")

    def test_manual_supervisor_cannot_bypass_user_service(self):
        self.invoke(self.args)
        code, value = self.invoke(["pilot", "supervise", "--state-root", str(self.state)])
        self.assertEqual(code, 78)
        self.assertEqual(value["errors"][0]["code"], "service_not_installed")

    def test_verification_failure_is_fatal_and_original_cause_survives_busy_write(self):
        self.invoke(self.args)
        journal = Journal(self.state)
        for busy in (False, True):
            with (
                self.subTest(busy=busy),
                patch.object(cli, "verify_service", side_effect=cli.failure("installed_interpreter_changed")),
                patch.object(cli, "Provider") as provider,
            ):
                import io
                from contextlib import nullcontext

                context = (
                    patch.object(journal, "change", side_effect=cli.failure("pilot_state_busy"))
                    if busy
                    else nullcontext()
                )
                with context, patch.object(sys, "stderr", new_callable=io.StringIO) as stderr:
                    with self.assertRaises(ops.OpsError) as raised:
                        cli.supervise(journal)
                self.assertEqual(raised.exception.code, "installed_interpreter_changed")
                self.assertEqual(raised.exception.exit_code, 78)
                self.assertIn("installed_interpreter_changed", stderr.getvalue())
                provider.assert_not_called()
        self.assertEqual(journal.read()["checkpoint"], "installed_interpreter_changed")

    def test_transient_verification_failure_remains_restartable(self):
        self.invoke(self.args)
        journal = Journal(self.state)
        with patch.object(cli, "verify_service", side_effect=cli.failure("user_systemd_unavailable")):
            with self.assertRaises(ops.OpsError) as raised:
                cli.supervise(journal)
        self.assertEqual(raised.exception.exit_code, 3)

    def test_missing_pinned_file_is_fatal_but_io_outage_is_retryable(self):
        import io

        self.invoke(self.args)
        journal = Journal(self.state)
        with (
            patch.object(cli, "verify_service", side_effect=FileNotFoundError(2, "secret-marker")),
            patch.object(sys, "stderr", new_callable=io.StringIO) as stderr,
        ):
            with self.assertRaises(ops.OpsError) as raised:
                cli.supervise(journal)
        self.assertEqual(raised.exception.code, "installed_service_unreadable")
        self.assertEqual(raised.exception.exit_code, 78)
        self.assertNotIn("secret-marker", stderr.getvalue())
        with (
            patch.object(cli, "verify_service", side_effect=OSError(5, "secret-marker")),
            patch.object(cli, "Provider") as provider,
        ):
            with self.assertRaises(OSError):
                cli.supervise(journal)
            provider.assert_not_called()

    def test_supervise_sigterm_drains_outstanding_obligations(self):
        self.exercise_sigterm_cleanup()

    def test_sigterm_retries_busy_stop_and_tick_without_losing_obligations(self):
        self.exercise_sigterm_cleanup(contended=True)

    def test_sigterm_does_not_treat_corrupt_history_as_transient_contention(self):
        with self.assertRaises(ops.OpsError) as raised:
            self.exercise_sigterm_cleanup(contended=True, failure_code="invalid_journal_history")
        self.assertEqual(raised.exception.code, "invalid_journal_history")

    def exercise_sigterm_cleanup(self, *, contended=False, failure_code="pilot_state_busy"):
        self.invoke(self.args)
        journal = Journal(self.state)
        clock, provider = FakeClock(), FakeProvider()
        from test_flowdc_pilot_lifecycle import synthetic_service

        journal.change(lambda record: record.update(service=synthetic_service()))
        original_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
        self.addCleanup(lambda: [signal.signal(sig, handler) for sig, handler in original_handlers.items()])
        sleeps = 0
        signalled = False
        busy_stop = 2 if contended else 0
        busy_tick = 2 if contended else 0
        supervisor = Supervisor(journal, provider, clock=clock)
        original_tick = supervisor.tick

        def stop_request(journal, command):
            nonlocal busy_stop
            if command == "stop" and busy_stop:
                busy_stop -= 1
                self.assertTrue(journal.supervisor_locked())
                self.assertEqual(sum(vm["account"]["obligation"] for vm in journal.read()["vms"].values()), 3)
                raise cli.failure(failure_code)
            return request(journal, command, clock=clock)

        def tick():
            nonlocal busy_tick
            if busy_tick and journal.read()["desired"] == "stop":
                busy_tick -= 1
                self.assertTrue(journal.supervisor_locked())
                raise cli.failure(failure_code)
            original_tick()

        def pause(seconds):
            self.assertEqual(seconds, 2)
            nonlocal sleeps, signalled
            sleeps += 1
            self.assertLess(sleeps, 100)
            clock.advance(10)
            if sleeps == 1:
                request(journal, "start", clock=clock)
            if not signalled and sum(action == "unshelve" for action, _ in provider.actions) == 3:
                signalled = True
                signal.raise_signal(signal.SIGTERM)

        with (
            patch.object(cli, "verify_service"),
            patch.object(cli, "Provider", return_value=provider),
            patch.object(cli, "Supervisor", return_value=supervisor),
            patch.object(supervisor, "tick", side_effect=tick),
            patch.object(cli, "request", side_effect=stop_request),
            patch.object(cli.time, "sleep", side_effect=pause),
        ):
            _, code = cli.supervise(journal)
        self.assertEqual(code, 0)
        self.assertEqual((busy_stop, busy_tick), (0, 0))
        self.assertEqual(journal.read()["desired"], "idle")
        self.assertTrue(all(provider.states[key] == "SHELVED_OFFLOADED" for key in VM_IDS))
        self.assertEqual(sum(action == "shelve" for action, _ in provider.actions), 3)
        value, _ = cli.status(journal)
        self.assertTrue(all(vm["last_cleanup_request"]["action"] == "shelve" for vm in value["data"]["vms"]))

    def test_standalone_legacy_and_unavailable_pilot(self):
        script = self.root / "standalone" / "flowdc_ops.py"
        script.parent.mkdir()
        shutil.copyfile(bootstrap.SCRIPT, script)
        for args, expected in (
            (
                [
                    "init",
                    "--config-root",
                    str(self.root / "legacy-config"),
                    "--state-root",
                    str(self.root / "legacy-state"),
                ],
                0,
            ),
            (["pilot", "status"], 3),
        ):
            result = subprocess.run(
                [sys.executable, str(script), *args], capture_output=True, text=True, timeout=5
            )
            self.assertEqual(result.returncode, expected, result.stderr)
            value = json.loads(result.stdout)
            if expected:
                self.assertEqual(value["errors"][0]["code"], "pilot_unavailable")

    def test_library_import_does_not_change_search_path(self):
        code = "import importlib.util,sys; before=list(sys.path); spec=importlib.util.spec_from_file_location('library_ops',sys.argv[1]); module=importlib.util.module_from_spec(spec); sys.modules[spec.name]=module; spec.loader.exec_module(module); assert sys.path==before"
        result = subprocess.run(
            [sys.executable, "-c", code, str(bootstrap.SCRIPT)], capture_output=True, timeout=5
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_rates_or_stale_inventory_do_not_register(self):
        spec = json.loads(self.fixture.spec_path.read_text())
        spec["vms"][0]["rate"] = None
        self.fixture.spec_path.write_text(json.dumps(spec))
        self.assertEqual(self.invoke(self.args)[0], 3)
        self.assertFalse((self.state / "pilot.sqlite3").exists())

    def test_explicit_install_pins_release_and_is_repeatable_without_replacing_unit(self):
        self.invoke(self.args)
        journal = Journal(self.state)
        interpreter = self.root / "pinned-python"
        shutil.copyfile(Path(sys.executable).resolve(), interpreter)
        interpreter.chmod(0o700)
        with (
            patch.object(sys, "executable", str(interpreter)),
            patch.object(Path, "home", return_value=self.root),
            patch.object(cli, "systemctl", return_value=b"") as systemctl,
        ):
            cli.install(journal)
            first = journal.read()["service"]
            cli.install(journal)
            self.assertEqual(first, journal.read()["service"])
            invocation = "a" * 32
            with (
                patch.object(cli, "__file__", str(Path(first["release"]) / "flowdc_pilot_cli.py")),
                patch.dict(os.environ, {"INVOCATION_ID": invocation}),
                patch.object(
                    cli,
                    "systemctl",
                    return_value=f"MainPID={os.getpid()}\nInvocationID={invocation}\n".encode(),
                ),
            ):
                cli.verify_service(journal.read())
                for field, expected in (
                    ("digest", "installed_release_changed"),
                    ("interpreter_digest", "installed_interpreter_changed"),
                ):
                    changed = journal.read()
                    changed["service"][field] = "0" * 64
                    with self.assertRaises(ops.OpsError) as raised:
                        cli.verify_service(changed)
                    self.assertEqual(raised.exception.code, expected)
                with (
                    patch.object(cli, "systemctl", return_value=b"MainPID=0\nInvocationID=wrong\n"),
                    self.assertRaises(ops.OpsError) as raised,
                ):
                    cli.verify_service(journal.read())
                self.assertEqual(raised.exception.code, "user_systemd_identity_mismatch")

            self.assertIn(
                (("enable", "--now", "flowdc-pilot.service"),),
                [(call.args,) for call in systemctl.call_args_list],
            )
        unit = self.root / ".config/systemd/user/flowdc-pilot.service"
        text = unit.read_text()
        self.assertIn("Restart=always", text)
        self.assertIn("TimeoutStopSec=infinity", text)
        self.assertIn("RestartPreventExitStatus=78", text)
        self.assertIn("StandardError=journal", text)
        self.assertIn(first["release"], text)
        self.assertNotIn(str(Path(bootstrap.SCRIPT).parent), text)
        self.assertEqual(unit.stat().st_mode & 0o777, 0o600)
        unit.write_text("operator-owned-conflict")
        with (
            patch.object(sys, "executable", str(interpreter)),
            patch.object(Path, "home", return_value=self.root),
            patch.object(cli, "systemctl"),
            self.assertRaises(ops.OpsError),
        ):
            cli.install(journal)
        self.assertEqual(unit.read_text(), "operator-owned-conflict")

    def test_fake_harness_refuses_live_shaped_profile_without_changing_journal(self):
        from pilot_systemd_smoke import validate_fake

        self.invoke(self.args)
        journal = Journal(self.state)
        before = journal.read()
        with self.assertRaisesRegex(RuntimeError, "fake_fixture_required"):
            validate_fake(journal)
        self.assertEqual(before, journal.read())

    def test_fake_supervisor_process_survives_foreground_requester(self):
        from flowdc_pilot_supervisor import heartbeat_fresh
        from pilot_systemd_smoke import accelerated_clock

        self.invoke(self.args)
        journal = Journal(self.state)
        journal.change(lambda record: record.update(service=synthetic_service()))
        self.fixture.profile_path.write_text('{"fake_only":true}')
        smoke = Path(__file__).with_name("pilot_systemd_smoke.py")
        process = subprocess.Popen(
            [sys.executable, str(smoke), "--serve", str(self.state)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            deadline = time.monotonic() + 25
            while not heartbeat_fresh(journal.read(), accelerated_clock()):
                self.assertIsNone(process.poll())
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.1)
            requester = subprocess.run(
                [sys.executable, str(smoke), "--request", str(self.state)], capture_output=True, timeout=5
            )
            self.assertEqual(requester.returncode, 0, requester.stderr)
            self.assertIsNone(process.poll())
            while time.monotonic() < deadline:
                record = journal.read()
                if record["desired"] == "idle" and record.get("fake_actions"):
                    break
                time.sleep(0.1)
            self.assertEqual(record["desired"], "idle")
            self.assertEqual(sum(action == "unshelve" for action, _ in record["fake_actions"]), 3)
            self.assertTrue(
                all(
                    not vm["account"]["obligation"] and vm["observed"]["state"] == "SHELVED_OFFLOADED"
                    for vm in record["vms"].values()
                )
            )
            self.assertIsNone(process.poll())
        finally:
            process.terminate()
            process.communicate(timeout=5)

    def test_start_requires_lingering_without_changing_session_settings(self):
        with patch.object(ops, "run_bounded", return_value=(0, b"no\n")) as run:
            with self.assertRaises(ops.OpsError) as raised:
                cli.require_persistent_session()
            self.assertEqual(raised.exception.code, "user_lingering_required")
            self.assertEqual(run.call_args.args[0][1], "show-user")
        with patch.object(ops, "run_bounded", return_value=(0, b"yes\n")):
            cli.require_persistent_session()

    def test_provider_error_classification_never_returns_diagnostics(self):
        for diagnostic, category in (
            ("Quota exceeded secret-marker", b"quota"),
            ("HTTP 403 Forbidden secret-marker", b"permission"),
            ("HTTP 403 Forbidden quota policy secret-marker", b"permission"),
            ("secret-marker", b"provider"),
        ):
            code, raw = ops.run_bounded(
                [
                    sys.executable,
                    "-c",
                    "import sys; print(sys.argv[1],file=sys.stderr);sys.exit(1)",
                    diagnostic,
                ],
                timeout=3,
                classify_errors=True,
            )
            self.assertEqual(code, 1)
            self.assertEqual(raw, category)

    def test_fixed_lifecycle_wrapper_suppresses_source_output_and_rejects_arbitrary_action(self):
        wrapper = self.root / "lifecycle"
        wrapper.write_text(LIFECYCLE_WRAPPER)
        wrapper.chmod(0o700)
        result = subprocess.run(
            [
                "/bin/bash",
                "-p",
                str(wrapper),
                str(self.fixture.credential),
                str(self.fixture.client),
                "delete",
                bootstrap.SERVERS[0],
            ],
            capture_output=True,
            timeout=5,
        )
        self.assertEqual(result.returncode, 64)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, b"")

    def test_fixed_adapter_unshelve_uses_verified_allowlist_and_suppresses_openrc_output(self):
        self.invoke(self.args)
        record = Journal(self.state).read()
        group_id = "66666666-6666-4666-8666-666666666666"
        record["network"]["seen_groups"]["manager"] = group_id
        interface = record["access"]["interfaces"]["manager"]
        rules = [
            {
                "direction": "ingress",
                "protocol": "tcp",
                "remote_ip_prefix": access()["operator_cidr"],
                "ethertype": "IPv4",
                "port_range_min": 22,
                "port_range_max": 22,
            }
        ]
        for role, peer in record["access"]["interfaces"].items():
            if role != "manager":
                rules.extend(
                    {
                        "direction": "ingress",
                        "protocol": protocol,
                        "remote_ip_prefix": peer["fixed_ip"] + "/32",
                        "ethertype": "IPv4",
                        "port_range_min": None,
                        "port_range_max": None,
                    }
                    for protocol in ("tcp", "udp", "icmp")
                )
        self.fixture.fixture["responses"].update(
            {
                "port show": {
                    "id": interface["port_id"],
                    "device_id": bootstrap.SERVERS[0],
                    "project_id": bootstrap.PROJECT.replace("-", ""),
                    "network_id": interface["network_id"],
                    "port_security_enabled": True,
                    "allowed_address_pairs": [],
                    "fixed_ips": [{"ip_address": interface["fixed_ip"], "subnet_id": interface["subnet_id"]}],
                    "security_group_ids": [group_id],
                },
                "security group": {
                    "id": group_id,
                    "project_id": bootstrap.PROJECT,
                    "description": "flowdc-" + record["network"]["generation"] + "-manager",
                    "rules": rules,
                },
                "server unshelve": {},
            }
        )
        self.fixture.write_fixture()
        provider = Provider(self.fixture.profile, before_activation=lambda: None)
        provider.lifecycle(record, bootstrap.SERVERS[0], "unshelve")
        self.assertEqual(self.fixture.calls()[-1], ["server", "unshelve", bootstrap.SERVERS[0]])
        self.assertIsNone(provider.verified_record)
        with self.assertRaises(ops.OpsError):
            provider.call("unshelve", bootstrap.SERVERS[1])

    def test_network_provider_quota_checkpoint_sanitizes_fixture_diagnostics(self):
        provider = Provider(self.fixture.profile)
        # Existing fake client supports a bounded failure before response lookup.
        self.fixture.fixture["failure"] = {"key": "port show", "action": "fail"}
        self.fixture.write_fixture()
        with provider.step(), self.assertRaises(ops.OpsError) as raised:
            provider.call("port", bootstrap.SERVERS[0])
        self.assertEqual(raised.exception.code, "provider_request_failed")
        self.assertNotIn("secret-marker", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
