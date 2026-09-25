"""Finite allowance request boundaries; private fixtures only, no provider calls."""

import copy
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))

import flowdc_ops as ops
import flowdc_pilot_journal as journal


class GrantRequestTests(unittest.TestCase):
    def request(self):
        return {
            "schema_version": 1,
            "grant_id": "11111111-1111-4111-8111-111111111111",
            "registration_id": "22222222-2222-4222-8222-222222222222",
            "expected_binding_sha256": "a" * 64,
            "expected_limits_seconds": {"manager": 3608, "worker": 3595, "origin": 3602},
            "additional_seconds": 600,
        }

    def test_valid_request_is_not_rewritten_and_digest_is_canonical(self):
        value = self.request()
        before = copy.deepcopy(value)
        self.assertEqual(journal.validate_grant_request(value), before)
        self.assertEqual(value, before)
        self.assertEqual(
            journal.grant_request_digest(value),
            hashlib.sha256(journal.encode(value).encode()).hexdigest(),
        )
        self.assertEqual(
            journal.grant_request_digest(value),
            journal.grant_request_digest(dict(reversed(list(value.items())))),
        )

    def test_strict_fields_types_and_bounds(self):
        cases = []
        for key in self.request():
            value = self.request()
            del value[key]
            cases.append(value)
        for key, invalids in {
            "schema_version": [True, 1.0, 2, None],
            "grant_id": [None, 12, "secret-marker"],
            "registration_id": [False, "secret-marker"],
            "expected_binding_sha256": [None, "A" * 64, "a" * 63, "g" * 64],
            "additional_seconds": [True, False, 0, -1, 1801, 1.5, "600", None, float("inf"), float("nan")],
            "expected_limits_seconds": [None, [], {}, {"manager": 3000, "worker": 3000}],
        }.items():
            for invalid in invalids:
                value = self.request()
                value[key] = invalid
                cases.append(value)
        value = self.request()
        value["unexpected"] = "secret-marker"
        cases.append(value)
        for role in ("manager", "worker", "origin", "extra"):
            for invalid in (True, 0, -1, 7201, 3600.0, "3600", None):
                value = self.request()
                value["expected_limits_seconds"][role] = invalid
                cases.append(value)
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(ops.OpsError) as raised:
                    journal.validate_grant_request(value)
                self.assertEqual(raised.exception.code, "invalid_allowance_grant")
                self.assertNotIn("secret-marker", str(raised.exception))

    def test_equal_addition_and_lifetime_ceiling(self):
        for addition in (1, 600, 1800):
            value = self.request()
            value["additional_seconds"] = addition
            value["expected_limits_seconds"] = dict.fromkeys(("manager", "worker", "origin"), 7200 - addition)
            journal.validate_grant_request(value)
            for role in value["expected_limits_seconds"]:
                overflow = copy.deepcopy(value)
                overflow["expected_limits_seconds"][role] += 1
                with self.assertRaises(ops.OpsError):
                    journal.validate_grant_request(overflow)

    def test_binding_pins_only_the_approved_registration_fields(self):
        record = {
            "registration_id": self.request()["registration_id"],
            "profile_path": "/tmp/private/profile.json",
            "spec": {"vms": []},
            "access": {"interfaces": {}},
            "service": {"digest": "b" * 64},
            "heartbeat": None,
            "vms": {"account": {"consumed": 12}},
        }
        before = journal.allowance_binding_sha256(record)
        for key in ("registration_id", "profile_path", "spec", "access", "service"):
            changed = copy.deepcopy(record)
            changed[key] = "changed"
            self.assertNotEqual(journal.allowance_binding_sha256(changed), before)
        record["heartbeat"] = {"utc": 123}
        record["vms"]["account"]["consumed"] = 13
        self.assertEqual(journal.allowance_binding_sha256(record), before)

    def test_private_file_and_strict_json_boundaries(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "grant.json"
            path.write_text(json.dumps(self.request()))
            path.chmod(0o600)
            self.assertEqual(journal.load_grant_request(str(path)), self.request())
            path.chmod(0o644)
            with self.assertRaises(ops.OpsError):
                journal.load_grant_request(str(path))
            path.chmod(0o600)
            for name, link in (("symlink.json", os.symlink), ("hardlink.json", os.link)):
                alias = Path(directory) / name
                link(path, alias)
                with self.assertRaises(ops.OpsError):
                    journal.load_grant_request(str(alias))
                alias.unlink()
            for raw in (
                '{"schema_version":1,"schema_version":1}',
                '{"additional_seconds":NaN}',
                " " * (ops.MAX_BYTES + 1),
            ):
                path.write_text(raw)
                with self.assertRaises(ops.OpsError):
                    journal.load_grant_request(str(path))


class GrantTransitionTests(unittest.TestCase):
    def setUp(self):
        from dataclasses import asdict

        from test_flowdc_pilot_lifecycle import FakeClock, access, spec, synthetic_service

        self.mask = os.umask(0o077)
        self.addCleanup(os.umask, self.mask)
        temporary = tempfile.TemporaryDirectory(prefix="flowdc-grant-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        config = self.root / "config"
        config.mkdir()
        self.profile = config / "profile.json"
        self.profile.write_text("{}")
        selected = spec()
        for index, vm in enumerate(selected["vms"]):
            vm["active_seconds"] = 3600 + index
        self.selected = copy.deepcopy(selected)
        self.journal = journal.register(self.profile, self.root / "state", selected, access())
        self.clock = FakeClock()

        def settled(record):
            record["service"] = synthetic_service()
            record["heartbeat"] = asdict(self.clock())
            record["events"].append({"kind": "historical_event", "data": {}, "count": 2})
            for index, vm in enumerate(record["vms"].values()):
                vm["observed"] = {"state": "SHELVED_OFFLOADED", "clock": asdict(self.clock())}
                vm["account"]["consumed"] = 123.5 + index

        self.journal.change(settled)
        self.before = self.journal.read()
        self.request = GrantRequestTests().request()
        self.request.update(
            registration_id=self.before["registration_id"],
            expected_binding_sha256=journal.allowance_binding_sha256(self.before),
            expected_limits_seconds={vm["role"]: vm["active_seconds"] for vm in selected["vms"]},
        )
        self.path = config / "grant.json"
        self.path.write_text(json.dumps(self.request))

    def apply(self, request=None, snapshot=None, start=None, recheck=lambda record: None):
        return self.journal.extend_allowance(
            request or self.request,
            snapshot or self.before,
            start or self.clock(),
            clock=self.clock,
            recheck=recheck,
        )

    def test_atomic_receipt_and_only_authorized_fields_change(self):
        receipt, applied, after = self.apply()
        self.assertTrue(applied)
        self.assertEqual(after, self.journal.read())
        self.assertEqual(receipt["request"], self.request)
        self.assertEqual(receipt["request_sha256"], journal.grant_request_digest(self.request))
        expected = copy.deepcopy(self.before)
        for selected in expected["spec"]["vms"]:
            selected["active_seconds"] += 600
            expected["vms"][selected["id"]]["account"]["limit"] += 600
            role = receipt["roles"][selected["role"]]
            self.assertEqual(role["new_limit_seconds"] - role["old_limit_seconds"], 600)
            self.assertEqual(role["consumed_seconds"], expected["vms"][selected["id"]]["account"]["consumed"])
        expected["events"].append({"kind": "allowance_granted", "data": receipt})
        self.assertEqual(after, expected)
        with self.journal.connection() as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 1)

    def test_replay_after_consumption_and_changed_binding_returns_original_receipt(self):
        receipt, _, _ = self.apply()

        def later(record):
            for vm in record["vms"].values():
                vm["account"]["consumed"] += 10
            record["service"]["digest"] = "c" * 64

        self.journal.change(later)
        before_retry = self.journal.read()
        actual, applied, after = self.apply(recheck=lambda record: self.fail("replay readiness probe"))
        self.assertFalse(applied)
        self.assertEqual(receipt, actual)
        self.assertEqual(before_retry, after)

    def test_conflict_canonical_uuid_alias_and_stale_new_identity(self):
        self.apply()
        for field, value in (
            ("additional_seconds", 601),
            ("grant_id", self.request["grant_id"].replace("-", "")),
        ):
            conflicting = dict(self.request, **{field: value})
            with self.assertRaises(ops.OpsError) as raised:
                self.apply(request=conflicting)
            self.assertEqual(raised.exception.code, "allowance_grant_conflict")
        from uuid import uuid4

        with self.assertRaises(ops.OpsError):
            self.apply(request=dict(self.request, grant_id=str(uuid4())), snapshot=self.journal.read())
        self.assertEqual(len(journal.grant_receipts(self.journal.read())), 1)

    def test_intervening_unrelated_grant_and_changed_snapshot_fields_refuse(self):
        from uuid import uuid4

        unrelated = dict(self.request, grant_id=str(uuid4()))
        self.apply(request=unrelated)
        after = self.journal.read()
        with self.assertRaises(ops.OpsError) as raised:
            self.apply()
        self.assertEqual(raised.exception.code, "allowance_state_changed")
        self.assertEqual(self.journal.read(), after)
        mutations = (
            lambda r: r["events"].append({"kind": "intervening_event", "data": {}}),
            lambda r: r["network"].update(generation=str(uuid4())),
            lambda r: next(iter(r["vms"].values()))["observed"]["clock"].update(utc=100001),
            lambda r: r["service"].update(digest="c" * 64),
            lambda r: r.update(window={"seconds": 1800, "inspection": True}),
            lambda r: r.update(checkpoint="intervening_checkpoint"),
        )
        for mutation in mutations:
            snapshot = self.journal.read()
            self.journal.change(mutation)
            intervening = self.journal.read()
            next_request = dict(self.request, grant_id=str(uuid4()))
            with self.subTest(mutation=mutation):
                with self.assertRaises(ops.OpsError) as raised:
                    self.apply(request=next_request, snapshot=snapshot)
                self.assertEqual(raised.exception.code, "allowance_state_changed")
                self.assertEqual(self.journal.read(), intervening)

    def test_subsequent_distinct_grant_and_stale_enrollment(self):
        from uuid import uuid4

        from test_flowdc_pilot_lifecycle import access

        self.apply()
        after = self.journal.read()
        next_request = dict(
            self.request,
            grant_id=str(uuid4()),
            expected_binding_sha256=journal.allowance_binding_sha256(after),
            expected_limits_seconds={vm["role"]: vm["active_seconds"] for vm in after["spec"]["vms"]},
        )
        self.apply(request=next_request, snapshot=after)
        self.assertEqual(len(journal.grant_receipts(self.journal.read())), 2)
        with self.assertRaises(ops.OpsError):
            journal.register(self.profile, self.journal.root, self.selected, access())

    def test_ordinary_change_preserves_receipts_limits_consumption_uncertainty(self):
        self.apply()
        original = self.journal.read()

        def limit(record):
            record["spec"]["vms"][0]["active_seconds"] += 1
            record["vms"][record["spec"]["vms"][0]["id"]]["account"]["limit"] += 1

        def consume(record):
            next(iter(record["vms"].values()))["account"]["consumed"] = 0

        def receipt(record):
            record["events"][-1]["data"]["clock"]["utc"] += 1

        for mutation in (
            limit,
            consume,
            receipt,
            lambda r: r["events"].pop(),
            lambda r: r["events"].append(copy.deepcopy(r["events"][-1])),
        ):
            with self.subTest(mutation=mutation), self.assertRaises(ops.OpsError):
                self.journal.change(mutation)
            self.assertEqual(self.journal.read(), original)
        self.journal.change(lambda r: next(iter(r["vms"].values()))["account"].update(uncertain=True))
        with self.assertRaises(ops.OpsError):
            self.journal.change(lambda r: next(iter(r["vms"].values()))["account"].update(uncertain=False))

    def test_invalid_receipt_contents_refuse_read_validation(self):
        self.apply()
        original = self.journal.read()
        for key, value in (("request_sha256", "0" * 64), ("roles", {}), ("clock", {})):
            candidate = copy.deepcopy(original)
            candidate["events"][-1]["data"][key] = value
            with self.assertRaises(ops.OpsError):
                journal.validate_record(candidate)

    def test_precommit_exception_rolls_back_and_postcommit_loss_replays(self):
        from unittest.mock import patch

        real_validate = journal.validate_record

        def interrupt(record):
            result = real_validate(record)
            if journal.grant_receipts(record):
                raise KeyboardInterrupt
            return result

        with patch.object(journal, "validate_record", side_effect=interrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.apply()
        self.assertEqual(self.journal.read(), self.before)
        self.apply()  # Simulate loss of this response.
        self.assertFalse(self.apply()[1])

    def test_snapshot_drift_and_idle_heartbeat_exception(self):
        from dataclasses import asdict

        from flowdc_pilot_supervisor import request

        for command in ("stop", "reconcile"):
            with self.subTest(command=command):
                # Capture each durable command independently.
                saved = self.journal.read()
                request(self.journal, command, clock=self.clock)
                after = self.journal.read()
                with self.assertRaises(ops.OpsError):
                    self.apply(snapshot=saved)
                self.assertEqual(self.journal.read(), after)
        # Return the fake fixture to settled idle before testing heartbeat progress.
        self.journal.change(lambda r: r.update(desired="idle"))
        self.journal.change(lambda r: [vm.update(phase="offloaded") for vm in r["vms"].values()])
        saved = self.journal.read()
        self.clock.advance()
        self.journal.change(lambda r: r.update(heartbeat=asdict(self.clock())))
        self.assertTrue(self.apply(snapshot=saved)[1])

    def test_stale_backward_reboot_and_discontinuous_proof(self):
        from flowdc_pilot import ClockSample

        for end in (
            ClockSample("fake-boot", 121, 100121),
            ClockSample("other", 1, 100001),
            ClockSample("fake-boot", 1, 100010),
        ):
            with self.subTest(end=end), self.assertRaises(ops.OpsError):
                self.journal.extend_allowance(
                    self.request, self.before, self.clock(), clock=lambda end=end: end, recheck=lambda r: None
                )
            self.assertEqual(self.journal.read(), self.before)
        with self.assertRaises(ops.OpsError):
            self.apply(start=ClockSample("fake-boot", 1, 100001))

    def test_unhealthy_snapshot_refused_without_grant(self):
        for mutation in (
            lambda r: r.update(checkpoint="test_checkpoint"),
            lambda r: r.update(desired="stop"),
            lambda r: r["network"].update(rolled_back=False),
            lambda r: r["network"].update(ready=True),
            lambda r: next(iter(r["vms"].values()))["account"].update(uncertain=True),
            lambda r: next(iter(r["vms"].values())).update(phase="pending"),
        ):
            candidate = copy.deepcopy(self.before)
            mutation(candidate)
            with self.assertRaises(ops.OpsError):
                journal.require_grant_idle(candidate)
        self.assertEqual(self.journal.read(), self.before)


class GrantCliTests(unittest.TestCase):
    setUp = GrantTransitionTests.setUp

    def invoke(self, provider=None):
        from unittest.mock import patch

        import flowdc_pilot_cli as cli

        with (
            patch.object(cli, "require_persistent_session"),
            patch.object(cli, "verify_grant_controller"),
            patch.object(ops, "load_profile", return_value={}),
            patch.object(cli, "Provider") as adapter,
        ):
            if provider:
                adapter.return_value.verify_idle.side_effect = provider
            result = cli.extend_allowance(self.journal, str(self.path), clock=self.clock)
            return result, adapter

    def test_application_replay_and_status_are_redacted(self):
        import flowdc_pilot_cli as cli

        (value, code), adapter = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(value["data"]["result"], "applied")
        self.assertFalse(value["data"]["current_readiness_verified"])
        adapter.return_value.verify_idle.assert_called_once()
        adapter.return_value.lifecycle.assert_not_called()
        (retry, code), adapter = self.invoke()
        self.assertEqual(retry["data"]["result"], "already_applied")
        self.assertEqual(value["data"]["receipt"], retry["data"]["receipt"])
        adapter.assert_not_called()
        status, _ = cli.status(self.journal)
        self.assertEqual(status["data"]["allowance_grants"], [value["data"]["receipt"]])
        self.assertEqual(
            status["data"]["allowance_binding_sha256"], journal.allowance_binding_sha256(self.journal.read())
        )
        self.assertNotIn(str(self.profile), json.dumps(value))

    def test_provider_failure_and_state_change_preserve_history(self):
        from flowdc_pilot_supervisor import request

        def failure(record):
            raise ops.OpsError("wrong_provider_identity", "fixed", "fixed", 3)

        with self.assertRaises(ops.OpsError):
            self.invoke(failure)
        self.assertEqual(self.journal.read(), self.before)

        def intervening(record):
            # This write succeeds while verification is in progress: provider
            # verification is outside SQLite transactions and the journal flock.
            request(self.journal, "stop", clock=self.clock)

        with self.assertRaises(ops.OpsError) as raised:
            self.invoke(intervening)
        self.assertEqual(raised.exception.code, "allowance_state_changed")
        self.assertEqual(self.journal.read()["events"][-1]["kind"], "stop_requested")
        self.assertEqual(journal.grant_receipts(self.journal.read()), [])

    def test_experiment_owner_and_both_locks_refuse(self):
        runs = self.journal.root / "runs"
        runs.mkdir()
        owner = runs / "active-experiment.json"
        owner.write_text(json.dumps({"run_id": "exp-" + "a" * 32}))
        with self.assertRaises(ops.OpsError):
            self.invoke()
        owner.write_text('{"run_id":null}')
        for directory, name in ((runs, "experiment.lock"), (self.journal.root, "maintenance.lock")):
            with ops.private_directory(directory) as parent, journal.private_lock(parent, name):
                with self.assertRaises(ops.OpsError):
                    self.invoke()
        self.assertEqual(self.journal.read(), self.before)
        self.invoke()

    def test_installation_provenance_and_liveness_refuse_before_provider(self):
        from unittest.mock import patch

        import flowdc_pilot_cli as cli

        with patch.object(cli, "require_persistent_session"), patch.object(cli, "Provider") as provider:
            with self.assertRaises(ops.OpsError) as raised:
                cli.extend_allowance(self.journal, str(self.path), clock=self.clock)
            self.assertEqual(raised.exception.code, "immutable_release_required")
            provider.assert_not_called()
        with (
            patch.object(cli, "require_persistent_session"),
            patch.object(cli, "__file__", self.before["service"]["release"] + "/flowdc_pilot_cli.py"),
            patch.object(sys, "executable", self.before["service"]["interpreter"]),
            patch.object(cli, "verify_release"),
            patch.object(
                cli, "unit_bytes", return_value=cli.service_unit(self.before["service"], self.journal.root)
            ),
            patch.object(cli, "verify_unit_origin"),
        ):
            with self.assertRaises(ops.OpsError) as raised:
                cli.verify_grant_controller(self.journal, self.before, clock=self.clock)
            self.assertEqual(raised.exception.code, "supervisor_not_ready")
            with self.journal.supervisor_lock():
                cli.verify_grant_controller(self.journal, self.before, clock=self.clock)
                self.clock.advance(31)
                with self.assertRaises(ops.OpsError):
                    cli.verify_grant_controller(self.journal, self.before, clock=self.clock)

    def test_commit_rechecks_local_provenance_and_liveness(self):
        from unittest.mock import patch

        import flowdc_pilot_cli as cli

        for code in ("installed_release_changed", "unexpected_unit_provenance", "supervisor_not_ready"):
            with (
                self.subTest(code=code),
                patch.object(cli, "require_persistent_session"),
                patch.object(cli, "verify_grant_controller", side_effect=[None, journal.failure(code)]),
                patch.object(ops, "load_profile", return_value={}),
                patch.object(cli, "Provider") as provider,
            ):
                with self.assertRaises(ops.OpsError) as raised:
                    cli.extend_allowance(self.journal, str(self.path), clock=self.clock)
                self.assertEqual(raised.exception.code, code)
                provider.return_value.verify_idle.assert_called_once()
                self.assertEqual(self.journal.read(), self.before)

    def test_private_cli_grammar_requires_explicit_grant_and_rejects_unknown(self):
        import subprocess

        script = Path(__file__).resolve().parents[1] / "bin" / "flowdc_ops.py"
        for extra in ([], ["--grant", str(self.path), "--seconds", "600"]):
            result = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "pilot",
                    "extend-allowance",
                    "--state-root",
                    str(self.journal.root),
                    *extra,
                ],
                capture_output=True,
                timeout=5,
            )
            self.assertEqual(result.returncode, 2)
        self.assertEqual(self.journal.read(), self.before)


class GrantProcessTests(unittest.TestCase):
    setUp = GrantTransitionTests.setUp
    apply = GrantTransitionTests.apply

    CHILD = """
import contextlib, json, os, sys
sys.path.insert(0, sys.argv[1])
import flowdc_ops as ops
from flowdc_pilot import ClockSample
from flowdc_pilot_journal import Journal, load_grant_request
journal = Journal(sys.argv[2])
request = load_grant_request(sys.argv[3])
snapshot = journal.read()
clock = lambda: ClockSample("fake-boot", 0, 100000)
mode = sys.argv[4]
if mode in ("before", "after"):
    original = journal.connection
    class Proxy:
        def __init__(self, connection): self.connection = connection
        def execute(self, *args): return self.connection.execute(*args)
        def commit(self):
            if mode == "before": os._exit(91)
            self.connection.commit()
            os._exit(92)
    @contextlib.contextmanager
    def connection():
        with original() as selected:
            yield Proxy(selected)
    journal.connection = connection
print("ready", flush=True)
sys.stdin.readline()
try:
    receipt, applied, current = journal.extend_allowance(request, snapshot, clock(), clock=clock, recheck=lambda r: None)
    print(json.dumps({"applied": applied, "receipt": receipt}), flush=True)
except ops.OpsError as exc:
    print(json.dumps({"refused": exc.code}), flush=True)
"""

    def child(self, path=None, mode="normal"):
        import subprocess

        process = subprocess.Popen(
            [
                sys.executable,
                "-B",
                "-c",
                self.CHILD,
                str(Path(journal.__file__).parent),
                str(self.journal.root),
                str(path or self.path),
                mode,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        def cleanup():
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)

        self.addCleanup(cleanup)
        self.assertEqual(process.stdout.readline().strip(), "ready")
        return process

    def finish(self, process):
        raw, error = process.communicate("go\n", timeout=10)
        self.assertEqual(error, "")
        self.assertEqual(process.returncode, 0)
        return json.loads(raw)

    def test_two_process_identical_requests_commit_once(self):
        first, second = self.child(), self.child()
        for process in (first, second):
            process.stdin.write("go\n")
            process.stdin.flush()
        results = []
        for process in (first, second):
            raw, error = process.communicate(timeout=10)
            self.assertEqual((process.returncode, error), (0, ""))
            results.append(json.loads(raw))
        one, two = results
        self.assertEqual(sorted(result["applied"] for result in results), [False, True])
        self.assertEqual(one["receipt"], two["receipt"])
        self.assertEqual(len(journal.grant_receipts(self.journal.read())), 1)

    def test_conflicting_process_requests(self):
        alternate = self.path.parent / "conflicting.json"
        alternate.write_text(json.dumps(dict(self.request, additional_seconds=601)))
        first, second = self.child(), self.child(alternate)
        self.assertTrue(self.finish(first)["applied"])
        self.assertEqual(self.finish(second)["refused"], "allowance_grant_conflict")
        self.assertEqual(len(journal.grant_receipts(self.journal.read())), 1)

    def test_process_death_at_both_transaction_boundaries(self):
        for mode, exit_code in (("before", 91), ("after", 92)):
            process = self.child(mode=mode)
            _, error = process.communicate("go\n", timeout=10)
            self.assertEqual(error, "")
            self.assertEqual(process.returncode, exit_code)
            if mode == "before":
                self.assertEqual(self.journal.read(), self.before)
            else:
                self.assertFalse(self.apply()[1])
                self.assertEqual(len(journal.grant_receipts(self.journal.read())), 1)

    def test_real_legacy_read_start_and_stop_during_verification(self):
        import subprocess

        legacy = Path(__file__).parent / "fixtures"
        script = """
import sys
sys.path[:0] = sys.argv[1:3]
from pilot_pre_grant_journal import Journal
from pilot_pre_grant_request import request
from flowdc_pilot import ClockSample
journal = Journal(sys.argv[3])
request(journal, "start", clock=lambda: ClockSample("fake-boot", 0, 100000))
request(journal, "stop", clock=lambda: ClockSample("fake-boot", 0, 100000))
"""
        with self.journal.supervisor_lock():
            process = self.child()
            result = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-c",
                    script,
                    str(legacy),
                    str(Path(journal.__file__).parent),
                    str(self.journal.root),
                ],
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.finish(process)["refused"], "allowance_state_changed")
        after = self.journal.read()
        self.assertEqual(after["desired"], "stop")
        self.assertEqual(journal.grant_receipts(after), [])
        self.assertEqual(
            [event["kind"] for event in after["events"]][-2:], ["start_requested", "stop_requested"]
        )

    def test_legacy_admission_after_grant_and_later_consumption_preserves_receipt(self):
        import subprocess

        self.apply()
        receipt = journal.grant_receipts(self.journal.read())
        script = """
import sys
sys.path[:0] = sys.argv[1:3]
from dataclasses import asdict
from pilot_pre_grant_journal import Journal, allowance
from pilot_pre_grant_request import request
from flowdc_pilot import ClockSample
journal = Journal(sys.argv[3])
record = journal.read()
assert all(vm["account"]["limit"] >= 4200 for vm in record["vms"].values())
request(journal, "start", clock=lambda: ClockSample("fake-boot", 0, 100000))
def consume(record):
    for vm in record["vms"].values():
        account = allowance(vm["account"]).activation_intent(ClockSample("fake-boot", 0, 100000), window_seconds=1800)
        vm["account"] = asdict(account.account(ClockSample("fake-boot", 10, 100010)))
        vm["phase"] = "requested"
journal.change(consume)
"""
        with self.journal.supervisor_lock():
            result = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-c",
                    script,
                    str(Path(__file__).parent / "fixtures"),
                    str(Path(journal.__file__).parent),
                    str(self.journal.root),
                ],
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
        current = self.journal.read()
        self.assertEqual(journal.grant_receipts(current), receipt)
        for vm_id, vm in current["vms"].items():
            self.assertEqual(vm["account"]["consumed"], self.before["vms"][vm_id]["account"]["consumed"] + 10)


class GrantExperimentTests(unittest.TestCase):
    def test_grant_invalidates_prepared_binding_and_new_preparation_succeeds(self):
        from dataclasses import asdict
        from unittest.mock import patch
        from uuid import uuid4

        import flowdc_experiment_transport as transport
        from flowdc_experiment_data import ExperimentError
        from flowdc_pilot_supervisor import sample_clock
        from test_flowdc_experiment import ExperimentTests
        from test_flowdc_pilot_lifecycle import spec

        selected = spec()
        for vm in selected["vms"]:
            vm["active_seconds"] = 3600
        case = ExperimentTests()
        with patch("test_flowdc_experiment.spec", return_value=selected):
            case.setUp()
        try:
            old_id, manifest, state = case.prepare()

            def settled(record):
                for vm in record["vms"].values():
                    vm["observed"] = {"state": "SHELVED_OFFLOADED", "clock": asdict(sample_clock())}

            case.journal.change(settled)
            before = case.journal.read()
            request_value = {
                "schema_version": 1,
                "grant_id": str(uuid4()),
                "registration_id": before["registration_id"],
                "expected_binding_sha256": journal.allowance_binding_sha256(before),
                "expected_limits_seconds": dict.fromkeys(("manager", "worker", "origin"), 3600),
                "additional_seconds": 600,
            }
            case.journal.extend_allowance(
                request_value, before, sample_clock(), clock=sample_clock, recheck=lambda r: None
            )
            with self.assertRaises(ExperimentError) as raised:
                transport.Controller(case.state, manifest["binding"])
            self.assertEqual(str(raised.exception), "registration_changed")
            new_id, new_manifest, new_state = case.prepare()
            self.assertNotEqual(old_id, new_id)
            self.assertEqual(new_manifest["binding"], transport.binding(case.journal.read()))
            self.assertNotEqual(manifest["binding"], new_manifest["binding"])
            self.assertEqual(case.store.json(old_id, "manifest.json"), manifest)
            self.assertTrue(
                all(vm["active_seconds"] == 4200 for vm in new_manifest["binding"]["spec"]["vms"])
            )
        finally:
            case.doCleanups()


if __name__ == "__main__":
    unittest.main()
