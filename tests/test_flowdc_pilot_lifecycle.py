"""Private SQLite and fake-provider lifecycle regressions; no cloud credentials."""

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
import flowdc_ops as ops
from flowdc_pilot import ClockSample
from flowdc_pilot_journal import Journal, allowance, register
from flowdc_pilot_provider import Provider
from flowdc_pilot_supervisor import Supervisor, request

PROJECT = "11111111-1111-4111-8111-111111111111"
VM_IDS = [f"22222222-2222-4222-8222-{index:012d}" for index in range(1, 4)]
PORT_IDS = [f"33333333-3333-4333-8333-{index:012d}" for index in range(1, 4)]
NETWORK = "44444444-4444-4444-8444-444444444444"
SUBNET = "55555555-5555-4555-8555-555555555555"
ROLES = ("manager", "worker", "origin")


def spec():
    return {
        "schema_version": 1,
        "context": {
            "project_id": PROJECT,
            "region": "test-region",
            "auth_url": "https://cloud.example.test/v3",
        },
        "vms": [
            {"id": vm_id, "role": role, "active_seconds": 7200, "rate": None}
            for vm_id, role in zip(VM_IDS, ROLES, strict=True)
        ],
    }


def access():
    return {
        "schema_version": 1,
        "operator_cidr": "192.0.2.1/32",
        "route": {
            "mode": "private",
            "external_network_id": None,
            "router_id": None,
            "operator_route_verified": True,
        },
        "interfaces": {
            role: {"port_id": port, "network_id": NETWORK, "subnet_id": SUBNET, "fixed_ip": f"10.0.0.{index}"}
            for index, (role, port) in enumerate(zip(ROLES, PORT_IDS, strict=True), 10)
        },
    }


def synthetic_service():
    return {
        "unit": "flowdc-test.service",
        "release": "/tmp/flowdc-fake-release",
        "digest": "0" * 64,
        "interpreter": "/tmp/flowdc-fake-python",
        "interpreter_digest": "0" * 64,
    }


class FakeClock:
    def __init__(self):
        self.seconds = 0
        self.boot = "fake-boot"
        self.wall_offset = 0

    def __call__(self):
        return ClockSample(self.boot, self.seconds, 100000 + self.seconds + self.wall_offset)

    def advance(self, seconds=2):
        self.seconds += seconds


class FakeProvider:
    def __init__(self):
        self.states = dict.fromkeys(VM_IDS, "SHELVED_OFFLOADED")
        self.actions = []
        self.fail_unshelve = None
        self.fail_cleanup = set()
        self.delay_offload = False
        self.after_action = None

    def preflight(self, record):
        if any(value != "SHELVED_OFFLOADED" for value in self.states.values()):
            raise ops.OpsError("initial_offload_required", "fixed", "fixed", 3)

    def network_step(self, journal, *, rollback):
        self.actions.append(("rollback" if rollback else "network", None))
        journal.change(lambda record: record["network"].update(ready=not rollback, rolled_back=rollback))

    def observe(self, record, vm_id):
        if vm_id in self.fail_cleanup:
            raise ops.OpsError("provider_unavailable", "fixed", "fixed", 3)
        return self.states[vm_id]

    def lifecycle(self, record, vm_id, action):
        self.actions.append((action, vm_id))
        if action == "unshelve":
            self.states[vm_id] = "ACTIVE"
            if vm_id == self.fail_unshelve:
                raise ops.OpsError("lost_response", "fixed", "fixed", 3)
        elif not self.delay_offload:
            self.states[vm_id] = "SHELVED_OFFLOADED"
        if self.after_action:
            self.after_action(action)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.mask = os.umask(0o077)
        self.addCleanup(os.umask, self.mask)
        self.temp = tempfile.TemporaryDirectory(prefix="flowdc-pilot-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = self.root / "config"
        self.config.mkdir(mode=0o700)
        self.profile = self.config / "profile.json"
        self.profile.write_text("{}")
        self.state = self.root / "state"
        self.journal = register(self.profile, self.state, spec(), access())
        self.clock = FakeClock()
        self.provider = FakeProvider()
        self.supervisor = Supervisor(self.journal, self.provider, clock=self.clock)
        self.lock = self.journal.supervisor_lock()
        self.lock.__enter__()
        self.addCleanup(self.lock.__exit__, None, None, None)
        self.journal.change(lambda record: record.update(service=synthetic_service()))
        self.supervisor.recover()

    def start(self):
        request(self.journal, "start", clock=self.clock)

    def tick(self, count=1):
        for _ in range(count):
            self.supervisor.tick()
            self.clock.advance()

    def drain(self):
        for _ in range(100):
            self.tick()
            if self.journal.read()["desired"] == "idle":
                return
        self.fail("cleanup never became idle")

    def test_busy_inside_tick_retries_without_checkpoint_or_replayed_unshelve(self):
        self.start()
        change = self.journal.change
        calls = 0

        def busy_intent(update):
            nonlocal calls
            calls += 1
            if calls == 2:  # account succeeds; activation intent contends
                raise ops.OpsError("pilot_state_busy", "fixed", "fixed", 3)
            return change(update)

        with patch.object(self.journal, "change", side_effect=busy_intent):
            with self.assertRaises(ops.OpsError):
                self.supervisor.tick()
        self.assertEqual(self.journal.read()["desired"], "run")
        self.assertIsNone(self.journal.read()["checkpoint"])
        self.tick(2)  # activation obligations and network setup
        calls = 0

        def busy_result(update):
            nonlocal calls
            calls += 1
            if calls == 4:  # account, account, unshelve intent, result
                raise ops.OpsError("pilot_state_busy", "fixed", "fixed", 3)
            return change(update)

        with patch.object(self.journal, "change", side_effect=busy_result):
            with self.assertRaises(ops.OpsError):
                self.supervisor.tick()
        self.assertEqual(self.journal.read()["vms"][VM_IDS[0]]["phase"], "unshelve_intent")
        self.tick(6)
        self.assertEqual(self.provider.actions.count(("unshelve", VM_IDS[0])), 1)
        self.clock.advance(1200)  # deadline wins even after transient contention
        calls = 0
        with patch.object(self.journal, "change", side_effect=busy_intent):
            with self.assertRaises(ops.OpsError):
                self.supervisor.tick()
        self.assertEqual(self.journal.read()["desired"], "stop")
        self.assertIsNone(self.journal.read()["checkpoint"])
        self.drain()
        self.assertTrue(all(not vm["account"]["obligation"] for vm in self.journal.read()["vms"].values()))

    def test_resolved_checkpoint_history_and_expiring_idle_observations(self):
        import flowdc_pilot_cli as cli

        self.start()
        self.tick(5)
        self.supervisor.checkpoint("synthetic_failure")
        self.drain()
        record = self.journal.read()
        self.assertIsNone(record["checkpoint"])
        self.assertTrue(any(event["kind"] == "checkpoint" for event in record["events"]))
        with patch.object(cli, "sample_clock", side_effect=self.clock):
            self.assertEqual(cli.status(self.journal)[1], 0)
            self.clock.advance(121)
            self.tick()  # fresh heartbeat, deliberately stale cloud observations
            value, code = cli.status(self.journal)
            self.assertEqual(code, 3)
            self.assertTrue(all(vm["provider_state"] == "UNKNOWN" for vm in value["data"]["vms"]))
            request(self.journal, "reconcile", clock=self.clock)
            self.drain()
            self.assertEqual(cli.status(self.journal)[1], 0)

    def test_saved_event_shapes_and_legacy_counts(self):
        import json

        record = self.journal.read()
        for event in (
            None,
            {},
            {"kind": 3, "data": {}},
            {"kind": "checkpoint", "data": []},
            {"kind": "checkpoint", "data": {}, "count": True},
            {"kind": "checkpoint", "data": {}, "count": 0},
        ):
            record["events"] = [event]
            with self.journal.connection() as connection:
                connection.execute("UPDATE pilot SET body=?", (json.dumps(record),))
                connection.commit()
            with self.assertRaises(ops.OpsError) as caught:
                self.journal.read()
            self.assertEqual(caught.exception.code, "invalid_journal_history")
        record["events"] = [{"kind": "checkpoint", "data": {"code": "old"}}]
        with self.journal.connection() as connection:
            connection.execute("UPDATE pilot SET body=?", (json.dumps(record),))
            connection.commit()
        self.journal.change(lambda r: self.journal.event(r, "checkpoint", {"code": "old"}))
        self.assertEqual(self.journal.read()["events"][0]["count"], 2)

    def test_saved_network_shapes_fail_closed(self):
        import copy
        import json

        original = self.journal.read()
        marker = "flowdc-" + original["network"]["generation"] + "-manager"
        valid = {"action": "group_create", "args": [marker]}
        for bad in (
            {"seen_groups": {"stranger": PORT_IDS[0]}},
            {"seen_groups": {"manager": "bad-uuid"}},
            {"intents": {"group-manager": []}},
            {"intents": {"group-manager": dict(valid, not_sent=1)}},
            {"intents": {"group-manager": dict(valid, error="raw diagnostic text")}},
            {"intents": {"group-manager": dict(valid, args="bad")}},
            {"intents": {"group-manager": dict(valid, action="offload")}},
        ):
            record = copy.deepcopy(original)
            record["network"].update(bad)
            with self.journal.connection() as connection:
                connection.execute("UPDATE pilot SET body=?", (json.dumps(record),))
                connection.commit()
            with self.assertRaises(ops.OpsError) as caught:
                self.journal.read()
            self.assertEqual(caught.exception.code, "invalid_journal_history")
        original["network"]["intents"] = {"group-manager": valid}
        with self.journal.connection() as connection:
            connection.execute("UPDATE pilot SET body=?", (json.dumps(original),))
            connection.commit()
        self.assertEqual(self.journal.read()["network"]["intents"]["group-manager"], valid)

    def test_idle_stop_does_not_observe_but_reconcile_does(self):
        with patch.object(
            self.provider, "observe", side_effect=ops.OpsError("provider_unavailable", "fixed", "fixed", 3)
        ) as observe:
            request(self.journal, "stop", clock=self.clock)
            self.tick()
            self.assertEqual(self.journal.read()["desired"], "idle")
            observe.assert_not_called()
            request(self.journal, "reconcile", clock=self.clock)
            self.tick()
            observe.assert_called_once()
            self.assertEqual(self.journal.read()["desired"], "stop")

    def test_idle_stop_requires_settled_phases_and_network(self):
        self.journal.change(lambda r: r["vms"][VM_IDS[0]].update(phase="verify_offload"))
        request(self.journal, "stop", clock=self.clock)
        self.assertEqual(self.journal.read()["desired"], "stop")
        self.drain()
        self.journal.change(lambda r: r["network"].update(rolled_back=False))
        request(self.journal, "stop", clock=self.clock)
        self.assertEqual(self.journal.read()["desired"], "stop")
        self.drain()
        self.assertIn(("rollback", None), self.provider.actions)

    def test_slow_refresh_after_rollback_finishes_with_fresh_observations(self):
        request(self.journal, "reconcile", clock=self.clock)
        self.journal.change(lambda r: r["network"].update(rolled_back=False))
        observe = self.provider.observe
        rollback = self.provider.network_step

        def slow_observe(record, vm_id):
            self.clock.advance(20)
            return observe(record, vm_id)

        def slow_rollback(journal, *, rollback):
            self.clock.advance(200)
            return original_rollback(journal, rollback=rollback)

        original_rollback = rollback
        with (
            patch.object(self.provider, "observe", side_effect=slow_observe),
            patch.object(self.provider, "network_step", side_effect=slow_rollback),
        ):
            self.drain()
        record = self.journal.read()
        self.assertTrue(record["network"]["rolled_back"])
        self.assertTrue(
            all(
                self.clock().boottime - vm["observed"]["clock"]["boottime"] < 120
                for vm in record["vms"].values()
            )
        )

    def test_optional_network_history_validation(self):
        for key, value in (
            ("configured", "manager"),
            ("configured", ["stranger"]),
            ("configured", ["manager", "manager"]),
            ("route_checked", 1),
        ):
            with self.subTest(key=key, value=value), self.assertRaises(ops.OpsError):
                self.journal.change(lambda r, key=key, value=value: r["network"].update({key: value}))
        self.journal.change(lambda r: r["network"].update(configured=["manager"], route_checked=True))

    def test_registration_closes_connection_on_success_and_failure(self):
        original_connect = sqlite3.connect
        for fail in (False, True):
            connections = []

            class TrackedConnection(sqlite3.Connection):
                def execute(self, sql, *args, fail=fail):
                    if fail and sql.startswith("INSERT"):
                        raise sqlite3.OperationalError("synthetic")
                    return super().execute(sql, *args)

            def connect(*args, connections=connections, factory=TrackedConnection, **kwargs):
                connection = original_connect(*args, **kwargs, factory=factory)
                connections.append(connection)
                return connection

            config = self.root / ("failed-config" if fail else "ok-config")
            config.mkdir(mode=0o700)
            profile = config / "profile.json"
            profile.write_text("{}")
            state = config / "state"
            with patch("flowdc_pilot_journal.sqlite3.connect", side_effect=connect):
                if fail:
                    with self.assertRaises(ops.OpsError):
                        register(profile, state, spec(), access())
                else:
                    register(profile, state, spec(), access())
            self.assertEqual(len(connections), 1)
            with self.assertRaises(sqlite3.ProgrammingError):
                connections[0].execute("SELECT 1")
            self.assertTrue((state / "pilot.sqlite3").exists())

    def test_delayed_activation_retains_obligation_across_restart(self):
        for lost_reply, restart in ((False, False), (True, False), (False, True), (True, True)):
            with self.subTest(lost_reply=lost_reply, restart=restart):
                original = self.provider.lifecycle

                def delayed(record, vm_id, action, lost_reply=lost_reply, original=original):
                    if action == "unshelve":
                        self.provider.actions.append((action, vm_id))
                        if lost_reply:
                            raise ops.OpsError("lost_response", "fixed", "fixed", 3)
                    else:
                        original(record, vm_id, action)

                self.provider.lifecycle = delayed
                self.start()
                self.tick(3)
                selected = [vm for action, vm in self.provider.actions if action == "unshelve"][-1]
                request(self.journal, "stop", clock=self.clock)
                if restart:
                    self.supervisor.recover()
                self.tick(30)
                record = self.journal.read()
                self.assertEqual(record["desired"], "stop")
                self.assertTrue(allowance(record["vms"][selected]["account"]).obligation)
                self.assertEqual(record["checkpoint"], "activation_completion_unresolved")
                self.provider.states[selected] = "ACTIVE"
                self.drain()
                self.assertEqual(self.provider.states[selected], "SHELVED_OFFLOADED")
                self.provider.lifecycle = original

    def test_stalled_connection_returns_bounded_busy_checkpoint(self):
        code = """import sys
sys.path.insert(0, sys.argv[1])
import flowdc_ops as ops
from flowdc_pilot_journal import Journal
from flowdc_pilot_supervisor import request
try:
    journal = Journal(sys.argv[2])
    if sys.argv[3] == "read":
        journal.read()
    else:
        request(journal, "stop")
except ops.OpsError as exc:
    print(exc.code, flush=True)
    sys.exit(exc.exit_code)
sys.exit(0)
"""
        for operation in ("read", "stop"):
            with self.subTest(operation=operation), self.journal.connection():
                child = subprocess.Popen(
                    [sys.executable, "-c", code, str(Path(ops.__file__).parent), str(self.state), operation],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                try:
                    stdout, stderr = child.communicate(timeout=4)
                finally:
                    if child.poll() is None:
                        child.kill()
                        child.communicate(timeout=2)
                self.assertEqual(child.returncode, 3, stderr)
                self.assertEqual(stdout.strip(), "pilot_state_busy")
        # Contention neither removes the lock nor damages the durable record.
        self.assertEqual(self.journal.read()["desired"], "idle")

    def test_sqlite_activity_serializes_auxiliary_inspection(self):
        # Exercise a real concurrent writer: no SQLite auxiliary file lifecycle
        # can run while another Journal connection is inspecting/using the DB.
        code = """import sys
sys.path.insert(0, sys.argv[1])
from flowdc_pilot_journal import Journal
print("ready", flush=True)
Journal(sys.argv[2]).change(lambda record: record.update(checkpoint="concurrent_test"))
"""
        with self.journal.connection():
            child = subprocess.Popen(
                [sys.executable, "-c", code, str(Path(ops.__file__).parent), str(self.state)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(child.stdout.readline().strip(), "ready")
            try:
                with self.assertRaises(subprocess.TimeoutExpired):
                    child.wait(timeout=0.2)
            finally:
                self.addCleanup(child.communicate, timeout=10)
        stdout, stderr = child.communicate(timeout=10)
        self.assertEqual(child.returncode, 0, (stdout, stderr))
        self.assertEqual(self.journal.read()["checkpoint"], "concurrent_test")

    def test_service_schema_and_cleanup_intent_are_validated(self):
        with self.assertRaises(ops.OpsError):
            self.journal.change(lambda record: record.update(service={"unit": "fake-only"}))
        for intent in ({"action": "delete", "clock": {}}, {"action": "shelve", "clock": {}}):
            with self.assertRaises(ops.OpsError):
                self.journal.change(
                    lambda record, intent=intent: record["vms"][VM_IDS[0]].update(cleanup_intent=intent)
                )

    def test_prepare_is_nonactivating_and_repeat_cannot_reset_account(self):
        self.start()
        self.tick(5)
        request(self.journal, "stop", clock=self.clock)
        self.drain()
        consumed = {key: value["account"]["consumed"] for key, value in self.journal.read()["vms"].items()}
        self.assertTrue(all(value > 0 for value in consumed.values()))
        restored = register(self.profile, self.state, spec(), access()).read()
        self.assertEqual(
            consumed, {key: value["account"]["consumed"] for key, value in restored["vms"].items()}
        )
        with self.assertRaises(ops.OpsError):
            register(self.profile, self.root / "another-run", spec(), access())
        changed = spec()
        changed["vms"][0]["id"] = "22222222-2222-4222-8222-999999999999"
        with self.assertRaises(ops.OpsError):
            register(self.profile, self.state, changed, access())

    def test_duplicate_commands_and_supervisor_lock(self):
        self.start()
        self.tick(3)
        before = self.journal.read()["vms"]
        self.start()
        self.assertEqual(before, self.journal.read()["vms"])
        with self.assertRaises(ops.OpsError), self.journal.supervisor_lock():
            pass
        self.assertEqual(sum(action == "unshelve" for action, _ in self.provider.actions), 1)

    def test_stop_committed_during_external_action_is_not_overwritten(self):
        self.start()
        self.provider.after_action = lambda action: request(self.journal, "stop", clock=self.clock)
        self.tick(3)
        self.assertEqual(self.journal.read()["desired"], "stop")
        self.drain()
        self.assertEqual(sum(action == "unshelve" for action, _ in self.provider.actions), 1)

    def test_partial_activation_and_lost_response_are_reconciled_without_replay(self):
        self.provider.fail_unshelve = VM_IDS[1]
        self.start()
        self.tick(4)
        self.assertEqual(self.journal.read()["desired"], "stop")
        self.drain()
        self.assertEqual([vm for action, vm in self.provider.actions if action == "unshelve"], VM_IDS[:2])
        self.assertTrue(all(state == "SHELVED_OFFLOADED" for state in self.provider.states.values()))
        self.assertTrue(
            all(not allowance(vm["account"]).obligation for vm in self.journal.read()["vms"].values())
        )

    def test_deadline_preempts_pending_activation(self):
        self.start()
        self.tick(2)
        self.clock.advance(1200)
        self.tick()
        self.assertFalse(any(action == "unshelve" for action, _ in self.provider.actions))
        self.drain()

    def test_restart_with_active_obligations_forces_cleanup(self):
        self.start()
        self.tick(5)
        restored = Supervisor(Journal(self.state), self.provider, clock=self.clock)
        restored.recover()
        self.assertEqual(self.journal.read()["desired"], "stop")
        self.supervisor = restored
        self.drain()
        self.assertEqual(sum(action == "unshelve" for action, _ in self.provider.actions), 3)

    def test_host_restart_and_clock_jump_exhaust_allowance(self):
        self.start()
        self.tick(3)
        self.clock.boot = "new-boot"
        self.supervisor.recover()
        self.drain()
        self.assertTrue(
            all(allowance(vm["account"]).remaining == 0 for vm in self.journal.read()["vms"].values())
        )
        with self.assertRaises(ops.OpsError):
            self.start()

    def test_failed_vm_does_not_starve_other_cleanup_and_ack_is_not_success(self):
        self.start()
        self.tick(5)
        self.provider.fail_cleanup.add(VM_IDS[0])
        self.provider.delay_offload = True
        request(self.journal, "stop", clock=self.clock)
        self.tick(20)
        self.assertEqual(self.journal.read()["desired"], "stop")
        self.assertTrue(
            all(allowance(vm["account"]).obligation for vm in self.journal.read()["vms"].values())
        )
        attempted = {vm for action, vm in self.provider.actions if action == "shelve"}
        self.assertEqual(attempted, set(VM_IDS[1:]))
        self.provider.delay_offload = False
        self.tick(20)
        self.assertTrue(allowance(self.journal.read()["vms"][VM_IDS[0]]["account"]).obligation)
        self.assertTrue(
            all(not allowance(self.journal.read()["vms"][key]["account"]).obligation for key in VM_IDS[1:])
        )

    def test_backward_wall_clock_does_not_postpone_cleanup_retries(self):
        self.start()
        self.tick(5)
        self.provider.fail_cleanup.update(VM_IDS)
        request(self.journal, "stop", clock=self.clock)
        self.tick(3)
        self.clock.wall_offset = -3600
        self.provider.fail_cleanup.clear()
        self.tick(30)
        self.assertEqual(self.journal.read()["desired"], "idle")
        self.assertTrue(all(state == "SHELVED_OFFLOADED" for state in self.provider.states.values()))

    def test_reconcile_idle_refreshes_and_cleans_unexpected_activity(self):
        self.provider.states[VM_IDS[0]] = "ACTIVE"
        request(self.journal, "reconcile", clock=self.clock)
        self.drain()
        record = self.journal.read()
        self.assertGreaterEqual(record["vms"][VM_IDS[0]]["account"]["consumed"], 7200)
        self.assertTrue(record["vms"][VM_IDS[0]]["account"]["uncertain"])
        self.assertTrue(all(vm["observed"]["state"] == "SHELVED_OFFLOADED" for vm in record["vms"].values()))

    def test_unready_start_and_exhausted_allowance_refused(self):
        self.clock.advance(31)
        with self.assertRaises(ops.OpsError):
            self.start()
        self.supervisor.recover()
        self.journal.change(lambda record: record["vms"][VM_IDS[0]]["account"].update(consumed=6600))
        with self.assertRaises(ops.OpsError):
            self.start()

    def test_missing_database_cannot_be_recreated_via_prepare(self):
        (self.state / "pilot.sqlite3").unlink()
        with self.assertRaises(ops.OpsError):
            register(self.profile, self.state, spec(), access())
        self.assertFalse((self.state / "pilot.sqlite3").exists())

    def test_private_sqlite_paths_reject_symlinks_hardlinks_and_permissions(self):
        for suffix in ("-journal", "-wal", "-shm"):
            path = self.state / ("pilot.sqlite3" + suffix)
            path.symlink_to(self.profile)
            with self.assertRaises(ops.OpsError):
                self.journal.read()
            path.unlink()
        db = self.state / "pilot.sqlite3"
        os.link(db, self.state / "hardlink")
        with self.assertRaises(ops.OpsError):
            self.journal.read()
        (self.state / "hardlink").unlink()
        db.chmod(0o644)
        with self.assertRaises(ops.OpsError):
            self.journal.read()
        db.chmod(0o600)
        self.assertEqual(self.journal.read()["schema_version"], 1)

    def test_corrupt_sqlite_is_a_fixed_checkpoint_without_raw_diagnostics(self):
        (self.state / "pilot.sqlite3").write_bytes(b"secret-invalid-db")
        with self.assertRaises(ops.OpsError) as raised:
            self.journal.read()
        self.assertNotIn("secret", str(raised.exception))

    def test_concurrent_stop_requests_are_serialized_without_lost_events(self):
        code = "import sys; sys.path.insert(0,sys.argv[1]); from flowdc_pilot_journal import Journal; from flowdc_pilot_supervisor import request; request(Journal(sys.argv[2]),'stop')"
        args = [sys.executable, "-c", code, str(Path(__file__).resolve().parents[1] / "bin"), str(self.state)]
        children = [subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(4)]
        for child in children:
            stdout, stderr = child.communicate(timeout=10)
            self.assertEqual(child.returncode, 0, (stdout, stderr))
        events = self.journal.read()["events"]
        self.assertEqual(
            sum(event.get("count", 1) for event in events if event["kind"] == "stop_requested"), 4
        )

    def test_journal_refuses_binding_changes_and_consumption_decreases(self):
        self.journal.change(lambda record: record["vms"][VM_IDS[0]]["account"].update(consumed=20))
        with self.assertRaises(ops.OpsError):
            self.journal.change(lambda record: record["vms"][VM_IDS[0]]["account"].update(consumed=0))
        with self.assertRaises(ops.OpsError):
            self.journal.change(lambda record: record["spec"]["context"].update(region="other-region"))
        self.assertEqual(self.journal.read()["vms"][VM_IDS[0]]["account"]["consumed"], 20)

    def test_missing_activation_obligation_is_rejected_as_corrupt_history(self):
        with self.assertRaises(ops.OpsError):
            self.journal.change(lambda record: record["vms"][VM_IDS[0]].update(phase="requested"))
        self.assertEqual(self.journal.read()["vms"][VM_IDS[0]]["phase"], "offloaded")

    def test_concurrent_duplicate_starts_create_one_request(self):
        code = "import sys; sys.path.insert(0,sys.argv[1]); sys.path.insert(0,sys.argv[2]); from test_flowdc_pilot_lifecycle import FakeClock; from flowdc_pilot_journal import Journal; from flowdc_pilot_supervisor import request; request(Journal(sys.argv[3]),'start',clock=FakeClock())"
        args = [
            sys.executable,
            "-c",
            code,
            str(Path(__file__).resolve().parents[1] / "bin"),
            str(Path(__file__).resolve().parent),
            str(self.state),
        ]
        children = [subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(4)]
        for child in children:
            stdout, stderr = child.communicate(timeout=10)
            self.assertEqual(child.returncode, 0, (stdout, stderr))
        self.assertEqual(
            sum(event["kind"] == "start_requested" for event in self.journal.read()["events"]), 1
        )

    def test_provider_calls_have_no_open_journal_transaction(self):
        original = self.provider.preflight

        def inspect(record):
            # A second connection can take an immediate write lock during calls.
            with self.journal.connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.rollback()
            original(record)

        self.provider.preflight = inspect
        self.start()
        self.tick()
        self.assertTrue(
            all(allowance(vm["account"]).obligation for vm in self.journal.read()["vms"].values())
        )


class ProviderBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.record = {
            "spec": spec(),
            "vms": {vm_id: {"role": role} for vm_id, role in zip(VM_IDS, ROLES, strict=True)},
        }
        self.profile = {
            "expected_project_id": PROJECT,
            "region": "test-region",
            "auth_url": "https://cloud.example.test/v3",
            "intended_server_ids": VM_IDS,
        }
        self.provider = Provider(self.profile)

    def test_wrong_context_stops_before_mutation(self):
        for context, project in (
            ({"region_name": "wrong", "auth_url": self.profile["auth_url"]}, PROJECT),
            ({"region_name": "test-region", "auth_url": self.profile["auth_url"]}, VM_IDS[0]),
        ):
            with (
                patch.object(ops, "cloud_query", side_effect=[context, {"project_id": project}]),
                patch.object(self.provider, "call") as call,
            ):
                with self.assertRaises(ops.OpsError):
                    self.provider.lifecycle(self.record, VM_IDS[0], "unshelve")
                call.assert_not_called()

    def test_nonallowlisted_or_mismatched_server_is_refused(self):
        with patch.object(ops, "cloud_query") as query:
            with self.assertRaises(ops.OpsError):
                self.provider.server(self.record, PROJECT)
            query.assert_not_called()
        with patch.object(
            ops, "cloud_query", return_value={"id": VM_IDS[1], "project_id": PROJECT, "status": "ACTIVE"}
        ):
            with self.assertRaises(ops.OpsError):
                self.provider.server(self.record, VM_IDS[0])

    def test_lifecycle_has_no_arbitrary_dispatch(self):
        with patch.object(self.provider, "call") as call:
            with self.assertRaises(ops.OpsError):
                self.provider.lifecycle(self.record, VM_IDS[0], "delete")
            call.assert_not_called()


if __name__ == "__main__":
    unittest.main()
