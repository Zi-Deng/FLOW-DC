"""Bounded diagnostics are evidence only, never accounting or cleanup authority."""

import copy
import importlib.util
import json
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
import flowdc_ops as ops
import flowdc_pilot_cli as cli
import test_flowdc_pilot_allowance as grants
import test_flowdc_pilot_lifecycle as lifecycle
from flowdc_pilot_journal import validate_record
from flowdc_pilot_provider import Provider
from flowdc_pilot_supervisor import Supervisor, cleanup_diagnostics, save_diagnostic


class DiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.case = lifecycle.LifecycleTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.journal = self.case.journal
        self.supervisor = self.case.supervisor

    def test_failures_coalesce_and_ring_preserves_all_other_history(self):
        self.journal.change(
            lambda r: r["events"].extend(
                [
                    {"kind": "activation_intent", "data": {"synthetic": True}},
                    {"kind": "network_history", "data": {"synthetic": True}},
                ]
            )
        )
        before = self.journal.read()
        detail = {
            "phase": "cleanup",
            "action": "offload",
            "role": "worker",
            "category": "conflict",
            "dispatch_possible": True,
            "elapsed_seconds": 7.25,
            "remaining_seconds": 12.75,
        }
        for _ in range(100):
            self.supervisor.checkpoint("provider_request_failed", detail)
            self.case.clock.advance()
        entries = cleanup_diagnostics(self.journal.read())
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["count"], 100)
        self.assertLess(entries[0]["first_utc"], entries[0]["last_utc"])
        for action in sorted(ops.DIAGNOSTIC_ACTIONS):
            for role in ("manager", "worker", "origin"):
                self.supervisor.checkpoint("provider_request_failed", dict(detail, action=action, role=role))
        after = self.journal.read()
        self.assertEqual(len(cleanup_diagnostics(after)), 32)
        self.assertEqual([e for e in after["events"] if e["kind"] != "cleanup_diagnostics"], before["events"])
        for key in ("vms", "network", "registration_id", "spec"):
            self.assertEqual(after[key], before[key])
        self.assertLess(len(json.dumps(cleanup_diagnostics(after))), 12000)
        self.assertEqual(after["desired"], "stop")

    def test_hostile_values_become_explicit_unknowns_and_status_is_bounded(self):
        detail = {
            key: "secret-canary https://private.example/?credential=value" for key in ops.safe_diagnostic({})
        }
        detail.update(
            elapsed_seconds=float("nan"),
            remaining_seconds=float("inf"),
            stderr="secret-canary",
            environment={"SECRET": "secret-canary"},
        )
        self.supervisor.checkpoint("provider_request_failed", detail)
        entries = cleanup_diagnostics(self.journal.read())
        self.assertEqual(entries[0]["action"], "unknown")
        self.assertIsNone(entries[0]["dispatch_possible"])
        self.assertIsNone(entries[0]["elapsed_seconds"])
        value, _ = cli.status(self.journal)
        raw = json.dumps(value)
        self.assertNotIn("secret-canary", raw)
        self.assertNotIn("private.example", raw)
        self.assertEqual(value["data"]["cleanup_diagnostics"], entries)
        self.assertIsNone(ops.safe_diagnostic({"elapsed_seconds": 10**400})["elapsed_seconds"])

    def test_journal_rejects_malformed_diagnostics_and_accepts_legacy_events(self):
        original = self.journal.read()
        validate_record(copy.deepcopy(original))
        for invalid in (
            [{}],
            [dict(ops.safe_diagnostic({}), count=0, first_utc=1, last_utc=1)],
            [dict(ops.safe_diagnostic({}), count=1, first_utc=10**400, last_utc=1)],
            [dict(ops.safe_diagnostic({}), count=1, first_utc=1, last_utc=1)] * 33,
        ):
            value = copy.deepcopy(original)
            value["events"].append({"kind": "cleanup_diagnostics", "data": {"entries": invalid}})
            with self.subTest(invalid=invalid), self.assertRaises(ops.OpsError):
                validate_record(value)
        # Run the retained legacy validator, not a model of its field checks.
        value = copy.deepcopy(original)
        save_diagnostic(value, {"phase": "cleanup", "action": "offload"}, 1)
        path = Path(__file__).parent / "fixtures/pilot_pre_grant_journal.py"
        selected = importlib.util.spec_from_file_location("legacy_journal", path)
        legacy = importlib.util.module_from_spec(selected)
        selected.loader.exec_module(legacy)
        legacy.validate_record(value)
        self.assertEqual(value["schema_version"], original["schema_version"])

    def test_receipts_binding_and_consumed_seconds_survive_diagnostic_rotation(self):
        case = grants.GrantTransitionTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.apply()
        before = case.journal.read()
        supervisor = Supervisor(case.journal, lifecycle.FakeProvider(), clock=case.clock)
        for action in sorted(ops.DIAGNOSTIC_ACTIONS):
            for role in lifecycle.ROLES:
                supervisor.checkpoint(
                    "provider_request_failed", {"phase": "cleanup", "action": action, "role": role}
                )
        after = case.journal.read()
        self.assertEqual(grants.journal.grant_receipts(after), grants.journal.grant_receipts(before))
        self.assertEqual(
            grants.journal.allowance_binding_sha256(after), grants.journal.allowance_binding_sha256(before)
        )
        self.assertEqual(after["vms"], before["vms"])
        self.assertEqual([e for e in after["events"] if e["kind"] != "cleanup_diagnostics"], before["events"])

    def test_only_explicit_http_markers_classify_cli_failures(self):
        for message, expected in (
            ("HTTP 409 secret-canary", b"conflict"),
            ("HTTP/1.1 503 secret-canary", b"transient"),
            ("quota exceeded secret-canary", b"quota"),
            ("forbidden secret-canary", b"permission"),
            ("resource 409 or conflict secret-canary", b"provider"),
        ):
            with self.subTest(message=message):
                code, category = ops.run_bounded(
                    [
                        sys.executable,
                        "-I",
                        "-B",
                        "-c",
                        "import sys; print(sys.argv[1], file=sys.stderr); sys.exit(1)",
                        message,
                    ],
                    timeout=2,
                    classify_errors=True,
                )
                self.assertEqual(code, 1)
                self.assertEqual(category, expected)

    def test_diagnostic_persistence_failure_cannot_undo_stop_or_accounting(self):
        self.case.start()
        self.case.tick(4)
        before = copy.deepcopy(self.journal.read()["vms"])
        self.case.clock.advance(5)
        self.supervisor.account()
        accounted = copy.deepcopy(self.journal.read()["vms"])
        change = self.journal.change
        count = 0

        def fail_second(update):
            nonlocal count
            count += 1
            if count == 2:
                raise ops.OpsError("pilot_state_busy", "fixed", "fixed", 3)
            return change(update)

        with patch.object(self.journal, "change", side_effect=fail_second):
            self.supervisor.checkpoint("provider_request_failed", {"phase": "cleanup"})
        record = self.journal.read()
        self.assertEqual(record["desired"], "stop")
        self.assertEqual(record["vms"], accounted)
        self.assertTrue(
            any(accounted[key]["account"]["consumed"] > before[key]["account"]["consumed"] for key in before)
        )

    def test_worker_error_keeps_its_action_after_all_other_workers_join(self):
        provider = Provider({})
        barrier = threading.Barrier(4, timeout=2)
        finished = []

        def probe(action, *args, on_dispatch=None, **kwargs):
            barrier.wait()
            if on_dispatch:
                on_dispatch()
            finished.append(action)
            if action == "port":
                raise ops.OpsError("provider_request_failed", "secret-canary", "secret-canary", 3)
            return {}

        with provider.step("network_rollback"), patch.object(provider, "_call", side_effect=probe):
            with self.assertRaises(ops.OpsError) as caught:
                provider.read_batch(
                    [(action, lifecycle.PROJECT) for action in ("port", "group", "network", "subnet")]
                )
        self.assertCountEqual(finished, ["port", "group", "network", "subnet"])
        self.assertEqual(caught.exception.diagnostic["action"], "port")
        self.assertEqual(caught.exception.diagnostic["phase"], "network_rollback")
        self.assertNotIn("secret-canary", json.dumps(caught.exception.diagnostic))

    def test_unknown_cleanup_failures_rotate_without_unbounded_checkpoint_events(self):
        self.case.start()
        self.case.tick(4)
        lifecycle.request(self.journal, "stop", clock=self.case.clock)
        self.case.provider.fail_cleanup = set(lifecycle.VM_IDS)
        self.case.tick(100)
        entries = cleanup_diagnostics(self.journal.read())
        self.assertEqual({e["role"] for e in entries}, set(lifecycle.ROLES))
        self.assertEqual(len(entries), 3)
        self.assertTrue(all(e["action"] == "server" and e["elapsed_seconds"] is None for e in entries))


if __name__ == "__main__":
    unittest.main()
