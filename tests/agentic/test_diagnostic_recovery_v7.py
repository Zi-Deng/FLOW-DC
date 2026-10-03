"""Revision-7 finite restart; all accounts, approvals and provider calls are synthetic."""

import copy
import json
import unittest
from unittest.mock import patch

import diagnostic_recovery_v6 as stopped
import test_diagnostic_recovery_v6 as prior_tests
from test_workflow import workflow

# isort: split
import claude_native_auth
import diagnostic_recovery as prior
import review_claude
import review_diagnostics as diagnostics
import review_policy
from claude_fixtures import AUTHENTICATION
from tasks import atomic_json

CONTRACT = {
    "issue": 33,
    "plan_comment": 5965161662,
    "issue_digest": "61045bd2bf6b675c9629ca511a19122aee659a6bfc8764b46391ad1d04d92d3d",
    "plan_digest": "2b1b9437bd1d665c930c079cc3e30655bca3c68a1f332b43b66142696b0b6137",
}


class RevisionSevenTests(unittest.TestCase):
    def setUp(self):
        previous = prior_tests.RevisionSixTests(methodName="runTest")
        previous.setUp()
        self.addCleanup(previous.doCleanups)
        self.fixture = f = previous.fixture
        previous.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute),
        ):
            self.assertEqual(f.run_diagnostic()["status"], "qualified")

        def incomplete(*args, **kwargs):
            body, diag, version = f.execute(*args, **kwargs)
            diag["reasons"].append("customization_or_mcp_loaded")
            return body, diag, version

        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=incomplete),
        ):
            self.assertEqual(f.run_diagnostic()["status"], "incomplete")
        self.original = {p: p.read_bytes() for p in f.root_state.rglob("*") if p.is_file()}
        self.record = json.loads(f.task_record.read_text())
        self.record["approval_history"].append(copy.deepcopy(self.record["approval"]))
        self.record["approval"] = {
            **self.record["approval"],
            "contract": CONTRACT,
            "plan_comment": CONTRACT["plan_comment"],
        }
        atomic_json(f.task_record, self.record)
        current = patch.dict(review_policy.PROVIDERS["claude-code"], adapter="claude-stream-json-2.1.282-v5")
        current.start()
        self.addCleanup(current.stop)

    def assert_preserved(self):
        self.assertEqual(self.original, {p: p.read_bytes() for p in self.original})

    def test_original_grant_uses_exact_historical_approval_without_authentication(self):
        f = self.fixture
        with (
            patch.object(claude_native_auth, "store", side_effect=AssertionError("No credential reads")),
            patch.object(review_claude, "execute", side_effect=AssertionError("No inference")),
        ):
            state = stopped.load(f.repo)
        self.assertEqual(
            [x["status"] for x in state["attempts"]],
            ["incomplete", "incomplete", "incomplete", "qualified", "incomplete"],
        )
        self.assert_preserved()

    def test_approved_preview_has_new_purposes_without_mutating_stopped_grant(self):
        preview = prior.prepare(self.fixture.repo, self.fixture.cfg)
        self.assertEqual(preview["max_total_attempts"], 7)
        self.assertEqual(
            preview["slots"],
            [
                {"number": 6, "purpose": "native-tools-and-source"},
                {"number": 7, "purpose": "isolation-refusal"},
            ],
        )
        self.assertEqual(
            preview["contract_digest"], "8c342fdbb9093d5b310bfe10854910dbf1e66e66527ac1a2514b7a90d615589b"
        )
        self.assert_preserved()

    def apply(self):
        f = self.fixture
        preview = prior.prepare(f.repo, f.cfg)
        return prior.prepare(f.repo, f.cfg, apply=True, preview_digest=preview["preview_digest"])

    def policy(self):
        return {
            **review_policy.policy(review_policy.choices("claude-code"), {}),
            "authentication": AUTHENTICATION,
        }

    def test_two_new_purposes_activate_and_eighth_call_is_refused(self):
        f = self.fixture
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute) as execute,
        ):
            self.assertEqual(f.run_diagnostic()["attempt"], 6)
            self.assertEqual(
                json.loads((f.root_state / "attempt-6/metadata.json").read_text())["diagnostic_purpose"],
                "native-tools-and-source",
            )
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(f.repo, self.policy())
            self.assertEqual(f.run_diagnostic()["attempt"], 7)
            self.assertEqual(execute.call_count, 2)
            with self.assertRaisesRegex(workflow.WorkflowError, "eighth"):
                f.run_diagnostic()
            self.assertEqual(execute.call_count, 2)
        diagnostics.require_activation(f.repo, self.policy())
        self.assert_preserved()

    def test_missing_edited_and_ambiguous_history_refuse_frozen_grant(self):
        f = self.fixture
        variants = []
        for change in (
            lambda r: r.pop("approval_history"),
            lambda r: r["approval_history"].append(copy.deepcopy(r["approval_history"][0])),
            lambda r: r["approval_history"][0].update(recorded_at="edited"),
            lambda r: r["approval_history"][0]["contract"].update(plan_digest="0" * 64),
            lambda r: r.update(approval_history="not a list"),
        ):
            value = copy.deepcopy(self.record)
            change(value)
            variants.append(value)
        for value in variants:
            with self.subTest(value=value):
                atomic_json(f.task_record, value)
                with (
                    patch.object(claude_native_auth, "bind") as bind,
                    self.assertRaises(workflow.WorkflowError),
                ):
                    prior.prepare(f.repo, f.cfg)
                bind.assert_not_called()
                with self.assertRaises(workflow.WorkflowError):
                    prior.load(f.repo)
        atomic_json(f.task_record, self.record)
        self.assert_preserved()

    def test_historical_v7_authority_allows_reads_but_not_new_calls_or_activation(self):
        import diagnostic_recovery_v7 as current

        f = self.fixture
        self.apply()
        changed = copy.deepcopy(self.record)
        changed["approval_history"].append(changed["approval"])
        changed["approval"] = {**changed["approval"], "contract": {**CONTRACT, "plan_digest": "0" * 64}}
        atomic_json(f.task_record, changed)
        with patch.object(claude_native_auth, "store", side_effect=AssertionError("No auth")):
            self.assertEqual(len(current.load(f.repo)["attempts"]), 5)
        with (
            patch.object(claude_native_auth, "bind") as bind,
            patch.object(review_claude, "execute") as execute,
        ):
            for action in (
                lambda: prior.prepare(f.repo, f.cfg),
                f.run_diagnostic,
                lambda: diagnostics.require_activation(f.repo, self.policy()),
            ):
                with self.assertRaises(workflow.WorkflowError):
                    action()
            bind.assert_not_called()
            execute.assert_not_called()
        self.assert_preserved()

    def test_current_approval_is_required_even_if_v7_is_in_history(self):
        f = self.fixture
        changed = copy.deepcopy(self.record)
        changed["approval_history"].append(changed["approval"])
        changed["approval"] = copy.deepcopy(self.record["approval_history"][0])
        atomic_json(f.task_record, changed)
        with patch.object(claude_native_auth, "bind") as bind, self.assertRaises(workflow.WorkflowError):
            self.apply()
        bind.assert_not_called()
        self.assert_preserved()

    def test_future_interruption_counts_and_stops_without_mutating_prior_trials(self):
        f = self.fixture
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=KeyboardInterrupt) as execute,
        ):
            with self.assertRaises(KeyboardInterrupt):
                f.run_diagnostic()
            self.assertEqual(diagnostics.ledger(f.repo)["attempts"][5]["status"], "incomplete")
            self.apply()  # Idempotent application cannot reset the counted interruption.
            self.assertEqual(len(diagnostics.ledger(f.repo)["attempts"]), 6)
            with self.assertRaisesRegex(workflow.WorkflowError, "stops"):
                f.run_diagnostic()
            self.assertEqual(execute.call_count, 1)
        self.assert_preserved()

    def test_future_partial_report_stays_exact_and_stops(self):
        f = self.fixture
        self.apply()

        def partial(*args, **kwargs):
            _, diag, version = f.execute(*args, **kwargs)
            return "exact partial\r\n", diag, version

        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=partial) as execute,
        ):
            result = f.run_diagnostic()
            self.assertEqual((result["status"], result["remaining_attempts"]), ("incomplete", 0))
            self.assertEqual((f.root_state / "attempt-6/report.txt").read_bytes(), b"exact partial\r\n")
            with self.assertRaisesRegex(workflow.WorkflowError, "stops"):
                f.run_diagnostic()
            self.assertEqual(execute.call_count, 1)
        self.assert_preserved()

    def test_limit_purpose_counter_policy_and_partial_migration_tampering_fail_closed(self):
        import diagnostic_recovery_v7 as current
        from tasks import digest

        f = self.fixture
        self.apply()
        path = f.root_state / current.FILENAME
        original = json.loads(path.read_text())
        changes = [
            lambda s: s["grant"].update(max_total_attempts=8),
            lambda s: s["grant"].update(max_total_timeout_seconds=2101),
            lambda s: s["grant"].update(max_total_estimated_usd=15),
            lambda s: s["grant"].update(max_prospective_timeout_seconds=601),
            lambda s: s["grant"].update(max_prospective_estimated_usd=5),
            lambda s: s["grant"].update(extra_spend_authorized_usd=1),
            lambda s: s["grant"]["slots"].reverse(),
            lambda s: s["grant"].update(stop_on_future_failure=False),
            lambda s: s["grant"]["slots"][0].update(purpose="isolation-refusal"),
            lambda s: s["grant"]["policy"].update(adapter=prior.REV4_ADAPTER),
            lambda s: s["grant"]["policy"]["budget"].update(estimated_usd=3),
            lambda s: s["attempts"].pop(),
        ]
        for change in changes:
            value = copy.deepcopy(original)
            change(value)
            value["grant_digest"] = digest(value["grant"])
            atomic_json(path, value)
            with (
                patch.object(claude_native_auth, "bind") as bind,
                patch.object(review_claude, "execute") as execute,
                self.assertRaises(workflow.WorkflowError),
            ):
                f.run_diagnostic()
            bind.assert_not_called()
            execute.assert_not_called()
        atomic_json(path, original)
        (f.root_state / "attempt-7").mkdir()
        with self.assertRaisesRegex(workflow.WorkflowError, "partial"):
            diagnostics.ledger(f.repo)
        self.assert_preserved()

    def test_stale_preview_interrupted_application_and_repeat_apply(self):
        import diagnostic_recovery_v7 as current

        f = self.fixture
        preview = prior.prepare(f.repo, f.cfg)
        with self.assertRaisesRegex(workflow.WorkflowError, "preview digest"):
            prior.prepare(f.repo, f.cfg, apply=True, preview_digest="wrong")
        with (
            patch.object(current, "atomic_json", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            self.apply()
        self.assertFalse((f.root_state / current.FILENAME).exists())
        self.assert_preserved()
        prior.prepare(f.repo, f.cfg, apply=True, preview_digest=preview["preview_digest"])
        data = (f.root_state / current.FILENAME).read_bytes()
        self.apply()
        self.assertEqual(data, (f.root_state / current.FILENAME).read_bytes())

    def test_missing_slot_six_evidence_blocks_seven_and_activation(self):
        f = self.fixture
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute),
        ):
            f.run_diagnostic()
        (f.root_state / "attempt-6/report.txt").write_bytes(b"changed")
        with patch.object(review_claude, "execute") as execute:
            with self.assertRaises(workflow.WorkflowError):
                f.run_diagnostic()
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(f.repo, self.policy())
            execute.assert_not_called()
        self.assert_preserved()

    def test_verified_same_account_renewal_preserves_grant_and_observed_generations(self):
        f = self.fixture
        self.apply()
        renewed = {**AUTHENTICATION, "generation_id": "33333333-3333-4333-8333-333333333333"}

        def lineage(observed, selected, timeout):
            return observed == selected or (observed == AUTHENTICATION and selected == renewed)

        with (
            patch.object(claude_native_auth, "current_binding", return_value=renewed),
            patch.object(claude_native_auth, "capability_lineage", side_effect=lineage),
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute),
        ):
            f.run_diagnostic()
            f.run_diagnostic()
            result = diagnostics.require_activation(f.repo, {**self.policy(), "authentication": renewed})
            self.assertEqual(result["observed_authentication"], [renewed, renewed])
            self.assertEqual(diagnostics.ledger(f.repo)["grant"]["policy"]["authentication"], AUTHENTICATION)
        with patch.object(
            claude_native_auth, "store", side_effect=AssertionError("Frozen reads never read credentials")
        ):
            self.assertEqual(len(diagnostics.ledger(f.repo)["attempts"]), 7)
        self.assert_preserved()

    def test_fresh_preflight_lineage_and_lock_failures_do_not_consume_calls(self):
        import fcntl

        f = self.fixture
        self.apply()
        with (
            patch.object(
                review_claude, "preflight", side_effect=workflow.WorkflowError("Missing prerequisite")
            ),
            patch.object(review_claude, "execute") as execute,
        ):
            with self.assertRaises(workflow.WorkflowError):
                f.run_diagnostic()
            execute.assert_not_called()
        self.assertEqual(len(diagnostics.ledger(f.repo)["attempts"]), 5)
        changed = {**AUTHENTICATION, "generation_id": "33333333-3333-4333-8333-333333333333"}
        with (
            patch.object(claude_native_auth, "current_binding", return_value=changed),
            patch.object(claude_native_auth, "capability_lineage", return_value=False),
            patch.object(review_claude, "execute") as execute,
        ):
            with self.assertRaises(workflow.WorkflowError):
                f.run_diagnostic()
            execute.assert_not_called()
        with (f.root_state / "ledger.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(workflow.WorkflowError, "already in progress"):
                self.apply()
        self.assert_preserved()

    def test_wrong_new_purpose_and_current_approval_changes_refuse_execution(self):
        import diagnostic_recovery_v7 as current

        f = self.fixture
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute),
        ):
            f.run_diagnostic()
        path = f.root_state / current.FILENAME
        original = json.loads(path.read_text())
        changed = copy.deepcopy(original)
        changed["attempts"][5]["purpose"] = "isolation-refusal"
        atomic_json(path, changed)
        with patch.object(review_claude, "execute") as execute, self.assertRaises(workflow.WorkflowError):
            f.run_diagnostic()
        execute.assert_not_called()
        atomic_json(path, original)
        record = copy.deepcopy(self.record)
        record["approval"]["source"] = "edited"
        atomic_json(f.task_record, record)
        with patch.object(claude_native_auth, "bind") as bind, self.assertRaises(workflow.WorkflowError):
            f.run_diagnostic()
        bind.assert_not_called()
        self.assert_preserved()

    def test_stopped_old_grants_never_reach_authentication_for_current_v5(self):
        f = self.fixture
        with (
            patch.object(claude_native_auth, "bind") as bind,
            patch.object(review_claude, "execute") as execute,
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "revision-7 grant"):
                f.run_diagnostic()
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(f.repo, self.policy())
            bind.assert_not_called()
            execute.assert_not_called()
        self.assert_preserved()

    def test_historical_bounds_and_partial_new_ledger_refuse_before_authentication(self):
        import diagnostic_recovery_v7 as current

        f = self.fixture
        for field, limit in (("MAX_HISTORY_FILES", 1), ("MAX_HISTORY_BYTES", 1), ("MAX_LEDGER_BYTES", 1)):
            with patch.object(current, field, limit), patch.object(claude_native_auth, "bind") as bind:
                with self.assertRaisesRegex(workflow.WorkflowError, "bound"):
                    self.apply()
                bind.assert_not_called()
        atomic_json(f.root_state / current.FILENAME, {})
        with patch.object(claude_native_auth, "bind") as bind:
            with self.assertRaisesRegex(workflow.WorkflowError, "ledger"):
                f.run_diagnostic()
            bind.assert_not_called()
        self.assert_preserved()

    def test_isolation_failure_does_not_activate_or_allow_eighth_call(self):
        f = self.fixture
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute),
        ):
            f.run_diagnostic()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=KeyboardInterrupt),
        ):
            with self.assertRaises(KeyboardInterrupt):
                f.run_diagnostic()
        self.assertEqual(diagnostics.ledger(f.repo)["attempts"][6]["status"], "incomplete")
        with patch.object(review_claude, "execute") as execute:
            with self.assertRaisesRegex(workflow.WorkflowError, "eighth"):
                f.run_diagnostic()
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(f.repo, self.policy())
            execute.assert_not_called()
        self.assert_preserved()
