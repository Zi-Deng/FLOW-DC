"""Revision-5 finite restart; all accounts, approvals and provider calls are synthetic."""

import copy
import json
import unittest
from unittest.mock import patch

import test_diagnostic_recovery as prior_tests
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
    "plan_comment": 5964179523,
    "issue_digest": "5040edb6c15ef83527e801e3712a7693b59a75e8a7b96bd773064e76c75e1621",
    "plan_digest": "b82c3f9610e13929529c1840468c4484eebdc81efd63315be3b175f70922a06f",
}


class RevisionFiveTests(unittest.TestCase):
    def setUp(self):
        self.fixture = f = prior_tests.RecoveryTests(methodName="runTest")
        f.setUp()
        self.addCleanup(f.doCleanups)
        self.addCleanup(f.tearDown)
        f.apply()

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
        self.record["approval_history"] = [copy.deepcopy(self.record["approval"])]
        self.record["approval"] = {
            **self.record["approval"],
            "contract": CONTRACT,
            "plan_comment": CONTRACT["plan_comment"],
        }
        atomic_json(f.task_record, self.record)
        current = patch.dict(review_policy.PROVIDERS["claude-code"], adapter="claude-stream-json-2.1.282-v3")
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
            state = prior.load(f.repo)
        self.assertEqual([x["status"] for x in state["attempts"]], ["incomplete", "incomplete"])
        self.assert_preserved()

    def test_approved_preview_has_new_purposes_without_mutating_stopped_grant(self):
        preview = prior.prepare(self.fixture.repo, self.fixture.cfg)
        self.assertEqual(preview["max_total_attempts"], 4)
        self.assertEqual(
            preview["slots"],
            [
                {"number": 3, "purpose": "native-tools-and-source"},
                {"number": 4, "purpose": "isolation-refusal"},
            ],
        )
        self.assertEqual(
            preview["contract_digest"], "5c8c8c8bc87c2cd02229ea1c0f74b98a9748542fa770fc93178234d73501c45f"
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

    def test_two_new_purposes_activate_and_fifth_call_is_refused(self):
        f = self.fixture
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute) as execute,
        ):
            self.assertEqual(f.run_diagnostic()["attempt"], 3)
            self.assertEqual(
                json.loads((f.root_state / "attempt-3/metadata.json").read_text())["diagnostic_purpose"],
                "native-tools-and-source",
            )
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(f.repo, self.policy())
            self.assertEqual(f.run_diagnostic()["attempt"], 4)
            self.assertEqual(execute.call_count, 2)
            with self.assertRaisesRegex(workflow.WorkflowError, "fifth"):
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

    def test_historical_v5_authority_allows_reads_but_not_new_calls_or_activation(self):
        import diagnostic_recovery_v5 as current

        f = self.fixture
        self.apply()
        changed = copy.deepcopy(self.record)
        changed["approval_history"].append(changed["approval"])
        changed["approval"] = {**changed["approval"], "contract": {**CONTRACT, "plan_digest": "0" * 64}}
        atomic_json(f.task_record, changed)
        with patch.object(claude_native_auth, "store", side_effect=AssertionError("No auth")):
            self.assertEqual(len(current.load(f.repo)["attempts"]), 2)
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

    def test_current_approval_is_required_even_if_v5_is_in_history(self):
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
            self.assertEqual(diagnostics.ledger(f.repo)["attempts"][2]["status"], "incomplete")
            self.apply()  # Idempotent application cannot reset the counted interruption.
            self.assertEqual(len(diagnostics.ledger(f.repo)["attempts"]), 3)
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
            self.assertEqual((f.root_state / "attempt-3/report.txt").read_bytes(), b"exact partial\r\n")
            with self.assertRaisesRegex(workflow.WorkflowError, "stops"):
                f.run_diagnostic()
            self.assertEqual(execute.call_count, 1)
        self.assert_preserved()

    def test_limit_purpose_counter_policy_and_partial_migration_tampering_fail_closed(self):
        import diagnostic_recovery_v5 as current
        from tasks import digest

        f = self.fixture
        self.apply()
        path = f.root_state / current.FILENAME
        original = json.loads(path.read_text())
        changes = [
            lambda s: s["grant"].update(max_total_attempts=5),
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
        (f.root_state / "attempt-4").mkdir()
        with self.assertRaisesRegex(workflow.WorkflowError, "partial"):
            diagnostics.ledger(f.repo)
        self.assert_preserved()

    def test_stale_preview_interrupted_application_and_repeat_apply(self):
        import diagnostic_recovery_v5 as current

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

    def test_missing_slot_three_evidence_blocks_four_and_activation(self):
        f = self.fixture
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute),
        ):
            f.run_diagnostic()
        (f.root_state / "attempt-3/report.txt").write_bytes(b"changed")
        with (
            patch.object(
                review_claude, "preflight", side_effect=AssertionError("Invalid evidence reached preflight")
            ) as preflight,
            patch.object(review_claude, "execute") as execute,
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "Diagnostic report bytes changed"):
                f.run_diagnostic()
            with self.assertRaisesRegex(workflow.WorkflowError, "activation requires matching"):
                diagnostics.require_activation(f.repo, self.policy())
            preflight.assert_not_called()
            execute.assert_not_called()
        self.assertEqual(len(diagnostics.ledger(f.repo)["attempts"]), 3)
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
            self.assertEqual(len(diagnostics.ledger(f.repo)["attempts"]), 4)
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
        self.assertEqual(len(diagnostics.ledger(f.repo)["attempts"]), 2)
        changed = {**AUTHENTICATION, "generation_id": "33333333-3333-4333-8333-333333333333"}
        with (
            patch.object(claude_native_auth, "current_binding", return_value=changed),
            patch.object(claude_native_auth, "capability_lineage", return_value=False),
            patch.object(
                review_claude, "preflight", side_effect=AssertionError("Invalid lineage reached preflight")
            ) as preflight,
            patch.object(review_claude, "execute") as execute,
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "verified authentication lineage changed"):
                f.run_diagnostic()
            preflight.assert_not_called()
            execute.assert_not_called()
        self.assertEqual(len(diagnostics.ledger(f.repo)["attempts"]), 2)
        with (f.root_state / "ledger.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(workflow.WorkflowError, "already in progress"):
                self.apply()
        self.assert_preserved()

    def test_wrong_new_purpose_and_current_approval_changes_refuse_execution(self):
        import diagnostic_recovery_v5 as current

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
        changed["attempts"][2]["purpose"] = "isolation-refusal"
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
