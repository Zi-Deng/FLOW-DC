"""Revision-8 finite restart; all accounts, approvals and provider calls are synthetic."""

import copy
import json
import unittest
from unittest.mock import patch

from test_workflow import workflow

# isort: split
import claude_native_auth
import diagnostic_recovery as prior
import diagnostic_recovery_v7 as stopped
import review_claude
import review_diagnostics as diagnostics
import review_policy
import test_diagnostic_recovery_v7 as prior_tests
from claude_fixtures import AUTHENTICATION
from tasks import atomic_json

CONTRACT = {
    "issue": 33,
    "plan_comment": 5965755308,
    "issue_digest": "2cb85f8a0db7f58d5929e49acbeec955f395f4f9f05c91f1848fc4f0c7af8207",
    "plan_digest": "d744a1e290ce55b459c75f9edb0466d59dc0f6e91173572091cb8bd458b3d9b1",
}


class RevisionEightTests(unittest.TestCase):
    def setUp(self):
        previous = prior_tests.RevisionSevenTests(methodName="runTest")
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
        current = patch.dict(review_policy.PROVIDERS["claude-code"], adapter="claude-stream-json-2.1.282-v6")
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
            ["incomplete", "incomplete", "incomplete", "qualified", "incomplete", "qualified", "incomplete"],
        )
        self.assert_preserved()

    def test_approved_preview_has_new_purposes_without_mutating_stopped_grant(self):
        preview = prior.prepare(self.fixture.repo, self.fixture.cfg)
        self.assertEqual(preview["max_total_attempts"], 9)
        self.assertEqual(
            preview["slots"],
            [
                {"number": 8, "purpose": "native-tools-and-source"},
                {"number": 9, "purpose": "isolation-refusal"},
            ],
        )
        self.assertEqual(
            preview["contract_digest"], "dd3a54615f7db9cd099b083df98c33ca3a525552a5a591d20e7dc68b6977191e"
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

    def test_two_new_purposes_activate_and_tenth_call_is_refused(self):
        f = self.fixture
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute) as execute,
        ):
            self.assertEqual(f.run_diagnostic()["attempt"], 8)
            self.assertEqual(
                json.loads((f.root_state / "attempt-8/metadata.json").read_text())["diagnostic_purpose"],
                "native-tools-and-source",
            )
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(f.repo, self.policy())
            self.assertEqual(f.run_diagnostic()["attempt"], 9)
            self.assertEqual(execute.call_count, 2)
            with self.assertRaisesRegex(workflow.WorkflowError, "tenth"):
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

    def test_historical_v8_authority_allows_reads_but_not_new_calls_or_activation(self):
        import diagnostic_recovery_v8 as current

        f = self.fixture
        self.apply()
        changed = copy.deepcopy(self.record)
        changed["approval_history"].append(changed["approval"])
        changed["approval"] = {**changed["approval"], "contract": {**CONTRACT, "plan_digest": "0" * 64}}
        atomic_json(f.task_record, changed)
        with patch.object(claude_native_auth, "store", side_effect=AssertionError("No auth")):
            self.assertEqual(len(current.load(f.repo)["attempts"]), 7)
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

    def test_current_approval_is_required_even_if_v8_is_in_history(self):
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
            self.assertEqual(diagnostics.ledger(f.repo)["attempts"][7]["status"], "incomplete")
            self.apply()  # Idempotent application cannot reset the counted interruption.
            self.assertEqual(len(diagnostics.ledger(f.repo)["attempts"]), 8)
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
            self.assertEqual((f.root_state / "attempt-8/report.txt").read_bytes(), b"exact partial\r\n")
            with self.assertRaisesRegex(workflow.WorkflowError, "stops"):
                f.run_diagnostic()
            self.assertEqual(execute.call_count, 1)
        self.assert_preserved()

    def test_limit_purpose_counter_policy_and_partial_migration_tampering_fail_closed(self):
        import diagnostic_recovery_v8 as current
        from tasks import digest

        f = self.fixture
        self.apply()
        path = f.root_state / current.FILENAME
        original = json.loads(path.read_text())
        changes = [
            lambda s: s["grant"].update(max_total_attempts=10),
            lambda s: s["grant"].update(max_total_timeout_seconds=2701),
            lambda s: s["grant"].update(max_total_estimated_usd=19),
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
        (f.root_state / "attempt-9").mkdir()
        with self.assertRaisesRegex(workflow.WorkflowError, "partial"):
            diagnostics.ledger(f.repo)
        self.assert_preserved()

    def test_stale_preview_interrupted_application_and_repeat_apply(self):
        import diagnostic_recovery_v8 as current

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

    def test_missing_slot_eight_evidence_blocks_nine_and_activation(self):
        f = self.fixture
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute),
        ):
            f.run_diagnostic()
        (f.root_state / "attempt-8/report.txt").write_bytes(b"changed")
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
            self.assertEqual(len(diagnostics.ledger(f.repo)["attempts"]), 9)
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
        self.assertEqual(len(diagnostics.ledger(f.repo)["attempts"]), 7)
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
        import diagnostic_recovery_v8 as current

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
        changed["attempts"][7]["purpose"] = "isolation-refusal"
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

    def test_stopped_old_grants_never_reach_authentication_for_current_v6(self):
        f = self.fixture
        with (
            patch.object(claude_native_auth, "bind") as bind,
            patch.object(review_claude, "execute") as execute,
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "revision-8 grant"):
                f.run_diagnostic()
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(f.repo, self.policy())
            bind.assert_not_called()
            execute.assert_not_called()
        self.assert_preserved()

    def test_historical_bounds_and_partial_new_ledger_refuse_before_authentication(self):
        import diagnostic_recovery_v8 as current

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

    def test_isolation_failure_does_not_activate_or_allow_tenth_call(self):
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
        self.assertEqual(diagnostics.ledger(f.repo)["attempts"][8]["status"], "incomplete")
        with patch.object(review_claude, "execute") as execute:
            with self.assertRaisesRegex(workflow.WorkflowError, "tenth"):
                f.run_diagnostic()
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(f.repo, self.policy())
            execute.assert_not_called()
        self.assert_preserved()

    def test_tool_contract_tampering_and_first_evidence_stop_before_authentication(self):
        import diagnostic_recovery_v8 as current
        from tasks import digest

        f = self.fixture
        self.apply()
        path = f.root_state / current.FILENAME
        original = json.loads(path.read_text())
        for change in (
            lambda grant: grant.pop("diagnostic_tool_contract"),
            lambda grant: grant["diagnostic_tool_contract"].update(schema_version=True),
            lambda grant: grant["diagnostic_tool_contract"]["grep_canary"].update(
                path="capability/fixture.txt"
            ),
            lambda grant: grant["diagnostic_tool_contract"]["grep_canary"].update(head_limit=10.0),
        ):
            altered = copy.deepcopy(original)
            change(altered["grant"])
            altered["grant_digest"] = digest(altered["grant"])
            atomic_json(path, altered)
            with patch.object(claude_native_auth, "bind", side_effect=AssertionError("No authentication")):
                with self.assertRaises(workflow.WorkflowError):
                    f.run_diagnostic()
        atomic_json(path, original)
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute),
        ):
            f.run_diagnostic()
        meta_path = f.root_state / "attempt-8/metadata.json"
        meta = json.loads(meta_path.read_text())
        saved = meta_path.read_bytes()
        del meta["diagnostic_tool_contract"]
        atomic_json(meta_path, meta)
        with patch.object(claude_native_auth, "bind", side_effect=AssertionError("No authentication")):
            with self.assertRaises(workflow.WorkflowError):
                f.run_diagnostic()
        meta_path.write_bytes(saved)
        (f.root_state / "attempt-8/report.txt").write_bytes(b"edited exact report")
        with patch.object(claude_native_auth, "bind", side_effect=AssertionError("No authentication")):
            with self.assertRaises(workflow.WorkflowError):
                f.run_diagnostic()
        self.assert_preserved()

    def test_new_packet_prompt_capture_and_assessment_bind_exact_tool_contract(self):
        import diagnostic_tool_contract
        from tasks import digest

        f = self.fixture
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=f.execute),
        ):
            f.run_diagnostic()
        directory = f.root_state / "attempt-8"
        meta = json.loads((directory / "metadata.json").read_text())
        assessment = json.loads((directory / "assessment.json").read_text())
        captured = json.loads((directory / "diagnostic-capture.json").read_text())
        self.assertEqual(meta["diagnostic_tool_contract"], diagnostic_tool_contract.contract())
        self.assertEqual(assessment["diagnostic_tool_contract"], meta["diagnostic_tool_contract"])
        self.assertEqual(captured["input_digest"], digest(meta))
        self.assertIn(diagnostic_tool_contract.instruction(), (directory / "packet/START.txt").read_text())
