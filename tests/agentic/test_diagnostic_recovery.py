"""Local synthetic records only; no actual recovery grant or provider call."""

import copy
import fcntl
import json
from unittest.mock import patch

from test_workflow import GitFixture, workflow

# isort: split
import claude_native_auth
import claude_telemetry
import claude_telemetry_v1
import diagnostic_recovery as recovery
import review_claude
import review_diagnostics as diagnostics
import review_policy
from claude_fixtures import AUTHENTICATION, native_stream
from tasks import atomic_json, digest


class RecoveryTests(GitFixture):
    def setUp(self):
        super().setUp()
        # Frozen revision-4 tests model its original v2 runtime explicitly.
        provider = patch.dict(review_policy.PROVIDERS["claude-code"], adapter=recovery.REV4_ADAPTER)
        provider.start()
        self.addCleanup(provider.stop)
        default = patch.object(
            diagnostics, "DEFAULT_POLICY", review_policy.policy(review_policy.choices("claude-code"), {})
        )
        default.start()
        self.addCleanup(default.stop)
        self.repo._info["nameWithOwner"] = "Zi-Deng/FLOW-DC"
        binding = patch.object(claude_native_auth, "current_binding", return_value=AUTHENTICATION)
        binding.start()
        self.addCleanup(binding.stop)
        self.cfg = workflow.configuration(self.root)
        self.task_record = self.root / ".agentic-local/tasks/issue-33.json"
        self.task_record.parent.mkdir(parents=True, exist_ok=True)
        self.approval = {
            "issue": 33,
            "plan_comment": recovery.CONTRACT["plan_comment"],
            "contract": recovery.CONTRACT,
            "source": "Synthetic actual-approval fixture, not live authorization",
            "recorded_at": "2026-10-03T00:12:45+00:00",
        }
        atomic_json(
            self.task_record,
            {"schema_version": 1, "key": "issue-33", "repository": self.repo.name, "approval": self.approval},
        )
        with (
            patch.dict(review_policy.PROVIDERS["claude-code"], adapter="claude-stream-json-2.1.282-v1"),
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=self.execute),
        ):
            self.run_diagnostic()
        self.root_state = diagnostics.state_directory(self.repo)
        self.original = {
            p: p.read_bytes() for p in self.root_state.rglob("*") if p.is_file() and p.name != "ledger.lock"
        }

    def run_diagnostic(self):
        return diagnostics.run(self.repo, self.cfg, review_provider="claude-code")

    def execute(self, repo, directory, meta, *, diagnostic):
        self.assertTrue(diagnostic)
        packet = directory / "packet"
        atomic_json(
            directory / "attempt.json",
            {
                "schema_version": 5,
                "input_digest": digest(meta),
                "policy_digest": digest(meta["review_policy"]),
                "status": "started",
                "requests": 1,
            },
        )
        body = "legacy partial\r\n" if meta["review_policy"]["adapter"].endswith("v1") else None
        parser = claude_telemetry_v1 if meta["review_policy"]["adapter"].endswith("v1") else claude_telemetry
        body, diag = parser.capture(
            native_stream(packet, packet, "fixture", body=body),
            packet,
            packet,
            meta["review_policy"],
            "fixture",
        )
        if meta["diagnostic_purpose"] == "isolation-refusal":
            diag["telemetry"]["controlled_refusals"] = 1
            diag["reasons"].append("controlled_refusal_diagnostic_only")
        return body, diag, "2.1.282"

    def apply(self):
        preview = recovery.prepare(self.repo, self.cfg)
        recovery.prepare(self.repo, self.cfg, apply=True, preview_digest=preview["preview_digest"])
        return preview

    def assert_original(self):
        self.assertEqual(self.original, {p: p.read_bytes() for p in self.original})

    def test_explicit_preview_and_apply_preserve_original_then_two_distinct_successes(self):
        preview = recovery.prepare(self.repo, self.cfg)
        self.assertEqual(preview["status"], "preview")
        self.assertFalse((self.root_state / recovery.FILENAME).exists())
        self.assert_original()
        with self.assertRaises(workflow.WorkflowError):
            recovery.prepare(self.repo, self.cfg, apply=True, preview_digest="changed")
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=self.execute) as execute,
        ):
            self.assertEqual(self.run_diagnostic()["attempt"], 2)
            meta = json.loads((self.root_state / "attempt-2/metadata.json").read_text())
            self.assertEqual(meta["diagnostic_purpose"], "native-tools-and-source")
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(
                    self.repo, {**diagnostics.DEFAULT_POLICY, "authentication": AUTHENTICATION}
                )
            self.assertEqual(self.run_diagnostic()["attempt"], 3)
            self.assertEqual(execute.call_count, 2)
            with self.assertRaisesRegex(workflow.WorkflowError, "fourth"):
                self.run_diagnostic()
            self.assertEqual(execute.call_count, 2)
        diagnostics.require_activation(
            self.repo, {**diagnostics.DEFAULT_POLICY, "authentication": AUTHENTICATION}
        )
        self.assert_original()

    def test_old_authorization_and_changed_approval_cannot_extend_or_activate(self):
        record = json.loads(self.task_record.read_text())
        record["approval"]["contract"]["plan_comment"] = 5959064895
        atomic_json(self.task_record, record)
        with self.assertRaises(workflow.WorkflowError):
            self.apply()
        self.assertFalse((self.root_state / recovery.FILENAME).exists())
        self.assert_original()

    def test_no_grant_retains_original_two_slot_semantics(self):
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=self.execute) as execute,
        ):
            self.assertEqual(self.run_diagnostic()["remaining_attempts"], 0)
            meta = json.loads((self.root_state / "attempt-2/metadata.json").read_text())
            self.assertEqual(meta["diagnostic_purpose"], "isolation-refusal")
            with self.assertRaisesRegex(workflow.WorkflowError, "Both authorized"):
                self.run_diagnostic()
            self.assertEqual(execute.call_count, 1)
        with self.assertRaises(workflow.WorkflowError):
            self.apply()

    def test_future_interruption_is_counted_and_stops_sequence(self):
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=KeyboardInterrupt) as execute,
        ):
            with self.assertRaises(KeyboardInterrupt):
                self.run_diagnostic()
            self.assertEqual(diagnostics.ledger(self.repo)["attempts"][1]["status"], "incomplete")
            with self.assertRaisesRegex(workflow.WorkflowError, "stops"):
                self.run_diagnostic()
            self.assertEqual(execute.call_count, 1)
        self.assert_original()

    def test_stale_grant_and_partial_state_fail_before_execution(self):
        self.apply()
        record = json.loads(self.task_record.read_text())
        record["approval"]["recorded_at"] = "changed"
        atomic_json(self.task_record, record)
        with patch.object(review_claude, "execute") as execute, self.assertRaises(workflow.WorkflowError):
            self.run_diagnostic()
        execute.assert_not_called()
        self.assert_original()

    def test_missing_required_evidence_or_wrong_purpose_blocks_slot_three(self):
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=self.execute),
        ):
            self.run_diagnostic()
        target = self.root_state / "attempt-2/report.txt"
        target.write_bytes(target.read_bytes() + b"\n")
        with patch.object(review_claude, "execute") as execute, self.assertRaises(workflow.WorkflowError):
            self.run_diagnostic()
        execute.assert_not_called()
        self.assert_original()

    def test_counter_truncation_and_limit_tampering_do_not_reset_slots(self):
        self.apply()
        path = self.root_state / recovery.FILENAME
        original = json.loads(path.read_text())
        for mutate in (lambda s: s["grant"].update(max_total_attempts=4), lambda s: s["attempts"].clear()):
            value = copy.deepcopy(original)
            mutate(value)
            value["grant_digest"] = digest(value["grant"])
            atomic_json(path, value)
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.ledger(self.repo)
        atomic_json(path, original)
        (self.root_state / "attempt-2").mkdir()
        with self.assertRaisesRegex(workflow.WorkflowError, "partial"):
            diagnostics.ledger(self.repo)
        self.assert_original()

    def test_verified_renewal_retains_observed_generation_without_reversing_lineage(self):
        self.apply()
        renewed = {**AUTHENTICATION, "generation_id": "33333333-3333-4333-8333-333333333333"}

        def lineage(observed, current, timeout):
            return observed == current or (observed == AUTHENTICATION and current == renewed)

        with (
            patch.object(claude_native_auth, "current_binding", return_value=renewed),
            patch.object(claude_native_auth, "capability_lineage", side_effect=lineage),
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=self.execute),
        ):
            self.run_diagnostic()
            self.run_diagnostic()
            state = diagnostics.ledger(self.repo)
            self.assertEqual(len(state["attempts"]), 3)
            result = diagnostics.require_activation(
                self.repo, {**diagnostics.DEFAULT_POLICY, "authentication": renewed}
            )
            self.assertEqual(result["observed_authentication"], [renewed, renewed])
            self.assertEqual(state["grant"]["policy"]["authentication"], AUTHENTICATION)
        self.assert_original()

    def test_future_incomplete_output_stops_without_repairing_exact_report(self):
        self.apply()

        def partial(*args, **kwargs):
            _, diag, version = self.execute(*args, **kwargs)
            return "unchanged partial report\r\n", diag, version

        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=partial) as execute,
        ):
            result = self.run_diagnostic()
            self.assertEqual(result["status"], "incomplete")
            self.assertEqual(result["remaining_attempts"], 0)
            body = (self.root_state / "attempt-2/report.txt").read_bytes()
            self.assertEqual(body, b"unchanged partial report\r\n")
            with self.assertRaisesRegex(workflow.WorkflowError, "stops"):
                self.run_diagnostic()
            self.assertEqual(execute.call_count, 1)
            self.assertEqual((self.root_state / "attempt-2/report.txt").read_bytes(), body)
        self.assert_original()

    def test_changed_purpose_policy_or_binding_cannot_authorize_slot_three(self):
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=self.execute),
        ):
            self.run_diagnostic()
        ledger = self.root_state / recovery.FILENAME
        metadata = self.root_state / "attempt-2/metadata.json"
        originals = {ledger: json.loads(ledger.read_text()), metadata: json.loads(metadata.read_text())}
        for target, mutate in (
            (ledger, lambda s: s["attempts"][1].update(purpose="isolation-refusal")),
            (metadata, lambda s: s["review_policy"].update(effort="high")),
            (metadata, lambda s: s["recovery"].update(number=3)),
            (metadata, lambda s: s["review_policy"]["authentication"].update(setup_provenance="edited")),
        ):
            with self.subTest(target=target.name):
                value = copy.deepcopy(originals[target])
                mutate(value)
                atomic_json(target, value)
                with (
                    patch.object(review_claude, "execute") as execute,
                    self.assertRaises(workflow.WorkflowError),
                ):
                    self.run_diagnostic()
                execute.assert_not_called()
                atomic_json(target, originals[target])
        self.assert_original()

    def test_preflight_failure_does_not_consume_or_infer_and_no_unverified_lineage(self):
        self.apply()
        with (
            patch.object(
                review_claude,
                "preflight",
                side_effect=workflow.WorkflowError("Synthetic prerequisite missing"),
            ),
            patch.object(review_claude, "execute") as execute,
            self.assertRaises(workflow.WorkflowError),
        ):
            self.run_diagnostic()
        execute.assert_not_called()
        self.assertEqual(len(diagnostics.ledger(self.repo)["attempts"]), 1)
        other = {**AUTHENTICATION, "generation_id": "33333333-3333-4333-8333-333333333333"}
        with (
            patch.object(claude_native_auth, "current_binding", return_value=other),
            patch.object(claude_native_auth, "capability_lineage", return_value=False),
            patch.object(review_claude, "execute") as execute,
            self.assertRaises(workflow.WorkflowError),
        ):
            self.run_diagnostic()
        execute.assert_not_called()
        self.assert_original()

    def test_stale_preview_lock_and_interrupted_application_preserve_original(self):
        preview = recovery.prepare(self.repo, self.cfg)
        other = {**AUTHENTICATION, "generation_id": "33333333-3333-4333-8333-333333333333"}
        with (
            patch.object(claude_native_auth, "current_binding", return_value=other),
            self.assertRaisesRegex(workflow.WorkflowError, "preview digest"),
        ):
            recovery.prepare(self.repo, self.cfg, apply=True, preview_digest=preview["preview_digest"])
        with (self.root_state / "ledger.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(workflow.WorkflowError, "already in progress"):
                self.apply()
        with (
            patch.object(recovery, "atomic_json", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            self.apply()
        self.assertFalse((self.root_state / recovery.FILENAME).exists())
        self.assert_original()
        self.apply()
        initial = (self.root_state / recovery.FILENAME).read_bytes()
        self.apply()
        self.assertEqual((self.root_state / recovery.FILENAME).read_bytes(), initial)

    def test_historical_change_or_missing_strict_completion_blocks_recovery(self):
        self.apply()
        report = self.root_state / "attempt-1/report.txt"
        original = report.read_bytes()
        report.write_bytes(original + b"changed")
        with self.assertRaises(workflow.WorkflowError):
            recovery.load(self.repo)
        report.write_bytes(original)
        self.assert_original()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=self.execute),
        ):
            self.run_diagnostic()
        attempt = self.root_state / "attempt-2/attempt.json"
        attempt.unlink()
        with patch.object(review_claude, "execute") as execute, self.assertRaises(workflow.WorkflowError):
            self.run_diagnostic()
        execute.assert_not_called()

    def test_linked_historical_evidence_is_refused_before_reading_target(self):
        target = self.root_state / "attempt-1/metadata.json"
        original = target.read_bytes()
        target.unlink()
        target.symlink_to(self.root / "not-an-authorized-input")
        try:
            with self.assertRaisesRegex(workflow.WorkflowError, "symlink"):
                recovery.historical(self.repo)
        finally:
            target.unlink()
            target.write_bytes(original)
        self.assert_original()

    def test_stopped_v2_grant_remains_readable_but_never_authorizes_v3(self):
        self.apply()
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=KeyboardInterrupt),
        ):
            with self.assertRaises(KeyboardInterrupt):
                self.run_diagnostic()
        paths = [p for p in self.root_state.rglob("*") if p.is_file()]
        original = {p: p.read_bytes() for p in paths}
        with patch.dict(review_policy.PROVIDERS["claude-code"], adapter="claude-stream-json-2.1.282-v3"):
            state = recovery.load(self.repo)
            self.assertEqual(state["attempts"][1]["status"], "incomplete")
            with patch.object(review_claude, "execute") as execute:
                with self.assertRaisesRegex(workflow.WorkflowError, "original v2"):
                    self.run_diagnostic()
                execute.assert_not_called()
            with patch.object(claude_native_auth, "bind") as bind:
                with self.assertRaisesRegex(workflow.WorkflowError, "approval"):
                    self.apply()
                bind.assert_not_called()
            policy = review_policy.policy(review_policy.choices("claude-code"), {})
            policy["authentication"] = AUTHENTICATION
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(self.repo, policy)
        self.assertEqual(original, {p: p.read_bytes() for p in paths})
