"""Diagnostic accounting and activation use real packets with synthetic native streams."""

from unittest.mock import patch

from test_workflow import GitFixture, workflow

# isort: split
import claude_telemetry
import review_claude
import review_diagnostics as diagnostics
import review_policy
from claude_fixtures import native_stream


class DiagnosticTests(GitFixture):
    def execute(self, repo, directory, meta, *, diagnostic):
        self.assertTrue(diagnostic)
        packet = directory / "packet"
        self.assertFalse((packet / "source").exists())
        self.assertEqual(meta["review_policy"]["budget"]["timeout_seconds"], 300)
        self.assertEqual(meta["review_policy"]["budget"]["estimated_usd"], 2)
        body, diag = claude_telemetry.capture(
            native_stream(packet, packet, "fixture"), packet, packet, meta["review_policy"], "fixture"
        )
        if meta["diagnostic_purpose"] == "isolation-refusal":
            # This double tests ledger policy only. The telemetry suite separately
            # exercises correlated real-shaped refusals; neither is live evidence.
            diag["telemetry"]["controlled_refusals"] = 1
            diag["reasons"].append("controlled_refusal_diagnostic_only")
        return body, diag, "2.1.282"

    def run_diagnostic(self):
        return diagnostics.run(self.repo, workflow.configuration(self.root), review_provider="claude-code")

    def test_two_attempt_limit_and_matching_policy_activation(self):
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=self.execute) as execute,
        ):
            self.assertEqual(self.run_diagnostic()["status"], "qualified")
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(self.repo, diagnostics.DEFAULT_POLICY)
            self.assertEqual(self.run_diagnostic()["remaining_attempts"], 0)
            with self.assertRaisesRegex(workflow.WorkflowError, "Both authorized"):
                self.run_diagnostic()
        self.assertEqual(execute.call_count, 2)
        policy = review_policy.policy(review_policy.choices("claude-code"), {})
        diagnostics.require_activation(self.repo, policy)
        policy["adapter"] = "unverified-next-adapter"
        with self.assertRaises(workflow.WorkflowError):
            diagnostics.require_activation(self.repo, policy)

    def test_missing_token_or_receipt_consumes_no_attempt(self):
        with (
            patch.object(review_claude, "preflight", side_effect=workflow.WorkflowError("missing receipt")),
            patch.object(review_claude, "execute") as execute,
        ):
            with self.assertRaises(workflow.WorkflowError):
                self.run_diagnostic()
        execute.assert_not_called()
        self.assertEqual(diagnostics.ledger(self.repo)["attempts"], [])

    def test_failed_call_remains_accounted_and_cannot_activate(self):
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=OSError("synthetic interruption")),
        ):
            with self.assertRaises(OSError):
                self.run_diagnostic()
        self.assertEqual(diagnostics.ledger(self.repo)["attempts"][0]["status"], "incomplete")
        with self.assertRaises(workflow.WorkflowError):
            diagnostics.require_activation(self.repo, diagnostics.DEFAULT_POLICY)

    def test_modified_exact_report_or_packet_invalidates_activation(self):
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=self.execute),
        ):
            self.run_diagnostic()
        directory = diagnostics.state_directory(self.repo) / "attempt-1"
        report = directory / "report.txt"
        report.write_bytes(report.read_bytes() + b"\n")
        with self.assertRaises(workflow.WorkflowError):
            diagnostics.require_activation(self.repo, diagnostics.DEFAULT_POLICY)
