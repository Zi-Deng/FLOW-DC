"""Diagnostic accounting and activation use real packets with synthetic native streams."""

from unittest.mock import patch

from test_workflow import GitFixture, workflow

# isort: split
import claude_native_auth
import claude_telemetry
import review_claude
import review_diagnostics as diagnostics
import review_policy
from claude_fixtures import AUTHENTICATION, native_stream


class DiagnosticTests(GitFixture):
    def setUp(self):
        super().setUp()
        # Frozen original two-attempt policy, never current v4 authorization.
        provider = patch.dict(review_policy.PROVIDERS["claude-code"], adapter="claude-stream-json-2.1.282-v3")
        provider.start()
        self.addCleanup(provider.stop)
        default = patch.object(
            diagnostics, "DEFAULT_POLICY", review_policy.policy(review_policy.choices("claude-code"), {})
        )
        default.start()
        self.addCleanup(default.stop)
        binding = patch.object(claude_native_auth, "current_binding", return_value=AUTHENTICATION)
        binding.start()
        self.addCleanup(binding.stop)

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
                diagnostics.require_activation(
                    self.repo, {**diagnostics.DEFAULT_POLICY, "authentication": AUTHENTICATION}
                )
            self.assertEqual(self.run_diagnostic()["remaining_attempts"], 0)
            with self.assertRaisesRegex(workflow.WorkflowError, "Both authorized"):
                self.run_diagnostic()
        self.assertEqual(execute.call_count, 2)
        policy = review_policy.policy(review_policy.choices("claude-code"), {})
        policy["authentication"] = AUTHENTICATION
        diagnostics.require_activation(self.repo, policy)
        policy["adapter"] = "unverified-next-adapter"
        with self.assertRaises(workflow.WorkflowError):
            diagnostics.require_activation(self.repo, policy)

    def test_explicit_lineage_retains_observed_generation_without_new_attempts(self):
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=self.execute),
        ):
            self.run_diagnostic()
            self.run_diagnostic()
        root = diagnostics.state_directory(self.repo)
        original = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
        current = {**AUTHENTICATION, "generation_id": "33333333-3333-4333-8333-333333333333"}
        policy = {**diagnostics.DEFAULT_POLICY, "authentication": current}
        with patch.object(claude_native_auth, "capability_lineage", return_value=False):
            with self.assertRaises(workflow.WorkflowError):
                diagnostics.require_activation(self.repo, policy)
        with patch.object(claude_native_auth, "capability_lineage", return_value=True):
            result = diagnostics.require_activation(self.repo, policy)
        self.assertFalse(result["current_generation_live_tested"])
        self.assertEqual(result["observed_authentication"], [AUTHENTICATION, AUTHENTICATION])
        self.assertEqual(result["current_authentication"], current)
        self.assertEqual(original, {p: p.read_bytes() for p in original})
        self.assertEqual(len(diagnostics.ledger(self.repo)["attempts"]), 2)

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
            diagnostics.require_activation(
                self.repo, {**diagnostics.DEFAULT_POLICY, "authentication": AUTHENTICATION}
            )

    def test_modified_exact_report_or_packet_invalidates_activation(self):
        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=self.execute),
        ):
            self.assertEqual(self.run_diagnostic()["status"], "qualified")
            self.assertEqual(self.run_diagnostic()["status"], "qualified")
        policy = {**diagnostics.DEFAULT_POLICY, "authentication": AUTHENTICATION}
        self.assertTrue(diagnostics.require_activation(self.repo, policy)["current_generation_live_tested"])
        directory = diagnostics.state_directory(self.repo) / "attempt-1"
        for path in (directory / "report.txt", directory / "packet/capability/fixture.txt"):
            with self.subTest(artifact=path.relative_to(directory)):
                original = path.read_bytes()
                try:
                    path.write_bytes(original + b"\n")
                    with self.assertRaisesRegex(workflow.WorkflowError, "activation requires matching"):
                        diagnostics.require_activation(self.repo, policy)
                finally:
                    path.write_bytes(original)
                self.assertTrue(
                    diagnostics.require_activation(self.repo, policy)["current_generation_live_tested"]
                )
        self.assertEqual(len(diagnostics.ledger(self.repo)["attempts"]), 2)

    def test_packet_inventory_ids_obey_the_supplied_report_schema(self):
        import json
        import re

        def inspect(repo, directory, meta, *, diagnostic):
            packet = directory / "packet"
            schema = json.loads((packet / "report-schema.json").read_text())
            pattern = schema["properties"]["reviewed"]["items"]["pattern"]
            required = json.loads((packet / "required-material.json").read_text())["required"]
            self.assertEqual(len({item["id"] for item in required}), len(required))
            for item in required:
                self.assertIsNotNone(re.fullmatch(pattern, item["id"]))
            return self.execute(repo, directory, meta, diagnostic=diagnostic)

        with (
            patch.object(review_claude, "preflight"),
            patch.object(review_claude, "execute", side_effect=inspect),
        ):
            self.run_diagnostic()
