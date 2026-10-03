"""Provider identity, exact allocation, deadline and durable-stop boundaries."""

import copy
import json
from unittest.mock import patch

from test_workflow import GitFixture, review, workflow

# isort: split

import claude_native_auth
import claude_telemetry
import review_batch as batch
import review_policy
import review_prompt
from batch_fixtures import authorize, limits
from claude_fixtures import native_events
from tasks import atomic_json
from test_review_providers import AUTHENTICATION


class ProviderBatchTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.commit_task()
        binding = patch.object(claude_native_auth, "current_binding", return_value=AUTHENTICATION)
        binding.start()
        self.addCleanup(binding.stop)
        self.directory = review.prepare(self.repo, 31, 12, 1234, review_provider="claude-code")
        self.bounds = {**limits(), "kind": "reference-usd", "cost": "100", "unit_cost": "0.1"}
        self.calls = []

    def provider(self, repo, directory, **kwargs):
        meta = review.verify_packet(directory)
        self.assertEqual(meta["review_policy"]["provider"], "claude-code")
        with patch.object(batch, "verify_harness"):
            effective = batch.dispatch_timeout(repo, directory, meta, kwargs["dispatch_context"])
        self.assertLessEqual(effective, self.bounds["unit_seconds"])
        packet = directory / "packet"
        prompt = review_prompt.native(directory, meta)
        self.assertIn("All three probes are mandatory", prompt)
        self.assertIn('"path": ".", "glob": "capability/fixture.txt"', prompt)
        self.assertNotIn("Inspect EVERY required-material.json entry", prompt)
        rows = native_events(packet, packet, "fixture")
        report = json.loads(rows[-1]["result"])
        report["reviewed"] = [key for key in report["reviewed"] if key in meta["batch_unit"]["required_ids"]]
        rows[-1]["result"] = json.dumps(report)
        raw = "\n".join(json.dumps(row) for row in rows)
        body, diagnostics = claude_telemetry.capture(raw, packet, packet, meta["review_policy"], "fixture")
        self.calls.append(directory)
        review.save_result(directory, meta, body, diagnostics, "2.1.282")
        return review.recover_review(repo, directory)

    def execute(self):
        batch.select(self.directory, self.bounds, authorize(self.repo, self.directory, self.bounds))
        with patch.object(review, "review", side_effect=self.provider):
            return batch.execute(self.repo, self.directory)

    def test_native_full_inventory_integration_and_mixed_provider_rejection(self):
        self.execute()
        result = review.qualification(self.directory, require=True)
        self.assertTrue(result["qualified"])
        self.assertEqual(result["inspected_count"], result["required_count"])
        self.assertEqual(len(self.calls), len(batch.plan(self.directory)["units"]))
        target = self.calls[0]
        path = target / "metadata.json"
        meta = review.verify_packet(target)
        for replacement in (
            review_policy.policy(review_policy.choices("copilot"), {}),
            {**meta["review_policy"], "effort": "high"},
            {**meta["review_policy"], "authentication": {**AUTHENTICATION, "generation": "changed"}},
        ):
            changed = {**meta, "review_policy": replacement, "requested_model": replacement["model"]}
            atomic_json(path, changed)
            with self.subTest(provider=replacement["provider"]), self.assertRaises(workflow.WorkflowError):
                review.qualification(self.directory, require=True)
        atomic_json(path, meta)
        self.assertTrue(review.coverage_ready(self.directory))

    def test_exact_decimal_budget_and_no_extra_spend(self):
        policy = review.verify_packet(self.directory)["review_policy"]
        normalized, child = review_policy.batch_budget(
            {**self.bounds, "requests": 3, "cost": "0.3"}, policy, 3
        )
        self.assertEqual(normalized["cost"], "0.3")
        self.assertEqual(child["budget"]["extra_spend_authorized_usd"], 0)
        for mutation in (
            {"cost": "0.2999999999999999999999999999"},
            {"kind": "ai-credits"},
            {"unit_cost": True},
            {"cost": "NaN"},
            {"unit_seconds": 901},
            {"requests": 2.0},
            {"unit_cost": "11"},
            {"requests": 2},
        ):
            with self.subTest(mutation=mutation), self.assertRaises(workflow.WorkflowError):
                review_policy.batch_budget({**self.bounds, **mutation}, policy, 3)
        changed = copy.deepcopy(policy)
        changed["budget"]["extra_spend_authorized_usd"] = 1
        with self.assertRaises(workflow.WorkflowError):
            review_policy.batch_budget(self.bounds, changed, 3)

    def test_missing_or_changed_authorization_never_calls_provider(self):
        auth = authorize(self.repo, self.directory, self.bounds)
        for value in (None, {**auth, "preview_digest": "0" * 64}, {**auth, "name": ""}):
            with self.assertRaises(workflow.WorkflowError):
                batch.select(self.directory, self.bounds, value)
        self.assertFalse((self.directory / "batch-state.json").exists())
        self.assertEqual(self.calls, [])

    def test_elapsed_budget_includes_final_unit_completion(self):
        batch.select(self.directory, self.bounds, authorize(self.repo, self.directory, self.bounds))
        now = [1000]

        def late(repo, directory, **kwargs):
            # Do not mock assessment: retain an actual complete synthetic capture.
            with patch.object(batch, "dispatch_timeout", return_value=60):
                result = self.provider(repo, directory, **kwargs)
            if review.verify_packet(directory)["batch_unit"]["unit"]["kind"] == "integration":
                now[0] = 1000 + self.bounds["seconds"] + 1
            return result

        with patch.object(review, "review", side_effect=late), self.assertRaises(workflow.WorkflowError):
            batch.execute(self.repo, self.directory, clock=lambda: now[0])
        self.assertEqual(len(self.calls), len(batch.plan(self.directory)["units"]))
        self.assertIsNotNone(batch.state_for(self.directory, batch.load(self.directory))["stop_reason"])

    def test_expiry_during_preflight_and_stop_cannot_be_cleared_by_recovery(self):
        batch.select(self.directory, self.bounds, authorize(self.repo, self.directory, self.bounds))

        def delayed(repo, directory, **kwargs):
            state = batch.state_for(self.directory, batch.load(self.directory))
            batch.dispatch_timeout(
                repo,
                directory,
                review.verify_packet(directory),
                kwargs["dispatch_context"],
                clock=lambda: state["deadline"] + 1,
            )

        with (
            patch.object(review, "review", side_effect=delayed),
            self.assertRaisesRegex(workflow.WorkflowError, "deadline expiry"),
        ):
            batch.execute(self.repo, self.directory)
        state = batch.state_for(self.directory, batch.load(self.directory))
        self.assertEqual(len(state["reservations"]), 1)
        self.assertIsNotNone(state["stop_reason"])
        with patch.object(review, "review") as provider:
            batch.execute(self.repo, self.directory, recover_only=True)
            with self.assertRaisesRegex(workflow.WorkflowError, "durably stopped"):
                batch.execute(self.repo, self.directory, resume=True)
            provider.assert_not_called()

    def test_clock_rollback_and_concurrent_lock_prevent_dispatch(self):
        batch.select(self.directory, self.bounds, authorize(self.repo, self.directory, self.bounds))
        with batch.locked(self.directory), self.assertRaises(workflow.WorkflowError):
            batch.execute(self.repo, self.directory)
        clock = iter([1000, 999])
        with (
            patch.object(review, "review") as provider,
            self.assertRaisesRegex(workflow.WorkflowError, "rollback"),
        ):
            batch.execute(self.repo, self.directory, clock=lambda: next(clock))
        provider.assert_not_called()

    def test_oversized_integration_stops_before_its_provider_call(self):
        self.bounds["max_integration_bytes"] = 1
        with self.assertRaisesRegex(workflow.WorkflowError, "Integration report material"):
            self.execute()
        self.assertEqual(len(self.calls), len(batch.plan(self.directory)["units"]) - 1)
        self.assertFalse(review.coverage_ready(self.directory))

    def test_current_harness_binding_cannot_name_arbitrary_subset(self):
        auth = authorize(self.repo, self.directory, self.bounds)
        with self.assertRaisesRegex(workflow.WorkflowError, "complete module hashes"):
            batch.verify_harness(auth)

    def test_real_native_adapter_retains_identity_probes_and_effective_timeout(self):
        import contextlib
        import subprocess

        import review_claude
        import review_cli
        import review_diagnostics
        import review_process

        calls = []
        home = self.parent / "synthetic-native-home"
        home.mkdir()

        @contextlib.contextmanager
        def snapshot(policy):
            self.assertEqual(policy["authentication"], AUTHENTICATION)
            yield {"HOME": str(home)}, lambda: None

        def provider(args, **kwargs):
            self.assertIn("claude-opus-5-5", args)
            self.assertIn("medium", args)
            prompt = args[-1]
            self.assertIn("All three probes are mandatory", prompt)
            self.assertIn("offset=start, limit=end-start+1", prompt)
            self.assertLessEqual(kwargs["timeout"], 60)
            session = args[args.index("--session-id") + 1]
            workspace = kwargs["cwd"]
            rows = native_events(workspace, workspace, session)
            calls.append(session)
            return subprocess.CompletedProcess(args, 0, "\n".join(json.dumps(r) for r in rows).encode(), b"")

        batch.select(self.directory, self.bounds, authorize(self.repo, self.directory, self.bounds))
        with (
            patch.object(review_claude, "preflight", return_value="/fixture/claude"),
            patch.object(review_diagnostics, "require_activation"),
            patch.object(claude_native_auth, "snapshot", side_effect=snapshot),
            patch.object(review_cli, "executable", return_value="/fixture/claude"),
            patch.object(review_claude, "managed_controls"),
            patch.object(batch, "verify_harness"),
            patch.object(review_process, "capture", side_effect=provider),
        ):
            batch.execute(self.repo, self.directory)
        self.assertEqual(len(calls), len(set(calls)))
        self.assertEqual(len(calls), len(batch.plan(self.directory)["units"]))
        self.assertTrue(review.coverage_ready(self.directory))
