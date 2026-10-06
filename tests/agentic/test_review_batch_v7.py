"""Reporting batch orchestration with simulated native processes and real local Git."""

from pathlib import Path
from unittest.mock import patch

from test_workflow import review, workflow

# isort: split
import batch_fixtures
import review_batch
import review_batch_v7 as batch
import test_reporting_admission as admission_tests
from test_reporting_diagnostic import ReportingDiagnosticFixture

RUN = review.review


class ReportingBatchFixture(ReportingDiagnosticFixture):
    qualify = admission_tests.ReportingAdmissionTests.qualify
    ordinary_response = admission_tests.ReportingAdmissionTests.ordinary_response

    def setUp(self):
        super().setUp()
        # Native storage is simulated in this orchestration fixture. Dedicated
        # lifetime regressions exercise the real reader against synthetic stores.
        import claude_native_auth

        patch(
            "review_lifetime.current_binding",
            side_effect=lambda unit, window: claude_native_auth.current_binding(unit),
        ).start()
        self.qualify()
        self.directory = review.prepare(
            self.repo,
            31,
            12,
            1234,
            review_provider="claude-code",
            reporting={"max_turns": 80, "limits": self.policy["reporting"]["limits"]},
        )
        self.bounds = {
            **batch_fixtures.limits(),
            "kind": "reference-usd",
            "cost": "100",
            "unit_cost": "2",
            "max_report_bytes": self.policy["reporting"]["limits"]["report_bytes"],
        }
        self.children = []

    def child(self, repo, directory, **kwargs):
        self.children.append(Path(directory))
        self.ordinary = Path(directory)
        self.policy = review.verify_packet(directory)["review_policy"]
        return RUN(repo, directory, **kwargs)

    def execute_batch(self, **kwargs):
        review_batch.select(
            self.directory, self.bounds, batch_fixtures.authorize(self.repo, self.directory, self.bounds)
        )
        with (
            self.isolated(process=self.ordinary_response),
            patch.object(review, "current_pr"),
            patch.object(review, "review", side_effect=self.child),
            patch.object(batch, "verify_harness"),
            patch.object(workflow, "Repo", return_value=self.repo),
        ):
            return review_batch.execute(self.repo, self.directory, **kwargs)


class ReportingBatchTests(ReportingBatchFixture):
    def test_full_scope_exact_reports_and_current_admission(self):
        planned = review_batch.plan(self.directory)
        self.assertEqual(planned["schema_version"], 7)
        self.execute_batch()
        with patch.object(workflow, "Repo", return_value=self.repo):
            result = review.qualification(self.directory, require=True)
            self.assertTrue(review.coverage_ready(self.directory))
        self.assertTrue(result["qualified"])
        self.assertEqual(result["schema_version"], 6)
        self.assertEqual(result["inspected_count"], result["required_count"])
        self.assertEqual(len(self.children), len(planned["units"]))
        integration = self.children[-1]
        for child in self.children[:-1]:
            self.assertEqual(
                (child / "review.md").read_bytes(),
                (integration / "packet/component-reports" / (child.name + ".txt")).read_bytes(),
            )
            self.assertFalse(review.coverage_ready(child))
        ledger = review_batch.state_for(self.directory, review_batch.load(self.directory))
        self.assertEqual(ledger["schema_version"], 3)
        import copy

        import review_capacity
        import review_prompt

        saved = review_batch.load(self.directory)
        review_capacity.actual(integration, saved)
        usage = review_capacity.observed(integration, saved)
        self.assertGreater(usage["optional_source_bytes"], 0)
        self.assertIn(
            "never substitute summaries", review_prompt.native(integration, review.verify_packet(integration))
        )
        for key in ("optional_source_bytes", "input_utf8_bytes"):
            altered = copy.deepcopy(saved)
            altered["authorization"]["integration_capacity"][key] = 1
            with self.subTest(bound=key), self.assertRaises(workflow.WorkflowError):
                if key == "input_utf8_bytes":
                    review_capacity.actual(integration, altered)
                else:
                    review_capacity.observed(integration, altered)

    def test_capacity_and_v6_namespace_refuse_before_dispatch(self):
        with self.assertRaises(workflow.WorkflowError):
            review_batch.plan(self.directory, version=6)
        with self.assertRaisesRegex(workflow.WorkflowError, "Integration allocation"):
            review_batch.preview(self.directory, {**self.bounds, "max_integration_bytes": 1})
        with self.assertRaisesRegex(workflow.WorkflowError, "report limit"):
            review_batch.preview(
                self.directory, {**self.bounds, "max_report_bytes": self.bounds["max_report_bytes"] - 1}
            )
        self.assertEqual(self.calls, 2)

    def test_real_independent_packets_cannot_reclaim_shared_source_obligations(self):
        import review_claims

        first = review_batch.select(
            self.directory, self.bounds, batch_fixtures.authorize(self.repo, self.directory, self.bounds)
        )
        another = review.prepare(
            self.repo,
            31,
            12,
            1234,
            review_provider="claude-code",
            reporting={"max_turns": 80, "limits": self.policy["reporting"]["limits"]},
        )
        second = review_batch.select(
            another, self.bounds, batch_fixtures.authorize(self.repo, another, self.bounds)
        )
        self.assertNotEqual(self.directory, another)
        unit1, unit2 = first["units"][0], second["units"][0]
        self.assertTrue(set(unit1["required_ids"]).intersection(unit2["required_ids"]))
        value = review_claims.reserve(self.repo, first, unit1, {"dispatch_id": "first"})
        with self.assertRaisesRegex(workflow.WorkflowError, "already claimed"):
            review_claims.reserve(self.repo, second, unit2, {"dispatch_id": "second"})
        with self.assertRaisesRegex(workflow.WorkflowError, "continuation lifecycle"):
            from tasks import digest

            review_claims.reserve(self.repo, second, unit2, {"dispatch_id": "second"}, previous=digest(value))
        self.assertEqual(self.calls, 2)

    def test_optional_input_overrun_retains_report_and_stops_without_retry(self):
        authorization = batch_fixtures.authorize(self.repo, self.directory, self.bounds)
        authorization["integration_capacity"]["optional_source_bytes"] = 1
        batch.select(self.directory, self.bounds, authorization)
        with (
            patch.object(batch_fixtures, "authorize", return_value=authorization),
            self.assertRaisesRegex(workflow.WorkflowError, "Incomplete unit"),
        ):
            self.execute_batch()
        state = batch.state_for(self.directory, batch.load(self.directory))
        self.assertIsNotNone(state["stop_reason"])
        self.assertEqual(state["reservations"][-1]["status"], "assessed-incomplete")
        final = self.children[-1]
        before = (final / "review.md").read_bytes()
        calls = self.calls
        with patch.object(review, "review", side_effect=AssertionError("Recovery cannot infer")):
            batch.execute(self.repo, self.directory, recover_only=True)
        self.assertEqual((final / "review.md").read_bytes(), before)
        self.assertEqual(self.calls, calls)
        self.assertFalse(batch.qualification(self.directory)["qualified"])

    def test_integration_input_does_not_require_available_archive_or_navigation_storage(self):
        import review_capacity
        import review_navigation

        planned = batch.plan(self.directory)
        estimate = review_capacity.estimate(self.directory, planned, self.bounds)
        mandatory = planned["units"][-1]["required_volume"]["bytes"]
        expected = (
            mandatory
            + estimate["mandatory_guidance_bytes"]
            + estimate["integration_report_bytes"]
            + estimate["schema_bytes"]
            + estimate["prompt_bytes"]
        )
        self.assertEqual(estimate["input_envelope_bytes"], expected)
        self.assertGreater(planned["resources"]["context_bytes"], mandatory)
        self.assertGreater(review_navigation.MAX_NAVIGATION_BYTES, expected)

    def test_reviewed_model_capacity_is_required_and_bound(self):
        import copy

        preview = batch.preview(self.directory, self.bounds)
        estimate = preview["integration_capacity_estimate"]
        self.assertEqual(estimate["retained_exact_report_bytes"], 0)
        self.assertEqual(
            estimate["worst_remaining_report_bytes"],
            (len(preview["units"]) - 1) * self.bounds["max_report_bytes"],
        )
        auth = batch_fixtures.authorize(self.repo, self.directory, self.bounds)
        for mutation in ("missing", "input", "output", "model", "tokens", "oversized", "optional"):
            altered = copy.deepcopy(auth)
            if mutation == "missing":
                del altered["integration_capacity"]
            elif mutation == "model":
                altered["integration_capacity"]["model"] = "another-model"
            elif mutation == "tokens":
                altered["integration_capacity"]["estimated_input_tokens"] = 1_000_000
            elif mutation == "oversized":
                altered["integration_capacity"]["input_utf8_bytes"] = 100_000_000
            elif mutation == "optional":
                altered["integration_capacity"]["optional_source_bytes"] = 4_000_000
            else:
                altered["integration_capacity"][mutation + "_utf8_bytes"] = 1
            with self.subTest(mutation=mutation), self.assertRaises(workflow.WorkflowError):
                batch.select(self.directory, self.bounds, altered)
        self.assertFalse((self.directory / "batch.json").exists())
        self.assertEqual(self.calls, 2)

    def test_claim_failure_retains_uncertain_slot_and_storage_only_aggregate(self):
        import review_claims

        with patch.object(
            review_claims, "reserve", side_effect=workflow.WorkflowError("claim persistence interrupted")
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "claim persistence"):
                self.execute_batch()
        result = batch.qualification(self.directory)
        self.assertFalse(result["qualified"])
        self.assertEqual(result["units"][0]["state"], "reserved/uncertain")
        self.assertEqual(result["combined_consumption"]["wrapper_reservations"], 1)
        self.assertEqual(self.calls, 2)
        with patch.object(review, "review", side_effect=AssertionError("No inference")):
            batch.execute(self.repo, self.directory, recover_only=True)
        with self.assertRaises(workflow.WorkflowError):
            batch.execute(self.repo, self.directory, resume=True)

    def test_expiry_and_clock_rollback_stop_before_any_native_call(self):
        auth = batch_fixtures.authorize(self.repo, self.directory, self.bounds)
        batch.select(self.directory, self.bounds, auth)
        with patch.object(review, "current_pr"):
            with self.assertRaisesRegex(workflow.WorkflowError, "full remaining time"):
                batch.execute(self.repo, self.directory, clock=lambda: auth["expires_at"] - 1)
            with self.assertRaisesRegex(workflow.WorkflowError, "clock rollback"):
                batch.execute(self.repo, self.directory, clock=iter([1000, 999]).__next__)
        self.assertIsNotNone(batch.state_for(self.directory, batch.load(self.directory))["stop_reason"])
        self.assertEqual(self.calls, 2)

    def test_interrupted_grant_selection_cannot_overwrite_immutable_record(self):
        auth = batch_fixtures.authorize(self.repo, self.directory, self.bounds)
        original = batch.atomic_json

        def interrupted(path, value):
            if path.name == "metadata.json":
                raise OSError("interrupted application metadata")
            return original(path, value)

        with patch.object(batch, "atomic_json", side_effect=interrupted):
            with self.assertRaises(OSError):
                batch.select(self.directory, self.bounds, auth)
        recorded = (self.directory / "batch.json").read_bytes()
        with self.assertRaises(workflow.WorkflowError):
            batch.select(self.directory, self.bounds, {**auth, "name": "replacement grant"})
        self.assertEqual((self.directory / "batch.json").read_bytes(), recorded)
        self.assertEqual(self.calls, 2)
