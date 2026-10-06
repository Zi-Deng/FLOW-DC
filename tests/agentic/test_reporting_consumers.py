"""Explicit reporting selection and managed preparation; no external operations."""

import copy
import json
from pathlib import Path
from unittest.mock import patch

from test_pipeline import PipelineFixture
from test_workflow import workflow

# isort: split
import claude_reporting_policy
import pipeline
import reporting_cli
import review
from test_claude_reporting import LIMITS


class ReportingConsumerTests(PipelineFixture):
    def profile(self):
        return {"max_turns": 80, "limits": copy.deepcopy(LIMITS)}

    def test_managed_preparation_binds_explicit_profile_and_requires_fresh_on_drift(self):
        value = pipeline.review_task(self.repo, 12, review_provider="claude-code", reporting=self.profile())
        directory = Path(value["directory"])
        meta = review.verify_packet(directory)
        self.assertEqual(meta["schema_version"], 7)
        self.assertEqual(meta["review_policy"]["reporting"]["max_turns"], 80)
        changed = self.profile()
        changed["max_turns"] = 81
        with self.assertRaisesRegex(workflow.WorkflowError, "fresh"):
            pipeline.review_task(self.repo, 12, review_provider="claude-code", reporting=changed)
        with patch("review_claude.execute", side_effect=AssertionError("No provider")):
            with self.assertRaisesRegex(workflow.WorkflowError, "separate reporting activation"):
                pipeline.review_task(
                    self.repo, 12, execute=True, review_provider="claude-code", reporting=self.profile()
                )
        self.assertFalse((directory / "attempt.json").exists())

    def test_native_config_profile_does_not_change_explicit_copilot(self):
        config = self.repo.root / ".agentic/config.json"
        data = json.loads(config.read_bytes())
        data["review_reporting"] = self.profile()
        config.write_text(json.dumps(data))
        native = review.prepare(self.repo, 31, 12, 1234, review_provider="claude-code")
        self.assertEqual(review.verify_packet(native)["schema_version"], 7)
        copilot = review.prepare(self.repo, 31, 12, 1234, review_provider="copilot")
        self.assertEqual(review.verify_packet(copilot)["schema_version"], 6)
        self.assertNotIn("reporting", review.verify_packet(copilot)["review_policy"])
        with self.assertRaisesRegex(workflow.WorkflowError, "Claude Code"):
            review.prepare(self.repo, 31, 12, 1234, review_provider="copilot", reporting=self.profile())

    def test_profile_file_is_bounded_and_strict(self):
        path = self.parent / "reporting.json"
        path.write_text(json.dumps(self.profile()))
        self.assertEqual(claude_reporting_policy.read_selection(path), self.profile())
        path.write_text('{"max_turns":80,"max_turns":81,"limits":{}}')
        with self.assertRaises(workflow.WorkflowError):
            claude_reporting_policy.read_selection(path)

    def test_diagnostic_cli_requires_one_explicit_number_and_never_chains(self):
        args = reporting_cli.parser().parse_args(["run", "--number", "12"])
        with (
            patch.object(self.repo, "assert_main") as guard,
            patch.object(reporting_cli.reporting_diagnostic, "run", return_value={"qualified": True}) as run,
        ):
            self.assertEqual(reporting_cli.dispatch(self.repo, args), {"qualified": True})
            guard.assert_called_once_with()
            run.assert_called_once_with(self.repo, number=12)
        with self.assertRaises(SystemExit):
            reporting_cli.parser().parse_args(["run", "--number", "14"])

    def test_cli_apply_needs_exact_preview_digest_and_does_not_run_diagnostics(self):
        path = self.parent / "preview.json"
        path.write_text('{"status":"preview"}')
        args = reporting_cli.parser().parse_args(
            ["apply", "--preview", str(path), "--preview-digest", "a" * 64]
        )
        with (
            patch.object(self.repo, "assert_main"),
            patch.object(reporting_cli.activation, "apply", return_value={"status": "applied"}) as apply,
            patch.object(reporting_cli.reporting_diagnostic, "run", side_effect=AssertionError("No chain")),
        ):
            self.assertEqual(reporting_cli.dispatch(self.repo, args), {"status": "applied"})
            apply.assert_called_once_with(self.repo, {"status": "preview"}, preview_digest="a" * 64)

    def test_managed_report_record_rejects_v7_policy_drift(self):
        from tasks import TaskStore

        value = pipeline.review_task(self.repo, 12, review_provider="claude-code", reporting=self.profile())
        directory = Path(value["directory"])
        state = TaskStore(self.repo).read("issue-12")
        record = state["review_rounds"][-1]
        record["run_attempted"] = True
        meta = review.verify_packet(directory)
        (directory / "review.md").write_bytes(b"boundary fixture only")
        meta.update(provider_version="2.1.282", review_sha256=review.digest(directory / "review.md"))
        with (
            patch.object(review, "verify_packet", return_value=meta),
            patch.object(review, "publication_body", return_value="exact envelope boundary"),
        ):
            self.assertEqual(pipeline.report_record(self.repo, state, record)[1], "exact envelope boundary")
            changed = copy.deepcopy(record)
            changed["review_policy"]["reporting"]["max_turns"] = 81
            with self.assertRaisesRegex(workflow.WorkflowError, "provider policy"):
                pipeline.report_record(self.repo, state, changed)
