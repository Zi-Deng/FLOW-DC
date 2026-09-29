"""Readiness and exact-output regressions established before issue #29's gates."""

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import test_pipeline
import test_review_recovery
from test_workflow import GitFixture, review, workflow


class CoverageRegressionTests(GitFixture):
    def test_plain_comment_and_green_checks_cannot_satisfy_preflight(self):
        self.commit_task()
        self.reviews = [{"commit_id": self.head, "state": "COMMENTED", "body": "Looks fine"}]
        original = workflow.run

        def checks(args, **kwargs):
            if args[:3] == ["gh", "pr", "checks"]:
                return subprocess.CompletedProcess(
                    args, 0, json.dumps([{"name": "quality", "bucket": "pass"}]), ""
                )
            return original(args, **kwargs)

        with patch.object(workflow, "run", side_effect=checks):
            with self.assertRaises(workflow.WorkflowError):
                workflow.merge_preflight(self.repo, 31, self.head)

    def test_exact_model_output_is_not_stripped_or_prefixed(self):
        case = test_review_recovery.ReviewRecoveryTests()
        self.addCleanup(case.doCleanups)
        case.setUp()
        case.model_report = "  Unicode café\r\n\r\ntrailing spaces  \r\n"
        report = case.invoke(case.packet())
        self.assertEqual(report.read_bytes(), case.model_report.encode("utf-8"))

    def test_unsigned_nonempty_report_cannot_designate_readiness(self):
        case = test_pipeline.PipelineFixture()
        self.addCleanup(case.doCleanups)
        case.setUp()
        import pipeline

        def unsigned(repo, directory):
            directory = Path(directory)
            report = directory / "review.md"
            report.write_text("No findings. Unsigned legacy-style report.\n")
            meta = review.verify_packet(directory)
            meta.update(review_sha256=review.digest(report), copilot_version="1.0.83")
            workflow.write_json(directory / "metadata.json", meta)
            return report

        with patch.object(review, "review", side_effect=unsigned):
            with self.assertRaises(workflow.WorkflowError):
                pipeline.review_task(case.repo, 12, execute=True, publish=True)
