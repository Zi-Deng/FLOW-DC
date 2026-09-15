"""Recover paid review output after write failures without a second model request."""

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

from test_workflow import SOURCE, GitFixture, git, review, workflow

HELP = (
    "--available-tools --no-custom-instructions --disable-builtin-mcps "
    "--no-remote-export --no-ask-user --usage-output-file --max-ai-credits"
)


class ReviewRecoveryTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.model_calls = 0
        self.version_fails = False
        self.model_report = "No material findings. Unicode café.\n"

    def packet(self):
        self.commit_task()
        (self.task_path / "code.py").write_text('value = "café"\n', encoding="utf-8")
        git(self.task_path, "commit", "-am", "unicode review fixture")
        self.head = git(self.task_path, "rev-parse", "HEAD")
        self.pr_data["head"]["sha"] = self.head
        git(self.task_path, "push", "origin", "HEAD:refs/pull/31/head")
        return review.prepare(self.repo, 31, 12, 1234)

    def invoke(self, directory):
        original = review.run

        def cli(args, **kwargs):
            if args[0] != "copilot":
                return original(args, **kwargs)
            if args[1] == "--help":
                return subprocess.CompletedProcess(args, 0, HELP, "")
            if args[1] == "--version":
                if self.version_fails:
                    raise workflow.WorkflowError("Version probe failed")
                return subprocess.CompletedProcess(args, 0, "Copilot test double\n", "")
            self.model_calls += 1
            return subprocess.CompletedProcess(args, 0, self.model_report, "")

        with (
            patch.dict(os.environ, {"COPILOT_GITHUB_TOKEN": "fake-test-token"}),
            patch.object(review, "run", side_effect=cli),
        ):
            return review.review(self.repo, directory)

    def test_version_failure_happens_before_any_paid_request(self):
        directory = self.packet()
        self.version_fails = True
        with self.assertRaisesRegex(workflow.WorkflowError, "Version probe"):
            self.invoke(directory)
        self.assertEqual(self.model_calls, 0)
        self.assertFalse((directory / "review-result.json").exists())

    def test_metadata_failure_recovers_and_publishes_exact_output_without_model(self):
        directory = self.packet()
        original = review.atomic_json

        def fail_metadata(path, value):
            if Path(path).name == "metadata.json":
                raise OSError("Injected metadata write failure")
            return original(path, value)

        with patch.object(review, "atomic_json", side_effect=fail_metadata):
            with self.assertRaisesRegex(OSError, "metadata write"):
                self.invoke(directory)
        original_bytes = (directory / "review.md").read_bytes()
        self.assertIsNone(review.verify_packet(directory).get("review_sha256"))
        with patch.object(review, "run", side_effect=AssertionError("No CLI call allowed during recovery")):
            report = review.review(self.repo, directory)
            review.publish(self.repo, directory)
        self.assertEqual(self.model_calls, 1)
        self.assertEqual(report.read_bytes(), original_bytes)
        self.assertEqual(review.digest(report), review.verify_packet(directory)["review_sha256"])
        self.assertEqual(self.posts[-1][1]["commit_id"], self.head)
        self.assertIn("café", self.posts[-1][1]["body"])

    def test_report_write_failure_recovers_from_durable_journal(self):
        directory = self.packet()
        with patch.object(review, "atomic_text", side_effect=OSError("Injected report write failure")):
            with self.assertRaisesRegex(OSError, "report write"):
                self.invoke(directory)
        self.assertFalse((directory / "review.md").exists())
        result = json.loads((directory / "review-result.json").read_text(encoding="utf-8"))
        with patch.object(review, "run", side_effect=AssertionError("No CLI call allowed during recovery")):
            report = review.review(self.repo, directory)
        self.assertEqual(report.read_text(encoding="utf-8"), result["body"])
        self.assertEqual(self.model_calls, 1)

    def test_completed_result_is_reused_but_changed_report_is_rejected(self):
        directory = self.packet()
        report = self.invoke(directory)
        self.invoke(directory)
        self.assertEqual(self.model_calls, 1)
        report.write_text("Changed completed report\n", encoding="utf-8")
        with self.assertRaisesRegex(workflow.WorkflowError, "changed"):
            self.invoke(directory)
        self.assertEqual(self.model_calls, 1)
        self.assertEqual(report.read_text(encoding="utf-8"), "Changed completed report\n")

    def test_changed_journal_or_packet_binding_cannot_be_recovered(self):
        directory = self.packet()
        self.invoke(directory)
        path = directory / "review-result.json"
        original = json.loads(path.read_text(encoding="utf-8"))
        for key, value in [("body", "Changed result"), ("input_digest", "0" * 64), ("schema_version", True)]:
            path.write_text(json.dumps({**original, key: value}), encoding="utf-8")
            with self.subTest(key=key), self.assertRaisesRegex(workflow.WorkflowError, "changed"):
                review.recover_review(self.repo, directory)
        self.assertEqual(self.model_calls, 1)

    def test_recovery_rejects_stale_head(self):
        directory = self.packet()
        with patch.object(review, "atomic_text", side_effect=OSError("Injected report write failure")):
            with self.assertRaises(OSError):
                self.invoke(directory)
        self.pr_data["head"]["sha"] = "a" * 40
        with self.assertRaisesRegex(workflow.WorkflowError, "changed"):
            review.recover_review(self.repo, directory)
        self.assertFalse((directory / "review.md").exists())
        self.assertEqual(self.model_calls, 1)

    def test_oversized_report_is_preserved_without_publication_or_retry(self):
        directory = self.packet()
        self.model_report = "x" * 61_000
        report = self.invoke(directory)
        original = report.read_bytes()
        with self.assertRaisesRegex(workflow.WorkflowError, "budget"):
            review.publish(self.repo, directory)
        self.assertEqual(report.read_bytes(), original)
        self.assertEqual(self.model_calls, 1)
        self.assertEqual(self.posts, [])

    def test_review_lifecycle_under_explicit_ascii_process_locale(self):
        code = """
from test_review_recovery import ReviewRecoveryTests
from test_workflow import review, workflow
import sys
assert sys.flags.utf8_mode == 0
case = ReviewRecoveryTests()
try:
    case.setUp()
    directory = case.packet()
    report = case.invoke(directory)
    review.publish(case.repo, directory)
    assert '\\u00e9' in report.read_text(encoding='utf-8')
    command = 'import sys; sys.stdout.buffer.write(bytes([195, 169]))'
    assert workflow.run([sys.executable, '-c', command]).stdout == '\\u00e9'
finally:
    case.doCleanups()
"""
        env = {
            **os.environ,
            "LC_ALL": "C",
            "PYTHONUTF8": "0",
            "PYTHONCOERCECLOCALE": "0",
            "PYTHONPATH": str(SOURCE / "tests/agentic"),
        }
        result = subprocess.run([sys.executable, "-B", "-c", code], capture_output=True, env=env)
        self.assertEqual(result.returncode, 0, result.stderr.decode("utf-8", errors="replace"))
