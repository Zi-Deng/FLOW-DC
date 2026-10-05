"""Synthetic dispatch boundary: no native process or real authentication access."""

import contextlib
import copy
import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

import test_claude_reporting as reporting_fixtures
from claude_fixtures import AUTHENTICATION, native_events
from test_claude_reporting import AUXILIARY, LIMITS
from test_workflow import GitFixture, review, workflow

# isort: split
import claude_native_auth
import review_claude
import review_prompt


class ClaudeExecutionV7Tests(GitFixture):
    def setUp(self):
        super().setUp()
        binding = patch.object(claude_native_auth, "current_binding", return_value=AUTHENTICATION)
        binding.start()
        self.addCleanup(binding.stop)
        self.commit_task()
        self.directory = review.prepare(
            self.repo,
            31,
            12,
            1234,
            review_provider="claude-code",
            reporting={"max_turns": 80, "limits": LIMITS},
        )
        self.packet = self.directory / "packet"
        self.meta = review.verify_packet(self.directory)
        self.policy = self.meta["review_policy"]
        rows = native_events(self.packet, self.packet, "session")
        self.body = " \r\n" + rows[-1]["result"] + "\r\n"
        value = json.loads(self.body)
        reporting_rows = reporting_fixtures.ReportingTests().native_rows()[3:]
        reporting_rows[2]["event"]["delta"]["partial_json"] = self.body
        reporting_rows[3]["message"]["content"][0]["input"] = value
        reporting_rows[-1].update(rows[-1], result=AUXILIARY, structured_output=value)
        rows[0]["tools"].append("StructuredOutput")
        self.rows = rows[:-1] + reporting_rows
        for i, row in enumerate(self.rows):
            row["uuid"] = f"original_{i}"
        self.process_calls = 0

    @contextlib.contextmanager
    def isolated(self, *, failure=None):
        @contextlib.contextmanager
        def snapshot(policy):
            self.assertEqual(policy, self.policy)
            with tempfile.TemporaryDirectory() as temporary:
                yield review_claude.environment(Path(temporary)), lambda: None

        def process(args, **kw):
            self.process_calls += 1
            self.assertEqual(kw["env"].get("MAX_STRUCTURED_OUTPUT_RETRIES"), "1")
            self.assertEqual(kw["timeout"], self.policy["budget"]["timeout_seconds"])
            self.assertEqual(kw["limit"], self.policy["reporting"]["stream_bytes"])
            self.assertEqual(args[args.index("--max-turns") + 1], "80")
            self.assertEqual(args[args.index("--json-schema") + 1], self.policy["reporting"]["schema_text"])
            self.assertIn("StructuredOutput", args[-1])
            self.assertNotIn("final assistant message itself must be JSON-only", args[-1])
            session = args[args.index("--session-id") + 1]
            rows = copy.deepcopy(self.rows)
            for row in rows:
                row["session_id"] = session
            # Native fixture uses packet paths; adapt only ephemeral absolute paths.
            raw = "\n".join(json.dumps(r) for r in rows).replace(str(self.packet), str(kw["cwd"]))
            response = subprocess.CompletedProcess(args, 0, raw.encode(), b"")
            response.failure_reason = failure
            return response

        with (
            patch.object(review_claude, "preflight", return_value="/verified/claude"),
            patch(
                "review_diagnostics.require_activation",
                side_effect=AssertionError("old grant cannot activate v7"),
            ),
            patch("claude_reporting_execution.require_activation"),
            patch.object(claude_native_auth, "snapshot", side_effect=snapshot),
            patch.object(review_claude.review_cli, "executable", return_value="/verified/claude"),
            patch.object(review_claude.review_process, "capture", side_effect=process),
        ):
            yield

    def test_native_boundary_binds_environment_prompt_and_exact_result(self):
        with self.isolated():
            result = review_claude.execute(self.repo, self.directory, self.meta)
        body, diagnostics, version, proof = result
        self.assertEqual(body.encode(), self.body.encode())
        self.assertEqual(version, "2.1.282")
        self.assertEqual(diagnostics["reasons"], [])
        self.assertTrue(proof["accepted"])
        self.assertEqual(self.process_calls, 1)

    def test_reporting_prompt_preserves_all_inspection_obligations(self):
        prompt = review_prompt.native(self.directory, self.meta)
        self.assertIn("exactly one StructuredOutput", prompt)
        self.assertIn("Read(", prompt)
        self.assertIn("Grep(", prompt)
        self.assertIn("Glob(", prompt)
        self.assertIn("EVERY required-material.json entry", prompt)
        self.assertIn("Never strip, reconstruct or infer", prompt)
        self.assertIn(str(self.policy["reporting"]["limits"]["report_bytes"]), prompt)
        self.assertNotIn("final assistant message itself must be JSON-only", prompt)

    def test_direct_duplicate_attempt_stops_before_authentication(self):
        with self.isolated():
            review_claude.execute(self.repo, self.directory, self.meta)
        before = (self.directory / "attempt.json").read_bytes()
        with patch.object(claude_native_auth, "snapshot", side_effect=AssertionError("no auth")):
            with self.assertRaisesRegex(workflow.WorkflowError, "attempt"):
                review_claude.execute(self.repo, self.directory, self.meta)
        self.assertEqual((self.directory / "attempt.json").read_bytes(), before)
        self.assertEqual(self.process_calls, 1)

    def test_runner_saves_exact_proof_auxiliary_and_execution_before_recovery(self):
        import claude_reporting_execution as execution

        with self.isolated(), patch.object(execution, "require_activation"):
            report = review.review(self.repo, self.directory)
        self.assertEqual(report.read_bytes(), self.body.encode())
        self.assertEqual((self.directory / "terminal.txt").read_bytes(), AUXILIARY.encode())
        captured = json.loads((self.directory / "review-capture.json").read_bytes())
        record = json.loads((self.directory / execution.FILENAME).read_bytes())
        self.assertEqual(captured["execution"], record)
        self.assertEqual(captured["reporting"]["report"], self.body)
        self.assertTrue(review.qualification(self.directory)["qualified"])
        self.assertFalse(review.coverage_ready(self.directory))
        with patch.object(review_claude, "execute", side_effect=AssertionError("no replay")):
            self.assertEqual(review.review(self.repo, self.directory), report)
        record["session_id"] = "00000000-0000-0000-0000-000000000000"
        (self.directory / execution.FILENAME).write_text(json.dumps(record))
        with self.assertRaisesRegex(workflow.WorkflowError, "execution binding"):
            review.qualification(self.directory)
        self.assertEqual(self.process_calls, 1)

    def test_failed_process_keeps_exact_report_but_never_qualifies(self):
        import claude_reporting_execution as execution

        with self.isolated(failure="provider_timeout"), patch.object(execution, "require_activation"):
            report = review.review(self.repo, self.directory)
        self.assertEqual(report.read_bytes(), self.body.encode())
        self.assertFalse(review.qualification(self.directory)["qualified"])
        self.assertEqual(self.process_calls, 1)

    def test_capture_survives_assessment_failure_without_a_second_call(self):
        import claude_reporting_execution as execution

        with (
            self.isolated(),
            patch.object(execution, "require_activation"),
            patch.object(review, "assess_result", side_effect=OSError("synthetic storage interruption")),
        ):
            with self.assertRaisesRegex(OSError, "synthetic"):
                review.review(self.repo, self.directory)
        captured = (self.directory / "review-capture.json").read_bytes()
        self.assertFalse((self.directory / "review-result.json").exists())
        with patch.object(review_claude, "execute", side_effect=AssertionError("no replay")):
            review.recover_review(self.repo, self.directory)
        self.assertEqual((self.directory / "review-capture.json").read_bytes(), captured)
        self.assertTrue(review.qualification(self.directory)["qualified"])
        self.assertEqual(self.process_calls, 1)

    def test_claim_without_attempt_is_uncertain_and_cannot_reenter(self):
        import uuid

        import claude_reporting_execution as execution

        record = execution.binding(
            self.directory, self.meta, str(uuid.uuid4()), review_prompt.native(self.directory, self.meta)
        )
        execution.exclusive(self.directory / execution.FILENAME, record)
        with (
            patch.object(review_claude, "preflight", side_effect=AssertionError("no preflight")),
            patch.object(claude_native_auth, "snapshot", side_effect=AssertionError("no auth")),
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "attempt"):
                review_claude.execute(self.repo, self.directory, self.meta)
        with self.assertRaisesRegex(workflow.WorkflowError, "attempt"):
            execution.exclusive(self.directory / execution.FILENAME, record)
        self.assertEqual(self.process_calls, 0)

    def test_no_activation_blocks_before_snapshot_or_provider(self):
        with (
            patch.object(claude_native_auth, "snapshot", side_effect=AssertionError("no auth")),
            patch.object(review_claude, "preflight", side_effect=AssertionError("no provider")),
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "separate reporting activation"):
                review.review(self.repo, self.directory)
        self.assertFalse((self.directory / "attempt.json").exists())

    def test_empty_reporting_retains_auxiliary_separately_and_stops(self):
        self.rows = [self.rows[0], self.rows[-1]]
        with self.isolated():
            report = review.review(self.repo, self.directory)
        self.assertEqual(report.read_bytes(), b"")
        self.assertEqual((self.directory / "terminal.txt").read_bytes(), AUXILIARY.encode())
        self.assertFalse(review.qualification(self.directory)["qualified"])
        with patch.object(review_claude, "execute", side_effect=AssertionError("no replay")):
            self.assertEqual(review.review(self.repo, self.directory), report)
        self.assertEqual(self.process_calls, 1)

    def test_interrupted_dispatch_is_consumed_without_capture_or_retry(self):
        with (
            self.isolated(),
            patch.object(review_claude.review_process, "capture", side_effect=OSError("interrupted")),
        ):
            with self.assertRaisesRegex(OSError, "interrupted"):
                review.review(self.repo, self.directory)
        self.assertTrue((self.directory / "attempt.json").exists())
        self.assertFalse((self.directory / "review-capture.json").exists())
        with self.isolated():
            with self.assertRaisesRegex(workflow.WorkflowError, "Prior review attempt"):
                review.review(self.repo, self.directory)
        self.assertEqual(self.process_calls, 0)

    def test_attempt_mutations_fail_after_positive_saved_result(self):
        with self.isolated():
            review.review(self.repo, self.directory)
        self.assertTrue(review.qualification(self.directory)["qualified"])
        path = self.directory / "attempt.json"
        original = path.read_bytes()
        for field, value in [
            ("requests", True),
            ("execution_sha256", "0" * 64),
            ("input_digest", "0" * 64),
            ("status", "approved"),
        ]:
            row = json.loads(original)
            row[field] = value
            path.write_text(json.dumps(row))
            with self.subTest(field=field):
                with self.assertRaisesRegex(workflow.WorkflowError, "attempt differs"):
                    review.qualification(self.directory)
        path.write_bytes(original)
        path = self.directory / "reporting-execution.json"
        original = path.read_bytes()
        path.unlink()
        with self.assertRaisesRegex(workflow.WorkflowError, "execution binding"):
            review.qualification(self.directory)
        path.write_bytes(original)
        self.assertTrue(review.qualification(self.directory)["qualified"])
