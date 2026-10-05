"""Admission over real local records and simulated native dispatch, never live proof."""

import copy
import json
from pathlib import Path
from unittest.mock import patch

from test_workflow import review, workflow

# isort: split
import claude_native_auth
import claude_reporting_execution as execution
import reporting_activation as activation
import reporting_admission as admission
import reporting_diagnostic as diagnostic
from tasks import atomic_json, digest
from test_reporting_diagnostic import ReportingDiagnosticFixture


class ReportingAdmissionTests(ReportingDiagnosticFixture):
    def qualify(self):
        with self.isolated():
            for number in (10, 11):
                self.assertTrue(diagnostic.run(self.repo, number=number)["qualified"])

    def test_both_native_purposes_admit_matching_policy(self):
        self.qualify()
        result = execution.require_activation(self.repo, {"schema_version": 7, "review_policy": self.policy})
        self.assertEqual(result["outcomes"].keys(), {"10", "11"})
        self.assertTrue(result["current_generation_live_tested"])
        self.assertEqual(self.calls, 2)

    def test_neither_one_nor_uncertain_second_purpose_admits(self):
        with self.assertRaises((workflow.WorkflowError, OSError)):
            admission.check(self.repo, self.policy)
        with self.isolated():
            diagnostic.run(self.repo, number=10)
        with self.assertRaises((workflow.WorkflowError, OSError)):
            admission.check(self.repo, self.policy)
        directory = diagnostic.prepare(self.repo, number=11)
        meta = review.verify_packet(directory)
        activation.reserve(self.repo, number=11, input_digest=digest(meta), now=self.now)
        with self.assertRaises((workflow.WorkflowError, OSError)):
            admission.check(self.repo, self.policy)
        self.assertEqual(self.calls, 1)

    def test_each_retained_artifact_mutation_blocks_admission(self):
        self.qualify()
        self.assertTrue(admission.check(self.repo, self.policy)["outcomes"])
        for number in (10, 11):
            directory = activation.root(self.repo) / f"evidence-{number}"
            for name in (
                "review.md",
                "terminal.txt",
                "review-capture.json",
                "reporting-proof.json",
                "diagnostics.json",
                "coverage.json",
                "reporting-execution.json",
                "reporting-finished.json",
                "attempt.json",
                "metadata.json",
                "packet/authentication-source.txt",
            ):
                with self.subTest(number=number, name=name):
                    path = directory / name
                    original = path.read_bytes()
                    path.write_bytes(original + b"X")
                    try:
                        with self.assertRaises((workflow.WorkflowError, OSError, ValueError)):
                            admission.check(self.repo, self.policy)
                    finally:
                        path.write_bytes(original)
        self.assertTrue(admission.check(self.repo, self.policy)["outcomes"])

    def test_saved_success_without_native_completion_cannot_admit(self):
        self.qualify()
        directory = activation.root(self.repo) / "evidence-10"
        (directory / diagnostic.FINISHED).unlink()
        with self.assertRaises((workflow.WorkflowError, OSError)):
            admission.check(self.repo, self.policy)

    def test_reporting_turn_retention_model_and_harness_drift_refuse(self):
        self.qualify()
        for key, value in (("max_turns", 81), ("retry_limit", 2), ("schema_text", "{}")):
            changed = copy.deepcopy(self.policy)
            changed["reporting"][key] = value
            with self.subTest(key=key), self.assertRaises(workflow.WorkflowError):
                admission.check(self.repo, changed)
        changed = copy.deepcopy(self.policy)
        changed["reporting"]["limits"]["report_bytes"] -= 1
        with self.assertRaises(workflow.WorkflowError):
            admission.check(self.repo, changed)
        self.harness["files"]["scripts/agentic/reporting_activation.py"] = "d" * 64
        with self.assertRaisesRegex(workflow.WorkflowError, "source changed"):
            admission.check(self.repo, self.policy)

    def test_renewal_requires_separate_verified_lineage_without_replaying_calls(self):
        self.qualify()
        renewed = copy.deepcopy(self.policy)
        renewed["authentication"]["generation_id"] = "33333333-3333-4333-8333-333333333333"
        before = (activation.root(self.repo) / "grant.json").read_bytes()
        with patch.object(claude_native_auth, "capability_lineage", return_value=False):
            with self.assertRaisesRegex(workflow.WorkflowError, "lineage"):
                admission.check(self.repo, renewed)
        with patch.object(claude_native_auth, "capability_lineage", return_value=True) as lineage:
            result = admission.check(self.repo, renewed)
            self.assertFalse(result["current_generation_live_tested"])
            lineage.assert_called_once_with(
                self.policy["authentication"], renewed["authentication"], renewed["budget"]["timeout_seconds"]
            )
        self.assertEqual(before, (activation.root(self.repo) / "grant.json").read_bytes())
        self.assertEqual(self.calls, 2)

    def test_expired_dispatch_window_does_not_repeat_or_erase_completed_capability(self):
        self.qualify()
        with patch.object(activation.time, "time", return_value=3000):
            self.assertTrue(admission.check(self.repo, self.policy)["outcomes"])
            with self.assertRaises(workflow.WorkflowError):
                activation.reserve(self.repo, number=11, input_digest="f" * 64)

    def test_storage_recovery_remains_independent_of_admission(self):
        self.qualify()
        directory = activation.root(self.repo) / "evidence-10"
        with patch.object(admission, "check", side_effect=AssertionError("No admission on recovery")):
            self.assertTrue(diagnostic.recover(self.repo, number=10)["qualified"])
            review.qualification(directory)
            self.assertFalse(review.coverage_ready(directory))

    def test_source_continuity_requires_exact_merged_head_and_real_git_ancestry(self):
        first = self.repo.git("rev-parse", "HEAD")
        original = {"head": first, "files": {"script.py": "a" * 64}}
        self.repo.git("commit", "--allow-empty", "-m", "Synthetic human merge receipt")
        merged = self.repo.git("rev-parse", "HEAD")
        self.repo.git("commit", "--allow-empty", "-m", "Unchanged workflow descendant")
        current = {**original, "head": self.repo.git("rev-parse", "HEAD")}
        pr = {
            "merged": True,
            "head": {"sha": first},
            "base": {"repo": {"full_name": self.repo.name}},
            "merge_commit_sha": merged,
        }
        with patch.object(self.repo, "api", return_value=pr):
            admission.source_continuity(self.repo, original, current)
        for key, value in (("merged", False), ("head", {"sha": merged}), ("merge_commit_sha", "f" * 40)):
            with self.subTest(key=key), patch.object(self.repo, "api", return_value={**pr, key: value}):
                with self.assertRaises(workflow.WorkflowError):
                    admission.source_continuity(self.repo, original, current)

    def test_ordinary_packet_needs_immutable_admission_and_execution_binding(self):
        self.qualify()
        directory = review.prepare(
            self.repo,
            31,
            12,
            1234,
            review_provider="claude-code",
            reporting={"max_turns": 80, "limits": self.policy["reporting"]["limits"]},
        )
        meta = review.verify_packet(directory)
        # Match the independently authorized ordinary budget as well.
        record = admission.retain(self.repo, directory, meta)
        with self.assertRaisesRegex(workflow.WorkflowError, "execution lacks"):
            admission.require_packet(directory, meta, repo=self.repo)
        import uuid

        import review_prompt

        attempt = {
            "schema_version": 7,
            "input_digest": digest(meta),
            "policy_digest": digest(meta["review_policy"]),
            "status": "started",
            "requests": 1,
        }
        execution.reserve(directory, meta, str(uuid.uuid4()), review_prompt.native(directory, meta), attempt)
        self.assertEqual(admission.require_packet(directory, meta, repo=self.repo), record)
        altered = copy.deepcopy(record)
        altered["outcomes"]["10"] = "f" * 64
        atomic_json(Path(directory) / admission.FILENAME, altered)
        with self.assertRaisesRegex(workflow.WorkflowError, "admission"):
            admission.require_packet(directory, meta, repo=self.repo)

    def ordinary_response(self, args, **kw):
        import subprocess
        import uuid

        import test_claude_reporting as fixtures
        from claude_fixtures import native_events

        self.calls += 1
        session = args[args.index("--session-id") + 1]
        rows = native_events(self.ordinary / "packet", kw["cwd"], session)
        body = " \r\n" + rows[-1]["result"] + "\r\n"
        output = fixtures.ReportingTests().native_rows()[3:]
        output[2]["event"]["delta"]["partial_json"] = body
        output[3]["message"]["content"][0]["input"] = json.loads(body)
        output[-1].update(rows[-1], result=fixtures.AUXILIARY, structured_output=json.loads(body))
        rows[0]["tools"].append("StructuredOutput")
        rows = rows[:-1] + output
        for row in rows:
            row["session_id"] = session
            row.setdefault("uuid", str(uuid.uuid4()))
        return subprocess.CompletedProcess(args, 0, "\n".join(json.dumps(r) for r in rows).encode(), b"")

    def ordinary_packet(self):
        self.ordinary = review.prepare(
            self.repo,
            31,
            12,
            1234,
            review_provider="claude-code",
            reporting={"max_turns": 80, "limits": self.policy["reporting"]["limits"]},
        )
        self.policy = review.verify_packet(self.ordinary)["review_policy"]

    def test_ordinary_native_capture_readiness_publication_and_drift(self):
        self.qualify()
        self.ordinary_packet()
        with self.isolated(process=self.ordinary_response), patch.object(review, "current_pr"):
            review.review(self.repo, self.ordinary)
        with patch.object(workflow, "Repo", return_value=self.repo):
            self.assertTrue(review.qualification(self.ordinary, require=True)["qualified"])
            self.assertTrue(review.coverage_ready(self.ordinary))
            envelope = review.publication_body(self.ordinary)
            exact = (self.ordinary / "review.md").read_bytes().decode()
            self.assertIn(exact, envelope)
            self.assertIn("coverage-qualified static inspection", envelope)
            self.harness["files"]["scripts/agentic/reporting_activation.py"] = "d" * 64
            self.assertFalse(review.coverage_ready(self.ordinary))
            with self.assertRaisesRegex(workflow.WorkflowError, "source changed"):
                review.qualification(self.ordinary, require=True)
            self.assertEqual(review.publication_body(self.ordinary), envelope)
        with patch.object(admission, "check", side_effect=AssertionError("No admission on offline recovery")):
            self.assertEqual(review.recover_review(self.repo, self.ordinary), self.ordinary / "review.md")
        self.assertEqual(self.calls, 3)

    def test_last_moment_harness_drift_stops_before_ordinary_launch(self):
        self.qualify()
        self.ordinary_packet()

        def changed():
            self.harness["files"]["scripts/agentic/reporting_activation.py"] = "d" * 64

        with (
            self.isolated(process=self.ordinary_response, recheck=changed),
            patch.object(review, "current_pr"),
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "source changed"):
                review.review(self.repo, self.ordinary)
        self.assertEqual(self.calls, 2)
        self.assertTrue((self.ordinary / execution.FILENAME).is_file())
        with self.assertRaises(workflow.WorkflowError):
            review.review(self.repo, self.ordinary)
        self.assertEqual(self.calls, 2)

    def test_current_preflight_refuses_missing_activation_before_binary_or_auth(self):
        import review_claude

        with patch.object(review_claude.review_cli, "executable", side_effect=AssertionError("No binary")):
            with self.assertRaises(workflow.WorkflowError):
                review_claude.preflight(self.repo, self.policy)
