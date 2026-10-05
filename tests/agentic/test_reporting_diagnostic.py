"""Real local packet/journal/capture, simulated native subprocess and auth only."""

import contextlib
import copy
import json
import subprocess
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import test_reporting_activation as journal_tests
from test_workflow import GitFixture, review, workflow

# isort: split
import claude_native_auth
import claude_reporting_execution as execution
import claude_reporting_policy
import reporting_activation as activation
import reporting_diagnostic as diagnostic
import review_claude
import review_policy
import test_claude_reporting as reporting_fixtures
from claude_fixtures import AUTHENTICATION, native_events
from tasks import atomic_json
from test_claude_reporting import AUXILIARY, LIMITS
from test_claude_v6 import FIXTURE

EXECUTE = review_claude.execute


class ReportingDiagnosticFixture(GitFixture):
    preview = journal_tests.ReportingActivationTests.preview
    apply = journal_tests.ReportingActivationTests.apply

    def setUp(self):
        super().setUp()
        self.repo._info["nameWithOwner"] = "Zi-Deng/FLOW-DC"
        self.commit_task()
        self.addCleanup(patch.stopall)
        patch.object(claude_native_auth, "current_binding", return_value=AUTHENTICATION).start()
        patch.object(claude_native_auth, "store", side_effect=AssertionError("No real credentials")).start()
        patch("review_claude.execute", side_effect=AssertionError("No provider process")).start()
        self.history = {
            "ledger_digest": "a" * 64,
            "files": {"ledger.json": "b" * 64},
            "attempts": list(range(9)),
        }
        patch.object(activation, "historical", side_effect=lambda repo: copy.deepcopy(self.history)).start()
        self.harness = {"head": "a" * 40, "files": {"scripts/agentic/reporting_activation.py": "c" * 64}}
        patch.object(activation, "harness", side_effect=lambda: copy.deepcopy(self.harness)).start()
        task = self.repo.main / ".agentic-local/tasks/issue-31.json"
        task.parent.mkdir(parents=True, exist_ok=True)
        atomic_json(
            task,
            {
                "repository": self.repo.name,
                "key": "issue-31",
                "approval": {
                    "issue": 31,
                    "plan_comment": 6001819615,
                    "contract": activation.CONTRACT,
                    "source": "Synthetic explicit test authority",
                    "recorded_at": "fixture",
                },
            },
        )
        self.policy = claude_reporting_policy.build(
            {
                **review_policy.policy(review_policy.choices("claude-code"), {}, diagnostic=True),
                "authentication": AUTHENTICATION,
            },
            max_turns=80,
            limits=LIMITS,
        )
        self.proposal = self.preview()

        self.apply()
        self.now = 1002
        self.monotonic = 50
        self.calls = 0
        self.prompts = []

    def response(self, args, **kw):
        self.calls += 1
        self.assertGreater(kw["timeout"], 0)
        self.assertLessEqual(kw["timeout"], 300)
        self.assertEqual(kw["env"]["MAX_STRUCTURED_OUTPUT_RETRIES"], "1")
        self.assertEqual(args[args.index("--tools") + 1], "Read,Grep,Glob,StructuredOutput")
        session = args[args.index("--session-id") + 1]
        number = 10 if self.calls == 1 else 11
        packet = activation.root(self.repo) / f"evidence-{number}" / "packet"
        rows = native_events(packet, kw["cwd"], session)
        for row in rows:
            for block in row.get("message", {}).get("content", []):
                if block.get("name") == "Grep":
                    block["input"] = copy.deepcopy(diagnostic.diagnostic_tool_contract.GREP)
        self.prompts.append(args[-1])
        if number == 11:
            outside = kw["cwd"].parent / "outside-refusal-canary.txt"
            self.assertTrue(outside.is_file())
            self.assertIn(str(outside), args[-1])
            f = copy.deepcopy(FIXTURE["positive"])
            for key in ("tool", "advisory", "result"):
                f[key]["session_id"] = session
            f["tool"]["message"]["content"][0]["input"]["file_path"] = str(outside)
            f["denials"][0]["tool_input"]["file_path"] = str(outside)
            message = f"{outside} is outside {kw['cwd']}; --restricted confines the file tools to the working directory."
            f["advisory"]["message"] = message
            f["result"]["message"]["content"][0]["content"] = message
            rows[-1]["permission_denials"] = f["denials"]
            rows[-1:-1] = [f["tool"], f["advisory"], f["result"]]
        body = " \r\n" + rows[-1]["result"] + "\r\n"
        output = reporting_fixtures.ReportingTests().native_rows()[3:]
        output[2]["event"]["delta"]["partial_json"] = body
        output[3]["message"]["content"][0]["input"] = json.loads(body)
        output[-1].update(rows[-1], result=AUXILIARY, structured_output=json.loads(body))
        rows[0]["tools"].append("StructuredOutput")
        rows = rows[:-1] + output
        for row in rows:
            row["session_id"] = session
            row.setdefault("uuid", str(uuid.uuid4()))
        self.now += 2
        self.monotonic += 2
        return subprocess.CompletedProcess(args, 0, "\n".join(json.dumps(r) for r in rows).encode(), b"")

    @contextlib.contextmanager
    def isolated(self, *, process=None, recheck=None):
        @contextlib.contextmanager
        def snapshot(policy):
            self.assertEqual(policy, self.policy)
            with tempfile.TemporaryDirectory() as temporary:
                yield review_claude.environment(Path(temporary)), recheck or (lambda: None)

        with (
            patch.object(review_claude, "execute", side_effect=EXECUTE),
            patch.object(review_claude, "preflight", return_value="/verified/claude") as preflight,
            patch.object(claude_native_auth, "snapshot", side_effect=snapshot),
            patch.object(review_claude.review_cli, "executable", return_value="/verified/claude"),
            patch.object(review_claude.review_process, "capture", side_effect=process or self.response),
            patch.object(diagnostic.time, "time", side_effect=lambda: self.now),
            patch.object(diagnostic.time, "monotonic", side_effect=lambda: self.monotonic),
            patch("review_diagnostics.require_activation", side_effect=AssertionError("No old grant")),
            patch.object(review, "current_pr", side_effect=AssertionError("Diagnostic recovery is offline")),
        ):
            yield preflight


class ReportingDiagnosticTests(ReportingDiagnosticFixture):
    def test_both_purposes_dispatch_once_and_recover_exact_bytes(self):
        with self.isolated() as preflight:
            first = diagnostic.run(self.repo, number=10)
            self.assertTrue(first["qualified"])
            second = diagnostic.run(self.repo, number=11)
            self.assertTrue(second["qualified"])
            self.assertEqual(preflight.call_count, 2)
            self.assertTrue(all(c.kwargs == {"reporting_diagnostic": True} for c in preflight.call_args_list))
        self.assertEqual(self.calls, 2)
        for number in (10, 11):
            directory = activation.root(self.repo) / f"evidence-{number}"
            capture = json.loads((directory / "review-capture.json").read_bytes())
            self.assertEqual((directory / "review.md").read_bytes(), capture["body"].encode())
            self.assertEqual((directory / "terminal.txt").read_bytes(), AUXILIARY.encode())
            self.assertEqual(capture["execution"]["schema_version"], 2)
            self.assertEqual(
                capture["execution"]["prompt_sha256"], review.coverage.checksum(self.prompts[number - 10])
            )
            self.assertFalse(review.coverage_ready(directory))
        with patch.object(activation, "context", side_effect=AssertionError("No auth on recovery")):
            self.assertEqual(diagnostic.run(self.repo, number=10), first)
            self.assertEqual(diagnostic.run(self.repo, number=11), second)
        with self.assertRaises(workflow.WorkflowError):
            diagnostic.run(self.repo, number=12)

    def test_durable_capture_recovers_after_assessment_crash(self):
        with self.isolated(), patch.object(review, "assess_result", side_effect=OSError("Storage failure")):
            with self.assertRaisesRegex(OSError, "Storage failure"):
                diagnostic.run(self.repo, number=10)
        directory = activation.root(self.repo) / "evidence-10"
        before = (directory / "review-capture.json").read_bytes()
        with patch.object(activation, "context", side_effect=AssertionError("No credentials")):
            result = diagnostic.recover(self.repo, number=10)
        self.assertTrue(result["qualified"])
        self.assertEqual(result["finished"], 1004)
        self.assertEqual((directory / "review-capture.json").read_bytes(), before)
        self.assertEqual(self.calls, 1)

    def test_uncertain_process_and_missing_capture_never_repeat(self):
        with self.isolated(process=lambda *a, **kw: (_ for _ in ()).throw(OSError("interrupted"))):
            with self.assertRaises(OSError):
                diagnostic.run(self.repo, number=10)
        with self.assertRaisesRegex(workflow.WorkflowError, "uncertain"):
            diagnostic.run(self.repo, number=10)
        with self.assertRaises((workflow.WorkflowError, FileNotFoundError)):
            diagnostic.run(self.repo, number=11)

    def test_deadline_rechecked_after_auth_immediately_before_launch(self):
        def expire():
            self.now = 1303

        with self.isolated(recheck=expire):
            with self.assertRaisesRegex(workflow.WorkflowError, "deadline"):
                diagnostic.run(self.repo, number=10)
        self.assertEqual(self.calls, 0)
        with self.assertRaisesRegex(workflow.WorkflowError, "uncertain"):
            diagnostic.run(self.repo, number=10)

    def test_native_timeout_is_reduced_by_preparation_elapsed_time(self):
        def delay():
            self.now += 17

        def process(*args, **kw):
            self.assertEqual(kw["timeout"], 283)
            return self.response(*args, **kw)

        with self.isolated(recheck=delay, process=process):
            self.assertTrue(diagnostic.run(self.repo, number=10)["qualified"])

    def test_late_or_failed_process_stops_successor_without_refund(self):
        def process(*args, **kw):
            result = self.response(*args, **kw)
            self.now += 300
            self.monotonic += 300
            return result

        with self.isolated(process=process):
            self.assertFalse(diagnostic.run(self.repo, number=10)["qualified"])
            with self.assertRaisesRegex(workflow.WorkflowError, "stopped"):
                diagnostic.run(self.repo, number=11)
        self.assertEqual(self.calls, 1)

    def test_completion_mutation_invalidates_outcome(self):
        with self.isolated():
            diagnostic.run(self.repo, number=10)
        path = activation.root(self.repo) / "evidence-10" / diagnostic.FINISHED
        data = json.loads(path.read_bytes())
        data["execution_digest"] = "b" * 64
        path.write_text(json.dumps(data))
        with self.assertRaisesRegex(workflow.WorkflowError, "completion binding"):
            activation.outcome(self.repo, 10)

    def test_prepared_packet_is_inert_and_not_a_pr_snapshot(self):
        directory = diagnostic.prepare(self.repo, number=10)
        meta = review.verify_packet(directory)
        self.assertNotIn("pr", meta)
        required = json.loads((directory / "packet/required-material.json").read_bytes())["required"]
        self.assertEqual(len(required), 2)
        self.assertEqual({r["kind"] for r in required}, {"diagnostic", "source"})
        self.assertEqual(meta["head_sha"], self.harness["head"])
        self.assertFalse((activation.root(self.repo) / "attempt-10.json").exists())

    def test_direct_diagnostic_execution_requires_ephemeral_dispatch(self):
        directory = diagnostic.prepare(self.repo, number=10)
        with self.isolated():
            with self.assertRaisesRegex(workflow.WorkflowError, "fresh journal"):
                EXECUTE(self.repo, directory, review.verify_packet(directory), diagnostic=True)
        self.assertEqual(self.calls, 0)

    def test_canary_prompt_path_tampering_fails_recovery(self):
        with self.isolated():
            diagnostic.run(self.repo, number=10)
            diagnostic.run(self.repo, number=11)
        directory = activation.root(self.repo) / "evidence-11"
        path = directory / execution.FILENAME
        data = json.loads(path.read_bytes())
        data["refusal_path"] = "/different/outside-refusal-canary.txt"
        path.write_text(json.dumps(data))
        with self.assertRaisesRegex(workflow.WorkflowError, "execution binding"):
            activation.outcome(self.repo, 11)

    def test_storage_only_capture_cannot_replace_native_execution_record(self):
        with self.isolated():
            diagnostic.run(self.repo, number=10)
        directory = activation.root(self.repo) / "evidence-10"
        capture = json.loads((directory / "review-capture.json").read_bytes())
        capture["execution"]["schema_version"] = 1
        reservation = activation.read(activation.root(self.repo) / "attempt-10.json")
        with self.assertRaisesRegex(workflow.WorkflowError, "completion binding"):
            diagnostic.completion(directory, capture, reservation)

    def test_context_drift_after_preflight_stops_before_process(self):
        def change():
            self.harness["head"] = "b" * 40

        with self.isolated(recheck=change):
            with self.assertRaisesRegex(workflow.WorkflowError, "context changed"):
                diagnostic.run(self.repo, number=10)
        self.assertEqual(self.calls, 0)

    def test_rehashed_packet_cannot_reduce_the_fixed_diagnostic_scope(self):
        directory = diagnostic.prepare(self.repo, number=10)
        path = directory / "packet/authentication-source.txt"
        path.write_text("A smaller synthetic source\n")
        meta = json.loads((directory / "metadata.json").read_bytes())
        meta["files"]["authentication-source.txt"] = review.digest(path)
        atomic_json(directory / "metadata.json", meta)
        with self.isolated():
            with self.assertRaisesRegex(workflow.WorkflowError, "scope differs"):
                diagnostic.run(self.repo, number=10)
        self.assertFalse((activation.root(self.repo) / "attempt-10.json").exists())
        self.assertEqual(self.calls, 0)

    def test_failed_process_retains_exact_partial_report_and_stops(self):
        def process(*args, **kw):
            result = self.response(*args, **kw)
            result.failure_reason = "provider_timeout"
            return result

        with self.isolated(process=process):
            result = diagnostic.run(self.repo, number=10)
            self.assertFalse(result["qualified"])
            with self.assertRaisesRegex(workflow.WorkflowError, "stopped"):
                diagnostic.run(self.repo, number=11)
        directory = activation.root(self.repo) / "evidence-10"
        captured = json.loads((directory / "review-capture.json").read_bytes())
        self.assertIn("provider_timeout", captured["diagnostics"]["reasons"])
        self.assertEqual((directory / "review.md").read_bytes(), captured["body"].encode())
        self.assertEqual(self.calls, 1)

    def test_monotonic_overshoot_cannot_hide_behind_wall_clock(self):
        def process(*args, **kw):
            result = self.response(*args, **kw)
            self.monotonic += 300
            return result

        with self.isolated(process=process):
            self.assertFalse(diagnostic.run(self.repo, number=10)["qualified"])
        self.assertEqual(self.calls, 1)

    def test_missing_usage_is_unknown_and_stops_successor(self):
        def process(*args, **kw):
            result = self.response(*args, **kw)
            rows = [json.loads(row) for row in result.stdout.splitlines()]
            del rows[-1]["total_cost_usd"]
            result.stdout = "\n".join(json.dumps(row) for row in rows).encode()
            return result

        with self.isolated(process=process):
            self.assertFalse(diagnostic.run(self.repo, number=10)["qualified"])
            with self.assertRaisesRegex(workflow.WorkflowError, "stopped"):
                diagnostic.run(self.repo, number=11)
        self.assertEqual(self.calls, 1)

    def test_capture_without_completion_receipt_cannot_repeat(self):
        with self.isolated():
            diagnostic.run(self.repo, number=10)
        directory = activation.root(self.repo) / "evidence-10"
        (directory / diagnostic.FINISHED).unlink()  # Synthetic interruption/mutation only.
        with self.assertRaises((workflow.WorkflowError, FileNotFoundError)):
            diagnostic.run(self.repo, number=10)
        self.assertEqual(self.calls, 1)

    def test_legacy_storage_success_cannot_dispatch_live_successor(self):
        journal_tests.ReportingActivationTests.evidence(self, 10)
        self.assertTrue(activation.complete(self.repo, number=10, now=1050)["qualified"])
        self.now = 1100
        with self.isolated(
            process=lambda *a, **kw: (_ for _ in ()).throw(AssertionError("No live successor"))
        ):
            with self.assertRaisesRegex(workflow.WorkflowError, "diagnostic input"):
                diagnostic.run(self.repo, number=11)
        self.assertEqual(self.calls, 0)

    def test_diagnostic_preflight_does_not_admit_ordinary_v7(self):
        def controls(args, **kw):
            self.assertIn(args[-1], {"--version", "--help"})
            output = "2.1.282 (Claude Code)" if args[-1] == "--version" else " ".join(review_claude.FLAGS)
            return subprocess.CompletedProcess(args, 0, output.encode(), b"")

        with (
            patch.object(review_claude.review_cli, "executable", return_value="/verified/claude"),
            patch.object(review_claude, "check_controls") as checked,
            patch.object(review_claude.review_process, "capture", side_effect=controls) as process,
        ):
            with self.assertRaises(workflow.WorkflowError):
                review_claude.preflight(self.repo, self.policy)
            process.assert_not_called()
            self.assertEqual(
                review_claude.preflight(self.repo, self.policy, reporting_diagnostic=True), "/verified/claude"
            )
            checked.assert_called_once_with(
                "/verified/claude", review_claude.trusted_settings(self.policy), self.policy
            )
            self.assertEqual(process.call_count, 2)
