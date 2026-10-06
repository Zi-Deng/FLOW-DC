"""Current finite v5 recovery; synthetic evidence never qualifies a provider."""

import contextlib
import copy
import hashlib
import json
import subprocess
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import claude_native_auth as auth
import claude_owned_auth
import claude_reporting_execution as execution
import reporting_activation_v2 as oldest
import reporting_activation_v4 as old
import reporting_activation_v5 as activation
import reporting_admission as admission
import reporting_cli
import reporting_diagnostic_v4 as old_diagnostic
import reporting_diagnostic_v5 as diagnostic
import reporting_recovery_history_v5 as history
import review_claude
import test_claude_reporting as reporting_fixtures
from claude_fixtures import native_events
from tasks import atomic_json, digest
from test_claude_reporting import AUXILIARY
from test_claude_v6 import FIXTURE
from test_reporting_diagnostic import EXECUTE
from test_reporting_owned_auth import BINDING, SNAPSHOT, STORE
from test_reporting_preflight import controls
from test_reporting_recovery_v4 import RecoveryFixture as PreviousFixture
from test_workflow import review, workflow

CURRENT_CHECK = admission.check


class CurrentCliTests(unittest.TestCase):
    def test_current_cli_accepts_only_new_pair(self):
        for operation in ("prepare", "run", "recover"):
            for number in (18, 19):
                args = reporting_cli.parser().parse_args([operation, "--number", str(number)])
                self.assertEqual(args.number, number)
                self.assertEqual(args.command, operation)


class RecoveryFixture(PreviousFixture):
    def setUp(self):
        self.v5_ready = False
        super().setUp()
        directory = old_diagnostic.prepare(self.repo, number=16)
        meta = review.verify_packet(directory)
        old.reserve(self.repo, number=16, input_digest=digest(meta), now=self.now)
        self.old_v4 = {str(p): p.read_bytes() for p in old.root(self.repo).rglob("*") if p.is_file()}
        state = old.read(self.task)
        old_approval = state["approval"]
        state["approval_history"].append(old_approval)
        state["approval"] = {
            **old_approval,
            "plan_comment": activation.CONTRACT["plan_comment"],
            "contract": activation.CONTRACT,
        }
        atomic_json(self.task, state)
        patch.object(history, "V4_GRANT", digest(old.load(self.repo)[0])).start()
        patch.object(history, "V4_APPROVAL", digest(old_approval)).start()
        patch.object(history, "V4_TREE", history.tree(old.root(self.repo))).start()
        body = "Synthetic stopped v4 reservation: usage unknown, no capture or outcome."
        patch.object(history, "PUBLIC_BODY_SHA256", hashlib.sha256(body.encode()).hexdigest()).start()
        path = self.repo.main / ".agentic-local" / history.PUBLIC_SNAPSHOT
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_json(path, {"conversation": [{"id": history.PUBLIC_COMMENT, "body": body}]})
        patch.object(activation, "harness", side_effect=lambda: copy.deepcopy(self.harness)).start()
        patch("reporting_admission.check", side_effect=CURRENT_CHECK).start()
        self.proposal = activation.preview(
            self.repo,
            self.policy,
            name="synthetic-recovery-v5",
            tested_head=self.harness["head"],
            expires_at=2200,
            now=self.now,
        )
        activation.apply(
            self.repo, self.proposal, preview_digest=self.proposal["preview_digest"], now=self.now
        )
        self.response_activation, self.first_number, self.calls = activation, 18, 0
        self.v5_ready = True

    @contextlib.contextmanager
    def isolated(self, **kwargs):
        if not getattr(self, "v5_ready", False):
            with super().isolated(**kwargs) as value:
                yield value
        else:
            with self.real_preflight(**kwargs) as value:
                yield value

    @contextlib.contextmanager
    def real_preflight(self, *, process=None):
        def capture(args, **kwargs):
            if args[-1] in ("--version", "--help"):
                return controls(args, **kwargs)
            with self.assertRaisesRegex(workflow.WorkflowError, "registration is busy"):
                BINDING(300)
            return (process or self.response)(args, **kwargs)

        with (
            patch.object(review_claude, "execute", EXECUTE),
            patch.object(claude_owned_auth, "snapshot", SNAPSHOT),
            patch.object(review_claude.review_cli, "executable", return_value="/synthetic/claude"),
            patch.object(review_claude, "check_controls") as checked,
            patch.object(review_claude.review_process, "capture", side_effect=capture) as captured,
            patch.object(time, "time", side_effect=lambda: self.now),
            patch.object(time, "monotonic", side_effect=lambda: self.monotonic),
        ):
            yield checked, captured

    def response(self, args, **kw):
        if self.response_activation is not activation:
            return super().response(args, **kw)
        self.calls += 1
        self.assertGreater(kw["timeout"], 0)
        self.assertLessEqual(kw["timeout"], 300)
        self.assertEqual(kw["env"]["MAX_STRUCTURED_OUTPUT_RETRIES"], "1")
        self.assertEqual(args[args.index("--tools") + 1], "Read,Grep,Glob,StructuredOutput")
        session = args[args.index("--session-id") + 1]
        number = self.first_number if self.calls == 1 else self.first_number + 1
        packet = self.response_activation.root(self.repo) / f"evidence-{number}" / "packet"
        rows = native_events(packet, kw["cwd"], session)
        for row in rows:
            for block in row.get("message", {}).get("content", []):
                if block.get("name") == "Grep":
                    block["input"] = copy.deepcopy(diagnostic.diagnostic_tool_contract.GREP)
        self.prompts.append(args[-1])
        if number == self.first_number:
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

    def qualify(self):
        with self.real_preflight():
            for number in (18, 19):
                self.assertTrue(diagnostic.run(self.repo, number=number)["qualified"])


class RecoveryTests(RecoveryFixture):
    def test_complete_pair_real_preflight_and_offline_recovery(self):
        with self.real_preflight() as (checked, captured):
            for number in (18, 19):
                self.assertTrue(diagnostic.run(self.repo, number=number)["qualified"])
        self.assertEqual((self.calls, checked.call_count, captured.call_count), (2, 2, 6))
        self.assertEqual(admission.check(self.repo, self.policy)["schema_version"], 5)
        with patch.object(review_claude.review_process, "capture", side_effect=AssertionError("No replay")):
            for number in (18, 19):
                self.assertTrue(diagnostic.recover(self.repo, number=number)["qualified"])
            with self.assertRaisesRegex(workflow.WorkflowError, "uncertain"):
                old_diagnostic.recover(self.repo, number=16)
        for path, raw in self.old_v4.items():
            self.assertEqual(Path(path).read_bytes(), raw)
        for number in (10, 11, 12, 13, 14, 15, 16, 17, 20, True):
            with self.assertRaises(workflow.WorkflowError):
                diagnostic.prepare(self.repo, number=number)

    def test_isolation_first_pair_and_offline_recovery_preserve_stops(self):
        def process(*args, **kwargs):
            with self.assertRaisesRegex(workflow.WorkflowError, "registration is busy"):
                BINDING(300)
            return self.response(*args, **kwargs)

        with self.isolated(process=process):
            self.assertTrue(diagnostic.run(self.repo, number=18)["qualified"])
            with self.assertRaises((workflow.WorkflowError, OSError)):
                admission.check(self.repo, self.policy)
            self.assertTrue(diagnostic.run(self.repo, number=19)["qualified"])
        with SNAPSHOT(self.policy) as handle:
            self.assertEqual(
                set(admission.check(self.repo, self.policy, owned_auth=handle)["outcomes"]), {"18", "19"}
            )
        with (
            patch.object(auth, "store", side_effect=AssertionError("offline")),
            patch.object(activation, "context", side_effect=AssertionError("offline")),
        ):
            for number in (18, 19):
                self.assertTrue(diagnostic.recover(self.repo, number=number)["qualified"])
                self.assertTrue(diagnostic.run(self.repo, number=number)["qualified"])
        for number in (10, 12, 13, 14, 15, 16, 17, 20, True):
            with self.assertRaises(workflow.WorkflowError):
                diagnostic.run(self.repo, number=number)
        self.assertEqual(self.calls, 2)
        self.assertEqual(
            self.old_v2, {str(p): p.read_bytes() for p in oldest.root(self.repo).rglob("*") if p.is_file()}
        )

    def test_structural_failure_retains_completion_and_blocks_second(self):
        def process(*args, **kwargs):
            result = self.response(*args, **kwargs)
            rows = [json.loads(row) for row in result.stdout.splitlines()]
            events = [
                {"type": "message_start", "message": {"id": "thought", "model": self.policy["model"]}},
                {"type": "content_block_start", "index": 0, "content_block": {"type": "thinking"}},
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {
                        "type": "thinking_delta",
                        "thinking": "PRIVATE-CONTENT",
                        "estimated_tokens": 0.5,
                    },
                },
                {"type": "content_block_stop", "index": 0},
                {"type": "message_stop"},
            ]
            rows[1:1] = [
                {
                    "type": "stream_event",
                    "session_id": rows[0]["session_id"],
                    "uuid": "structural-" + str(i),
                    "event": event,
                }
                for i, event in enumerate(events)
            ]
            result.stdout = "\n".join(json.dumps(row) for row in rows).encode()
            return result

        with self.isolated(process=process):
            self.assertFalse(diagnostic.run(self.repo, number=18)["qualified"])
            with self.assertRaisesRegex(workflow.WorkflowError, "stopped"):
                diagnostic.run(self.repo, number=19)
        directory = activation.root(self.repo) / "evidence-18"
        capture = old.read(directory / "review-capture.json")
        shape = capture["diagnostics"]["telemetry"]["partial_stream"]["samples"][0]["delta_shape"]
        self.assertEqual(shape["estimated_tokens_class"], "fractional")
        self.assertEqual(shape["other_fields"], 0)
        self.assertTrue(capture["reporting"]["accepted"])
        self.assertEqual(capture["diagnostics"]["telemetry"]["controlled_refusals"], 1)
        self.assertEqual(old.read(directory / diagnostic.FINISHED)["schema_version"], 5)
        self.assertNotIn(
            b"PRIVATE-CONTENT", b"".join(p.read_bytes() for p in directory.rglob("*") if p.is_file())
        )
        self.assertFalse(diagnostic.recover(self.repo, number=18)["qualified"])
        self.assertEqual(self.calls, 1)

    def test_predecessor_and_external_lock_are_required_before_provider(self):
        with self.isolated(), self.assertRaises((workflow.WorkflowError, OSError)):
            diagnostic.run(self.repo, number=19)
        with (
            self.isolated(),
            STORE(self.native.root),
            self.assertRaisesRegex(workflow.WorkflowError, "registration is busy"),
        ):
            diagnostic.run(self.repo, number=18)
        self.assertEqual(self.calls, 0)
        self.assertFalse((activation.root(self.repo) / "attempt-18.json").exists())

    def test_last_moment_source_and_receipt_mutation_refuse_with_consumed_claim(self):
        reserve = execution.reserve

        def mutate(*args, **kwargs):
            record = reserve(*args, **kwargs)
            self.harness["files"]["scripts/agentic/reporting_activation.py"] = "f" * 64
            return record

        with (
            self.isolated(),
            patch.object(execution, "reserve", side_effect=mutate),
            self.assertRaises(workflow.WorkflowError),
        ):
            diagnostic.run(self.repo, number=18)
        self.assertEqual(self.calls, 0)
        with self.assertRaisesRegex(workflow.WorkflowError, "uncertain"):
            diagnostic.recover(self.repo, number=18)

    def test_old_history_and_approval_mutations_refuse_before_reservation(self):
        good = activation.context(self.repo, self.policy)
        self.assertEqual(good["history"]["stopped_v1"]["usage"], "unknown")
        for path in (
            self.task,
            oldest.root(self.repo) / "outcome-13.json",
            oldest.root(self.repo) / "evidence-12/review.md",
        ):
            raw = path.read_bytes()
            path.write_bytes(raw + b"changed")
            try:
                with self.assertRaises((workflow.WorkflowError, ValueError)):
                    activation.context(self.repo, self.policy)
            finally:
                path.write_bytes(raw)
        self.assertEqual(activation.context(self.repo, self.policy), good)
        self.assertEqual(self.calls, 0)

    def test_actual_pair_renewal_and_full_window_keep_owned_lock(self):
        self.qualify()
        old_auth, new_auth = self.renew()
        self.policy["authentication"] = new_auth
        with SNAPSHOT(self.policy) as handle:
            self.assertEqual(admission.check(self.repo, self.policy, owned_auth=handle)["schema_version"], 5)
            self.assertTrue(handle.capability_lineage(old_auth, new_auth, 300))
            self.assertEqual(handle.current_binding(300, 3600.5), new_auth)
            with self.assertRaisesRegex(workflow.WorkflowError, "full batch window"):
                handle.current_binding(300, 7000)
        self.ordinary = review.prepare(
            self.repo,
            31,
            12,
            1234,
            review_provider="claude-code",
            reporting={"max_turns": 400, "limits": self.policy["reporting"]["limits"]},
        )
        self.policy = review.verify_packet(self.ordinary)["review_policy"]
        with self.isolated(process=self.ordinary_response), patch.object(review, "current_pr"):
            review.review(self.repo, self.ordinary)
        with patch.object(workflow, "Repo", return_value=self.repo):
            self.assertTrue(review.coverage_ready(self.ordinary))
