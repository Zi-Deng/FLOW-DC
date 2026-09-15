"""Managed-model doubles use local Python processes, never a model service."""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

from test_workflow import GitFixture, workflow

# The shared fixture establishes the scripts import path.
# isort: split
import sessions
import tasks

IDENTITY = "12345678-1234-4234-8234-123456789abc"
OTHER_IDENTITY = "22345678-1234-4234-8234-123456789abc"
HELP = (
    "SESSION_ID --ask-for-approval --sandbox --cd --add-dir --json "
    "--model --output-schema --output-last-message"
)
MODEL_DOUBLE = r"""
import json
import pathlib
import sys
import time

events = json.loads(sys.argv[1])
result_path = pathlib.Path(sys.argv[2])
state_path = pathlib.Path(sys.argv[3])
result = json.loads(sys.argv[4])
verify_saved = sys.argv[5] == "yes"
stderr_bytes = int(sys.argv[6])
for event in events:
    print(json.dumps(event), flush=True)
    if verify_saved and event["type"] == "thread.started":
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = json.loads(state_path.read_text())
            if state["executor"].get("uuid") == event["thread_id"]:
                break
            time.sleep(0.01)
        else:
            sys.exit(7)
        if stderr_bytes:
            sys.stderr.write("x" * stderr_bytes)
            sys.stderr.flush()
result_path.write_text(json.dumps(result))
sys.stdout.write(sys.argv[7])
sys.stdout.flush()
"""


class SessionTests(GitFixture):
    def setUp(self):
        super().setUp()
        tasks.approve_plan(self.repo, 12, 1234, "Explicit task authorization")
        prepared = tasks.prepare(self.repo, 12, "correct-value")
        self.task_path = Path(prepared["worktree"])
        self.model_calls = []

    def invoke(
        self,
        *,
        role="implement",
        task=12,
        identity=IDENTITY,
        status="completed",
        no_thread=False,
        verify_saved=True,
        stderr_bytes=0,
        output_budget=12_000_000,
        final_event="turn.completed",
        trailing_stdout="",
    ):
        original_run = sessions.run
        original_popen = subprocess.Popen
        events = [] if no_thread else [{"type": "thread.started", "thread_id": identity}]
        events.append({"type": final_event, "usage": {}})
        config = workflow.configuration(self.root)
        config["managed_max_output_bytes"] = output_budget
        result = {
            "status": status,
            "summary": "Local model-process double",
            "checks": ["fixture validation only"],
            "blockers": ["Deliberate blocker"] if status == "blocked" else [],
        }

        def fake_run(args, **kwargs):
            if args[0] == "codex":
                return subprocess.CompletedProcess(args, 0, HELP, "")
            return original_run(args, **kwargs)

        def fake_popen(args, **kwargs):
            if args[0] != "codex":
                return original_popen(args, **kwargs)
            self.model_calls.append((args, kwargs))
            result_path = args[args.index("--output-last-message") + 1]
            state_path = self.root / ".agentic-local/tasks/issue-12.json"
            return original_popen(
                [
                    sys.executable,
                    "-c",
                    MODEL_DOUBLE,
                    json.dumps(events),
                    result_path,
                    str(state_path),
                    json.dumps(result),
                    "yes" if verify_saved else "no",
                    str(stderr_bytes),
                    trailing_stdout,
                ],
                **kwargs,
            )

        with (
            patch.object(sessions, "run", side_effect=fake_run),
            patch.object(sessions, "Popen", side_effect=fake_popen),
            patch.object(sessions, "configuration", return_value=config),
        ):
            return sessions.managed_launch(self.repo, role, task, execute=True)

    def test_thread_id_is_durable_before_next_event(self):
        result = self.invoke()
        self.assertEqual(result["executor_uuid"], IDENTITY)
        self.assertFalse(result["incomplete"])
        self.assertEqual(result["status"], "completed")
        args, options = self.model_calls[0]
        self.assertEqual(options["cwd"], self.task_path)
        self.assertEqual(options["env"]["AGENTIC_EXECUTOR_ROLE"], "implement")
        self.assertEqual(args[args.index("--model") + 1], "gpt-6-astra")
        self.assertEqual(args[args.index("--sandbox") + 1], "workspace-write")
        self.assertEqual(args[args.index("--add-dir") + 1], str(self.repo.common))
        self.assertEqual(args[args.index("--ask-for-approval") + 1], "never")

    def test_resume_uses_original_uuid_even_with_a_newer_record(self):
        self.invoke()
        (self.root / ".agentic-local/newer-session.json").write_text(json.dumps({"uuid": OTHER_IDENTITY}))
        self.invoke()
        args = self.model_calls[-1][0]
        self.assertEqual(args[args.index("resume") + 1], IDENTITY)
        self.assertNotIn("--last", args)
        self.assertNotIn("--ephemeral", args)

    def test_managed_repair_resumes_original_executor_with_all_feedback_surfaces(self):
        self.invoke()
        self.pr_data = {
            "state": "open",
            "merged": False,
            "head": {
                "sha": self.base,
                "ref": "issue-12-correct-value",
                "repo": {"full_name": self.repo.name},
            },
            "base": {"sha": self.base, "ref": "trunk"},
        }
        feedback = {
            "reviews": [{"id": 101, "body": "Review finding", "commit_id": self.base}],
            "inline": [{"id": 102, "body": "Inline finding", "path": "code.py", "line": 1}],
            "conversation": [{"id": 103, "body": "Public disposition context"}],
        }
        routes = {
            "pulls/31/reviews?per_page=100": feedback["reviews"],
            "pulls/31/comments?per_page=100": feedback["inline"],
            "issues/31/comments?per_page=100": feedback["conversation"],
        }
        original_api = self.repo.api

        def api(suffix, **kwargs):
            if suffix in routes:
                self.assertTrue(kwargs.get("paginate"))
                return routes[suffix]
            return original_api(suffix, **kwargs)

        with patch.object(self.repo, "api", side_effect=api):
            result = self.invoke(role="repair", task=31)
        self.assertFalse(result["incomplete"])
        self.assertEqual(result["executor_uuid"], IDENTITY)
        args, options = self.model_calls[-1]
        self.assertEqual(args[args.index("resume") + 1], IDENTITY)
        self.assertNotIn("--last", args)
        self.assertEqual(options["cwd"], self.task_path)
        self.assertEqual(options["env"]["AGENTIC_EXECUTOR_ROLE"], "repair")
        prompt = (Path(result["directory"]) / "prompt.txt").read_text()
        public = json.loads(prompt.split("Public task data:\n", 1)[1])
        self.assertEqual(public["feedback"], feedback)
        state = tasks.TaskStore(self.repo).read("issue-12")
        self.assertEqual(state["pr"], 31)
        self.assertEqual(state["executor"]["uuid"], IDENTITY)
        self.assertEqual(
            [record["role"] for record in state["executor"]["runs"]],
            ["implement", "repair"],
        )

    def test_returned_replacement_uuid_is_refused(self):
        self.invoke()
        result = self.invoke(identity=OTHER_IDENTITY, verify_saved=False)
        self.assertTrue(result["incomplete"])
        self.assertEqual(result["status"], "failed")
        self.assertEqual(tasks.TaskStore(self.repo).read("issue-12")["executor"]["uuid"], IDENTITY)

    def test_blocked_turn_remains_incomplete(self):
        result = self.invoke(status="blocked")
        self.assertEqual(result["status"], "blocked")
        self.assertTrue(result["incomplete"])
        self.assertEqual(result["executor_uuid"], IDENTITY)

    def test_missing_thread_id_cannot_trigger_a_replacement_start(self):
        first = self.invoke(no_thread=True)
        self.assertTrue(first["incomplete"])
        with self.assertRaisesRegex(workflow.WorkflowError, "UUID is missing"):
            self.invoke()
        self.assertEqual(len(self.model_calls), 1)

    def test_recovery_requires_matching_saved_uuid_and_worktree(self):
        record = self.parent / "saved-session.jsonl"
        record.write_text(
            json.dumps(
                {
                    "type": "session_meta",
                    "payload": {"id": IDENTITY, "cwd": str(self.task_path)},
                }
            )
            + "\n"
        )
        with self.assertRaises(workflow.WorkflowError):
            sessions.recover_executor(self.repo, 12, OTHER_IDENTITY, record, "Saved executor record", True)
        recovered = sessions.recover_executor(
            self.repo, 12, IDENTITY, record, "Original executor verified by coordinator", True
        )
        self.assertEqual(recovered["executor_uuid"], IDENTITY)
        self.invoke()
        self.assertIn("resume", self.model_calls[-1][0])

    def test_recovery_does_not_replace_an_existing_identity(self):
        self.invoke()
        record = self.parent / "different-session.jsonl"
        record.write_text(json.dumps({"type": "thread.started", "thread_id": OTHER_IDENTITY}) + "\n")
        with self.assertRaisesRegex(workflow.WorkflowError, "cannot replace"):
            sessions.recover_executor(self.repo, 12, OTHER_IDENTITY, record, "Different record", True)

    def test_preview_does_not_start_or_register_a_session(self):
        with patch.object(sessions, "check_cli"), patch.object(sessions, "Popen") as process:
            result = sessions.managed_launch(self.repo, "implement", 12)
        process.assert_not_called()
        self.assertIsNone(result["executor_uuid"])
        self.assertNotIn("executor", tasks.TaskStore(self.repo).read("issue-12"))

    def test_stderr_is_bounded_and_preserves_the_saved_uuid(self):
        result = self.invoke(stderr_bytes=100_000, output_budget=4096)
        self.assertTrue(result["incomplete"])
        self.assertEqual(result["executor_uuid"], IDENTITY)
        directory = Path(result["directory"])
        self.assertLessEqual(
            (directory / "stdout.jsonl").stat().st_size + (directory / "stderr.log").stat().st_size,
            4096,
        )

    def test_zero_exit_interrupted_event_remains_incomplete(self):
        result = self.invoke(final_event="turn.interrupted")
        self.assertEqual(result["status"], "interrupted")
        self.assertTrue(result["incomplete"])
        self.assertEqual(result["executor_uuid"], IDENTITY)

    def test_truncated_final_json_records_failure_and_preserves_resume_identity(self):
        truncated = '{"type":'
        with self.assertRaises(json.JSONDecodeError) as decoding:
            json.loads(truncated)
        result = self.invoke(trailing_stdout=truncated)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["incomplete"])
        self.assertEqual(result["executor_uuid"], IDENTITY)
        self.assertEqual(result["error"], str(decoding.exception))
        self.assertIsNone(result["result"])
        output = (Path(result["directory"]) / "stdout.jsonl").read_bytes()
        self.assertTrue(output.endswith(truncated.encode()))
        state = tasks.TaskStore(self.repo).read("issue-12")
        record = state["executor"]["runs"][-1]
        self.assertEqual(state["executor"]["uuid"], IDENTITY)
        self.assertEqual(record["status"], "failed")
        self.assertTrue(record["incomplete"])
        self.assertEqual(record["error"], result["error"])
        self.assertEqual(record["head_after"], self.base)

        resumed = self.invoke()
        self.assertFalse(resumed["incomplete"])
        args = self.model_calls[-1][0]
        self.assertEqual(args[args.index("resume") + 1], IDENTITY)
        self.assertNotIn("--last", args)

    def test_early_checkpoint_is_not_implementation_completion(self):
        result = self.invoke(status="checkpoint")
        self.assertEqual(result["status"], "checkpoint")
        self.assertTrue(result["incomplete"])
        self.assertEqual(result["executor_uuid"], IDENTITY)
