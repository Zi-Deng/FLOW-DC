"""Exercise real merge gates with deterministic gh output and no Git/model service."""

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from test_workflow import SOURCE, workflow

# The shared module establishes the scripts import path.
# isort: split
import finish

HEAD = "a" * 40
BASE = "b" * 40


class FinishGateTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="agentic-gates-")
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root / ".agentic").mkdir()
        config = workflow.configuration(SOURCE)
        config["required_checks"] = ["quality"]
        (root / ".agentic/config.json").write_text(json.dumps(config), encoding="utf-8")
        self.repo = SimpleNamespace(root=root, name="example/project")

    @staticmethod
    def response(value, code=0):
        return subprocess.CompletedProcess([], code, json.dumps(value), "")

    def gh(self, *responses):
        responses = iter(responses)

        def execute(args, **kwargs):
            self.assertEqual(args[0], "gh")
            self.assertIs(kwargs.get("check"), False)
            return next(responses)

        return patch.object(finish, "run", side_effect=execute)

    @staticmethod
    def page(
        *,
        head=HEAD,
        base=BASE,
        nodes=None,
        more=False,
        cursor=None,
        queued=False,
    ):
        return {
            "data": {
                "repository": {
                    "pullRequest": {
                        "headRefOid": head,
                        "baseRefOid": base,
                        "mergeQueueEntry": {"id": "queue-1"} if queued else None,
                        "reviewThreads": {
                            "nodes": [] if nodes is None else nodes,
                            "pageInfo": {"hasNextPage": more, "endCursor": cursor},
                        },
                    }
                }
            }
        }

    def test_required_checks_accept_all_passing_server_required_checks(self):
        checks = [
            {"name": "quality", "bucket": "pass", "state": "SUCCESS"},
            {"name": "security", "bucket": "pass", "state": "SUCCESS"},
        ]
        with self.gh(self.response(checks)) as command:
            self.assertEqual(finish.required_checks(self.repo, 31), checks)
        self.assertIn("--required", command.call_args.args[0])
        self.assertEqual(command.call_args.args[0][:4], ["gh", "pr", "checks", "31"])

    def test_required_checks_reject_nonpassing_missing_and_malformed_evidence(self):
        passing = [{"name": "quality", "bucket": "pass"}]
        cases = [
            (0, [{"name": "quality", "bucket": "pending"}], "Every required check"),
            (0, [{"name": "quality", "bucket": "fail"}], "Every required check"),
            (8, [{"name": "quality", "bucket": "pending"}], "pending"),
            (1, [{"name": "quality", "bucket": "fail"}], "failing"),
            (0, [{"name": "quality", "bucket": "skipping"}], "Every required check"),
            (0, [], "configuration"),
            (0, [{"name": "security", "bucket": "pass"}], "policy"),
            (0, passing + [{"name": "security", "bucket": "fail"}], "Every required check"),
            (1, passing, "gh exit 1"),
            (2, passing, "gh exit 2"),
            (8, passing, "gh exit 8"),
            (0, None, "Malformed"),
            (0, [{}], "Malformed"),
            (0, [{"name": [], "bucket": "pass"}], "Malformed"),
            (0, [{"name": "quality", "bucket": "unknown"}], "Malformed"),
        ]
        for code, checks, message in cases:
            with self.subTest(code=code, checks=checks):
                with self.gh(self.response(checks, code)):
                    with self.assertRaisesRegex(workflow.WorkflowError, message):
                        finish.required_checks(self.repo, 31)
        for code in (0, 1):
            with self.subTest(malformed_json_exit=code):
                with self.gh(subprocess.CompletedProcess([], code, "{", "transport error")):
                    with self.assertRaisesRegex(workflow.WorkflowError, "Malformed.*gh exit"):
                        finish.required_checks(self.repo, 31)

    def test_query_transport_exceptions_become_workflow_errors(self):
        for gate in (finish.required_checks, finish.server_view):
            for failure in (OSError("transport unavailable"), subprocess.TimeoutExpired("gh", 120)):
                with self.subTest(gate=gate.__name__, failure=type(failure).__name__):
                    with patch.object(finish, "run", side_effect=failure):
                        with self.assertRaisesRegex(workflow.WorkflowError, "could not complete"):
                            gate(self.repo, 31)

    def test_server_view_paginates_and_retains_unresolved_threads_and_queue(self):
        first = {"id": "thread-1", "isResolved": True}
        second = {"id": "thread-2", "isResolved": False}
        with self.gh(
            self.response(self.page(nodes=[first], more=True, cursor="page-2", queued=True)),
            self.response(self.page(nodes=[second], queued=True)),
        ) as command:
            result = finish.server_view(self.repo, 31)
        self.assertEqual(
            result,
            {"head_sha": HEAD, "base_sha": BASE, "queued": True, "threads": [first, second]},
        )
        variables = [json.loads(call.kwargs["input"])["variables"] for call in command.call_args_list]
        self.assertEqual([item["cursor"] for item in variables], [None, "page-2"])
        self.assertTrue(all(item["number"] == 31 for item in variables))
        self.assertEqual(command.call_args.args[0], ["gh", "api", "graphql", "--input", "-"])
        with self.gh(self.response(self.page())):
            result = finish.server_view(self.repo, 31)
        self.assertFalse(result["queued"])
        self.assertEqual(result["threads"], [])

    def test_server_view_rejects_changed_head_or_base(self):
        for changed in ({"head": "c" * 40}, {"base": "c" * 40}):
            with self.subTest(changed=changed):
                with self.gh(
                    self.response(self.page(more=True, cursor="next")),
                    self.response(self.page(**changed)),
                ):
                    with self.assertRaisesRegex(workflow.WorkflowError, "changed during"):
                        finish.server_view(self.repo, 31)

    def test_server_view_rejects_duplicate_threads_and_cursor_cycles(self):
        thread = {"id": "thread-1", "isResolved": True}
        cases = [
            (
                [
                    self.page(nodes=[thread], more=True, cursor="next"),
                    self.page(nodes=[thread]),
                ],
                "duplicate",
            ),
            ([self.page(more=True)], "did not advance"),
            (
                [self.page(more=True, cursor="next"), self.page(more=True, cursor="next")],
                "did not advance",
            ),
            (
                [
                    self.page(more=True, cursor="one"),
                    self.page(more=True, cursor="two"),
                    self.page(more=True, cursor="one"),
                ],
                "did not advance",
            ),
        ]
        for pages, message in cases:
            with self.subTest(message=message, pages=len(pages)):
                with self.gh(*(self.response(page) for page in pages)):
                    with self.assertRaisesRegex(workflow.WorkflowError, message):
                        finish.server_view(self.repo, 31)

    def test_server_view_rejects_errors_and_malformed_data(self):
        missing_queue = self.page()
        del missing_queue["data"]["repository"]["pullRequest"]["mergeQueueEntry"]
        malformed_queue = self.page()
        malformed_queue["data"]["repository"]["pullRequest"]["mergeQueueEntry"] = {}
        values = [
            {"errors": [{"message": "unavailable"}]},
            {"data": {"repository": None}},
            {},
            [],
            missing_queue,
            malformed_queue,
            self.page(head=None),
            self.page(more="false"),
            self.page(nodes={}),
            self.page(nodes=[{"id": "thread", "isResolved": "false"}]),
            self.page(nodes=[{"id": [], "isResolved": True}]),
            self.page(cursor=12),
        ]
        for value in values:
            with self.subTest(value=value):
                with self.gh(self.response(value)):
                    with self.assertRaises(workflow.WorkflowError):
                        finish.server_view(self.repo, 31)
        with self.gh(subprocess.CompletedProcess([], 0, "{", "")):
            with self.assertRaisesRegex(workflow.WorkflowError, "Malformed"):
                finish.server_view(self.repo, 31)
        with self.gh(self.response(self.page(), code=1)):
            with self.assertRaisesRegex(workflow.WorkflowError, "gh exit 1"):
                finish.server_view(self.repo, 31)
