"""Closed stopped-v4 history, including its absence of execution and unknown use."""

import copy
import socket
from pathlib import Path
from unittest.mock import patch

import claude_native_auth as auth
import reporting_activation_v4 as old
import reporting_activation_v5 as activation
import reporting_cli
import reporting_recovery_history_v5 as history
import review_claude
from tasks import atomic_json, digest
from test_reporting_recovery_v5 import RecoveryFixture
from test_workflow import workflow


class HistoryTests(RecoveryFixture):
    def test_unknown_reservation_and_all_available_legacy_outcomes_are_offline(self):
        with (
            patch.object(auth, "store", side_effect=AssertionError("No credentials")),
            patch.object(auth, "current_binding", side_effect=AssertionError("No authentication")),
            patch.object(review_claude, "preflight", side_effect=AssertionError("No preflight")),
            patch.object(review_claude.review_process, "capture", side_effect=AssertionError("No provider")),
            patch.object(socket.socket, "connect", side_effect=AssertionError("No network")),
        ):
            result = history.stopped(self.repo)
            self.assertEqual(result["stopped_v4"]["usage"], "unknown")
            self.assertEqual(result["stopped_v4"]["unavailable"], [17])
            self.assertEqual(result["stopped_v4"]["state"], "consumed-uncertain-no-retry")
            self.assertNotIn("outcomes", result["stopped_v4"])
            for version, numbers in ((2, (12, 13)), (3, (14, 15))):
                for number in numbers:
                    args = reporting_cli.parser().parse_args([f"recover-v{version}", "--number", str(number)])
                    self.assertEqual(
                        reporting_cli.dispatch(self.repo, args)["qualified"], number == numbers[0]
                    )
            for number in (16, 17):
                args = reporting_cli.parser().parse_args(["recover-v4", "--number", str(number)])
                with self.assertRaises((workflow.WorkflowError, OSError)):
                    reporting_cli.dispatch(self.repo, args)
        for path, raw in {**self.old_v2, **self.old_v3, **self.old_v4}.items():
            self.assertEqual(Path(path).read_bytes(), raw)

    def test_original_closed_tree_refuses_added_execution_successor_and_symlink(self):
        baseline = history.stopped(self.repo)
        root = old.root(self.repo)
        for name in (
            "evidence-16/attempt.json",
            "evidence-16/reporting-execution.json",
            "evidence-16/review-capture.json",
            "evidence-16/reporting-finished.json",
            "outcome-16.json",
            "attempt-17.json",
            "evidence-17/metadata.json",
        ):
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}")
            try:
                with self.subTest(name=name), self.assertRaises(workflow.WorkflowError):
                    activation.preview(
                        self.repo,
                        self.policy,
                        name="no-rebind",
                        tested_head=self.harness["head"],
                        expires_at=2200,
                        now=self.now,
                    )
            finally:
                path.unlink()
        path = root / "attempt-16.json"
        raw = path.read_bytes()
        path.unlink()
        path.symlink_to(self.task)
        try:
            with self.assertRaises(workflow.WorkflowError):
                history.stopped(self.repo)
        finally:
            path.unlink()
            path.write_bytes(raw)
        self.assertEqual(history.stopped(self.repo), baseline)

    def test_reservation_closed_fields_are_validated_beyond_tree_identity(self):
        history.stopped(self.repo)
        path = old.root(self.repo) / "attempt-16.json"
        original = old.read(path)
        original_bytes = path.read_bytes()
        for key, value in (
            ("schema_version", True),
            ("number", 17),
            ("purpose", "native-tools-and-source"),
            ("grant_digest", "f" * 64),
            ("input_digest", "a" * 64),
            ("started", True),
            ("started", 1),
            ("deadline", original["deadline"] + 1),
            ("reserved_seconds", 299),
            ("reserved_reference_usd", 1),
            ("wrapper_processes", True),
            ("finished", original["started"] + 1),
        ):
            atomic_json(path, {**original, key: value})
            try:
                # Synthetic alternate closed pin isolates the semantic record guard.
                with patch.object(history, "V4_TREE", history.tree(old.root(self.repo))):
                    with self.subTest(key=key), self.assertRaises(workflow.WorkflowError):
                        history.stopped(self.repo)
            finally:
                path.write_bytes(original_bytes)
        history.stopped(self.repo)

    def test_exact_historical_approval_duplicate_and_current_authority_refuse(self):
        baseline = activation.context(self.repo, self.policy)
        original = old.read(self.task)
        old_row = next(
            row for row in original["approval_history"] if row["plan_comment"] == old.CONTRACT["plan_comment"]
        )
        for change in ("duplicate", "changed-duplicate", "missing", "modified", "current"):
            state = copy.deepcopy(original)
            if change == "duplicate":
                state["approval_history"].append(old_row)
            elif change == "changed-duplicate":
                state["approval_history"].append({**old_row, "source": "other provenance"})
            elif change == "missing":
                state["approval_history"].remove(old_row)
            elif change == "modified":
                next(
                    row for row in state["approval_history"] if row["plan_comment"] == old_row["plan_comment"]
                )["source"] = "changed"
            else:
                state["approval"] = old_row
            atomic_json(self.task, state)
            try:
                with self.subTest(change=change), self.assertRaises(workflow.WorkflowError):
                    activation.context(self.repo, self.policy)
            finally:
                atomic_json(self.task, original)
        self.assertEqual(activation.context(self.repo, self.policy), baseline)
        self.assertEqual(digest(old.load(self.repo)[0]), history.V4_GRANT)
