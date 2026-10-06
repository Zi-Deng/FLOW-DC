"""Original c303 closure must be checked before any prospective binding."""

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import reporting_recovery_history_v3 as history
from tasks import digest
from workflow import WorkflowError


class OriginalClosureTests(unittest.TestCase):
    def test_original_closure_then_extra_missing_and_changed_prepreview_refuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = SimpleNamespace(main=Path(tmp), name="Zi-Deng/FLOW-DC")
            directory = repo.main / ".agentic-local" / history.C303_DIRECTORY
            directory.mkdir(parents=True)
            (directory / "original").write_bytes(b"unchanged historical source")
            original = history.tree(directory)
            units = [
                {"id": str(i), "state": "complete" if i < 19 else "incomplete", "review_sha256": "a" * 64}
                for i in range(20)
            ]
            assessment = {"qualified": False, "inspected_count": 60, "required_count": 776, "units": units}
            snapshot = {
                "reviews": [
                    {"id": i, "body": str(i), "commit_id": history.C303_HEAD, "state": "COMMENTED"}
                    for i in range(21)
                ]
            }
            # The actual traversal remains real. Synthetic semantic/publication
            # baselines isolate extra outer material that packet checks do not see.
            with (
                patch.object(history, "C303_TREE", original, create=True),
                patch.object(
                    history.review,
                    "verify_packet",
                    return_value={
                        "repository": repo.name,
                        "pr": 32,
                        "head_sha": history.C303_HEAD,
                        "plan_comment": 5966428269,
                    },
                ),
                patch.object(
                    history.review_batch,
                    "load",
                    return_value={
                        "contract_digest": history.C303_CONTRACT,
                        "schema_version": 6,
                        "units": [{}] * 139,
                    },
                ),
                patch.object(
                    history.review_batch,
                    "state_for",
                    return_value={
                        "stop_reason": "execution_incomplete_or_interrupted",
                        "reservations": [{}] * 20,
                    },
                ),
                patch.object(history.review, "qualification", return_value=assessment),
                patch.object(history, "approval", return_value="a" * 64),
                patch.object(history.old, "read", return_value=snapshot),
                patch.object(history.review, "publication_body", side_effect=lambda path: path.name),
                patch.object(history.review_batch, "publication_body", return_value="20"),
                patch.object(history.review, "digest", return_value="b" * 64),
            ):
                self.assertEqual({k: history.c303(repo)[k] for k in original}, original)
                (directory / "extra").write_bytes(b"additional outer material")
                with self.assertRaisesRegex(WorkflowError, "original.*closure"):
                    history.c303(repo)
                (directory / "extra").unlink()
                raw = (directory / "original").read_bytes()
                (directory / "original").unlink()
                with self.assertRaisesRegex(WorkflowError, "original.*closure"):
                    history.c303(repo)
                (directory / "original").write_bytes(b"changed historical source")
                with self.assertRaisesRegex(WorkflowError, "original.*closure"):
                    history.c303(repo)
                (directory / "original").write_bytes(raw)
                self.assertEqual(digest({k: history.c303(repo)[k] for k in original}), digest(original))
