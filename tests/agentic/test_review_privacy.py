"""Exercise project data exclusions and local-state permissions using real Git."""

import os
import stat
from unittest.mock import patch

from test_workflow import GitFixture, git, review, workflow

# isort: split
import tasks


class ReviewPrivacyTests(GitFixture):
    def publish_tip(self):
        git(self.task_path, "add", "-A")
        git(self.task_path, "commit", "-m", "privacy fixture")
        self.pr_data["head"]["sha"] = git(self.task_path, "rev-parse", "HEAD")
        git(self.task_path, "push", "origin", "HEAD:refs/pull/31/head")

    def test_project_snapshot_excludes_data_but_retains_maintained_source(self):
        restricted = [
            "files/input/urls.txt",
            "files/output/overview.json",
            "files/biotrove_train_stats.json",
            "benchmark/manifests/run/metadata.json",
            "benchmark/results/run/report.json",
            "playground/biotrove_tar_paths/chunk.txt",
            "archives/legacy/config.json",
        ]
        allowed = [
            "files/config/example.json",
            "benchmark/core/flowdc_adapter.py",
            "benchmark/results_summary.py",
            "files/input_format.py",
        ]
        for name in restricted + allowed:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture only\n", encoding="utf-8")
        git(self.root, "add", "-A")
        git(self.root, "commit", "-m", "snapshot fixture")
        output = self.parent / "snapshot"
        index = {
            item["path"]: item
            for item in review.snapshot(
                self.repo, git(self.root, "rev-parse", "HEAD"), output, workflow.configuration(self.root)
            )
        }
        for name in restricted:
            with self.subTest(path=name):
                self.assertEqual(index[name]["omitted"], "private/data path excluded")
                self.assertNotIn("snapshot", index[name])
        for name in allowed:
            with self.subTest(path=name):
                self.assertIn("snapshot", index[name])

    def test_restricted_additions_fail_before_creating_packet(self):
        self.commit_task()
        for name in review.PRIVATE_PATHS:
            path = self.task_path / name
            if not path.suffix:
                path /= "record.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture\n", encoding="utf-8")
        self.publish_tip()
        with self.assertRaisesRegex(workflow.WorkflowError, "private/data"):
            review.prepare(self.repo, 31, 12, 1234)
        self.assertFalse((self.root / ".agentic-local/reviews").exists())

    def test_restricted_deletion_or_rename_to_public_path_is_refused(self):
        original = self.root / "files/input/source.txt"
        original.parent.mkdir(parents=True)
        original.write_text("private fixture\n", encoding="utf-8")
        git(self.root, "add", "files")
        git(self.root, "commit", "-m", "private base fixture")
        git(self.root, "push", "origin", "trunk")
        self.base = git(self.root, "rev-parse", "HEAD")
        self.commit_task()
        source = self.task_path / "files/input/source.txt"
        source.rename(self.task_path / "public.txt")
        self.publish_tip()
        for operation in ("rename", "delete"):
            with self.subTest(operation=operation):
                with self.assertRaisesRegex(workflow.WorkflowError, "private/data"):
                    review.prepare(self.repo, 31, 12, 1234)
                self.assertFalse((self.root / ".agentic-local/reviews").exists())
            if operation == "rename":
                (self.task_path / "public.txt").unlink()
                self.publish_tip()

    def test_review_state_is_private_even_with_permissive_umask(self):
        self.commit_task()
        state = self.root / ".agentic-local"
        state.mkdir(mode=0o755)
        previous = os.umask(0)
        try:
            directory = review.prepare(self.repo, 31, 12, 1234)
        finally:
            os.umask(previous)
        for path in (state, state / "reviews", directory):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o755)

    def test_symlinked_output_is_rejected_without_writing_through_it(self):
        self.commit_task()
        target = self.parent / "outside"
        target.mkdir()
        link = self.parent / "link"
        link.symlink_to(target, target_is_directory=True)
        with self.assertRaisesRegex(workflow.WorkflowError, "symlink"):
            review.prepare(self.repo, 31, 12, 1234, output=link / "packet")
        self.assertEqual(list(target.iterdir()), [])

    def test_atomic_records_create_private_parents_and_reject_links(self):
        path = self.parent / "state/nested/record.json"
        previous = os.umask(0)
        try:
            tasks.atomic_json(path, {"fixture": "private"})
        finally:
            os.umask(previous)
        for directory in (path.parent, path.parent.parent):
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        link = self.parent / "link"
        link.symlink_to(path.parent, target_is_directory=True)
        with self.assertRaisesRegex(workflow.WorkflowError, "symlink"):
            tasks.private_directory(link)
        with patch.object(tasks, "atomic_text") as write:
            with self.assertRaisesRegex(workflow.WorkflowError, "symlink"):
                tasks.private_directory(link / "new")
            write.assert_not_called()
