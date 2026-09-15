"""Archive failure and recovery tests never touch real task worktrees."""

import errno
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_workflow import workflow

# The shared fixture establishes the scripts import path.
# isort: split
import archives


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.parent = Path(self.temporary.name)
        self.source = self.parent / "source"
        self.source.mkdir()
        self.destination = self.parent / "archives/task"
        self.identity = {"repository": "example/project", "head_sha": "a" * 40}
        self.root = self.source / "results.cache"
        self.root.mkdir()
        (self.root / "data").write_bytes(b"valuable result")

    def archive(self):
        return archives.archive(self.source, self.destination, self.identity, ["results.cache"], lambda: None)

    def cross_filesystem(self):
        original = archives.rename_noreplace

        def rename(source, destination):
            if Path(source) == self.root:
                raise OSError(errno.EXDEV, "simulated filesystem boundary")
            return original(source, destination)

        return patch.object(archives, "rename_noreplace", side_effect=rename)

    def test_rename_preserves_links_without_reading_their_targets(self):
        outside = self.parent / "outside"
        outside.write_text("must stay outside")
        (self.root / "external").symlink_to(outside)
        (self.root / "broken").symlink_to("/nonexistent/target")
        result = self.archive()
        target = self.destination / "payload/results.cache"
        self.assertTrue(result["complete"])
        self.assertTrue((target / "external").is_symlink())
        self.assertEqual(os.readlink(target / "external"), str(outside))
        self.assertEqual(os.readlink(target / "broken"), "/nonexistent/target")
        self.assertEqual(outside.read_text(), "must stay outside")
        self.assertFalse(self.root.exists())

    def test_destination_collision_preserves_both_sides(self):
        archives.initialize(self.source, self.destination, self.identity, ["results.cache"])
        target = self.destination / "payload/results.cache"
        target.mkdir(parents=True)
        (target / "data").write_text("existing archive")
        with self.assertRaisesRegex(workflow.WorkflowError, "collision"):
            self.archive()
        self.assertEqual((self.root / "data").read_bytes(), b"valuable result")
        self.assertEqual((target / "data").read_text(), "existing archive")

    def test_interrupted_rename_is_reconciled_without_overwrite(self):
        original = archives.save_journal

        def interrupted(destination, journal):
            if any(entry["phase"] == "done" for entry in journal["entries"]):
                raise OSError("interrupted after rename")
            return original(destination, journal)

        with patch.object(archives, "save_journal", side_effect=interrupted):
            with self.assertRaises(OSError):
                self.archive()
        self.assertFalse(self.root.exists())
        self.assertTrue(self.archive()["complete"])
        self.assertEqual((self.destination / "payload/results.cache/data").read_bytes(), b"valuable result")

    def test_cross_filesystem_copy_is_verified_before_removal(self):
        (self.root / "link").symlink_to("data")
        with self.cross_filesystem():
            result = self.archive()
        self.assertTrue(result["complete"])
        self.assertFalse(self.root.exists())
        target = self.destination / "payload/results.cache"
        self.assertEqual((target / "data").read_bytes(), b"valuable result")
        self.assertEqual(os.readlink(target / "link"), "data")

    def test_failed_verification_retains_source_and_bad_copy_for_retry(self):
        original = archives.copy_tree

        def corrupt(source, destination):
            original(source, destination)
            (Path(destination) / "data").write_text("corrupted copy")

        with self.cross_filesystem(), patch.object(archives, "copy_tree", side_effect=corrupt):
            with self.assertRaisesRegex(workflow.WorkflowError, "verification failed"):
                self.archive()
        journal = archives.load_journal(self.destination)
        bad = archives.copy_path(self.destination, journal["entries"][0]["copy"])
        self.assertEqual((bad / "data").read_text(), "corrupted copy")
        self.assertEqual((self.root / "data").read_bytes(), b"valuable result")
        with self.cross_filesystem():
            result = self.archive()
        self.assertIn(str(bad), result["retained_copies"])
        self.assertEqual((bad / "data").read_text(), "corrupted copy")

    def test_concurrent_source_change_prevents_source_removal(self):
        original = archives.copy_tree

        def changed(source, destination):
            original(source, destination)
            (Path(source) / "data").write_text("new user result")

        with self.cross_filesystem(), patch.object(archives, "copy_tree", side_effect=changed):
            with self.assertRaisesRegex(workflow.WorkflowError, "Source changed"):
                self.archive()
        self.assertEqual((self.root / "data").read_text(), "new user result")
        journal = archives.load_journal(self.destination)
        copy = archives.copy_path(self.destination, journal["entries"][0]["copy"])
        self.assertEqual((copy / "data").read_bytes(), b"valuable result")

    def test_interrupted_source_unlink_resumes_from_pending_journal_step(self):
        original = archives.os.unlink
        interrupted = False

        def unlink(name, *args, **kwargs):
            nonlocal interrupted
            result = original(name, *args, **kwargs)
            if name == "data" and not interrupted:
                interrupted = True
                raise OSError("interrupted after unlink")
            return result

        with self.cross_filesystem(), patch.object(archives.os, "unlink", side_effect=unlink):
            with self.assertRaises(OSError):
                self.archive()
        with self.cross_filesystem():
            result = self.archive()
        self.assertTrue(result["complete"])
        self.assertFalse(self.root.exists())
        self.assertEqual((self.destination / "payload/results.cache/data").read_bytes(), b"valuable result")

    def test_fifo_is_refused_before_source_transfer(self):
        os.mkfifo(self.root / "pipe")
        with self.assertRaisesRegex(workflow.WorkflowError, "Unsupported"):
            self.archive()
        self.assertTrue((self.root / "data").exists())
        self.assertFalse(self.destination.exists())

    def test_symlink_destination_ancestor_is_not_followed(self):
        outside = self.parent / "outside"
        outside.mkdir()
        (self.parent / "archives").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(workflow.WorkflowError):
            self.archive()
        self.assertEqual(list(outside.iterdir()), [])
        self.assertTrue((self.root / "data").exists())
