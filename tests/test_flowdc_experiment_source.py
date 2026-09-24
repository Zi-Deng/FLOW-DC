"""Exercise committed source selection through real local Git subprocesses."""

import hashlib
import importlib.util
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

MODULE = Path(__file__).resolve().parents[1] / "bin/flowdc_experiment_source.py"
SPEC = importlib.util.spec_from_file_location("flowdc_experiment_source", MODULE)
source = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(source)


class SourceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.repo = Path(temporary.name)
        self.git("init", "-q")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "Test")
        (self.repo / "bin").mkdir()
        self.contents = {}
        for path in source.SOURCE_PATHS:
            self.contents[path] = f"# committed {path}\n".encode()
            (self.repo / path).write_bytes(self.contents[path])
        (self.repo / "private-key").write_text("excluded sentinel")
        self.commit = self.commit_all()

    def git(self, *args):
        return (
            subprocess.check_output(
                ["git", "-C", str(self.repo), *args], stderr=subprocess.DEVNULL, timeout=10
            )
            .decode()
            .strip()
        )

    def commit_all(self):
        self.git("add", ".")
        self.git("commit", "-qm", "fixture")
        return self.git("rev-parse", "HEAD")

    def test_snapshot_ignores_dirty_and_unrelated_files(self):
        (self.repo / source.SOURCE_PATHS[0]).write_text("uncommitted changes")
        manifest, files = source.read_source(self.repo, self.commit[:12])
        self.assertEqual(files, self.contents)
        self.assertEqual(manifest["commit"], self.commit)
        self.assertEqual(manifest["schema_version"], 1)
        for path, content in self.contents.items():
            self.assertEqual(
                manifest["files"][path],
                {"bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()},
            )

    def test_rejects_moving_names_options_and_revision_expressions(self):
        for revision in ("HEAD", "main", "--help", self.commit + "^", "abc", None):
            with (
                self.subTest(revision=revision),
                self.assertRaisesRegex(source.SourceError, "source_commit_required"),
            ):
                source.read_source(self.repo, revision)

    def test_rejects_noncommit_object(self):
        blob = self.git("rev-parse", f"{self.commit}:{source.SOURCE_PATHS[0]}")
        with self.assertRaisesRegex(source.SourceError, "source_commit_required"):
            source.read_source(self.repo, blob)

    def test_missing_object_has_fixed_diagnostic(self):
        with self.assertRaisesRegex(source.SourceError, "^source_git_unavailable$"):
            source.read_source(self.repo, "0" * 40)

    def test_rejects_committed_symlink_and_missing_entrypoint(self):
        target = self.repo / source.SOURCE_PATHS[0]
        target.unlink()
        missing = self.commit_all()
        with self.assertRaisesRegex(source.SourceError, "source_entrypoint_invalid"):
            source.read_source(self.repo, missing)
        target.symlink_to("../private-key")
        symlink = self.commit_all()
        with self.assertRaisesRegex(source.SourceError, "source_entrypoint_invalid"):
            source.read_source(self.repo, symlink)

    def test_bounds_total_source_bytes(self):
        with (
            patch.object(source, "MAX_SOURCE_BYTES", 1),
            self.assertRaisesRegex(source.SourceError, "source_size_limit"),
        ):
            source.read_source(self.repo, self.commit)

    def test_ignores_git_routing_and_replacement_objects(self):
        (self.repo / source.SOURCE_PATHS[0]).write_text("replacement")
        replacement = self.commit_all()
        self.git("replace", self.commit, replacement)
        with patch.dict(os.environ, {"GIT_DIR": "/nonexistent", "GIT_WORK_TREE": "/nonexistent"}):
            manifest, files = source.read_source(self.repo, self.commit)
        self.assertEqual(manifest["commit"], self.commit)
        self.assertEqual(files, self.contents)


if __name__ == "__main__":
    unittest.main()
