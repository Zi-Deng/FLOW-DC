"""Offline operations CLI tests; all writes stay in disposable temporary roots."""

import contextlib
import importlib.util
import io
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "bin/flowdc_ops.py"
SPEC = importlib.util.spec_from_file_location("flowdc_ops", SCRIPT)
ops = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ops)


class InitTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="flowdc-ops-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.state = self.root / "state"
        self.config = self.root / "config"
        self.arguments = ["init", "--state-root", str(self.state), "--config-root", str(self.config)]

    def run_cli(self, *arguments, env=None):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), *arguments], capture_output=True, text=True, timeout=10, env=env
        )
        self.assertEqual(result.stderr, "", result.stderr)
        value = json.loads(result.stdout)
        self.assertEqual(value["schema_version"], 1)
        self.assertEqual(
            set(value), {"schema_version", "operation", "status", "checks", "errors", "next_actions", "data"}
        )
        return result.returncode, value

    def initialize(self):
        code, value = self.run_cli(*self.arguments)
        self.assertEqual(code, 0, value)
        self.assertEqual(value["status"], "ok")
        return value

    def assert_invalid(self, error="unsafe_path"):
        code, value = self.run_cli(*self.arguments)
        self.assertEqual(code, 2, value)
        self.assertEqual(value["errors"][0]["code"], error)
        self.assertTrue(value["next_actions"])

    def test_init_creates_private_layout_and_unconfigured_profile(self):
        value = self.initialize()
        for directory in (self.state, self.config, *(self.state / name for name in ops.DIRECTORIES)):
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        profile = self.config / "profile.json"
        self.assertEqual(stat.S_IMODE(profile.stat().st_mode), 0o600)
        self.assertEqual(
            json.loads(profile.read_text()),
            {
                "schema_version": 1,
                "expected_project_id": None,
                "region": None,
                "credential_file": None,
                "wrapper_path": None,
                "openstack_client": None,
                "intended_server_ids": [],
                "known_seed_id": None,
            },
        )
        self.assertIn({"name": "cloud_readiness", "state": "not_assessed"}, value["checks"])
        self.assertTrue(value["next_actions"])

    def test_repeat_preserves_contents_inode_and_mtime_without_reading_profile(self):
        self.initialize()
        profile = self.config / "profile.json"
        profile.write_text("operator-edited-secret-marker")
        previous = profile.stat()
        artifact = self.state / "runs" / "keep"
        artifact.write_text("previous run")
        stdout = io.StringIO()
        with patch.object(Path, "read_text", side_effect=AssertionError("must not read existing files")):
            with contextlib.redirect_stdout(stdout):
                self.assertEqual(ops.main(self.arguments), 0)
        current = profile.stat()
        self.assertEqual((previous.st_ino, previous.st_mtime_ns), (current.st_ino, current.st_mtime_ns))
        self.assertEqual(profile.read_text(), "operator-edited-secret-marker")
        self.assertEqual(artifact.read_text(), "previous run")
        self.assertNotIn("secret-marker", stdout.getvalue())
        self.assertIn({"name": "profile", "state": "preserved"}, json.loads(stdout.getvalue())["checks"])

    def test_default_locations_use_home(self):
        code, value = self.run_cli("init", env={**os.environ, "HOME": str(self.root)})
        self.assertEqual(code, 0, value)
        self.assertTrue((self.root / ".local/share/flowdc-ops/inventory").is_dir())
        self.assertTrue((self.root / ".config/flowdc/profile.json").is_file())

    def test_parent_permissions_unchanged_and_missing_parents_private(self):
        self.root.chmod(0o755)
        self.state = self.root / "nested" / "state"
        self.arguments[2] = str(self.state)
        self.initialize()
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o755)
        self.assertEqual(stat.S_IMODE(self.state.parent.stat().st_mode), 0o700)

    def test_creation_modes_and_umask_restoration(self):
        for mask in (0, 0o777):
            with self.subTest(mask=mask), tempfile.TemporaryDirectory(dir=self.root) as directory:
                args = ["init", "--state-root", f"{directory}/state", "--config-root", f"{directory}/config"]
                old = os.umask(mask)
                try:
                    with contextlib.redirect_stdout(io.StringIO()):
                        self.assertEqual(ops.main(args), 0)
                    observed_mask = os.umask(mask)
                    self.assertEqual(observed_mask, mask)
                finally:
                    os.umask(old)
                self.assertEqual(stat.S_IMODE(Path(directory, "state").stat().st_mode), 0o700)
                self.assertEqual(stat.S_IMODE(Path(directory, "config/profile.json").stat().st_mode), 0o600)

    def test_rejects_symlink_root_without_writing_target(self):
        target = self.root / "target"
        target.mkdir(mode=0o700)
        self.state.symlink_to(target, target_is_directory=True)
        self.assert_invalid()
        self.assertEqual(list(target.iterdir()), [])
        self.assertFalse(self.config.exists())

    def test_rejects_symlink_ancestor_and_dangling_link(self):
        target = self.root / "target"
        target.mkdir(mode=0o700)
        link = self.root / "link"
        link.symlink_to(target, target_is_directory=True)
        self.arguments[2] = str(link / "state")
        self.assert_invalid()
        self.assertEqual(list(target.iterdir()), [])
        self.arguments[2] = str(self.state)
        self.state.symlink_to(self.root / "missing")
        self.assert_invalid()

    def test_rejects_symlink_child_before_other_creation(self):
        self.state.mkdir(mode=0o700)
        (self.state / "runs").symlink_to(self.root / "missing")
        self.assert_invalid()
        self.assertFalse((self.state / "inventory").exists())
        self.assertFalse(self.config.exists())

    def test_rejects_symlink_profile_and_preserves_target(self):
        self.config.mkdir(mode=0o700)
        target = self.root / "target"
        target.write_text("keep me")
        (self.config / "profile.json").symlink_to(target)
        self.assert_invalid()
        self.assertEqual(target.read_text(), "keep me")
        self.assertFalse(self.state.exists())

    def test_rejects_nonregular_profile(self):
        self.config.mkdir(mode=0o700)
        profile = self.config / "profile.json"
        for kind in ("directory", "fifo"):
            with self.subTest(kind=kind):
                if kind == "directory":
                    profile.mkdir(mode=0o700)
                else:
                    os.mkfifo(profile, 0o600)
                self.assert_invalid()
                if kind == "directory":
                    profile.rmdir()
                else:
                    profile.unlink()
        self.assertFalse(self.state.exists())

    def test_rejects_hardlinked_profile(self):
        self.config.mkdir(mode=0o700)
        target = self.root / "target"
        target.write_text("private")
        target.chmod(0o600)
        (self.config / "profile.json").hardlink_to(target)
        self.assert_invalid()
        self.assertEqual(target.read_text(), "private")

    def test_rejects_nonprivate_root_and_keeps_mode(self):
        self.state.mkdir(mode=0o755)
        self.assert_invalid("unsafe_permissions")
        self.assertEqual(stat.S_IMODE(self.state.stat().st_mode), 0o755)
        self.assertEqual(list(self.state.iterdir()), [])

    def test_rejects_nonprivate_profile_and_keeps_mode(self):
        self.initialize()
        profile = self.config / "profile.json"
        profile.chmod(0o644)
        self.assert_invalid("unsafe_permissions")
        self.assertEqual(stat.S_IMODE(profile.stat().st_mode), 0o644)

    def test_rejects_wrong_owner(self):
        actual = self.root.stat()
        wrong_owner = os.stat_result(
            (
                actual.st_mode,
                actual.st_ino,
                actual.st_dev,
                actual.st_nlink,
                os.geteuid() + 1,
                actual.st_gid,
                actual.st_size,
                actual.st_atime,
                actual.st_mtime,
                actual.st_ctime,
            )
        )
        with self.assertRaises(ops.OpsError) as caught:
            ops.private_metadata(wrong_owner, directory=True)
        self.assertEqual(caught.exception.code, "unsafe_permissions")

    def test_rejects_untrusted_writable_ancestor_without_chmod(self):
        self.root.chmod(0o777)
        self.assert_invalid()
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o777)
        self.assertFalse(self.state.exists())

    def test_rejects_file_as_state_root(self):
        self.state.write_text("keep")
        self.assert_invalid()
        self.assertEqual(self.state.read_text(), "keep")

    def test_rejects_git_checkout_roots(self):
        for kind in ("directory", "worktree_file"):
            with self.subTest(kind=kind):
                marker = self.root / ".git"
                if kind == "directory":
                    marker.mkdir()
                    (marker / "HEAD").write_text("ref: refs/heads/main\n")
                else:
                    marker.write_text("gitdir: /not/read")
                self.assert_invalid("git_workspace")
                if kind == "directory":
                    (marker / "HEAD").unlink()
                    marker.rmdir()
                else:
                    marker.unlink()
        self.assertFalse(self.state.exists())

    def test_empty_git_marker_is_not_a_checkout(self):
        (self.root / ".git").mkdir()
        self.initialize()

    def test_rejects_relative_traversal_and_overlapping_roots(self):
        for path in (
            "relative",
            "/",
            f"{self.root}/../escape",
            str(self.config),
            str(self.config / "state"),
            f"/{self.config}",
        ):
            with self.subTest(path=path):
                self.arguments[2] = path
                self.assert_invalid()
        self.assertFalse(self.config.exists())

    def test_disappearing_existing_profile_is_an_operational_error(self):
        self.initialize()
        stdout = io.StringIO()
        with patch.object(ops, "preflight_root"):
            original_stat = ops.os.stat

            def disappeared(path, *args, **kwargs):
                if path == "profile.json":
                    raise FileNotFoundError
                return original_stat(path, *args, **kwargs)

            with patch.object(ops.os, "stat", side_effect=disappeared):
                with contextlib.redirect_stdout(stdout):
                    self.assertEqual(ops.main(self.arguments), 1)
        self.assertEqual(json.loads(stdout.getvalue())["errors"][0]["code"], "concurrent_change")

    def test_paths_with_shell_text_remain_literal_data(self):
        self.arguments[2] = str(self.root / "state; $(touch should-not-exist)")
        self.initialize()
        self.assertTrue(Path(self.arguments[2]).is_dir())
        self.assertEqual(
            sorted(path.name for path in self.root.iterdir()), ["config", "state; $(touch should-not-exist)"]
        )

    def test_errors_and_help_are_json_and_do_not_echo_argument_values(self):
        for args in ([], ["inventory"], ["--token=secret-marker"], ["init", "--state", "secret-marker"]):
            with self.subTest(args=args):
                code, value = self.run_cli(*args)
                self.assertEqual(code, 2)
                self.assertEqual(value["errors"][0]["code"], "invalid_arguments")
                self.assertNotIn("secret-marker", json.dumps(value))
        code, value = self.run_cli("--help")
        self.assertEqual(code, 0)
        self.assertEqual(value["operation"], "help")
        self.assertIn("init", value["data"]["help"])

    def test_local_io_error_suppresses_raw_exception_and_restores_umask(self):
        old = os.umask(0o027)
        stdout = io.StringIO()
        try:
            with patch.object(ops.os, "mkdir", side_effect=OSError("provider-secret-marker")):
                with contextlib.redirect_stdout(stdout):
                    self.assertEqual(ops.main(self.arguments), 1)
            self.assertEqual(os.umask(0o027), 0o027)
        finally:
            os.umask(old)
        value = json.loads(stdout.getvalue())
        self.assertEqual(value["errors"][0]["code"], "local_io_error")
        self.assertNotIn("secret-marker", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
