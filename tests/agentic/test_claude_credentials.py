"""Protected subscription setup uses temporary fake credentials, never real accounts."""

import contextlib
import datetime as dt
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_workflow import workflow

# isort: split
import claude_credentials as credentials

TOKEN = "sk-ant-oat01-fixture-not-a-real-token"


class CredentialTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "dedicated/token"

    def setup_token(self, **kwargs):
        with (
            patch.object(credentials.sys.stdin, "isatty", return_value=True),
            patch.object(credentials.getpass, "getpass", return_value=TOKEN),
        ):
            return credentials.setup(self.path, paid_usage_disabled=True, **kwargs)

    def test_hidden_setup_produces_private_bound_receipt_without_output(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            result = self.setup_token()
        self.assertNotIn(TOKEN, output.getvalue() + json.dumps(result))
        self.assertEqual(credentials.read(self.path), TOKEN)
        self.assertEqual(self.path.parent.stat().st_mode & 0o777, 0o700)
        for path in (self.path, credentials.receipt_path(self.path)):
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(workflow.WorkflowError):
            self.setup_token()

    def test_setup_requires_receipt_assertion_and_real_hidden_input(self):
        with self.assertRaises(workflow.WorkflowError):
            credentials.setup(self.path)
        with (
            patch.object(credentials.sys.stdin, "isatty", return_value=False),
            patch.object(credentials.getpass, "getpass") as prompt,
        ):
            with self.assertRaises(workflow.WorkflowError):
                credentials.setup(self.path, paid_usage_disabled=True)
            prompt.assert_not_called()
        self.assertFalse(self.path.exists())

    def test_missing_expired_future_changed_receipts_refuse_without_secret_errors(self):
        self.setup_token()
        receipt_path = credentials.receipt_path(self.path)
        original = receipt_path.read_bytes()
        receipt_path.unlink()
        with self.assertRaises(workflow.WorkflowError) as caught:
            credentials.read(self.path)
        self.assertNotIn(TOKEN, str(caught.exception))
        credentials.write_private(receipt_path, original)
        for now in [
            dt.datetime.now(dt.UTC) + dt.timedelta(days=8),
            dt.datetime.now(dt.UTC) - dt.timedelta(days=1),
        ]:
            with self.assertRaises(workflow.WorkflowError):
                credentials.read(self.path, now=now)
        self.path.write_text(TOKEN + "changed")
        with self.assertRaises(workflow.WorkflowError):
            credentials.read(self.path)

    def test_unsafe_file_modes_symlinks_hardlinks_and_fifo_are_refused(self):
        self.setup_token()
        self.path.chmod(0o644)
        with self.assertRaises(workflow.WorkflowError):
            credentials.read(self.path)
        with self.assertRaises(workflow.WorkflowError):
            self.setup_token(replace=True)
        self.path.chmod(0o600)
        link = self.path.parent / "hardlink"
        os.link(self.path, link)
        with self.assertRaises(workflow.WorkflowError):
            credentials.read(self.path)
        link.unlink()
        self.path.unlink()
        self.path.symlink_to(credentials.receipt_path(self.path))
        with self.assertRaises(workflow.WorkflowError):
            credentials.read(self.path)
        self.path.unlink()
        os.mkfifo(self.path, 0o600)
        with self.assertRaises(workflow.WorkflowError):
            credentials.read(self.path)

    def test_checkout_and_symlinked_directory_are_refused(self):
        (self.root / ".git").mkdir()
        (self.root / ".git/HEAD").write_text("ref: refs/heads/main\n")
        with self.assertRaises(workflow.WorkflowError):
            self.setup_token()
        (self.root / ".git/HEAD").unlink()
        (self.root / ".git").rmdir()
        target = self.root / "other"
        target.mkdir(mode=0o700)
        self.path.parent.symlink_to(target)
        with self.assertRaises(workflow.WorkflowError):
            self.setup_token()
        self.assertEqual(list(target.iterdir()), [])

    def test_ordinary_claude_login_remains_untouched(self):
        ordinary = self.root / ".claude"
        ordinary.mkdir()
        login = ordinary / ".credentials.json"
        login.write_text("ordinary-login-fixture")
        with (
            patch.object(credentials.Path, "home", return_value=self.root),
            patch.object(credentials.sys.stdin, "isatty", return_value=True),
            patch.object(credentials.getpass, "getpass", return_value=TOKEN),
        ):
            credentials.setup(paid_usage_disabled=True)
            dedicated = self.root / ".config/flowdc-agentic/claude-review-token"
            self.assertEqual(credentials.read(dedicated), TOKEN)
            self.assertTrue(credentials.receipt_path(dedicated).is_file())
        self.assertEqual(login.read_text(), "ordinary-login-fixture")
        self.assertEqual(list(ordinary.iterdir()), [login])
        self.assertFalse(self.path.exists())

    def test_api_key_and_whitespace_are_rejected(self):
        for value in [b"sk-ant-api03-fake-api-key", b"arbitrary-secret", TOKEN.encode() + b"\n", b"\xff"]:
            with self.subTest(value=value[:5]), self.assertRaises(workflow.WorkflowError):
                credentials.validate_token(value)
