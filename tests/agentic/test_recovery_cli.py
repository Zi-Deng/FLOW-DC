"""The recovery interface exposes an explicit preview/application, never execution."""

import contextlib
import io
import sys
import unittest
from unittest.mock import patch

from test_workflow import workflow


class RecoveryInterfaceTests(unittest.TestCase):
    def test_recovery_help_offers_apply_and_digest_without_repository_or_authentication(self):
        output = io.StringIO()
        with (
            patch.object(sys, "argv", ["workflow.py", "claude-diagnostic-recovery", "--help"]),
            patch.object(workflow, "Repo") as repository,
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(output),
            self.assertRaises(SystemExit) as exited,
        ):
            workflow.main()
        self.assertEqual(exited.exception.code, 0, output.getvalue())
        self.assertIn("--apply", output.getvalue())
        self.assertIn("--preview-digest", output.getvalue())
        self.assertNotIn("--execute", output.getvalue())
        repository.assert_not_called()
