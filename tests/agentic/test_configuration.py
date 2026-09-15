"""User-visible configuration defaults and invalid budget handling."""

import json
import tempfile
import unittest
from pathlib import Path

from test_workflow import workflow


class ConfigurationTests(unittest.TestCase):
    def config(self, **overrides):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".agentic").mkdir()
            (root / ".agentic/config.json").write_text(json.dumps({"schema_version": 1, **overrides}))
            return workflow.configuration(root)

    def test_missing_diff_cap_is_unlimited_and_prompt_remains_bounded(self):
        config = self.config()
        self.assertIsNone(config["max_diff_bytes"])
        self.assertEqual(config["managed_max_prompt_bytes"], 300_000)

    def test_explicit_null_or_positive_diff_cap_is_supported(self):
        for value in [None, 1, 300_000, 1_000_000]:
            with self.subTest(value=value):
                self.assertEqual(self.config(max_diff_bytes=value)["max_diff_bytes"], value)

    def test_invalid_diff_caps_are_rejected(self):
        for value in [True, False, 0, -1, "300000", "unlimited", 1.5, [], {}]:
            with self.subTest(value=value), self.assertRaisesRegex(workflow.WorkflowError, "max_diff_bytes"):
                self.config(max_diff_bytes=value)

    def test_prompt_cap_requires_positive_integer(self):
        self.assertEqual(self.config(managed_max_prompt_bytes=1)["managed_max_prompt_bytes"], 1)
        for value in [None, True, False, 0, -1, "300000", 1.5, [], {}]:
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(workflow.WorkflowError, "managed_max_prompt_bytes"),
            ):
                self.config(managed_max_prompt_bytes=value)
