"""User-visible configuration defaults and invalid budget handling."""

import json
import tempfile
import unittest
from pathlib import Path

from test_workflow import SOURCE, workflow


class ConfigurationTests(unittest.TestCase):
    def config(self, *, omit=(), **overrides):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".agentic").mkdir()
            base = json.loads((SOURCE / ".agentic/config.json").read_text(encoding="utf-8"))
            for key in ["max_diff_bytes", "managed_max_prompt_bytes", *omit]:
                base.pop(key, None)
            (root / ".agentic/config.json").write_text(json.dumps({**base, **overrides}), encoding="utf-8")
            return workflow.configuration(root)

    def test_missing_diff_cap_is_unlimited_and_prompt_remains_bounded(self):
        config = self.config()
        self.assertIsNone(config["max_diff_bytes"])
        self.assertEqual(config["managed_max_prompt_bytes"], 300_000)

    def test_schema_version_requires_the_supported_integer(self):
        for value in [None, True, "1", 0, 2]:
            with self.subTest(value=value), self.assertRaisesRegex(workflow.WorkflowError, "schema"):
                self.config(schema_version=value)

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

    def test_required_review_settings_fail_with_actionable_errors(self):
        for key in [
            "review_timeout_seconds",
            "review_max_ai_credits",
            "max_source_file_bytes",
            "max_snapshot_bytes",
            "openai_model",
            "copilot_model",
            "domain_rubric",
            "required_checks",
        ]:
            with self.subTest(key=key), self.assertRaisesRegex(workflow.WorkflowError, key):
                self.config(omit=[key])

    def test_other_budgets_reject_invalid_types_and_nonpositive_values(self):
        for key in [
            "review_timeout_seconds",
            "review_max_ai_credits",
            "max_source_file_bytes",
            "max_snapshot_bytes",
            "managed_timeout_seconds",
            "managed_max_output_bytes",
        ]:
            for value in [None, True, 0, -1, "100", 1.5]:
                with self.subTest(key=key, value=value), self.assertRaisesRegex(workflow.WorkflowError, key):
                    self.config(**{key: value})

    def test_model_rubric_and_check_names_are_validated(self):
        for key in ["openai_model", "copilot_model", "domain_rubric"]:
            for value in [None, True, 100, "", " ", []]:
                with self.subTest(key=key, value=value), self.assertRaisesRegex(workflow.WorkflowError, key):
                    self.config(**{key: value})
        for value in [None, "quality", [], [""], [False], [["quality"]], ["quality", "quality"]]:
            with self.subTest(value=value), self.assertRaisesRegex(workflow.WorkflowError, "required_checks"):
                self.config(required_checks=value)
