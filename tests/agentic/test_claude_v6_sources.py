"""Offline extracted native branches with synthetic dependencies, never live evidence."""

import hashlib
import json
import shutil
import subprocess
import unittest
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


class NativeV6Sources(unittest.TestCase):
    def run_fixture(self, name):
        node = shutil.which("node")
        self.assertIsNotNone(node, "Node is required for native pure fixtures")
        result = subprocess.run(
            [node, str(FIXTURES / (name + ".mjs"))], capture_output=True, text=True, timeout=10
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_normal_transport_preserves_exact_optional_direct_caller(self):
        result = self.run_fixture("claude-transport-2.1.282-v6")
        expected = json.loads((FIXTURES / "claude-transport-2.1.282-v6-expected.json").read_text())
        self.assertEqual(result, expected)
        self.assertEqual(result["cases"]["direct"]["message"]["content"][0]["caller"], {"type": "direct"})
        self.assertNotIn("caller", result["cases"]["absent"]["message"]["content"][0])
        # Transport preserves unsupported metadata too; the wrapper must refuse it.
        self.assertIsNone(result["cases"]["nullCaller"]["message"]["content"][0]["caller"])

    def test_directory_render_and_single_file_difference(self):
        result = self.run_fixture("claude-grep-2.1.282-v6")
        evidence = json.loads((FIXTURES / "claude-grep-2.1.282-v6.json").read_text())
        self.assertEqual(result["outputs"], evidence["outputs"])
        self.assertEqual(result["command"], evidence["command"])

    def test_fully_typed_grep_normalization_is_unchanged(self):
        self.assertEqual(
            self.run_fixture("claude-grep-normalization-2.1.282-v6"),
            {
                "typed_command_unchanged": True,
                "absent_and_direct_caller_preserved": True,
                "not_live_evidence": True,
            },
        )

    def test_source_ranges_and_executed_normalization_bodies_have_exact_hashes(self):
        for name in (
            "claude-transport-2.1.282-v6",
            "claude-grep-2.1.282-v6",
            "claude-grep-normalization-2.1.282-v6",
        ):
            evidence = json.loads((FIXTURES / (name + ".json")).read_text())
            for row in evidence["ranges"] + evidence.get("read_input_ranges", []):
                with self.subTest(name=name, label=row["label"]):
                    raw = row["source"].encode()
                    self.assertEqual(len(raw), row["end"] - row["start"])
                    self.assertEqual(hashlib.sha256(raw).hexdigest(), row["sha256"])
                    if name == "claude-grep-normalization-2.1.282-v6" and row["source"].startswith(
                        "function "
                    ):
                        self.assertIn(row["source"], (FIXTURES / (name + ".mjs")).read_text())
