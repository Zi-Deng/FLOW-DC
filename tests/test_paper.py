"""A changed live input must not produce a successful publication receipt."""

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class BuildBindingTests(unittest.TestCase):
    def test_concurrent_input_edit_aborts_publication(self):
        script = Path(__file__).resolve().parents[1] / "paper/scripts/build.py"
        spec = importlib.util.spec_from_file_location("manuscript_builder", script)
        builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(builder)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "input.tex"
            source.write_text("original staged source")

            def edit_during_compile(work):
                self.assertEqual((work / "input.tex").read_text(), "original staged source")
                source.write_text("changed while compiling")

            with (
                patch.object(builder, "ROOT", root),
                patch.object(builder, "INPUTS", ["input.tex"]),
                patch.object(builder, "compile_paper", side_effect=edit_during_compile),
            ):
                with self.assertRaisesRegex(ValueError, "inputs changed"):
                    builder.build(root / "output")
            self.assertFalse((root / "output/receipt.json").exists())
            self.assertFalse((root / "output/FLOW-DC-source.zip").exists())
