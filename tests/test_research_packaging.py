"""Packaging must reject writes under its read-only source before touching it."""

import contextlib
import io
import runpy
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


class PackagingTests(unittest.TestCase):
    def test_output_inside_source_or_symlinked_parent_refused_before_packaging(self):
        source = Path(__file__).resolve().parents[1] / "benchmark/package_environment.py"
        for symlink in (False, True):
            with self.subTest(symlink=symlink), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                script = root / "benchmark/package_environment.py"
                script.parent.mkdir()
                script.write_bytes(source.read_bytes())
                prefix = root / ".agentic-local/env"
                prefix.mkdir(parents=True)
                parent = prefix
                if symlink:
                    parent = root / "alias"
                    parent.symlink_to(prefix, target_is_directory=True)
                output = parent / "package"
                calls = Mock(side_effect=SystemExit(0))
                conda = types.ModuleType("conda_pack")
                conda.CondaEnv = types.SimpleNamespace(from_prefix=calls)
                core = types.ModuleType("conda_pack.core")
                core.File = Mock()
                package = types.ModuleType("ndcctools")
                vine = types.ModuleType("ndcctools.taskvine")
                vine.cvine = types.SimpleNamespace(vine_version_string=lambda: "7.17.2")
                package.taskvine = vine
                poncho = types.ModuleType("ndcctools.poncho")
                create = types.ModuleType("ndcctools.poncho.package_create")
                create._copy_run_in_env = Mock()
                modules = {
                    "conda_pack": conda,
                    "conda_pack.core": core,
                    "ndcctools": package,
                    "ndcctools.taskvine": vine,
                    "ndcctools.poncho": poncho,
                    "ndcctools.poncho.package_create": create,
                }
                with (
                    patch.dict(sys.modules, modules),
                    patch.object(sys, "prefix", str(prefix)),
                    patch.object(sys, "argv", [str(script), "--output", str(output)]),
                    contextlib.redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit) as stopped,
                ):
                    runpy.run_path(str(script), run_name="__main__")
                self.assertEqual(stopped.exception.code, 2)
                calls.assert_not_called()
                self.assertFalse(output.exists())
