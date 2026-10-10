"""Packaging must reject writes under its read-only source before touching it."""

import contextlib
import io
import json
import runpy
import sys
import tarfile
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


class PackagingTests(unittest.TestCase):
    def test_installed_native_patch_overrides_original_conda_cache(self):
        source = Path(__file__).resolve().parents[1] / "benchmark/package_environment.py"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "benchmark/package_environment.py"
            script.parent.mkdir()
            script.write_bytes(source.read_bytes())
            prefix, cache = root / ".agentic-local/env", root / "cache"
            names = ("bin/vine_worker", "lib/python3.12/site-packages/ndcctools/taskvine/_cvine.so")
            for name in names:
                for base, contents in ((prefix, b"patched"), (cache, b"unpatched")):
                    path = base / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(contents)

            class File:
                def __init__(self, source, target, **kwargs):
                    self.source, self.target = source, target

            class Env:
                def __init__(self, prefix, files):
                    self.files = files

                @classmethod
                def from_prefix(cls, prefix, **kwargs):
                    return cls(prefix, [File(str(cache / name), name) for name in names])

                def pack(self, output, **kwargs):
                    with tarfile.open(output, "w:gz") as archive:
                        for file in self.files:
                            archive.add(file.source, arcname=file.target)

            def launchers(path):
                for name in ("run_in_env", "poncho_package_run"):
                    (Path(path) / "env/bin" / name).write_bytes(b"launcher")

            conda, core = types.ModuleType("conda_pack"), types.ModuleType("conda_pack.core")
            conda.CondaEnv, conda.__version__, core.File = Env, "test", File
            package, vine = types.ModuleType("ndcctools"), types.ModuleType("ndcctools.taskvine")
            vine.cvine = types.SimpleNamespace(vine_version_string=lambda: "7.17.2")
            package.taskvine = vine
            poncho, create = types.ModuleType("ndcctools.poncho"), types.ModuleType("ndcctools.poncho.package_create")
            create._copy_run_in_env = launchers
            output = root / "package"
            with (
                patch.dict(sys.modules, {"conda_pack": conda, "conda_pack.core": core,
                                         "ndcctools": package, "ndcctools.taskvine": vine,
                                         "ndcctools.poncho": poncho, "ndcctools.poncho.package_create": create}),
                patch.object(sys, "prefix", str(prefix)),
                patch.object(sys, "argv", [str(script), "--output", str(output)]),
                patch("importlib.metadata.version", return_value="test"),
                patch.dict("os.environ"),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                runpy.run_path(str(script), run_name="__main__")
            with tarfile.open(output / "research-env.tar.gz") as archive:
                for name in names:
                    self.assertEqual(archive.extractfile(name).read(), b"patched")
                    self.assertEqual((prefix / name).read_bytes(), b"patched")
                    self.assertEqual((cache / name).read_bytes(), b"unpatched")
            receipt = json.loads((output / "result.json").read_text())
            self.assertTrue(receipt["archive_native_binaries_match"])
            self.assertTrue(receipt["source_unchanged"])

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
