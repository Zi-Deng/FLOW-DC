"""Package staging and source-bound native runtime checks without cluster access."""

import hashlib
import json
import tempfile
import unittest
import zipfile
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

from flowdc.config import DownloadConfig
from flowdc.distributed import bundle_package, run_distributed
from flowdc.distributed.runtime import verify_runtime
from flowdc.partition import partition_manifest


class StagingTests(unittest.TestCase):
    def test_explicit_manifest_format_and_preflight_before_destination_creation(self):
        import sys

        import polars as pl

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "manifest.without_csv_extension"
            pl.DataFrame({"url": ["http://fixture.invalid/a"]}).write_csv(source)
            parts = partition_manifest(source, root / "parts", input_format="csv")
            self.assertEqual(pl.read_parquet(parts[0])["url"].to_list(), ["http://fixture.invalid/a"])
            pl.DataFrame({"wrong": [1]}).write_csv(source)
            receipt = root / "runtime.json"
            receipt.write_text("{}")
            with self.assertRaises(ValueError):
                run_distributed(
                    DownloadConfig(input_path=str(source), input_format="csv", output_dir=str(root / "run")),
                    {
                        "manager_python": sys.executable,
                        "worker_binary": sys.executable,
                        "runtime_receipt": str(receipt),
                        "start_local_workers": True,
                    },
                )
            self.assertFalse((root / "run").exists())

    def test_bundle_is_complete_deterministic_and_excludes_operations(self):
        with tempfile.TemporaryDirectory() as temporary:
            a, b = [Path(temporary) / n for n in ("a.zip", "b.zip")]
            self.assertEqual(bundle_package(a), bundle_package(b))
            with zipfile.ZipFile(a) as archive:
                names = archive.namelist()
                self.assertIn("flowdc/distributed/cctools.json", names)
                self.assertIn("flowdc/integrity.py", names)
                self.assertFalse(any("__pycache__" in n or "/ops/" in n for n in names))

    def test_build_receipt_checks_actual_binding_worker_and_patch_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pin = json.loads(files("flowdc.distributed").joinpath("cctools.json").read_text())
            binding, worker = root / "_cvine.so", root / "vine_worker"
            binding.write_bytes(b"binding fixture")
            worker.write_bytes(b"worker fixture")
            receipt = {
                "source_revision": pin["revision"],
                "patch_sha256": pin["patch_sha256"],
                "binaries": {
                    "lib/python3.12/site-packages/ndcctools/taskvine/_cvine.so": hashlib.sha256(
                        binding.read_bytes()
                    ).hexdigest(),
                    "bin/vine_worker": hashlib.sha256(worker.read_bytes()).hexdigest(),
                },
            }
            path = root / "build.json"
            path.write_text(json.dumps(receipt))
            vine = SimpleNamespace(__version__=pin["release"], __file__=str(root / "__init__.py"))
            self.assertEqual(verify_runtime(vine, path, worker)["version"], pin["release"])
            worker.write_bytes(b"different build")
            with self.assertRaisesRegex(ValueError, "artifact differs"):
                verify_runtime(vine, path, worker)
            receipt["patch_sha256"] = "0" * 64
            path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "source patch"):
                verify_runtime(vine, path)
