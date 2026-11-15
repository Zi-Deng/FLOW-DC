"""Package staging and source-bound native runtime checks without cluster access."""

import hashlib
import json
import tempfile
import unittest
import zipfile
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

from flowdc.distributed import bundle_package
from flowdc.distributed.runtime import verify_runtime


class StagingTests(unittest.TestCase):
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
