"""Offline TaskVine artifact/scope invariants; real runtime fixtures are separate."""

import copy
import io
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
from flowdc_shared_state import Ledger  # noqa: E402
from flowdc_staging import WORKER_FILES  # noqa: E402
from flowdc_vine import Reconciler, dispatch_records, prepare, validate_config  # noqa: E402
from flowdc_vine_protocol import digest, encode, pack_return, unpack_return, write_new  # noqa: E402

from benchmark.core.truth import Truth  # noqa: E402


class VineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def bundle(self, name="source"):
        source = self.root / name
        source.mkdir()
        write_new(source / "identity.json", {"attempt": "one"})
        write_new(source / "receipt.json", {"status": "returned"})
        (source / "native.tar").write_bytes(b"known native artifact")
        archive = self.root / (name + ".tar")
        pack_return(source, archive)
        return archive

    def test_native_hardlinked_publication_is_packaged_as_regular_bytes(self):
        source = self.root / "hardlinks"
        source.mkdir()
        write_new(source / "identity.json", {"attempt": "one"})
        write_new(source / "receipt.json", {"status": "returned"})
        (source / "original").write_bytes(b"original bytes")
        os.link(source / "original", source / "published")
        archive = self.root / "hardlinks.tar"
        pack_return(source, archive)
        unpack_return(archive, self.root / "returned")
        self.assertEqual((self.root / "returned/published").read_bytes(), b"original bytes")

    def test_return_roundtrip_and_reject_collision(self):
        archive = self.bundle()
        target = self.root / "returned"
        identity, receipt, sha = unpack_return(archive, target)
        self.assertEqual(identity, {"attempt": "one"})
        self.assertEqual(receipt["status"], "returned")
        self.assertEqual(sha, digest(archive.read_bytes()))
        with self.assertRaises(FileExistsError):
            unpack_return(archive, target)
        self.assertEqual((target / "native.tar").read_bytes(), b"known native artifact")

    def test_corruption_truncation_duplicate_unexpected_unsafe_fail_before_extraction(self):
        raw = self.bundle().read_bytes()
        for kind in ("corrupt", "truncated", "duplicate", "unsafe", "unexpected", "link"):
            path = self.root / (kind + ".tar")
            if kind == "corrupt":
                path.write_bytes(raw.replace(b"known native artifact", b"wrong native artifact"))
            elif kind == "truncated":
                path.write_bytes(raw[:2048])
            else:
                with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as old, tarfile.open(path, "w") as new:
                    for member in old:
                        new.addfile(member, old.extractfile(member))
                    entry = tarfile.TarInfo(
                        {
                            "duplicate": "native.tar",
                            "unsafe": "../escape",
                            "unexpected": "extra",
                            "link": "bad-link",
                        }[kind]
                    )
                    if kind == "link":
                        entry.type, entry.linkname = tarfile.SYMTYPE, "native.tar"
                    new.addfile(entry, io.BytesIO(b""))
            with self.subTest(kind=kind), self.assertRaises((ValueError, tarfile.TarError, KeyError)):
                unpack_return(path, self.root / (kind + "-output"))
            self.assertFalse((self.root / (kind + "-output")).exists())

    def test_all_worker_modules_import_from_isolated_sandbox(self):
        for name in WORKER_FILES:
            shutil.copyfile(ROOT / "bin" / name, self.root / name)
        result = subprocess.run(
            [sys.executable, "-B", "flowdc_vine_worker.py", "--help"],
            cwd=self.root,
            env={**os.environ, "PYTHONPATH": ""},
            capture_output=True,
            timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertIn(b"--spec", result.stdout)

    def config(self):
        return {
            "distributed_profile": "shared-origin-v1",
            "original_manifest": str(self.root / "input.parquet"),
            "catalog": str(self.root / "catalog.json"),
            "output_directory": str(self.root / "output"),
            "environment_archive": str(self.root / "env.tar.gz"),
            "environment_sha256": digest(b"environment"),
            "workers": 2,
            "download": {"control_method": "fixed-v1"},
        }

    def test_profile_explicit_defaults_and_bounds_fail_closed(self):
        config, download = validate_config(self.config())
        self.assertEqual(config["max_attempts"], 1)
        self.assertTrue(download.research_profile)
        self.assertFalse(download.force_overwrite)
        for field, value in (
            ("workers", 3),
            ("workers", True),
            ("max_attempts", 0),
            ("deadline_s", 181),
            ("task_deadline_s", float("nan")),
            ("distributed_profile", "unknown"),
        ):
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_config({**self.config(), field: value})
        for options in (
            {},
            {"control_method": "img2dataset"},
            {"control_method": "fixed-v1", "force_overwrite": True},
        ):
            with self.assertRaises(ValueError):
                validate_config({**self.config(), "download": options})

    def test_metadata_rejected_before_output_or_http(self):
        config = self.config()
        pl.DataFrame({"url": ["http://example.test/x"], "nested": [[1]]}).write_parquet(
            config["original_manifest"]
        )
        Path(config["catalog"]).write_bytes(encode({"http://example.test/x": None}))
        with self.assertRaisesRegex(ValueError, "dtype"):
            prepare(config)
        self.assertFalse(Path(config["output_directory"]).exists())

    def test_actual_dispatch_records_do_not_infer_execution_from_unknown_metric(self):
        path = self.root / "transactions"
        path.write_text(
            "# example\n12 34 TASK 1 READY 0\n13 34 TASK 1 RUNNING worker-a 1\n14 34 TASK 1 RUNNING worker-b 1\n"
        )
        records = dispatch_records(self.root)
        self.assertEqual([record["worker_id"] for record in records], ["worker-a", "worker-b"])
        self.assertEqual([record["task_id"] for record in records], [1, 1])

    def test_duplicate_attempt_and_logical_rows_have_no_duplicate_credit(self):
        url = "http://example.test/object"
        pl.DataFrame({"url": [url, url, None]}).write_parquet(self.root / "input.parquet")
        truth = Truth.load(self.root / "input.parquet", {url: {"bytes": 7, "sha256": "c" * 64}}).record
        rows = [row["row_id"] for row in truth["rows"] if row["eligible"]]
        spec = {
            "scope_id": uuid4().hex,
            "binding": {},
            "partition_sha256": "a" * 64,
            "row_ids": rows,
            "files": {},
            "environment_sha256": "b" * 64,
        }
        result = Reconciler(truth)
        attempt = uuid4().hex
        identity = {"schema": "flowdc-vine-attempt-v1", "attempt_id": attempt, **spec}
        identity["source_files"] = identity.pop("files")
        receipt = {
            "schema": "flowdc-vine-receipt-v1",
            "attempt_id": attempt,
            "scope_id": spec["scope_id"],
            "status": "returned",
        }
        verification = {
            "rows": [
                {"row_id": row, "position": i, "disposition": "verified", "useful_bytes": 7, "error": None}
                for i, row in enumerate(rows)
            ],
            "artifacts_valid": True,
        }
        clients = {attempt: {"scope": spec["scope_id"]}}
        with (
            patch("flowdc_vine.unpack_return", return_value=(identity, receipt, "d" * 64)),
            patch("flowdc_vine.verify_native", return_value=verification),
            patch("flowdc_vine.write_new"),
        ):
            result.accept(self.root / "one", "unused", spec, {"successful": True}, clients)
            result.accept(self.root / "two", "unused", spec, {"successful": True}, clients)
        self.assertEqual(result.summary()["original_rows"], 3)
        self.assertEqual(result.summary()["useful_bytes"], 14)
        self.assertTrue(result.returns[-1]["duplicate"])
        with patch("flowdc_vine.unpack_return", return_value=(identity, receipt, "e" * 64)):
            result.accept(self.root / "conflict", "unused", spec, {"successful": True}, clients)
        self.assertFalse(result.returns[-1]["accepted"])
        self.assertTrue(result.errors)

    def test_owner_quiescence_retains_unknown_work_and_late_ack_is_not_new_feedback(self):
        binding = {
            "run_id": uuid4().hex,
            "source_sha256": "a" * 64,
            "config_sha256": "b" * 64,
            "method": "fixed-v1",
        }
        ledger = Ledger(self.root / "ledger", binding)
        self.addCleanup(ledger.close)
        scope, client = uuid4().hex, uuid4().hex
        ledger.enroll(scope, ["c" * 64])
        ledger.connect(scope, client, "worker", 1)
        ledger.configure_origin("http://example.test/x", 1)
        permit = ledger.acquire(scope, client, "c" * 64, uuid4().hex, "http://example.test/x")["permit_id"]
        ledger.dispatch(scope, client, permit, 1)
        proof = {
            "kind": "owned_process_tree_exit",
            "client_id": client,
            "run_id": binding["run_id"],
            "source_sha256": binding["source_sha256"],
            "evidence_sha256": "d" * 64,
        }
        before = copy.deepcopy(ledger.snapshot())
        for key, value in (
            ("kind", "heartbeat_expired"),
            ("client_id", uuid4().hex),
            ("run_id", uuid4().hex),
        ):
            with self.assertRaises(ValueError):
                ledger.prove_quiescent(client, {**proof, key: value})
            self.assertEqual(ledger.snapshot(), before)
        self.assertFalse(ledger.prove_quiescent(client, proof)["duplicate"])
        self.assertTrue(ledger.prove_quiescent(client, proof)["duplicate"])
        self.assertEqual(ledger.snapshot()["permits"][permit]["state"], "uncertain")
        self.assertEqual(len(ledger.outstanding(ledger.current())), 1)
        ledger.fence("stopped")
        with self.assertRaises(ValueError):
            ledger.recover_closed_epoch()
        self.assertFalse(ledger.prove_origin_drained(client, "e" * 64)["duplicate"])
        self.assertTrue(ledger.prove_origin_drained(client, "e" * 64)["duplicate"])
        self.assertEqual(ledger.snapshot()["permits"][permit]["state"], "quiescent")
        self.assertIsNone(ledger.snapshot()["permits"][permit]["completion"])
        self.assertEqual(ledger.outstanding(ledger.current()), [])
        ledger.fence("stopped")
        self.assertEqual(ledger.recover_closed_epoch(), 2)
        late = ledger.complete(scope, client, permit, {"status": 503, "retry_after": 2})
        self.assertTrue(late["late"])
        self.assertTrue(late["duplicate"])
        self.assertTrue(
            ledger.complete(scope, client, permit, {"status": 503, "retry_after": 2})["duplicate"]
        )


if __name__ == "__main__":
    unittest.main()
