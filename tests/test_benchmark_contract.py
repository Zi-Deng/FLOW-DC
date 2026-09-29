"""Benchmark regressions; offline evidence does not replace real-client V2."""

import asyncio
import io
import json
import os
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import polars as pl
import psutil

from benchmark.compare_results import load_report
from benchmark.core.flowdc_adapter import FlowDCAdapter, FlowDCConfig
from benchmark.core.http_cases import CASES, case_plan, policy_record
from benchmark.core.img2dataset_adapter import Img2DatasetAdapter, Img2DatasetConfig
from benchmark.core.lifecycle import run_verified
from benchmark.core.metrics import BenchmarkResult, ResourceMetrics
from benchmark.core.runner import BenchmarkConfig, BenchmarkRunner
from benchmark.core.truth import PROVENANCE, Truth, digest, encode
from benchmark.core.verifier import archive_members, verify_native


class BenchmarkRegressions(unittest.TestCase):
    def test_response_cases_are_predetermined_and_bounded(self):
        payloads = {"JPEG": b"jpeg-original", "PNG": b"png-original"}
        for name in CASES:
            with self.subTest(name=name):
                plan = case_plan(name, payloads)
                self.assertLessEqual(len(plan["paths"]) + 3, 256)
                self.assertLessEqual(plan["process_deadline"], 180)
                self.assertEqual(
                    set(plan["paths"]),
                    set(plan["objects"])
                    if name != "http-failure"
                    else set(plan["objects"]) - {"/alias.png", "/plain.png"},
                )
                self.assertEqual(json.loads(encode(policy_record(plan)))["case"], name)
        retry = case_plan("retry", payloads)
        self.assertEqual(retry["attempt_budget"], 2)
        self.assertEqual([r.status for r in retry["policies"]["/retry429.jpg"]], [429, 200])
        failure = case_plan("http-failure", payloads)
        self.assertEqual(failure["attempt_budget"], 1)
        self.assertTrue(failure["policies"]["/truncated.jpg"][0].truncate)
        self.assertGreater(
            failure["policies"]["/delayed.jpg"][0].first_byte_delay, failure["request_timeout"]
        )

    def test_historical_reader_labels_old_reports_and_refuses_new_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text('{"metadata":{},"results":{}}')
            self.assertEqual(load_report(path)["benchmark_schema"], "historical-native-counters-v1")
            path.write_text('{"schema":"flowdc-known-truth-v1"}')
            with self.assertRaisesRegex(ValueError, "do not mix"):
                load_report(path)

    def test_adapters_refuse_existing_evidence_without_deletion(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            sentinel = path / "keep"
            sentinel.write_bytes(b"retained evidence")
            configurations = [
                (FlowDCAdapter(), FlowDCConfig("unused", directory, "url", None, 2, 5, True)),
                (Img2DatasetAdapter(), Img2DatasetConfig("unused", directory, "url", 2)),
            ]
            for adapter, config in configurations:
                with self.assertRaises(FileExistsError):
                    asyncio.run(adapter.run(config, 0))
                self.assertEqual(sentinel.read_bytes(), b"retained evidence")

    def test_serialization_preserves_measured_precision(self):
        value = 1.234567890123
        result = BenchmarkResult(
            "flowdc", "test", 1, 1, elapsed_seconds=value, throughput_mbps=value, success_rate_percent=value
        )
        record = result.to_dict()
        for field in ("elapsed_seconds", "throughput_mbps", "success_rate_percent"):
            self.assertEqual(record[field], value)
        self.assertEqual(ResourceMetrics(cpu_avg_percent=value).to_dict()["cpu_avg_percent"], value)

    def test_img2dataset_original_byte_command_is_explicit(self):
        config = Img2DatasetConfig("input.parquet", "output", "url", 2, output_format="webdataset")
        command = Img2DatasetAdapter().build_command(config)
        options = dict(zip(command[1::2], command[2::2], strict=True))
        self.assertEqual(options.get("--disable_all_reencoding"), "True")
        self.assertEqual(options.get("--extract_exif"), "False")
        self.assertEqual(options.get("--max_shard_retry"), "0")
        self.assertEqual(options.get("--retries"), "0")
        self.assertEqual(options.get("--output_format"), "webdataset")

    def test_runner_retains_native_artifacts_including_warmup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = BenchmarkConfig(
                "unused", output_base_dir=str(root), concurrency_levels=[2], warmup_runs=1, measured_runs=1
            )
            with patch.object(BenchmarkRunner, "_count_urls", return_value=1):
                runner = BenchmarkRunner(config)
            artifacts = []

            async def native_run(*, config, **kwargs):
                output = Path(config.output_folder)
                output.mkdir()
                artifact = output / "evidence.json"
                artifact.write_text(json.dumps({"partial": True}))
                artifacts.append(artifact)
                return BenchmarkResult("test", "test", 2, kwargs["run_number"])

            runner.flowdc.run = AsyncMock(side_effect=native_run)
            runner.img2dataset.run = AsyncMock(side_effect=native_run)
            asyncio.run(runner._run_flowdc(2, True))
            asyncio.run(runner._run_img2dataset(2))
            self.assertEqual(len(artifacts), 4)
            for artifact in artifacts:
                self.assertTrue(artifact.is_file(), str(artifact))


class KnownTruthFixtures(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.payload = b"original bytes with an intentionally misleading native suffix"
        self.urls = ["http://fixture.invalid/left/same.png", "http://fixture.invalid/right/same.png"]
        self.manifest = self.root / "input.parquet"
        self.frame = pl.DataFrame(
            {
                "url": [*self.urls, self.urls[0], None, "", "bad-url"],
                "label": ["a", None, "duplicate", "null", "blank", "bad"],
                "integer": [1, 2, 3, 4, 5, 6],
            }
        )
        self.frame.write_parquet(self.manifest)
        self.catalog = {
            url: {"bytes": len(self.payload), "sha256": digest(self.payload)} for url in self.urls
        }
        self.truth = Truth.load(self.manifest, self.catalog)
        self.native = self.root / "native"

    @staticmethod
    def tar(path, members):
        with tarfile.open(path, "w") as archive:
            for name, raw in members:
                info = tarfile.TarInfo(name)
                info.size = len(raw)
                archive.addfile(info, io.BytesIO(raw))

    def img_fixture(self, *, failed=False):
        self.native.mkdir()
        members, metadata = [], []
        for i, row in enumerate(self.truth.record["rows"][:3]):
            status = "failed_to_download" if failed and i == 1 else "success"
            key = f"{i:09d}"
            native = dict(row["metadata"])
            native.update(
                zip(
                    PROVENANCE,
                    (
                        self.truth.record["manifest_sha256"],
                        row["position"],
                        6,
                        row["row_id"],
                        digest(encode(row["metadata"])),
                    ),
                    strict=True,
                )
            )
            native.update(
                key=key,
                status=status,
                error_message="fixture failure" if status != "success" else None,
                width=None,
                height=None,
                original_width=None,
                original_height=None,
                sha256=digest(self.payload) if status == "success" else None,
            )
            metadata.append(native)
            if status == "success":
                members.extend([(key + ".jpg", self.payload), (key + ".json", encode(native))])
        pl.DataFrame(metadata).write_parquet(self.native / "00000.parquet")
        self.tar(self.native / "00000.tar", members)
        (self.native / "00000_stats.json").write_bytes(
            encode(
                {
                    "count": 3,
                    "successes": 2 if failed else 3,
                    "failed_to_download": 1 if failed else 0,
                    "failed_to_resize": 0,
                }
            )
        )
        return members

    def flow_fixture(self):
        # This calls the actual maintained native publication path, with no HTTP.
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
        import download_batch as base
        import flowdc_integrity as protocol

        source = self.truth.write(self.root / "prepared")
        cfg = base.normalize_config(
            base.Config(input_path=str(source), output_folder=str(self.native), research_profile=True)
        )
        frame, manifest = base.load_manifest(cfg)
        self.native.mkdir()
        with protocol.RunStore(
            self.native,
            manifest=manifest,
            config=protocol.effective_config(cfg),
            rows=protocol.plan_rows(frame, cfg, base.render_filename),
        ) as store:
            for row in frame.iter_rows(named=True):
                key = row[PROVENANCE[3]]
                store.publish(store.begin(key), key, self.payload)

            def report_factory(config, outcomes, elapsed):
                return base.generate_overview_report(
                    cfg=config, outcomes=outcomes, elapsed_sec=elapsed, df_total=len(outcomes)
                )

            with patch.object(base, "shutdown_flag", False):
                base.finalize_run(cfg, store, store.reconcile(), 0.0123456789, report_factory)

    def test_original_denominator_and_provenance_survive_filtering(self):
        source = self.truth.write(self.root / "prepared")
        rows = pl.read_parquet(source)
        self.assertEqual(rows.height, 3)
        self.assertEqual(rows[PROVENANCE[2]].to_list(), [6, 6, 6])
        self.assertEqual(len(set(rows[PROVENANCE[3]])), 3)
        self.assertEqual((source.parent / "original.parquet").read_bytes(), self.manifest.read_bytes())
        with self.assertRaises(FileExistsError):
            self.truth.write(source.parent)

    def test_unsupported_or_lossy_metadata_fails_before_output_creation(self):
        import datetime
        from decimal import Decimal

        for series in (
            pl.Series("extra", [1], dtype=pl.Int32),
            pl.Series("extra", [2**53], dtype=pl.Int64),
            pl.Series("extra", [float("nan")]),
            pl.Series("extra", [datetime.date(2026, 1, 1)]),
            pl.Series("extra", [Decimal("1.1")]),
            pl.Series("extra", [[1, 2]]),
            pl.Series("extra", [datetime.timedelta(seconds=1)]),
        ):
            with self.subTest(dtype=series.dtype):
                pl.DataFrame({"url": [self.urls[0]]}).with_columns(series).write_parquet(self.manifest)
                with self.assertRaises(ValueError):
                    Truth.load(self.manifest, self.catalog).write(self.native)
                self.assertFalse(self.native.exists())

    def test_img_sidecar_accounting_and_original_bytes_without_extension_inference(self):
        self.img_fixture()
        result = verify_native("img2dataset", self.native, self.truth.record)
        self.assertTrue(result["artifacts_valid"], result)
        self.assertEqual([r["disposition"] for r in result["rows"]], ["verified"] * 3 + ["skipped"] * 3)
        self.assertEqual(sum(r["useful_bytes"] for r in result["rows"]), 3 * len(self.payload))
        self.assertIsNone(result["rows"][0]["native_attempts"])
        before = {p: p.read_bytes() for p in self.native.iterdir()}
        self.assertEqual(verify_native("img2dataset", self.native, self.truth.record), result)
        self.assertEqual({p: p.read_bytes() for p in self.native.iterdir()}, before)

    def test_failed_rows_remain_accounted_and_do_not_gain_bytes(self):
        self.img_fixture(failed=True)
        result = verify_native("img2dataset", self.native, self.truth.record)
        self.assertTrue(result["artifacts_valid"], result)
        self.assertEqual(result["rows"][1]["disposition"], "failed")
        self.assertEqual(sum(r["useful_bytes"] for r in result["rows"]), 2 * len(self.payload))

    def test_real_flowdc_closed_publication_matches_independent_original_rows(self):
        self.flow_fixture()
        result = verify_native("flowdc", self.native, self.truth.record)
        self.assertTrue(result["artifacts_valid"], result)
        self.assertEqual(sum(r["useful_bytes"] for r in result["rows"]), 3 * len(self.payload))
        self.assertEqual(len(result["rows"]), 6)
        self.assertEqual(verify_native("flowdc", self.native, self.truth.record), result)

    def test_tampering_cannot_fabricate_archive_credit(self):
        members = self.img_fixture()
        variants = {
            "duplicate": members + [members[0]],
            "unexpected": members + [("unexpected", b"content")],
            "missing": members[1:],
            "corrupt": [(members[0][0], b"changed")] + members[1:],
            "wrong_row": [members[0], (members[1][0], b"{}"), *members[2:]],
            "traversal": [("../payload.jpg", self.payload), *members[1:]],
        }
        for label, bad in variants.items():
            with self.subTest(label=label):
                self.tar(self.native / "00000.tar", bad)
                result = verify_native("img2dataset", self.native, self.truth.record)
                self.assertFalse(result["artifacts_valid"], result)
                self.assertEqual(sum(r["useful_bytes"] for r in result["rows"]), 0)
        self.tar(self.native / "00000.tar", members)
        (self.native / "00000_stats.json").write_text('{"count":3,"successes":999}')
        self.assertFalse(verify_native("img2dataset", self.native, self.truth.record)["artifacts_valid"])

    def test_unclosed_compressed_concatenated_and_link_archives_are_rejected(self):
        path = self.root / "bad.tar"
        self.tar(path, [("one", b"data")])
        good = path.read_bytes()
        for raw in (good[:1024], good + b"not-zero", good + good):
            path.write_bytes(raw)
            with self.assertRaises(ValueError):
                archive_members(path)
        with tarfile.open(path, "w:gz") as archive:
            info = tarfile.TarInfo("one")
            archive.addfile(info)
        with self.assertRaises(tarfile.ReadError):
            archive_members(path)
        with tarfile.open(path, "w") as archive:
            info = tarfile.TarInfo("one")
            info.type, info.linkname = tarfile.SYMTYPE, "/etc/passwd"
            archive.addfile(info)
        with self.assertRaises(ValueError):
            archive_members(path)

    def test_duplicate_native_return_or_missing_sidecar_is_invalid(self):
        self.img_fixture()
        path = self.native / "00000.parquet"
        frame = pl.read_parquet(path)
        pl.concat([frame, frame.head(1)]).write_parquet(path)
        self.assertFalse(verify_native("img2dataset", self.native, self.truth.record)["artifacts_valid"])
        path.unlink()
        result = verify_native("img2dataset", self.native, self.truth.record)
        self.assertEqual([r["disposition"] for r in result["rows"]], ["missing"] * 3 + ["skipped"] * 3)

    def test_nonzero_exit_and_verification_cost_remain_in_primary_boundary(self):
        self.img_fixture()

        def verify():
            time.sleep(0.015)
            return verify_native("img2dataset", self.native, self.truth.record)

        result = run_verified(
            [sys.executable, "-c", "raise SystemExit(7)"],
            self.root / "run",
            self.truth.record,
            verify,
            deadline=5,
        )
        self.assertEqual(result["status"], "nonzero_exit")
        self.assertEqual(result["process_exit_code"], 7)
        self.assertFalse(result["run_complete"])
        self.assertEqual(result["useful_payload_bytes"], 3 * len(self.payload))
        self.assertGreaterEqual(result["verification_ns"], 15_000_000)
        self.assertGreaterEqual(result["elapsed_ns"], result["process_ns"] + result["verification_ns"])
        self.assertTrue((self.root / "run/outcomes.json").is_file())
        self.assertIsNone(result["resources"])

    def test_timeout_cleans_owned_process_and_retains_missing_rows(self):
        result = run_verified(
            [sys.executable, "-c", "import time; time.sleep(10)"],
            self.root / "run",
            self.truth.record,
            lambda: verify_native("img2dataset", self.native, self.truth.record),
            deadline=0.05,
            cleanup=1,
        )
        self.assertEqual(result["status"], "timeout")
        self.assertFalse(result["run_complete"])
        self.assertEqual(result["original_rows"], 6)
        self.assertEqual(result["useful_payload_bytes"], 0)
        self.assertLess(result["elapsed_ns"], 3_000_000_000)

    def test_launch_failure_preserves_denominator_and_failure(self):
        result = run_verified(
            [str(self.root / "no-executable")],
            self.root / "run",
            self.truth.record,
            lambda: verify_native("flowdc", self.native, self.truth.record),
        )
        self.assertEqual(result["status"], "launch_failed")
        self.assertIsNone(result["process_exit_code"])
        self.assertEqual(result["original_rows"], 6)
        self.assertEqual(result["useful_payload_bytes"], 0)

    def test_timeout_quiesces_real_child_before_verifying(self):
        child_pid = self.root / "child.pid"
        script = (
            "import subprocess,sys,time; "
            "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(10)']); "
            "open(sys.argv[1],'w').write(str(child.pid)); time.sleep(10)"
        )

        observed_live = []

        def verify():
            try:
                process = psutil.Process(int(child_pid.read_text()))
                observed_live.append(process.status() != psutil.STATUS_ZOMBIE)
            except psutil.NoSuchProcess:
                observed_live.append(False)
            return verify_native("flowdc", self.native, self.truth.record)

        result = run_verified(
            [sys.executable, "-c", script, str(child_pid)],
            self.root / "run",
            self.truth.record,
            verify,
            deadline=0.2,
            cleanup=1,
        )
        self.assertEqual(result["status"], "timeout")
        self.assertEqual(observed_live, [False])
        self.assertTrue(child_pid.exists(), "child must actually be started to exercise cleanup")
        pid = int(child_pid.read_text())
        if psutil.pid_exists(pid):
            self.assertEqual(psutil.Process(pid).status(), psutil.STATUS_ZOMBIE)

    def test_process_group_is_signalled_before_leader_pid_is_reaped(self):
        from benchmark.core import lifecycle

        actions = []
        native_popen, native_signal = subprocess.Popen, lifecycle._signal_group

        class TrackingPopen(native_popen):
            def wait(self, *args, **kwargs):
                actions.append("reap")
                return super().wait(*args, **kwargs)

        def signal_group(*args):
            actions.append("signal")
            return native_signal(*args)

        command = [
            sys.executable,
            "-c",
            "import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time;time.sleep(10)'])",
        ]
        with (
            patch.object(lifecycle.subprocess, "Popen", TrackingPopen),
            patch.object(lifecycle, "_signal_group", signal_group),
        ):
            record = run_verified(
                command,
                self.root / "orphan-run",
                self.truth.record,
                lambda: verify_native("flowdc", self.native, self.truth.record),
                deadline=2,
                cleanup=1,
            )
        self.assertEqual(record["status"], "descendants_remaining")
        self.assertIn("signal", actions)
        self.assertGreater(
            actions.index("reap"), max(i for i, action in enumerate(actions) if action == "signal")
        )

    def test_term_interrupts_wrapper_once_and_repeated_term_cannot_abort_cleanup(self):
        truth_path = self.root / "truth-for-child.json"
        truth_path.write_bytes(encode(self.truth.record))
        run = self.root / "term-run"
        ready = self.root / "child-ready"
        child = "import signal,time,sys; signal.signal(signal.SIGTERM,signal.SIG_IGN); open(sys.argv[1],'w').write('ready'); time.sleep(20)"
        wrapper = (
            "import json,sys; from pathlib import Path; from benchmark.core.lifecycle import run_verified; "
            "from benchmark.core.truth import initial_outcomes; t=json.load(open(sys.argv[1])); "
            "r=run_verified([sys.executable,'-c',sys.argv[4],sys.argv[3]],Path(sys.argv[2]),t,"
            "lambda:{'rows':list(initial_outcomes(t).values()),'artifacts_valid':False,'errors':[]},deadline=20,cleanup=1); "
            "print(json.dumps(r))"
        )
        process = subprocess.Popen(
            [sys.executable, "-B", "-c", wrapper, str(truth_path), str(run), str(ready), child],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            until = time.monotonic() + 5
            while not ready.exists() and time.monotonic() < until and process.poll() is None:
                time.sleep(0.01)
            self.assertTrue(ready.exists(), "native child must install its TERM handler")
            process.send_signal(signal.SIGTERM)
            time.sleep(0.05)
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr)
            result = json.loads(stdout)
            self.assertEqual(result["status"], "interrupted")
            self.assertFalse(result["run_complete"])
            self.assertEqual(result["original_rows"], 6)
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=3)
            # Only this fixture's positively identified child can be cleaned here.
            owner = run / "process-owner.json"
            if owner.exists():
                record = json.loads(owner.read_bytes())
                try:
                    native = psutil.Process(record["pid"])
                    if native.create_time() == record["create_time"]:
                        os.killpg(record["pgid"], signal.SIGKILL)
                except (ProcessLookupError, psutil.NoSuchProcess):
                    pass

    def test_failed_quiescence_never_reads_mutating_native_files(self):
        with patch("benchmark.core.lifecycle._group_running", return_value=True):
            verify = AsyncMock(side_effect=AssertionError("must not read live artifacts"))
            result = run_verified(
                [sys.executable, "-c", "pass"],
                self.root / "run",
                self.truth.record,
                verify,
                deadline=1,
                cleanup=0.02,
            )
        verify.assert_not_called()
        self.assertEqual(result["status"], "cleanup_failed")
        self.assertFalse(result["run_complete"])

    def test_duplicate_json_and_nonfinite_metadata_do_not_grant_credit(self):
        members = self.img_fixture()
        name, metadata = members[1]
        for bad in (b'{"key":1,"key":2}', b'{"key":NaN}'):
            with self.subTest(bad=bad):
                self.tar(self.native / "00000.tar", [members[0], (name, bad), *members[2:]])
                result = verify_native("img2dataset", self.native, self.truth.record)
                self.assertFalse(result["artifacts_valid"])
                self.assertEqual(sum(r["useful_bytes"] for r in result["rows"]), 0)


if __name__ == "__main__":
    unittest.main()
