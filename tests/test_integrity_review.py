"""Regressions for PR #24's first independent review (temporary owned fixtures)."""

import asyncio
import errno
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, patch

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
import download_batch as base  # noqa: E402
import download_batch_gradient as gradient  # noqa: E402
import flowdc_integrity as protocol  # noqa: E402
import single_download as single  # noqa: E402


class ReviewRegressions(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.addCleanup(setattr, base, "shutdown_flag", base.shutdown_flag)
        base.shutdown_flag = False

    def setup_run(self, name="run", module=base, **options):
        source, output = self.root / (name + ".parquet"), self.root / name
        pl.DataFrame({"url": ["http://fixture.invalid/a.jpg", "http://fixture.invalid/b.jpg"]}).write_parquet(
            source
        )
        settings = dict(
            input_path=str(source),
            output_folder=str(output),
            naming_mode="row_id",
            create_tar=False,
            compress_tar=False,
            enable_paarc=False,
            retry_backoff_sec=0.01,
        )
        cfg = base.normalize_config(module.Config(**(settings | options)))
        frame, manifest = base.load_manifest(cfg)
        output.mkdir()
        store = protocol.RunStore(
            output,
            manifest=manifest,
            config=protocol.effective_config(cfg),
            rows=protocol.plan_rows(frame, cfg, base.render_filename),
        )
        self.addCleanup(store.close)
        return cfg, frame, store

    @staticmethod
    def report_factory(cfg, outcomes, elapsed):
        return base.generate_overview_report(
            cfg=cfg, df_total=len(outcomes), outcomes=outcomes, elapsed_sec=elapsed
        )

    def test_tar_helper_preserves_finalized_record_and_report_binding(self):
        cfg, frame, store = self.setup_run()
        for key in frame["__key__"]:
            store.publish(store.begin(key), key, b"independent payload")
        base.finalize_run(cfg, store, store.reconcile(), 1, self.report_factory)
        store.close()
        before = {p: p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        with self.assertRaisesRegex(protocol.IntegrityError, "finalized.*--reconcile"):
            base.create_tar(cfg.output_folder, compress=False)
        self.assertEqual({p: p.read_bytes() for p in self.root.rglob("*") if p.is_file()}, before)
        report = json.loads((self.root / "run_overview.json").read_bytes())
        record = (self.root / "run/.flowdc/final.json").read_bytes()
        self.assertTrue(json.loads(record)["run_complete"])
        self.assertEqual(
            report["output_integrity"]["completion_record_sha256"], hashlib.sha256(record).hexdigest()
        )

    def test_gradient_resume_marks_whole_run_counters_unavailable(self):
        for remaining in (0, 1):
            with self.subTest(remaining=remaining):
                cfg, frame, store = self.setup_run(
                    name=f"gradient-{remaining}", module=gradient, enable_paarc=True
                )
                for key in frame["__key__"].head(2 - remaining):
                    store.publish(store.begin(key), key, b"already committed")
                prior = {"gradient_hold_events": 7, "avg_gradient_confidence": 0.8}

                def factory(c, outcomes, elapsed, prior=prior):
                    return gradient.generate_overview_report(
                        cfg=c,
                        df_total=len(outcomes),
                        outcomes=outcomes,
                        elapsed_sec=elapsed,
                        gradient_summary=prior,
                    )

                base.finalize_run(cfg, store, store.reconcile(), 2, factory)
                store.close()
                fetch = AsyncMock(return_value=(b"remaining payload", 200, None, None))
                with (
                    patch.object(gradient, "parse_args", return_value=replace(cfg, resume=True)),
                    patch.object(single, "download_via_http_get", fetch),
                ):
                    asyncio.run(gradient.main())
                report = json.loads((Path(cfg.output_folder) / "overview.json").read_bytes())
                self.assertEqual(fetch.await_count, remaining)
                self.assertIsNone(report["gradient_summary"])
                self.assertEqual(report["gradient_summary_scope"], "unavailable_across_resume")
                self.assertEqual(report["summary"]["successful_downloads"], 2)
                # Offline reconciliation must retain the same unavailable scope.
                with patch.object(base.aiohttp, "ClientSession", side_effect=AssertionError("offline HTTP")):
                    offline = asyncio.run(base.run_acquisition(replace(cfg, reconcile=True)))
                self.assertIsNone(offline["gradient_summary"])
                self.assertEqual(offline["gradient_summary_scope"], "unavailable_across_resume")

    def test_unrecorded_attempt_start_failure_aborts_instead_of_retrying(self):
        for cut in ("mkdir", "intent"):
            with self.subTest(cut=cut):
                cfg, _, store = self.setup_run(name=cut, max_retry_attempts=2, concurrent_downloads=1)
                store.close()
                original_mkdir, original_atomic = protocol.Files.mkdir, protocol.Files.atomic
                calls = []

                def mkdir(fs, path, cut=cut, calls=calls, original_mkdir=original_mkdir):
                    if cut == "mkdir" and path.startswith(".flowdc/attempts/"):
                        calls.append(path)
                        raise OSError(errno.ENOSPC, "injected attempt directory failure")
                    return original_mkdir(fs, path)

                def atomic(fs, path, value, cut=cut, calls=calls, original_atomic=original_atomic, **kwargs):
                    if cut == "intent" and path.endswith("/intent.json"):
                        calls.append(path)
                        raise OSError(errno.ENOSPC, "injected intent failure")
                    return original_atomic(fs, path, value, **kwargs)

                fetch = AsyncMock(side_effect=AssertionError("HTTP before durable intent"))

                async def exercise(cfg=cfg):
                    with self.assertRaisesRegex(OSError, "injected") as failure:
                        await asyncio.wait_for(base.run_acquisition(replace(cfg, resume=True)), 0.3)
                    self.assertEqual(failure.exception.errno, errno.ENOSPC)

                with (
                    patch.object(protocol.Files, "mkdir", mkdir),
                    patch.object(protocol.Files, "atomic", atomic),
                    patch.object(single, "download_via_http_get", fetch),
                ):
                    asyncio.run(exercise())
                self.assertEqual(len(calls), 1)
                fetch.assert_not_awaited()
                self.assertFalse(
                    json.loads((Path(cfg.output_folder) / ".flowdc/final.json").read_bytes())["run_complete"]
                )
                if cut == "intent":
                    with protocol.RunStore(cfg.output_folder) as reopened:
                        failed = [r for r in reopened.reconcile()["rows"] if r["disposition"] == "failed"]
                        self.assertEqual(len(failed), 1)
                        self.assertTrue(failed[0]["attempt_information_uncertain"])
                        self.assertEqual(failed[0]["attempt_intents"], 1)
                        self.assertEqual(failed[0]["remaining_attempts"], 1)

    def parse_adapter(self, summary):
        from benchmark.core.flowdc_adapter import FlowDCAdapter, FlowDCConfig
        from benchmark.core.metrics import ResourceMetrics

        path = self.root / "overview.json"
        path.write_text(json.dumps({"report_schema_version": 2, "summary": summary}))
        return FlowDCAdapter(ROOT)._parse_overview(
            path, FlowDCConfig("unused", str(self.root), "url", None, 2, 5, False), 0, ResourceMetrics()
        )

    def test_adapter_rejects_unavailable_elapsed_instead_of_inventing_zero(self):
        for elapsed in (None, -1, "unknown", True, float("nan"), float("inf")):
            with (
                self.subTest(elapsed=elapsed),
                self.assertRaisesRegex(ValueError, r"overview.json.*elapsed_sec"),
            ):
                self.parse_adapter({"elapsed_sec": elapsed, "verified_payload_bytes": 1_000_000})

    def test_adapter_diagnoses_missing_or_invalid_verified_bytes(self):
        for fields in (
            {},
            {"verified_payload_bytes": None},
            {"verified_payload_bytes": -1},
            {"verified_payload_bytes": True},
            {"verified_payload_bytes": 1.5},
        ):
            with (
                self.subTest(fields=fields),
                self.assertRaisesRegex(ValueError, r"overview.json.*verified_payload_bytes"),
            ):
                self.parse_adapter({"elapsed_sec": 1, **fields})

    def test_empty_partition_cli_reports_saved_empty_manifest(self):
        source, output = self.root / "empty.parquet", self.root / "partitions"
        pl.DataFrame({"url": []}, schema={"url": pl.String}).write_parquet(source)
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                str(ROOT / "bin/SplitParquet.py"),
                "--parquet",
                str(source),
                "--url_col",
                "url",
                "--groups",
                "2",
                "--output_folder",
                str(output),
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Empty input: 0 rows", result.stdout)
        self.assertIn("1 empty partition", result.stdout)
        parts = list(output.glob("*.parquet"))
        self.assertEqual(len(parts), 1)
        self.assertEqual(pl.read_parquet(parts[0]).height, 0)
        self.assertIn(str(parts[0]), result.stdout)

    def test_local_failures_supply_latency_samples_without_success_denominator(self):
        async def exercise(module):
            controller = module.PAARCController("fixture.invalid", module.PAARCConfig(N_min=10, N_init=5))
            metrics = controller.metrics
            for latency in (1, 2, 3, 4, 5):
                await metrics.record(
                    200, latency, 0, acquisition_success=False, is_local_error=True, latency_eligible=True
                )
            snap = await controller.step_interval()
            self.assertEqual(controller.state, base.PAARCState.STARTUP)
            self.assertEqual(controller._concurrency, 4)
            self.assertEqual(
                (snap["total"], snap["n_samples"], snap["n_success"], snap["n_local_failures"]), (5, 5, 0, 5)
            )
            self.assertAlmostEqual(snap["p10_raw"], 1.4)
            self.assertAlmostEqual(snap["p50_raw"], 3)
            self.assertAlmostEqual(snap["p95_raw"], 4.8)
            self.assertEqual((snap["goodput_rps"], snap["goodput_bps"], snap["n_errors"]), (0, 0, 0))
            self.assertFalse(snap["has_overload"])
            if module is gradient:
                self.assertEqual(snap["gradient_sample_count"], 5)
                self.assertEqual(snap["gradient_confidence"], 0.5)
            excluded = module.PAARCController("fixture.invalid", module.PAARCConfig(N_min=10, N_init=5))
            for latency in (1, 2, 3, 4, 5):
                await excluded.metrics.record(
                    200, latency, 0, acquisition_success=False, is_local_error=True, latency_eligible=False
                )
            without_samples = await excluded.step_interval()
            self.assertEqual(without_samples["n_samples"], 0)
            self.assertEqual(without_samples["n_local_failures"], 5)
            self.assertEqual(excluded.state, base.PAARCState.INIT)

        for module in (base, gradient):
            with self.subTest(module=module.__name__):
                asyncio.run(exercise(module))

    def test_transient_postcommit_lookup_failure_retains_distinct_attempt_and_row_outcomes(self):
        cfg, frame, store = self.setup_run()
        row = frame.row(0, named=True)
        key = row["__key__"]
        read_json = store.fs.json

        def fail_commit_lookup(path):
            if path == f".flowdc/commits/{key}.json":
                raise OSError("transient post-commit lookup failure")
            return read_json(path)

        with (
            patch.object(
                single, "download_via_http_get", AsyncMock(return_value=(b"committed truth", 200, None, None))
            ),
            patch.object(store.fs, "json", side_effect=fail_commit_lookup),
        ):
            attempt = asyncio.run(
                base.download_one(
                    row=row,
                    cfg=cfg,
                    session=None,
                    total_bytes=[],
                    manager=None,
                    sequential_namer=base.SequentialNamer(),
                    global_written_paths={},
                    store=store,
                )
            )
        self.assertFalse(attempt.success)
        self.assertEqual(attempt.bytes_downloaded, 0)
        self.assertIn("transient post-commit", read_json(f".flowdc/attempts/{key}/1/result.json")["error"])
        snapshot = store.reconcile()
        verified = snapshot["rows"][0]
        self.assertEqual((verified["disposition"], verified["attempt_intents"]), ("verified", 1))
        self.assertEqual(snapshot["verified_payload_bytes"], len(b"committed truth"))
        self.assertEqual(verified["payload_sha256"], hashlib.sha256(b"committed truth").hexdigest())
        self.assertEqual(snapshot, store.reconcile())


if __name__ == "__main__":
    unittest.main()
