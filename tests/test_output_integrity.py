"""Output-integrity regressions for issue #22, using temporary owned files.

The response double supplies body reads, not a saved-output result or metrics.
The real HTTP helper, save functions, final size lookup and controllers execute.
LocalHTTPTests in test_http_measurement.py additionally exercises a real server.
"""

import os
import sys
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import aiohttp
from yarl import URL

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
import download_batch as base  # noqa: E402
import download_batch_gradient as gradient  # noqa: E402


class OutputObservationTests(unittest.IsolatedAsyncioTestCase):
    async def attempt(self, module, root, failure, payload=b"complete fixture body", body_error=None):
        url = "http://fixture.invalid/payload.jpg"
        cfg = base.Config(
            input_path="unused", output_folder=str(root), output_format="webdataset",
            naming_mode="url_based", file_name_pattern="payload",
        )
        manager = module.HostControllerManager(module.PAARCConfig()) if module else None
        sizes, written, traces, lookups = [], {}, [], []
        getsize = os.path.getsize

        def verify_size(path):
            # Observe the actual closed payload before injecting a failed stat.
            self.assertEqual(Path(path).read_bytes(), payload)
            lookups.append(path)
            if failure == "metadata" and len(lookups) == 1:
                (root / "payload.json").mkdir()
            if failure == "save-stat" or (failure == "final-stat" and len(lookups) == 2):
                raise OSError("injected output stat failure")
            if failure == "missing" and len(lookups) == 1:
                size = getsize(path)
                Path(path).unlink()
                return size
            return getsize(path)

        async with aiohttp.ClientSession() as session:
            @asynccontextmanager
            async def response(*args, **kwargs):
                trace = base.TRACE_CTX.get()
                traces.append(trace)
                await base.build_trace_config()._dispatch(
                    session, SimpleNamespace(measurement=trace), SimpleNamespace(url=URL(url))
                )
                yield SimpleNamespace(
                    status=200,
                    content=SimpleNamespace(read=AsyncMock(side_effect=[payload[:1], body_error or payload[1:]])),
                )

            with patch.object(session, "get", side_effect=response), patch("os.path.getsize", side_effect=verify_size):
                out = await base.download_one(
                    row={"url": url, "__key__": "original-row"}, cfg=cfg, session=session,
                    total_bytes=sizes, manager=manager, sequential_namer=base.SequentialNamer(),
                    global_written_paths=written,
                )
        if manager is not None:
            ctrl = await manager.get_controller(url)
            self.assertEqual(ctrl.semaphore.inflight, 0)
            snap = await ctrl.metrics.finish_interval()
            self.assertEqual((await ctrl.metrics.finish_interval())["total"], 0)
        else:
            snap = None
        return out, snap, sizes, written, traces[0], lookups

    async def test_post_save_stat_failure_is_a_local_failed_outcome(self):
        for module in (base, gradient, None):
            with self.subTest(module=getattr(module, "__name__", "fixed")), tempfile.TemporaryDirectory() as tmp:
                out, snap, sizes, written, trace, lookups = await self.attempt(module, Path(tmp), "final-stat")
                self.assertEqual(len(lookups), 2, "the helper save must finish before the final stat fails")
                self.assertFalse(out.success)
                self.assertEqual(out.status_code, 200)
                self.assertIn("injected output stat failure", out.error)
                self.assertEqual(out.bytes_downloaded, 0)
                self.assertEqual(sizes, [])
                self.assertEqual(written, {})
                self.assertGreater(trace["ttfb"], 0)
                self.assertTrue(trace["latency_eligible"])
                self.assertIsNotNone(trace["body_completed_at"])
                self.assertEqual(Path(out.file_path).read_bytes(), b"complete fixture body")
                if snap is not None:
                    self.assert_local_failure(snap, samples=1)
                report = base.generate_overview_report(
                    cfg=base.Config(input_path="unused", output_folder=tmp), df_total=1,
                    outcomes={out.key: out}, elapsed_sec=1,
                )
                self.assertEqual(report["summary"]["successful_downloads"], 0)
                self.assertEqual(report["summary"]["failed_downloads"], 1)
                self.assertEqual(report["summary"]["downloaded_mb"], 0)

    async def test_save_stat_and_metadata_failure_preserve_completed_body_observation(self):
        for module in (base, gradient):
            for failure in ("save-stat", "metadata"):
                with self.subTest(module=module.__name__, failure=failure), tempfile.TemporaryDirectory() as tmp:
                    out, snap, sizes, written, trace, _ = await self.attempt(module, Path(tmp), failure)
                    self.assertFalse(out.success)
                    self.assertEqual(out.bytes_downloaded, 0)
                    self.assertEqual(sizes, [])
                    self.assertEqual(written, {})
                    self.assertEqual(Path(out.file_path).read_bytes(), b"complete fixture body")
                    self.assertIsNotNone(trace["body_completed_at"])
                    self.assert_local_failure(snap, samples=1)
                    self.assertTrue(trace["latency_eligible"])
                    self.assertGreater(trace["ttfb"], 0)

    async def test_missing_output_is_not_reported_as_a_zero_byte_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            out, snap, sizes, written, _, _ = await self.attempt(base, Path(tmp), "missing")
            self.assertFalse(out.success)
            self.assertIsNotNone(out.error)
            self.assertEqual(out.bytes_downloaded, 0)
            self.assertEqual(sizes, [])
            self.assertEqual(written, {})
            self.assert_local_failure(snap, samples=1)

    async def test_empty_success_and_failure_remain_ineligible(self):
        for failure in (None, "save-stat", "metadata", "final-stat"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                out, snap, sizes, _, trace, _ = await self.attempt(base, Path(tmp), failure, payload=b"")
                self.assertEqual(out.success, failure is None)
                self.assertEqual(out.bytes_downloaded, 0)
                self.assertFalse(trace["latency_eligible"])
                self.assertIsNone(trace["ttfb"])
                self.assertEqual(snap["n_samples"], 0)
                if failure is None:
                    self.assertEqual(sizes, [0])
                    self.assertEqual(snap["n_success"], 1)
                else:
                    self.assertEqual(sizes, [])
                    self.assert_local_failure(snap, samples=0)

    async def test_incomplete_body_has_no_eligible_sample_or_output_credit(self):
        for module in (base, gradient):
            with self.subTest(module=module.__name__), tempfile.TemporaryDirectory() as tmp:
                out, snap, sizes, written, trace, lookups = await self.attempt(
                    module, Path(tmp), None, body_error=aiohttp.ClientPayloadError("incomplete body"),
                )
                self.assertFalse(out.success)
                self.assertEqual(out.bytes_downloaded, 0)
                self.assertEqual(sizes, [])
                self.assertEqual(written, {})
                self.assertEqual(lookups, [])
                self.assertFalse(trace["latency_eligible"])
                self.assertIsNone(trace["ttfb"])
                self.assertIsNone(trace["body_completed_at"])
                self.assertEqual(snap["n_transport_failures"], 1)
                self.assertEqual(snap["n_local_failures"], 0)
                self.assertEqual(snap["n_samples"], 0)
                self.assertEqual(snap["bytes"], 0)

    async def test_successful_output_is_credited_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            out, snap, sizes, written, trace, _ = await self.attempt(base, Path(tmp), None)
            self.assertTrue(out.success)
            self.assertEqual(out.bytes_downloaded, len(b"complete fixture body"))
            self.assertEqual(sizes, [len(b"complete fixture body")])
            self.assertEqual(written, {out.file_path: out.key})
            self.assertTrue(trace["latency_eligible"])
            self.assertEqual(snap["n_samples"], 1)
            self.assertEqual(snap["n_success"], 1)
            self.assertEqual(snap["n_failed"], 0)
            self.assertEqual(snap["bytes"], len(b"complete fixture body"))

    async def test_explicit_eligibility_requires_valid_timing(self):
        for module in (base, gradient):
            for timing in (None, 0, -1, float("nan"), float("inf"), 0.2):
                with self.subTest(module=module.__name__, timing=timing):
                    metrics = module.HostMetrics(module.PAARCConfig())
                    await metrics.record(
                        200, timing, acquisition_success=False, is_local_error=True, latency_eligible=True,
                    )
                    self.assert_local_failure(await metrics.finish_interval(), samples=int(timing == 0.2))
            metrics = module.HostMetrics(module.PAARCConfig())
            await metrics.record(200, 0.2, 10, acquisition_success=True, latency_eligible=False)
            snap = await metrics.finish_interval()
            self.assertEqual(snap["n_samples"], 0)
            self.assertEqual(snap["n_success"], 1)
            self.assertEqual(snap["bytes"], 10)

    def assert_local_failure(self, snap, *, samples):
        self.assertEqual(snap["total"], 1)
        self.assertEqual(snap["n_failed"], 1)
        self.assertEqual(snap["n_local_failures"], 1)
        for field in ("n_success", "n_http_failures", "n_transport_failures", "n_unknown_failures", "n_errors", "bytes"):
            self.assertEqual(snap[field], 0, field)
        self.assertEqual(snap["n_samples"], samples)
        self.assertEqual(snap["goodput_bps"], 0)
        self.assertEqual(snap["goodput_rps"], 0)
        self.assertIsNone(snap["avg_file_size"])
        self.assertFalse(snap["has_overload"])


if __name__ == "__main__":
    unittest.main()
