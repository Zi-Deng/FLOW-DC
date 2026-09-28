"""Measurement/policy regressions for issue #20; never use external origins.

Run: python -m unittest discover -s tests -p 'test_http_measurement.py' -v
These assertions describe the required behavior and deliberately fail before repair.
Socket restrictions are errors, not skips or evidence of a product regression.
"""

import asyncio
import json
import sys
import tempfile
import time
import unittest
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path

import aiohttp
import polars as pl
from aiohttp import web

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
import download_batch as base  # noqa: E402
import download_batch_gradient as gradient  # noqa: E402
from single_download import download_via_http_get  # noqa: E402


class ClassificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_404_is_neither_useful_success_nor_overload(self):
        for module in (base, gradient):
            with self.subTest(module=module.__name__):
                metrics = module.HostMetrics(module.PAARCConfig())
                await metrics.record(status_code=404, ttfb=None, bytes_downloaded=0)
                snap = await metrics.finish_interval()
                self.assertEqual(snap["total"], 1)
                self.assertFalse(snap["has_overload"])
                self.assertEqual(snap["bytes"], 0)
                self.assertEqual(snap["n_samples"], 0)
                self.assertEqual(snap["n_success"], 0)

    async def test_overload_remains_actionable(self):
        for module in (base, gradient):
            for status in (408, 429, 503):
                with self.subTest(module=module.__name__, status=status):
                    metrics = module.HostMetrics(module.PAARCConfig())
                    await metrics.record(status_code=status, ttfb=None)
                    snap = await metrics.finish_interval()
                    self.assertEqual(snap["total"], 1)
                    self.assertEqual(snap["n_success"], 0)
                    self.assertEqual(snap["n_samples"], 0)
                    self.assertTrue(snap["has_overload"])


class LocalHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="flowdc-http-measurement-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.events = {}
        self.retry_headers = {}
        self.retry_deadlines = {}
        app = web.Application()
        app.router.add_get("/{name}", self.handle)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.addAsyncCleanup(self.runner.cleanup)
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def handle(self, request):
        name = request.match_info["name"]
        events = self.events.setdefault(name, [])
        events.append(("request", time.monotonic()))
        if name in self.retry_headers:
            if sum(kind == "request" for kind, _ in events) == 1:
                header = self.retry_headers[name]
                if header == "date":
                    # HTTP dates have whole-second precision. Record the actual
                    # advertised deadline, not an assumed two-second duration.
                    date = (datetime.now(UTC) + timedelta(seconds=2)).replace(microsecond=0)
                    header = format_datetime(date, usegmt=True)
                    delay = max(0.0, date.timestamp() - time.time())
                else:
                    delay = float(header)
                self.retry_deadlines[name] = time.monotonic() + delay
                return web.Response(status=429, headers={"Retry-After": header})
            return web.Response(body=b"saved payload")
        if name == "empty":
            return web.Response(body=b"")
        if name == "missing":
            return web.Response(status=404)
        if name == "headers":
            await asyncio.sleep(0.4)
        response = web.StreamResponse(headers={"Content-Length": "2"})
        await response.prepare(request)
        events.append(("headers", time.monotonic()))
        if name == "first":
            await asyncio.sleep(0.4)
        await response.write(b"a")
        events.append(("first", time.monotonic()))
        if name == "tail":
            await asyncio.sleep(0.4)
        await response.write(b"b")
        events.append(("tail", time.monotonic()))
        await response.write_eof()
        return response

    async def fetch(self, name):
        trace = {}
        token = base.TRACE_CTX.set(trace)
        try:
            async with aiohttp.ClientSession(trace_configs=[base.build_trace_config()]) as session:
                result = await asyncio.wait_for(download_via_http_get(session, f"{self.url}/{name}", 5), 7)
            return result, trace, time.monotonic()
        finally:
            base.TRACE_CTX.reset(token)

    async def test_delayed_tail_does_not_inflate_first_byte(self):
        result, trace, finished = await self.fetch("tail")
        self.assertEqual(result[:3], (b"ab", 200, None))
        events = dict(self.events["tail"])
        tail_gap = events["tail"] - events["first"]
        self.assertGreaterEqual(tail_gap, 0.35, "fixture did not delay the tail")
        self.assertIsNotNone(trace.get("ttfb"))
        # Compare independent server events with the client observation. The
        # generous half-gap bound detects whole-body timing without requiring
        # a particular absolute localhost latency.
        observed_first = trace["t0"] + trace["ttfb"]
        self.assertLess(observed_first, events["first"] + tail_gap / 2)
        self.assertGreater(finished - observed_first, tail_gap / 2)

    async def test_delayed_headers_are_observable(self):
        result, trace, _ = await self.fetch("headers")
        self.assertEqual(result[:3], (b"ab", 200, None))
        self.assertGreaterEqual(trace["ttfb"], 0.35)

    async def test_delayed_first_body_byte_is_observable(self):
        result, trace, _ = await self.fetch("first")
        self.assertEqual(result[:3], (b"ab", 200, None))
        events = dict(self.events["first"])
        self.assertGreaterEqual(events["first"] - events["headers"], 0.35)
        self.assertGreaterEqual(trace["ttfb"], 0.35)

    async def test_empty_body_has_no_first_byte_sample(self):
        result, trace, _ = await self.fetch("empty")
        self.assertEqual(result[:3], (b"", 200, None))
        self.assertIsNone(trace.get("ttfb"))

    async def test_failed_status_has_no_first_byte_sample(self):
        result, trace, _ = await self.fetch("missing")
        self.assertIsNone(result[0])
        self.assertEqual(result[1], 404)
        self.assertIsNotNone(result[2])
        self.assertIsNone(trace.get("ttfb"))

    async def check_retry_after(self, header):
        for script in ("download_batch.py", "download_batch_gradient.py"):
            for enabled in (False, True):
                with self.subTest(script=script, enable_paarc=enabled, header=header):
                    name = f"{script}-{enabled}"
                    self.retry_headers[name] = header
                    manifest = self.root / f"{name}.parquet"
                    pl.DataFrame({"url": [f"{self.url}/{name}"]}).write_parquet(manifest)
                    output = self.root / name
                    config = self.root / f"{name}.json"
                    config.write_text(
                        json.dumps(
                            {
                                "input": str(manifest),
                                "output": str(output),
                                "url": "url",
                                "enable_paarc": enabled,
                                "concurrent_downloads": 1,
                                "C_init": 1,
                                "C_min": 1,
                                "C_max": 1,
                                "timeout": 5,
                                "max_retry_attempts": 2,
                                "retry_backoff_sec": 0,
                                "create_tar": False,
                                "create_overview": True,
                            }
                        )
                    )
                    proc = await asyncio.create_subprocess_exec(
                        sys.executable,
                        str(ROOT / "bin" / script),
                        "--config",
                        str(config),
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    try:
                        stdout, stderr = await asyncio.wait_for(proc.communicate(), 15)
                    finally:
                        if proc.returncode is None:
                            proc.kill()
                            await proc.communicate()
                    self.assertEqual(proc.returncode, 0, (stdout + stderr).decode())
                    report = json.loads(Path(f"{output}_overview.json").read_text())
                    self.assertEqual(report["summary"]["successful_downloads"], 1)
                    requests = [when for kind, when in self.events[name] if kind == "request"]
                    self.assertEqual(len(requests), 2, "max_retry_attempts counts all attempts")
                    self.assertGreaterEqual(
                        requests[1],
                        self.retry_deadlines[name] - 0.1,
                        f"retry arrived {self.retry_deadlines[name] - requests[1]:.3f}s early",
                    )

    async def test_numeric_retry_after_delays_retry_in_all_modes(self):
        await self.check_retry_after("2")

    async def test_http_date_retry_after_delays_retry_in_all_modes(self):
        await self.check_retry_after("date")


if __name__ == "__main__":
    unittest.main()
