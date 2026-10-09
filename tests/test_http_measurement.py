"""Measurement/policy regressions for issue #20; never use external origins.

Run: python -m unittest discover -s tests -p 'test_http_measurement.py' -v
These assertions describe the required behavior and deliberately fail before repair.
Socket restrictions are errors, not skips or evidence of a product regression.
"""

import asyncio
import json
import os
import sys
import tempfile
import time
import unittest
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import polars as pl
from aiohttp import web
from yarl import URL

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
import download_batch as base  # noqa: E402
import download_batch_gradient as gradient  # noqa: E402
from single_download import (  # noqa: E402
    RetryAfterGate,
    download_via_http_get,  # noqa: E402
    http_authority,
    parse_retry_after,
    save_webdataset,
    session_http_gate,
)


class ClassificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_unexpected_body_errors_are_unclassified_without_invented_overload(self):
        # Fault injection checks honest attribution, not whether these raw errors
        # escape the installed aiohttp transport's normal ClientError wrapping.
        for module in (base, gradient):
            for error in (OSError("injected raw read error"), ValueError("injected decoder/programming error"),
                          aiohttp.ClientOSError("wrapped transport error"), aiohttp.ClientPayloadError("truncated")):
                with self.subTest(module=module.__name__, error=type(error).__name__):
                    unknown = not isinstance(error, aiohttp.ClientError)
                    manager = module.HostControllerManager(module.PAARCConfig())
                    response = SimpleNamespace(status=200, content=SimpleNamespace(read=AsyncMock(side_effect=error)))
                    response_context = AsyncMock()
                    response_context.__aenter__.return_value = response
                    async with aiohttp.ClientSession() as session:
                        with patch.object(session, "get", return_value=response_context):
                            out = await base.download_one(
                                row={"url": "http://a.test/body", "__key__": "key"},
                                cfg=base.Config(input_path="unused", output_folder="unused"),
                                session=session, total_bytes=[], manager=manager,
                                sequential_namer=base.SequentialNamer(), global_written_paths={},
                            )
                    self.assertFalse(out.success)
                    ctrl = await manager.get_controller("http://a.test/body")
                    snap = await ctrl.metrics.finish_interval()
                    self.assertEqual(snap["n_local_failures"], 0)
                    self.assertEqual(snap["n_transport_failures"], int(not unknown))
                    self.assertEqual(snap["n_unknown_failures"], int(unknown))
                    self.assertEqual(snap["n_failed"], 1)
                    self.assertEqual(snap["total"], 1)
                    self.assertEqual(snap["n_success"], 0)
                    self.assertEqual(snap["n_samples"], 0)
                    self.assertEqual(snap["bytes"], 0)
                    self.assertEqual(snap["has_overload"], not unknown)
                    self.assertEqual(ctrl.semaphore.inflight, 0)
                    self.assertEqual((await ctrl.metrics.finish_interval())["n_unknown_failures"], 0)

    async def test_admission_failures_do_not_erase_prior_overload_cooldown(self):
        for module in (base, gradient):
            with self.subTest(module=module.__name__):
                manager = module.HostControllerManager(module.PAARCConfig())
                ctrl = await manager.get_controller("http://a.test")
                await ctrl.metrics.record(429, None, retry_after_sec=60, acquisition_success=False)
                overload = await ctrl.metrics.finish_interval()
                self.assertTrue(overload["has_overload"])
                await ctrl._step_init(overload, 100)
                self.assertEqual(ctrl.state, base.PAARCState.BACKOFF)
                self.assertGreaterEqual(ctrl._cooldown_until, 160)
                limit = ctrl.semaphore.limit
                await ctrl.metrics.record(408, None, acquisition_success=False, is_local_error=True)
                waiting = await ctrl.metrics.finish_interval()
                self.assertFalse(waiting["has_overload"])
                self.assertEqual(waiting["n_local_failures"], 1)
                await ctrl._step_backoff(waiting, 101)
                self.assertEqual(ctrl.state, base.PAARCState.BACKOFF)
                self.assertEqual(ctrl.semaphore.limit, limit)
                self.assertGreaterEqual(ctrl._cooldown_until, 160)

    def test_benchmark_adapter_reads_generated_overview_with_additive_metadata(self):
        from benchmark.core.flowdc_adapter import FlowDCAdapter, FlowDCConfig
        from benchmark.core.metrics import ResourceMetrics

        outcomes = {
            "ok": base.DownloadOutcome("ok", "http://a.test/ok", True, "saved", None, 200, None, 1000),
            "fail": base.DownloadOutcome("fail", "http://a.test/fail", False, None, None, 404, "HTTP 404"),
        }
        cfg = base.Config(input_path="unused", output_folder="unused")
        report = base.generate_overview_report(cfg=cfg, df_total=3, outcomes=outcomes, elapsed_sec=2)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "overview.json"
            path.write_text(json.dumps(report))
            config = FlowDCConfig("unused", tmp, "url", None, 2, 5, True)
            result = FlowDCAdapter(ROOT)._parse_overview(path, config, 1, ResourceMetrics())
        self.assertEqual(result.input_urls, 3)
        self.assertEqual(result.successful_downloads, 1)
        self.assertEqual(result.failed_downloads, 1)
        self.assertEqual(result.throughput_imgs_per_sec, 0.5)
        self.assertEqual(result.error_counts, {"404_HTTP 404": 1})
        self.assertEqual(result.extra_metrics["paarc_version"], report["paarc_version"])

    async def test_normalized_dispatch_feedback_uses_held_controller(self):
        # These URLs need no DNS or privileged/default-port listener. Exercise
        # real manager ownership and the actual dispatch hook with YARL URLs.
        with tempfile.TemporaryDirectory() as tmp:
            saved = Path(tmp) / "payload.jpg"
            saved.write_bytes(b"payload")
            for module in (base, gradient):
                for url in ("http://example.invalid:80/path", "https://example.invalid:443/path",
                            "http://bücher.invalid/path"):
                    for status in (200, 503):
                        with self.subTest(module=module.__name__, url=url, status=status):
                            manager = module.HostControllerManager(module.PAARCConfig())
                            held = await manager.get_controller(url)

                            async def observed_download(status=status, **kwargs):
                                trace = base.build_trace_config()
                                ctx = SimpleNamespace(measurement=base.TRACE_CTX.get())
                                await trace._dispatch(kwargs["session"], ctx, SimpleNamespace(url=URL(kwargs["url"])))
                                ctx.measurement["ttfb"] = 0.1
                                if status == 200:
                                    return "key", str(saved), None, None, 200, None
                                return "key", None, None, "HTTP 503", 503, None

                            async with aiohttp.ClientSession() as session:
                                with patch.object(base, "download_single", side_effect=observed_download):
                                    await base.download_one(
                                        row={"url": url, "__key__": "key"},
                                        cfg=base.Config(input_path="unused", output_folder=tmp),
                                        session=session, total_bytes=[], manager=manager,
                                        sequential_namer=base.SequentialNamer(), global_written_paths={},
                                    )
                            snap = await held.metrics.finish_interval()
                            self.assertEqual(snap["total"], 1)
                            self.assertEqual(snap["n_success"], int(status == 200))
                            self.assertEqual(snap["n_samples"], int(status == 200))
                            self.assertEqual(snap["bytes"], 7 if status == 200 else 0)
                            self.assertEqual(snap["has_overload"], status == 503)
                            self.assertEqual(held.semaphore.inflight, 0)
                            self.assertEqual(await manager.all_controllers(), [held])

    async def test_cancelling_redirect_admission_preserves_permit_ownership(self):
        for module in (base, gradient):
            for during_smoothing in (False, True):
                with self.subTest(module=module.__name__, during_smoothing=during_smoothing):
                    manager = module.HostControllerManager(module.PAARCConfig(C_init=1, C_min=1))
                    origin = await manager.get_controller("http://a.test/start")
                    destination = await manager.get_controller("http://b.test/final")
                    entered = asyncio.Event()
                    if during_smoothing:
                        async def smooth(entered=entered):
                            entered.set()
                            await asyncio.Event().wait()
                        destination.smoother = SimpleNamespace(acquire=smooth)
                    else:
                        await destination.semaphore.acquire()
                        acquire = destination.semaphore.acquire

                        async def observed_acquire(acquire=acquire, entered=entered):
                            entered.set()
                            await acquire()
                        self.enterContext(patch.object(destination.semaphore, "acquire", side_effect=observed_acquire))

                    async def redirected_download(**kwargs):
                        ctx = SimpleNamespace(measurement=base.TRACE_CTX.get())
                        response = SimpleNamespace(
                            url=URL("http://a.test/start"), status=302,
                            headers={"Location": "http://b.test/final"}, release=Mock(),
                        )
                        await base.build_trace_config()._redirect(kwargs["session"], ctx, SimpleNamespace(response=response))
                        self.fail("redirect admission should still be waiting")

                    async with aiohttp.ClientSession() as session:
                        with patch.object(base, "download_single", side_effect=redirected_download):
                            task = asyncio.create_task(base.download_one(
                                row={"url": "http://a.test/start", "__key__": "key"},
                                cfg=base.Config(input_path="unused", output_folder="unused"),
                                session=session, total_bytes=[], manager=manager,
                                sequential_namer=base.SequentialNamer(), global_written_paths={},
                            ))
                            try:
                                await asyncio.wait_for(entered.wait(), 1)
                                self.assertEqual(origin.semaphore.inflight, 0)
                                task.cancel()
                                with self.assertRaises(asyncio.CancelledError):
                                    await task
                                self.assertEqual(destination.semaphore.inflight, 0 if during_smoothing else 1)
                            finally:
                                task.cancel()
                                await asyncio.gather(task, return_exceptions=True)
                                if not during_smoothing:
                                    await destination.semaphore.release()

    async def test_redirected_acquisition_uses_destination_adaptive_permit(self):
        for module in (base, gradient):
            with self.subTest(module=module.__name__):
                manager = module.HostControllerManager(module.PAARCConfig())
                origin = await manager.get_controller("http://a.test/start")
                destination = await manager.get_controller("http://b.test/final")

                async def redirected_download(origin=origin, destination=destination, **kwargs):
                    trace = base.build_trace_config()
                    ctx = SimpleNamespace(measurement=base.TRACE_CTX.get())
                    response = SimpleNamespace(
                        url=URL("http://a.test/start"), status=302,
                        headers={"Location": "http://b.test/final"}, release=Mock(),
                    )
                    await trace._redirect(kwargs["session"], ctx, SimpleNamespace(response=response))
                    await trace._dispatch(kwargs["session"], ctx, SimpleNamespace(url=URL("http://b.test/final")))
                    self.assertEqual(destination.semaphore.inflight, 1)
                    self.assertEqual(origin.semaphore.inflight, 0)
                    return "key", None, None, "HTTP 404", 404, None

                async with aiohttp.ClientSession() as session:
                    with patch.object(base, "download_single", side_effect=redirected_download):
                        await base.download_one(
                            row={"url": "http://a.test/start", "__key__": "key"},
                            cfg=base.Config(input_path="unused", output_folder="unused"),
                            session=session, total_bytes=[], manager=manager,
                            sequential_namer=base.SequentialNamer(), global_written_paths={},
                        )
                self.assertEqual(origin.semaphore.inflight, 0)
                self.assertEqual(destination.semaphore.inflight, 0)
                self.assertEqual((await origin.metrics.finish_interval())["total"], 0)
                self.assertEqual((await destination.metrics.finish_interval())["n_http_failures"], 1)

    def test_overview_labels_semantics_and_unfinished_denominator(self):
        cfg = base.Config(input_path="unused", output_folder="unused")
        outcomes = {
            "ok": base.DownloadOutcome("ok", "http://a.test", True, "saved", None, 200, None, 10),
            "fail": base.DownloadOutcome("fail", "http://a.test", False, None, None, 404, "missing"),
        }
        report = base.generate_overview_report(cfg=cfg, df_total=3, outcomes=outcomes, elapsed_sec=1)
        self.assertEqual(report["http_measurement"]["version"], "4-output-independent-delay-signals")
        self.assertIn("independent of local output success", report["http_measurement"]["latency_eligibility"])
        self.assertEqual(report["summary"]["successful_downloads"], 1)
        self.assertEqual(report["summary"]["failed_downloads"], 1)
        self.assertEqual(report["summary"]["unattempted_or_cancelled_urls"], 1)
        gradient_report = gradient.generate_overview_report(
            cfg=gradient.Config(input_path="unused", output_folder="unused"),
            df_total=3,
            outcomes=outcomes,
            elapsed_sec=1,
            gradient_summary={},
        )
        self.assertEqual(gradient_report["http_measurement"], report["http_measurement"])

    async def test_disjoint_outcomes_and_sample_eligibility(self):
        for module in (base, gradient):
            with self.subTest(module=module.__name__):
                metrics = module.HostMetrics(module.PAARCConfig())
                for status in (400, 401, 403, 404, 410, 503):
                    await metrics.record(status, 0.1, 50, acquisition_success=False)
                await metrics.record(200, 0.1, 50, acquisition_success=False, is_local_error=True)
                await metrics.record(None, 0.1, 50, is_conn_error=True, acquisition_success=False)
                await metrics.record(200, 0.1, 10, acquisition_success=True)
                await metrics.record(200, None, 0, acquisition_success=True)
                await metrics.record(200, float("nan"), 5, acquisition_success=True)
                snap = await metrics.finish_interval()
                self.assertEqual(snap["total"], 11)
                self.assertEqual(snap["n_success"], 3)
                self.assertEqual(snap["n_failed"], 8)
                self.assertEqual(snap["n_http_failures"], 6)
                self.assertEqual(snap["n_local_failures"], 1)
                self.assertEqual(snap["n_transport_failures"], 1)
                self.assertEqual(snap["n_errors"], 2)
                self.assertEqual(snap["n_samples"], 1)
                self.assertEqual(snap["bytes"], 15)
                empty = await metrics.finish_interval()
                self.assertEqual(empty["total"], 0)
                self.assertEqual(empty["n_failed"], 0)
                self.assertFalse(empty["has_overload"])

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


class GateTests(unittest.IsolatedAsyncioTestCase):
    async def test_redirect_retry_after_delays_follow_without_embargoing_destination(self):
        # Exercise the actual redirect hook with a deterministic clock. No socket
        # or scheduler tolerance can hide an immediate cross-authority follow.
        for location in ("http://a.test/next", "http://b.test/next"):
            for header in ("2", format_datetime(datetime.fromtimestamp(1002, UTC), usegmt=True)):
                with self.subTest(location=location, header=header):
                    now = [10.0]
                    waits = []

                    async def sleep(delay, waits=waits, now=now):
                        waits.append(delay)
                        now[0] += delay

                    gate = RetryAfterGate(clock=lambda now=now: now[0], wall_clock=lambda: 1000, sleep=sleep)
                    response = SimpleNamespace(
                        url=URL("http://a.test/start"), status=302,
                        headers={"Location": location, "Retry-After": header}, release=Mock(),
                    )
                    ctx = SimpleNamespace(measurement={"hops": []})
                    with patch("single_download.session_http_gate", return_value=gate):
                        await base.build_trace_config()._redirect(None, ctx, SimpleNamespace(response=response))
                    response.release.assert_called_once()
                    self.assertEqual(waits, [2])
                    self.assertEqual(now[0], 12)
                    await gate.wait("http://b.test/direct")
                    self.assertEqual(waits, [2], "A's redirect delay must not embargo unrelated B traffic")

    async def test_redirect_without_location_observes_header_only_once(self):
        now = [10.0]
        gate = RetryAfterGate(clock=lambda: now[0], wall_clock=lambda: 1000)
        response = SimpleNamespace(url=URL("http://a.test"), status=302, headers={"Retry-After": "2"})
        ctx = SimpleNamespace(measurement={})
        trace = base.build_trace_config()
        with patch("single_download.session_http_gate", return_value=gate):
            await trace._redirect(None, ctx, SimpleNamespace(response=response))
            now[0] = 11.0
            await trace._end(None, ctx, SimpleNamespace(response=response))
        self.assertEqual(gate._deadlines[http_authority(response.url)], 12.0)
        self.assertEqual(ctx.measurement["retry_after"], 2)

    def test_retry_after_parser(self):
        for value, expected in (("2", 2), ("0", 0), ("1.5", 1.5), ("1e2", 100), ("+2", 2),
                                ("1_0", 10), (" 2 ", 2)):
            with self.subTest(value=value):
                self.assertEqual(parse_retry_after(value, 1000), expected)
        for value in (None, "", "-1", "nan", "inf", "-inf", "1e999", "bad", "Wed, 99 Foo 2000"):
            with self.subTest(value=value):
                self.assertIsNone(parse_retry_after(value, 1000))
        date = format_datetime(datetime.fromtimestamp(1005, UTC), usegmt=True)
        self.assertEqual(parse_retry_after(date, 1000), 5)
        self.assertEqual(parse_retry_after(date, 1008), 0)

    def test_authority_normalization(self):
        self.assertEqual(http_authority("http://EXAMPLE.org/a"), ("example.org", 80))
        self.assertEqual(http_authority("https://user:pass@example.org/a"), ("example.org", 443))
        self.assertEqual(http_authority("http://example.org:443"), http_authority("https://example.org"))
        self.assertNotEqual(http_authority("http://example.org"), http_authority("https://example.org"))
        self.assertEqual(http_authority("http://[::1]:8080/a"), ("::1", 8080))
        self.assertEqual(http_authority("http://bücher.de"), http_authority("http://xn--bcher-kva.de"))

    async def test_concurrent_waiters_recheck_extensions_and_cancel(self):
        now = [100.0]
        sleeps = []

        async def sleep(delay):
            future = asyncio.get_running_loop().create_future()
            sleeps.append((delay, future))
            await future

        gate = RetryAfterGate(clock=lambda: now[0], wall_clock=lambda: 1000, sleep=sleep)
        gate.observe("http://example.org", "1")
        first = asyncio.create_task(gate.wait("http://EXAMPLE.org:80/a"))
        second = asyncio.create_task(gate.wait("http://example.org/b"))
        try:
            await asyncio.sleep(0)
            self.assertEqual([delay for delay, _ in sleeps], [1, 1])
            await gate.wait("http://example.org:81")
            now[0] = 100.2
            gate.observe("http://example.org", "2")
            gate.observe("http://example.org", "0.1")
            now[0] = 101.0
            for _, future in list(sleeps):
                future.set_result(None)
            await asyncio.sleep(0)
            self.assertEqual(len(sleeps), 4)
            for delay, _ in sleeps[2:]:
                self.assertAlmostEqual(delay, 1.2)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            now[0] = 102.2
            sleeps[-1][1].set_result(None)
            await asyncio.wait_for(second, 1)
        finally:
            first.cancel()
            second.cancel()
            await asyncio.gather(first, second, return_exceptions=True)

    async def test_invalid_values_do_not_poison_and_wall_jumps_do_not_move_deadline(self):
        now = [10.0]
        wall = [1000.0]
        waits = []

        async def sleep(delay):
            waits.append(delay)
            now[0] += delay

        gate = RetryAfterGate(clock=lambda: now[0], wall_clock=lambda: wall[0], sleep=sleep)
        date = format_datetime(datetime.fromtimestamp(1002, UTC), usegmt=True)
        gate.observe("http://a.test", date)
        wall[0] += 9000
        for value in ("bad", "nan", "inf", "-1"):
            self.assertIsNone(gate.observe("http://a.test", value))
        await gate.wait("http://a.test")
        self.assertEqual(waits, [2])

    async def test_session_gate_installs_only_once_and_is_not_global(self):
        async with aiohttp.ClientSession() as first, aiohttp.ClientSession() as second:
            gate = session_http_gate(first)
            self.assertIs(session_http_gate(first), gate)
            self.assertEqual(len(first.trace_configs), 1)
            self.assertIsNot(session_http_gate(second), gate)

    def test_webdataset_metadata_failure_does_not_credit_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "item.jpg"
            path.with_suffix(".json").mkdir()
            sizes = []
            success, error = save_webdataset(b"image", str(path), "key", "http://a.test", None, sizes)
            self.assertFalse(success)
            self.assertIsNotNone(error)
            self.assertEqual(sizes, [])


class LocalHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="flowdc-http-measurement-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.events = {}
        self.retry_headers = {}
        self.retry_deadlines = {}
        self.handlers = {}
        self.url = await self.add_origin()

    async def add_origin(self):
        app = web.Application()
        app.router.add_get("/{name}", self.handle)
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        return f"http://127.0.0.1:{runner.addresses[0][1]}"

    async def handle(self, request):
        name = request.match_info["name"]
        events = self.events.setdefault(name, [])
        events.append(("request", time.monotonic()))
        if name in self.handlers:
            return await self.handlers[name](request)
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

    async def download(self, session, url, manager, *, timeout=5, output=None):
        # Timing/policy cases acquire separate outputs. Collision rejection has
        # independent coverage and must not obscure these HTTP observations.
        self.download_count = getattr(self, "download_count", 0) + 1
        cfg = base.Config(
            input_path="unused", output_folder=str(output or self.root / f"output-{self.download_count}"), timeout_sec=timeout
        )
        return await base.download_one(
            row={"url": url, "__key__": url.rsplit("/", 1)[-1]},
            cfg=cfg,
            session=session,
            total_bytes=[],
            manager=manager,
            sequential_namer=base.SequentialNamer(),
            global_written_paths={},
        )

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
        self.assertFalse(trace["latency_eligible"])

    async def test_failed_status_has_no_first_byte_sample(self):
        result, trace, _ = await self.fetch("missing")
        self.assertIsNone(result[0])
        self.assertEqual(result[1], 404)
        self.assertIsNotNone(result[2])
        self.assertIsNone(trace.get("ttfb"))

    async def test_timestamps_distinguish_headers_first_byte_and_completion(self):
        for name in ("headers", "first", "tail", "empty"):
            with self.subTest(name=name):
                result, trace, _ = await self.fetch(name)
                self.assertEqual(result[1], 200)
                self.assertLessEqual(trace["attempt_started_at"], trace["t0"])
                self.assertLessEqual(trace["t0"], trace["final_headers_at"])
                self.assertLessEqual(trace["final_headers_at"], trace["body_completed_at"])
                self.assertEqual(len(trace["hops"]), 1)
                if name == "empty":
                    self.assertIsNone(trace["first_body_byte_at"])
                else:
                    self.assertLessEqual(trace["final_headers_at"], trace["first_body_byte_at"])
                    self.assertLessEqual(trace["first_body_byte_at"], trace["body_completed_at"])
                    self.assertAlmostEqual(trace["ttfb"], trace["first_body_byte_at"] - trace["t0"])
                if name == "headers":
                    self.assertGreaterEqual(trace["final_headers_at"] - trace["t0"], 0.35)
                elif name == "first":
                    self.assertGreaterEqual(trace["first_body_byte_at"] - trace["final_headers_at"], 0.35)
                elif name == "tail":
                    self.assertGreaterEqual(trace["body_completed_at"] - trace["first_body_byte_at"], 0.35)

    async def test_independent_authority_progresses_while_other_is_embargoed(self):
        other = await self.add_origin()
        async with aiohttp.ClientSession() as session:
            session_http_gate(session).observe(self.url, "5")
            waiting = asyncio.create_task(download_via_http_get(session, f"{self.url}/waiting", 10))
            try:
                result = await asyncio.wait_for(download_via_http_get(session, f"{other}/independent", 2), 3)
                self.assertEqual(result[:3], (b"ab", 200, None))
                self.assertFalse(waiting.done())
                self.assertNotIn("waiting", self.events)
            finally:
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)

    async def test_concurrent_extension_and_shorter_update_integration(self):
        async with aiohttp.ClientSession() as session:
            gate = session_http_gate(session)
            gate.observe(self.url, "0.3")
            tasks = [
                asyncio.create_task(download_via_http_get(session, f"{self.url}/wait{i}", 3))
                for i in range(3)
            ]
            try:
                await asyncio.sleep(0.1)
                deadline = time.monotonic() + 0.5
                gate.observe(self.url, "0.5")
                gate.observe(self.url, "0.01")
                results = await asyncio.wait_for(asyncio.gather(*tasks), 4)
                self.assertTrue(all(result[1] == 200 for result in results))
                for i in range(3):
                    self.assertGreaterEqual(self.events[f"wait{i}"][0][1], deadline - 0.05)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def test_redirect_retry_after_delays_chain_but_not_unrelated_destination_requests(self):
        other = await self.add_origin()
        for form in ("numeric", "date"):
            with self.subTest(form=form):
                observed = asyncio.Event()
                deadline = []

                async def redirect(request, form=form, deadline=deadline):
                    header, delay = "2", 2
                    if form == "date":
                        date = (datetime.now(UTC) + timedelta(seconds=3)).replace(microsecond=0)
                        header, delay = format_datetime(date, usegmt=True), date.timestamp() - time.time()
                    deadline.append(time.monotonic() + delay)
                    return web.Response(status=302, headers={
                        "Location": f"{other}/destination-{form}", "Retry-After": header,
                    })

                async def on_redirect(*_args, observed=observed):
                    observed.set()

                self.handlers["redirect"] = redirect
                trace = base.build_trace_config()
                trace.on_request_redirect.insert(0, on_redirect)
                async with aiohttp.ClientSession(trace_configs=[trace]) as session:
                    following = asyncio.create_task(download_via_http_get(session, f"{self.url}/redirect", 5))
                    waiting = None
                    try:
                        await asyncio.wait_for(observed.wait(), 2)
                        waiting = asyncio.create_task(download_via_http_get(session, f"{self.url}/blocked", 5))
                        direct = await download_via_http_get(session, f"{other}/direct-{form}", 2)
                        self.assertEqual(direct[1], 200)
                        self.assertLess(self.events[f"direct-{form}"][0][1], deadline[0])
                        self.assertFalse(following.done())
                        self.assertFalse(waiting.done())
                        self.assertNotIn("blocked", self.events)
                        waiting.cancel()
                        await asyncio.gather(waiting, return_exceptions=True)
                        result = await asyncio.wait_for(following, 6)
                        self.assertEqual(result[:3], (b"ab", 200, None))
                        self.assertIsNone(result[3], "source header must not be attributed to destination")
                        self.assertGreaterEqual(self.events[f"destination-{form}"][0][1], deadline[0] - 0.05)
                    finally:
                        tasks = [task for task in (following, waiting) if task is not None]
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)

    async def test_redirect_delay_timeout_and_cancellation_release_resources(self):
        other = await self.add_origin()
        for module in (None, base, gradient):
            for cancel in (False, True):
                with self.subTest(module=getattr(module, "__name__", "fixed"), cancel=cancel):
                    seen = asyncio.Event()

                    async def redirect(request):
                        # Leave a response body open: the callback must release
                        # its connection before waiting on the header's delay.
                        response = web.StreamResponse(status=302, headers={
                            "Location": f"{other}/never-followed", "Retry-After": "30",
                            "Content-Length": "100",
                        })
                        await response.prepare(request)
                        return response

                    async def on_redirect(*_args, seen=seen):
                        seen.set()

                    self.handlers["long-redirect"] = redirect
                    manager = module.HostControllerManager(module.PAARCConfig()) if module else None
                    trace = base.build_trace_config()
                    trace.on_request_redirect.insert(0, on_redirect)
                    async with aiohttp.ClientSession(
                        connector=aiohttp.TCPConnector(limit=1), trace_configs=[trace]
                    ) as session:
                        task = asyncio.create_task(self.download(
                            session, f"{self.url}/long-redirect", manager, timeout=5 if cancel else 0.2
                        ))
                        try:
                            await asyncio.wait_for(seen.wait(), 2)
                            if cancel:
                                task.cancel()
                                with self.assertRaises(asyncio.CancelledError):
                                    await task
                            else:
                                out = await asyncio.wait_for(task, 2)
                                self.assertEqual(out.status_code, 408)
                                self.assertFalse(out.success)
                            self.assertNotIn("never-followed", self.events)
                            # Same connector has capacity after the aborted hop.
                            self.assertEqual((await download_via_http_get(session, f"{other}/available", 2))[1], 200)
                            if manager:
                                ctrl = await manager.get_controller(self.url)
                                self.assertEqual(ctrl.semaphore.inflight, 0)
                                snap = await ctrl.metrics.finish_interval()
                                self.assertFalse(snap["has_overload"])
                                self.assertEqual(snap["n_samples"], 0)
                        finally:
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)

    async def test_same_authority_redirect_hop_waits(self):
        deadline = []

        async def redirect(request):
            deadline.append(time.monotonic() + 0.4)
            return web.Response(status=302, headers={"Location": "/destination", "Retry-After": "0.4"})

        self.handlers["redirect"] = redirect
        result, trace, _ = await self.fetch("redirect")
        self.assertEqual(result[:3], (b"ab", 200, None))
        self.assertEqual(len(trace["hops"]), 2)
        self.assertGreaterEqual(self.events["destination"][0][1], deadline[0] - 0.05)
        self.assertGreaterEqual(trace["t0"], deadline[0] - 0.05)
        self.assertLess(trace["ttfb"], trace["body_completed_at"] - trace["attempt_started_at"] - 0.25)

    async def test_cross_authority_redirect_respects_destination_embargo_and_feedback(self):
        other = await self.add_origin()

        async def redirect(request):
            return web.Response(status=302, headers={"Location": f"{other}/destination"})

        self.handlers["redirect"] = redirect
        for module in (base, gradient):
            with self.subTest(module=module.__name__):
                manager = module.HostControllerManager(module.PAARCConfig())
                async with aiohttp.ClientSession() as session:
                    deadline = time.monotonic() + 0.4
                    session_http_gate(session).observe(other, "0.4")
                    out = await self.download(session, f"{self.url}/redirect", manager)
                self.assertTrue(out.success, out.error)
                requests = [when for kind, when in self.events["destination"] if kind == "request"]
                self.assertGreaterEqual(requests[-1], deadline - 0.05)
                origin_ctrl = await manager.get_controller(self.url)
                final_ctrl = await manager.get_controller(other)
                origin = await origin_ctrl.metrics.finish_interval()
                final = await final_ctrl.metrics.finish_interval()
                self.assertEqual(origin["total"], 0)
                self.assertEqual(final["n_success"], 1)
                self.assertEqual(final["n_samples"], 1)
                self.assertEqual(origin_ctrl.semaphore.inflight, 0)
                self.assertEqual(final_ctrl.semaphore.inflight, 0)

    async def test_redirects_obey_destination_adaptive_limit(self):
        other = await self.add_origin()
        for module in (base, gradient):
            with self.subTest(module=module.__name__):
                manager = module.HostControllerManager(module.PAARCConfig(C_init=1, C_min=1))
                redirects, destinations = [], []
                both_redirects, arrived, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

                async def redirect(request, redirects=redirects, both_redirects=both_redirects):
                    redirects.append(request.match_info["name"])
                    if len(redirects) == 2:
                        both_redirects.set()
                    return web.Response(status=302, headers={"Location": f"{other}/limited"})

                async def destination(request, destinations=destinations, arrived=arrived, release=release):
                    destinations.append(time.monotonic())
                    arrived.set()
                    await release.wait()
                    return web.Response(body=b"payload")

                self.handlers.update(redirect0=redirect, redirect1=redirect, limited=destination)
                async with aiohttp.ClientSession() as session:
                    tasks = [asyncio.create_task(self.download(
                        session, f"{self.url}/redirect{i}", manager, output=self.root / f"limit-{module.__name__}-{i}"
                    ))
                             for i in range(2)]
                    try:
                        await asyncio.wait_for(asyncio.gather(both_redirects.wait(), arrived.wait()), 2)
                        origin_ctrl = await manager.get_controller(self.url)
                        final_ctrl = await manager.get_controller(other)
                        await asyncio.sleep(0.05)
                        self.assertEqual(len(destinations), 1, "second hop must wait for destination's permit")
                        self.assertEqual(origin_ctrl.semaphore.inflight, 0)
                        self.assertEqual(final_ctrl.semaphore.inflight, 1)
                        release.set()
                        outcomes = await asyncio.wait_for(asyncio.gather(*tasks), 3)
                        self.assertTrue(all(out.success for out in outcomes))
                        self.assertEqual(len(destinations), 2)
                        self.assertEqual(final_ctrl.semaphore.inflight, 0)
                        self.assertEqual((await final_ctrl.metrics.finish_interval())["n_success"], 2)
                    finally:
                        release.set()
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)

    async def test_reciprocal_redirects_release_before_acquiring_destination(self):
        other = await self.add_origin()
        for module in (base, gradient):
            with self.subTest(module=module.__name__):
                manager = module.HostControllerManager(module.PAARCConfig(C_init=1, C_min=1))
                started = []
                both = asyncio.Event()

                async def redirect(request, started=started, both=both):
                    started.append(request.match_info["name"])
                    if len(started) == 2:
                        both.set()
                    await both.wait()
                    target = other if request.match_info["name"] == "from-a" else self.url
                    return web.Response(status=302, headers={"Location": f"{target}/reciprocal-final"})

                self.handlers.update({"from-a": redirect, "from-b": redirect})
                async with aiohttp.ClientSession() as session:
                    tasks = [asyncio.create_task(self.download(
                        session, url, manager, output=self.root / f"reciprocal-{module.__name__}-{i}"
                    )) for i, url in enumerate((f"{self.url}/from-a", f"{other}/from-b"))]
                    try:
                        outcomes = await asyncio.wait_for(asyncio.gather(*tasks), 3)
                        self.assertTrue(all(out.success for out in outcomes))
                        for authority in (self.url, other):
                            ctrl = await manager.get_controller(authority)
                            self.assertEqual(ctrl.semaphore.inflight, 0)
                            self.assertEqual((await ctrl.metrics.finish_interval())["n_success"], 1)
                    finally:
                        both.set()
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)

    async def test_destination_permit_wait_timeout_and_cancellation(self):
        other = await self.add_origin()
        for module in (base, gradient):
            for cancel in (False, True):
                with self.subTest(module=module.__name__, cancel=cancel):
                    manager = module.HostControllerManager(module.PAARCConfig(C_init=1, C_min=1))
                    final_ctrl = await manager.get_controller(other)
                    await final_ctrl.semaphore.acquire()
                    waiting = asyncio.Event()
                    acquire = final_ctrl.semaphore.acquire

                    async def observed_acquire(waiting=waiting, acquire=acquire):
                        waiting.set()
                        await acquire()

                    async def redirect(request):
                        return web.Response(status=302, headers={"Location": f"{other}/permit-blocked"})

                    self.handlers["permit-redirect"] = redirect
                    async with aiohttp.ClientSession() as session:
                        with patch.object(final_ctrl.semaphore, "acquire", side_effect=observed_acquire):
                            task = asyncio.create_task(self.download(
                                session, f"{self.url}/permit-redirect", manager, timeout=5 if cancel else 0.2
                            ))
                            try:
                                await asyncio.wait_for(waiting.wait(), 2)
                                if cancel:
                                    task.cancel()
                                    with self.assertRaises(asyncio.CancelledError):
                                        await task
                                else:
                                    out = await asyncio.wait_for(task, 2)
                                    self.assertEqual(out.status_code, 408)
                                    snap = await final_ctrl.metrics.finish_interval()
                                    self.assertEqual(snap["n_local_failures"], 1)
                                    self.assertFalse(snap["has_overload"])
                                self.assertNotIn("permit-blocked", self.events)
                                self.assertEqual((await manager.get_controller(self.url)).semaphore.inflight, 0)
                                self.assertEqual(final_ctrl.semaphore.inflight, 1, "another request's permit must remain owned")
                            finally:
                                task.cancel()
                                await asyncio.gather(task, return_exceptions=True)
                                await final_ctrl.semaphore.release()

    async def test_connector_wait_rechecks_embargo_observed_at_headers(self):
        requested, headers_allowed, tail_allowed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        queued, headers_seen = asyncio.Event(), asyncio.Event()
        deadline = []

        async def announce(request):
            requested.set()
            await headers_allowed.wait()
            response = web.StreamResponse(headers={"Content-Length": "2", "Retry-After": "0.5"})
            deadline.append(time.monotonic() + 0.5)
            await response.prepare(request)
            await response.write(b"a")
            await tail_allowed.wait()
            await response.write(b"b")
            return response

        async def on_queued(*_args):
            queued.set()

        async def on_headers(_session, _ctx, params):
            if params.url.path == "/announce":
                headers_seen.set()

        self.handlers["announce"] = announce
        trace = base.build_trace_config()
        trace.on_connection_queued_start.append(on_queued)
        trace.on_request_end.append(on_headers)
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=1), trace_configs=[trace]
        ) as session:
            first = asyncio.create_task(download_via_http_get(session, f"{self.url}/announce", 4))
            second = None
            try:
                await asyncio.wait_for(requested.wait(), 2)
                second = asyncio.create_task(download_via_http_get(session, f"{self.url}/queued", 4))
                await asyncio.wait_for(queued.wait(), 2)
                headers_allowed.set()
                await asyncio.wait_for(headers_seen.wait(), 2)
                self.assertFalse(first.done(), "header observed before body completion")
                tail_allowed.set()
                results = await asyncio.wait_for(asyncio.gather(first, second), 5)
                self.assertTrue(all(result[1] == 200 for result in results))
                self.assertGreaterEqual(self.events["queued"][0][1], deadline[0] - 0.05)
            finally:
                headers_allowed.set()
                tail_allowed.set()
                tasks = [task for task in (first, second) if task is not None]
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def test_concurrent_responses_cannot_shorten_active_embargo(self):
        arrived = {name: asyncio.Event() for name in ("long", "short")}
        release = {name: asyncio.Event() for name in arrived}
        deadline = []

        async def respond(request):
            name = request.match_info["name"]
            arrived[name].set()
            await release[name].wait()
            if name == "long":
                deadline.append(time.monotonic() + 0.6)
            return web.Response(status=429, headers={"Retry-After": "0.6" if name == "long" else "0.01"})

        self.handlers.update(long=respond, short=respond)
        async with aiohttp.ClientSession() as session:
            tasks = {
                name: asyncio.create_task(download_via_http_get(session, f"{self.url}/{name}", 3))
                for name in arrived
            }
            try:
                await asyncio.wait_for(asyncio.gather(*(event.wait() for event in arrived.values())), 2)
                release["long"].set()
                self.assertEqual((await tasks["long"])[1], 429)
                release["short"].set()
                self.assertEqual((await tasks["short"])[1], 429)
                result = await asyncio.wait_for(download_via_http_get(session, f"{self.url}/after", 3), 4)
                self.assertEqual(result[1], 200)
                self.assertGreaterEqual(self.events["after"][0][1], deadline[0] - 0.05)
            finally:
                for event in release.values():
                    event.set()
                for task in tasks.values():
                    task.cancel()
                await asyncio.gather(*tasks.values(), return_exceptions=True)

    async def test_cancellation_during_body_read_releases_adaptive_permit(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def stall(request):
            response = web.StreamResponse(headers={"Content-Length": "2"})
            await response.prepare(request)
            await response.write(b"a")
            started.set()
            await release.wait()
            return response

        self.handlers["cancel-body"] = stall
        manager = base.HostControllerManager(base.PAARCConfig())
        async with aiohttp.ClientSession() as session:
            task = asyncio.create_task(self.download(session, f"{self.url}/cancel-body", manager))
            try:
                await asyncio.wait_for(started.wait(), 2)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                ctrl = await manager.get_controller(self.url)
                self.assertEqual(ctrl.semaphore.inflight, 0)
                self.assertEqual((await ctrl.metrics.finish_interval())["n_samples"], 0)
            finally:
                release.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_local_output_failure_is_not_success_or_overload(self):
        blocked = self.root / "not-a-directory"
        blocked.write_text("preserve this file")
        for module in (base, gradient):
            with self.subTest(module=module.__name__):
                manager = module.HostControllerManager(module.PAARCConfig())
                async with aiohttp.ClientSession() as session:
                    out = await self.download(session, f"{self.url}/output", manager, output=blocked)
                self.assertFalse(out.success)
                ctrl = await manager.get_controller(self.url)
                snap = await ctrl.metrics.finish_interval()
                self.assertEqual(snap["total"], 1)
                self.assertEqual(snap["n_local_failures"], 1)
                self.assertEqual(snap["n_success"], 0)
                self.assertEqual(snap["bytes"], 0)
                self.assertEqual(snap["n_samples"], 1)
                self.assertFalse(snap["has_overload"])
                self.assertEqual(ctrl.semaphore.inflight, 0)
        self.assertEqual(blocked.read_text(), "preserve this file")

    async def test_completed_body_survives_stat_and_metadata_failures(self):
        for module in (base, gradient):
            for failure in ("save-stat", "final-stat", "metadata"):
                with self.subTest(module=module.__name__, failure=failure):
                    output = self.root / f"{module.__name__}-{failure}"
                    output.mkdir()
                    cfg = base.Config(
                        input_path="unused", output_folder=str(output), output_format="webdataset",
                        naming_mode="url_based", file_name_pattern="payload",
                    )
                    manager = module.HostControllerManager(module.PAARCConfig())
                    getsize, lookups, sizes = os.path.getsize, [], []

                    def verify_size(path, failure=failure, lookups=lookups, getsize=getsize, output=output):
                        self.assertEqual(Path(path).read_bytes(), b"ab")
                        lookups.append(path)
                        if failure == "metadata" and len(lookups) == 1:
                            (output / "payload.json").mkdir()
                        if failure == "save-stat" or (failure == "final-stat" and len(lookups) == 2):
                            raise OSError("injected output stat failure")
                        return getsize(path)

                    async with aiohttp.ClientSession() as session:
                        with patch("os.path.getsize", side_effect=verify_size):
                            out = await base.download_one(
                                row={"url": f"{self.url}/output", "__key__": "row"},
                                cfg=cfg, session=session, total_bytes=sizes, manager=manager,
                                sequential_namer=base.SequentialNamer(), global_written_paths={},
                            )
                    self.assertFalse(out.success)
                    self.assertEqual(out.status_code, 200)
                    self.assertEqual(out.bytes_downloaded, 0)
                    self.assertEqual(sizes, [])
                    self.assertEqual(Path(out.file_path).read_bytes(), b"ab")
                    ctrl = await manager.get_controller(self.url)
                    snap = await ctrl.metrics.finish_interval()
                    self.assertEqual(snap["total"], 1)
                    self.assertEqual(snap["n_failed"], 1)
                    self.assertEqual(snap["n_local_failures"], 1)
                    self.assertEqual(snap["n_success"], 0)
                    self.assertEqual(snap["bytes"], 0)
                    self.assertEqual(snap["n_samples"], 1)
                    self.assertFalse(snap["has_overload"])
                    self.assertEqual(ctrl.semaphore.inflight, 0)

    async def test_admission_timeout_is_bounded_and_releases_permit(self):
        for module in (base, gradient):
            with self.subTest(module=module.__name__):
                manager = module.HostControllerManager(module.PAARCConfig())
                async with aiohttp.ClientSession() as session:
                    session_http_gate(session).observe(self.url, "30")
                    out = await asyncio.wait_for(
                        self.download(session, f"{self.url}/timeout", manager, timeout=0.1), 2
                    )
                self.assertEqual(out.status_code, 408)
                self.assertFalse(out.success)
                self.assertNotIn("timeout", self.events)
                ctrl = await manager.get_controller(self.url)
                self.assertEqual(ctrl.semaphore.inflight, 0)
                snap = await ctrl.metrics.finish_interval()
                self.assertEqual(snap["n_local_failures"], 1)
                self.assertFalse(snap["has_overload"], "admission timeout is not server overload")

    async def test_body_timeout_and_truncation_do_not_supply_success_samples(self):
        release = asyncio.Event()

        async def stall(request):
            response = web.StreamResponse(headers={"Content-Length": "2"})
            await response.prepare(request)
            await response.write(b"a")
            await release.wait()
            return response

        async def truncate(request):
            response = web.StreamResponse(headers={"Content-Length": "2"})
            await response.prepare(request)
            await response.write(b"a")
            request.transport.close()
            return response

        self.handlers.update(stall=stall, truncate=truncate)
        try:
            for name in ("stall", "truncate"):
                with self.subTest(name=name):
                    trace = {}
                    token = base.TRACE_CTX.set(trace)
                    try:
                        async with aiohttp.ClientSession() as session:
                            result = await asyncio.wait_for(
                                download_via_http_get(session, f"{self.url}/{name}", 0.2), 2
                            )
                    finally:
                        base.TRACE_CTX.reset(token)
                    self.assertIsNone(result[0])
                    self.assertIsNotNone(result[2])
                    self.assertIsNone(trace["ttfb"])
                    self.assertFalse(trace["latency_eligible"])
                    self.assertIsNone(trace["body_completed_at"])
                    self.assertEqual(trace["failure_kind"], "transport")
        finally:
            release.set()

    async def test_batch_cancellation_and_shutdown_release_waiting_permits(self):
        for module in (base, gradient):
            for shutdown in (False, True):
                with (
                    self.subTest(module=module.__name__, shutdown=shutdown),
                    patch.object(base, "shutdown_flag", False),
                ):
                    manager = module.HostControllerManager(module.PAARCConfig(C_init=1, C_min=1, C_max=1))
                    cfg = base.Config(input_path="unused", output_folder=str(self.root / "batch"))
                    before = asyncio.all_tasks()
                    async with aiohttp.ClientSession() as session:
                        session_http_gate(session).observe(self.url, "1000")
                        task = asyncio.create_task(
                            base.download_batch_bounded(
                                cfg=cfg,
                                session=session,
                                df=pl.DataFrame(
                                    {"url": [f"{self.url}/a", f"{self.url}/b"], "__key__": ["a", "b"]}
                                ),
                                manager=manager,
                                sequential_namer=base.SequentialNamer(),
                                global_written_paths={},
                                effective_workers=2,
                            )
                        )
                        try:
                            ctrl = await manager.get_controller(self.url)
                            async with asyncio.timeout(2):
                                while ctrl.semaphore.inflight == 0:
                                    await asyncio.sleep(0)
                            if shutdown:
                                base.shutdown_flag = True
                                self.assertEqual(await asyncio.wait_for(task, 2), {})
                            else:
                                task.cancel()
                                with self.assertRaises(asyncio.CancelledError):
                                    await task
                            self.assertEqual(ctrl.semaphore.inflight, 0)
                        finally:
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                    self.assertEqual(asyncio.all_tasks() - before, set())

    async def check_retry_after(self, header, *, exhaust_budget=False):
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
                                "timeout": 1 if exhaust_budget else 5,
                                "max_retry_attempts": 3 if exhaust_budget else 2,
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
                    requests = [when for kind, when in self.events[name] if kind == "request"]
                    if exhaust_budget:
                        self.assertEqual(report["summary"]["successful_downloads"], 0)
                        self.assertEqual(report["summary"]["failed_downloads"], 1)
                        self.assertEqual(report["error_breakdown"][0]["status_code"], 408)
                        self.assertEqual(len(requests), 1, "embargo must not be bypassed by timeout/retry")
                        self.assertIn("[Attempt 3]", stdout.decode())
                        self.assertNotIn("[Attempt 4]", stdout.decode())
                        continue
                    self.assertEqual(report["summary"]["successful_downloads"], 1)
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

    async def test_long_retry_after_can_exhaust_bounded_attempt_budget(self):
        await self.check_retry_after("30", exhaust_budget=True)


if __name__ == "__main__":
    unittest.main()
