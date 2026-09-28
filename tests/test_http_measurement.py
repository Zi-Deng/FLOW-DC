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
from unittest.mock import patch

import aiohttp
import polars as pl
from aiohttp import web

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
    def test_overview_labels_semantics_and_unfinished_denominator(self):
        cfg = base.Config(input_path="unused", output_folder="unused")
        outcomes = {
            "ok": base.DownloadOutcome("ok", "http://a.test", True, "saved", None, 200, None, 10),
            "fail": base.DownloadOutcome("fail", "http://a.test", False, None, None, 404, "missing"),
        }
        report = base.generate_overview_report(cfg=cfg, df_total=3, outcomes=outcomes, elapsed_sec=1)
        self.assertEqual(report["http_measurement"]["version"], "2-body-first-byte")
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
    def test_retry_after_parser(self):
        for value, expected in (("2", 2), ("0", 0), ("1.5", 1.5), ("1e2", 100), ("+2", 2)):
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
        cfg = base.Config(
            input_path="unused", output_folder=str(output or self.root / "output"), timeout_sec=timeout
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

    async def test_redirect_retry_after_applies_to_responding_authority(self):
        other = await self.add_origin()

        async def redirect(request):
            return web.Response(status=302, headers={"Location": f"{other}/destination", "Retry-After": "5"})

        self.handlers["redirect"] = redirect
        async with aiohttp.ClientSession() as session:
            result = await asyncio.wait_for(download_via_http_get(session, f"{self.url}/redirect", 2), 3)
            self.assertEqual(result[:3], (b"ab", 200, None))
            self.assertIsNone(result[3], "redirect header must not be attributed to final authority")
            waiting = asyncio.create_task(download_via_http_get(session, f"{self.url}/blocked", 10))
            try:
                await asyncio.sleep(0.05)
                self.assertFalse(waiting.done())
                self.assertNotIn("blocked", self.events)
            finally:
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)

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
                self.assertEqual(snap["n_samples"], 0)
                self.assertFalse(snap["has_overload"])
                self.assertEqual(ctrl.semaphore.inflight, 0)
        self.assertEqual(blocked.read_text(), "preserve this file")

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
