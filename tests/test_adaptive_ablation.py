"""Controller, exposure and native planned-window regressions; loopback only."""

import asyncio
import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl
from aiohttp import web
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
import download_batch as downloader
from flowdc_methods import DelayPolicy, MethodConfig

from benchmark.real_server_study import ablation, ablation_run, common, telemetry, verify


class DesignTests(unittest.TestCase):
    def test_matrix_bounded_and_grid_complete(self):
        rows = ablation.schedule()
        self.assertEqual(len(rows), 36)
        self.assertEqual(len({r["run_id"] for r in rows}), 36)
        for method in ablation.METHODS:
            self.assertEqual(
                sorted(r["grid"] for r in rows if r["phase"] == "screen" and r["method"] == method),
                list(range(8)),
            )
        self.assertLessEqual(len(rows) * ablation.ROWS + 684, ablation.LIMITS["max_requests"])
        self.assertEqual(rows, ablation.schedule())

    def test_cold_start_reaches_150_without_target_and_holds_when_application_limited(self):
        for method in ablation.METHODS:
            cfg = MethodConfig.from_config(
                downloader.Config("input", "output", control_method=method, C_max=512)
            )
            policy = DelayPolicy(cfg)
            limits = []
            for n in range(30):
                row = policy.step(float(n), [0.05] * 10, inflight=policy.limit)
                limits.append(row["limit"])
            self.assertEqual(limits[0], 4)
            self.assertGreater(max(limits), 150)
            before = policy.limit
            row = policy.step(31.0, [0.05] * 10, inflight=0)
            self.assertEqual(row["limit"], before)
            self.assertEqual(row["reason"], "utilization_hold")
            row = policy.step(32.0, [], overload=True, inflight=before)
            self.assertEqual(row["limit"], max(2, before // 2))

    def test_sampling_baseline_refresh_and_option_bounds(self):
        cfg = MethodConfig.from_config(
            downloader.Config(
                "input",
                "output",
                control_method="ratio-v2",
                C_max=512,
                method_options={"baseline_max_age_s": 10},
            )
        )
        p = DelayPolicy(cfg)
        p.step(0.0, [0.05] * 5, inflight=4)
        self.assertEqual(p.step(1.0, [], inflight=4)["reason"], "empty_hold")
        self.assertEqual(p.step(10.0, [0.05] * 5, inflight=4)["reason"], "baseline_probe")
        self.assertEqual(p.limit, 2)
        self.assertEqual(p.step(11.0, [0.05] * 5, inflight=2)["reason"], "baseline_reset")
        for bad in (0, 1, 3, float("nan")):
            with self.assertRaises(ValueError):
                MethodConfig(method="ratio-v2", startup_growth_factor=bad)
        with self.assertRaises(ValueError):
            downloader.normalize_config(downloader.Config("i", "o", http_connection_limit=0))

    def test_actual_exposure_does_not_credit_permit_peaks_or_short_backlog(self):
        def events(active, backlog):
            out = [{"event": "dispatch", "monotonic_s": 0.0, "inflight": active}]
            out += [
                {
                    "event": "sample",
                    "monotonic_s": float(t),
                    "inflight": active,
                    "cap": 512,
                    "backlog": backlog,
                }
                for t in range(100)
            ]
            return out

        self.assertFalse(ablation.exposure(events(149, 10), "validation", {}, 0.1)["qualified"])
        self.assertTrue(ablation.exposure(events(150, 10), "validation", {}, 0.1)["qualified"])
        self.assertFalse(ablation.exposure(events(200, 0), "validation", {}, 0.1)["qualified"])
        self.assertFalse(ablation.exposure(events(200, 10)[:30], "validation", {}, 0.1)["qualified"])

    def test_three_x_warns_five_x_stops_and_refusal_is_immediate(self):
        p = ablation.Policy(0.1, 100)
        for n in range(101):
            p.complete(n / 3, 0.31)
            p.tick(n / 3)
        self.assertTrue(p.warning)
        self.assertIsNone(p.reason)
        p = ablation.Policy(0.1, 100)
        for n in range(101):
            p.complete(n / 3, 0.51)
            p.tick(n / 3)
        self.assertEqual(p.reason, "sustained_latency")
        p = ablation.Policy(0.1, 100)
        p.header(0, 429)
        self.assertEqual(p.reason, "explicit_429")
        p.header(1, 403)
        self.assertTrue(p.fatal)


class NativeWindowTests(unittest.IsolatedAsyncioTestCase):
    async def test_planned_prefix_preserves_archive_credit_and_original_denominator(self):
        payload = io.BytesIO()
        Image.new("RGB", (17, 13), (17, 90, 4)).save(payload, "JPEG")
        raw = payload.getvalue()

        async def handle(request):
            await asyncio.sleep(0.03)
            return web.Response(body=raw, content_type="image/jpeg")

        app = web.Application()
        app.router.add_get("/{name}", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            with tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                native = directory / "native"
                manifest = directory / "input.parquet"
                urls = [f"http://127.0.0.1:{port}/{n}.jpg" for n in range(256)]
                pl.DataFrame({"url": urls}).write_parquet(manifest)
                data = dict(
                    input=str(manifest),
                    output=str(native),
                    url="url",
                    control_method="ratio-v2",
                    concurrent_downloads=16,
                    http_connection_limit=8,
                    C_min=2,
                    C_init=4,
                    C_max=16,
                    max_retry_attempts=1,
                    research_profile=True,
                    research_workload=ablation.PROFILE,
                )
                common.write_new(directory / "flowdc-config.json", data)
                common.write_new(directory / "attempt.json", {"protocol_sha256": "fixture"})
                old_argv = sys.argv
                sys.argv = ["download_batch.py", "--config", str(directory / "flowdc-config.json")]
                cfg = downloader.parse_args()
                sys.argv = old_argv
                policy = ablation.Policy(0.05, 100)
                with (directory / "request-audit.jsonl").open("x") as stream:
                    guard = ablation_run.WindowGuard(
                        urls, stream, 0.05, policy=policy, body_limit=1024**2, seconds=0.25
                    )
                    guard.emit("start")
                    with patch.object(telemetry, "checked_url", side_effect=lambda u: u):
                        restore = guard.install()
                        downloader.shutdown_flag = False
                        started = __import__("time").monotonic()
                        try:
                            await downloader.run_acquisition(cfg)
                        finally:
                            restore()
                            for task in (guard.timer, guard.sampler):
                                if task:
                                    task.cancel()
                                    with contextlib.suppress(asyncio.CancelledError):
                                        await task
                        guard.emit("closed", active=len(guard.active), stop_reason=policy.reason)
                ended = __import__("time").monotonic()
                common.write_new(
                    directory / "planned-stop.json", {"reason": policy.reason, "protocol_sha256": "fixture"}
                )
                result = verify.verify(directory, manifest, ROOT, started, ended, True, True)
                self.assertEqual(result["original_rows"], 256)
                self.assertGreater(result["credited_rows"], 0)
                self.assertLess(result["credited_rows"], 256)
                self.assertFalse(result["native_run_complete"])
                self.assertTrue(result["protocol_window_complete"])
                self.assertEqual(sum(result["dispositions"].values()), 256)
                self.assertEqual(sum(result["credited_url_bytes"].values()), result["credited_bytes"])
                self.assertLessEqual(
                    max(
                        e.get("inflight", 0)
                        for e in ablation_run.audit_events(directory / "request-audit.jsonl")[0]
                    ),
                    8,
                )
        finally:
            downloader.shutdown_flag = False
            await runner.cleanup()


class ReceiptTests(unittest.TestCase):
    def test_selection_uses_dispatched_denominator_and_separates_target(self):
        def row(n, median, goodput, qualifies):
            return dict(
                run_id=f"screen-ratio-v2-{n}",
                phase="screen",
                method="ratio-v2",
                grid=n,
                record_sha256=str(n),
                protocol_window_complete=True,
                stop_reason=None,
                verification={"credited_rows": 100, "dispatched_rows": 100, "original_rows": 32768},
                exposure=dict(
                    balanced=True,
                    qualified=qualifies,
                    warning_response="not_exercised",
                    window_goodput_Bps=goodput,
                    active_median=median,
                    p95_ttfb_s=0.2,
                ),
            )

        rows = [row(0, 100, 1000, False), row(1, 160, 990, True), row(2, 155, 960, True)]
        choices = ablation.select(rows)["choices"]
        self.assertEqual(choices["balanced-ratio-v2"]["screen_run"], rows[0]["run_id"])
        self.assertEqual(choices["high-ratio-v2"]["screen_run"], rows[2]["run_id"])
        rows[2]["exposure"]["warning_response"] = "failed"
        self.assertEqual(ablation.select(rows)["choices"]["high-ratio-v2"]["screen_run"], rows[1]["run_id"])
        self.assertNotIn("balanced-gradient-candidate-v2", choices)

    def test_metadata_bundle_digest_and_path_checks(self):
        from benchmark.real_server_study.ablation_analysis import seal
        from benchmark.real_server_study.ablation_collect import extract

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "run"
            run.mkdir()
            (run / "native").mkdir()
            common.write_new(run / "native/overview.json", {"fixture": True})
            (run / "native/payload.jpg").write_bytes(b"payload excluded")
            receipt = seal(run)
            dest = root / "out"
            dest.mkdir()
            extract(run / receipt["path"], dest, 1024**2)
            self.assertTrue(common.read_json(dest / "native/overview.json")["fixture"])
            self.assertFalse((dest / "native/payload.jpg").exists())
            with self.assertRaises(ValueError):
                seal(run)
            with self.assertRaises(ValueError):
                extract(run / receipt["path"], root / "small", 1)

    def test_v1_rejects_new_options_and_v2_does_not_force_150(self):
        for method in ("ratio-v1", "gradient-candidate-v1"):
            with self.assertRaises(ValueError):
                MethodConfig.from_config(
                    downloader.Config(
                        "i", "o", control_method=method, method_options={"target_delay_ratio": 3}
                    )
                )
        for method in ablation.METHODS:
            policy = DelayPolicy(
                MethodConfig.from_config(downloader.Config("i", "o", control_method=method, C_max=512))
            )
            policy.step(0.0, [0.1] * 10, inflight=4)
            for n in range(1, 20):
                policy.step(float(n), [0.7] * 10, inflight=policy.limit)
            self.assertLess(policy.limit, 150)
            self.assertEqual(policy.limit, 2)

    def test_interrupted_probe_keeps_reservation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "health").mkdir()
            common.write_new(root / "health/probe-intent.json", {})
            self.assertEqual(ablation_run.budget(root), 2**30)


class CloudBoundaryTests(unittest.TestCase):
    def test_provider_failure_quiesces_and_cleans_without_scientific_replay(self):
        from unittest.mock import MagicMock
        from benchmark.real_server_study import ablation_cloud
        import flowdc_ops
        import flowdc_pilot_provider
        import flowdc_experiment_transport

        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            common.write_new(
                packet / "operator.json",
                dict(source=str(ROOT), state="fixture", guest_root="/fixture", worker_id="worker"),
            )
            common.write_new(packet / "deployment.json", dict(campaign_id="fixture"))
            for name in ("volume.json", "volume-mount.json", "deployment-receipt.json"):
                common.write_new(packet / name, {})
            current = {"profile_path": "fixture", "vms": {"worker": {"account": {"campaign_id": "fixture"}}}}
            journal = MagicMock()
            journal.read.return_value = current
            controller = MagicMock()
            record = {"baseline": {"worker": 0}, "campaign_id": "fixture"}
            calls = []

            def remote(_packet, _number, action, _seconds):
                calls.append(action)
                if action == "acquire":
                    raise OSError("simulated transport loss")
                return {"audit_elapsed_s": 0.1}

            def collect(*_args):
                (packet / "collected").mkdir()
                return packet / "collected"

            with (
                patch.object(ablation_cloud, "authorize", return_value=(journal, record)),
                patch.object(ablation_cloud.cloud, "imports", return_value=(None, None, None, None)),
                patch.object(ablation_cloud.cloud, "controller_for", return_value=controller),
                patch.object(ablation_cloud.cloud, "refresh_ssh"),
                patch.object(ablation_cloud.cloud, "wait_ssh"),
                patch.object(ablation_cloud.storage, "mount"),
                patch.object(ablation_cloud, "wall_left", return_value=28800),
                patch.object(ablation_cloud, "remote", side_effect=remote),
                patch.object(ablation_cloud.onset_cloud, "ready"),
                patch.object(
                    ablation_cloud.onset_cloud, "quiesce", return_value={"after": "inactive"}
                ) as quiet,
                patch.object(ablation_cloud.onset_cloud, "local_status", return_value={}),
                patch.object(ablation_cloud.onset_cloud, "exposure", return_value=1),
                patch.object(flowdc_experiment_transport, "remaining", return_value=2400),
                patch.object(flowdc_experiment_transport, "idle", return_value=True),
                patch.object(flowdc_pilot_provider, "Provider"),
                patch.object(flowdc_ops, "load_profile"),
                patch("benchmark.real_server_study.ablation_collect.collect", side_effect=collect),
                patch(
                    "benchmark.real_server_study.ablation_analysis.report", return_value=packet / "report.pdf"
                ),
            ):
                result = ablation_cloud.run(packet)
            self.assertTrue(result["cleanup_complete"])
            self.assertEqual(calls, ["acquire", "audit"])
            quiet.assert_called_once()
            self.assertEqual([c.args[0] for c in controller.call.call_args_list], ["start", "stop"])
            self.assertTrue((packet / "acquisition-ended.json").exists())

    def test_qualification_refuses_wrong_review_head_before_cloud_calls(self):
        from benchmark.real_server_study import ablation_cloud

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            review = root / "review"
            review.mkdir()
            common.write_new(root / "qualification.json", dict(head="new", review_directory=str(review)))
            common.write_new(
                review / "review.json", dict(repository="Zi-Deng/FLOW-DC", head="old", status="completed")
            )
            common.write_new(review / "report.json", {"findings": []})
            with self.assertRaises(ValueError):
                ablation_cloud.qualified(root, {"source": {"head": "new"}})
