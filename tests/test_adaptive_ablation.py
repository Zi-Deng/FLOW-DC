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
                verification={
                    "credited_rows": 100,
                    "dispatched_rows": 100,
                    "dispatched_terminal_rows": 100,
                    "original_rows": 32768,
                },
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
            with self.assertRaises(ValueError):
                extract(run / receipt["path"], root / "expired", 1024**2, deadline=0)
            self.assertTrue(common.read_json(dest / "native/overview.json")["fixture"])
            self.assertFalse((dest / "native/payload.jpg").exists())
            self.assertEqual(seal(run)["sha256"], receipt["sha256"])
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
        self._cloud_scenario("lost_ack")

    def test_late_window_is_reserved_for_finalization(self):
        self._cloud_scenario("late")

    def test_cooldown_that_cannot_fit_finalizes_in_current_window(self):
        self._cloud_scenario("cooldown")

    def test_declining_clock_collects_before_next_window_becomes_inadmissible(self):
        self._cloud_scenario("edge")

    def _cloud_scenario(self, scenario):
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
            clock = {"left": {"lost_ack": 28800, "late": 8500, "cooldown": 9500, "edge": 9250}[scenario]}

            def remote(_packet, _number, action, _seconds):
                calls.append(action)
                if action == "acquire":
                    if scenario == "lost_ack":
                        raise OSError("simulated transport loss")
                    if scenario == "cooldown":
                        return dict(status="cooldown", until_utc=__import__("time").time() + 600)
                    if scenario == "edge":
                        clock["left"] -= 4000
                        return dict(status="complete")
                    raise AssertionError("acquisition must not start in finalization reserve")
                if scenario == "edge":
                    clock["left"] -= 1000
                    return dict(audit_elapsed_s=1000, audit_pending=1)
                return {"audit_elapsed_s": 0.1, "audit_pending": int(scenario == "lost_ack" and _number == 0)}

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
                patch.object(
                    ablation_cloud,
                    "wall_left",
                    side_effect=lambda _record: clock["left"],
                ),
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
            self.assertEqual(
                calls,
                {
                    "lost_ack": ["acquire", "audit", "audit"],
                    "late": ["audit"],
                    "cooldown": ["acquire", "audit"],
                    "edge": ["acquire", "audit"],
                }[scenario],
            )
            self.assertEqual(quiet.call_count, int(scenario == "lost_ack"))
            self.assertEqual(
                [c.args[0] for c in controller.call.call_args_list],
                ["start", "stop"] * (2 if scenario == "lost_ack" else 1),
            )
            self.assertEqual((packet / "recovery-skipped.json").exists(), scenario == "cooldown")
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


class NativeLaunchTests(unittest.IsolatedAsyncioTestCase):
    async def test_launch_timer_controls_the_same_native_module(self):
        payload = io.BytesIO()
        Image.new("RGB", (17, 13), (17, 90, 4)).save(payload, "JPEG")
        raw = payload.getvalue()

        async def handle(request):
            await asyncio.sleep(0.03)
            return web.Response(body=raw, content_type="image/jpeg")

        app = web.Application()
        app.router.add_get("/{name}", handle)
        server = web.AppRunner(app)
        await server.setup()
        site = web.TCPSite(server, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            with tempfile.TemporaryDirectory() as tmp:
                study = Path(tmp)
                directory = study / "runs/fixture"
                directory.mkdir(parents=True)
                manifest = study / "input.parquet"
                pl.DataFrame({"url": [f"http://127.0.0.1:{port}/{n}.jpg" for n in range(256)]}).write_parquet(
                    manifest
                )
                common.write_new(study / "protocol.json", {})
                common.write_new(study / "execution.json", {})
                common.write_new(study / "host-gate.json", {"ignore_time": False})
                common.write_new(study / "probe-summary.json", {"ttfb_p50_s": 0.05})
                cfg = dict(
                    input=str(manifest),
                    output=str(directory / "native"),
                    url="url",
                    control_method="ratio-v2",
                    concurrent_downloads=16,
                    http_connection_limit=8,
                    C_min=2,
                    C_init=4,
                    C_max=16,
                    research_profile=True,
                    research_workload=ablation.PROFILE,
                    max_retry_attempts=1,
                    compress_tar=False,
                )
                common.write_new(directory / "flowdc-config.json", cfg)
                common.write_new(directory / "execution.json", {})
                common.write_new(
                    directory / "attempt.json",
                    dict(
                        protocol_sha256=common.file_hash(study / "protocol.json"),
                        execution_sha256=common.file_hash(study / "execution.json"),
                        slice_sha256=common.file_hash(manifest),
                        body_reservation=2**20,
                    ),
                )
                script = """import sys
from pathlib import Path
from benchmark.real_server_study import ablation,ablation_run,telemetry
from benchmark.real_server_study.common import read_json
root=Path(sys.argv[1]);ablation.check=lambda *a:None;ablation_run.guest_check=lambda *a:None
ablation.schedule=lambda:[dict(run_id='fixture',phase='screen',network_seconds=.25)]
ablation.config=lambda *a:read_json(root/'runs/fixture/flowdc-config.json')
telemetry.checked_url=lambda u:u
ablation_run.launch(root,Path(sys.argv[2]),'fixture')
"""
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-c",
                    script,
                    str(study),
                    str(ROOT),
                    cwd=ROOT,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, err = await asyncio.wait_for(process.communicate(), 15)
                self.assertEqual(process.returncode, 0, err.decode())
                events, invalid = ablation_run.audit_events(directory / "request-audit.jsonl")
                self.assertFalse(invalid)
                self.assertEqual(events[-1]["stop_reason"], "planned_window_end")
                index = common.read_json(directory / "native/outcome-index.json")
                self.assertGreater(index["counts"]["unattempted"], 128)
                self.assertTrue((directory / "native.tar").exists())
        finally:
            await server.cleanup()


class StopRecoveryTests(unittest.TestCase):
    def test_refusals_during_planned_drain_are_terminal(self):
        for planned in ablation.PLANNED:
            for status in (401, 403, 429, 503):
                policy = ablation.Policy(0.1, 100)
                policy.reason = planned
                policy.header(1, status)
                self.assertTrue(policy.fatal)
                self.assertNotIn(policy.reason, ablation.PLANNED)
                self.assertEqual(ablation.terminal_fault(dict(event="headers", status=status)), policy.reason)

    def test_planned_body_budget_during_drain_does_not_become_fatal(self):
        policy = ablation.Policy(0.1, 100)
        policy.reason = "planned_window_end"
        policy.fatal = True
        policy.fatal_reason = "body_budget"
        guard = ablation_run.WindowGuard([], io.StringIO(), 0.1, policy=policy, seconds=1)
        try:
            with self.assertRaises(RuntimeError):
                guard.check()
            self.assertFalse(policy.fatal)
            self.assertIsNone(policy.fatal_reason)
            policy.header(2, 429)
            with self.assertRaises(RuntimeError):
                guard.check()
            self.assertTrue(policy.fatal)
            self.assertEqual(policy.fatal_reason, "explicit_429")
        finally:
            downloader.shutdown_flag = False

    def test_recovery_requires_all_planned_requests_and_no_stop(self):
        value = dict(passed=True, reason=None, requests=128, valid=125, ttfb_p50_s=0.1)
        self.assertTrue(ablation.recovered(value, 0.1))
        for change in (
            {"passed": False},
            {"reason": "health_refusal"},
            {"requests": 41, "valid": 40},
            {"valid": 120},
            {"ttfb_p50_s": 0.151},
        ):
            self.assertFalse(ablation.recovered({**value, **change}, 0.1))

    def test_interrupted_archive_caches_recover_without_replaying_source(self):
        from benchmark.real_server_study.ablation_analysis import seal
        from benchmark.real_server_study.ablation_collect import pack, extract

        with tempfile.TemporaryDirectory() as tmp:
            study = Path(tmp)
            run = study / "runs/run"
            run.mkdir(parents=True)
            common.write_new(run / "intent.json", {"attempt": 1})
            (run / "metadata.tar.gz.partial").write_bytes(b"truncated")
            receipt = seal(run)
            self.assertFalse((run / "metadata.tar.gz.partial").exists())
            # Receipt was committed but archive rename was interrupted.
            (run / "metadata.tar.gz").rename(run / "metadata.tar.gz.partial")
            self.assertEqual(seal(run), receipt)
            (study / "collection.tar.gz.partial").write_bytes(b"truncated")
            closed = pack(study)
            self.assertEqual(pack(study), closed)
            dest = study / "out"
            dest.mkdir()
            extract(study / "collection.tar.gz", dest, 2**20)
            self.assertTrue((dest / "runs/run/metadata-bundle.json").exists())
            with (run / "metadata.tar.gz").open("ab") as out:
                out.write(b"corruption")
            with self.assertRaises(ValueError):
                seal(run)


class ControlledFailureTests(unittest.TestCase):
    def test_timeout_retains_failed_record_and_refuses_replay(self):
        import subprocess
        from unittest.mock import MagicMock
        from benchmark.real_server_study import ablation_controlled

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cell = ablation_controlled.cases()[0]
            process = MagicMock()
            process.wait.side_effect = subprocess.TimeoutExpired("fixture", 120)
            with (
                patch.object(ablation_controlled, "cases", return_value=[cell]),
                patch.object(common, "remaining", return_value=1000),
                patch.object(ablation_controlled.subprocess, "Popen", return_value=process) as spawn,
                patch.object(ablation_controlled, "terminate"),
            ):
                with self.assertRaises(ValueError):
                    ablation_controlled.window(root, ROOT)
                record = common.read_json(root / "controlled" / cell["run_id"] / "record.json")
                self.assertEqual(record["status"], "timeout")
                self.assertFalse(record["artifacts_valid"])
                with self.assertRaises(ValueError):
                    ablation_controlled.window(root, ROOT)
                spawn.assert_called_once()
                process.wait.assert_called_once_with(timeout=120)


class FinalRepairTests(unittest.TestCase):
    def test_extraction_timeout_retries_from_local_verified_archive(self):
        import shutil
        from types import SimpleNamespace
        from benchmark.real_server_study import ablation_collect

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            study = root / "study"
            study.mkdir()
            packet = root / "packet"
            packet.mkdir()
            common.write_new(study / "protocol.json", {"fixture": True})
            receipt = ablation_collect.pack(study)
            shutil.copyfile(study / "collection.tar.gz", packet / "collection.tar.gz")
            common.write_new(packet / "collection.json", receipt)
            common.write_new(packet / "operator.json", {"guest_root": "/unused"})
            common.write_new(packet / "deployment-receipt.json", {"python": sys.executable})

            def interrupted(_archive, destination, *_args, **_kwargs):
                (destination / "incomplete").write_bytes(b"prefix")
                raise TimeoutError("fixture deadline")

            with (
                patch.object(
                    ablation_collect.shutil, "disk_usage", return_value=SimpleNamespace(free=100 * 2**30)
                ),
                patch.object(ablation_collect.cloud, "ssh") as ssh,
            ):
                with patch.object(ablation_collect, "extract", side_effect=interrupted):
                    with self.assertRaises(TimeoutError):
                        ablation_collect.collect(packet, 10)
                self.assertFalse((packet / "collected.partial").exists())
                # Also recover temporary extraction left by an uncatchable kill.
                (packet / "collected.partial").mkdir()
                (packet / "collected.partial/stale").write_text("derived")
                target = ablation_collect.collect(packet, 10)
                self.assertTrue(common.read_json(target / "protocol.json")["fixture"])
                ssh.assert_not_called()
                self.assertLess(ablation_collect.collection_spent(packet), 20)

    def test_status_only_inventory_preserves_alias_missing_and_interrupted(self):
        from benchmark.real_server_study import ablation_analysis

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            study = root / "study"
            study.mkdir()
            common.write_new(study / "protocol.json", {"fixture": True})
            runs = ablation.schedule()
            alias = next(r for r in runs if r["phase"] == "validation" and r["arm"].startswith("high-"))
            target = next(
                r
                for r in runs
                if r["phase"] == "validation"
                and r["block"] == alias["block"]
                and r["arm"] == alias["arm"].replace("high-", "balanced-", 1)
            )
            for run, status in ((alias, "alias"), (runs[0], "no_candidate")):
                p = study / "runs" / run["run_id"]
                p.mkdir(parents=True)
                common.write_new(p / "record.json", dict(run, status=status, alias_of=target["run_id"]))
            (study / "runs" / runs[1]["run_id"]).mkdir()
            common.write_new(root / "recovery-skipped.json", {"reason": "finite reserve fixture"})
            with patch.object(ablation_analysis.subprocess, "run"):
                ablation_analysis.report(study, root / "report")
            values = {r["run_id"]: r for r in common.read_json(root / "report/results.json")["runs"]}
            self.assertEqual(values[alias["run_id"]]["status"], "alias")
            self.assertEqual(values[runs[0]["run_id"]]["status"], "no_candidate")
            self.assertEqual(values[runs[1]["run_id"]]["status"], "interrupted")
            self.assertIn("finite reserve fixture", (root / "report/report.tex").read_text())

    def test_bounded_final_assessment_binds_delta_and_all_material_findings(self):
        from benchmark.real_server_study import ablation_cloud
        import json

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            review = root / "review"
            review.mkdir()
            old = "1" * 40
            new = "2" * 40
            delta = b"fixed operational delta"
            common.write_new(root / "operator.json", {"source": str(ROOT)})
            common.write_new(review / "report.json", {"findings": [{"severity": "P1"}, {"severity": "P2"}]})
            report_sha = common.file_hash(review / "report.json")
            common.write_new(
                review / "review.json",
                dict(
                    repository="Zi-Deng/FLOW-DC",
                    head=old,
                    status="completed",
                    invocation=2,
                    report_sha256=report_sha,
                ),
            )
            assessment = dict(
                reviewed_head=old,
                final_head=new,
                report_sha256=report_sha,
                delta_sha256=common.sha256(delta),
                review_invocations_used=2,
                repair_rounds_used=2,
                independent_final_head_review=False,
                material_dispositions={"0": {"status": "fixed"}, "1": {"status": "fixed"}},
            )
            common.write_new(root / "final-repair-assessment.json", assessment)
            proof = dict(
                head=new,
                reviewed_head=old,
                review_directory=str(review),
                review_metadata_sha256=common.file_hash(review / "review.json"),
                repair_assessment_sha256=common.file_hash(root / "final-repair-assessment.json"),
                ci_runs=[1, 2],
            )
            common.write_new(root / "qualification.json", proof)

            def output(args, **kwargs):
                if args[0] == "git":
                    return (
                        "benchmark/real_server_study/ablation_cloud.py\n" if "--name-only" in args else delta
                    )
                return json.dumps(
                    dict(
                        headSha=new,
                        conclusion="success",
                        event="workflow_dispatch",
                        name="FLOW-DC regression tests" if args[3] == "1" else "agentic workflow tests",
                    )
                )

            with patch.object(ablation_cloud.subprocess, "check_output", side_effect=output):
                ablation_cloud.qualified(root, {"source": {"head": new}})
                assessment["material_dispositions"].pop("1")
                common.replace_derived(root / "final-repair-assessment.json", assessment)
                proof["repair_assessment_sha256"] = common.file_hash(root / "final-repair-assessment.json")
                common.replace_derived(root / "qualification.json", proof)
                with self.assertRaises(ValueError):
                    ablation_cloud.qualified(root, {"source": {"head": new}})
