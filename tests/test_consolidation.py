"""Offline regression checks for the GitHub/local consolidation.

Run with: python -m unittest discover -s tests -v
Downloads only use an ephemeral HTTP server on 127.0.0.1 and temporary files.
"""

import asyncio
import base64
import importlib
import json
import signal
import subprocess
import sys
import tarfile
import tempfile
import threading
import unittest
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT))
import download_batch as base
import download_batch_gradient as gradient
from benchmark.core.flowdc_adapter import FlowDCAdapter, FlowDCConfig

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII="
)


class Handler(BaseHTTPRequestHandler):
    counts = Counter()

    def do_GET(self):
        type(self).counts[self.path] += 1
        if self.path.endswith("missing.png"):
            code = 404
        elif self.path.endswith("retry.png") and self.counts[self.path] == 1:
            code = 503
        else:
            code = 200
        body = PNG if code == 200 else b"temporary or missing"
        self.send_response(code)
        self.send_header("Content-Type", "image/png")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class ConsolidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="flowdc-consolidation-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        Handler.counts.clear()

    def manifest(self, names=("ok.png",)):
        path = self.root / "input.parquet"
        pl.DataFrame({"url": [f"{self.url}/{name}" for name in names]}).write_parquet(path)
        return path

    def run_script(self, script, config, *args, stdin=""):
        path = self.root / "config.json"
        path.write_text(json.dumps(config))
        proc = subprocess.run(
            [sys.executable, str(ROOT / "bin" / script), "--config", str(path), *args],
            input=stdin, capture_output=True, text=True, timeout=30,
        )
        return proc

    def test_both_downloaders_retry_archive_and_report(self):
        for script in ("download_batch.py", "download_batch_gradient.py"):
            for compress in (False, True):
                with self.subTest(script=script, compress=compress):
                    Handler.counts.clear()
                    out = self.root / f"{script}-{compress}"
                    cfg = {
                        "input": str(self.manifest(("ok.png", "retry.png", "missing.png"))),
                        "output": str(out), "url": "url", "concurrent_downloads": 0,
                        "C_init": 2, "C_min": 1, "C_max": 4,
                        "max_retry_attempts": 2, "retry_backoff_sec": 0,
                        "create_tar": True, "compress_tar": compress,
                        "create_overview": True, "naming_mode": "url_based",
                        "file_name_pattern": "{segment_last}",
                        "output_format": "webdataset" if compress else "imagefolder",
                    }
                    proc = self.run_script(script, cfg)
                    self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                    self.assertNotIn("[PAARC] Error", proc.stdout)
                    report = json.loads(Path(f"{out}_overview.json").read_text())
                    self.assertEqual(report["summary"]["successful_downloads"], 2)
                    self.assertEqual(report["summary"]["failed_downloads"], 1)
                    self.assertEqual(Handler.counts["/retry.png"], 2)
                    self.assertEqual(Handler.counts["/missing.png"], 1)
                    payloads = list(out.rglob("*.png"))
                    self.assertEqual(len(payloads), 2)
                    self.assertTrue(all(path.read_bytes() == PNG for path in payloads))
                    archive = Path(report["summary"]["tar_path"])
                    self.assertTrue(str(archive).endswith(".tar.gz" if compress else ".tar"))
                    with tarfile.open(archive) as tf:
                        member = next(m for m in tf.getmembers() if m.name.endswith("/overview.json") or m.name == "overview.json")
                        internal = json.load(tf.extractfile(member))
                        self.assertEqual(internal["summary"]["successful_downloads"], 2)
                        if "gradient" in script:
                            self.assertEqual(internal["controller_variant"], "gradient")
                            self.assertIn("gradient_summary", internal)
                            self.assertEqual(internal["gradient_summary"], report["gradient_summary"])

    def test_existing_output_requires_consent_and_config_force_works(self):
        for script in ("download_batch.py", "download_batch_gradient.py"):
            with self.subTest(script=script):
                out = self.root / script
                out.mkdir()
                marker = out / "keep.txt"
                marker.write_text("existing data")
                cfg = {"input": str(self.manifest()), "output": str(out),
                       "create_tar": False, "enable_paarc": False,
                       "concurrent_downloads": 2}
                refused = self.run_script(script, cfg, stdin="n\n")
                self.assertNotEqual(refused.returncode, 0)
                self.assertEqual(marker.read_text(), "existing data")
                forced = self.run_script(script, cfg, "--force")
                self.assertEqual(forced.returncode, 0, forced.stdout + forced.stderr)
                self.assertFalse(marker.exists())
                report = json.loads(Path(f"{out}_overview.json").read_text())
                self.assertEqual(report["summary"]["successful_downloads"], 1)

    def test_benchmark_adapter_with_precreated_output(self):
        cfg = FlowDCConfig(
            input_path=str(self.manifest()), output_folder=str(self.root / "benchmark"),
            url_column="url", label_column=None, concurrent_downloads=2,
            timeout_sec=5, enable_paarc=False,
        )
        result = asyncio.run(asyncio.wait_for(
            FlowDCAdapter(ROOT).run(cfg, run_number=0, monitor_resources=False), timeout=30,
        ))
        self.assertEqual(result.successful_downloads, 1, result.extra_metrics)
        self.assertNotIn("returncode", result.extra_metrics)

    def test_sample_aware_scheduler_is_used(self):
        async def exercise():
            ctrl = base.PAARCController("example.org", base.PAARCConfig())
            ctrl.state = base.PAARCState.PROBE_BW
            ctrl._get_rtprop = lambda: 0.1
            self.assertEqual(ctrl._calculate_control_interval({"goodput_rps": 1}), ctrl.config.N_min)
            snap = {"goodput_rps": 1}
            ctrl.step_interval = AsyncMock(return_value=snap)
            original = ctrl._calculate_control_interval
            ctrl._calculate_control_interval = MagicMock(side_effect=original)
            manager = MagicMock()
            manager.all_controllers = AsyncMock(return_value=[ctrl])
            async def stop(_delay):
                raise asyncio.CancelledError
            with patch.object(base.asyncio, "sleep", stop):
                with self.assertRaises(asyncio.CancelledError):
                    await base.controller_loop(manager)
            ctrl._calculate_control_interval.assert_called_once_with(snap)
        asyncio.run(exercise())

    def test_idle_controller_returns_snapshot_for_both_variants(self):
        async def exercise():
            for module in (base, gradient):
                ctrl = module.PAARCController("example.org", module.PAARCConfig())
                snap = await ctrl.step_interval()
                self.assertIsInstance(snap, dict)
                self.assertEqual(snap["total"], 0)
        asyncio.run(exercise())

    def test_gradient_config_preserves_defaults_and_accepts_upstream_options(self):
        for concurrency in (None, 0, 8):
            with self.subTest(concurrency=concurrency):
                path = self.root / "gradient.json"
                path.write_text(json.dumps({"input": "in", "output": "out",
                    "concurrent_downloads": concurrency, "force_overwrite": True,
                    "rtprop_window": 35.0}))
                with patch.object(sys, "argv", ["gradient", "--config", str(path)]):
                    cfg = gradient.parse_args()
                self.assertEqual(cfg.concurrent_downloads, concurrency)
                self.assertTrue(cfg.force_overwrite)
                self.assertEqual(cfg.to_paarc_config().rtprop_window, 35.0)
        with patch.object(sys, "argv", ["gradient", "--input", "in", "--output", "out"]):
            cfg = gradient.parse_args()
        self.assertEqual((cfg.concurrent_downloads, cfg.mu, cfg.rtprop_window), (256, 0.75, 15.0))

    def test_import_does_not_register_signal_handlers(self):
        code = (
            "import signal, sys; sys.path.insert(0, 'bin'); "
            "before = signal.getsignal(signal.SIGINT); "
            "import download_batch_gradient; "
            "assert signal.getsignal(signal.SIGINT) == before"
        )
        proc = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_cloud_config_uses_paarc_fields(self):
        # This checks pure config translation only, not a live TaskVine cluster.
        with patch.dict(sys.modules, {"ndcctools": MagicMock(), "ndcctools.taskvine": MagicMock()}):
            cloud = importlib.import_module("TaskvineFLOWDCCloud")
            config = cloud.create_partition_config({
                "enable_paarc": False, "C_init": 3, "C_max": 40,
                "timeout": 7, "mu": 0.9, "compress_tar": False,
            }, "partition.parquet", "output")
            self.assertFalse(config["enable_paarc"])
            self.assertEqual((config["C_init"], config["C_max"], config["timeout"]), (3, 40, 7))
            self.assertTrue(config["force_overwrite"])
            self.assertTrue(config["create_tar"])
            self.assertTrue(config["compress_tar"])
            old = cloud.create_partition_config({"enable_polite_controller": False,
                "per_host_conc_init": 5, "per_host_conc_cap": 50}, "in", "out")
            self.assertFalse(old["enable_paarc"])
            self.assertEqual((old["C_init"], old["C_max"]), (5, 50))


if __name__ == "__main__":
    unittest.main()
