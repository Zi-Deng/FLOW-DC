"""Real, owned local TaskVine processes; never activates cloud resources."""

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from flowdc.config import DownloadConfig
from flowdc.distributed import run_distributed, verify_partition
from tests.test_experiments import ImageHandler

NATIVE_PYTHON = os.environ.get("FLOWDC_NATIVE_PYTHON")
NATIVE_WORKER = os.environ.get("FLOWDC_NATIVE_WORKER")


@unittest.skipUnless(
    NATIVE_PYTHON and NATIVE_WORKER and os.environ.get("FLOWDC_NATIVE_RECEIPT"),
    "set FLOWDC_NATIVE_PYTHON/WORKER/RECEIPT to the qualified TaskVine runtime",
)
class NativeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), ImageHandler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        import polars as pl

        pl.DataFrame(
            {"url": [f"http://127.0.0.1:{self.server.server_port}/{i}.jpg" for i in range(4)]}
        ).write_parquet(self.root / "input.parquet")

    def settings(self, workers):
        return {
            "manager_python": NATIVE_PYTHON,
            "runtime_receipt": os.environ["FLOWDC_NATIVE_RECEIPT"],
            "worker_binary": NATIVE_WORKER,
            "python_executable": sys.executable,
            "workers": workers,
            "start_local_workers": True,
            "timeout_s": 20,
        }

    def test_one_and_two_worker_verified_return_and_archive_corruption(self):
        for workers, method in [(1, "paarc-grad"), (2, "paarc-ratio")]:
            source = self.root / "input.parquet"
            if workers == 1:
                import polars as pl

                source = self.root / "input.explicit-format"
                pl.read_parquet(self.root / "input.parquet").write_csv(source)
            result = run_distributed(
                DownloadConfig(
                    input_path=str(source),
                    input_format="csv" if workers == 1 else None,
                    output_dir=str(self.root / f"run-{workers}"),
                    method=method,
                    c_max=8,
                ),
                self.settings(workers),
            )
            self.assertTrue(result["run_complete"], result)
            self.assertTrue(result["cleanup"]["all_stopped"])
            self.assertEqual(sum(r["counts"]["verified"] for r in result["partitions"]), 4)
            self.assertEqual(len(result["cleanup"]["launches"]), workers)
            root = self.root / f"run-{workers}"
            part = root / "part-0"
            archive = part / "native.tar"
            data = bytearray(archive.read_bytes())
            data[1024] ^= 1
            archive.write_bytes(data)
            with self.assertRaises((ValueError, OSError)):
                verify_partition(part, root / "inputs/part-0000.parquet")

    def test_worker_loss_returns_no_fabricated_credit_and_cleans_descendants(self):
        class SlowHandler(ImageHandler):
            began = threading.Event()

            def do_GET(self):
                type(self).began.set()
                time.sleep(6)
                try:
                    super().do_GET()
                except OSError:
                    pass

        slow = ThreadingHTTPServer(("127.0.0.1", 0), SlowHandler)
        threading.Thread(target=slow.serve_forever, daemon=True).start()
        self.addCleanup(slow.server_close)
        self.addCleanup(slow.shutdown)
        import polars as pl

        source = self.root / "slow.parquet"
        pl.DataFrame({"url": [f"http://127.0.0.1:{slow.server_port}/slow.jpg"]}).write_parquet(source)
        settings = self.settings(1)
        settings["timeout_s"] = 4
        settings_path = self.root / "settings.json"
        settings_path.write_text(json.dumps(settings))
        destination = self.root / "failed"
        command = [
            sys.executable,
            "-m",
            "flowdc",
            "download",
            "--input-path",
            str(source),
            "--output-dir",
            str(destination),
            "--backend",
            "taskvine",
            "--taskvine-config",
            str(settings_path),
        ]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(SlowHandler.began.wait(10))
            owner = json.loads((destination / "worker-0.json").read_text())
            import psutil

            self.assertEqual(psutil.Process(owner["pid"]).create_time(), owner["create_time"])
            from flowdc.distributed.native import pidfd
            from flowdc.distributed.ownership import signal_pidfd

            fd = pidfd(owner["pid"])
            try:
                signal_pidfd(fd, signal.SIGKILL)
            finally:
                os.close(fd)
            stdout, stderr = process.communicate(timeout=25)
            self.assertEqual(process.returncode, 1, stderr + (destination / "manager.log").read_text())
            result = json.loads((destination / "result.json").read_text())
            self.assertFalse(result["run_complete"])
            self.assertEqual(result["credited_bytes"], 0)
            self.assertTrue(result["cleanup"]["all_stopped"])
            self.assertEqual(len(result["cleanup"]["launches"]), 1)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    def test_aggregate_request_ceiling_covers_multiple_origins(self):
        class CountedHandler(ImageHandler):
            lock = threading.Lock()
            active = peak = 0

            def do_GET(self):
                with type(self).lock:
                    type(self).active += 1
                    type(self).peak = max(type(self).peak, type(self).active)
                try:
                    time.sleep(0.1)
                    super().do_GET()
                finally:
                    with type(self).lock:
                        type(self).active -= 1

        server = ThreadingHTTPServer(("127.0.0.1", 0), CountedHandler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        import polars as pl

        source = self.root / "multiple-origins.parquet"
        pl.DataFrame(
            {
                "url": [
                    f"http://{('127.0.0.1' if i % 4 < 2 else 'localhost')}:{server.server_port}/{i}.jpg"
                    for i in range(16)
                ]
            }
        ).write_parquet(source)
        result = run_distributed(
            DownloadConfig(input_path=str(source), output_dir=str(self.root / "budget"), c_max=4),
            self.settings(2),
        )
        self.assertTrue(result["run_complete"], result)
        self.assertGreater(CountedHandler.peak, 1)
        self.assertLessEqual(CountedHandler.peak, 4)

    @unittest.skipUnless(os.environ.get("FLOWDC_COMPARATOR_PYTHON"), "isolated comparator runtime required")
    def test_two_worker_comparator_has_matched_verified_return(self):
        comparator = os.environ["FLOWDC_COMPARATOR_PYTHON"]
        settings = self.settings(2)
        settings.update(python_executable=comparator, timeout_s=45)
        context = {"source": "loopback", "body_limit_bytes": 2**20, "baseline_s": 0.1}
        result = run_distributed(
            DownloadConfig(
                input_path=str(self.root / "input.parquet"),
                output_dir=str(self.root / "comparator"),
                c_max=4,
                max_attempts=1,
            ),
            settings,
            tool={**context, "processes": 1, "threads": 4},
            experiment=context,
        )
        self.assertTrue(result["execution_complete"], result)
        self.assertEqual(sum(p["counts"]["verified"] for p in result["partitions"]), 4)
        self.assertTrue(result["cleanup"]["all_stopped"])
