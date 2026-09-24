"""Real guest serialization/deployment and maintained-downloader synthetic integration.

No cloud, SSH, systemd or TaskVine runtime is used by these local tests.
"""

import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import threading
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
import flowdc_experiment_guest as guest
from flowdc_experiment_artifacts import bundle, members
from flowdc_experiment_data import LIMIT, digest, encode
from flowdc_experiment_fixture import generate
from flowdc_experiment_guest import origin_server

REPO = Path(__file__).resolve().parents[1]
GUEST = REPO / "bin/flowdc_experiment_guest.py"


class GuestProtocolTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="experiment-guest-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / ("exp-" + "1" * 32)

    def deploy(self, raw):
        return subprocess.run(
            [sys.executable, str(GUEST), "deploy", str(self.root), "manager", "all", digest(raw), str(LIMIT)],
            input=raw,
            capture_output=True,
            timeout=5,
        )

    def test_deploy_preserves_existing_paths_and_collects_actual_bytes(self):
        raw = bundle({"guest.json": b"{}", "results/case/manager.log": b"evidence"})
        result = self.deploy(raw)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual((self.root / "results/case/manager.log").read_bytes(), b"evidence")
        self.assertNotEqual(self.deploy(raw).returncode, 0)
        result = subprocess.run(
            [sys.executable, str(GUEST), "collect", str(self.root), "manager", "case", str(LIMIT)],
            capture_output=True,
            timeout=5,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(members(result.stdout, LIMIT), {"manager.log": b"evidence"})
        (self.root / "results/case/link").symlink_to("manager.log")
        result = subprocess.run(
            [sys.executable, str(GUEST), "collect", str(self.root), "manager", "case", str(LIMIT)],
            capture_output=True,
            timeout=5,
        )
        self.assertNotEqual(result.returncode, 0)

    def test_guest_rejects_traversal_without_writing_outside_owned_root(self):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w") as archive:
            entry = tarfile.TarInfo("../escape")
            entry.size = 3
            archive.addfile(entry, io.BytesIO(b"bad"))
        self.assertNotEqual(self.deploy(raw.getvalue()).returncode, 0)
        self.assertFalse((self.root.parent / "escape").exists())

    def test_system_services_run_as_guest_user_with_finite_limits(self):
        self.root.mkdir(mode=0o700)
        (self.root / "guest.json").write_bytes(
            encode(
                {
                    "service_mode": "system",
                    "python": "/venv/bin/python",
                    "worker": "/venv/bin/vine_worker",
                    "addresses": {"manager": "10.0.0.10"},
                    "bounds": {"memory_mb": 1024, "output_bytes": 1048576, "cores": 1, "disk_mb": 512},
                }
            )
        )
        with patch.object(guest, "command", return_value=b"") as command:
            guest.launch(self.root, "worker", "case", 30)
        argv = command.call_args.args[0]
        self.assertEqual(argv[:3], ["sudo", "-n", "/usr/bin/systemd-run"])
        self.assertEqual(argv[argv.index("--uid") + 1], str(os.getuid()))
        self.assertIn("--property=RuntimeMaxSec=30", argv)
        self.assertIn("--property=TimeoutStopSec=10", argv)
        self.assertIn("--property=KillMode=control-group", argv)
        self.assertIn("--workspace", argv)
        self.assertIn("--single-shot", argv)
        self.assertIn("--wall-time", argv)
        self.assertNotIn("--user", argv)
        self.assertEqual((self.root / "results/case/worker.log").stat().st_uid, os.getuid())
        self.assertEqual((self.root / "results/case/worker.log").stat().st_mode & 0o777, 0o600)

    def test_guest_stop_blocks_late_launch_and_uncertain_acknowledgement(self):
        self.root.mkdir(mode=0o700)
        state = {"LoadState": "not-found"}
        with patch.object(guest, "service_status", return_value=state):
            self.assertTrue(guest.stop(self.root, "worker", "late")["stopped"])
            with self.assertRaises(ValueError):
                guest.launch(self.root, "worker", "late", 30)
            pending = guest.unit(self.root.name, "worker", "pending")
            (self.root / (pending + ".intent")).write_text("pending")
            with self.assertRaises(ValueError):
                guest.stop(self.root, "worker", "pending")

    def test_user_service_mode_requires_linger_and_system_mode_requires_sudo(self):
        self.root.mkdir(mode=0o700)
        with patch.object(guest, "command", return_value=b"no\n") as command, self.assertRaises(ValueError):
            guest.probe(self.root, "origin", "/unused", 0, "user")
        self.assertIn("/usr/bin/loginctl", command.call_args.args[0])
        with (
            patch.object(guest, "command", side_effect=ValueError("missing capability")) as command,
            self.assertRaises(ValueError),
        ):
            guest.probe(self.root, "origin", "/unused", 0, "system")
        self.assertEqual(command.call_args.args[0], ["sudo", "-n", "-l", "/usr/bin/systemd-run"])


class DownloaderFixtureTests(unittest.TestCase):
    def test_actual_downloader_both_paarc_modes_hashes_and_injected_retries(self):
        import polars as pl

        with tempfile.TemporaryDirectory(prefix="experiment-http-") as directory:
            root = Path(directory)
            config = {"cases": ["paarc-on", "paarc-off"], "addresses": {"origin": "127.0.0.1"}}
            (root / "guest.json").write_bytes(encode(config))
            # The maintained guest origin implementation chooses an ephemeral local
            # port for this test only; production CLI has no port override.
            server = origin_server(root, port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                files, expected = generate("127.0.0.1", config["cases"])
                for path, raw in files.items():
                    destination = root / path
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(raw)
                for name, mode in (("paarc-on", True), ("paarc-off", False)):
                    for part in expected[name]:
                        path = root / "inputs" / name / part["name"]
                        frame = pl.read_parquet(path)
                        frame.with_columns(
                            pl.col("url").str.replace(":8000/", f":{server.server_port}/")
                        ).write_parquet(path)
                        output = root / f"{name}-{part['name']}"
                        config_path = root / "download.json"
                        config_path.write_bytes(
                            encode(
                                {
                                    "input": str(path),
                                    "input_format": "parquet",
                                    "output": str(output),
                                    "url": "url",
                                    "enable_paarc": mode,
                                    "C_init": 2,
                                    "C_min": 1,
                                    "C_max": 4,
                                    "concurrent_downloads": 4,
                                    "max_retry_attempts": 3,
                                    "retry_backoff_sec": 0.01,
                                    "timeout": 5,
                                    "create_tar": True,
                                    "compress_tar": True,
                                    "create_overview": True,
                                    "force_overwrite": True,
                                }
                            )
                        )
                        result = subprocess.run(
                            [
                                sys.executable,
                                str(REPO / "bin/download_batch.py"),
                                "--config",
                                str(config_path),
                            ],
                            capture_output=True,
                            timeout=40,
                        )
                        self.assertEqual(result.returncode, 0, result.stderr.decode()[-1000:])
                        archive = members(Path(str(output) + ".tar.gz").read_bytes(), LIMIT)
                        overview = json.loads(
                            next(value for key, value in archive.items() if key.endswith("/overview.json"))
                        )
                        self.assertEqual(overview["script_inputs"]["enable_paarc"], mode)
                        self.assertEqual(overview["summary"]["successful_downloads"], 32)
                        self.assertEqual(overview["summary"]["failed_downloads"], 0)
                        observed = [
                            digest(value)
                            for key, value in archive.items()
                            if value is not None and not key.endswith("/overview.json")
                        ]
                        self.assertEqual(Counter(observed), Counter(part["expected_sha256"]))
                rows = [json.loads(line) for line in (root / "origin.jsonl").read_text().splitlines()]
                self.assertEqual(len(rows), 130)
                self.assertEqual(sum(row["status"] == 503 for row in rows), 2)
                for name in config["cases"]:
                    self.assertEqual(
                        [row["status"] for row in rows if row["path"] == f"/{name}/0.png"], [503, 200]
                    )
            finally:
                server.shutdown()
                server.server_close()
                server.fixture_log.close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
