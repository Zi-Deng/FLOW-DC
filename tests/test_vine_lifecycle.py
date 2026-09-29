"""Process ownership and graceful native shutdown invariants, without sockets."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
from flowdc_vine_native import NativeManager

from benchmark.taskvine_local import OwnedWorkers


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_receipt_allows_graceful_exit_before_escalation(self):
        manager = NativeManager.__new__(NativeManager)
        manager.process = Mock()
        type(manager.process).exitcode = property(lambda _: next(statuses))
        statuses = iter([None, None, None, 0, 0, 0, 0])
        manager.fd = os.open("/dev/null", os.O_RDONLY)
        manager.connection, manager.pending = Mock(), False
        manager.call = AsyncMock(return_value={"closed": True})
        with patch("flowdc_vine_native.stop_fd") as kill:
            result = await manager.close()
        self.assertEqual(result["native_manager_exit"], 0)
        self.assertTrue(result["shutdown_receipt"])
        kill.assert_not_called()

    async def test_worker_cleanup_preserves_unrelated_helper(self):
        with tempfile.TemporaryDirectory() as directory:
            helper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
            owned = OwnedWorkers(Path(directory), Path(sys.executable), 1)
            try:
                owned.excluded.add((helper.pid, psutil.Process(helper.pid).create_time()))
                worker = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
                worker.flowdc_identity = (worker.pid, psutil.Process(worker.pid).create_time())
                owned.roots.append(worker)
                owned.capture()
                self.assertIn(worker.pid, owned.handles)
                self.assertNotIn(helper.pid, owned.handles)
                proof = await owned.close()
                self.assertTrue(proof["all_stopped"])
                self.assertIsNone(helper.poll())
            finally:
                helper.terminate()
                helper.wait(timeout=5)

    async def test_unknown_adopted_child_prevents_quiescence_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            owned = OwnedWorkers(Path(directory), Path(sys.executable), 1)

            def capture():
                owned.unknown = {(123, 1.0)}

            with patch.object(owned, "capture", capture):
                with self.assertRaisesRegex(ValueError, "unattributed"):
                    await owned.stop_tree(0)

    async def test_descendants_discovered_during_shutdown_are_included(self):
        with tempfile.TemporaryDirectory() as directory:
            owned = OwnedWorkers(Path(directory), Path(sys.executable), 1)
            records = [{"fd": 1, "created": 1.0, "worker": 0}, {"fd": 2, "created": 2.0, "worker": 0}]
            captures, stopped = [], set()

            def capture():
                captures.append(True)
                owned.handles[1] = records[0]
                if len(captures) >= 2:
                    owned.handles[2] = records[1]

            with (
                patch.object(owned, "capture", capture),
                patch.object(owned, "dead", lambda r: r["fd"] in stopped),
                patch("benchmark.taskvine_local.signal_pidfd", lambda fd, sig: stopped.add(fd)),
            ):
                proof = await owned.stop_tree(0)
            self.assertEqual(proof["pids"], [1, 2])
            self.assertEqual(stopped, {1, 2})
