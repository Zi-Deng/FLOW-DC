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
    async def test_missing_signal_capability_prevents_native_or_worker_launch(self):
        with (
            patch("flowdc_vine_native.signal_fd", side_effect=PermissionError("not available")),
            patch("flowdc_vine_native.multiprocessing.get_context") as spawn,
            self.assertRaises(PermissionError),
        ):
            NativeManager({})
        spawn.assert_not_called()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("flowdc_vine_ownership.signal_pidfd", side_effect=PermissionError("not available")),
            self.assertRaises(PermissionError),
        ):
            OwnedWorkers(Path(directory), Path(sys.executable), 1)

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
                patch("flowdc_vine_ownership.signal_pidfd", lambda fd, sig: stopped.add(fd)),
            ):
                proof = await owned.stop_tree(0)
            self.assertEqual(proof["pids"], [1, 2])
            self.assertEqual(stopped, {1, 2})

    async def test_pid_reuse_keeps_old_handle_and_signals_only_current_owned_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            owned = OwnedWorkers(Path(directory), Path(sys.executable), 1)
            root = Mock(flowdc_identity=(777, 1.0))
            owned.roots = [root]
            old = {"fd": 10, "created": 1.0, "worker": 0}
            owned.handles[123] = old
            parent = Mock(pid=777)
            parent.create_time.return_value = 1.0
            reused = Mock(pid=123)
            reused.create_time.return_value = 2.0
            reused.parents.return_value = [parent]
            stopped = {10}
            with (
                patch("flowdc_vine_ownership.psutil.Process") as process,
                patch("flowdc_vine_ownership.open_pidfd", return_value=99),
                patch.object(owned, "dead", lambda r: r["fd"] in stopped),
                patch("flowdc_vine_ownership.signal_pidfd", lambda fd, sig: stopped.add(fd)),
            ):
                process.return_value.children.return_value = [reused]
                owned.capture()
                self.assertEqual(owned.retired, [(123, old)])
                self.assertEqual(owned.handles[123]["created"], 2.0)
                proof = await owned.stop_tree(0)
            self.assertEqual(
                proof["identities"], [{"pid": 123, "created": 1.0}, {"pid": 123, "created": 2.0}]
            )
            self.assertEqual(stopped, {10, 99})

    async def test_launch_budget_is_spent_before_spawn_and_cannot_be_replayed(self):
        from flowdc_vine_cohort import cohort

        with tempfile.TemporaryDirectory() as directory:
            owned = OwnedWorkers(Path(directory), Path(sys.executable), 1)
            plan = cohort(1)
            owned.bind_cohort(plan)
            feature = plan["slots"][0]["feature"]
            with patch(
                "flowdc_vine_ownership.subprocess.Popen", side_effect=OSError("spawn failed")
            ) as spawn:
                with self.assertRaises(OSError):
                    owned.start(1234, Path(directory) / "credential", feature)
                with self.assertRaisesRegex(ValueError, "budget exhausted"):
                    owned.start(1234, Path(directory) / "credential", feature, replacement=True)
            self.assertEqual(spawn.call_count, 1)
            self.assertTrue((Path(directory) / "worker-0-intent.json").is_file())
            await owned.close()

    async def test_replacement_recaptures_late_unattributed_adoption_before_launch(self):
        from flowdc_vine_cohort import cohort

        with tempfile.TemporaryDirectory() as directory:
            owned = OwnedWorkers(Path(directory), Path(sys.executable), 1)
            plan = cohort(1, replacements=(0,))
            owned.bind_cohort(plan)
            feature = plan["slots"][0]["feature"]
            owned.launches = [feature]
            owned.roots = [Mock(flowdc_feature=feature, flowdc_identity=(777, 1.0))]

            # The root died and a previously unobserved detached child was
            # adopted since the last watch tick. Cached state looked quiescent.
            def capture():
                owned.unknown = {(888, 2.0)}

            with (
                patch.object(owned, "capture", capture),
                patch("flowdc_vine_ownership.subprocess.Popen") as spawn,
            ):
                with self.assertRaisesRegex(ValueError, "not quiescent"):
                    owned.start(1234, Path(directory) / "credential", feature, replacement=True)
            spawn.assert_not_called()
            self.assertEqual(owned.launches, [feature])
            self.assertFalse((Path(directory) / "worker-1-intent.json").exists())
