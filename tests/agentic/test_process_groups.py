"""Bounded termination regressions using disposable, locally owned process groups."""

import os
import selectors
import signal
import subprocess
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from test_workflow import workflow

# The shared fixture establishes the scripts import path.
# isort: split
import sessions

CHILD = """
import os
import signal
import sys
import time
if sys.argv[1] == "ignore":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
print(os.getpid(), flush=True)
time.sleep(60)
"""

LEADER = """
import subprocess
import sys
import time
child = subprocess.Popen(
    [sys.executable, "-c", sys.argv[1], sys.argv[2]],
    stdout=subprocess.PIPE, text=True,
)
print(child.stdout.readline().strip(), flush=True)
if sys.argv[3] == "stay":
    time.sleep(60)
"""


@unittest.skipUnless(Path("/proc/self/stat").exists(), "Linux process-state observation required")
class ProcessGroupTests(unittest.TestCase):
    def spawn(self, *, leader_stays, child_ignores_term):
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                LEADER,
                CHILD,
                "ignore" if child_ignores_term else "default",
                "stay" if leader_stays else "exit",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            start_new_session=True,
        )
        self.addCleanup(self.dispose_group, process)
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            self.assertTrue(selector.select(timeout=5), "Fixture child did not report readiness")
        child = int(process.stdout.readline())
        self.assertEqual(os.getpgid(child), process.pid)
        return process, child

    @staticmethod
    def dispose_group(process):
        # Independent emergency cleanup ensures a failing regression leaves no
        # sleeping fixture descendants. This group was created by this test.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()

    @staticmethod
    def running(pid):
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        except FileNotFoundError:
            return False
        # Orphan zombies await the system reaper but cannot execute or write.
        return fields[0] not in {"Z", "X"}

    def assert_stopped(self, pid):
        deadline = time.monotonic() + 3
        while self.running(pid) and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertFalse(self.running(pid), "Owned descendant survived group termination")

    def test_descendant_is_stopped_after_leader_was_already_reaped(self):
        process, child = self.spawn(leader_stays=False, child_ignores_term=False)
        process.wait(timeout=5)
        self.assertTrue(self.running(child))
        started = time.monotonic()
        sessions.stop_process(process)
        self.assertLess(time.monotonic() - started, 8)
        self.assert_stopped(child)

    def test_escalation_kills_ignoring_descendant_after_leader_exits_on_term(self):
        process, child = self.spawn(leader_stays=True, child_ignores_term=True)
        unrelated, unrelated_child = self.spawn(leader_stays=True, child_ignores_term=True)
        started = time.monotonic()
        sessions.stop_process(process)
        self.assertLess(time.monotonic() - started, 8)
        self.assertIsNotNone(process.returncode)
        self.assert_stopped(child)
        self.assertIsNone(unrelated.poll())
        self.assertTrue(self.running(unrelated_child))

    def test_leader_exit_between_group_and_session_lookup_still_escalates(self):
        process, child = self.spawn(leader_stays=True, child_ignores_term=True)

        def lose_leader(pid):
            self.assertEqual(pid, process.pid)
            process.kill()
            process.wait(timeout=5)
            raise ProcessLookupError("Fixture leader exited between ownership lookups")

        started = time.monotonic()
        with (
            patch.object(sessions.os, "getsid", side_effect=lose_leader) as lookup,
            patch.object(sessions.os, "killpg", wraps=os.killpg) as group_signal,
        ):
            sessions.stop_process(process)
        self.assertLess(time.monotonic() - started, 8)
        lookup.assert_called_once_with(process.pid)
        group_signal.assert_any_call(process.pid, signal.SIGTERM)
        group_signal.assert_any_call(process.pid, signal.SIGKILL)
        self.assertTrue(all(call.args[0] == process.pid for call in group_signal.call_args_list))
        self.assert_stopped(child)

    def test_process_outside_a_dedicated_session_is_refused(self):
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            with self.assertRaisesRegex(workflow.WorkflowError, "ownership changed"):
                sessions.stop_process(process)
            self.assertIsNone(process.poll())
        finally:
            process.kill()
            process.wait(timeout=5)
