"""Second review regressions; real harness signal tests have a separate entrypoint."""

import contextlib
import io
import json
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import UUID

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
import flowdc_experiment_transport as transport  # noqa: E402
from download_batch import AdaptiveSemaphore  # noqa: E402
from flowdc_experiment_source import dependency_required  # noqa: E402
from flowdc_methods import CandidateController, MethodConfig  # noqa: E402
from flowdc_topology import selection  # noqa: E402

from benchmark import compare_results  # noqa: E402
from benchmark.core import lifecycle  # noqa: E402
from benchmark.core.controlled_origin import ControlledOrigin  # noqa: E402
from benchmark.core.metrics import (  # noqa: E402
    AggregatedResult,
    BenchmarkReport,
    BenchmarkResult,
    ResourceMetrics,
)
from benchmark.shared_interrupt import cleanup_fixture  # noqa: E402


class SecondReviewTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.truth = {
            "manifest_sha256": "a" * 64,
            "original_rows": 1,
            "rows": [{"row_id": "b" * 64, "position": 0, "eligible": True}],
        }
        self.index = {
            "rows": [{"row_id": "b" * 64, "disposition": "verified", "useful_bytes": 3}],
            "artifacts_valid": True,
            "errors": [],
        }

    def test_relative_imports_require_missing_staged_module(self):
        for text in (
            "from . import flowdc_methods",
            "from .flowdc_methods import MethodConfig",
            "import flowdc_methods",
            "from flowdc_methods import MethodConfig",
        ):
            with self.subTest(text=text):
                self.assertTrue(dependency_required("bin/flowdc_methods.py", {"bin/download_batch.py": text}))
        self.assertTrue(
            dependency_required(
                "benchmark/core/truth.py", {"benchmark/core/verifier.py": "from .truth import digest"}
            )
        )
        self.assertFalse(
            dependency_required(
                "bin/flowdc_methods.py", {"bin/download_batch.py": "# flowdc_methods.py\nvalue='unrelated'"}
            )
        )

    def test_report_cli_distinguishes_schema_json_and_io_errors(self):
        path = self.root / "report.json"
        for raw, expected in (
            ('{"schema":"flowdc-known-truth-v1"}', "Unsupported report contract"),
            ("not json", "Invalid JSON"),
        ):
            path.write_text(raw)
            output = io.StringIO()
            with patch.object(sys, "argv", ["compare", str(path)]), contextlib.redirect_stdout(output):
                self.assertEqual(compare_results.main(), 1)
            self.assertIn(expected, output.getvalue())
        with (
            patch.object(sys, "argv", ["compare", str(path)]),
            patch.object(compare_results, "load_report", side_effect=PermissionError("denied")),
            contextlib.redirect_stdout(io.StringIO()) as output,
        ):
            self.assertEqual(compare_results.main(), 1)
        self.assertIn("Cannot read report", output.getvalue())

    def test_serializers_distinguish_unrounded_precision_from_legacy_unknown(self):
        records = (
            ResourceMetrics(),
            BenchmarkResult("flowdc", "base", 1, 1),
            AggregatedResult("flowdc", "base", 1),
            BenchmarkReport("test", "now", "input", 1),
        )
        for record in records:
            self.assertEqual(record.to_dict().get("numeric_precision"), "unrounded")
        path = self.root / "report.json"
        path.write_text('{"metadata":{},"results":{}}')
        self.assertEqual(compare_results.load_report(path).get("numeric_precision"), "unspecified_legacy")

    def test_thread_without_signal_owner_is_refused_before_launch(self):
        errors = []

        def worker():
            try:
                lifecycle.run_verified(
                    [sys.executable, "-c", "pass"], self.root / "run", self.truth, lambda: self.index
                )
            except ValueError as exc:
                errors.append(str(exc))

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertTrue(errors, "off-main invocation silently lacks interruption tracking")
        self.assertFalse((self.root / "run").exists())

    def test_owner_stat_error_retains_result_and_cleans_owned_process(self):
        # A real child is still cleaned via the group's unreaped Popen identity.
        with (
            patch.object(lifecycle.psutil, "Process", side_effect=psutil.AccessDenied(1)),
            patch.object(lifecycle, "_group_running", return_value=False),
        ):
            record = lifecycle.run_verified(
                [sys.executable, "-c", "pass"], self.root / "run", self.truth, lambda: self.index, cleanup=1
            )
        self.assertEqual(record["status"], "owner_record_unavailable")
        self.assertFalse(record["run_complete"])
        self.assertEqual(json.loads((self.root / "run/result.json").read_text()), record)

    def test_main_signal_owner_interrupts_and_joins_thread_lifecycle(self):
        for signum in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=signum):
                directory = self.root / signum.name
                result, errors = [], []
                previous = signal.getsignal(signum)
                with lifecycle.interruption_signals(raise_on_signal=False) as interruption:

                    def worker(result=result, errors=errors, directory=directory, interruption=interruption):
                        try:
                            result.append(
                                lifecycle.run_verified(
                                    [sys.executable, "-c", "import time; time.sleep(10)"],
                                    directory,
                                    self.truth,
                                    lambda: self.index,
                                    interruption=interruption,
                                    deadline=3,
                                    cleanup=0.5,
                                )
                            )
                        except BaseException as exc:
                            errors.append(exc)

                    thread = threading.Thread(target=worker)
                    thread.start()
                    try:
                        until = time.monotonic() + 2
                        while not (directory / "process-owner.json").exists() and thread.is_alive():
                            self.assertLess(time.monotonic(), until)
                            time.sleep(0.01)
                        signal.raise_signal(signum)
                        signal.raise_signal(signum)
                    finally:
                        interruption["requested"] = True
                        thread.join(5)
                    self.assertFalse(thread.is_alive())
                    self.assertFalse(errors)
                    self.assertEqual(result[0]["status"], "interrupted")
                    self.assertEqual(result[0]["interruption_signal"], signum.name)
                    self.assertFalse(result[0]["run_complete"])
                    self.assertEqual(result[0]["original_rows"], 1)
                self.assertEqual(signal.getsignal(signum), previous)

    def test_signal_fixture_cleans_partial_startup_without_owner_record(self):
        # The fake harness launches one child in its own group, then stalls before
        # publishing any lifecycle owner record. Cleanup must still own and join it.
        path = self.root / "child-pid"
        script = (
            "import pathlib,subprocess,sys,time; "
            "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'],start_new_session=True); "
            "pathlib.Path(sys.argv[1]).write_text(str(p.pid)); time.sleep(30)"
        )
        parent = subprocess.Popen([sys.executable, "-c", script, str(path)], start_new_session=True)
        evidence, handles = {"cleanup": []}, {}
        try:
            until = time.monotonic() + 3
            while not path.exists():
                self.assertLess(time.monotonic(), until)
                time.sleep(0.01)
        finally:
            cleanup_fixture(parent, handles, evidence)
        self.assertTrue(handles)
        self.assertTrue(evidence["cleanup_quiescent"])
        self.assertIsNotNone(parent.returncode)

    def test_signal_permission_failure_is_retained_without_verifying_live_files(self):
        fake = SimpleNamespace(pid=123456789, wait=Mock(return_value=0))
        verify = Mock(side_effect=AssertionError("verification must not run"))
        # No real signal target: process/group primitives are all mocked.
        with (
            patch.object(lifecycle.subprocess, "Popen", return_value=fake),
            patch.object(lifecycle.psutil, "Process", return_value=SimpleNamespace(create_time=lambda: 1)),
            patch.object(lifecycle, "_await_exit", side_effect=subprocess.TimeoutExpired("fixture", 1)),
            patch.object(lifecycle, "_group_running", return_value=True),
            patch.object(lifecycle.os, "killpg", side_effect=PermissionError("fixture")),
        ):
            record = lifecycle.run_verified(
                ["fake"], self.root / "run", self.truth, verify, deadline=1, cleanup=0.02
            )
        verify.assert_not_called()
        self.assertEqual(record["status"], "cleanup_failed")
        self.assertTrue(record["cleanup_errors"])
        self.assertFalse(record["run_complete"])
        self.assertEqual(record["original_rows"], 1)

    def test_origin_failure_precedes_audit_and_keeps_instrumentation_diagnostic(self):
        origin = ControlledOrigin.__new__(ControlledOrigin)
        origin.condition = threading.Condition()
        origin.model = SimpleNamespace(stop=Mock())
        origin.elapsed = lambda: 1
        origin.failure = RuntimeError("original model fault")
        origin.stop_without_events = Mock()
        origin.server = Mock()
        origin.server_thread = Mock(is_alive=lambda: False)
        origin.clock_thread = Mock(is_alive=lambda: False)
        origin.log = Mock()
        with self.assertRaisesRegex(ValueError, "origin instrumentation/model failed"):
            origin.__exit__(None, None, None)
        self.assertEqual(str(origin.failure), "original model fault")
        origin.server.server_close.assert_called_once()

    def test_dependency_literal_is_conservative_and_package_names_use_imports(self):
        self.assertTrue(
            dependency_required("bin/flowdc_methods.py", {"bin/a.py": 'message="flowdc_methods.py"'})
        )
        self.assertFalse(dependency_required("benchmark/__init__.py", {"bin/a.py": 'message="__init__.py"'}))
        self.assertTrue(
            dependency_required("benchmark/__init__.py", {"bin/a.py": "from benchmark.core import truth"})
        )

    def test_selection_drift_blocks_running_status_but_not_conservative_stop(self):
        ids = [str(UUID(int=i)) for i in range(1, 5)]
        spec = {
            "schema_version": 2,
            "vms": [
                {"id": vm_id, "role": role}
                for vm_id, role in zip(ids, ("manager", "worker", "worker-2", "origin"), strict=True)
            ],
            "topology": {"manager": ids[0], "workers": ids[1:3], "origin": ids[3]},
        }
        expected = {
            "registration_id": "fixture",
            "spec": spec,
            "service": {},
            "access": {},
            "selection": selection(spec, [ids[1]]),
        }
        changed = {**expected, "selection": selection(spec, [ids[2]])}
        controller = transport.Controller.__new__(transport.Controller)
        controller.root, controller.expected, controller.record = self.root, expected, expected
        controller.journal = Mock(read=Mock(return_value=changed))
        controller.command = ["fixture"]
        data = {
            "registration_id": "fixture",
            "desired": "run",
            "selected_ids": [ids[0], ids[2], ids[3]],
            "vms": [{"id": vm_id} for vm_id in ids],
        }
        with (
            patch.object(transport, "verify_release"),
            patch.object(
                transport, "execute", return_value=(0, json.dumps({"data": data}).encode())
            ) as execute,
        ):
            for action in ("start", "status"):
                with self.assertRaisesRegex(transport.ExperimentError, "run_selection_changed"):
                    controller.call(action)
            for action in ("stop", "reconcile"):
                self.assertEqual(controller.call(action), data)
            execute.reset_mock()
            controller.journal.read.return_value = {**changed, "registration_id": "different"}
            for action in ("start", "status", "stop", "reconcile"):
                with self.assertRaisesRegex(transport.ExperimentError, "registration_changed"):
                    controller.call(action)
            execute.assert_not_called()


class ObservationEvidence(unittest.IsolatedAsyncioTestCase):
    async def test_new_observations_are_counted_once_while_samples_remain_pending(self):
        events = []
        config = MethodConfig(method="gradient-candidate-v1", sample_min=3, sample_window_s=1.5)
        controller = CandidateController("origin", config, AdaptiveSemaphore, events.append)
        with patch("flowdc_methods.time.monotonic", return_value=1):
            for _ in range(2):
                await controller.metrics.record(200, 0.1, 1, dispatch_at=0.9)
            await controller.step_interval()
        with patch("flowdc_methods.time.monotonic", return_value=1.1):
            await controller.step_interval()
        with patch("flowdc_methods.time.monotonic", return_value=1.2):
            await controller.metrics.record(200, 0.1, 1, dispatch_at=1.1)
            await controller.step_interval()
        self.assertEqual([e["new_observations"] for e in events], [2, 0, 1])
        self.assertEqual([e["pending_samples"] for e in events], [2, 2, 0])
        self.assertEqual(controller._calculate_control_interval({"total": 100}), config.interval_s)
