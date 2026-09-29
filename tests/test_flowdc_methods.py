"""Deterministic engineering contract and acquisition-gate checks (V1)."""

import asyncio
import json
import math
import shutil
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
import download_batch as base  # noqa: E402
import download_batch_gradient as legacy  # noqa: E402
import flowdc_integrity as integrity  # noqa: E402
from flowdc_methods import (  # noqa: E402
    ABLATIONS,
    METHODS,
    ControllerManager,
    ControlTrace,
    DelayPolicy,
    MethodConfig,
    ObservationBuffer,
    control_records,
    method_record,
    origin_key,
)

LOW = [0.1] * 5
QUEUED = [0.1, 0.1, 0.2, 0.2, 0.2]


class TransitionTests(unittest.TestCase):
    def test_elapsed_smoothing_derivative_and_persistence(self):
        policy = DelayPolicy(MethodConfig())
        first = policy.step(0, LOW)
        self.assertEqual(first["reason"], "baseline_reset")
        self.assertIsNone(first["derivative_per_s"])
        record = policy.step(0.5, QUEUED)
        alpha = 1 - math.exp(-0.5)
        self.assertAlmostEqual(record["queue_alpha"], alpha)
        self.assertAlmostEqual(record["queue_ewma_s"], 0.1 * alpha)
        self.assertAlmostEqual(record["derivative_per_s"], 2 * alpha)
        self.assertEqual(record["reason"], "gradient_persistence_hold")
        record = policy.step(1, QUEUED)
        self.assertEqual(record["reason"], "gradient_decrease")
        self.assertEqual(record["limit"], 3)  # floor(4 * .8)
        self.assertIsNone(policy.last_sample)

    def test_empty_sparse_intervals_hold_and_use_actual_elapsed_sample_time(self):
        policy = DelayPolicy(MethodConfig())
        policy.step(0, LOW)
        self.assertEqual(policy.step(0.2, [])["reason"], "empty_hold")
        self.assertEqual(policy.step(0.4, [0.01])["reason"], "sparse_hold")
        record = policy.step(0.6, QUEUED)
        self.assertEqual(record["sample_elapsed_s"], 0.6)
        self.assertEqual(record["baseline_s"], 0.1)
        self.assertAlmostEqual(record["queue_alpha"], 1 - math.exp(-0.6))
        policy.step(2.6, [])  # equality with stale_after clears confidence
        record = policy.step(2.7, QUEUED)
        self.assertEqual(record["reason"], "sample_initialize")
        self.assertIsNone(record["derivative_per_s"])
        self.assertEqual(record["limit"], 4)

    def test_baseline_probe_requires_fresh_samples_and_drain(self):
        policy = DelayPolicy(MethodConfig(baseline_max_age_s=2))
        policy.step(0, LOW)
        self.assertEqual(policy.step(2, [], inflight=4)["reason"], "baseline_probe")
        self.assertEqual(policy.limit, 2)
        self.assertEqual(policy.step(2.5, [0.2] * 5, inflight=3)["reason"], "probe_drain_hold")
        record = policy.step(2.6, [0.2] * 5, inflight=2)
        self.assertEqual(record["reason"], "baseline_reset")
        self.assertEqual(record["baseline_s"], 0.2)
        self.assertIsNone(record["derivative_per_s"])

    def test_lower_baseline_resets_derivative_and_numerical_floor(self):
        policy = DelayPolicy(MethodConfig())
        policy.step(0, LOW)
        policy.step(0.3, QUEUED)
        record = policy.step(0.5, [0.05] * 5)
        self.assertEqual(record["reason"], "baseline_reset")
        self.assertEqual(record["baseline_s"], 0.05)
        self.assertIsNone(record["gradient_ewma_per_s"])
        policy.step(0.6, [1e-9] * 5)
        self.assertEqual(policy.step(0.7, [1e-9] * 5)["reason"], "eligible_increase")
        self.assertEqual(policy.baseline, 1e-6)

    def test_overload_precedes_sparse_stale_and_recovery(self):
        policy = DelayPolicy(MethodConfig(c_init=9))
        policy.step(0, LOW)
        record = policy.step(10, [], overload=True)
        self.assertEqual(record["reason"], "overload_decrease")
        self.assertEqual(record["limit"], 4)
        self.assertIsNone(policy.probe_started)
        self.assertEqual(policy.step(10.2, [])["reason"], "baseline_probe")
        record = policy.step(10.8, LOW)
        self.assertEqual(record["reason"], "baseline_reset")
        self.assertEqual(policy.step(10.9, LOW)["reason"], "recovery_hold")
        self.assertEqual(policy.step(11, LOW)["reason"], "eligible_increase")

    def test_exact_gradient_thresholds_queue_floor_and_hard_decrease(self):
        # No smoothing makes the threshold construction exact in binary.
        config = MethodConfig(
            ablation="no-elapsed-smoothing",
            c_init=8,
            gradient_hold_per_s=0.125,
            gradient_decrease_per_s=0.25,
            queue_floor_s=0.125,
            persistence=1,
        )
        policy = DelayPolicy(config)
        policy.step(0, [1] * 5)
        record = policy.step(1, [1, 1, 1.125, 1.125, 1.125])
        self.assertEqual(record["reason"], "gradient_hold")
        record = policy.step(2, [1, 1, 1.375, 1.375, 1.375])
        self.assertEqual(record["reason"], "gradient_decrease")
        self.assertEqual(record["limit"], 6)
        policy.step(3, [1] * 5)
        record = policy.step(4, [1, 1, 3, 3, 3])
        self.assertEqual(record["reason"], "hard_queue_decrease")
        self.assertEqual(record["limit"], 3)

    def test_fixed_and_ratio_bounds_and_rounding(self):
        fixed = DelayPolicy(MethodConfig(method="fixed-v1", c_init=7))
        for now, delays, overloaded in ((0, LOW, False), (1, [], True), (50, [10] * 5, True)):
            self.assertEqual(fixed.step(now, delays, overload=overloaded)["limit"], 7)
        policy = DelayPolicy(MethodConfig(method="ratio-v1", c_init=10, c_max=11, queue_tau_s=0.001))
        policy.step(0, LOW)
        record = policy.step(1, QUEUED)
        self.assertEqual(record["reason"], "ratio_update")
        self.assertEqual(record["limit"], 6)  # floor(10 * .11/.2 + 1)
        for now in range(2, 9):
            record = policy.step(now, LOW)
        self.assertEqual(record["limit"], 11)

    def test_each_named_ablation_changes_only_its_mechanism(self):
        differences = {}
        for name in ABLATIONS:
            normal = DelayPolicy(MethodConfig())
            ablated = DelayPolicy(MethodConfig(ablation=name))
            normal.step(0, LOW)
            ablated.step(0, LOW)
            if name == "no-baseline-refresh":
                arguments = [(10, LOW, {})]
            elif name == "no-recovery-grace":
                arguments = [(0.1, [], {"overload": True}), (0.2, LOW, {}), (0.3, LOW, {})]
            else:
                arguments = [(0.5, [0.1] if name == "no-sample-gate" else QUEUED, {})]
            for now, values, kwargs in arguments:
                left, right = normal.step(now, values, **kwargs), ablated.step(now, values, **kwargs)
            differences[name] = (
                left["reason"] != right["reason"] or left["queue_alpha"] != right["queue_alpha"]
            )
            self.assertGreaterEqual(right["limit"], 2)
            self.assertLessEqual(right["limit"], 10000)
            self.assertEqual(ablated.step(11, [], overload=True)["reason"], "overload_decrease")
        self.assertEqual(differences, dict.fromkeys(ABLATIONS, True))

    def test_rejects_nonfinite_conflicts_and_clock_reversal(self):
        for field, value in (
            ("queue_tau_s", float("nan")),
            ("sample_min", True),
            ("c_init", 10001),
            ("gradient_hold_per_s", float("inf")),
            ("ablation", "remove-safety"),
        ):
            with self.subTest(field=field), self.assertRaises(ValueError):
                MethodConfig(**{field: value})
        config = base.Config(
            "unused",
            "unused",
            control_method="gradient-candidate-v1",
            method_options={"legacy_alpha": 0.2, "alpha_reference_s": 0.5},
        )
        mapped = MethodConfig.from_config(config)
        self.assertAlmostEqual(1 - math.exp(-0.5 / mapped.queue_tau_s), 0.2)
        for options in (
            {"legacy_alpha": 0.2},
            {"legacy_alpha": 0.2, "alpha_reference_s": 1, "queue_tau_s": 1},
            {"unexpected": 1},
            {"c_min": 1},
        ):
            with self.assertRaises(ValueError):
                MethodConfig.from_config(replace(config, method_options=options))
        policy = DelayPolicy(MethodConfig())
        for values in ([float("nan")], [0], [-1], [True], [float("inf")]):
            with self.assertRaises(ValueError):
                policy.step(0, values)
        policy.step(0, LOW)
        with self.assertRaises(ValueError):
            policy.step(0, LOW)


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_batches_do_not_accumulate_and_overload_discards_pending_samples(self):
        self.assertIsNone(MethodConfig().sample_window_s)
        buffer = ObservationBuffer()
        with patch("flowdc_methods.time.monotonic", return_value=1):
            await buffer.record(200, 0.1, latency_eligible=True, dispatch_at=0.9)
        self.assertEqual(buffer.consume()["delays"], [0.1])
        self.assertEqual(buffer.consume()["delays"], [])
        with patch("flowdc_methods.time.monotonic", return_value=2):
            await buffer.record(200, 0.1, latency_eligible=True, dispatch_at=1.9)
            await buffer.record(429, None, latency_eligible=False)
        snap = buffer.consume(now=2.1, window=1.5, minimum=5)
        self.assertTrue(snap["overload"])
        self.assertEqual(snap["delays"], [0.1])
        following = buffer.consume(now=2.2, window=1.5, minimum=5)
        self.assertFalse(following["overload"])
        self.assertEqual(following["delays"], [])

    async def test_sampling_window_rejects_ambiguous_or_stale_intervals(self):
        for window in (0, 0.1, 2, float("nan"), float("inf"), True):
            with self.subTest(window=window), self.assertRaises(ValueError):
                MethodConfig(sample_window_s=window)
        MethodConfig(sample_window_s=0.2)  # One whole control interval is allowed.

    async def test_bounded_fresh_sample_accumulation_never_reuses_or_resurrects_evidence(self):
        buffer = ObservationBuffer()
        with patch("flowdc_methods.time.monotonic", return_value=0):
            for _ in range(4):
                await buffer.record(200, 0.1, latency_eligible=True, dispatch_at=0)
        self.assertEqual(len(buffer.consume(now=0.2, window=1.5, minimum=5)["delays"]), 4)
        with patch("flowdc_methods.time.monotonic", return_value=0.4):
            await buffer.record(200, 0.1, latency_eligible=True, dispatch_at=0.3)
        self.assertEqual(len(buffer.consume(now=0.4, window=1.5, minimum=5)["delays"]), 5)
        self.assertEqual(buffer.consume(now=0.6, window=1.5, minimum=5)["delays"], [])
        with patch("flowdc_methods.time.monotonic", return_value=1):
            await buffer.record(200, 0.1, latency_eligible=True, dispatch_at=0.9)
        self.assertEqual(buffer.consume(now=2.5, window=1.5, minimum=5)["delays"], [])
        with patch("flowdc_methods.time.monotonic", return_value=3):
            await buffer.record(200, 0.1, latency_eligible=True, dispatch_at=2.9)
        self.assertEqual(buffer.consume(since=3, now=3.1, window=1.5, minimum=5)["delays"], [])

    async def test_low_rate_cold_start_and_probe_refresh_with_explicit_window(self):
        records = []
        config = base.Config(
            "unused",
            "unused",
            control_method="gradient-candidate-v1",
            method_options={"sample_window_s": 1.5, "baseline_max_age_s": 2},
        )
        manager = ControllerManager(config, base.AdaptiveSemaphore, base.PAARCController, records.append)
        controller = await manager.get_controller("http://fixture.invalid")
        for i in range(1, 22):
            now = i * 0.25  # Four completions/second: insufficient in every single tick.
            with patch("flowdc_methods.time.monotonic", return_value=now):
                await controller.metrics.record(200, 0.1, latency_eligible=True, dispatch_at=now - 0.1)
                await controller.step_interval()
        reasons = [record["reason"] for record in records]
        self.assertGreaterEqual(reasons.count("baseline_reset"), 2)
        self.assertIn("eligible_increase", reasons)
        self.assertIn("baseline_probe", reasons)
        self.assertTrue(all(record["pending_samples"] < 5 for record in records))

    async def test_selected_controller_changes_actual_acquisition_gate(self):
        records = []
        manager = ControllerManager(
            base.Config("unused", "unused", control_method="gradient-candidate-v1"),
            base.AdaptiveSemaphore,
            base.PAARCController,
            records.append,
        )
        controller = await manager.get_controller("http://EXAMPLE.invalid/object")
        self.assertIs(controller, await manager.get_controller("http://example.invalid:80/other"))
        self.assertIsNot(controller, await manager.get_controller("https://example.invalid:80/other"))
        for _ in range(4):
            await controller.semaphore.acquire()
        await controller.metrics.record(429, None, retry_after_sec=1)
        await controller.step_interval()
        self.assertEqual(controller.semaphore.limit, 2)
        waiter = asyncio.create_task(controller.semaphore.acquire())
        await asyncio.sleep(0)
        for _ in range(2):
            await controller.semaphore.release()
        await asyncio.sleep(0)
        self.assertFalse(waiter.done())  # Outstanding == reduced limit.
        await controller.semaphore.release()
        await asyncio.wait_for(waiter, 1)
        self.assertEqual(controller.semaphore.inflight, 2)
        await controller.semaphore.release()
        await controller.semaphore.release()
        self.assertEqual(records[0]["reason"], "overload_decrease")

    async def test_observations_keep_complete_body_on_save_failure_and_filter_probe(self):
        buffer = ObservationBuffer()
        await buffer.record(200, 0.1, latency_eligible=True, is_local_error=True, dispatch_at=1)
        await buffer.record(200, 0.2, latency_eligible=True, dispatch_at=3)
        await buffer.record(200, 0.3, latency_eligible=False, dispatch_at=3)
        await buffer.record(503, None, dispatch_at=3)
        self.assertEqual(buffer.consume(2), {"delays": [0.2], "overload": True, "total": 4})
        self.assertEqual(buffer.consume(), {"delays": [], "overload": False, "total": 0})

    async def test_explicit_base_uses_existing_state_machine_and_failure_closes_admission(self):
        manager = ControllerManager(
            base.Config("unused", "unused", control_method="paarc-base-v2"),
            base.AdaptiveSemaphore,
            base.PAARCController,
            lambda _: None,
        )
        controller = await manager.get_controller("http://fixture.invalid")
        self.assertIsInstance(controller, base.PAARCController)
        await controller.step_interval()
        manager.failure = ValueError("cannot record trajectory")
        with self.assertRaisesRegex(RuntimeError, "admission stopped"):
            await manager.get_controller("http://fixture.invalid")


class SelectionAndEvidenceTests(unittest.TestCase):
    def test_new_research_method_rejects_narrow_metadata_before_forced_overwrite(self):
        import polars as pl

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "input.parquet"
            output = root / "output"
            output.mkdir()
            marker = output / "keep"
            marker.write_text("existing user work")
            pl.DataFrame(
                {"url": ["http://fixture.invalid/image"], "narrow": pl.Series([7], dtype=pl.Int8)}
            ).write_parquet(manifest)
            config = base.Config(str(manifest), str(output), control_method="fixed-v1", force_overwrite=True)
            with self.assertRaisesRegex(ValueError, "unsupported research metadata"):
                base.validate_and_load(config)
            self.assertEqual(marker.read_text(), "existing user work")

    def test_defaults_legacy_label_and_invalid_config_before_output_mutation(self):
        self.assertIsNone(
            base.Config(
                "unused",
                "unused",
            ).control_method
        )
        self.assertEqual(
            method_record(
                legacy.Config(
                    "unused",
                    "unused",
                )
            )["id"],
            "gradient-legacy-v0",
        )
        self.assertFalse(
            method_record(
                base.Config(
                    "unused",
                    "unused",
                )
            )["explicit"]
        )
        for cfg in (
            base.Config("unused", "unused", control_method="unknown"),
            base.Config("unused", "unused", control_method="fixed-v1", enable_paarc=False),
            base.Config("unused", "unused", method_options={"queue_tau_s": 1}),
            legacy.Config("unused", "unused", control_method="gradient-candidate-v1"),
        ):
            with self.assertRaises(ValueError):
                base.normalize_config(cfg)
        self.assertEqual(origin_key("https://EXAMPLE.invalid/object"), ("https", "example.invalid", 443))

    def test_cli_selection_and_isolated_worker_dependency_closure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in (
                "download_batch.py",
                "single_download.py",
                "flowdc_integrity.py",
                "flowdc_methods.py",
            ):
                shutil.copyfile(ROOT / "bin" / name, root / name)
            result = subprocess.run(
                [sys.executable, "-B", str(root / "download_batch.py"), "--help"],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=15,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            for method in METHODS:
                self.assertIn(method, result.stdout)
            path = root / "config.json"
            path.write_text(json.dumps({"control_method": "fixed-v1"}))
            with patch.object(sys, "argv", ["download_batch.py", "--config", str(path)]):
                self.assertEqual(base.parse_args().control_method, "fixed-v1")
            with patch.object(sys, "argv", ["download_batch_gradient.py", "--config", str(path)]):
                with self.assertRaisesRegex(ValueError, "gradient-legacy-v0"):
                    legacy.parse_args()

    def test_trajectory_retains_interrupted_invocations_and_detects_tampering(self):
        from types import SimpleNamespace

        with tempfile.TemporaryDirectory() as directory, integrity.Files(directory) as fs:
            store = SimpleNamespace(fs=fs, owner={"run_id": "fixture-run"})
            config = base.Config("unused", "unused", control_method="fixed-v1")
            interrupted = ControlTrace(store, config)
            interrupted.emit({"limit": 4})
            interrupted.stream.close()  # Simulated process death, no finish descriptor.
            complete = ControlTrace(store, config)
            complete.emit({"limit": 4})
            complete.close(complete=True)
            records = control_records(store)
            self.assertEqual(len(records), 2)
            self.assertEqual(sum(record["closed"] for record in records), 1)
            self.assertEqual(sum(record["complete"] for record in records), 1)
            with (Path(directory) / complete.path).open("ab") as stream:
                stream.write(b"{}\n")
            with self.assertRaisesRegex(ValueError, "modified"):
                control_records(store)


if __name__ == "__main__":
    unittest.main()
