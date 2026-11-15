"""Frozen source decisions, observation freshness and adaptive admission."""

import asyncio
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from flowdc.admission import AdaptiveSemaphore
from flowdc.controllers import DelayPolicy, MethodConfig, ObservationBuffer


class TransitionTests(unittest.TestCase):
    def test_all_560_frozen_v2_transitions(self):
        fixture = json.loads((Path(__file__).parent / "fixtures/controllers-v2.json").read_text())
        reasons = set()
        for method, expected in fixture["methods"].items():
            policy = DelayPolicy(MethodConfig(method=method))
            for event, target in zip(fixture["events"], expected, strict=True):
                actual = policy.step(**event)
                self.assertEqual({k: actual[k] for k in target}, target)
                reasons.add(actual["reason"])
        self.assertTrue(
            {
                "startup_increase",
                "baseline_reset",
                "hard_queue_decrease",
                "gradient_persistence_hold",
                "overload_decrease",
                "empty_hold",
                "sparse_hold",
                "delay_budget_decrease",
            }
            <= reasons
        )

    def test_invalid_configuration_and_clock(self):
        for options in [
            {"method": "fixed-v1"},
            {"c_max": 1},
            {"c_init": True},
            {"sample_window_s": 5},
            {"target_delay_ratio": float("nan")},
        ]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                MethodConfig(**options)
        policy = DelayPolicy(MethodConfig())
        policy.step(1, [0.1] * 5)
        with self.assertRaises(ValueError):
            policy.step(1, [0.1] * 5)

    def test_probe_waits_for_owned_requests_then_refreshes_baseline(self):
        policy = DelayPolicy(MethodConfig())
        policy.step(1, [0.1] * 5, inflight=4)
        self.assertEqual(policy.step(121, [0.2] * 5, inflight=4)["reason"], "baseline_probe")
        self.assertEqual(policy.limit, 2)
        self.assertEqual(policy.step(122, [0.2] * 5, inflight=3)["reason"], "probe_drain_hold")
        result = policy.step(123, [0.2] * 5, inflight=2)
        self.assertEqual(result["reason"], "baseline_reset")
        self.assertEqual(result["baseline_s"], 0.2)
        self.assertEqual(result["baseline_age_s"], 0)
        self.assertIsNone(policy.gradient)

    def test_overload_precedes_probe_and_stale_derivative_is_reset(self):
        policy = DelayPolicy(MethodConfig())
        policy.step(1, [0.1] * 5, inflight=4)
        policy.step(2, [0.11] * 5, inflight=4)
        self.assertEqual(policy.step(121, [0.1] * 5, overload=True)["reason"], "overload_decrease")
        self.assertIsNone(policy.probe_started)
        self.assertIsNone(policy.gradient)
        policy.step(122, [0.1] * 5)
        policy.step(123, [0.1] * 5)
        self.assertEqual(policy.step(124, [0.1] * 5, inflight=0)["reason"], "utilization_hold")
        self.assertEqual(policy.step(130, [0.1] * 5)["reason"], "sample_initialize")


class ObservationTests(unittest.IsolatedAsyncioTestCase):
    async def test_sparse_samples_are_retained_once_and_stale_dispatches_removed(self):
        buffer = ObservationBuffer()
        for _ in range(3):
            with patch("flowdc.controllers.time.monotonic", return_value=10):
                await buffer.record(200, 0.1, 8, latency_eligible=True, dispatch_at=9)
        self.assertEqual(len(buffer.consume(now=10, window=2, minimum=5)["delays"]), 3)
        self.assertEqual(len(buffer.samples), 3)
        with patch("flowdc.controllers.time.monotonic", return_value=10.1):
            for _ in range(2):
                await buffer.record(200, 0.2, 8, latency_eligible=True, dispatch_at=10)
        self.assertEqual(len(buffer.consume(now=10.1, window=2, minimum=5)["delays"]), 5)
        self.assertFalse(buffer.samples)
        with patch("flowdc.controllers.time.monotonic", return_value=15):
            await buffer.record(200, 0.1, 8, latency_eligible=True, dispatch_at=11)
        self.assertFalse(buffer.consume(now=15, window=2)["delays"])

    async def test_local_save_failure_retains_http_latency_but_not_overload(self):
        buffer = ObservationBuffer()
        with patch("flowdc.controllers.time.monotonic", return_value=10):
            await buffer.record(
                200,
                0.1,
                0,
                acquisition_success=False,
                is_local_error=True,
                latency_eligible=True,
                dispatch_at=9,
            )
        sample = buffer.consume(now=10, window=2)
        self.assertEqual(sample["delays"], [0.1])
        self.assertFalse(sample["overload"])
        await buffer.record(429, None)
        self.assertTrue(buffer.consume()["overload"])

    async def test_shrinking_permits_drains_without_cancelling_requests(self):
        gate = AdaptiveSemaphore(4, 2, 8)
        for _ in range(4):
            await gate.acquire()
        gate.set_limit(2)
        waiter = asyncio.create_task(gate.acquire())
        await asyncio.sleep(0.01)
        self.assertFalse(waiter.done())
        for _ in range(3):
            await gate.release()
        await asyncio.wait_for(waiter, 1)
        self.assertEqual(gate.inflight, 2)
        await gate.release()
        await gate.release()
        self.assertEqual(gate.inflight, 0)
