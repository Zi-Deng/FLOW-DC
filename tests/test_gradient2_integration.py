"""Acquisition adapter ordering, missing observations and shared deduplication."""
import sys
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
import download_batch as batch
from flowdc_gradient2 import Gradient2, nanoseconds
from flowdc_methods import ControllerManager, GRADIENT2_METHOD, MethodConfig, method_record
from flowdc_shared import Authority, observation
from flowdc_shared_state import SCHEMA
from single_download import HTTP_MEASUREMENT_VERSION


def configuration(**options):
    return batch.normalize_config(batch.Config(
        "unused", "unused", control_method=GRADIENT2_METHOD,
        C_min=2, C_init=4, C_max=16, method_options=options,
    ))


def complete(**updates):
    return {
        "status": 200, "retry_after": None, "ttfb": .01, "body_delay": .2,
        "measurement_version": HTTP_MEASUREMENT_VERSION, "body_bytes": 8,
        "latency_eligible": True, "body_complete": True, "response_complete": True,
        "is_conn_error": False, "is_local_error": False, "is_unknown_error": False,
        "reason": "final", **updates,
    }


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_decisions_match_engine_without_tick_and_local_save_failure_keeps_delay(self):
        config = configuration()
        records = []
        manager = ControllerManager(config, batch.AdaptiveSemaphore, batch.PAARCController, records.append)
        controller = await manager.get_controller("http://fixture.test/a")
        reference = Gradient2(MethodConfig.from_config(config).engine_config())
        for i, delay in enumerate([.01] * 10 + [.1] * 20 + [.005] * 20):
            await controller.metrics.record(
                status_code=200, ttfb=delay, latency_eligible=True,
                is_local_error=True, acquisition_success=False, inflight=4,
                body_delay=delay + .1,
            )
            reference.sample(nanoseconds(delay), 4)
            self.assertEqual(controller.engine.state(), reference.state())
            self.assertEqual(controller.semaphore.limit, reference.limit)
        self.assertEqual(len(records), 50)
        await controller.step_interval()
        before = controller.engine.state()
        await controller.step_interval()
        self.assertEqual(controller.engine.state(), before)
        self.assertEqual(records[-1]["reason"], "no_observation_hold")

    async def test_no_sample_from_overload_partial_stale_or_missing_signal(self):
        records = []
        config = configuration(signal="body-completion-delay")
        controller = await ControllerManager(
            config, batch.AdaptiveSemaphore, batch.PAARCController, records.append,
        ).get_controller("http://fixture.test/a")
        for status, eligible in [(503, False), (200, False), (429, False)]:
            await controller.record(status_code=status, ttfb=None, latency_eligible=eligible, inflight=4)
        self.assertEqual(controller.engine.observations, 0)
        with self.assertRaises(ValueError):
            await controller.record(status_code=200, ttfb=.01, latency_eligible=True, inflight=4)
        self.assertEqual(controller.engine.observations, 0)
        await controller.record(status_code=200, ttfb=.01, body_delay=.3, latency_eligible=True, inflight=4)
        self.assertEqual(controller.engine.last_delay, 300000000)
        await controller.record(status_code=200, ttfb=.01, body_delay=.3, latency_eligible=True, inflight=1)
        self.assertEqual(controller.engine.reason, "application_limited")
        self.assertEqual(controller.engine.observations, 2)
        self.assertEqual(method_record(config)["measurement"], "body-completion-delay")

    async def test_shared_pre_retirement_inflight_and_completion_replay(self):
        with tempfile.TemporaryDirectory() as folder:
            authority = Authority(Path(folder) / "authority", configuration(queue_size=4, smoothing=1))
            try:
                token = authority.ledger.enroll(uuid4().hex, ["a" * 64, "b" * 64], attempts=1)
                client = uuid4().hex

                async def rpc(op, **args):
                    return await authority.request(token, {
                        "schema": SCHEMA, "binding": authority.ledger.current()["binding"],
                        "client_id": client, "operation": op, "arguments": args,
                    })

                await rpc("connect", worker_id="fixture-worker", epoch=1)
                permits = []
                for row in ("a" * 64, "b" * 64):
                    issued = await rpc("acquire", row_id=row, request_id=uuid4().hex, url="http://fixture.test/a")
                    await rpc("dispatch", permit_id=issued["permit_id"], epoch=1)
                    permits.append(issued["permit_id"])
                result = await rpc("complete", permit_id=permits[0], observation=complete())
                self.assertFalse(result["duplicate"])
                controller = await authority.controller("http://fixture.test/a")
                self.assertEqual(controller.engine.observations, 1)
                # Two were outstanding: 2 >= 4/2 updates to 8. A post-retirement
                # count of one would incorrectly hold at 4.
                self.assertEqual(controller.engine.limit, 8)
                ledger = authority.ledger.current()
                self.assertEqual(ledger["origins"]["http://fixture.test:80"]["limit"], 8)
                self.assertTrue((await rpc("complete", permit_id=permits[0], observation=complete()))["duplicate"])
                self.assertEqual(controller.engine.observations, 1)
                await rpc("complete", permit_id=permits[1], observation=complete())
                self.assertEqual(controller.engine.observations, 2)
                self.assertEqual(controller.engine.reason, "application_limited")
            finally:
                authority.ledger.close()

    def test_strict_configuration_and_wire_signal_validation(self):
        for options in [{"ablation": "no-gradient-term"}, {"hard_factor": .5},
                        {"sample_min": 1}, {"signal": "packet-rtt"}, {"queue_size": -1}]:
            with self.subTest(options=options), self.assertRaises((ValueError, TypeError)):
                configuration(**options)
        for value in [complete(body_delay=None), complete(body_delay=.001),
                      complete(measurement_version="3-output-independent-latency")]:
            with self.assertRaises(ValueError):
                observation(value)


if __name__ == "__main__":
    unittest.main()
