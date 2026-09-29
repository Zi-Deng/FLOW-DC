"""Review regressions and executable evidence; socket gates run separately."""

import asyncio
import copy
import dataclasses
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
import download_batch as batch  # noqa: E402
from flowdc_methods import ControllerManager, DelayPolicy, MethodConfig  # noqa: E402
from flowdc_shared import Authority, RemoteAttempt, SharedClient, SharedControlError  # noqa: E402
from flowdc_topology import validate_roles  # noqa: E402
from single_download import HTTP_TRACE_CTX, download_via_http_get  # noqa: E402


class ConfigurationTests(unittest.TestCase):
    def test_shared_unbounded_timeout_is_rejected_before_input_or_output(self):
        for value in (0, -1, float("nan"), float("inf"), True):
            with self.subTest(value=value):
                config = batch.Config(
                    "missing-input",
                    "untouched-output",
                    control_method="fixed-v1",
                    shared_control_file="private.json",
                    timeout_sec=value,
                )
                with self.assertRaisesRegex(ValueError, "shared.*timeout"):
                    batch.normalize_config(config)
                with patch.object(batch, "load_manifest") as load:
                    with self.assertRaisesRegex(ValueError, "shared.*timeout"):
                        batch.validate_and_load(config)
                    load.assert_not_called()
        # Historical local no-timeout settings keep their existing meaning.
        self.assertEqual(batch.normalize_config(batch.Config("in", "out", timeout_sec=0)).timeout_sec, 0)

    def test_policy_clamps_its_own_state_and_rejects_same_clock_before_mutation(self):
        for method in ("gradient-candidate-v1", "ratio-v1"):
            policy = DelayPolicy(MethodConfig(method=method, c_min=3, c_init=4, c_max=8))
            for now in range(20):
                record = policy.step(now, [], overload=True)
                self.assertEqual(record["limit"], 3)
                self.assertEqual(policy.limit, 3)
            before = copy.deepcopy(vars(policy))
            with self.assertRaisesRegex(ValueError, "clock must advance"):
                policy.step(19, [0.1] * 5)
            self.assertEqual(vars(policy), before)
            policy.step(20, [0.1] * 5)
            self.assertEqual(policy.step(21, [0.1] * 5)["limit"], 4)

    def test_topology_version_keeps_strict_integer_check(self):
        for value in (True, False, 1.0, 2.0, None, "1", 0, 3):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_roles(("manager", "worker", "origin"), value)
        for version in (1, 2):
            self.assertEqual(
                validate_roles(("manager", "worker", "origin"), version), ("manager", "worker", "origin")
            )


class AsyncReviewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.config = batch.Config("in", "out", control_method="fixed-v1", C_min=1, C_init=1, C_max=1)
        self.authority = Authority(self.root / "authority", self.config)
        self.addCleanup(self.authority.ledger.close)
        self.authority.endpoint = "http://127.0.0.1:12345"
        self.path = self.root / "private.json"
        self.scope = uuid4().hex
        self.row = "1" * 64
        self.authority.enroll(self.scope, [self.row], self.path)
        self.events = []
        self.client = SharedClient(
            dataclasses.replace(self.config, shared_control_file=str(self.path)), self.events.append
        )
        self.authority.ledger.connect(self.scope, self.client.client_id, self.client.worker_id, 1)

    def session(self, statuses):
        calls = []
        authority, client = self.authority, self.client

        class Response:
            async def __aenter__(self):
                self.status = statuses[min(len(calls) - 1, len(statuses) - 1)]
                value = (
                    (await authority.request(client.description["credential"], calls[-1]))
                    if self.status == 200
                    else {"error": "backpressure"}
                )
                self.content = SimpleNamespace(read=AsyncMock(side_effect=[json.dumps(value).encode(), b""]))
                return self

            async def __aexit__(self, *args):
                pass

        def post(url, **kwargs):
            self.assertFalse(kwargs["allow_redirects"])
            calls.append(copy.deepcopy(kwargs["json"]))
            return Response()

        client.session = SimpleNamespace(post=post)
        return calls

    async def test_transient_backpressure_retries_same_acquire_without_duplicate_permit(self):
        calls = self.session([429, 429, 200])
        result = await self.client.rpc(
            "acquire", row_id=self.row, request_id=uuid4().hex, url="http://fixture.test/a"
        )
        self.assertEqual(result["state"], "issued")
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(message == calls[0] for message in calls))
        self.assertIsNone(self.client.failure)
        self.assertEqual(len(self.authority.ledger.current()["permits"]), 1)

    async def test_persistent_backpressure_has_finite_retries_and_remains_fail_closed(self):
        calls = self.session([429])
        with self.assertRaises(SharedControlError):
            await self.client.rpc("heartbeat")
        self.assertEqual(len(calls), 4)
        self.assertIsNotNone(self.client.failure)
        self.assertFalse(self.authority.ledger.current()["permits"])

    async def test_refusal_and_storage_failure_are_not_retried(self):
        for status in (403, 503):
            with self.subTest(status=status):
                self.client.failure = None
                calls = self.session([status, 200])
                with self.assertRaises(SharedControlError):
                    await self.client.rpc("heartbeat")
                self.assertEqual(len(calls), 1)

    async def test_message_queue_wait_shares_the_rpc_deadline(self):
        calls = self.session([200])
        for _ in range(4):
            await self.client.messages.acquire()
        try:
            with patch("flowdc_shared.RPC_TIMEOUT_S", 0.02):
                with self.assertRaises(SharedControlError):
                    await asyncio.wait_for(self.client.rpc("heartbeat"), 1)
            self.assertFalse(calls)
            self.assertIsNotNone(self.client.failure)
        finally:
            for _ in range(4):
                self.client.messages.release()

    async def test_backpressure_cancellation_does_not_replay_after_caller_stops(self):
        calls = self.session([429, 200])
        sleeping = asyncio.Event()

        async def wait(_):
            sleeping.set()
            await asyncio.Future()

        with patch("flowdc_shared.asyncio.sleep", side_effect=wait):
            task = asyncio.create_task(self.client.rpc("heartbeat"))
            await asyncio.wait_for(sleeping.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(len(calls), 1)
        self.assertFalse(self.authority.ledger.current()["permits"])

    async def test_acquire_and_dispatch_waits_are_inside_acquisition_deadline(self):
        for issued in (False, True):
            with self.subTest(issued=issued):
                replies = AsyncMock(return_value={"state": "wait"})
                if issued:
                    replies.side_effect = lambda op, **kw: (
                        {"state": "issued", "permit_id": "a" * 64} if op == "acquire" else {"state": "wait"}
                    )
                client = SimpleNamespace(
                    active_tasks=set(),
                    pending=set(),
                    description={"epoch": 1},
                    check_health=Mock(),
                    rpc=replies,
                )
                attempt = RemoteAttempt(client, self.row)
                measure = {}

                class Request:
                    def __init__(self, trace, remote):
                        self.trace, self.remote = trace, remote

                    async def __aenter__(self):
                        self.trace["phase"] = "admission"
                        await self.remote.dispatch("http://fixture.test/a")
                        raise AssertionError("no HTTP may dispatch without a permit receipt")

                    async def __aexit__(self, *args):
                        pass

                session = SimpleNamespace(get=Mock(return_value=Request(measure, attempt)))
                token = HTTP_TRACE_CTX.set(measure)
                try:
                    with patch(
                        "single_download.session_http_gate", return_value=SimpleNamespace(wait=AsyncMock())
                    ):
                        result = await asyncio.wait_for(
                            download_via_http_get(session, "http://fixture.test/a", 0.02), 1
                        )
                    self.assertEqual(result, (None, 408, "Request Timeout", None))
                    self.assertEqual(measure["failure_kind"], "admission")
                    self.assertFalse(measure["latency_eligible"])
                finally:
                    HTTP_TRACE_CTX.reset(token)
                    client.active_tasks.discard(asyncio.current_task())

    async def test_complete_without_dispatch_cannot_supply_delay_observation(self):
        calls = self.session([200])
        permit = await self.client.rpc(
            "acquire", row_id=self.row, request_id=uuid4().hex, url="http://fixture.test/a"
        )
        value = dict(
            status=200,
            retry_after=None,
            ttfb=0.1,
            body_bytes=3,
            latency_eligible=True,
            body_complete=True,
            response_complete=True,
            is_conn_error=False,
            is_local_error=False,
            is_unknown_error=False,
            reason="final",
        )
        with self.assertRaises(SharedControlError):
            await self.client.rpc("complete", permit_id=permit["permit_id"], observation=value)
        controller = await self.authority.controller("http://fixture.test/a")
        self.assertEqual(controller.metrics.consume()["delays"], [])
        self.assertEqual(len(calls), 2)
        self.assertIsNone(self.authority.ledger.current()["permits"][permit["permit_id"]]["completion"])

    async def test_concurrent_base_lookup_constructs_one_controller(self):
        manager = ControllerManager(
            dataclasses.replace(self.config, control_method="paarc-base-v2"),
            batch.AdaptiveSemaphore,
            batch.PAARCController,
            lambda _: None,
        )
        found = await asyncio.gather(*(manager.get_controller("http://fixture.test/a") for _ in range(64)))
        self.assertEqual(len(manager.controllers), 1)
        self.assertTrue(all(controller is found[0] for controller in found))


if __name__ == "__main__":
    unittest.main()
