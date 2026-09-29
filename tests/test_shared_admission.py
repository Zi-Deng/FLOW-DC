"""Distributed admission invariants, durable replay and conservative recovery."""

import copy
import dataclasses
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

from yarl import URL

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
from download_batch import Config  # noqa: E402
from flowdc_shared import (  # noqa: E402
    Authority,
    RemoteAttempt,
    SharedClient,
    SharedControlError,
    descriptor,
    protected_endpoint,
    runtime_binding,
)
from flowdc_shared_state import Ledger, origin, read_private, write_private  # noqa: E402
from flowdc_staging import DOWNLOAD_FILES  # noqa: E402
from single_download import HTTPTraceConfig  # noqa: E402


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 10.0
        self.binding = {
            "run_id": uuid4().hex,
            "source_sha256": "a" * 64,
            "config_sha256": "b" * 64,
            "method": "fixed-v1",
        }
        self.ledger = Ledger(self.root / "ledger", self.binding, clock=lambda: self.now)
        self.addCleanup(lambda: self.ledger.close())
        self.scope, self.client = [], []
        self.tokens = []
        self.rows = [str(i) * 64 for i in range(1, 5)]
        for i in range(2):
            scope, client = uuid4().hex, uuid4().hex
            self.tokens.append(self.ledger.enroll(scope, self.rows[i * 2 : i * 2 + 2]))
            self.ledger.connect(scope, client, f"worker-{i}", 1)
            self.scope.append(scope)
            self.client.append(client)
        self.url = "http://example.test/path"
        self.ledger.configure_origin(self.url, 2)

    def acquire(self, worker=0, request=None, url=None):
        return self.ledger.acquire(
            self.scope[worker],
            self.client[worker],
            self.rows[worker * 2],
            request or uuid4().hex,
            url or self.url,
        )

    def dispatch(self, permit, worker=0):
        return self.ledger.dispatch(self.scope[worker], self.client[worker], permit["permit_id"], 1)

    def complete(self, permit, worker=0, observation=None):
        return self.ledger.complete(
            self.scope[worker],
            self.client[worker],
            permit["permit_id"],
            {"response_complete": True, **(observation or {"reason": "response_eof"})},
        )

    def test_cancelled_dispatched_work_and_native_exit_retain_origin_capacity(self):
        first, second = self.acquire(0), self.acquire(1)
        self.dispatch(first, 0)
        self.dispatch(second, 1)
        self.complete(first, observation={"reason": "cancelled", "response_complete": False})
        self.assertEqual(self.acquire(1), {"state": "wait"})
        proof = {
            "kind": "native_task_exit",
            "client_id": self.client[0],
            "run_id": self.binding["run_id"],
            "source_sha256": self.binding["source_sha256"],
            "evidence_sha256": "d" * 64,
        }
        self.ledger.prove_quiescent(self.client[0], proof)
        self.assertEqual(self.acquire(1), {"state": "wait"})
        self.assertEqual(len(self.ledger.outstanding(self.ledger.current())), 2)

    def test_late_overload_headers_do_not_release_cancelled_origin_work(self):
        first, second = self.acquire(0), self.acquire(1)
        self.dispatch(first, 0)
        self.dispatch(second, 1)
        self.complete(first, observation={"reason": "cancelled", "response_complete": False})
        result = self.ledger.headers(self.scope[0], self.client[0], first["permit_id"], 503, 4)
        self.assertFalse(result["duplicate"])
        self.assertEqual(self.ledger.snapshot()["origins"][origin(self.url)]["embargo_until"], 14)
        self.now = 20
        self.assertEqual(self.acquire(1), {"state": "wait"})
        self.assertEqual(len(self.ledger.outstanding(self.ledger.current())), 2)

    def test_workers_share_cap_and_reduced_limit_drains_without_revocation(self):
        first, second = self.acquire(0), self.acquire(1)
        self.dispatch(first, 0)
        self.dispatch(second, 1)
        self.assertEqual(self.acquire(1), {"state": "wait"})
        self.ledger.configure_origin(self.url, 1)
        self.assertEqual(len(self.ledger.outstanding(self.ledger.snapshot())), 2)
        self.complete(first)
        self.assertEqual(self.acquire(0), {"state": "wait"})
        self.complete(second, 1)
        self.assertEqual(self.acquire(0)["state"], "issued")

    def test_request_replay_and_duplicate_completion_never_reissue_useful_work(self):
        request = uuid4().hex
        first = self.acquire(request=request)
        self.assertEqual(first, self.acquire(request=request))
        self.dispatch(first)
        with self.assertRaisesRegex(ValueError, "already dispatched"):
            self.dispatch(first)
        self.assertFalse(self.complete(first)["duplicate"])
        self.assertTrue(self.complete(first)["duplicate"])
        self.assertEqual(self.acquire(request=request)["state"], "complete")
        with self.assertRaisesRegex(ValueError, "conflicting completion"):
            self.complete(first, observation={"reason": "different"})
        with self.assertRaisesRegex(ValueError, "conflicting request"):
            self.acquire(request=request, url="http://example.test/different")
        events = [event for event in self.ledger.events() if event["action"] == "complete"]
        self.assertEqual(len(events), 1)

    def test_retry_after_is_aggregate_rechecked_at_dispatch_and_deduplicated(self):
        first, second = self.acquire(0), self.acquire(1)
        self.dispatch(first)
        self.ledger.headers(self.scope[0], self.client[0], first["permit_id"], 429, 3)
        self.assertEqual(self.dispatch(second, 1), {"state": "wait"})
        self.now = 11
        self.ledger.headers(self.scope[0], self.client[0], first["permit_id"], 429, 3)
        self.assertEqual(self.ledger.snapshot()["origins"][origin(self.url)]["embargo_until"], 13)
        self.complete(first, observation={"status": 429, "retry_after": 3})
        self.assertEqual(self.acquire(0), {"state": "wait"})
        self.now = 13
        self.assertEqual(self.dispatch(second, 1)["state"], "dispatched")

    def test_reordered_completion_carries_headers_before_releasing_capacity(self):
        first = self.acquire()
        self.dispatch(first)
        self.complete(first, observation={"status": 503, "retry_after": 4})
        self.assertEqual(self.acquire(1), {"state": "wait"})
        self.now = 11
        result = self.ledger.headers(self.scope[0], self.client[0], first["permit_id"], 503, 4)
        self.assertTrue(result["duplicate"])
        self.assertEqual(self.ledger.snapshot()["origins"][origin(self.url)]["embargo_until"], 14)

    def test_lost_heartbeat_and_late_acknowledgement_preserve_capacity(self):
        first, second = self.acquire(0), self.acquire(1)
        self.dispatch(first)
        self.dispatch(second, 1)
        self.now += 6
        self.ledger.heartbeat(self.scope[1], self.client[1])
        self.assertEqual(self.ledger.expire_clients(), [self.client[0]])
        self.assertEqual(self.acquire(1), {"state": "wait"})
        with self.assertRaisesRegex(ValueError, "fenced"):
            self.ledger.heartbeat(self.scope[0], self.client[0])
        self.complete(first)  # A late acknowledged completion is still valid.
        self.assertEqual(self.acquire(1)["state"], "issued")

    def test_restart_fences_until_all_clients_and_permits_acknowledge_closure(self):
        permit = self.acquire()
        self.dispatch(permit)
        self.ledger.headers(self.scope[0], self.client[0], permit["permit_id"], 429, 20)
        self.ledger.close()
        self.now = 0  # A different monotonic epoch after restart must be safe.
        self.ledger = Ledger(self.root / "ledger", self.binding, reopen=True, clock=lambda: self.now)
        self.assertEqual(self.ledger.snapshot()["origins"][origin(self.url)]["embargo_until"], 20)
        with self.assertRaisesRegex(ValueError, "fenced"):
            self.acquire(1)
        with self.assertRaisesRegex(ValueError, "uncertain"):
            self.ledger.recover_closed_epoch()
        self.complete(permit, observation={"status": 429, "retry_after": 20})
        with self.assertRaisesRegex(ValueError, "unclosed"):
            self.ledger.recover_closed_epoch()
        for scope, client in zip(self.scope, self.client, strict=True):
            self.ledger.close_client(scope, client)
        self.assertEqual(self.ledger.recover_closed_epoch(), 2)
        with self.assertRaisesRegex(ValueError, "fenced epoch"):
            self.ledger.connect(self.scope[0], uuid4().hex, "worker-new", 2)
        scope, client = uuid4().hex, uuid4().hex
        self.ledger.enroll(scope, self.rows[:2])
        self.ledger.connect(scope, client, "worker-new", 2)
        self.ledger.configure_origin(self.url, 2)
        self.assertEqual(
            self.ledger.acquire(scope, client, self.rows[0], uuid4().hex, self.url), {"state": "wait"}
        )
        self.assertEqual(len(self.ledger.snapshot()["permits"]), 1)

    def test_identity_authentication_scope_and_task_replay_bounds(self):
        self.assertEqual(self.ledger.authenticate(self.tokens[0]), self.scope[0])
        with self.assertRaisesRegex(ValueError, "authentication"):
            self.ledger.authenticate("f" * 64)
        with self.assertRaisesRegex(ValueError, "client identity"):
            self.ledger.acquire(self.scope[0], self.client[1], self.rows[0], uuid4().hex, self.url)
        with self.assertRaisesRegex(ValueError, "row outside"):
            self.ledger.acquire(self.scope[0], self.client[0], self.rows[2], uuid4().hex, self.url)
        first = self.acquire()
        with self.assertRaisesRegex(ValueError, "permit identity"):
            self.complete(first, 1)
        self.ledger.connect(self.scope[0], uuid4().hex, "worker-replay", 1)
        with self.assertRaisesRegex(ValueError, "replay bound"):
            self.ledger.connect(self.scope[0], uuid4().hex, "worker-third", 1)
        exported = json.dumps(self.ledger.export())
        self.assertNotIn("token", exported)
        self.assertTrue(all(token not in exported for token in self.tokens))

    def test_crash_before_commit_has_no_partial_permit_and_owner_lock_is_exclusive(self):
        before = copy.deepcopy(self.ledger.snapshot())
        self.ledger.fault = lambda _: (_ for _ in ()).throw(OSError("test crash cut"))
        with self.assertRaisesRegex(OSError, "crash cut"):
            self.acquire()
        self.assertEqual(self.ledger.snapshot(), before)
        self.ledger.fault = lambda _: None
        with self.assertRaises(BlockingIOError):
            Ledger(self.root / "ledger", self.binding, reopen=True)
        self.assertEqual(self.acquire()["state"], "issued")

    def test_private_descriptors_reject_symlinks_and_public_modes(self):
        path = self.root / "descriptor.json"
        write_private(path, {"test": True})
        self.assertEqual(read_private(path), {"test": True})
        link = self.root / "link"
        link.symlink_to(path)
        with self.assertRaises(OSError):
            read_private(link)
        path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "owner-private"):
            read_private(path)

    def test_canonical_origins_include_scheme_effective_port_and_idna(self):
        self.assertEqual(origin("HTTP://EXAMPLE.test/a"), origin("http://example.test:80/b"))
        self.assertNotEqual(origin("https://example.test:80"), origin("http://example.test"))
        self.assertEqual(origin("http://[::1]/a"), "http://[::1]:80")
        self.assertEqual(origin("https://b\u00fccher.test/a"), "https://xn--bcher-kva.test:443")

    def test_retained_completed_permits_do_not_cause_quadratic_serialization(self):
        import flowdc_shared_state as state_module

        volumes = []
        original_encode = state_module.encode
        for count in (16, 32):
            ledger = Ledger(self.root / f"volume-{count}", self.binding)
            try:
                scope, client = uuid4().hex, uuid4().hex
                ledger.enroll(scope, [self.rows[0]])
                ledger.connect(scope, client, "worker", 1)
                ledger.configure_origin(self.url, 2)
                volume = 0

                def measured(value):
                    nonlocal volume
                    raw = original_encode(value)
                    volume += len(raw)
                    return raw

                with patch.object(state_module, "encode", measured):
                    for _ in range(count):
                        permit = ledger.acquire(scope, client, self.rows[0], uuid4().hex, self.url)
                        ledger.dispatch(scope, client, permit["permit_id"], 1)
                        ledger.headers(scope, client, permit["permit_id"], 200, None)
                        ledger.complete(
                            scope,
                            client,
                            permit["permit_id"],
                            {"status": 200, "retry_after": None, "response_complete": True},
                        )
                volumes.append(volume)
                self.assertEqual(len(ledger.snapshot()["permits"]), count)
            finally:
                ledger.close()
        self.assertLess(volumes[1], volumes[0] * 2.5, volumes)


class NativeFixtureTests(unittest.IsolatedAsyncioTestCase):
    def test_arrival_accounting_detects_offered_work_hidden_by_service_queue(self):
        from benchmark.core.controlled_origin import ServiceModel, audit_events

        events = []

        def emit(value):
            events.append({"sequence": len(events) + 1, "origin_monotonic_ns": len(events), **value})

        model = ServiceModel([[0, 2]], 8, emit)
        for request in range(3):
            model.arrive(0, request, f"/{request}")
        for request in range(3):
            model.finish(1, request)
            emit({"phase": "response", "request_id": request})
        result = audit_events(events, {"instrumented": True, "requests": 3, "responses": 3})
        self.assertEqual(result.get("peak_open_requests"), 3)

    async def test_pidfd_injection_does_not_require_cpython_pidfd_bindings(self):
        import signal

        import psutil

        from benchmark.shared_origin import inject_failure

        process = subprocess.Popen(
            [sys.executable, "-B", "-c", "import time; time.sleep(10)"], start_new_session=True
        )
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                owner = root / "client-0/run/process-owner.json"
                owner.parent.mkdir(parents=True)
                owner.write_text(
                    json.dumps({"pid": process.pid, "create_time": psutil.Process(process.pid).create_time()})
                )
                with (
                    patch.object(os, "pidfd_open", None, create=True),
                    patch.object(signal, "pidfd_send_signal", None, create=True),
                ):
                    await inject_failure(
                        root,
                        "worker-loss",
                        None,
                        [SimpleNamespace(done=lambda: False)],
                        [{"phase": "arrival"}] * 4,
                    )
            self.assertEqual(process.wait(timeout=3), -signal.SIGKILL)
        finally:
            if process.poll() is None:
                process.terminate()  # This Popen owns the still-unreaped child.
                process.wait(timeout=3)


class AcquisitionBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = Config("input", "output", control_method="fixed-v1", C_min=1, C_init=1, C_max=2)
        self.authority = Authority(self.root / "authority", self.config)
        self.addCleanup(self.authority.ledger.close)
        self.authority.endpoint = "http://127.0.0.1:12345"
        self.clients = []
        for i in range(2):
            scope, client_id, row = uuid4().hex, uuid4().hex, str(i + 1) * 64
            token = self.authority.ledger.enroll(scope, [row])
            self.authority.ledger.connect(scope, client_id, f"worker-{i}", 1)

            class Bridge:
                """Pure authenticated RPC bridge; does not claim network validation."""

                def __init__(self, scope, client_id, row, token, authority):
                    self.authority = authority
                    self.scope, self.client_id, self.row, self.token = scope, client_id, row, token
                    self.pending, self.active_tasks = set(), set()
                    self.description = {"epoch": 1}

                def check_health(self):
                    pass

                async def rpc(self, operation, **arguments):
                    return await self.authority.request(
                        self.token,
                        {
                            "schema": "flowdc-shared-admission-v1",
                            "binding": self.authority.ledger.snapshot()["binding"],
                            "client_id": self.client_id,
                            "operation": operation,
                            "arguments": arguments,
                        },
                    )

            self.clients.append(Bridge(scope, client_id, row, token, self.authority))

    async def test_actual_http_hooks_release_reciprocal_redirects_before_next_origin(self):
        trace = HTTPTraceConfig()
        attempts = [RemoteAttempt(client, client.row) for client in self.clients]
        contexts = []
        for attempt in attempts:
            measure = {"shared_dispatch": attempt.dispatch, "shared_headers": attempt.headers}

            async def release(attempt=attempt, measure=measure):
                await attempt.finish_hop(measure, "redirect")

            measure["shared_redirect_release"] = release
            contexts.append(SimpleNamespace(measurement=measure))
        gate = SimpleNamespace(wait=AsyncMock(), observe=Mock(return_value=None))
        urls = [URL("http://one.test/a"), URL("http://two.test/b")]
        with patch("single_download.session_http_gate", return_value=gate):
            for ctx, url in zip(contexts, urls, strict=True):
                await trace._dispatch(None, ctx, SimpleNamespace(url=url))
            for i, ctx in enumerate(contexts):
                response = SimpleNamespace(
                    url=urls[i],
                    headers={"Location": str(urls[1 - i])},
                    status=302,
                    release=Mock(),
                    content=SimpleNamespace(read=AsyncMock(side_effect=[b"redirect body", b""])),
                )
                await trace._redirect(None, ctx, SimpleNamespace(response=response))
                response.release.assert_called_once()
                self.assertEqual(response.content.read.await_count, 2)
            self.assertFalse(self.authority.ledger.outstanding(self.authority.ledger.snapshot()))
            for i, ctx in enumerate(contexts):
                await trace._dispatch(None, ctx, SimpleNamespace(url=urls[1 - i]))
                response = SimpleNamespace(url=urls[1 - i], headers={}, status=200)
                await trace._end(None, ctx, SimpleNamespace(response=response))
                ctx.measurement.update(
                    ttfb=0.1, observed_response_body_bytes=3, latency_eligible=True, body_completed_at=1
                )
                await attempts[i].finish_hop(ctx.measurement, "final")
                await attempts[i].close(ctx.measurement)
        state = self.authority.ledger.snapshot()
        self.assertEqual(len(state["permits"]), 4)
        self.assertFalse(self.authority.ledger.outstanding(state))
        self.assertEqual(sum(p["completion"]["reason"] == "redirect" for p in state["permits"].values()), 2)

    async def test_same_origin_and_invalid_redirect_targets_also_release(self):
        client = self.clients[0]
        for destination in ("/again", "file:///unsupported"):
            attempt = RemoteAttempt(client, client.row)
            await attempt.dispatch("http://one.test/a")
            trace = HTTPTraceConfig()

            measure = {"shared_headers": attempt.headers}

            async def release(attempt=attempt, measure=measure):
                await attempt.finish_hop(measure, "redirect")

            measure["shared_redirect_release"] = release
            ctx = SimpleNamespace(measurement=measure)
            response = SimpleNamespace(
                url=URL("http://one.test/a"),
                headers={"Location": destination},
                status=302,
                release=Mock(),
                content=SimpleNamespace(read=AsyncMock(return_value=b"")),
            )
            gate = SimpleNamespace(wait=AsyncMock(), observe=Mock(return_value=None))
            with patch("single_download.session_http_gate", return_value=gate):
                await trace._redirect(None, ctx, SimpleNamespace(response=response))
            self.assertIsNone(attempt.permit)
            self.assertFalse(client.pending)
            await attempt.close({})

    async def test_interrupted_redirect_body_retains_capacity_and_never_dispatches_target(self):
        client = self.clients[0]
        attempt = RemoteAttempt(client, client.row)
        await attempt.dispatch("http://one.test/a")
        trace = HTTPTraceConfig()
        measure = {"shared_headers": attempt.headers, "shared_redirect_release": AsyncMock()}
        response = SimpleNamespace(
            url=URL("http://one.test/a"),
            headers={"Location": "http://two.test/b"},
            status=302,
            release=Mock(),
            content=SimpleNamespace(read=AsyncMock(side_effect=TimeoutError("body incomplete"))),
        )
        gate = SimpleNamespace(wait=AsyncMock(), observe=Mock(return_value=None))
        with patch("single_download.session_http_gate", return_value=gate):
            with self.assertRaises(TimeoutError):
                await trace._redirect(
                    None, SimpleNamespace(measurement=measure), SimpleNamespace(response=response)
                )
        measure["shared_redirect_release"].assert_not_awaited()
        response.release.assert_not_called()
        await attempt.close(measure)
        state = self.authority.ledger.snapshot()
        self.assertEqual(len(state["permits"]), 1)
        self.assertEqual(self.authority.ledger.outstanding(state)[0]["state"], "uncertain")

    async def test_lost_acquire_acknowledgement_never_recycles_the_issued_permit(self):
        path = self.root / "private.json"
        scope, row = uuid4().hex, "3" * 64
        self.authority.enroll(scope, [row], path)
        config = dataclasses.replace(self.config, shared_control_file=str(path))
        client = SharedClient(config, lambda _: None)
        self.authority.ledger.connect(scope, client.client_id, client.worker_id, 1)
        await self.authority.controller("http://one.test/a")
        original = self.authority.ledger.snapshot()

        class LostReply:
            async def __aenter__(inner):
                await self.authority.request(client.description["credential"], inner.message)
                raise OSError("injected acknowledgement loss")

            async def __aexit__(inner, *args):
                pass

        def post(url, **kwargs):
            reply = LostReply()
            reply.message = kwargs["json"]
            self.assertFalse(kwargs["allow_redirects"])
            return reply

        client.session = SimpleNamespace(post=post)
        with self.assertRaises(SharedControlError):
            await client.rpc("acquire", row_id=row, request_id=uuid4().hex, url="http://one.test/a")
        self.assertEqual(len(original["permits"]), 0)
        self.assertEqual(len(self.authority.ledger.outstanding(self.authority.ledger.snapshot())), 1)
        with self.assertRaises(SharedControlError):
            client.check_health()

    async def test_private_binding_rejects_method_or_source_mismatch_before_client_creation(self):
        path = self.root / "private.json"
        self.authority.enroll(uuid4().hex, ["3" * 64], path)
        config = dataclasses.replace(self.config, shared_control_file=str(path))
        self.assertEqual(
            descriptor(config)["binding"]["source_sha256"], runtime_binding(config)["source_sha256"]
        )
        with self.assertRaisesRegex(ValueError, "mismatch"):
            descriptor(dataclasses.replace(config, C_init=2))
        for endpoint in (
            "http://example.test:123",
            "http://192.0.2.1",
            "http://user:secret@127.0.0.1",
            "http://127.0.0.1/path",
        ):
            with self.assertRaises(ValueError):
                protected_endpoint(endpoint)
        self.assertEqual(protected_endpoint("https://example.test:123"), "https://example.test:123")

    async def test_rpc_reads_complete_fragmented_receipts_with_a_cumulative_bound(self):
        path = self.root / "framing-private.json"
        self.authority.enroll(uuid4().hex, ["3" * 64], path)
        config = dataclasses.replace(self.config, shared_control_file=str(path))
        receipt = {"state": "dispatched", "epoch": 1}
        raw = json.dumps(receipt).encode()

        class Response:
            status = 200

            def __init__(self, fragments):
                self.fragments = iter(fragments)
                self.content = self

            async def read(self, size):
                return next(self.fragments, b"")

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        for fragments, valid in (
            ([raw[:6], raw[6:]], True),
            ([raw], True),
            ([b" " * 16000] * 3, False),
            ([raw[:-1]], False),
            ([raw, b"{}"], False),
        ):
            client = SharedClient(config, lambda _: None)
            client.session = SimpleNamespace(
                post=lambda *args, fragments=fragments, **kwargs: Response(fragments)
            )
            if valid:
                self.assertEqual(await client.rpc("dispatch", permit_id="f" * 64, epoch=1), receipt)
                self.assertIsNone(client.failure)
            else:
                with self.assertRaises(SharedControlError):
                    await client.rpc("dispatch", permit_id="f" * 64, epoch=1)
                self.assertIsNotNone(client.failure)

    async def test_isolated_declared_module_closure_imports_worker_cli_and_shared_path(self):
        sandbox = self.root / "sandbox"
        sandbox.mkdir()
        for name in DOWNLOAD_FILES:
            shutil.copyfile(ROOT / "bin" / name, sandbox / name)
        environment = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
        commands = [
            [sys.executable, "-B", "download_batch.py", "--help"],
            [
                sys.executable,
                "-B",
                "-c",
                "from flowdc_shared import runtime_binding; from download_batch import Config; print(runtime_binding(Config('input','output',control_method='fixed-v1')))",
            ],
        ]
        for command in commands:
            result = subprocess.run(
                command, cwd=sandbox, env=environment, capture_output=True, text=True, timeout=15
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    async def test_smoke_native_configuration_round_trips_without_binding_drift(self):
        sys.path.insert(0, str(ROOT))
        from download_batch import normalize_config, parse_args

        from benchmark.shared_origin import native_config

        config = normalize_config(
            dataclasses.replace(
                self.config, research_profile=True, shared_control_file="control-private.json"
            )
        )
        path = self.root / "config.json"
        path.write_text(json.dumps(native_config(config)))
        with patch.object(sys, "argv", ["download_batch", "--config", str(path)]):
            actual = normalize_config(parse_args())
        self.assertEqual(actual, config)

    async def test_independent_event_replay_rejects_cap_multiplication(self):
        sys.path.insert(0, str(ROOT))
        from benchmark.shared_origin import audit_admission

        attempt = RemoteAttempt(self.clients[0], self.clients[0].row)
        await attempt.dispatch("http://one.test/a")
        events = self.authority.ledger.events()
        self.assertEqual(audit_admission(events)["peak_issued_per_origin"], 1)
        acquire = copy.deepcopy(next(event for event in events if event["action"] == "acquire"))
        acquire.update(sequence=len(events) + 1, permit_id="f" * 64)
        with self.assertRaisesRegex(ValueError, "aggregate admission"):
            audit_admission([*events, acquire])
        await attempt.finish_hop({}, "cancelled")
        self.assertEqual(audit_admission(self.authority.ledger.events())["outstanding_permits"], 1)
        self.assertEqual(
            self.authority.ledger.outstanding(self.authority.ledger.current())[0]["state"], "uncertain"
        )
        await attempt.close({})


if __name__ == "__main__":
    unittest.main()
