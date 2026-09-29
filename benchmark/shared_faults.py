#!/usr/bin/env python3
"""Bounded real loopback control faults; run separately from ordinary unit tests."""

import argparse
import asyncio
import dataclasses
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import aiohttp
from aiohttp import web

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT))
from download_batch import Config  # noqa: E402
from flowdc_shared import Authority, RemoteAttempt, SharedClient, SharedControlError  # noqa: E402
from flowdc_shared_state import SCHEMA, read_private  # noqa: E402
from flowdc_staging import DOWNLOAD_FILES  # noqa: E402
from single_download import HTTP_TRACE_CTX, download_via_http_get  # noqa: E402

from benchmark.core.provenance import environment_record  # noqa: E402

CONFIG = Config("input", "output", control_method="fixed-v1", C_min=1, C_init=2, C_max=2)
ROWS = ["1" * 64, "2" * 64]


def sources():
    return {n: hashlib.sha256((ROOT / "bin" / n).read_bytes()).hexdigest() for n in DOWNLOAD_FILES}


def observation(status=200, delay=None):
    return dict(
        status=status,
        retry_after=delay,
        ttfb=None,
        body_bytes=0,
        latency_eligible=False,
        body_complete=False,
        response_complete=True,
        is_conn_error=False,
        is_local_error=False,
        is_unknown_error=False,
        reason="final",
    )


async def close_resources(authority, *closers):
    """Attempt every resource closure even when a client cannot acknowledge closure."""
    try:
        outcomes = await asyncio.gather(*(close() for close in closers), return_exceptions=True)
        failures = [value for value in outcomes if isinstance(value, BaseException)]
        if failures:
            raise BaseExceptionGroup("fixture resource cleanup failed", failures)
    finally:
        try:
            await authority.stop()
        finally:
            authority.ledger.close()


class DroppedReplyAuthority(Authority):
    drop_operation = None
    drops = 0

    async def handle(self, request):
        message = await request.json()
        response = await super().handle(request)
        if message.get("operation") == self.drop_operation and response.status == 200:
            self.drop_operation = None
            self.drops += 1
            request.transport.abort()
        return response


async def enroll(authority, path, i):
    desc = path / f"private-{i}.json"
    authority.enroll(uuid4().hex, [ROWS[i]], desc)
    return read_private(desc)


async def rpc(session, endpoint, desc, client, operation, *, expected=200, token=None, **arguments):
    message = dict(
        schema=SCHEMA, binding=desc["binding"], client_id=client, operation=operation, arguments=arguments
    )
    async with session.post(
        endpoint + "/control",
        json=message,
        headers={"Authorization": "Bearer " + (token or desc["credential"])},
    ) as reply:
        data = await reply.json()
        assert reply.status == expected, (operation, reply.status, data)
        return data


async def lost_ack(output, results, operation):
    path = output / ("lost-" + operation)
    path.mkdir(mode=0o700)
    authority = DroppedReplyAuthority(path / "authority", CONFIG)
    client = None
    try:
        await authority.start()
        await enroll(authority, path, 0)
        client = SharedClient(
            dataclasses.replace(CONFIG, shared_control_file=str(path / "private-0.json")), lambda event: None
        )
        await client.start()
        args = dict(row_id=ROWS[0], request_id=uuid4().hex, url="http://127.0.0.1:1/object")
        if operation == "complete":
            permit = await client.rpc("acquire", **args)
            await client.rpc("dispatch", permit_id=permit["permit_id"], epoch=1)
            # Complete response evidence is deliberately absent here: this case
            # tests transport failure with uncertain dispatched origin work.
            obs = {**observation(), "response_complete": False, "reason": "cancelled"}
            args = dict(permit_id=permit["permit_id"], observation=obs)
        authority.drop_operation = operation
        try:
            await client.rpc(operation, **args)
            raise AssertionError("dropped reply unexpectedly accepted")
        except SharedControlError:
            pass
        assert client.failure is not None and authority.drops == 1
        state = authority.ledger.snapshot()
        outstanding = authority.ledger.outstanding(state)
        assert len(state["permits"]) == len(outstanding) == 1
        assert outstanding[0]["state"] == ("issued" if operation == "acquire" else "uncertain")
        (path / "ledger-export.json").write_text(json.dumps(authority.ledger.export(), indent=2))
        results.append(
            dict(
                case="lost-" + operation,
                passed=True,
                retained_permits=1,
                client_failed_closed=True,
                actual_tcp_abort_count=authority.drops,
            )
        )
    finally:
        await close_resources(authority, *([client.close] if client is not None else []))


async def protocol(output, results):
    path = output / "protocol"
    path.mkdir(mode=0o700)
    authority = Authority(path / "authority", CONFIG)
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3))
    origin_events = []

    async def serve(request):
        origin_events.append({"path": request.path, "event": "request"})
        return web.Response(status=503, body=b"bounded fixture", headers={"Retry-After": "1"})

    app = web.Application()
    app.router.add_get("/object", serve)
    origin_runner = web.AppRunner(app, shutdown_timeout=2)
    try:
        await origin_runner.setup()
        site = web.TCPSite(origin_runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/object"
        await authority.start()
        descs = [await enroll(authority, path, i) for i in range(2)]
        clients = [uuid4().hex, uuid4().hex]

        async def call(i, op, **args):
            return await rpc(session, authority.endpoint, descs[i], clients[i], op, **args)

        for i in range(2):
            await call(i, "connect", worker_id=f"worker-{i}", epoch=1)
        # Reconnect is idempotent; changed identity, cross-scope and bad token fail.
        await call(0, "connect", worker_id="worker-0", epoch=1)
        await call(0, "connect", worker_id="forged-worker", epoch=1, expected=403)
        await call(0, "heartbeat", token="f" * 64, expected=403)
        await rpc(session, authority.endpoint, descs[0], clients[1], "heartbeat", expected=403)
        await call(0, "acquire", row_id=ROWS[1], request_id=uuid4().hex, url=url, expected=403)
        permits = await asyncio.gather(
            *[call(i, "acquire", row_id=ROWS[i], request_id=uuid4().hex, url=url) for i in range(2)]
        )
        assert all(p["state"] == "issued" for p in permits)
        assert await call(0, "acquire", row_id=ROWS[0], request_id=uuid4().hex, url=url) == {"state": "wait"}
        for i in range(2):
            await call(i, "dispatch", permit_id=permits[i]["permit_id"], epoch=1)
        async with session.get(url) as response:
            await response.read()
            assert response.status == 503
        obs = observation(503, 1)
        # Completion arrives before the explicit header message.
        first = await call(0, "complete", permit_id=permits[0]["permit_id"], observation=obs)
        duplicate = await call(0, "complete", permit_id=permits[0]["permit_id"], observation=obs)
        late = await call(0, "headers", permit_id=permits[0]["permit_id"], status=503, retry_after=1)
        assert first["duplicate"] is False and duplicate["duplicate"] and late["duplicate"]
        await call(0, "complete", permit_id=permits[0]["permit_id"], observation=observation(), expected=403)
        assert await call(0, "acquire", row_id=ROWS[0], request_id=uuid4().hex, url=url) == {"state": "wait"}
        before = authority.ledger.export()
        run_id = before["state"]["binding"]["run_id"]
        # Close the first manager and its SQLite handle, then instantiate a fresh
        # authority/socket from the retained journal. This is not a host reboot.
        await authority.stop()
        authority.ledger.close()
        authority = Authority(path / "authority", CONFIG, run_id=run_id, reopen=True)
        await authority.start()
        assert len(authority.ledger.outstanding(authority.ledger.current())) == 1
        await call(0, "acquire", row_id=ROWS[0], request_id=uuid4().hex, url=url, expected=403)
        await call(0, "connect", worker_id="worker-0", epoch=1, expected=403)
        try:
            authority.ledger.recover_closed_epoch()
            raise AssertionError("recovered uncertain epoch")
        except ValueError:
            pass
        # The second simulated dispatch has no origin request; acknowledge that
        # fact explicitly as a cancellation, retaining uncertainty before proof.
        await call(
            1,
            "complete",
            permit_id=permits[1]["permit_id"],
            observation={**observation(), "response_complete": False, "reason": "cancelled"},
        )
        assert len(authority.ledger.outstanding(authority.ledger.current())) == 1
        # Retain this unresolved epoch instead of inventing a drainage proof.
        after = authority.ledger.export()
        assert after["state"]["epoch"] == 1 and after["state"]["phase"] == "fenced"
        (path / "before.json").write_text(json.dumps(before, indent=2))
        (path / "after.json").write_text(json.dumps(after, indent=2))
        (path / "origin-events.json").write_text(json.dumps(origin_events, indent=2))
        results.append(
            dict(
                case="network-protocol",
                passed=True,
                simultaneous_permits=2,
                forged_requests_rejected=4,
                real_origin_requests=1,
                duplicate_and_reordered_messages_checked=True,
                reconnect_checked=True,
                reopened_manager_fenced=True,
                retained_uncertain_permits=1,
            )
        )
    finally:
        await close_resources(authority, session.close, origin_runner.cleanup)


async def backpressure(output, results):
    """Fill all real HTTP handlers, then release them after a genuine 429."""
    path = output / "backpressure"
    path.mkdir(mode=0o700)

    class BusyAuthority(Authority):
        def __init__(self):
            super().__init__(path / "authority", CONFIG)
            self.held = asyncio.Event()
            self.release = asyncio.Event()
            self.refused = 0

        async def request(self, token, message):
            if message["operation"] == "heartbeat" and not self.release.is_set():
                if self.connections == 64:
                    self.held.set()
                await self.release.wait()
            return await super().request(token, message)

        async def handle(self, request):
            response = await super().handle(request)
            if response.status == 429:
                self.refused += 1
                self.release.set()
            return response

    authority = BusyAuthority()
    client = raw_session = None
    requests = []
    try:
        await authority.start()
        authority.enroll(uuid4().hex, ROWS, path / "descriptor.json")
        trace = []
        client = SharedClient(
            dataclasses.replace(CONFIG, shared_control_file=str(path / "descriptor.json")), trace.append
        )
        # Explicit connect keeps the periodic heartbeat from being an extra test
        # participant. All 64 held requests use actual authenticated HTTP.
        client.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3))
        await client.rpc("connect", worker_id=client.worker_id, epoch=1)
        message = dict(
            schema=SCHEMA,
            binding=client.description["binding"],
            client_id=client.client_id,
            operation="heartbeat",
            arguments={},
        )
        raw_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=3), connector=aiohttp.TCPConnector(limit=64)
        )

        async def occupy():
            async with raw_session.post(
                authority.endpoint + "/control",
                json=message,
                headers={"Authorization": "Bearer " + client.description["credential"]},
            ) as response:
                assert response.status == 200
                assert await response.json() == {"status": "open"}

        requests = [asyncio.create_task(occupy()) for _ in range(64)]
        await asyncio.wait_for(authority.held.wait(), 3)
        receipt = await client.rpc("heartbeat")
        await asyncio.gather(*requests)
        assert receipt == {"status": "open"} and client.failure is None
        assert authority.refused >= 1
        assert any(event.get("shared_event") == "backpressure" for event in trace)
        (path / "trace.json").write_text(json.dumps(trace, indent=2))
        results.append(
            dict(
                case="handler-backpressure",
                passed=True,
                held_handlers=64,
                actual_429=authority.refused,
                heartbeat_recovered=True,
            )
        )
    finally:
        authority.release.set()
        for request in requests:
            request.cancel()
        await asyncio.gather(*requests, return_exceptions=True)
        await close_resources(authority, *[item.close for item in (raw_session, client) if item is not None])


async def admission_deadline(output, results):
    """Real HTTP hook waits behind an uncertain permit, bounded by acquisition."""
    path = output / "admission-deadline"
    path.mkdir(mode=0o700)
    config = dataclasses.replace(CONFIG, C_init=1, C_max=1, timeout_sec=1)
    authority = Authority(path / "authority", config)
    client = origin_runner = None
    origin_events = []

    async def origin(request):
        origin_events.append("unexpected arrival")
        return web.Response(body=b"never expected")

    try:
        app = web.Application()
        app.router.add_get("/image", origin)
        origin_runner = web.AppRunner(app, access_log=None)
        await origin_runner.setup()
        site = web.TCPSite(origin_runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/image"
        await authority.start()
        authority.enroll(uuid4().hex, ROWS, path / "descriptor.json")
        trace = []
        client = SharedClient(
            dataclasses.replace(config, shared_control_file=str(path / "descriptor.json")), trace.append
        )
        await client.start()
        permit = await client.rpc("acquire", row_id=ROWS[0], request_id=uuid4().hex, url=url)
        await client.rpc("dispatch", permit_id=permit["permit_id"], epoch=1)
        incomplete = observation(status=None)
        incomplete.update(response_complete=False, reason="cancelled")
        await client.rpc("complete", permit_id=permit["permit_id"], observation=incomplete)
        attempt = RemoteAttempt(client, ROWS[1])
        measurement = {"shared_dispatch": attempt.dispatch, "shared_headers": attempt.headers}
        token = HTTP_TRACE_CTX.set(measurement)
        try:
            async with aiohttp.ClientSession() as session:
                receipt = await asyncio.wait_for(download_via_http_get(session, url, 1), 3)
            assert receipt == (None, 408, "Request Timeout", None)
            assert measurement["failure_kind"] == "admission" and not measurement["latency_eligible"]
            assert not origin_events and attempt.permit is None
        finally:
            HTTP_TRACE_CTX.reset(token)
            await attempt.close(measurement)
        remaining = authority.ledger.outstanding(authority.ledger.current())
        assert len(remaining) == 1 and remaining[0]["state"] == "uncertain"
        assert client.failure is None and authority.failure is None
        try:
            await client.close()
            raise AssertionError("client closed despite its unresolved dispatched permit")
        except SharedControlError:
            pass
        assert client.session is None and authority.failure is None
        after_close = authority.ledger.current()
        assert after_close["clients"][client.client_id]["status"] == "open"
        remaining = authority.ledger.outstanding(after_close)
        assert len(remaining) == 1 and remaining[0]["state"] == "uncertain"
        (path / "trace.json").write_text(json.dumps(trace, indent=2))
        (path / "ledger.json").write_text(json.dumps(authority.ledger.export(), indent=2))
        results.append(
            dict(
                case="admission-deadline",
                passed=True,
                origin_arrivals=0,
                status=408,
                retained_uncertain_permits=1,
                client_close_refused=True,
            )
        )
    finally:
        closers = [client.close] if client is not None else []
        if origin_runner is not None:
            closers.append(origin_runner.cleanup)
        await close_resources(authority, *closers)


async def main(output):
    output.mkdir(mode=0o700, parents=True)
    (output / "environment.json").write_text(json.dumps(environment_record(ROOT), indent=2))
    results = []
    before = sources()
    await lost_ack(output, results, "acquire")
    await lost_ack(output, results, "complete")
    await protocol(output, results)
    await backpressure(output, results)
    await admission_deadline(output, results)
    after = sources()
    assert before == after, "source changed during probe"
    result = dict(
        cases=results,
        sources=before,
        head=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        dirty=subprocess.check_output(["git", "status", "--porcelain"], text=True),
        limitation="Real loopback control transport and retained-ledger re-instantiation; not host reboot, TaskVine, or a complete downloader workload.",
    )
    (output / "result.json").write_text(json.dumps(result, indent=2))
    print(json.dumps({"cases": results, "source_unchanged": True}), flush=True)


async def bounded(output):
    try:
        async with asyncio.timeout(180):
            await main(output)
    except BaseException as exc:
        if output.is_dir() and not (output / "failure.json").exists():
            (output / "failure.json").write_text(
                json.dumps({"status": "failed", "error_type": type(exc).__name__})
            )
        raise


if __name__ == "__main__":
    if not __debug__:
        raise RuntimeError("fixture assertions require normal Python execution")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="Fresh retained evidence directory")
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        parser.error("refusing output collision")
    asyncio.run(bounded(args.output.absolute()))
