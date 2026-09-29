#!/usr/bin/env python3
"""Bounded real manager-process SIGKILL/restart and retained-epoch fixture."""

import argparse
import asyncio
import hashlib
import json
import os
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
from flowdc_shared import Authority  # noqa: E402
from flowdc_shared_state import SCHEMA, Ledger, read_private  # noqa: E402
from flowdc_staging import DOWNLOAD_FILES  # noqa: E402

from benchmark.core.provenance import environment_record  # noqa: E402

CONFIG = Config("input", "output", control_method="fixed-v1", C_min=1, C_init=1, C_max=1)
ROW = "1" * 64


async def serve(path, phase):
    reopen = phase == "restarted"
    desc = read_private(path / "descriptor.json") if reopen else None
    authority = Authority(
        path / "authority", CONFIG, run_id=desc["binding"]["run_id"] if reopen else None, reopen=reopen
    )
    try:
        await authority.start()
        if not reopen:
            authority.enroll(uuid4().hex, [ROW], path / "descriptor.json")
        ready = path / (phase + "-ready.json")
        temporary = path / (phase + "-ready.tmp")
        temporary.write_text(json.dumps({"endpoint": authority.endpoint, "pid": os.getpid()}))
        temporary.replace(ready)
        async with asyncio.timeout(180):
            await asyncio.Event().wait()
    finally:
        await authority.stop()
        authority.ledger.close()


async def main(path):
    path.mkdir(mode=0o700, parents=True)
    (path / "environment.json").write_text(json.dumps(environment_record(ROOT), indent=2))
    processes = []
    logs = []
    received = asyncio.Event()
    release = asyncio.Event()
    events = []

    async def hold(request):
        events.append({"event": "arrival"})
        received.set()
        await release.wait()
        events.append({"event": "response"})
        return web.Response(body=b"controlled payload")

    app = web.Application()
    app.router.add_get("/hold", hold)
    runner = web.AppRunner(app, shutdown_timeout=2)
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    source_before = {n: hashlib.sha256((ROOT / "bin" / n).read_bytes()).hexdigest() for n in DOWNLOAD_FILES}

    async def launch(phase):
        log = (path / (phase + ".log")).open("xb")
        logs.append(log)
        process = subprocess.Popen(
            [sys.executable, "-B", __file__, "--serve", str(path), phase],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        processes.append(process)
        for _ in range(200):
            ready = path / (phase + "-ready.json")
            if ready.exists():
                return process, json.loads(ready.read_text())["endpoint"]
            assert process.poll() is None, "manager startup failed"
            await asyncio.sleep(0.05)
        raise TimeoutError("manager startup")

    fetch = None
    try:
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/hold"
        first, endpoint = await launch("initial")
        desc = read_private(path / "descriptor.json")
        client = uuid4().hex

        async def rpc(operation, expected=200, **arguments):
            message = dict(
                schema=SCHEMA,
                binding=desc["binding"],
                client_id=client,
                operation=operation,
                arguments=arguments,
            )
            async with session.post(
                endpoint + "/control", json=message, headers={"Authorization": "Bearer " + desc["credential"]}
            ) as response:
                data = await response.json()
                assert response.status == expected, (operation, response.status, data)
                return data

        await rpc("connect", worker_id="process-restart-client", epoch=1)
        permit = await rpc("acquire", row_id=ROW, request_id=uuid4().hex, url=url)
        await rpc("dispatch", permit_id=permit["permit_id"], epoch=1)

        async def acquire_origin():
            async with session.get(url) as response:
                body = await response.read()
                return response.status, body

        fetch = asyncio.create_task(acquire_origin())
        await asyncio.wait_for(received.wait(), 2)
        first.kill()
        assert first.wait(timeout=3) == -9
        second, endpoint = await launch("restarted")
        assert not fetch.done() and events == [{"event": "arrival"}]
        await rpc("heartbeat", expected=403)
        await rpc("connect", expected=403, worker_id="process-restart-client", epoch=1)
        await rpc("acquire", expected=403, row_id=ROW, request_id=uuid4().hex, url=url)
        await rpc("close", expected=403)
        release.set()
        status, body = await fetch
        assert status == 200 and body == b"controlled payload"
        observation = dict(
            status=200,
            retry_after=None,
            ttfb=None,
            body_bytes=len(body),
            latency_eligible=False,
            body_complete=True,
            response_complete=True,
            is_conn_error=False,
            is_local_error=False,
            is_unknown_error=False,
            reason="final",
        )
        result = await rpc("complete", permit_id=permit["permit_id"], observation=observation)
        assert result["duplicate"] is False
        await rpc("close")
        second.kill()
        assert second.wait(timeout=3) == -9
        ledger = Ledger(path / "authority", desc["binding"], reopen=True)
        try:
            export = ledger.export()
            assert not ledger.outstanding(ledger.current())
            assert len(export["state"]["permits"]) == 1
            assert export["state"]["phase"] == "fenced"
            assert export["state"]["clients"][client]["status"] == "closed"
            (path / "ledger-export.json").write_text(json.dumps(export, indent=2))
        finally:
            ledger.close()
        source_after = {
            n: hashlib.sha256((ROOT / "bin" / n).read_bytes()).hexdigest() for n in DOWNLOAD_FILES
        }
        assert source_before == source_after
        result = dict(
            passed=True,
            origin_events=events,
            actual_manager_processes=2,
            manager_exit_codes=[p.returncode for p in processes],
            rejected_operations_while_origin_inflight=4,
            verified_response_bytes=len(body),
            outstanding_after_late_completion=0,
            recovered_automatically=False,
            source_sha256=source_before,
            head=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
            dirty=subprocess.check_output(["git", "status", "--porcelain"], text=True),
        )
        (path / "result.json").write_text(json.dumps(result, indent=2))
        print(json.dumps({k: v for k, v in result.items() if k not in ("source_sha256", "dirty")}))
    finally:
        release.set()
        if fetch is not None:
            await asyncio.gather(fetch, return_exceptions=True)
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)
        for log in logs:
            log.close()
        await session.close()
        await runner.cleanup()


async def bounded(path):
    try:
        async with asyncio.timeout(180):
            await main(path)
    except BaseException as exc:
        if path.is_dir() and not (path / "failure.json").exists():
            (path / "failure.json").write_text(
                json.dumps({"status": "failed", "error_type": type(exc).__name__})
            )
        raise


if __name__ == "__main__":
    if not __debug__:
        raise RuntimeError("fixture assertions require normal Python execution")
    if len(sys.argv) == 4 and sys.argv[1] == "--serve":
        asyncio.run(serve(Path(sys.argv[2]), sys.argv[3]))
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--output", required=True, type=Path, help="Fresh retained evidence directory")
        args = parser.parse_args()
        if args.output.exists() or args.output.is_symlink():
            parser.error("refusing output collision")
        asyncio.run(bounded(args.output.absolute()))
