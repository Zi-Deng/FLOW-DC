"""Run completion requires control closure, independently of useful byte credit."""

import asyncio
import inspect
import json
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
import flowdc_vine as vine
from flowdc_vine_cohort import cohort
from flowdc_vine_protocol import digest


class VerificationResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_return_verification_keeps_control_socket_responsive(self):
        entered, release = threading.Event(), threading.Event()

        def accept():
            entered.set()
            self.assertTrue(release.wait(2))
            return {"accepted": True}

        async def reply(reader, writer):
            await reader.readexactly(1)
            writer.write(b"x")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(reply, "127.0.0.1", 0)
        verification = asyncio.create_task(vine.accept_return(types.SimpleNamespace(accept=accept)))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            self.assertFalse(verification.done())
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1]), .5)
            writer.write(b"x")
            await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(1), .5), b"x")
            writer.close()
            await writer.wait_closed()
        finally:
            release.set()
            server.close()
            await server.wait_closed()
        self.assertEqual(await verification, {"accepted": True})

    async def test_repeated_cancellation_drains_verifier_before_cleanup(self):
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()

        def accept():
            entered.set()
            release.wait(2)
            finished.set()

        verification = asyncio.create_task(vine.accept_return(types.SimpleNamespace(accept=accept)))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            for _ in range(2):
                verification.cancel()
                await asyncio.sleep(0)
                self.assertFalse(verification.done())
                self.assertFalse(finished.is_set())
        finally:
            release.set()
        with self.assertRaises(asyncio.CancelledError):
            await verification
        self.assertTrue(finished.is_set())


class CompletionTests(unittest.TestCase):
    def result(self, permit="complete", client="closed", shutdown=True):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            pl.DataFrame({"url": ["http://fixture.invalid/image"]}).write_parquet(root / "input.parquet")
            (root / "catalog.json").write_text(
                json.dumps({"http://fixture.invalid/image": {"bytes": 1, "sha256": digest(b"x")}})
            )
            (root / "env.tar.gz").write_bytes(b"environment")
            state = {
                "binding": {},
                "clients": {"old": {"status": client}},
                "permits": {"old": {"state": permit}},
            }

            class Authority:
                def __init__(self, *args):
                    self.ledger = types.SimpleNamespace(
                        current=lambda: state,
                        export=lambda: {"state": state, "events": []},
                        close=lambda: None,
                    )

                async def start(self):
                    pass

                async def stop(self):
                    pass

                def enroll(self, *args, **kwargs):
                    pass

            class Manager:
                def __init__(self, settings):
                    logs = Path(settings["root"]) / "run-info/session"
                    logs.mkdir(parents=True)
                    (logs / "transactions").write_text("1 1 TASK 1 RUNNING worker\n")

                async def start(self):
                    pass

                async def call(self, command, **kwargs):
                    return (
                        {"task_id": 1}
                        if command == "submit"
                        else {
                            "task_id": 1,
                            "stdout": "",
                            "result": "success",
                            "successful": True,
                            "exit_code": 0,
                        }
                    )

                async def close(self):
                    return {"native_manager_exit": 0 if shutdown else -9, "shutdown_receipt": shutdown}

            class Reconciler:
                def __init__(self, *args):
                    self.returns = []

                def accept(self, directory, archive, spec, native, clients):
                    result = {"accepted": True, "native": native, "receipt": {"status": "returned"}}
                    self.returns.append(result)
                    return result

                def summary(self):
                    return {
                        "rows": [{"row_id": "known", "disposition": "verified", "useful_bytes": 1}],
                        "errors": [],
                        "returns": self.returns,
                        "useful_bytes": 1,
                        "verified_rows": 1,
                    }

            runtime = types.ModuleType("ndcctools.taskvine")
            runtime.cvine = types.SimpleNamespace(vine_version_string=lambda: "7.17.2")
            package = types.ModuleType("ndcctools")
            package.taskvine = runtime
            config = {
                "distributed_profile": "shared-origin-v1",
                "workers": 1,
                "original_manifest": str(root / "input.parquet"),
                "catalog": str(root / "catalog.json"),
                "output_directory": str(root / "output"),
                "environment_archive": str(root / "env.tar.gz"),
                "environment_sha256": digest(b"environment"),
                "download": {"control_method": "fixed-v1"},
            }
            options = (
                {"owned_cohort": cohort(1)}
                if "owned_cohort" in inspect.signature(vine.run).parameters
                else {}
            )
            with (
                patch.dict(sys.modules, {"ndcctools": package, "ndcctools.taskvine": runtime}),
                patch.object(vine, "Authority", Authority),
                patch.object(vine, "NativeManager", Manager),
                patch.object(vine, "Reconciler", Reconciler),
            ):
                return asyncio.run(vine.run(config, **options))

    def test_verified_payloads_do_not_hide_uncertain_work_or_failed_shutdown(self):
        for fields in ({"permit": "uncertain"}, {"client": "uncertain"}, {"shutdown": False}):
            with self.subTest(fields=fields):
                result = self.result(**fields)
                self.assertEqual(result["useful_bytes"], 1)
                self.assertFalse(result["run_complete"])
        self.assertTrue(self.result()["run_complete"])
