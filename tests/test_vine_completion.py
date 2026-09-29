"""Run completion requires control closure, independently of useful byte credit."""

import asyncio
import inspect
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
import flowdc_vine as vine
from flowdc_vine_cohort import cohort
from flowdc_vine_protocol import digest


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
