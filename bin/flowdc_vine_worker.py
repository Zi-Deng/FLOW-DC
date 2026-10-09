#!/usr/bin/env python3
"""Source-bound native TaskVine task; acquisition stays in this one process."""

import argparse
import asyncio
import contextlib
import importlib
import os
import signal
import sys
import time
from pathlib import Path
from uuid import uuid4

import download_batch as download
from flowdc_shared import SharedClient, descriptor, runtime_binding
from flowdc_staging import WORKER_FILES
from flowdc_vine_protocol import digest, pack_return, parse, require, write_new


def runtime_record():
    from ndcctools.taskvine import __version__

    require(__version__ == "7.17.2", "worker runtime must match pinned TaskVine 7.17.2")
    modules = {}
    prefix = Path(sys.prefix).resolve()
    for name in ("aiohttp", "polars", "psutil", "tqdm"):
        module = importlib.import_module(name)
        path = Path(module.__file__).resolve()
        require(path.is_relative_to(prefix), "dependency escaped staged environment")
        modules[name] = str(path.relative_to(prefix))
    return {"python": sys.version, "prefix": str(prefix), "executable": sys.executable, "modules": modules}


async def execute(spec):
    require(spec["schema"] == "flowdc-vine-task-v1", "invalid native task schema")
    require(
        spec["files"] == {name: digest(Path(name).read_bytes()) for name in WORKER_FILES},
        "staged source closure mismatch",
    )
    require(
        digest(Path("partition.parquet").read_bytes()) == spec["partition_sha256"],
        "staged partition mismatch",
    )
    config = download.normalize_config(download.Config(**spec["download"]))
    require(
        config.input_path == "partition.parquet"
        and config.output_folder == "return/native"
        and config.shared_control_file == "control-private.json"
        and not config.force_overwrite
        and config.research_profile,
        "invalid native task paths/profile",
    )
    description = descriptor(config)
    require(description["binding"] == spec["binding"], "task/session binding mismatch")
    ca_pem = description.get("ca_pem")
    require(
        (digest(ca_pem.encode("ascii")) if ca_pem is not None else None) == spec.get("control_ca_sha256"),
        "task control trust mismatch",
    )
    require(
        runtime_binding(config) == {k: spec["binding"][k] for k in runtime_binding(config)},
        "effective task binding mismatch",
    )
    frame, _ = download.load_manifest(config)  # Metadata guard before any output or HTTP.
    require(sorted(frame["__key__"].to_list()) == spec["row_ids"], "logical partition mismatch")
    runtime = runtime_record()
    attempt = uuid4().hex
    root = Path("return")
    root.mkdir(exist_ok=False)
    identity = {
        "schema": "flowdc-vine-attempt-v1",
        "attempt_id": attempt,
        "scope_id": spec["scope_id"],
        "binding": spec["binding"],
        "partition_sha256": spec["partition_sha256"],
        "row_ids": spec["row_ids"],
        "source_files": spec["files"],
        "environment_sha256": spec["environment_sha256"],
        "runtime": runtime,
        "pid": os.getpid(),
    }
    write_new(root / "identity.json", identity)
    started, status, error = time.monotonic_ns(), "failed", None
    client = None

    def factory(cfg, emit):
        nonlocal client
        client = SharedClient(cfg, emit)
        client.client_id = attempt
        client.worker_id = f"vine-{os.getpid()}"
        return client

    current = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, current.cancel)
    try:
        if spec.get("engineering_fault") == "preconnect-pause":
            # Bounded local fault gate: native execution exists before admission.
            await asyncio.sleep(10)
        if spec.get("engineering_fault") == "sandbox-exhaustion":
            (root / "bounded-disk-fault").write_bytes(b"x" * (4 * 1024 * 1024))
            await asyncio.sleep(12)  # Native 5s disk check, within the task deadline.
        with (root / "stdout.txt").open("x") as out, (root / "stderr.txt").open("x") as err:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                await asyncio.wait_for(
                    download.run_acquisition(config, shared_client_factory=factory),
                    timeout=spec["deadline_s"],
                )
        status = "returned"
        if spec.get("engineering_fault") == "partial-artifact":
            archive = root / "native.tar"
            with archive.open("r+b") as stream:
                stream.truncate(512)
    except BaseException as exc:
        error = type(exc).__name__
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(signum)
        write_new(
            root / "receipt.json",
            {
                "schema": "flowdc-vine-receipt-v1",
                "attempt_id": attempt,
                "scope_id": spec["scope_id"],
                "status": status,
                "error_type": error,
                "elapsed_ns": time.monotonic_ns() - started,
                "control_client_closed": client is not None and client.session is None,
                "engineering_fault": spec.get("engineering_fault"),
            },
        )
        pack_return(root, "return.tar", research_workload=config.research_workload)
    return 0 if status == "returned" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", required=True)
    args = parser.parse_args()
    try:
        return asyncio.run(execute(parse(Path(args.spec).read_bytes())))
    except BaseException as exc:
        print(f"Task preflight/return failed: {type(exc).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
