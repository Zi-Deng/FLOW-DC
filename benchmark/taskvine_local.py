#!/usr/bin/env python3
"""Bounded real TaskVine 1/2/4-worker engineering fixtures; never cloud activation."""

import argparse
import asyncio
import ctypes
import json
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path

import polars as pl
import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "bin"))
from flowdc_vine import RUNTIME, file_digest, run  # noqa: E402
from flowdc_vine_protocol import digest, encode, require, write_new  # noqa: E402

from benchmark.core.controlled_origin import (  # noqa: E402
    ControlledOrigin,
    audit_events,
    public_scenario,
    scenario,
)
from benchmark.core.provenance import environment_record  # noqa: E402
from benchmark.known_truth import image_payloads  # noqa: E402
from benchmark.shared_origin import audit_admission, open_pidfd, signal_pidfd  # noqa: E402

CASES = ("primary", "worker-loss", "manager-stop", "timeout", "partial-artifact")


class OwnedWorkers:
    """Linux subreaper + pinned process handles; no unrelated PID/group kills.

    All descendants belong to this dedicated fixture process. Adoption keeps
    detached task children observable after a native worker dies. PID reuse cannot
    retarget a signal, and only owned child handles are reaped.
    """

    def __init__(self, directory, executable, count):
        self.directory, self.executable, self.count = directory, executable, count
        self.roots, self.handles, self.logs, self.exits = [], {}, [], []
        self.stopping = False
        self.excluded, self.retired, self.unknown = set(), [], set()
        libc = ctypes.CDLL(None, use_errno=True)
        require(libc.prctl(36, 1, 0, 0, 0) == 0, "local fixture requires child subreaper")
        fd = open_pidfd(os.getpid())
        os.close(fd)

    def capture(self):
        unknown = set()
        for process in psutil.Process().children(recursive=True):
            try:
                created = process.create_time()
                identity = (process.pid, created)
                if identity in self.excluded:
                    continue
                old = self.handles.get(process.pid)
                if old is not None and old["created"] == created:
                    continue
                if old is not None:
                    require(self.dead(old), "live child identity changed")
                    self.retired.append((process.pid, old))
                ancestors = {(p.pid, p.create_time()) for p in process.parents()}
                owner = next(
                    (
                        i
                        for i, root in enumerate(self.roots)
                        if identity == root.flowdc_identity or root.flowdc_identity in ancestors
                    ),
                    None,
                )
                if owner is None:
                    # Never signal/reap an unrelated manager/helper. An adopted
                    # child that was not observed under a worker is uncertainty,
                    # not evidence that a selected worker's tree has stopped.
                    unknown.add(identity)
                    continue
                fd = open_pidfd(process.pid)
                if process.create_time() != created:
                    os.close(fd)
                    raise ValueError("owned child changed identity")
                self.handles[process.pid] = {"fd": fd, "created": created, "worker": owner}
            except (psutil.NoSuchProcess, ProcessLookupError):
                continue
        self.unknown = unknown

    @staticmethod
    def dead(record):
        return bool(select.select([record["fd"]], [], [], 0)[0])

    async def watch(self):
        while not self.stopping:
            self.capture()
            await asyncio.sleep(0.025)

    def start(self, port, password, feature, *, replacement=False):
        # Replacement is allowed only after the earlier root/tree was stopped.
        require(
            sum(not self.dead(self.handles[p.pid]) for p in self.roots) < self.count,
            "worker process bound exceeded",
        )
        index = len(self.roots)
        log = (self.directory / f"worker-{index}.log").open("xb")
        self.logs.append(log)
        command = [
            str(self.executable),
            "--ssl",
            "-P",
            str(password),
            "--single-shot",
            "--parent-death",
            "--connect-timeout=20",
            "--idle-timeout=20",
            "--wall-time=170",
            "--cores=1",
            "--memory=2048",
            "--disk=8192",
            "--gpus=0",
            "--feature",
            feature,
            "--workspace",
            str(self.directory / f"scratch-{index}"),
            "--keep-workspace",
            "127.0.0.1",
            str(port),
        ]
        if not self.roots:
            # The native manager and spawn helper already exist before ready().
            self.excluded = {(p.pid, p.create_time()) for p in psutil.Process().children(recursive=True)}
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        process.flowdc_identity = (process.pid, psutil.Process(process.pid).create_time())
        self.roots.append(process)
        self.capture()
        write_new(
            self.directory / f"worker-{index}.json",
            {
                "command": command,
                "pid": process.pid,
                "create_time": self.handles[process.pid]["created"],
                "replacement": replacement,
            },
        )

    async def stop_tree(self, index=None):
        selected, stable = {}, 0
        for sig, seconds in ((signal.SIGTERM, 8), (signal.SIGKILL, 5)):
            until = time.monotonic() + seconds
            while time.monotonic() < until:
                self.capture()
                for pid, record in [*self.retired, *self.handles.items()]:
                    if index is None or record["worker"] == index:
                        selected[(pid, record["created"])] = record
                for record in selected.values():
                    if not self.dead(record):
                        try:
                            signal_pidfd(record["fd"], sig)
                        except ProcessLookupError:
                            pass
                stable = stable + 1 if all(self.dead(r) for r in selected.values()) else 0
                if stable >= 3:
                    require(not self.unknown, "unattributed adopted child prevents quiescence proof")
                    return {
                        "pids": sorted({pid for pid, _ in selected}),
                        "all_stopped": True,
                        "identities": [{"pid": pid, "created": created} for pid, created in sorted(selected)],
                    }
                await asyncio.sleep(0.025)
        raise ValueError("owned process tree failed to stop")

    async def close(self):
        evidence = await self.stop_tree()
        self.stopping = True
        for process in self.roots:
            self.exits.append({"pid": process.pid, "exit_code": process.wait(timeout=1)})
        # Reap adopted descendants only; ordinary Popen roots were reaped above.
        for pid in self.handles:
            if any(p.pid == pid for p in self.roots):
                continue
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
        for record in [*self.handles.values(), *(record for _, record in self.retired)]:
            os.close(record["fd"])
        for stream in self.logs:
            stream.close()
        return {**evidence, "workers": self.exits}


async def fixture(args):
    import ndcctools.taskvine as vine

    require(vine.__version__ == RUNTIME, "native fixture requires matching pinned runtime")
    directory = Path(args.output).absolute()
    require(not directory.exists(), "refusing fixture collision")
    environment = environment_record(ROOT)
    executable = Path(sys.executable).parent / "vine_worker"
    require(executable.is_file(), "official vine_worker absent from active environment")
    environment["vine_worker_sha256"] = file_digest(executable)
    environment["portable_environment_sha256"] = file_digest(args.environment)
    directory.mkdir(mode=0o700, parents=True)
    write_new(directory / "environment.json", environment)
    payloads = image_payloads()
    plan = scenario("steady", payloads, rows=32)
    plan["schedule"] = [[0, 2]]
    for item in plan["objects"].values():
        item["service_s"] = 0.25 if args.case in ("worker-loss", "manager-stop", "timeout") else 0.06
    write_new(directory / "scenario.json", public_scenario(plan))
    origin_path = directory / "origin"
    origin_path.mkdir()
    owned = OwnedWorkers(directory, executable, args.workers)
    watch = asyncio.create_task(owned.watch())
    authority_holder, injection = [], []
    password_holder, port_holder = [], []
    cleanup = None
    try:
        with ControlledOrigin(origin_path, plan) as origin:
            paths = list(plan["objects"])
            pl.DataFrame(
                {
                    "url": [origin.base_url + paths[i % len(paths)] for i in range(32)]
                    + [None, "", "invalid"],
                    "label": [f"original-{i}" for i in range(35)],
                }
            ).write_parquet(directory / "input.parquet")
            catalog = {
                origin.base_url + path: {"bytes": len(spec["payload"]), "sha256": digest(spec["payload"])}
                for path, spec in plan["objects"].items()
            }
            write_new(directory / "catalog.json", catalog)
            options = {
                "control_method": args.method,
                "C_min": 1,
                "C_init": 2,
                "C_max": 2,
                "concurrent_downloads": 2,
                "timeout_sec": 5,
                "max_retry_attempts": 1,
            }
            config = {
                "distributed_profile": "shared-origin-v1",
                "original_manifest": str(directory / "input.parquet"),
                "catalog": str(directory / "catalog.json"),
                "output_directory": str(directory / "manager"),
                "environment_archive": str(Path(args.environment).resolve()),
                "environment_sha256": environment["portable_environment_sha256"],
                "workers": args.workers,
                "download": options,
                "deadline_s": 170,
                "task_deadline_s": 1 if args.case == "timeout" else 110,
                "max_attempts": 2 if args.case == "worker-loss" else 1,
            }
            write_new(directory / "config.json", config)
            features = [f"flowdc-local-{i}" for i in range(args.workers)]

            async def ready(manager, authority, password):
                authority_holder.append(authority)
                password_holder.append(password)
                port_holder.append(manager.port)
                for feature in features:
                    owned.start(manager.port, password, feature)

            async def pulse(manager, authority, tasks):
                owned.capture()
                if args.case not in ("worker-loss", "manager-stop") or injection or origin.requests < 2:
                    return
                if args.case == "manager-stop":
                    await authority.stop()
                    injection.append({"kind": "manager-stop"})
                    return
                state = authority.ledger.snapshot()
                targets = {
                    client: int(info["worker_id"].removeprefix("vine-"))
                    for client, info in state["clients"].items()
                    if info["worker_id"].startswith("vine-")
                }
                targets = {
                    client: pid
                    for client, pid in targets.items()
                    if pid in owned.handles and owned.handles[pid]["worker"] == 0
                }
                if not targets:
                    return
                proof = await owned.stop_tree(0)
                write_new(directory / "worker-loss-proof.json", proof)
                # The origin independently closes the disconnected exchanges before
                # this controlled fixture admits replacement work.
                until = time.monotonic() + 3
                while time.monotonic() < until and origin.requests > origin.responses:
                    await asyncio.sleep(0.05)
                for client in targets:
                    authority.ledger.prove_quiescent(
                        client,
                        {
                            "kind": "owned_process_tree_exit",
                            "client_id": client,
                            "run_id": state["binding"]["run_id"],
                            "source_sha256": state["binding"]["source_sha256"],
                            "evidence_sha256": digest(encode(proof)),
                        },
                    )
                injection.append({"kind": "worker-loss", "clients": list(targets), "proof": proof})
                owned.start(manager.port, password_holder[0], features[0], replacement=True)

            result = await run(
                config,
                ready=ready,
                pulse=pulse,
                worker_features=features,
                engineering_fault="partial-artifact" if args.case == "partial-artifact" else None,
            )
            cleanup = await owned.close()
            write_new(directory / "cleanup.json", cleanup)
            snapshot = origin.snapshot()
        events = [json.loads(line) for line in (origin_path / "origin.jsonl").read_text().splitlines()]
        origin_audit = audit_events(events, snapshot)
        require(
            origin_audit["peak_open_requests"] <= 2 and origin_audit["unclosed_requests"] == 0,
            "independent origin aggregate bound violated",
        )
        control = json.loads((directory / "manager/control.json").read_text())
        admission = audit_admission(control["events"])
        require(result["original_rows"] == 35 and cleanup["all_stopped"], "denominator/cleanup invariant")
        if args.case in ("primary", "worker-loss"):
            require(result["run_complete"] and result["verified_rows"] == 32, "native acquisition incomplete")
            require(
                len({r["native"]["addrport"] for r in result["returns"]}) == args.workers,
                "selected native workers did not all participate",
            )
        else:
            require(not result["run_complete"], "failure fixture fabricated complete success")
        if args.case in ("worker-loss", "manager-stop"):
            require(bool(injection), "failure was not injected")
        receipts = [
            json.loads(p.read_bytes())
            for p in (directory / "manager").glob("partition-*/returned/receipt.json")
        ]
        if args.case == "timeout":
            require(
                len(receipts) == args.workers
                and all(r.get("error_type") == "TimeoutError" for r in receipts),
                "expected task timeout receipts missing",
            )
        if args.case == "manager-stop":
            require(
                receipts
                and all(r.get("error_type") in ("SharedControlError", "CancelledError") for r in receipts),
                "expected manager-loss receipts missing",
            )
        if args.case == "partial-artifact":
            require(
                any(
                    r.get("engineering_fault") == "partial-artifact" and r.get("error_type") is None
                    for r in receipts
                ),
                "expected partial-artifact receipt missing",
            )
            require(any(r["accepted"] for r in result["returns"]), "healthy partition did not verify")
        assessment = {
            "case": args.case,
            "workers": args.workers,
            "status": "passed",
            "origin": origin_audit,
            "admission": admission,
            "cleanup": cleanup,
            "injection": injection,
            "original_rows": result["original_rows"],
            "verified_rows": result["verified_rows"],
            "useful_bytes": result["useful_bytes"],
            "run_complete": result["run_complete"],
        }
        write_new(directory / "assessment.json", assessment)
        return assessment
    finally:
        try:
            if cleanup is None:
                try:
                    cleanup = await owned.close()
                finally:
                    if not (directory / "cleanup.json").exists():
                        write_new(directory / "cleanup.json", cleanup or {"all_stopped": False})
        finally:
            owned.stopping = True
            await watch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--environment", required=True, help="Pinned TaskVine 7.17.2 portable environment archive"
    )
    parser.add_argument("--workers", type=int, choices=(1, 2, 4), required=True)
    parser.add_argument(
        "--method",
        choices=("paarc-base-v2", "gradient-candidate-v1", "fixed-v1", "ratio-v1"),
        default="fixed-v1",
    )
    parser.add_argument("--case", choices=CASES, default="primary")
    args = parser.parse_args()
    try:
        print(json.dumps(asyncio.run(fixture(args))))
        return 0
    except BaseException as exc:
        path = Path(args.output)
        if path.is_dir() and not (path / "failure.json").exists():
            write_new(path / "failure.json", {"error_type": type(exc).__name__, "detail": str(exc)})
        print(f"Native fixture failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
