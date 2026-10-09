#!/usr/bin/env python3
"""Bounded real TaskVine 1/2/4-worker engineering fixtures; never cloud activation."""

import argparse
import asyncio
import json
import signal
import sys
import time
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "bin"))
from flowdc_methods import METHODS
from flowdc_vine import RUNTIME, dispatch_records, file_digest, run  # noqa: E402
from flowdc_vine_cohort import cohort  # noqa: E402
from flowdc_vine_ownership import OwnedWorkers  # noqa: E402
from flowdc_vine_protocol import digest, encode, require, write_new  # noqa: E402

from benchmark.core.controlled_origin import (  # noqa: E402
    ControlledOrigin,
    audit_events,
    public_scenario,
    scenario,
)
from benchmark.core.provenance import environment_record  # noqa: E402
from benchmark.known_truth import image_payloads  # noqa: E402
from benchmark.shared_origin import audit_admission  # noqa: E402

CASES = (
    "primary",
    "worker-loss",
    "preconnect-loss",
    "manager-stop",
    "timeout",
    "partial-artifact",
    "sandbox-exhaustion",
    "forsaken",
)


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
    worker_cohort = cohort(
        args.workers, replacements=(0,) if args.case in ("worker-loss", "preconnect-loss") else ()
    )
    owned.bind_cohort(worker_cohort)
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
                "max_attempts": 2 if args.case in ("worker-loss", "preconnect-loss") else 1,
            }
            write_new(directory / "config.json", config)
            features = [slot["feature"] for slot in worker_cohort["slots"]]

            async def ready(manager, authority, password):
                authority_holder.append(authority)
                password_holder.append(password)
                port_holder.append(manager.port)
                for feature in features:
                    owned.start(manager.port, password, feature)

            async def pulse(manager, authority, tasks):
                owned.capture()
                if args.case == "preconnect-loss" and not injection:
                    task_id = next(iter(tasks))
                    scope = tasks[task_id][1]["scope_id"]
                    dispatched = [
                        e for e in dispatch_records(directory / "manager/run-info") if e["task_id"] == task_id
                    ]
                    if not dispatched:
                        return
                    require(
                        not any(c["scope"] == scope for c in authority.ledger.snapshot()["clients"].values()),
                        "pre-connect injection missed window",
                    )
                    proof = await owned.stop_tree(0)
                    injection.append(
                        {
                            "kind": "preconnect-loss",
                            "task_id": task_id,
                            "scope": scope,
                            "dispatches": dispatched,
                            "proof": proof,
                        }
                    )
                    write_new(directory / "preconnect-loss.json", injection[-1])
                    owned.start(manager.port, password_holder[0], features[0], replacement=True)
                    return
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
                require(
                    origin.requests == origin.responses, "controlled origin did not drain after worker exit"
                )
                drain = {"process_proof": proof, "origin": origin.snapshot()}
                write_new(directory / "worker-loss-origin-drain.json", drain)
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
                    authority.ledger.prove_origin_drained(client, digest(encode(drain)))
                injection.append({"kind": "worker-loss", "clients": list(targets), "proof": proof})
                owned.start(manager.port, password_holder[0], features[0], replacement=True)

            result = await run(
                config,
                ready=ready,
                pulse=pulse,
                owned_cohort=worker_cohort,
                engineering_fault={
                    "partial-artifact": "partial-artifact",
                    "preconnect-loss": "preconnect-pause",
                    "sandbox-exhaustion": "sandbox-exhaustion",
                    "forsaken": "forsaken",
                }.get(args.case),
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
        if args.case in ("primary", "worker-loss", "preconnect-loss"):
            require(result["run_complete"] and result["verified_rows"] == 32, "native acquisition incomplete")
            require(
                len({r["native"]["addrport"] for r in result["returns"]}) == args.workers,
                "selected native workers did not all participate",
            )
        else:
            require(not result["run_complete"], "failure fixture fabricated complete success")
        if args.case in ("worker-loss", "preconnect-loss", "manager-stop"):
            require(bool(injection), "failure was not injected")
        require(result["native_dispatch_bound"]["within_bound"], "native dispatch ceiling exceeded")
        if args.case == "preconnect-loss":
            target = injection[0]["task_id"]
            require(
                sum(e["task_id"] == target for e in result["dispatches"]) >= 2,
                "native redispatch was not observed",
            )
            require(
                sum(c["scope"] == injection[0]["scope"] for c in result["acquisition_attempts"].values())
                == 1,
                "pre-connect loss fabricated an admitted attempt",
            )
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
        native_failure = None
        if args.case in ("sandbox-exhaustion", "forsaken"):
            target = json.loads((directory / "manager/partition-0/submission.json").read_bytes())[
                "native_task_id"
            ]
            count = sum(e["task_id"] == target for e in result["dispatches"])
            require(count == (1 if args.case == "forsaken" else 2), "unexpected native retry count")
            if args.case == "sandbox-exhaustion":
                native = json.loads((directory / "manager/partition-0/native.json").read_bytes())
                require(native["result"] == "sandbox exhaustion", "expected native failure result missing")
                require(any(r["accepted"] for r in result["returns"]), "healthy partition did not verify")
            elif not args.patched_runtime:
                # Upstream 7.17.2 exit_debug_message divides by zero after a
                # first FORSAKEN task. Assert this specific retained failure;
                # a transaction is not a returned native/API task receipt.
                events = []
                for path in (directory / "manager/run-info").rglob("transactions"):
                    for line in path.read_text().splitlines():
                        fields = line.split()
                        if len(fields) >= 6 and fields[2:6] == ["TASK", str(target), "RETRIEVED", "FORSAKEN"]:
                            events.append(line)
                native_failure = json.loads((directory / "manager/native-cleanup.json").read_bytes())
                require(len(events) == 1, "actual native FORSAKEN transaction missing")
                require(
                    native_failure["native_manager_exit"] == -signal.SIGFPE
                    and native_failure["shutdown_receipt"] is False
                    and result["status"] == "failed"
                    and result["error_type"] == "EOFError"
                    and result["native_complete"] is False,
                    "expected pinned native crash was not retained",
                )
                require(
                    not (directory / "manager/partition-0/native.json").exists()
                    and not any(
                        r["scope_id"]
                        == json.loads((directory / "manager/partition-0/submission.json").read_bytes())[
                            "scope_id"
                        ]
                        for r in result["returns"]
                    ),
                    "FORSAKEN crash fabricated a returned task receipt",
                )
                native_failure = {
                    **native_failure,
                    "transaction_events": events,
                    "kind": "upstream-7.17.2-forsaken-sigfpe",
                }
            if args.case == "forsaken" and args.patched_runtime:
                native_failure = json.loads((directory / "manager/native-cleanup.json").read_bytes())
                returned_native = json.loads((directory / "manager/partition-0/native.json").read_bytes())
                require(native_failure["native_manager_exit"] == 0
                        and native_failure["shutdown_receipt"] is True
                        and returned_native["result"] == "forsaken"
                        and returned_native["successful"] is False,
                        "patched native FORSAKEN did not return and shut down normally")
                native_failure = {**native_failure, "kind": "patched-7.17.2-forsaken-returned"}
        assessment = {
            "case": args.case,
            "patched_runtime": args.patched_runtime,
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
            "native_failure": native_failure,
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
        choices=METHODS,
        default="fixed-v1",
    )
    parser.add_argument("--case", choices=CASES, default="primary")
    parser.add_argument("--patched-runtime", action="store_true", help="Assess the disclosed zero-completion guard; record actual manager library hashes separately")
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
