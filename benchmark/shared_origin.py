#!/usr/bin/env python3
"""Real bounded concurrent FLOW-DC clients using one authenticated authority."""

import argparse
import asyncio
import copy
import ctypes
import dataclasses
import json
import os
import signal
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import polars as pl
import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "bin"))

from download_batch import Config, normalize_config  # noqa: E402
from flowdc_shared import Authority  # noqa: E402

from benchmark.core.controlled_origin import (  # noqa: E402
    ControlledOrigin,
    audit_events,
    public_scenario,
    scenario,
)
from benchmark.core.lifecycle import _group_running, interruption_signals, run_verified  # noqa: E402
from benchmark.core.provenance import environment_record  # noqa: E402
from benchmark.core.study import write_new  # noqa: E402
from benchmark.core.truth import PROVENANCE, Truth, digest, parse, partition_truth, require  # noqa: E402
from benchmark.core.verifier import verify_native  # noqa: E402
from benchmark.known_truth import image_payloads  # noqa: E402


def audit_admission(events):
    """Independent replay of issued capacity and manager-clock embargoes."""
    origins, permits = {}, {}
    peak = 0
    for sequence, event in enumerate(events, 1):
        require(event["sequence"] == sequence, "admission event sequence gap")
        action, now = event["action"], event["manager_monotonic_s"]
        if action == "limit":
            entry = origins.setdefault(event["origin"], {"active": set(), "embargo": 0})
            entry["limit"] = event["limit"]
        elif action == "acquire":
            key, permit = event["origin"], event["permit_id"]
            entry = origins[key]
            require(
                permit not in permits and len(entry["active"]) < entry["limit"] and now >= entry["embargo"],
                "aggregate admission violated",
            )
            permits[permit] = {
                "origin": key,
                "client_id": event["client_id"],
                "dispatched": False,
                "headers": False,
                "complete": False,
                "quiescent": False,
            }
            entry["active"].add(permit)
            peak = max(peak, len(entry["active"]))
        elif action in ("dispatch", "headers", "complete"):
            permit = event["permit_id"]
            work = permits[permit]
            entry = origins[work["origin"]]
            require(
                not work["complete"] or (work["quiescent"] and action in ("complete", "headers")),
                "event after completed permit",
            )
            if action == "dispatch":
                require(not work["dispatched"] and now >= entry["embargo"], "embargo/replay violation")
                work["dispatched"] = True
            else:
                observation = event["observation"]
                if not work["headers"] and observation.get("status") is not None:
                    require(work["dispatched"], "headers without dispatch")
                    delay = observation.get("retry_after")
                    if delay is not None:
                        entry["embargo"] = max(entry["embargo"], now + delay)
                    work["headers"] = True
                if action == "complete":
                    work["complete"] = (
                        not work["dispatched"]
                        or observation.get("response_complete") is True
                        or work["quiescent"]
                    )
                    if work["complete"]:
                        entry["active"].discard(permit)
        elif action == "prove_quiescent":
            for permit, work in permits.items():
                if (
                    work["client_id"] == event["client_id"]
                    and not work["complete"]
                    and not work["dispatched"]
                ):
                    work.update(complete=True, quiescent=True)
                    origins[work["origin"]]["active"].discard(permit)
        elif action == "origin_drained":
            for permit, work in permits.items():
                if work["client_id"] == event["client_id"]:
                    work.update(complete=True, quiescent=True)
                    origins[work["origin"]]["active"].discard(permit)
        elif action in ("restart_fence", "recover_closed_epoch"):
            raise ValueError("this single-epoch audit cannot reinterpret restart clocks")
    return {
        "single_epoch_valid": True,
        "peak_issued_per_origin": peak,
        "outstanding_permits": sum(len(entry["active"]) for entry in origins.values()),
    }


def native_config(config):
    values = dataclasses.asdict(config)
    rename = {
        "input_path": "input",
        "output_folder": "output",
        "url_col": "url",
        "label_col": "label",
        "timeout_sec": "timeout",
    }
    return {rename.get(key, key): value for key, value in values.items()}


def _pidfd_call(name, signature, *args):
    """Some conda CPython builds omit pidfd wrappers; use the same libc primitive.

    Missing libc/kernel support fails explicitly, never downgrades to PID signals.
    """
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, name, None)
    require(function is not None, "worker-loss fixture requires Linux libc pidfd support")
    function.argtypes, function.restype = signature, ctypes.c_int
    result = function(*args)
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return result


def open_pidfd(pid):
    function = getattr(os, "pidfd_open", None)
    return function(pid) if function else _pidfd_call("pidfd_open", [ctypes.c_int, ctypes.c_uint], pid, 0)


def signal_pidfd(fd, signum):
    function = getattr(signal, "pidfd_send_signal", None)
    if function:
        function(fd, signum)
    else:
        _pidfd_call(
            "pidfd_send_signal",
            [ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint],
            fd,
            signum,
            None,
            0,
        )


async def inject_failure(directory, case, authority, futures, events, interruption):
    if case in ("primary", "reciprocal-redirect", "retry-after"):
        return
    until = time.monotonic() + 30
    while time.monotonic() < until and not all(future.done() for future in futures):
        if interruption["requested"]:
            return
        if sum(event["phase"] == "arrival" for event in events) >= 4:
            if case == "manager-stop":
                await authority.stop()
                return
            owner_file = directory / "client-0/run/process-owner.json"
            if owner_file.exists():
                owner = parse(owner_file.read_bytes())
                rows = {
                    row["row_id"]
                    for row in parse((directory / "client-0/partition-truth.json").read_bytes())["rows"]
                }
                outstanding = [
                    permit["permit_id"]
                    for permit in authority.ledger.outstanding(authority.ledger.current())
                    if permit["row_id"] in rows and permit["state"] == "dispatched"
                ]
                if not outstanding:
                    await asyncio.sleep(0.01)
                    continue  # Other workers' arrivals do not prove this worker owns remote work.
                # Open a kernel process handle before checking retained identity;
                # PID reuse afterward cannot redirect this fixture's signal.
                try:
                    fd = open_pidfd(owner["pid"])
                except ProcessLookupError:
                    break
                try:
                    process = psutil.Process(owner["pid"])
                    require(process.create_time() == owner["create_time"], "owned child identity changed")
                    write_new(
                        directory / "failure-injection.json",
                        {"case": case, "owner": owner, "outstanding_dispatched_permits": outstanding},
                    )
                    signal_pidfd(fd, signal.SIGKILL)
                finally:
                    os.close(fd)
                return
        await asyncio.sleep(0.01)
    raise ValueError("failure injection did not observe real in-flight work")


async def run(directory, workers, method, case):
    # The event-loop thread owns signals for all child lifecycles. Requests are
    # observed by each worker's bounded wait, without cancelling to_thread futures.
    with interruption_signals(raise_on_signal=False) as interruption:
        return await _run(directory, workers, method, case, interruption)


async def _run(directory, workers, method, case, interruption):
    require(not directory.exists() and not directory.is_symlink(), "shared smoke output collision")
    environment = environment_record(ROOT)
    directory.mkdir(parents=True, exist_ok=False)
    write_new(directory / "environment.json", environment)
    payloads = image_payloads()
    plan = scenario("overload" if case == "retry-after" else "steady", payloads, rows=32)
    # Long enough to inject a loss during actual work, still a tiny bounded case.
    for item in plan["objects"].values():
        item["service_s"] = 0.06
    settings = normalize_config(
        Config(
            "unused",
            "unused",
            control_method=method,
            method_options={"sample_window_s": 1.5}
            if method in ("gradient-candidate-v1", "ratio-v1")
            else {},
            C_min=1,
            C_init=2,
            C_max=2,
            concurrent_downloads=2,
            timeout_sec=5,
            max_retry_attempts=2 if case == "retry-after" else 1,
            research_profile=True,
        )
    )
    authority = Authority(directory / "manager-private", settings)
    results, futures = [], []
    started = time.monotonic_ns()
    try:
        await authority.start()
        with ExitStack() as origins:
            servers = []
            for i in range(2 if case == "reciprocal-redirect" else 1):
                origin_dir = directory / f"origin-{i}"
                origin_dir.mkdir()
                servers.append(origins.enter_context(ControlledOrigin(origin_dir, copy.deepcopy(plan))))
            paths = list(plan["objects"])
            if case == "reciprocal-redirect":
                for i, server in enumerate(servers):
                    server.plan["objects"][paths[0]]["payload"] = payloads["PNG"]
                    server.plan["objects"][paths[0]]["responses"] = [
                        {"status": 302, "location": servers[1 - i].base_url + paths[1]}
                    ]
            for i, server in enumerate(servers):
                write_new(directory / f"origin-{i}/scenario.json", public_scenario(server.plan))
            frame = pl.DataFrame(
                {
                    "url": [
                        servers[i % len(servers)].base_url
                        + paths[0 if case == "reciprocal-redirect" else i % len(paths)]
                        for i in range(32)
                    ],
                    "label": [f"logical-row-{i}" for i in range(32)],
                }
            )
            frame.write_parquet(directory / "original.parquet")
            catalog = {
                server.base_url + path: {"bytes": len(spec["payload"]), "sha256": digest(spec["payload"])}
                for server in servers
                for path, spec in server.plan["objects"].items()
            }
            truth = Truth.load(directory / "original.parquet", catalog)
            eligible = pl.read_parquet(truth.write(directory / "fixture"))
            preparations = []
            for index in range(workers):
                client_dir = directory / f"client-{index}"
                client_dir.mkdir()
                part = eligible.slice(
                    index * 32 // workers, (index + 1) * 32 // workers - index * 32 // workers
                )
                part_path = client_dir / "partition.parquet"
                part.write_parquet(part_path)
                ids = set(part[PROVENANCE[3]].to_list())
                selected_truth = partition_truth(truth.record, ids)
                write_new(client_dir / "partition-truth.json", selected_truth)
                private = client_dir / "control-private.json"
                authority.enroll(uuid4().hex, sorted(ids), private, attempts=1)
                output = client_dir / "run/native"
                config = dataclasses.replace(
                    settings,
                    input_path=str(part_path),
                    output_folder=str(output),
                    shared_control_file=str(private),
                )
                config_path = client_dir / "config.json"
                write_new(config_path, native_config(config))
                preparations.append((client_dir, output, config_path, selected_truth))
            started = time.monotonic_ns()  # Common launch-through-manager verification boundary.
            for client_dir, output, config_path, selected_truth in preparations:
                futures.append(
                    asyncio.create_task(
                        asyncio.to_thread(
                            run_verified,
                            [
                                sys.executable,
                                "-B",
                                str(ROOT / "bin/download_batch.py"),
                                "--config",
                                str(config_path),
                            ],
                            client_dir / "run",
                            selected_truth,
                            lambda output=output, selected_truth=selected_truth: verify_native(
                                "flowdc", output, selected_truth
                            ),
                            cwd=ROOT,
                            deadline=120,
                            cleanup=20,
                            interruption=interruption,
                            provenance={
                                "scope": "partition",
                                "parent_original_rows": truth.record["original_rows"],
                                "method": method,
                            },
                        )
                    )
                )
            await inject_failure(directory, case, authority, futures, servers[0].events, interruption)
            results = await asyncio.shield(asyncio.gather(*futures))
        origin_work = [server.snapshot() for server in servers]
        origin_audit = [
            audit_events(server.events, work) for server, work in zip(servers, origin_work, strict=True)
        ]
        write_new(directory / "origin-work.json", origin_work)
        write_new(directory / "origin-audit.json", origin_audit)
        state = authority.ledger.export()
        write_new(directory / "manager-evidence.json", state)
        manager_audit = audit_admission(state["events"])
        write_new(directory / "manager-audit.json", manager_audit)
        peak_service = max(
            (event.get("active", 0) for server in servers for event in server.events), default=0
        )
        outcomes = [parse((directory / f"client-{i}/run/outcomes.json").read_bytes()) for i in range(workers)]
        rows = [row for index in outcomes for row in index["rows"]]
        require(
            len(rows) == 32 and len({row["row_id"] for row in rows}) == 32,
            "distributed logical row reconciliation failed",
        )
        require(peak_service <= 2, "origin-observed service exceeds the aggregate two-request cap")
        require(
            all(audit["peak_open_requests"] <= 2 for audit in origin_audit),
            "origin arrival-to-response work exceeds aggregate cap, including queued requests",
        )
        for i in range(workers):
            owner_path = directory / f"client-{i}/run/process-owner.json"
            if not owner_path.exists():
                require(
                    results[i]["interruption_requested"] and results[i]["process_exit_code"] is None,
                    "native process ownership unavailable",
                )
                continue  # Interruption before launch has no child group to clean.
            owner = parse(owner_path.read_bytes())
            require(not _group_running(owner["pgid"]), "owned process group remains active/uncertain")
        if not interruption["requested"] and case in ("primary", "reciprocal-redirect", "retry-after"):
            require(all(result["run_complete"] for result in results), "native shared client failed")
            require(
                not authority.ledger.outstanding(state["state"]),
                "successful clients left outstanding permits",
            )
            require(
                sum(work["requests"] for work in origin_work)
                == sum(event["action"] == "dispatch" for event in state["events"]),
                "origin attempts do not match manager dispatches",
            )
        elif not interruption["requested"]:
            require(any(not result["run_complete"] for result in results), "loss fixture fabricated success")
            require(bool(authority.ledger.outstanding(state["state"])), "uncertain remote work was recycled")
        summary = {
            "schema": "flowdc-shared-smoke-v1",
            "status": "interrupted" if interruption["requested"] else "assessed",
            "run_complete": not interruption["requested"]
            and all(result["run_complete"] for result in results),
            "interruption_signal": interruption["signal"],
            "case": case,
            "method": method,
            "workers": workers,
            "original_rows": 32,
            "verified_rows": sum(row["disposition"] == "verified" for row in rows),
            "useful_payload_bytes": sum(row["useful_bytes"] for row in rows),
            "origin_peak_service": peak_service,
            "origin_peak_open_requests": max(audit["peak_open_requests"] for audit in origin_audit),
            "elapsed_ns": time.monotonic_ns() - started,
            "native": results,
            "origin_audit": origin_audit,
            "manager_audit": manager_audit,
            "outstanding_uncertain_permits": len(authority.ledger.outstanding(state["state"])),
            "process_groups_quiescent": True,
            "claim": "aggregate admission/accounting engineering only; not TaskVine or efficacy",
        }
        write_new(directory / "result.json", summary)
        return summary
    except BaseException as exc:
        write_new(
            directory / "failure.json",
            {
                "status": "interrupted" if interruption["requested"] else "failed",
                "interruption_signal": interruption["signal"],
                "exception_type": type(exc).__name__,
                "original_rows": 32,
                "case": case,
                "workers": workers,
                "note": "Native outputs and independent audits remain retained; no successful run claim.",
            },
        )
        raise
    finally:
        # Even external task cancellation must join the actual lifecycle threads
        # before closing the authority; cancelling a to_thread wrapper is not proof
        # that its child process stopped.
        try:
            if any(not future.done() for future in futures):
                interruption["requested"] = True
            if futures:
                await asyncio.shield(asyncio.gather(*futures, return_exceptions=True))
        finally:
            try:
                await authority.stop()
            finally:
                authority.ledger.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, choices=(1, 2, 4), required=True)
    parser.add_argument(
        "--method",
        choices=("paarc-base-v2", "gradient-candidate-v1", "fixed-v1", "ratio-v1", "gradient2-application-delay-v1"),
        default="fixed-v1",
    )
    parser.add_argument(
        "--case",
        choices=("primary", "reciprocal-redirect", "retry-after", "worker-loss", "manager-stop"),
        default="primary",
    )
    args = parser.parse_args()
    try:
        result = asyncio.run(run(args.output.absolute(), args.workers, args.method, args.case))
        print(
            json.dumps(
                {
                    key: result[key]
                    for key in ("case", "workers", "original_rows", "verified_rows", "origin_peak_service")
                }
            )
        )
        return 2 if result["status"] == "interrupted" else 0
    except (ValueError, OSError) as exc:
        print(f"Shared integration unavailable/failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
