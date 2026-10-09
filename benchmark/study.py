#!/usr/bin/env python3
"""Bounded retained study cells and provisional plans; no implicit campaign approval."""

import argparse
import fcntl
import io
import json
import os
import random
import re
import sys
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "bin"))

from benchmark.core.controlled_origin import (  # noqa: E402
    SCENARIOS,
    RESEARCH_SCENARIOS,
    ControlledOrigin,
    audit_events,
    public_scenario,
    scenario,
)
from benchmark.core.flowdc_adapter import FlowDCAdapter, FlowDCConfig  # noqa: E402
from benchmark.core.lifecycle import run_verified  # noqa: E402
from benchmark.core.provenance import environment_record  # noqa: E402
from benchmark.core.study import (  # noqa: E402
    METHODS,
    authorize_plan,
    freeze_protocol,
    make_plan,
    paired_summary,
    precision_plan,
    read_plan,
    tuning_catalog,
    write_new,
)
from benchmark.core.truth import Truth, digest, encode, parse, require  # noqa: E402
from benchmark.core.verifier import verify_native  # noqa: E402
from benchmark.known_truth import image_payloads  # noqa: E402


def originals(seed):
    from PIL import Image

    payloads = image_payloads()
    stream = io.BytesIO()
    Image.frombytes("RGB", (257, 257), random.Random(seed).randbytes(257 * 257 * 3)).save(
        stream, format="PNG"
    )
    payloads["largePNG"] = stream.getvalue()
    with Image.open(io.BytesIO(payloads["largePNG"])) as image:
        image.verify()
    return payloads


def identities(environment):
    source = digest(encode(environment["source_files_sha256"]))
    packages = {
        key: value
        for key, value in environment.items()
        if key not in ("source_files_sha256", "git_head", "git_dirty")
    }
    return {"source_sha256": source, "environment_sha256": digest(encode(packages))}


def execute_cell(directory, cell, config, environment, *, instrument=True, deadline=180, cleanup=60, research_workload=None):
    """Exactly one downloader subprocess; preparation is outside the common timer."""
    payloads = originals(cell["fixture_seed"])
    plan = scenario(cell["scenario"], payloads, rows=cell["rows"], research_workload=research_workload)
    write_new(directory / "scenario.json", public_scenario(plan))
    payload_dir = directory / "originals"
    payload_dir.mkdir()
    for name, raw in payloads.items():
        (payload_dir / (name + ".bin")).write_bytes(raw)
    origins = []
    with ExitStack() as resources:
        for i in range(plan["origins"]):
            origin_dir = directory / f"origin-{i}"
            origin_dir.mkdir()
            origins.append(resources.enter_context(ControlledOrigin(origin_dir, plan, instrument=instrument)))
        urls = [origins[item["origin"]].base_url + item["path"] for item in plan["assignments"]]
        frame = pl.DataFrame(
            {
                "url": urls,
                "label": [f"row-{i}" for i in range(len(urls))],
                "origin_index": [row["origin"] for row in plan["assignments"]],
            }
        )
        frame.write_parquet(directory / "input.parquet")
        catalog = {
            origin.base_url + path: {"bytes": len(spec["payload"]), "sha256": digest(spec["payload"])}
            for origin in origins
            for path, spec in plan["objects"].items()
        }
        truth = Truth.load(directory / "input.parquet", catalog, research_workload=research_workload)
        eligible = truth.write(directory / "fixture")
        native = directory / "run/native"
        settings = FlowDCConfig(
            str(eligible),
            str(native),
            "url",
            None,
            16,
            30 if research_workload is not None else 5,
            True,
            paarc_c_init=config["C_init"],
            paarc_c_min=config["C_min"],
            paarc_c_max=config["C_max"],
            control_method=config["control_method"],
            method_options=config["method_options"],
            research_profile=True,
            research_workload=research_workload,
            max_retry_attempts=2 if cell["scenario"] in ("overload", "sustained-overload", "transient-overload", "recovery") else 1,
        )
        adapter = FlowDCAdapter(ROOT)
        configuration = directory / "flowdc-config.json"
        adapter.generate_config(settings, configuration)
        result = run_verified(
            [sys.executable, str(adapter.bin_path), "--config", str(configuration)],
            directory / "run",
            truth.record,
            lambda: verify_native("flowdc", native, truth.record),
            cwd=ROOT,
            deadline=deadline,
            cleanup=cleanup,
            provenance={
                **identities(environment),
                "cell": cell,
                "scenario_sha256": digest(encode(public_scenario(plan))),
                "config_sha256": digest(configuration.read_bytes()),
                "truth_sha256": digest(encode(truth.record)),
                "instrumentation": instrument,
            },
        )
    # ExitStack stops admission and joins handlers before these counters close.
    work = [origin.snapshot() for origin in origins]
    write_new(directory / "origin-work.json", work)
    audit = [audit_events(origin.events, snapshot) for origin, snapshot in zip(origins, work, strict=True)]
    write_new(directory / "origin-audit.json", audit)
    return {"native": result, "origin_work": work, "origin_audit": audit, "instrumentation": instrument}


def atomic_record(path, value):
    temporary = path.with_name(path.name + ".writing-" + uuid4().hex)
    write_new(temporary, value)
    os.replace(temporary, path)


def require_quiescent(previous):
    from benchmark.core.lifecycle import _group_running

    run = previous / "run"
    if not run.exists():
        return
    result_path = run / "result.json"
    if result_path.exists() and parse(result_path.read_bytes())["status"] != "cleanup_failed":
        return
    owner_path = run / "process-owner.json"
    require(
        owner_path.exists(), "interrupted process ownership uncertain; prove quiescence before continuing"
    )
    owner = parse(owner_path.read_bytes())
    require(type(owner["pgid"]) is int and owner["pgid"] > 0, "invalid retained process ownership")
    require(
        not _group_running(owner["pgid"]),
        "previous owned process group is active/uncertain; continuation refused",
    )


def attempts_for(cell_dir, cell):
    attempts = sorted(path for path in cell_dir.iterdir() if path.is_dir()) if cell_dir.exists() else []
    for number, attempt in enumerate(attempts, 1):
        require(
            re.fullmatch(r"[0-9]{4,}-[0-9a-f]{32}", attempt.name) is not None
            and int(attempt.name.split("-")[0]) == number
            and not attempt.is_symlink(),
            "unexpected/conflicting cell directory; existing evidence preserved",
        )
        record = attempt / "record.json"
        if record.exists():
            require(parse(record.read_bytes())["cell"] == cell, "retained cell identity mismatch")
    return attempts


def run_cell(plan, study_root, cell_index, *, resume=False, rerun=False, protocol=None, instrument=True):
    environment = environment_record(ROOT)
    identity = identities(environment)
    authorize_plan(plan, protocol, **identity)
    require(type(cell_index) is int and 0 <= cell_index < len(plan["cells"]), "invalid cell index")
    require(
        instrument
        or (plan["purpose"] == "engineering" and plan["cells"][cell_index]["method"] == "fixed-v1"),
        "instrumentation-off is fixed-client engineering calibration only",
    )
    directory = study_root.absolute() / plan["namespace"]
    require(
        not directory.is_symlink() and (not directory.exists() or resume),
        "study collision; use explicit resume",
    )
    if not directory.exists():
        directory.mkdir(parents=True, exist_ok=False)
        write_new(directory / "plan.json", plan)
        write_new(directory / "environment.json", environment)
        write_new(directory / "binding.json", {"plan_sha256": digest(encode(plan)), **identity})
    with (directory / ".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        require(
            parse((directory / "binding.json").read_bytes())
            == {"plan_sha256": digest(encode(plan)), **identity},
            "resume source/environment/plan mismatch",
        )
        # Execution follows the pre-generated ordering, including failed cells.
        # Merely supplying a later index cannot bypass a pending earlier cell.
        for preceding in plan["cells"][:cell_index]:
            earlier = attempts_for(directory / preceding["cell_id"], preceding)
            require(
                earlier
                and (earlier[0] / "record.json").exists()
                and parse((earlier[0] / "record.json").read_bytes())["status"] != "started",
                "frozen ordering requires preceding cells to be recorded first",
            )
            for previous in earlier:
                require_quiescent(previous)
        cell = plan["cells"][cell_index]
        cell_dir = directory / cell["cell_id"]
        cell_dir.mkdir(exist_ok=True)
        attempts = attempts_for(cell_dir, cell)
        for attempt in attempts:
            record_path = attempt / "record.json"
            if not record_path.exists() or parse(record_path.read_bytes()).get("status") == "started":
                # Never rerun uncertain work on an ordinary harness resume.
                atomic_record(
                    record_path,
                    {
                        **(parse(record_path.read_bytes()) if record_path.exists() else {}),
                        "cell": cell,
                        "attempt_id": attempt.name,
                        "status": "interrupted",
                        "native": None,
                        "reason": "harness interrupted; retained raw work not credited",
                    },
                )
        if attempts and not rerun:
            return {"status": "already_recorded", "cell": cell["cell_id"], "attempts": len(attempts)}
        if attempts:
            for previous in attempts:
                require_quiescent(previous)
        attempt_id = f"{len(attempts) + 1:04d}-{uuid4().hex}"
        attempt = cell_dir / attempt_id
        attempt.mkdir()
        record = {
            "cell": cell,
            "attempt_id": attempt_id,
            "status": "started",
            "native": None,
            "deliberate_rerun": bool(attempts),
            "source": identity,
        }
        write_new(attempt / "started.json", record)
        write_new(attempt / "record.json", record)
        try:
            evidence = execute_cell(
                attempt, cell, plan["methods"][cell["method"]], environment, instrument=instrument,
                deadline=plan["limits"]["process_deadline_s"], cleanup=plan["limits"]["cleanup_reserve_s"],
                research_workload=plan.get("workload", {}).get("name"),
            )
            record.update(evidence, status="recorded")
        except KeyboardInterrupt:
            record.update(status="interrupted", reason="harness interrupted; raw work retained")
        except Exception as exc:
            record.update(status="failed", reason=f"{type(exc).__name__}: {exc}")
        atomic_record(attempt / "record.json", record)
        return record


def summarize(plan, study_root):
    directory = study_root.absolute() / plan["namespace"]
    binding = parse((directory / "binding.json").read_bytes())
    require(binding["plan_sha256"] == digest(encode(plan)), "summary plan mismatch")
    records = []
    selected = {}
    for cell in plan["cells"]:
        cell_dir = directory / cell["cell_id"]
        attempts = [
            parse((path / "record.json").read_bytes())
            if (path / "record.json").exists()
            else {"cell": cell, "attempt_id": path.name, "status": "interrupted", "native": None}
            for path in attempts_for(cell_dir, cell)
        ]
        records.append(
            {
                "cell": cell,
                "attempts": attempts,
                "status": "pending" if not attempts else attempts[0]["status"],
            }
        )
        # Deliberate reruns remain evidence; never replace the first result with a favorable retry.
        selected[cell["cell_id"]] = attempts[0] if attempts else None
    comparisons = []
    for family in plan["families"]:
        for method in list(plan["methods"])[1:]:
            pairs = []
            for block in range(plan["blocks"]):
                pair = {"block": block, "scenario": family, "candidate_method": method, "censored": False}
                for label, item_method in (("reference", next(iter(plan["methods"]))), ("candidate", method)):
                    cell_id = f"{plan['namespace']}-b{block:03d}-{family}-{item_method}"
                    item = selected[cell_id]
                    native = item.get("native") if item else None
                    pair[label] = None
                    pair[label + "_status"] = (
                        native["status"] if native else item["status"] if item else "pending"
                    )
                    if native and native.get("elapsed_ns", 0) > 0:
                        pair[label] = native["useful_payload_bytes"] / (native["elapsed_ns"] / 1e9)
                        pair["censored"] |= native["status"] in ("timeout", "interrupted", "cleanup_failed")
                pairs.append(pair)
            summary = paired_summary(pairs)
            comparisons.append(
                {
                    "scenario": family,
                    "candidate": method,
                    "summary": summary,
                    "precision": precision_plan(summary),
                }
            )
    return {
        "schema": "flowdc-study-summary-v1",
        "binding": binding,
        "planned_cells": records,
        "metric": "verified original row bytes / launch-through-verification seconds",
        "comparisons": comparisons,
        "warning": "engineering/pilot estimates do not establish efficacy; all planned cells retained",
    }


def calibrate(directory, seed, family):
    require(not directory.exists() and not directory.is_symlink(), "calibration output collision")
    environment = environment_record(ROOT)
    directory.mkdir(parents=True, exist_ok=False)
    write_new(directory / "environment.json", environment)
    order = [True, False]
    random.Random(seed).shuffle(order)
    write_new(directory / "ordering.json", {"seed": seed, "instrumentation_order": order})
    plan = make_plan(
        seed=seed, blocks=1, families=[family], namespace="engineering", purpose="engineering", rows=64
    )
    cell = next(cell for cell in plan["cells"] if cell["method"] == "fixed-v1")
    records = []
    for i, instrument in enumerate(order):
        attempt = directory / f"{i}-instrumentation-{instrument}"
        attempt.mkdir()
        try:
            record = execute_cell(
                attempt,
                cell,
                plan["methods"]["fixed-v1"],
                environment,
                instrument=instrument,
                deadline=60,
                cleanup=15,
            )
        except Exception as exc:
            record = {
                "native": None,
                "instrumentation": instrument,
                "failure": f"{type(exc).__name__}: {exc}",
            }
        write_new(attempt / "calibration.json", record)
        records.append(record)
    result = {
        "schema": "flowdc-calibration-v1",
        "records": records,
        "warning": "one independent fixed-client pair; instrumentation includes origin event logging only; no publishable overhead/efficacy estimate",
    }
    write_new(directory / "calibration.json", result)
    return all(record.get("native") and record["native"]["run_complete"] for record in records)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan", help="Write a new immutable proposed ordering; no execution")
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--seed", type=int, required=True)
    plan.add_argument("--blocks", type=int, default=6)
    plan.add_argument(
        "--families", nargs="+", choices=RESEARCH_SCENARIOS, default=None
    )
    plan.add_argument("--namespace", choices=("tuning", "evaluation", "engineering"), default="evaluation")
    plan.add_argument(
        "--purpose", choices=("engineering", "provisional-pilot", "confirmatory"), default="provisional-pilot"
    )
    plan.add_argument("--rows", type=int, default=128)
    plan.add_argument("--research-workload", choices=("bounded-fixture-v1", "bounded-research-v2"))
    plan.add_argument(
        "--method-configs",
        type=Path,
        help="Explicit configurations chosen before evaluation; hash-bound by protocol",
    )
    catalog = commands.add_parser(
        "tuning-catalog", help="Write eight proposed configurations per method; no execution"
    )
    catalog.add_argument("--output", type=Path, required=True)
    catalog.add_argument("--research-workload", choices=("bounded-fixture-v1", "bounded-research-v2"))
    calibration = commands.add_parser(
        "calibrate", help="Two bounded independent fixed-client runs with origin instrumentation on/off"
    )
    calibration.add_argument("--output", type=Path, required=True)
    calibration.add_argument("--seed", type=int, required=True)
    calibration.add_argument("--scenario", choices=("steady", "mixed-sizes"), default="steady")
    origin = commands.add_parser(
        "origin", help="Bounded loopback origin, also usable in a guest over an explicit SSH forward"
    )
    origin.add_argument("--output", type=Path, required=True)
    origin.add_argument("--scenario", choices=SCENARIOS, default="steady")
    origin.add_argument("--port", type=int, default=0)
    origin.add_argument("--seconds", type=float, default=60)
    origin.add_argument("--seed", type=int, required=True)
    run = commands.add_parser(
        "run-cell", help="Run one bounded cell; broad campaigns require frozen protocol"
    )
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--study-root", type=Path, required=True)
    run.add_argument("--cell-index", type=int, required=True)
    run.add_argument("--resume", action="store_true")
    run.add_argument(
        "--rerun",
        action="store_true",
        help="Retain a distinct attempt; never replace the original analysis cell",
    )
    run.add_argument("--protocol", type=Path)
    run.add_argument(
        "--instrumentation-off",
        action="store_true",
        help="Engineering fixed-client overhead calibration only",
    )
    summary = commands.add_parser(
        "summarize", help="Include every planned cell, zero, failure and cancellation"
    )
    summary.add_argument("--plan", type=Path, required=True)
    summary.add_argument("--study-root", type=Path, required=True)
    summary.add_argument("--output", type=Path, required=True)
    freeze = commands.add_parser(
        "freeze", help="Human checkpoint: bind supplied advisor decisions to exact plan/source/environment"
    )
    freeze.add_argument("--plan", type=Path, required=True)
    freeze.add_argument("--decisions", type=Path, required=True)
    freeze.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "plan":
            write_new(
                args.output,
                make_plan(
                    seed=args.seed,
                    blocks=args.blocks,
                    families=args.families,
                    namespace=args.namespace,
                    purpose=args.purpose,
                    rows=args.rows,
                    configurations=parse(args.method_configs.read_bytes()) if args.method_configs else None,
                    research_workload=args.research_workload,
                ),
            )
        elif args.command == "tuning-catalog":
            write_new(args.output, tuning_catalog(research_workload=args.research_workload))
        elif args.command == "calibrate":
            return 0 if calibrate(args.output.absolute(), args.seed, args.scenario) else 1
        elif args.command == "origin":
            require(
                0 < args.seconds <= 180 and (args.port == 0 or 1024 <= args.port <= 65535),
                "invalid origin deadline/port",
            )
            require(0 <= args.seed < 2**32, "seed must be a uint32")
            payloads, environment = originals(args.seed), environment_record(ROOT)
            plan = scenario(args.scenario, payloads)
            args.output.mkdir(parents=True, exist_ok=False)
            write_new(args.output / "scenario.json", public_scenario(plan))
            write_new(args.output / "environment.json", environment)
            (args.output / "originals").mkdir()
            for name, raw in payloads.items():
                (args.output / "originals" / (name + ".bin")).write_bytes(raw)
            with ControlledOrigin(args.output, plan, port=args.port) as service:
                write_new(
                    args.output / "ready.json",
                    {"url": service.base_url, "pid": os.getpid(), "seconds": args.seconds},
                )
                service.stop_event.wait(args.seconds)
            write_new(args.output / "origin-work.json", service.snapshot())
        elif args.command == "freeze":
            frozen = freeze_protocol(
                read_plan(args.plan),
                parse(args.decisions.read_bytes()),
                **identities(environment_record(ROOT)),
            )
            write_new(args.output, frozen)
        elif args.command == "summarize":
            write_new(args.output, summarize(read_plan(args.plan), args.study_root))
        else:
            record = run_cell(
                read_plan(args.plan),
                args.study_root,
                args.cell_index,
                resume=args.resume,
                rerun=args.rerun,
                protocol=parse(args.protocol.read_bytes()) if args.protocol else None,
                instrument=not args.instrumentation_off,
            )
            print(
                json.dumps(
                    {"status": record["status"], "native_status": (record.get("native") or {}).get("status")}
                )
            )
            return 0 if record["status"] in ("recorded", "already_recorded") else 1
    except (OSError, ValueError, KeyError) as exc:
        print(f"Study unavailable/refused: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
