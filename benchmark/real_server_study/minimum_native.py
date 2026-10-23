"""Matched TaskVine sharding and manager-side verification for long public jobs."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import resource
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import time
from pathlib import Path

import polars as pl

from .common import file_hash, read_json, require, utc, write_new
from .minimum_study import LIMITS, PROFILE, ceiling, configuration, manifest
from .public_batch import terminate
from .public_metrics import transport
from .public_guard import latency_stop_enabled


def pack(directory, destination, *, metadata_only=False, partial=False):
    """Keep native archive and all journals; omit only loose archive duplicates."""
    directory = Path(directory)
    excluded = set()
    excluded_inodes = set()
    canonical_inodes = {}
    index_path = directory / "native/outcome-index.json"
    if not partial and index_path.exists() and (directory / "native.tar").exists():
        for row in read_json(index_path)["rows"]:
            if row["disposition"] == "verified":
                for key in ("payload", "metadata"):
                    excluded.add(directory / "native" / row[key])
        # Integrity publication uses hardlinks. Keep one archive copy, plus all
        # journals, rather than returning its export-stage alias and loose body
        # aliases as well. The manager independently verifies the native archive.
        for path in excluded:
            require(not path.is_symlink(), "symlink in archived duplicate")
            info = path.stat()
            excluded_inodes.add((info.st_dev, info.st_ino))
        for path in (directory / "native.tar", index_path, directory / "native/overview.json"):
            if path.exists():
                require(not path.is_symlink(), "symlink in canonical return")
                info = path.stat()
                canonical_inodes[(info.st_dev, info.st_ino)] = path
    total = 0
    with tarfile.open(destination, "x", dereference=True) as archive:
        for path in sorted(directory.rglob("*")):
            require(not path.is_symlink(), "symlink in owned return")
            if path.is_file() and path not in excluded:
                info = path.stat()
                inode = (info.st_dev, info.st_ino)
                if inode in excluded_inodes or (
                    inode in canonical_inodes and canonical_inodes[inode] != path
                ):
                    continue
                if partial:
                    relative = path.relative_to(directory)
                    # Keep committed PAARC journal payloads and native tool
                    # shards; omit public hardlink aliases and unfinished tar.
                    flow_owned = (directory / "native/.flowdc/owner.json").exists()
                    if flow_owned and relative.parts[0] == "native" and ".flowdc" not in relative.parts:
                        continue
                    if flow_owned and (relative == Path("native.tar") or "exports" in relative.parts):
                        continue
                if metadata_only and (path.parent != directory or path.stat().st_size > 64 * 2**20):
                    continue  # Full partial work remains on its owning guest.
                total += path.stat().st_size
                require(total <= 192 * 2**30, "finite return exceeded")
                archive.add(path, arcname=str(path.relative_to(directory)), recursive=False)
    return total


def guard_stop(directory):
    """Read only recent flushed events; propagate refusal without archive delay."""
    for path in [directory / "request-audit.jsonl", *directory.glob("tool-request-*.jsonl")]:
        if not path.exists():
            continue
        with path.open("rb") as stream:
            stream.seek(max(0, path.stat().st_size - 16384))
            for line in stream.read(16384).splitlines():
                try:
                    event = json.loads(line)
                except (ValueError, UnicodeError):
                    continue
                if event.get("event") == "stop" or (
                    event.get("event") == "closed" and event.get("stop_reason")
                ):
                    return event.get("reason") or event["stop_reason"]
    return None


def unpack(archive_path, destination, deadline):
    destination = Path(destination)
    destination.mkdir()
    count, total = 0, 0
    with tarfile.open(archive_path, "r:") as archive:
        names = set()
        for item in archive:
            name = Path(item.name)
            require(
                item.isfile()
                and not item.issparse()
                and not name.is_absolute()
                and ".." not in name.parts
                and item.name not in names,
                "unsafe return member",
            )
            count += 1
            total += item.size
            require(
                count <= 8388608 and total <= 192 * 2**30 and time.monotonic() < deadline,
                "return count/bytes/deadline exceeded",
            )
            names.add(item.name)
            path = destination / name
            path.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(item) as src, path.open("xb") as dst:
                shutil.copyfileobj(src, dst, 1024 * 1024)
    return total


def worker(spec_path, input_path, output_path):
    spec = read_json(spec_path)
    cell, root = spec["cell"], Path(spec["guest_root"])
    source = root / "source"
    require((source / ".flowdc-source-head").read_text().strip() == spec["head"], "worker source changed")
    require(file_hash(input_path) == spec["input_sha256"], "partition identity changed")
    study = root / "jobs" / cell["run_id"] / spec["role"] / "study"
    run = study / "runs" / cell["run_id"]
    run.mkdir(parents=True)
    (study / "gates").mkdir()
    write_new(
        study / "execution.json",
        dict(
            executor="jetstream2",
            vm_id=spec["vm_id"],
            campaign_id=spec["campaign_id"],
            hostname=socket.gethostname(),
            deadline_utc=spec["deadline_utc"],
            source_root=str(source),
        ),
    )
    from .prepare import guest_check

    guest_check(study)
    write_new(study / "protocol.json", spec["protocol"])
    write_new(study / "gates" / (cell["source"] + ".json"), spec["gate"])
    shutil.copyfile(input_path, run / "input.parquet")
    frame = pl.read_parquet(run / "input.parquet")
    cfg = configuration(run, cell, frame)
    attempt = dict(
        **cell,
        workload=PROFILE,
        body_limit=LIMITS["job_body_bytes"] // cell["workers"],
        input_sha256=spec["input_sha256"],
        protocol_sha256=file_hash(study / "protocol.json"),
    )
    write_new(run / "attempt.json", attempt)
    write_new(run / "config.json", cfg)
    if cell["method"] != "img2dataset-1.47.0":
        write_new(run / "flowdc-config.json", cfg)
    started = time.monotonic()
    record = dict(
        status="stopped",
        started=started,
        role=spec["role"],
        vm_id=spec["vm_id"],
        hostname=socket.gethostname(),
        source_head=spec["head"],
        latency_stop_enabled=latency_stop_enabled(spec["protocol"]),
    )
    try:
        with (run / "stdout.log").open("x") as out, (run / "stderr.log").open("x") as err:
            proc = subprocess.Popen(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "benchmark.real_server_study.public_launch",
                    "--study",
                    str(study),
                    "--run-id",
                    cell["run_id"],
                ],
                cwd=source,
                stdout=out,
                stderr=err,
                start_new_session=True,
            )
            try:
                until = time.monotonic() + min(
                    LIMITS["native_seconds"], spec["deadline_utc"] - time.time() - 1800
                )
                while proc.poll() is None:
                    reason = guard_stop(run)
                    require(not reason, "public source guard: " + str(reason))
                    require(time.monotonic() < until, "native acquisition/processing deadline")
                    time.sleep(0.5)
            finally:
                terminate(proc)
        require(proc.returncode == 0, "native acquisition/processing failed")
        metrics = transport(run / "request-audit.jsonl", ceiling(cell["source"]))
        require(metrics["attempts"] == frame.height, "partition attempts incomplete")
        record.update(status="complete", transport=metrics)
    except Exception as error:
        record["error"] = type(error).__name__ + ": " + str(error)
        from .public_metrics import partial_transport
        record["transport"] = partial_transport(run, LIMITS["job_body_bytes"])
    record.update(
        ended=time.monotonic(),
        utc=utc(),
        child_max_rss_kib=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
        child_cpu_s=sum(os.times()[2:4]),
    )
    write_new(run / "worker-record.json", record)
    pack(run, output_path, partial=record["status"] != "complete")


def manager(request_path):
    import ndcctools.taskvine as vine

    require(vine.__version__ == "7.17.2", "qualified TaskVine version required")
    request = read_json(request_path)
    root, study = Path(request["guest_root"]), Path(request["study"])
    cell = request["cell"]
    require(
        len(request["roles"]) == cell["workers"]
        and len(set(request["roles"])) == cell["workers"]
        and len({request["vms"][r] for r in request["roles"]}) == cell["workers"],
        "distinct owned worker slots required",
    )
    directory = study / "runs" / cell["run_id"]
    directory.mkdir()
    started = time.monotonic()
    job_seconds = cell.get("job_seconds", LIMITS["job_seconds"])
    require(0 < job_seconds <= LIMITS["job_seconds"], "invalid job deadline")
    deadline = started + job_seconds
    record = dict(
        **cell,
        status="stopped",
        original_rows=cell["rows"],
        started_utc=utc(),
        source_head=request["head"],
        latency_stop_enabled=latency_stop_enabled(read_json(study / "protocol.json")),
        timing="manager-submit-through-returned-verification-v1",
    )
    write_new(directory / "attempt.json", dict(**record, request=request))
    frame = manifest(study, cell["source"], cell["phase"], cell["rows"], cell.get("block", 0))
    frame.write_parquet(directory / "input.parquet")
    manager = None
    try:
        manager = vine.Manager(
            port=9123,
            run_info_path=str(directory / "vine-info"),
            staging_path=str(root / "vine-staging" / cell["run_id"]),
            ssl=True,
            shutdown=True,
            init_fn=lambda m: m.set_password_file(str(root / "native-password")),
        )
        manager.disable_peer_transfers()
        require(manager.enable_disconnect_slow_workers(0) == 1, "cannot disable speculative disconnection")
        require(manager.set_category_mode("public-download", "fixed") == 1, "fixed task resources required")
        tasks, received = {}, []
        for slot, role in enumerate(request["roles"]):
            node = directory / role
            node.mkdir()
            part = frame.gather_every(cell["workers"], offset=slot)
            part.write_parquet(node / "input.parquet")
            spec = dict(
                cell=cell,
                guest_root=str(root),
                role=role,
                vm_id=request["vms"][role],
                campaign_id=request["campaign_id"],
                head=request["head"],
                deadline_utc=time.time() + max(1, deadline - time.monotonic()),
                input_sha256=file_hash(node / "input.parquet"),
                protocol=read_json(study / "protocol.json"),
                gate=read_json(study / "gates" / (cell["source"] + ".json")),
            )
            write_new(node / "task.json", spec)
            task = vine.Task(
                shlex.join(
                    [
                        str(root / "venv/bin/python"),
                        "-B",
                        "-m",
                        "benchmark.real_server_study.minimum_native",
                        "worker",
                        "--spec",
                        "task.json",
                        "--input",
                        "input.parquet",
                        "--output",
                        "return.tar",
                    ]
                )
            )
            task.set_env_var("PYTHONPATH", str(root / "source"))
            for key, value in {
                "POLARS_MAX_THREADS": "8",
                "OMP_NUM_THREADS": "1",
                "OPENBLAS_NUM_THREADS": "1",
                "NO_ALBUMENTATIONS_UPDATE": "1",
                "WANDB_MODE": "disabled",
            }.items():
                task.set_env_var(key, value)
            task.set_cores(8)
            task.set_memory(24576)
            task.set_disk(300000)
            task.set_category("public-download")
            # In pinned7.17.2, zero means unlimited. One is its smallest
            # finite native retry setting. The persistent worker run directory
            # rejects redispatch before any image HTTP (including lost ACKs).
            task.set_retries(1)
            task.set_max_forsaken(0)
            task.set_time_max(min(LIMITS["native_seconds"], job_seconds - 1800) + 600)
            task.add_feature(request["campaign_id"] + "-" + role)
            for name in ("input.parquet", "task.json"):
                task.add_input(manager.declare_file(str(node / name), cache=False, peer_transfer=False), name)
            task.add_output(
                manager.declare_file(str(node / "return.tar"), cache=False, peer_transfer=False), "return.tar"
            )
            tasks[manager.submit(task)] = role
        write_new(directory / "ready.json", dict(port=manager.port, tasks=tasks))

        def verify_return(role):
            node = directory / role
            returned = node / "returned"
            unpack(node / "return.tar", returned, deadline)
            native = read_json(returned / "worker-record.json")
            require(
                native["status"] in ("complete", "stopped")
                and native["vm_id"] == request["vms"][role]
                and native["source_head"] == request["head"]
                and file_hash(returned / "input.parquet") == file_hash(node / "input.parquet"),
                "worker/partition/closure identity differs",
            )
            # Verifier is a separately bounded process: C decoding cannot hold
            # the manager control loop indefinitely.
            with (node / "verification.json").open("x") as out, (node / "verification.log").open("x") as err:
                proc = subprocess.Popen(
                    [
                        sys.executable,
                        "-B",
                        "-m",
                        "benchmark.real_server_study.public_verify",
                        "--directory",
                        str(returned),
                        "--source",
                        str(root / "source"),
                        "--method",
                        cell["method"],
                        "--started",
                        str(native["started"]),
                        "--ended",
                        str(native["ended"]),
                        *(["--partial"] if native["status"] != "complete" else []),
                    ],
                    stdout=out,
                    stderr=err,
                    start_new_session=True,
                )
                try:
                    proc.wait(timeout=max(1, min(LIMITS["audit_seconds"], deadline - time.monotonic())))
                finally:
                    terminate(proc)
            require(proc.returncode == 0, "manager independent verification failed")
            verified = read_json(node / "verification.json")
            from .public_metrics import progress
            write_new(returned / "progress.json", progress(returned, verified["payloads"]))
            write_new(
                node / "retention.json",
                dict(verified=True, utc=utc(), vm_id=request["vms"][role], archives=verified["archives"]),
            )
            return dict(role=role, worker=native, verification=verified)

        with ThreadPoolExecutor(max_workers=cell["workers"]) as executor:
            futures = []
            while tasks:
                require(time.monotonic() < deadline, "complete-job deadline")
                task = manager.wait(1)
                if task is None:
                    continue
                role = tasks.pop(task.id)
                node = directory / role
                result = dict(
                    role=role,
                    id=task.id,
                    successful=task.successful(),
                    exit_code=task.exit_code,
                    result=task.result,
                    hostname=task.hostname,
                    addrport=task.addrport,
                    bytes_received=task.get_metric("bytes_received"),
                    bytes_sent=task.get_metric("bytes_sent"),
                    stdout=task.std_output,
                )
                write_new(node / "vine-result.json", result)
                require(task.successful() and task.exit_code == 0, "native task failed; no replay")
                futures.append(executor.submit(verify_return, role))
            received = [future.result() for future in futures]
        elapsed = time.monotonic() - started
        credited = sum(r["verification"]["credited_bytes"] for r in received)
        credited_rows = sum(r["verification"]["credited_rows"] for r in received)
        require(
            sum(r["verification"]["original_rows"] for r in received) == frame.height,
            "original denominator differs",
        )
        record.update(
            status="complete" if all(r["worker"]["status"] == "complete" for r in received) else "stopped",
            elapsed_s=elapsed,
            credited_bytes=credited,
            coverage=credited_rows / frame.height,
            goodput_Bps=credited / elapsed if all(r["worker"]["status"] == "complete" for r in received) else None,
            body_bytes=sum(r["worker"]["transport"]["body_bytes"] for r in received),
            native_seconds=max(r["worker"]["ended"] - r["worker"]["started"] for r in received),
            http_span_s=max(r["worker"]["transport"].get("http_span_s") or 0 for r in received),
            mean_http_concurrency=sum(r["worker"]["transport"].get("mean_http_concurrency") or 0 for r in received)
                if all(r["worker"]["status"] == "complete" for r in received) else None,
            stop_evidence=[r["worker"].get("transport", {}) for r in received if r["worker"]["status"] != "complete"],
            # Integral is additive across hosts, requires no synchronized clocks.
            mean_job_concurrency=sum((r["worker"]["transport"].get("request_seconds") or 0) for r in received) / elapsed
                if all(r["worker"]["status"] == "complete" for r in received) else None,
            participants=[
                dict(
                    role=r["role"],
                    vm_id=r["worker"]["vm_id"],
                    hostname=r["worker"]["hostname"],
                    transport=r["worker"]["transport"],
                )
                for r in received
            ],
        )
    except Exception as error:
        record.update(error=type(error).__name__ + ": " + str(error), elapsed_s=time.monotonic() - started)
    finally:
        if manager is not None:
            manager.cancel_all()
            manager.__exit__(None, None, None)
    record["closed_utc"] = utc()
    record["evidence"] = {
        str(p.relative_to(directory)): file_hash(p)
        for p in [
            directory / "input.parquet",
            *directory.glob("*/verification.json"),
            *directory.glob("*/returned/worker-record.json"),
            *directory.glob("*/returned/progress.json"),
        ]
        if p.is_file()
    }
    write_new(directory / "record.json", record)
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    worker_args = sub.add_parser("worker")
    for name in ("spec", "input", "output"):
        worker_args.add_argument("--" + name, type=Path, required=True)
    manager_args = sub.add_parser("manager")
    manager_args.add_argument("--request", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "worker":
        worker(args.spec, args.input, args.output)
    else:
        print(json.dumps(manager(args.request)))
