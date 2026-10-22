"""Finite real-source batches: immutable inputs, four-way blocks and worker jobs."""

import argparse
import contextlib
import hashlib
import itertools
import json
import math
import os
import random
import shutil
import signal
import socket
import ssl
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
import urllib.robotparser
from pathlib import Path

import polars as pl

from .common import (
    decode_image,
    file_hash,
    read_json,
    require,
    source_identity,
    tls_context,
    utc,
    write_new,
)
from .public_guard import MAX_OBJECT, SOURCES as SOURCE_CATALOG, UA, NoRedirect, PublicPolicy, checked_url

# Historical pilot scope is fixed even as the guarded source catalog expands.
SOURCES = {key: SOURCE_CATALOG[key] for key in ("taibif-moth", "openimages-cvdf")}


def terminate(proc):
    """Close the entire owned group, including children of an exited leader."""
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGTERM)
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=2)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)


METHODS = ("ratio-v2", "gradient-candidate-v2", "fixed-v1", "img2dataset-1.47.0")
CAPS = (8, 32, 96, 192)
GRID = [
    dict(startup_growth_factor=a, baseline_max_age_s=b, target_delay_ratio=2.0)
    for a, b in itertools.product((1.25, 1.5), (10.0, 120.0))
]
LIMITS = dict(
    wall_seconds=28800,
    dispatch_seconds=19800,
    max_windows=3,
    window_seconds=3600,
    cleanup_seconds=600,
    budget_su=100,
    max_requests=150000,
    body_bytes=32 * 2**30,
    archive_bytes=40 * 2**30,
    free_bytes=50 * 2**30,
    job_seconds=240,
    native_seconds=150,
    verification_seconds=60,
    finalization_seconds=30,
    audit_seconds=1200,
    collection_seconds=900,
    metadata_bytes=512 * 2**20,
)
SEED = 20261022


def order(seed=SEED):
    rng = random.Random(seed)
    base = list(METHODS)
    rng.shuffle(base)
    offsets = list(range(4))
    rng.shuffle(offsets)
    return [base[n:] + base[:n] for n in offsets]


def prepare(study, source, populations, provenance):
    """Offline only; populations have real scalar media metadata, not payloads."""
    study, source = Path(study).resolve(), Path(source).resolve()
    require(not study.exists(), "new batch directory required")
    study.mkdir(parents=True, mode=0o700)
    for name in ("pools", "runs", "gates", "analysis"):
        (study / name).mkdir()
    files = {}
    rng = random.Random(SEED)
    for source_id in ("taibif-moth", "openimages-cvdf"):
        frame = pl.read_parquet(populations[source_id])
        required = {"url", "record_id", "author", "license", "landing_url"}
        require(
            required <= set(frame.columns) and frame["url"].n_unique() == frame.height,
            "invalid public population",
        )
        frame = frame.select(sorted(required)).cast({n: pl.String for n in required})
        require(all(v is not None for row in frame.iter_rows() for v in row), "null source metadata")
        for url in frame["url"]:
            checked_url(url, source_id)
        require(frame.height >= 13344, "insufficient disjoint real inputs")
        indices = rng.sample(range(frame.height), 13344)
        pool = frame[indices]
        pool = pool.with_columns(
            pl.Series(
                "study_row_id",
                [
                    hashlib.sha256((source_id + "|" + str(i) + "|" + row["record_id"]).encode()).hexdigest()
                    for i, row in enumerate(pool.to_dicts())
                ],
            )
        )
        path = study / "pools" / f"{source_id}.parquet"
        pool.write_parquet(path)
        files[str(path.relative_to(study))] = file_hash(path)
    write_new(
        study / "protocol.json",
        dict(
            schema="flowdc-public-batch-v1",
            seed=SEED,
            limits=LIMITS,
            methods=list(METHODS),
            grid=GRID,
            caps=list(CAPS),
            sources=list(SOURCES),
            files=files,
            provenance=provenance,
            source=source_identity(source),
            timing="Job launch through verified retained worker output; batch metadata transfer reported separately",
            interpretation="Exploratory four-block precision pilot; no confirmatory or universal superiority claim",
        ),
    )
    return read_json(study / "protocol.json")


def pool_slice(study, source_id, phase, index=0, rows=2048):
    pool = pl.read_parquet(Path(study) / "pools" / f"{source_id}.parquet")
    if phase == "preflight":
        start, size = 0, 32
    elif phase == "qualification":
        start, size = 32 + 128 * index, 128
    elif phase == "calibration":
        start, size = (544, 512) if index == 0 else (1056, 2048)
    elif phase == "tuning":
        start, size = 3104, rows
    elif phase == "evaluation":
        start, size = 5152 + 2048 * index, rows
    else:
        raise ValueError("unknown public phase")
    return pool.slice(start, size)


def host_gate(study, source_id):
    """Guest-only32-object sequential probe; no successful-row filtering."""
    from .prepare import guest_check

    guest_check(study)
    target = Path(study) / "gates" / f"{source_id}.json"
    if target.exists():
        return read_json(target)
    intent = target.with_suffix(".intent.json")
    require(not intent.exists(), "interrupted public probe; no replay")
    provenance = read_json(Path(study) / "protocol.json")["provenance"][source_id]
    require(
        provenance.get("media_rights_qualified") is True,
        "media rights not qualified: " + provenance.get("limitation", "source preparation incomplete"),
    )
    write_new(intent, dict(utc=utc(), source=source_id))
    host, prefix = SOURCE_CATALOG[source_id]
    ignore = False
    try:
        with socket.create_connection((host, 443), timeout=15) as raw:
            with tls_context().wrap_socket(raw, server_hostname=host):
                pass
    except ssl.SSLCertVerificationError as error:
        require(source_id == "taibif-moth" and error.verify_code in (9, 10), "unqualified TLS failure")
        require(
            read_json(Path(study) / "protocol.json")["provenance"][source_id].get(
                "retained_time_exception_authority"
            )
            is True,
            "missing retained moth-only time exception authority",
        )
        ignore = True
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        NoRedirect(),
        urllib.request.HTTPSHandler(context=tls_context(ignore)),
    )
    robots = ""
    status = None
    try:
        with opener.open(
            urllib.request.Request("https://" + host + "/robots.txt", headers={"User-Agent": UA}), timeout=15
        ) as r:
            raw = r.read(512 * 1024 + 1)
            require(len(raw) <= 512 * 1024, "oversized robots")
            robots = raw.decode("utf8", "replace")
            status = r.status
    except urllib.error.HTTPError as e:
        require(e.code == 404, "robots observation refused")
        status = 404
    parser = urllib.robotparser.RobotFileParser()
    parser.parse(robots.splitlines())
    for entry in [*parser.entries, *([parser.default_entry] if parser.default_entry else [])]:
        if entry.applies_to(UA):
            require(not entry.delay and not entry.req_rate, "source requires unqualified pacing")
            require(
                not any("*" in urllib.parse.unquote(r.path) or "$" in r.path for r in entry.rulelines),
                "unsupported robots pattern",
            )
    if robots:
        require(
            all(
                parser.can_fetch(UA, url)
                for url in pl.read_parquet(Path(study) / "pools" / f"{source_id}.parquet")["url"]
            ),
            "robots disallows a frozen planned path",
        )
    probe = pool_slice(study, source_id, "preflight")
    observations = []
    policy = PublicPolicy(1.0)
    for row in probe.to_dicts():
        url = row["url"]
        require(not robots or parser.can_fetch(UA, url), "robots disallows planned public path")
        started = time.monotonic()
        with intent.with_suffix(".jsonl").open("a") as stream:
            stream.write(json.dumps(dict(event="attempt", url=url, utc=utc())) + "\n")
        with opener.open(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=30) as response:
            policy.inspect(response.headers)
            policy.header(time.monotonic(), response.status, response.headers.get("Retry-After"))
            require(not policy.reason and response.status == 200, "public probe refusal")
            first = response.read(1)
            delay = time.monotonic() - started
            data = first + response.read(MAX_OBJECT)
            require(len(data) <= MAX_OBJECT, "oversized probe image")
            decode_image(data)
        observations.append(
            dict(url=url, bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), delay_s=delay)
        )
        with intent.with_suffix(".jsonl").open("a") as stream:
            stream.write(json.dumps(dict(event="result", **observations[-1])) + "\n")
    value = dict(
        source=source_id,
        utc=utc(),
        ignore_time=ignore,
        robots_status=status,
        robots_txt=robots,
        baseline_s=statistics.median(r["delay_s"] for r in observations),
        probe=observations,
        probe_body_bytes=sum(r["bytes"] for r in observations),
        requests=len(observations),
        passed=True,
    )
    write_new(target, value)
    return value


def configuration(directory, cell, source_root):
    directory = Path(directory)
    method = cell["method"]
    setting = cell["setting"]
    ceiling = 4 if cell["phase"] == "qualification" else CAPS[setting]
    if method == "img2dataset-1.47.0":
        return dict(
            url_list=str(directory / "input.parquet"),
            input_format="parquet",
            url_col="url",
            output_folder=str(directory / "native"),
            output_format="webdataset",
            thread_count=ceiling,
            processes_count=1,
            timeout=30,
            retries=0,
            max_shard_retry=0,
            resize_mode="no",
            disable_all_reencoding=True,
            extract_exif=False,
            encode_format="jpg",
            compute_hash="sha256",
            distributor="multiprocessing",
            number_sample_per_shard=max(512, pl.read_parquet(directory / "input.parquet").height),
            user_agent_token="flowdc-public-study",
            save_additional_columns=[
                c for c in pl.read_parquet(directory / "input.parquet").columns if c != "url"
            ],
        )
    fixed = ceiling if method == "fixed-v1" else None
    maximum = 4 if cell["phase"] == "qualification" else 192
    return dict(
        input=str(directory / "input.parquet"),
        input_format="parquet",
        output=str(directory / "native"),
        output_format="webdataset",
        url="url",
        label=None,
        concurrent_downloads=192,
        timeout=30,
        enable_paarc=True,
        control_method=method,
        method_options=GRID[setting] if fixed is None else {},
        C_min=fixed or 2,
        C_init=fixed or 4,
        C_max=fixed or maximum,
        max_retry_attempts=1,
        research_profile=True,
        research_workload="bounded-external-v1",
        create_tar=True,
        compress_tar=False,
        create_overview=True,
        force_overwrite=False,
    )


def audit_transport(path):
    events = [json.loads(line) for line in Path(path).read_text().splitlines()]
    require(
        events and events[0]["event"] == "start" and events[-1]["event"] == "closed",
        "transport journal incomplete",
    )
    active, seen, attempts, ended = set(), set(), set(), set()
    previous = events[0]["monotonic_s"]
    area = 0
    first = None
    last = None
    peak = 0
    latencies = []
    for e in events:
        now = e["monotonic_s"]
        require(now >= previous, "transport clock reversed")
        area += len(active) * (now - previous)
        previous = now
        if e["event"] == "attempt":
            require(e["url"] not in attempts, "replayed public URL")
            attempts.add(e["url"])
        elif e["event"] == "dispatch":
            require(e["url"] in attempts, "dispatch lacks attempt intent")
            token = e["token"]
            require(token not in seen and len(active) < 192, "duplicate/excess dispatch")
            active.add(token)
            seen.add(token)
            first = now if first is None else first
            peak = max(peak, len(active))
            require(e["inflight"] == len(active), "dispatch count differs")
        elif e["event"] == "network_end":
            require(
                e["token"] not in ended and e["dispatched"] == (e["token"] in active),
                "network closure differs",
            )
            ended.add(e["token"])
            active.discard(e["token"])
            last = now
            require(e["inflight"] == len(active), "end count differs")
            if e.get("ttfb_s") is not None:
                latencies.append(e["ttfb_s"])
    require(
        not active and events[-1]["active"] == 0 and not events[-1].get("stop_reason"),
        "unsafe/unclosed transport",
    )
    duration = last - first if first is not None and last is not None else 0
    return dict(
        attempts=len(attempts),
        dispatches=len(seen),
        body_bytes=events[-1]["body_bytes"],
        peak=peak,
        mean_active=area / duration if duration > 0 else 0,
        http_span_s=duration,
        p95_s=sorted(latencies)[math.ceil(0.95 * len(latencies)) - 1] if len(latencies) >= 100 else None,
        status_counts={
            str(s): sum(
                e["event"] == "headers" and e["status"] == s for e in events if e["event"] == "headers"
            )
            for s in {e["status"] for e in events if e["event"] == "headers"}
        },
    )


def execute(study, source_root, cell, frame):
    started = time.monotonic()
    study, source_root = Path(study), Path(source_root)
    directory = study / "runs" / cell["run_id"]
    directory.mkdir()
    frame.write_parquet(directory / "input.parquet")
    attempt = {
        **cell,
        "protocol_sha256": file_hash(study / "protocol.json"),
        "input_sha256": file_hash(directory / "input.parquet"),
    }
    write_new(directory / "attempt.json", attempt)
    cfg = configuration(directory, cell, source_root)
    write_new(directory / "config.json", cfg)
    if cell["method"] != "img2dataset-1.47.0":
        write_new(directory / "flowdc-config.json", cfg)
    command = [
        sys.executable,
        "-B",
        "-m",
        "benchmark.real_server_study.public_launch",
        "--study",
        str(study),
        "--run-id",
        cell["run_id"],
    ]
    result = {**attempt, "status": "incomplete", "started_monotonic": started, "verification": None}
    try:
        with (directory / "stdout.log").open("x") as out, (directory / "stderr.log").open("x") as err:
            proc = subprocess.Popen(command, cwd=source_root, stdout=out, stderr=err, start_new_session=True)
            try:
                proc.wait(timeout=min(LIMITS["native_seconds"], max(1, started + 155 - time.monotonic())))
            finally:
                terminate(proc)
        acquired = time.monotonic()
        result.update(returncode=proc.returncode, native_seconds=acquired - started)
        require(proc.returncode == 0, "native process failed or exceeded deadline")
        transport = audit_transport(directory / "request-audit.jsonl")
        require(
            transport["attempts"] == frame.height
            and transport["body_bytes"] <= 2**30
            and transport["peak"] <= cfg.get("thread_count", cfg.get("C_max")),
            "transport attempt/body budget differs",
        )
        verify = [
            sys.executable,
            "-B",
            "-m",
            "benchmark.real_server_study.public_verify",
            "--directory",
            str(directory),
            "--source",
            str(source_root),
            "--method",
            cell["method"],
            "--started",
            str(started),
            "--ended",
            str(acquired),
        ]
        with (
            (directory / "verification.json").open("x") as out,
            (directory / "verification.log").open("x") as err,
        ):
            verified = subprocess.Popen(
                verify, cwd=source_root, stdout=out, stderr=err, start_new_session=True
            )
            try:
                verified.wait(
                    timeout=min(LIMITS["verification_seconds"], max(1, started + 210 - time.monotonic()))
                )
            finally:
                terminate(verified)
        require(verified.returncode == 0, "independent verification failed")
        verification = read_json(directory / "verification.json")
        ended = time.monotonic()
        result.update(
            status="complete",
            verification=verification,
            transport=transport,
            elapsed_s=ended - started,
            verification_s=ended - acquired,
            goodput_Bps=verification["credited_bytes"] / (ended - started),
            native_goodput_Bps=verification["credited_bytes"] / (acquired - started),
            coverage=verification["coverage"],
            full_success=verification["full_success"],
        )
        if cell["method"] != "img2dataset-1.47.0":
            # Validated archive is authoritative; preserve all journals, drop loose duplicates.
            try:
                index = read_json(directory / "native/outcome-index.json")
                removed = 0
                for r in index["rows"]:
                    require(
                        time.monotonic() - started < LIMITS["job_seconds"] - 2, "job finalization deadline"
                    )
                    if r["disposition"] == "verified":
                        for field in ("payload", "metadata"):
                            name = r[field]
                            require(Path(name).name == name, "unsafe compact path")
                            (directory / "native" / name).unlink()
                            removed += 1
                write_new(
                    directory / "compaction.json",
                    dict(removed=removed, archive_sha256=verification["archives"]["native.tar"]["sha256"]),
                )
            except Exception as error:
                # Compaction is storage housekeeping after the measured boundary.
                # A retained verified archive still supplies valid scientific credit.
                result["compaction_error"] = type(error).__name__ + ": " + str(error)
    except Exception as error:
        result.update(
            status="stopped",
            error=type(error).__name__ + ": " + str(error),
            elapsed_s=time.monotonic() - started,
        )
    result["closed_utc"] = utc()
    result["evidence"] = {
        p.name: file_hash(p)
        for p in directory.iterdir()
        if p.is_file() and p.suffix in (".json", ".jsonl", ".parquet")
    }
    write_new(directory / "record.json", result)
    return result


def choose_size(records):
    require(records and all(r["status"] == "complete" for r in records), "calibration incomplete")
    for rows in (2048, 512):
        predicted = 30 + 1.5 * rows * max(
            r["elapsed_s"] / r["verification"]["original_rows"] for r in records
        )
        if predicted <= LIMITS["job_seconds"]:
            return rows
    raise ValueError("no bounded workload fits; source deferred")


def select(records):
    selected = {}
    for method in METHODS:
        rows = [r for r in records if r["method"] == method and r["status"] == "complete"]
        require(len(rows) == 4, "incomplete tuning catalog")
        reference = next(r for r in records if r["method"] == "fixed-v1" and r["setting"] == 1)
        eligible = [r for r in rows if r["coverage"] >= reference["coverage"] - 0.01]
        require(eligible, "no coverage-eligible configuration")
        best = max(eligible, key=lambda r: (r["goodput_Bps"], -r["setting"]))
        selected[method] = best["setting"]
    return selected


def inventory(study):
    records = []
    interrupted = []
    for directory in sorted((Path(study) / "runs").iterdir()):
        if (directory / "record.json").exists():
            records.append(read_json(directory / "record.json"))
        else:
            interrupted.append(directory.name)
    return records, interrupted


def usage(study):
    records, interrupted = inventory(study)
    requests = 0
    body = 0
    for record in records:
        transport = record.get("transport")
        requests += (
            transport["attempts"]
            if transport
            else pl.read_parquet(Path(study) / "runs" / record["run_id"] / "input.parquet").height
        )
        body += transport["body_bytes"] if transport else 2**30
    for run_id in interrupted:
        requests += pl.read_parquet(Path(study) / "runs" / run_id / "input.parquet").height
        body += 2**30
    for intent in (Path(study) / "gates").glob("*.intent.json"):
        gate = intent.with_name(intent.name.replace(".intent", ""))
        if gate.exists():
            observation = read_json(gate)
            requests += observation["requests"]
            body += observation["probe_body_bytes"]
        else:
            requests += 32
            body += 32 * MAX_OBJECT
    archives = sum(p.stat().st_size for p in (Path(study) / "runs").rglob("*.tar"))
    return dict(requests=requests, body_bytes=body, archive_bytes=archives, interrupted=interrupted)


def planned_cells(study, source_id):
    """One deterministic first-attempt schedule; choices freeze before evaluation."""
    for i, method in enumerate(METHODS):
        yield dict(phase="qualification", index=i, method=method, setting=1, rows=128)
    for i, n in enumerate((512, 2048)):
        yield dict(phase="calibration", index=i, method="fixed-v1", setting=1, rows=n)
    sizing = Path(study) / "gates" / f"{source_id}-size.json"
    if not sizing.exists():
        return
    n = read_json(sizing)["rows"]
    for setting in range(4):
        for method in order()[setting]:
            yield dict(phase="tuning", index=setting, method=method, setting=setting, rows=n)
    selected_path = Path(study) / "gates" / f"{source_id}-selection.json"
    if not selected_path.exists():
        return
    selected = read_json(selected_path)["selected"]
    for block, methods in enumerate(order()):
        for method in methods:
            yield dict(phase="evaluation", index=block, method=method, setting=selected[method], rows=n)


def freeze_choices(study, source_id, records):
    rows = [r for r in records if r["source"] == source_id]
    sizing = Path(study) / "gates" / f"{source_id}-size.json"
    qualification = [r for r in rows if r["phase"] == "qualification"]
    calibration = [r for r in rows if r["phase"] == "calibration"]
    if not sizing.exists() and len(qualification) == 4 and len(calibration) == 2:
        write_new(
            sizing,
            dict(
                rows=choose_size(qualification + calibration),
                utc=utc(),
                calibration_runs=[r["run_id"] for r in qualification + calibration],
            ),
        )
    selection = Path(study) / "gates" / f"{source_id}-selection.json"
    tuning = [r for r in rows if r["phase"] == "tuning"]
    if not selection.exists() and len(tuning) == 16:
        write_new(
            selection,
            dict(
                selected=select(tuning),
                utc=utc(),
                runs=[r["run_id"] for r in tuning],
                rule="Maximum verified end-to-end goodput subject to coverage within .01 of Fixed32; exploratory, latency reported separately",
            ),
        )


def window(study, source, seconds):
    from .common import lock
    from .prepare import guest_check

    study, source = Path(study), Path(source)
    guest_check(study)
    protocol = read_json(study / "protocol.json")
    execution = read_json(study / "execution.json")
    require(
        protocol["limits"] == LIMITS and protocol["source"] == source_identity(source),
        "frozen protocol/source changed",
    )
    for name, digest in protocol["files"].items():
        require(file_hash(study / name) == digest, "frozen input changed")
    deadline = min(
        time.monotonic() + seconds, time.monotonic() + execution["dispatch_deadline_utc"] - time.time()
    )
    with lock(study / "study.lock"):
        for source_id in SOURCES:
            stop = study / "gates" / f"{source_id}-stopped.json"
            if stop.exists():
                continue
            try:
                if not (study / "gates" / f"{source_id}.json").exists():
                    if deadline - time.monotonic() < 1100:
                        return dict(status="window_complete", reason="probe reserve")
                    require(
                        shutil.disk_usage(study).free >= LIMITS["free_bytes"] + 2**31, "probe free reserve"
                    )
                    used = usage(study)
                    require(
                        used["requests"] + 32 <= LIMITS["max_requests"]
                        and used["body_bytes"] + 32 * MAX_OBJECT <= LIMITS["body_bytes"],
                        "probe budget",
                    )
                    host_gate(study, source_id)
                while True:
                    records, interrupted = inventory(study)
                    require(
                        not any(name.startswith(source_id + "-") for name in interrupted),
                        "interrupted source first attempt; no acquisition replay",
                    )
                    prior = [r for r in records if r["source"] == source_id]
                    require(
                        all(r["status"] == "complete" for r in prior), "source has incomplete first attempt"
                    )
                    freeze_choices(study, source_id, records)
                    seen = {r["run_id"] for r in records}
                    pending = []
                    for cell in planned_cells(study, source_id):
                        cell = {
                            **cell,
                            "source": source_id,
                            "run_id": f"{source_id}-{cell['phase']}-{cell['index']}-{cell['method']}",
                        }
                        if cell["run_id"] not in seen:
                            pending.append(cell)
                    if not pending:
                        break
                    if deadline - time.monotonic() < LIMITS["job_seconds"] + 30:
                        return dict(status="window_complete", reason="job reserve")
                    cell = pending[0]
                    used = usage(study)
                    require(
                        used["requests"] + cell["rows"] <= LIMITS["max_requests"]
                        and used["body_bytes"] + 2**30 <= LIMITS["body_bytes"]
                        and used["archive_bytes"] + 2**31 <= LIMITS["archive_bytes"],
                        "batch traffic/archive budget",
                    )
                    require(shutil.disk_usage(study).free >= LIMITS["free_bytes"] + 3 * 2**30, "free reserve")
                    frame = pool_slice(study, source_id, cell["phase"], cell["index"], cell["rows"])
                    result = execute(study, source, cell, frame)
                    require(
                        result["status"] == "complete", "first attempt stopped: " + str(result.get("error"))
                    )
                    v = result["verification"]
                    t = result["transport"]
                    require(
                        v["coverage"] >= 0.95
                        and sum(t["status_counts"].get(str(n), 0) for n in (404, 410)) / cell["rows"] <= 0.05,
                        "public content/dead-link stop",
                    )
            except Exception as error:
                write_new(stop, dict(utc=utc(), reason=type(error).__name__ + ": " + str(error)))
        records, interrupted = inventory(study)
        complete = all(
            len([r for r in records if r["source"] == s and r["phase"] == "evaluation"]) == 16
            for s in SOURCES
        )
        return dict(
            status="complete" if complete else "stopped",
            usage=usage(study),
            runs=len(records),
            interrupted=interrupted,
        )


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command", required=True)
    w = sub.add_parser("window")
    w.add_argument("--study", type=Path, required=True)
    w.add_argument("--source", type=Path, required=True)
    w.add_argument("--seconds", type=int, required=True)
    a = p.parse_args()
    require(0 < a.seconds <= 3000, "finite guest window required")
    print(json.dumps(window(a.study, a.source, a.seconds), allow_nan=False))


if __name__ == "__main__":
    main()
