"""Twenty-four native controlled ablations, with realized stimulus and byte truth.

The normalized service model tests mechanisms, not 150-request host capacity.
Sparse demand is paced before row intent/permit acquisition, never inside HTTP.
"""

import argparse
import asyncio
import contextlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from . import ablation
from .common import file_hash, read_json, require, source_identity, utc, write_new
from .runner import terminate


def cases():
    return [
        dict(
            run_id=f"lab-{method}-{variant}-{condition}", method=method, variant=variant, condition=condition
        )
        for method in ablation.METHODS
        for variant in ablation.VARIANTS
        for condition in ("steady-sparse", "drop-recovery")
    ]


async def native(directory, source, cell, seconds=45):
    import polars as pl

    sys.path.insert(0, str(source / "bin"))
    import download_batch as downloader

    from benchmark.core.controlled_origin import ControlledOrigin, audit_events, public_scenario, scenario
    from benchmark.core.truth import Truth, digest
    from benchmark.core.verifier import verify_native
    from benchmark.known_truth import image_payloads

    plan = scenario(
        "drop-recovery" if cell["condition"] == "drop-recovery" else "steady",
        image_payloads(),
        rows=8192,
        research_workload=ablation.PROFILE,
    )
    write_new(directory / "scenario.json", public_scenario(plan))
    opts = (
        {}
        if cell["variant"] == "full"
        else (
            {"baseline_max_age_s": 10}
            if cell["variant"] == "short-refresh"
            else {"ablation": cell["variant"]}
        )
    )
    paced = []
    started = time.monotonic()
    next_due = started
    original = downloader.download_one

    async def demand(**kwargs):
        nonlocal next_due
        elapsed = time.monotonic() - started
        if cell["condition"] == "steady-sparse" and 10 <= elapsed < 20:
            due = max(time.monotonic(), next_due)
            next_due = due + 0.5
            await asyncio.sleep(max(0, min(started + 20, due) - time.monotonic()))
            paced.append(time.monotonic() - started)
        return await original(**kwargs)

    async def finish():
        await asyncio.sleep(seconds)
        downloader.shutdown_flag = True

    with ControlledOrigin(directory, plan) as origin:
        urls = [origin.base_url + item["path"] for item in plan["assignments"]]
        manifest = directory / "input.parquet"
        pl.DataFrame({"url": urls}).write_parquet(manifest)
        catalog = {
            origin.base_url + path: dict(bytes=len(spec["payload"]), sha256=digest(spec["payload"]))
            for path, spec in plan["objects"].items()
        }
        truth = Truth.load(manifest, catalog, research_workload=ablation.PROFILE)
        eligible = truth.write(directory / "truth")
        cfg = downloader.Config(
            str(eligible),
            str(directory / "native"),
            control_method=cell["method"],
            method_options=opts,
            concurrent_downloads=16,
            http_connection_limit=16,
            C_min=2,
            C_init=4,
            C_max=64,
            research_profile=True,
            research_workload=ablation.PROFILE,
            max_retry_attempts=1,
            compress_tar=False,
        )
        write_new(directory / "flowdc-config.json", __import__("dataclasses").asdict(cfg))
        downloader.shutdown_flag = False
        downloader.download_one = demand
        timer = asyncio.create_task(finish())
        began = time.monotonic()
        try:
            await downloader.run_acquisition(cfg)
        finally:
            downloader.download_one = original
            timer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await timer
        acquired = time.monotonic()
        verification = verify_native("flowdc", directory / "native", truth.record)
    snapshot = origin.snapshot()
    audit = audit_events(origin.events, snapshot)
    write_new(directory / "origin-work.json", snapshot)
    write_new(directory / "origin-audit.json", audit)
    write_new(directory / "verification.json", verification)
    capacities = [e["slots"] for e in origin.events if e["phase"] == "capacity"]
    realized = (
        capacities == [4, 1, 4]
        if cell["condition"] == "drop-recovery"
        else bool(paced) and acquired - began >= 40
    )
    rows = verification.pop("rows")
    credited = [r for r in rows if r["disposition"] == "verified"]
    traces = [
        json.loads(line)
        for path in (directory / "native/.flowdc/control").glob("*.jsonl")
        for line in path.read_text().splitlines()
    ]
    reasons = __import__("collections").Counter(e["reason"] for e in traces if "reason" in e)
    row = dict(
        cell,
        utc=utc(),
        source=source_identity(source),
        realized_stimulus=realized,
        capacity_steps=capacities,
        sparse_paced_rows=len(paced),
        elapsed_s=time.monotonic() - began,
        acquisition_s=acquired - began,
        verified_rows=len(credited),
        verified_bytes=sum(r["useful_bytes"] for r in credited),
        original_rows=len(rows),
        artifacts_valid=verification["artifacts_valid"],
        verification_errors=verification["errors"],
        control_reasons=dict(reasons),
        origin_audit=audit,
        interpretation="One normalized mechanism run; not an independent capacity or efficacy estimate.",
    )
    from .ablation_analysis import seal

    sys.stdout.flush()
    sys.stderr.flush()
    row["metadata_bundle"] = seal(directory)
    row["evidence"] = {
        str(p.relative_to(directory)): file_hash(p) for p in directory.iterdir() if p.is_file()
    }
    write_new(directory / "record.json", row)
    require(row["artifacts_valid"] and realized, "controlled evidence qualification failed")
    return row


def window(study, source):
    from .common import remaining

    root = Path(study) / "controlled"
    root.mkdir(exist_ok=True)
    for cell in cases():
        directory = root / cell["run_id"]
        if directory.exists():
            require((directory / "record.json").exists(), "interrupted controlled case; no replay")
            row = read_json(directory / "record.json")
            require(row["artifacts_valid"] and row["realized_stimulus"], "controlled case unqualified")
            continue
        if remaining(study) < 120:
            return False
        directory.mkdir()
        write_new(directory / "intent.json", dict(cell, utc=utc()))
        command = [
            sys.executable,
            "-B",
            "-m",
            "benchmark.real_server_study.ablation_controlled",
            "--directory",
            str(directory),
            "--source",
            str(source),
            "--cell",
            json.dumps(cell),
        ]
        with (directory / "stdout.log").open("xb") as out, (directory / "stderr.log").open("xb") as err:
            process = subprocess.Popen(command, cwd=source, stdout=out, stderr=err, start_new_session=True)
            try:
                code = process.wait(timeout=60)
            finally:
                terminate(process)
        require(code == 0, "controlled case failed; no public-host testing")
    return True


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--directory", type=Path, required=True)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--cell", required=True)
    a = p.parse_args()
    os.umask(0o077)
    asyncio.run(native(a.directory, a.source, json.loads(a.cell)))
