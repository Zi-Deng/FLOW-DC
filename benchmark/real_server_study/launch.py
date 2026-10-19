"""One guarded child process around the frozen maintained downloader."""
import argparse
import runpy
import sys
from pathlib import Path

from .common import file_hash, protocol_check, read_json, require
from .design import config
from .telemetry import Guard


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--study", required=True, type=Path)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    study, source = args.study.resolve(), args.source.resolve()
    protocol_check(study, source)
    require(not (study / "STOP.json").exists(), "campaign stopped")
    runs = read_json(study / "schedule.json")["runs"]
    run = next(r for r in runs if r["run_id"] == args.run_id)
    directory = study / "runs" / run["run_id"]
    binding = read_json(directory / "attempt.json")
    require(binding["protocol_sha256"] == file_hash(study / "protocol.json"), "attempt protocol changed")
    plan = read_json(study / "main-plan.json") if run["phase"] != "calibration" else None
    selected = read_json(study / "fixed-selection.json") if run["phase"] == "fixed" else None
    if plan:
        require(binding["main_plan_sha256"] == file_hash(study / "main-plan.json"), "main selection changed")
        name=f"slices/{run['slice']}.parquet"
        require(file_hash(study/name)==plan["files"][name], "main slice differs from frozen plan")
    if selected:
        require(binding["fixed_selection_sha256"] == file_hash(study / "fixed-selection.json"), "Fixed selection changed")
    wanted = config(run, study, directory, selected)
    require(read_json(directory / "flowdc-config.json") == wanted, "run config changed")
    import polars as pl
    urls = pl.read_parquet(wanted["input"])["url"].to_list()
    require(file_hash(wanted["input"]) == binding["slice_sha256"], "run slice changed")
    sys.path.insert(0, str(source / "bin"))
    with (directory / "request-audit.jsonl").open("x", buffering=65536) as audit:
        gate = read_json(study / "host-gate.json")
        guard = Guard(urls, audit, read_json(study / "probe-summary.json")["ttfb_p50_s"], gate["ignore_time"])
        guard.emit("start", run_id=run["run_id"], rows=len(urls), definition_sha256=file_hash(study / "protocol.json"))
        restore = guard.install()
        try:
            sys.argv = [str(source / "bin/download_batch.py"), "--config", str(directory / "flowdc-config.json")]
            runpy.run_path(sys.argv[0], run_name="__main__")
        finally:
            restore()
            guard.emit("closed", active=len(guard.active), logical_attempts=len(guard.counts),
                       stop_reason=guard.policy.reason, fatal=guard.policy.fatal,
                       cooldown_until_utc=guard.policy.cooldown_until)


if __name__ == "__main__":
    main()
