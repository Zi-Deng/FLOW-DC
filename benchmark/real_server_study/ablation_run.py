"""Finite native ablation execution; no successful or uncertain attempt is replayed."""

import argparse
import asyncio
import json
import os
import runpy
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

from . import ablation
from .common import file_hash, lock, read_json, remaining, replace_derived, require, utc, write_new
from .onset_run import audit_events, fatal_signal
from .prepare import guest_check, host_gate, robots_permit
from .runner import compact_outputs, terminate
from .telemetry import Guard


class WindowGuard(Guard):
    def __init__(self, *args, seconds, **kwargs):
        super().__init__(*args, **kwargs)
        self.seconds = seconds
        self.timer = None
        self.first_dispatch = None

    def check(self):
        if self.policy.reason == "body_budget":
            self.policy.reason = "planned_body_budget"
            self.policy.fatal = False
            self.policy.fatal_reason = None
        if self.policy.reason in ablation.PLANNED:
            import download_batch

            download_batch.shutdown_flag = True
        super().check()

    def emit(self, event, **fields):
        if event == "dispatch" and self.timer is None:
            self.first_dispatch = time.monotonic()

            async def finish():
                await asyncio.sleep(self.seconds)
                import download_batch

                self.policy.reason = self.policy.reason or "planned_window_end"
                download_batch.shutdown_flag = True
                super(WindowGuard, self).emit("planned_end", reason=self.policy.reason)

            self.timer = asyncio.create_task(finish())
        if event == "sample":
            fields.update(
                warning=getattr(self.policy, "warning", False),
                permit_inflight=self.semaphore.inflight if self.semaphore else None,
            )
        super().emit(event, **fields)


def launch(study, source, run_id):
    import polars as pl

    study, source = Path(study), Path(source)
    ablation.check(study, source)
    guest_check(study)
    run = next(r for r in ablation.schedule() if r["run_id"] == run_id)
    directory = study / "runs" / run_id
    cfg = ablation.config(study, run)
    binding = read_json(directory / "attempt.json")
    require(
        binding["protocol_sha256"] == file_hash(study / "protocol.json")
        and read_json(directory / "flowdc-config.json") == cfg,
        "attempt changed",
    )
    require(
        binding["execution_sha256"]
        == file_hash(directory / "execution.json")
        == file_hash(study / "execution.json"),
        "execution lease changed",
    )
    require(binding["slice_sha256"] == file_hash(cfg["input"]), "attempt input changed")
    if run["phase"] == "validation":
        require(binding["selection_sha256"] == file_hash(study / "selection.json"), "selection changed")
    gate = read_json(study / "host-gate.json")
    baseline = read_json(study / "probe-summary.json")
    sys.path.insert(0, str(source / "bin"))
    policy = ablation.Policy(baseline["ttfb_p50_s"])
    # body-collapse baseline must describe backlogged load, not the two-client probe.
    if (study / "body-baseline.json").exists():
        policy.rate = read_json(study / "body-baseline.json")["rate"]
    with (directory / "request-audit.jsonl").open("x", buffering=65536) as stream:
        guard = WindowGuard(
            pl.read_parquet(cfg["input"])["url"].to_list(),
            stream,
            policy.baseline,
            gate["ignore_time"],
            policy=policy,
            body_limit=binding["body_reservation"],
            seconds=run["network_seconds"],
        )
        guard.emit("start", run_id=run_id, definition_sha256=binding["protocol_sha256"])
        restore = guard.install()
        try:
            sys.argv = [
                str(source / "bin/download_batch.py"),
                "--config",
                str(directory / "flowdc-config.json"),
            ]
            runpy.run_path(sys.argv[0], run_name="__main__")
        finally:
            restore()
            if guard.timer:
                guard.timer.cancel()
            guard.emit(
                "closed",
                active=len(guard.active),
                logical_attempts=len(guard.counts),
                body_bytes=guard.body_bytes,
                stop_reason=policy.reason,
                fatal=policy.fatal,
                fatal_reason=policy.fatal_reason,
                cooldown_until_utc=policy.cooldown_until,
            )


def budget(study):
    body = 0
    for intent in study.glob("runs/*/attempt.json"):
        row = read_json(intent.with_name("record.json")) if intent.with_name("record.json").exists() else {}
        body += row.get("body_bytes", read_json(intent)["body_reservation"])
    for path in study.glob("*summary.json"):
        if path.name == "probe-summary.json" or path.name.startswith("recovery-"):
            body += read_json(path).get("observed_bytes", 0)
    for root in (study / "health", *study.glob("recovery-[0-9]")):
        if (root / "probe-intent.json").exists() and not (
            study / ("probe-summary.json" if root.name == "health" else root.name + "-summary.json")
        ).exists():
            body += 2**30  # Unclosed probe retains its full finite reservation.
    return body


def execute(study, source, run):
    directory = study / "runs" / run["run_id"]
    require(not directory.exists(), "first attempt consumed")
    require(remaining(study) >= run["budget_seconds"] + 60, "run and cleanup reserve cannot fit")
    allowance = min(
        (4 if run["phase"] == "screen" else 6) * 2**30, ablation.LIMITS["body_bytes"] - budget(study)
    )
    require(allowance >= 64 * 2**20, "body allowance exhausted")
    used = sum(p.stat().st_size for p in study.parent.rglob("*") if p.is_file() and not p.is_symlink())
    require(
        used + 2 * allowance + 2**30 <= ablation.LIMITS["artifact_bytes"]
        and shutil.disk_usage(study).free >= ablation.LIMITS["free_bytes"] + 2 * allowance + 2**30,
        "storage reserve insufficient",
    )
    directory.mkdir(parents=True)
    cfg = ablation.config(study, run)
    write_new(directory / "flowdc-config.json", cfg)
    binding = dict(
        run,
        utc=utc(),
        protocol_sha256=file_hash(study / "protocol.json"),
        slice_sha256=file_hash(cfg["input"]),
        execution_sha256=file_hash(study / "execution.json"),
        body_reservation=allowance,
        selection_sha256=file_hash(study / "selection.json") if run["phase"] == "validation" else None,
    )
    write_new(directory / "attempt.json", binding)
    write_new(directory / "execution.json", read_json(study / "execution.json"))
    robots_permit(
        read_json(study / "host-gate.json").get("robots_txt", ""),
        __import__("polars").read_parquet(cfg["input"])["url"].to_list(),
    )
    started = time.monotonic()
    reason = fatal = None
    events = []
    command = [
        sys.executable,
        "-B",
        "-m",
        "benchmark.real_server_study.ablation_run",
        "launch",
        "--study",
        str(study),
        "--source",
        str(source),
        "--run-id",
        run["run_id"],
    ]
    with (directory / "stdout.log").open("xb") as out, (directory / "stderr.log").open("xb") as err:
        process = subprocess.Popen(command, cwd=source, stdout=out, stderr=err, start_new_session=True)
        offset = 0
        deadline = started + min(run["budget_seconds"] - 100, run["network_seconds"] + 120)
        try:
            while process.poll() is None:
                path = directory / "request-audit.jsonl"
                if path.exists():
                    with path.open() as stream:
                        stream.seek(offset)
                        while True:
                            at = stream.tell()
                            line = stream.readline()
                            if not line.endswith("\n"):
                                offset = at
                                break
                            event = json.loads(line)
                            fatal = fatal or fatal_signal(event)
                            if event["event"] == "stop" and event.get("reason") not in ablation.PLANNED:
                                reason = reason or event["reason"]
                if reason or fatal or time.monotonic() >= deadline:
                    reason = reason or fatal or "native_process_deadline"
                    terminate(process)
                    break
                time.sleep(0.25)
        finally:
            terminate(process)
    acquired = time.monotonic()
    events, invalid = (
        audit_events(directory / "request-audit.jsonl")
        if (directory / "request-audit.jsonl").exists()
        else ([], True)
    )
    for e in events:
        fatal = fatal or fatal_signal(e)
        if e["event"] in ("stop", "closed") and (e.get("reason") or e.get("stop_reason")) not in (
            *ablation.PLANNED,
            None,
        ):
            reason = reason or e.get("reason") or e.get("stop_reason")
    closed = events[-1] if events and events[-1]["event"] == "closed" else {}
    planned = closed.get("stop_reason") or "input_exhausted"
    normal = (
        process.returncode == 0 and not reason and not fatal and not invalid and closed.get("active") == 0
    )
    verification = {}
    if normal:
        write_new(
            directory / "planned-stop.json", dict(reason=planned, protocol_sha256=binding["protocol_sha256"])
        )
        command = [
            sys.executable,
            "-B",
            "-m",
            "benchmark.real_server_study.verify",
            "--run-dir",
            str(directory),
            "--slice",
            cfg["input"],
            "--source",
            str(source),
            "--started",
            str(started),
            "--ended",
            str(acquired),
            "--credit-urls",
            "--planned-window",
        ]
        with (
            (directory / "verification.json").open("xb") as out,
            (directory / "verification.log").open("xb") as err,
        ):
            try:
                v = subprocess.run(
                    command,
                    cwd=source,
                    stdout=out,
                    stderr=err,
                    timeout=max(1, started + run["budget_seconds"] - time.monotonic()),
                )
                if v.returncode == 0:
                    verification = read_json(directory / "verification.json")
                else:
                    reason = fatal = "verification_failed"
            except subprocess.TimeoutExpired:
                reason = fatal = "verification_timeout"
    credits = verification.get("credited_url_bytes", {})
    exp = ablation.exposure(
        events, run["phase"], credits, read_json(study / "probe-summary.json")["ttfb_p50_s"]
    )
    if run["run_id"] == "capacity-1" and not (study / "body-baseline.json").exists():
        starts = [e["monotonic_s"] for e in events if e["event"] == "dispatch"]
        if starts:
            rate = (
                sum(
                    e["event"] == "network_end"
                    and e.get("status") == 200
                    and min(starts) + 10 < e["monotonic_s"] <= min(starts) + 20
                    for e in events
                )
                / 10
            )
            if rate >= 5:
                write_new(study / "body-baseline.json", {"rate": rate, "source_run": run["run_id"]})
    if verification:
        compact_outputs(directory)
    ended = time.monotonic()
    row = dict(
        run,
        utc=utc(),
        protocol_window_complete=bool(verification) and not reason and not fatal,
        native_returncode=process.returncode,
        stop_reason=reason,
        fatal=bool(fatal),
        fatal_reason=fatal,
        audit_invalid=invalid,
        planned_termination=planned if normal else None,
        elapsed_s=ended - started,
        acquisition_s=acquired - started,
        body_bytes=closed.get("body_bytes", allowance),
        exposure=exp,
        verification={
            k: v for k, v in verification.items() if k not in ("credited_url_bytes", "observations")
        },
        observations=verification.get("observations", {}),
        goodput_Bps=verification.get("credited_bytes", 0) / (ended - started) if verification else None,
    )
    from .ablation_analysis import seal

    row["metadata_bundle"] = seal(directory)
    row["evidence"] = {
        str(p.relative_to(directory)): file_hash(p)
        for p in directory.iterdir()
        if p.is_file() and p.name != "native.tar"
    }
    if verification:
        row["evidence"]["native.tar"] = verification["archive_sha256"]
    write_new(directory / "record.json", row)
    return {**row, "record_sha256": file_hash(directory / "record.json")}


async def probe(study, recovery=None):
    from .onset_run import health

    # Reuse the two-client guarded probe on an independent private subdirectory.
    name = "health" if recovery is None else f"recovery-{recovery}"
    root = study / name
    root.mkdir(exist_ok=True)
    for dest, origin in [
        ("host-gate.json", "host-gate.json"),
        ("execution.json", "execution.json"),
        ("probe.parquet", "probe.parquet" if recovery is None else f"recovery-{recovery}.parquet"),
    ]:
        if not (root / dest).exists():
            write_new(root / dest, (study / origin).read_bytes())
        elif dest == "execution.json":
            replace_derived(root / dest, read_json(study / origin))
    require(not (root / "probe-intent.json").exists(), "probe first attempt already consumed")
    value = await health(root)
    write_new(
        study / ("probe-summary.json" if recovery is None else f"recovery-{recovery}-summary.json"), value
    )
    return value


def window(study, source):
    study, source = Path(study), Path(source)
    ablation.check(study, source)
    guest_check(study)
    with lock(study / "study.lock"):
        if (study / "END.json").exists():
            return {"status": "stopped"}
        from .ablation_controlled import window as controlled

        if not controlled(study, source):
            return {"status": "pending", "stage": "controlled"}
        if not (study / "host-gate.json").exists():
            host_gate(study)
        if not (study / "probe-summary.json").exists():
            if not asyncio.run(probe(study))["passed"]:
                write_new(study / "END.json", {"reason": "health_failed"})
                return {"status": "stopped"}
        if (study / "cooldown.json").exists():
            pause = read_json(study / "cooldown.json")
            n = pause["index"]
            if time.time() < pause["until_utc"]:
                return dict(status="cooldown", until_utc=pause["until_utc"])
            if not (study / f"recovery-{n}-summary.json").exists():
                value = asyncio.run(probe(study, n))
                base = read_json(study / "probe-summary.json")
                passed = (
                    value["valid"] >= 0.95 * value["requests"]
                    and value["ttfb_p50_s"] is not None
                    and value["ttfb_p50_s"] <= 1.5 * base["ttfb_p50_s"]
                )
                if not passed:
                    write_new(study / "END.json", {"reason": "recovery_failed_or_cooldown_limit"})
                    return {"status": "stopped"}
        records = []
        for run in ablation.schedule():
            path = study / "runs" / run["run_id"]
            if path.exists():
                if not (path / "record.json").exists():
                    write_new(study / "END.json", {"reason": "interrupted_attempt", "run_id": run["run_id"]})
                    return {"status": "stopped"}
                records.append(
                    {**read_json(path / "record.json"), "record_sha256": file_hash(path / "record.json")}
                )
                continue
            if run["phase"] == "validation" and not (study / "selection.json").exists():
                write_new(study / "selection.json", ablation.select(records))
            if (
                run["phase"] == "validation"
                and run["method"] in ablation.METHODS
                and run["arm"] not in read_json(study / "selection.json")["choices"]
            ):
                path.mkdir(parents=True)
                write_new(
                    path / "record.json", {**run, "protocol_window_complete": False, "status": "no_candidate"}
                )
                continue
            if run["phase"] == "validation" and run["arm"].startswith("high-"):
                balanced = run["arm"].replace("high-", "balanced-", 1)
                choices = read_json(study / "selection.json")["choices"]
                if balanced in choices and choices[balanced]["options"] == choices[run["arm"]]["options"]:
                    other = next(
                        r for r in ablation.schedule() if r["block"] == run["block"] and r["arm"] == balanced
                    )
                    # The alias can refer forward in the fixed randomized schedule.
                    path.mkdir(parents=True)
                    write_new(
                        path / "record.json",
                        {
                            **run,
                            "status": "alias",
                            "alias_of": other["run_id"],
                            "protocol_window_complete": False,
                        },
                    )
                    continue
            if remaining(study) < run["budget_seconds"] + 60:
                return {"status": "pending", "runs": len(records)}
            row = execute(study, source, run)
            records.append(row)
            if row["stop_reason"] or row["fatal"] or not row["protocol_window_complete"]:
                if row["stop_reason"] == "sustained_latency" and not row["fatal"]:
                    n = len(list(study.glob("recovery-*-summary.json")))
                    if n < ablation.LIMITS["max_cooldowns"]:
                        pause = dict(index=n, run_id=run["run_id"], until_utc=time.time() + 600)
                        replace_derived(study / "cooldown.json", pause)
                        return {"status": "cooldown", **pause}
                write_new(
                    study / "END.json", dict(reason=row["stop_reason"] or "incomplete", run_id=run["run_id"])
                )
                return {"status": "stopped"}
        return {"status": "complete", "runs": len(records)}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=["launch", "window"])
    p.add_argument("--study", type=Path, required=True)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--run-id")
    p.add_argument("--campaign-id")
    p.add_argument("--vm-id")
    p.add_argument("--seconds", type=int)
    a = p.parse_args()
    os.umask(0o077)
    if a.action == "launch":
        launch(a.study, a.source, a.run_id)
    else:
        from .cloud import execution_receipt

        require(120 <= a.seconds <= 3000, "bounded guest lease required")
        replace_derived(
            a.study / "execution.json",
            execution_receipt(
                {"campaign_id": a.campaign_id}, {"worker_id": a.vm_id}, a.seconds, socket.gethostname()
            ),
        )
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:4])
        print(json.dumps(window(a.study, a.source)))


if __name__ == "__main__":
    main()
