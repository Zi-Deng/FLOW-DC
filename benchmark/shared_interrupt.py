#!/usr/bin/env python3
"""Bounded real INT/TERM regression for the concurrent shared-origin harness."""

import argparse
import hashlib
import json
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path

import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmark.shared_origin import open_pidfd, signal_pidfd  # noqa: E402


def read(path):
    return json.loads(path.read_bytes())


def capture_child(owner, handles):
    key = (owner["pid"], owner["create_time"])
    if key in handles:
        return
    try:
        fd = open_pidfd(owner["pid"])
    except ProcessLookupError:
        return
    try:
        if psutil.Process(owner["pid"]).create_time() != owner["create_time"]:
            raise AssertionError("owned process identity changed")
    except BaseException:
        os.close(fd)
        raise
    handles[key] = fd


def cleanup_fixture(process, handles, evidence):
    """Freeze the owned harness before capturing partial-startup descendants."""
    try:
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGSTOP)
            parent = psutil.Process(process.pid)
            until = time.monotonic() + 2
            while parent.status() not in (psutil.STATUS_STOPPED, psutil.STATUS_ZOMBIE):
                if time.monotonic() >= until:
                    raise AssertionError("owned harness did not stop for cleanup")
                time.sleep(0.01)
            # The unreaped Popen leader pins identity. Its stopped threads cannot
            # launch another child while these kernel handles are collected.
            for child in parent.children(recursive=True):
                try:
                    capture_child({"pid": child.pid, "create_time": child.create_time()}, handles)
                except psutil.NoSuchProcess:
                    continue
    finally:
        try:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=5)
        finally:
            until = time.monotonic() + 5
            try:
                for fd in handles.values():
                    try:
                        signal_pidfd(fd, signal.SIGKILL)
                        evidence["cleanup"].append("owned_pidfd_signalled")
                    except ProcessLookupError:
                        evidence["cleanup"].append("owned_process_exited")
                for fd in handles.values():
                    poller = select.poll()
                    poller.register(fd, select.POLLIN)
                    if not poller.poll(max(0, int((until - time.monotonic()) * 1000))):
                        raise AssertionError("owned child cleanup deadline")
                evidence["cleanup_quiescent"] = True
            finally:
                for fd in handles.values():
                    os.close(fd)


def run(output, workers, signum, source_root=ROOT):
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    native = output / "shared"
    command = [
        sys.executable,
        "-B",
        str(source_root / "benchmark/shared_origin.py"),
        "--output",
        str(native),
        "--workers",
        str(workers),
        "--method",
        "fixed-v1",
    ]
    sources = {
        str(path.relative_to(source_root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for folder in ("bin", "benchmark", "benchmark/core")
        for path in (source_root / folder).glob("*.py")
    }
    evidence = {
        "command": command,
        "signal": signal.Signals(signum).name,
        "workers": workers,
        "sources": sources,
        "passed": False,
        "native": [],
        "cleanup": [],
    }
    owners, handles = [], {}
    process = None
    try:
        with (output / "stdout.log").open("xb") as stdout, (output / "stderr.log").open("xb") as stderr:
            process = subprocess.Popen(
                command, cwd=source_root, stdout=stdout, stderr=stderr, start_new_session=True
            )
            until = time.monotonic() + 30
            while time.monotonic() < until:
                paths = [native / f"client-{i}/run/process-owner.json" for i in range(workers)]
                for path in paths:
                    if path.is_file():
                        try:
                            capture_child(read(path), handles)
                        except (json.JSONDecodeError, psutil.NoSuchProcess):
                            pass  # Owner write/exit raced this bounded startup probe.
                arrivals = native / "origin-0/origin.jsonl"
                if (
                    all(path.is_file() for path in paths)
                    and arrivals.exists()
                    and '"phase":"arrival"' in arrivals.read_text()
                ):
                    break
                if process.poll() is not None:
                    raise AssertionError("harness exited before real requests")
                time.sleep(0.01)
            else:
                raise AssertionError("native startup deadline")
            for path in paths:
                owner = read(path)
                capture_child(owner, handles)
                owners.append(owner)
            # Popen has not reaped this child; no recycled PID can receive the signal.
            process.send_signal(signum)
            evidence["signal_sent"] = True
            # A repeated signal must not abort the cleanup reserve.
            time.sleep(0.05)
            if process.poll() is None:
                process.send_signal(signum)
            code = process.wait(timeout=45)
            evidence["process_exit_code"] = code
            for i, owner in enumerate(owners):
                path = native / f"client-{i}/run/result.json"
                value = read(path) if path.exists() else None
                evidence["native"].append(value)
                try:
                    child = psutil.Process(owner["pid"])
                    alive = (
                        child.create_time() == owner["create_time"] and child.status() != psutil.STATUS_ZOMBIE
                    )
                except psutil.NoSuchProcess:
                    alive = False
                if alive:
                    raise AssertionError("native process remains active after harness exit")
                if value is None or value["status"] != "interrupted" or value["run_complete"]:
                    raise AssertionError("native interruption not retained")
                index = read(native / f"client-{i}/run/outcomes.json")
                if index["original_rows"] != 32 or len(index["rows"]) != 32 // workers:
                    raise AssertionError("interruption changed original denominator/partition")
            summary = read(native / "result.json")
            if code != 2 or summary.get("status") != "interrupted" or summary.get("run_complete"):
                raise AssertionError("harness interruption not retained")
            if summary["original_rows"] != 32 or not summary["process_groups_quiescent"]:
                raise AssertionError("harness denominator/cleanup invalid")
            evidence["summary"] = summary
            evidence["passed"] = True
    except BaseException as exc:
        evidence["failure"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        try:
            cleanup_fixture(process, handles, evidence)
        except BaseException as exc:
            evidence["passed"] = False
            evidence["cleanup_failure"] = {"type": type(exc).__name__, "message": str(exc)}
            raise
        finally:
            evidence["sources_unchanged"] = all(
                hashlib.sha256((source_root / name).read_bytes()).hexdigest() == digest
                for name, digest in sources.items()
            )
            (output / "result.json").write_text(json.dumps(evidence, indent=2))
    if not evidence["sources_unchanged"]:
        raise AssertionError("source changed during signal fixture")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Fresh retained evidence directory")
    parser.add_argument("--workers", type=int, choices=(1, 2, 4), required=True)
    parser.add_argument("--signal", choices=("INT", "TERM"), required=True)
    parser.add_argument("--source-root", type=Path, default=ROOT, help="Trusted source snapshot under test")
    args = parser.parse_args()
    run(
        args.output.absolute(),
        args.workers,
        getattr(signal, "SIG" + args.signal),
        args.source_root.absolute(),
    )
    print(json.dumps({"passed": True, "workers": args.workers, "signal": args.signal}))


if __name__ == "__main__":
    main()
