"""One bounded subprocess/verification boundary for known-truth comparisons."""

import os
import signal
import subprocess
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import psutil

from .truth import SCHEMA, digest, encode, initial_outcomes, parse, require


def _signal_group(pid, sig):
    try:
        os.killpg(pid, sig)
        return True
    except ProcessLookupError:
        return False


def _group_running(pgid):
    # A reaped parent can leave children behind. Check the owned group before
    # reading artifacts; zombies cannot mutate files and await their own reaper.
    for process in psutil.process_iter(["pid"]):
        try:
            if os.getpgid(process.pid) == pgid and process.status() != psutil.STATUS_ZOMBIE:
                return True
        except (ProcessLookupError, psutil.NoSuchProcess):
            continue
        except (PermissionError, psutil.AccessDenied):
            return True  # Quiescence is uncertain; never start verification.
    return False


@contextmanager
def interruption_signals():
    """First INT/TERM requests cleanup; repeats cannot interrupt owned cleanup."""
    previous, state = {}, {"requested": False, "cleanup": False}

    def interrupt(signum, frame):
        first = not state["requested"]
        state["requested"] = True
        if first and not state["cleanup"]:
            raise KeyboardInterrupt

    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, interrupt)
    try:
        yield state
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def _await_exit(pid, seconds):
    """Observe without reaping; the group leader's PID remains reserved."""
    until = time.monotonic() + seconds
    while True:
        status = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        if status is not None:
            return status.si_status if status.si_code == os.CLD_EXITED else -status.si_status
        if time.monotonic() >= until:
            raise subprocess.TimeoutExpired(str(pid), seconds)
        time.sleep(min(0.01, max(0, until - time.monotonic())))


def run_verified(command, directory, truth, verify, *, cwd=None, deadline=180, cleanup=60, provenance=None):
    with interruption_signals() as interruption:
        return _run_verified(
            command,
            directory,
            truth,
            verify,
            cwd=cwd,
            deadline=deadline,
            cleanup=cleanup,
            provenance=provenance,
            interruption=interruption,
        )


def _run_verified(command, directory, truth, verify, *, cwd, deadline, cleanup, provenance, interruption):
    """Retain logs and reconcile partial output after exit, timeout or interruption.

    Preparation/provenance is outside the timer. The primary timer starts directly
    before Popen and ends after the common index is closed and read back. Rendering
    and writing the final timing record are outside that boundary. POSIX children
    are confined to a new process group; there is no machine-wide process cleanup.
    """
    require(type(deadline) in (int, float) and 0 < deadline <= 180, "deadline must be in (0,180]")
    require(type(cleanup) in (int, float) and 0 < cleanup <= 60, "cleanup must be in (0,60]")
    require(signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL, "exclusive child reaping ownership required")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "invocation.json").write_bytes(
        encode(
            {
                "command": command,
                "cwd": str(cwd),
                "deadline_seconds": deadline,
                "cleanup_reserve_seconds": cleanup,
                "provenance": provenance,
            }
        )
    )
    proc, returncode, failure, error = None, None, None, None
    with (directory / "stdout.log").open("xb") as stdout, (directory / "stderr.log").open("xb") as stderr:
        started = time.monotonic_ns()
        try:
            proc = subprocess.Popen(command, cwd=cwd, stdout=stdout, stderr=stderr, start_new_session=True)
            (directory / "process-owner.json").write_bytes(
                encode(
                    {"pid": proc.pid, "pgid": proc.pid, "create_time": psutil.Process(proc.pid).create_time()}
                )
            )
            returncode = _await_exit(proc.pid, deadline)
            if returncode != 0:
                failure = "nonzero_exit"
        except subprocess.TimeoutExpired:
            failure = "timeout"
        except KeyboardInterrupt:
            failure = "interrupted"
        except OSError as exc:
            failure, error = "launch_failed", str(exc)
        finally:
            interruption["cleanup"] = True
            if proc is not None:
                cleanup_until = time.monotonic() + cleanup
                if failure is None:
                    drain_until = time.monotonic() + min(0.5, cleanup / 4)
                    while _group_running(proc.pid) and time.monotonic() < drain_until:
                        time.sleep(0.01)
                # Even an exited parent may have left descendants. Such a run
                # cannot claim completion; terminate only its owned process group.
                if _group_running(proc.pid):
                    failure = failure or "descendants_remaining"
                    _signal_group(proc.pid, signal.SIGTERM)
                    grace = min(cleanup_until, time.monotonic() + min(5, cleanup / 2))
                    while _group_running(proc.pid) and time.monotonic() < grace:
                        time.sleep(0.01)
                    _signal_group(proc.pid, signal.SIGKILL)
                while _group_running(proc.pid) and time.monotonic() < cleanup_until:
                    time.sleep(0.01)
                if _group_running(proc.pid):
                    failure, error = "cleanup_failed", "owned descendants remain active"
                # All group operations precede reaping: a recycled PID must never
                # identify an unrelated group for a later signal or group scan.
                try:
                    returncode = proc.wait(timeout=max(0, cleanup_until - time.monotonic()))
                except subprocess.TimeoutExpired:
                    failure, error = "cleanup_failed", "owned process did not exit within reserve"
        process_ended = time.monotonic_ns()

    verification_started = time.monotonic_ns()
    try:
        require(failure != "cleanup_failed", "artifact verification unavailable before process quiescence")
        index = verify()
    except Exception as exc:
        # A failed verifier must retain the original denominator and every row.
        index = {
            "rows": list(initial_outcomes(truth).values()),
            "artifacts_valid": False,
            "errors": [f"{type(exc).__name__}: {exc}"],
        }
    rows = index["rows"]
    require(
        [row["row_id"] for row in rows] == [row["row_id"] for row in truth["rows"]],
        "verifier changed the original denominator/order",
    )
    index.update(
        schema=SCHEMA, manifest_sha256=truth["manifest_sha256"], original_rows=truth["original_rows"]
    )
    raw_index = encode(index)
    index_path = directory / "outcomes.json"
    index_path.write_bytes(raw_index)
    require(index_path.read_bytes() == raw_index and parse(raw_index) == index, "index closure mismatch")
    ended = time.monotonic_ns()
    if interruption["requested"]:
        failure = failure or "interrupted"
    complete = (
        not failure
        and index["artifacts_valid"]
        and all(row["disposition"] in ("verified", "skipped") for row in rows)
    )
    total_bytes = sum(row["useful_bytes"] for row in rows)
    verified = sum(row["disposition"] == "verified" for row in rows)
    record = {
        "schema": SCHEMA,
        "status": "complete" if complete else failure or "incomplete",
        "process_exit_code": returncode,
        "process_error": error,
        "run_complete": complete,
        "original_rows": truth["original_rows"],
        "verified_rows": verified,
        "verified_coverage": verified / truth["original_rows"],
        "useful_payload_bytes": total_bytes,
        "elapsed_ns": ended - started,
        "process_ns": process_ended - started,
        "verification_ns": ended - verification_started,
        "boundary": "process launch through closed and checked common outcome index",
        "outcome_index_sha256": digest(raw_index),
        "resources": None,
        "resource_status": "not_measured",
        "attempt_attribution": "origin aggregate; duplicate-row attribution unavailable",
        "provenance": provenance,
    }
    (directory / "result.json").write_bytes(encode(record))
    return record
