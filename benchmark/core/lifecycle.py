"""One bounded subprocess/verification boundary for known-truth comparisons."""

import os
import signal
import subprocess
import time
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


def run_verified(command, directory, truth, verify, *, cwd=None, deadline=180, cleanup=60, provenance=None):
    """Retain logs and reconcile partial output after exit, timeout or interruption.

    Preparation/provenance is outside the timer. The primary timer starts directly
    before Popen and ends after the common index is closed and read back. Rendering
    and writing the final timing record are outside that boundary. POSIX children
    are confined to a new process group; there is no machine-wide process cleanup.
    """
    require(type(deadline) in (int, float) and 0 < deadline <= 180, "deadline must be in (0,180]")
    require(type(cleanup) in (int, float) and 0 < cleanup <= 60, "cleanup must be in (0,60]")
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
            returncode = proc.wait(timeout=deadline)
            if returncode != 0:
                failure = "nonzero_exit"
        except subprocess.TimeoutExpired:
            failure = "timeout"
        except KeyboardInterrupt:
            failure = "interrupted"
        except OSError as exc:
            failure, error = "launch_failed", str(exc)
        finally:
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
                    try:
                        proc.wait(timeout=min(5, cleanup / 2))
                    except subprocess.TimeoutExpired:
                        pass
                    _signal_group(proc.pid, signal.SIGKILL)
                try:
                    returncode = proc.wait(timeout=cleanup / 2)
                except subprocess.TimeoutExpired:
                    failure, error = "cleanup_failed", "owned process did not exit within reserve"
                while _group_running(proc.pid) and time.monotonic() < cleanup_until:
                    time.sleep(0.01)
                if _group_running(proc.pid):
                    failure, error = "cleanup_failed", "owned descendants remain active"
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
