"""One bounded subprocess/verification boundary for known-truth comparisons."""

import os
import signal
import subprocess
import threading
import time
from contextlib import contextmanager, nullcontext
from pathlib import Path

import psutil

from .truth import digest, encode, initial_outcomes, parse, require, truth_workload


class ResourceSampler:
    """Observed process-tree RSS/CPU at 50ms; short peaks can be missed."""
    def __init__(self, pid, *, descendants=True):
        try:
            self.parent = psutil.Process(pid)
        except psutil.NoSuchProcess:
            self.parent = None
        self.descendants = descendants
        self.stop = threading.Event()
        self.peak_rss, self.samples, self.cpu = 0, 0, {}
        self.thread = threading.Thread(target=self.watch, daemon=True)
        self.thread.start()

    def watch(self):
        if self.parent is None:
            return
        while True:
            try:
                processes = [self.parent]
                if self.descendants:
                    processes += self.parent.children(recursive=True)
                rss = 0
                for process in processes:
                    try:
                        with process.oneshot():
                            key = (process.pid, process.create_time())
                            rss += process.memory_info().rss
                            times = process.cpu_times()
                            self.cpu[key] = times.user + times.system
                    except psutil.Error:
                        continue
                self.peak_rss = max(self.peak_rss, rss)
                self.samples += 1
            except psutil.Error:
                pass
            if self.stop.wait(.05):
                return

    def close(self):
        self.stop.set()
        self.thread.join(timeout=1)
        require(not self.thread.is_alive(), "resource sampler did not stop")
        return {"peak_observed_rss_bytes": self.peak_rss, "observed_cpu_seconds": sum(self.cpu.values()),
                "samples": self.samples, "interval_s": .05,
                "interpretation": "Sampled RSS sum; shared pages may be counted more than once; brief peaks/CPU may be missed."}


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
def interruption_signals(*, raise_on_signal=True):
    """First INT/TERM requests cleanup; repeats cannot interrupt owned cleanup."""
    require(
        threading.current_thread() is threading.main_thread(),
        "worker-thread lifecycle requires an explicit main-thread interruption owner",
    )
    previous, state = {}, {"requested": False, "cleanup": False, "signal": None}

    def interrupt(signum, frame):
        first = not state["requested"]
        state["requested"] = True
        if first:
            state["signal"] = signal.Signals(signum).name
        if first and raise_on_signal and not state["cleanup"]:
            raise KeyboardInterrupt

    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.signal(sig, interrupt)
    try:
        yield state
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def _await_exit(pid, seconds, interruption=None):
    """Observe without reaping; the group leader's PID remains reserved."""
    until = time.monotonic() + seconds
    while True:
        if interruption is not None and interruption["requested"]:
            raise KeyboardInterrupt
        status = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        if status is not None:
            return status.si_status if status.si_code == os.CLD_EXITED else -status.si_status
        if time.monotonic() >= until:
            raise subprocess.TimeoutExpired(str(pid), seconds)
        time.sleep(min(0.01, max(0, until - time.monotonic())))


def run_verified(
    command,
    directory,
    truth,
    verify,
    *,
    cwd=None,
    deadline=180,
    cleanup=60,
    provenance=None,
    interruption=None,
):
    # Threads must borrow the main harness's signal state. They never silently
    # claim signal ownership or change process-global handlers themselves.
    with interruption_signals() if interruption is None else nullcontext(interruption) as interruption:
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
    require(type(deadline) in (int, float) and 0 < deadline <= truth_workload(truth).acquisition_seconds,
            "deadline exceeds finite truth workload")
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
    sampler, process_resources = None, None
    cleanup_errors = []

    def signal_owned(sig):
        try:
            _signal_group(proc.pid, sig)
        except PermissionError:
            cleanup_errors.append({"signal": sig.name, "error": "permission_denied"})

    with (directory / "stdout.log").open("xb") as stdout, (directory / "stderr.log").open("xb") as stderr:
        started = time.monotonic_ns()
        try:
            if interruption["requested"]:
                raise KeyboardInterrupt
            proc = subprocess.Popen(command, cwd=cwd, stdout=stdout, stderr=stderr, start_new_session=True)
            sampler = ResourceSampler(proc.pid)
            try:
                (directory / "process-owner.json").write_bytes(
                    encode(
                        {
                            "pid": proc.pid,
                            "pgid": proc.pid,
                            "create_time": psutil.Process(proc.pid).create_time(),
                        }
                    )
                )
            except (OSError, psutil.Error) as exc:
                failure, error = "owner_record_unavailable", type(exc).__name__
            if failure is None:
                returncode = _await_exit(proc.pid, deadline, interruption)
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
                    signal_owned(signal.SIGTERM)
                    grace = min(cleanup_until, time.monotonic() + min(5, cleanup / 2))
                    while _group_running(proc.pid) and time.monotonic() < grace:
                        time.sleep(0.01)
                    signal_owned(signal.SIGKILL)
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
        if sampler is not None:
            process_resources = sampler.close()

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
        schema=truth["schema"], manifest_sha256=truth["manifest_sha256"], original_rows=truth["original_rows"]
    )
    scope = {key: truth[key] for key in ("scope", "partition_rows", "parent_truth_sha256") if key in truth}
    index.update(scope)
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
        "schema": truth["schema"],
        **scope,
        "status": "complete" if complete else failure or "incomplete",
        "process_exit_code": returncode,
        "process_error": error,
        "cleanup_errors": cleanup_errors,
        "interruption_requested": bool(interruption["requested"]),
        "interruption_signal": interruption.get("signal"),
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
        "resources": {"acquisition_process_tree": process_resources},
        "resource_status": "sampled" if process_resources is not None else "unavailable",
        "attempt_attribution": "origin aggregate; duplicate-row attribution unavailable",
        "provenance": provenance,
    }
    (directory / "result.json").write_bytes(encode(record))
    return record
