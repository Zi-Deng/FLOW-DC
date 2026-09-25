"""Finite subprocesses with bounded output and process-group cleanup."""

import os
import selectors
import signal
import subprocess
import tempfile
import time
from contextvars import ContextVar

from flowdc_experiment_data import ExperimentError

CANCEL_CHECK = ContextVar("experiment_cancel_check", default=None)


def check_cancel():
    callback = CANCEL_CHECK.get()
    if callback is not None:
        callback()


def execute(argv, *, seconds, maximum=262144, data=b""):
    check_cancel()
    if seconds <= 0:
        raise ExperimentError("deadline_expired")
    # This CLI owns child reaping; automatic/external reapers can release a PID
    # before group cleanup. Require the default disposition before spawning.
    if signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL:
        raise ExperimentError("subprocess_child_management")
    environment = {
        k: v
        for k, v in os.environ.items()
        if k
        in ("PATH", "HOME", "USER", "LOGNAME", "SSH_AUTH_SOCK", "XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS")
    }
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    process = None
    try:
        with tempfile.TemporaryFile() as source:
            source.write(data)
            source.seek(0)
            process = subprocess.Popen(
                argv,
                stdin=source,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                env=environment,
            )
            deadline = time.monotonic() + seconds
            output = bytearray()
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while selector.get_map():
                    check_cancel()
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ExperimentError("subprocess_timeout")
                    for key, _ in selector.select(min(remaining, 0.2)):
                        chunk = os.read(key.fd, min(65536, maximum + 1 - len(output)))
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        output.extend(chunk)
                        if len(output) > maximum:
                            raise ExperimentError("subprocess_output_limit")
                # EOF does not imply exit. Observe without reaping, retaining PID
                # ownership until the process group has been signaled below.
                while os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
                    check_cancel()
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ExperimentError("subprocess_timeout")
                    time.sleep(min(remaining, 0.01))
    except OSError:
        raise ExperimentError("subprocess_unavailable") from None
    finally:
        if process is not None:
            # Signal before reaping the leader, preventing PID reuse before killpg.
            # This also stops descendants that closed stdout but kept running.
            try:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    code = process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    raise ExperimentError("subprocess_cleanup_timeout") from None
            finally:
                process.stdout.close()
    return code, bytes(output)
