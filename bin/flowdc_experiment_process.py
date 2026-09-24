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
                try:
                    code = process.wait(timeout=max(0.001, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    raise ExperimentError("subprocess_timeout") from None
            return code, bytes(output)
    except OSError:
        raise ExperimentError("subprocess_unavailable") from None
    finally:
        if process is not None:
            # Also reap descendants that kept running after the group leader exited.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            process.stdout.close()
