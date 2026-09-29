"""Bounded wrapper-owned CLI capture; no raw provider logs are saved to disk."""

import os
import selectors
import subprocess
import time

from sessions import interruption_signals, stop_process


def capture(argv, *, cwd, env, timeout, limit=16000000):
    process = None
    output = bytearray()
    stderr_bytes = 0
    reason = None
    deadline = time.monotonic() + timeout
    with interruption_signals():
        try:
            process = subprocess.Popen(
                argv,
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ, "stdout")
                selector.register(process.stderr, selectors.EVENT_READ, "stderr")
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        reason = "provider_timeout"
                        break
                    for key, _ in selector.select(min(remaining, 0.2)):
                        block = os.read(key.fileobj.fileno(), 65536)
                        if not block:
                            selector.unregister(key.fileobj)
                        elif key.data == "stdout":
                            if len(output) + len(block) > limit:
                                reason = "stream_limit_exceeded"
                                break
                            output.extend(block)
                        else:
                            stderr_bytes += len(block)
                            if stderr_bytes > limit:
                                reason = "stream_limit_exceeded"
                                break
                    if reason:
                        break
                if not reason:
                    try:
                        process.wait(timeout=max(0.001, deadline - time.monotonic()))
                    except subprocess.TimeoutExpired:
                        reason = "provider_timeout"
        except (OSError, KeyboardInterrupt, InterruptedError):
            reason = "provider_interrupted_or_unavailable"
        finally:
            # A leader exit does not establish that its subprocesses stopped.
            if process is not None:
                stop_process(process)
                process.stdout.close()
                process.stderr.close()
    result = subprocess.CompletedProcess(argv, process.returncode if process else -1, bytes(output), b"")
    result.failure_reason = reason
    return result
