"""TaskVine C runtime in a dedicated process, independent of admission's clock.

SWIG native waits can hold the GIL. A Python thread does not isolate those waits
from an asyncio admission server. Only this child owns native manager objects.
The private multiprocessing pipe never accepts network or untrusted pickles.
"""

import asyncio
import ctypes
import multiprocessing
import os
import signal
import time
from pathlib import Path

from flowdc_staging import WORKER_FILES
from flowdc_vine_cohort import CATEGORY, NATIVE_RETRIES
from flowdc_vine_protocol import require

RUNTIME = "7.17.2"
METRICS = (
    "time_when_submitted",
    "time_when_done",
    "time_workers_execute_last",
    "time_workers_execute_all",
    "time_workers_execute_failure",
    "bytes_received",
    "bytes_sent",
)


def pidfd(pid):
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "pidfd_open", None)
    require(function is not None, "native child cleanup requires Linux pidfd")
    function.argtypes, function.restype = [ctypes.c_int, ctypes.c_uint], ctypes.c_int
    fd = function(pid, 0)
    if fd < 0:
        raise OSError(ctypes.get_errno(), "pidfd unavailable")
    return fd


def signal_fd(fd, signum):
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "pidfd_send_signal", None)
    require(function is not None, "native child cleanup requires pidfd_send_signal")
    function.argtypes, function.restype = (
        [ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint],
        ctypes.c_int,
    )
    if function(fd, signum, None, 0) < 0 and ctypes.get_errno() != 3:
        raise OSError(ctypes.get_errno(), "native child termination failed")


def stop_fd(fd):
    signal_fd(fd, signal.SIGKILL)


def native_child(connection, settings):
    import ndcctools.taskvine as vine

    manager = None
    try:
        require(vine.__version__ == RUNTIME, "native manager runtime mismatch")
        root = Path(settings["root"])
        manager = vine.Manager(
            port=settings["port"],
            run_info_path=str(root / "run-info"),
            staging_path=str(root / "staging"),
            ssl=True,
            shutdown=True,
            init_fn=lambda m: m.set_password_file(settings["password"]),
        )
        manager.disable_peer_transfers()
        require(manager.enable_disconnect_slow_workers(0) == 1, "native fast-abort disable failed")
        require(
            manager.enable_disconnect_slow_workers_category(CATEGORY, 0) == 1,
            "category fast-abort disable failed",
        )
        require(manager.set_category_mode(CATEGORY, "fixed") == 1, "native fixed allocation failed")
        environment = manager.declare_poncho(settings["package"], cache=True, peer_transfer=False)
        connection.send({"port": manager.port})
        while True:
            command = connection.recv()
            if command["operation"] == "close":
                manager.cancel_all()
                manager.__exit__(None, None, None)
                manager = None
                connection.send({"closed": True})
                break
            if command["operation"] == "wait":
                task = manager.wait(1)
                value = (
                    None
                    if task is None
                    else {
                        "task_id": task.id,
                        "result": task.result,
                        "exit_code": task.exit_code,
                        "successful": task.successful(),
                        "addrport": task.addrport,
                        "hostname": task.hostname,
                        "metrics": {key: task.get_metric(key) for key in METRICS},
                        "stdout": task.std_output or "",
                    }
                )
                connection.send(value)
                continue
            require(command["operation"] == "submit", "unknown native owner command")
            directory, spec = Path(command["directory"]), command["spec"]
            task = vine.Task("python -B flowdc_vine_worker.py --spec task.json")
            task.set_cores(1)
            task.set_memory(1024)
            task.set_disk(1 if spec.get("engineering_fault") == "sandbox-exhaustion" else 4096)
            task.set_category(CATEGORY)
            task.set_retries(NATIVE_RETRIES)
            task.set_max_forsaken(0)
            task.set_time_max(spec["deadline_s"] + 10)
            require(
                task.resources_requested.wall_time == spec["deadline_s"] + 10, "native time unit mismatch"
            )
            for name, value in (("NO_ALBUMENTATIONS_UPDATE", "1"), ("WANDB_MODE", "disabled")):
                task.set_env_var(name, value)
            task.add_execution_context(environment)
            require(command["feature"], "owned worker feature required")
            task.add_feature(command["feature"])
            for name in WORKER_FILES:
                task.add_input(
                    manager.declare_file(str(root / "source" / name), cache=True, peer_transfer=False), name
                )
            if spec.get("engineering_fault") == "forsaken":
                # Deterministic engineering fixture: a file cannot also be the
                # parent directory of another input. Native stage-in must fail.
                source = manager.declare_file(
                    str(root / "source/flowdc_vine_worker.py"), cache=False, peer_transfer=False
                )
                task.add_input(source, "cohort-collision")
                task.add_input(source, "cohort-collision/child")
            for name in ("partition.parquet", "task.json", "control-private.json"):
                task.add_input(
                    manager.declare_file(str(directory / name), cache=False, peer_transfer=False),
                    name,
                    mount_symlink=False,
                )
            task.add_output(
                manager.declare_file(str(directory / "return.tar"), cache=False, peer_transfer=False),
                "return.tar",
            )
            connection.send({"task_id": manager.submit(task)})
    except BaseException as exc:
        try:
            connection.send({"error_type": type(exc).__name__})
        except (BrokenPipeError, EOFError):
            pass
    finally:
        if manager is not None:
            manager.__exit__(None, None, None)
        connection.close()


class NativeManager:
    def __init__(self, settings):
        # Check the required cleanup capability before creating any child.
        check_fd = pidfd(os.getpid())
        try:
            signal_fd(check_fd, 0)  # Capability/permission probe only; no termination.
        finally:
            os.close(check_fd)
        context = multiprocessing.get_context("spawn")
        self.connection, child = context.Pipe()
        self.process = context.Process(target=native_child, args=(child, settings))
        self.process.start()
        try:
            self.fd = pidfd(self.process.pid)
        except BaseException:
            # multiprocessing owns this unreaped child; its identity cannot be
            # recycled until join. No numeric PID from an external record is used.
            self.process.kill()
            self.process.join(timeout=5)
            self.connection.close()
            raise
        finally:
            child.close()
        self.port = None
        self.pending = True

    async def receive(self, seconds=20):
        until = time.monotonic() + seconds
        while not self.connection.poll():
            require(self.process.exitcode is None, "native manager exited without receipt")
            require(time.monotonic() < until, "native manager control deadline")
            await asyncio.sleep(0.01)
        value = self.connection.recv()
        self.pending = False
        require(not isinstance(value, dict) or "error_type" not in value, "native manager operation failed")
        return value

    async def start(self):
        self.port = (await self.receive())["port"]

    async def call(self, operation, **fields):
        require(not self.pending, "native command still pending")
        self.connection.send({"operation": operation, **fields})
        self.pending = True
        return await self.receive()

    async def close(self):
        clean = False
        try:
            if self.pending:
                await self.receive(seconds=5)
            if self.process.exitcode is None:
                clean = (await self.call("close"))["closed"]
        except (ValueError, EOFError, BrokenPipeError):
            pass
        finally:
            until = time.monotonic() + 5
            while self.process.exitcode is None and time.monotonic() < until:
                await asyncio.sleep(0.01)
            if self.process.exitcode is None:
                # pidfd pins identity even if a prior exitcode check reaped it.
                stop_fd(self.fd)
            self.process.join(timeout=5)
            require(self.process.exitcode is not None, "native manager child failed to stop")
            os.close(self.fd)
            self.connection.close()
        return {
            "native_manager_exit": self.process.exitcode,
            "shutdown_receipt": clean,
            "worker_quiescence_proven": False,
        }
