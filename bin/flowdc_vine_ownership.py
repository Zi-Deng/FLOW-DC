"""Linux-owned official TaskVine workers, with finite durable launch budgets."""

import asyncio
import copy
import ctypes
import os
import select
import signal
import subprocess
import time

import psutil
from flowdc_vine_cohort import validate
from flowdc_vine_native import pidfd as open_pidfd
from flowdc_vine_protocol import require, write_new


def signal_pidfd(fd, signum):
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "pidfd_send_signal", None)
    require(function is not None, "owned worker cleanup requires pidfd_send_signal")
    function.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
    function.restype = ctypes.c_int
    if function(fd, signum, None, 0) < 0:
        raise OSError(ctypes.get_errno(), "owned process signal failed")


class OwnedWorkers:
    """Linux subreaper + pinned process handles; no unrelated PID/group kills.

    All descendants belong to this dedicated fixture process. Adoption keeps
    detached task children observable after a native worker dies. PID reuse cannot
    retarget a signal, and only owned child handles are reaped.
    """

    def __init__(self, directory, executable, count):
        self.directory, self.executable, self.count = directory, executable, count
        self.roots, self.handles, self.logs, self.exits = [], {}, [], []
        self.stopping = False
        self.cohort = None
        self.launches = []
        self.excluded, self.retired, self.unknown = set(), [], set()
        libc = ctypes.CDLL(None, use_errno=True)
        require(libc.prctl(36, 1, 0, 0, 0) == 0, "local fixture requires child subreaper")
        fd = open_pidfd(os.getpid())
        try:
            signal_pidfd(fd, 0)
        finally:
            os.close(fd)

    def bind_cohort(self, value):
        require(self.cohort is None and not self.roots, "cohort already bound")
        value = validate(value, self.count)
        require(value["owner"] == "local-process-v1", "local cohort owner required")
        self.cohort = copy.deepcopy(value)
        write_new(self.directory / "worker-cohort.json", self.cohort)

    def capture(self):
        unknown = set()
        for process in psutil.Process().children(recursive=True):
            try:
                created = process.create_time()
                identity = (process.pid, created)
                if identity in self.excluded:
                    continue
                old = self.handles.get(process.pid)
                if old is not None and old["created"] == created:
                    continue
                if old is not None:
                    require(self.dead(old), "live child identity changed")
                    self.retired.append((process.pid, old))
                ancestors = {(p.pid, p.create_time()) for p in process.parents()}
                owner = next(
                    (
                        i
                        for i, root in enumerate(self.roots)
                        if identity == root.flowdc_identity or root.flowdc_identity in ancestors
                    ),
                    None,
                )
                if owner is None:
                    # Never signal/reap an unrelated manager/helper. An adopted
                    # child that was not observed under a worker is uncertainty,
                    # not evidence that a selected worker's tree has stopped.
                    unknown.add(identity)
                    continue
                fd = open_pidfd(process.pid)
                if process.create_time() != created:
                    os.close(fd)
                    raise ValueError("owned child changed identity")
                self.handles[process.pid] = {"fd": fd, "created": created, "worker": owner}
            except (psutil.NoSuchProcess, ProcessLookupError):
                continue
        self.unknown = unknown

    @staticmethod
    def dead(record):
        return bool(select.select([record["fd"]], [], [], 0)[0])

    async def watch(self):
        while not self.stopping:
            self.capture()
            await asyncio.sleep(0.025)

    def start(self, port, password, feature, *, replacement=False):
        require(self.cohort is not None, "owned worker cohort not bound")
        slots = {slot["feature"]: slot for slot in self.cohort["slots"]}
        require(feature in slots, "worker feature outside cohort")
        spent = self.launches.count(feature)
        self.capture()
        earlier = [i for i, p in enumerate(self.roots) if p.flowdc_feature == feature]
        require(spent < slots[feature]["launch_limit"], "worker launch budget exhausted")
        require(replacement == bool(spent), "invalid replacement identity")
        # Root exit alone is insufficient: all previously observed descendants must
        # be quiescent before an owner can spend a replacement launch.
        require(
            not earlier
            or (
                not self.unknown
                and all(self.dead(record) for record in self.handles.values() if record["worker"] in earlier)
            ),
            "previous worker tree is not quiescent",
        )
        identities = {p.flowdc_identity for p in self.roots}
        require(
            sum(
                not self.dead(record)
                for pid, record in self.handles.items()
                if (pid, record["created"]) in identities
            )
            < self.count,
            "worker process bound exceeded",
        )
        index = len(self.launches)
        # Spend the budget durably before process creation. A lost/spawn failure is
        # not authority for another launch. Output collisions refuse owner restart.
        write_new(self.directory / f"worker-{index}-intent.json", {"feature": feature, "index": index})
        self.launches.append(feature)
        log = (self.directory / f"worker-{index}.log").open("xb")
        self.logs.append(log)
        command = [
            str(self.executable),
            "--ssl",
            "-P",
            str(password),
            "--single-shot",
            "--parent-death",
            "--connect-timeout=20",
            "--idle-timeout=20",
            "--wall-time=170",
            "--cores=1",
            "--memory=2048",
            "--disk=8192",
            "--gpus=0",
            "--feature",
            feature,
            "--workspace",
            str(self.directory / f"scratch-{index}"),
            "--keep-workspace",
            "127.0.0.1",
            str(port),
        ]
        if not self.roots:
            # The native manager and spawn helper already exist before ready().
            self.excluded = {(p.pid, p.create_time()) for p in psutil.Process().children(recursive=True)}
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        process.flowdc_identity = (process.pid, psutil.Process(process.pid).create_time())
        process.flowdc_feature = feature
        self.roots.append(process)
        self.capture()
        write_new(
            self.directory / f"worker-{index}.json",
            {
                "command": command,
                "pid": process.pid,
                "create_time": self.handles[process.pid]["created"],
                "replacement": replacement,
            },
        )

    async def stop_tree(self, index=None):
        selected, stable = {}, 0
        for sig, seconds in ((signal.SIGTERM, 8), (signal.SIGKILL, 5)):
            until = time.monotonic() + seconds
            while time.monotonic() < until:
                self.capture()
                for pid, record in [*self.retired, *self.handles.items()]:
                    if index is None or record["worker"] == index:
                        selected[(pid, record["created"])] = record
                for record in selected.values():
                    if not self.dead(record):
                        try:
                            signal_pidfd(record["fd"], sig)
                        except ProcessLookupError:
                            pass
                stable = stable + 1 if all(self.dead(r) for r in selected.values()) else 0
                if stable >= 3:
                    require(not self.unknown, "unattributed adopted child prevents quiescence proof")
                    return {
                        "pids": sorted({pid for pid, _ in selected}),
                        "all_stopped": True,
                        "identities": [{"pid": pid, "created": created} for pid, created in sorted(selected)],
                    }
                await asyncio.sleep(0.025)
        raise ValueError("owned process tree failed to stop")

    async def close(self):
        evidence = await self.stop_tree()
        self.stopping = True
        for process in self.roots:
            self.exits.append({"pid": process.pid, "exit_code": process.wait(timeout=1)})
        # Reap adopted descendants only; ordinary Popen roots were reaped above.
        for pid in self.handles:
            if any(p.pid == pid for p in self.roots):
                continue
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
        for record in [*self.handles.values(), *(record for _, record in self.retired)]:
            os.close(record["fd"])
        for stream in self.logs:
            stream.close()
        return {**evidence, "workers": self.exits, "launches": self.launches, "cohort": self.cohort}
