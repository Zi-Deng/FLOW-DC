"""Fixed, standalone guest protocol. Invoked only with runner-generated arguments.

No installation, credential handling, checkout mutation or general shell command.
The same bytes are hashed into the run bundle and sent over pinned SSH.
"""

import fcntl
import hashlib
import importlib
import io
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path, PurePosixPath


def check(value):
    if not value:
        raise ValueError("guest_protocol_refused")


def private_path(path):
    path = Path(path)
    check(path.is_absolute() and ".." not in path.parts)
    for parent in (*reversed(path.parents), path):
        info = parent.lstat()
        check(stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode))
        check(
            info.st_uid in (0, os.getuid(), os.stat("/").st_uid)
            and (not info.st_mode & 0o022 or info.st_mode & stat.S_ISVTX)
        )
    check(path.stat().st_uid == os.getuid() and stat.S_IMODE(path.stat().st_mode) == 0o700)
    return path


def write(path, content):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def command(args):
    result = subprocess.run(args, stdin=subprocess.DEVNULL, capture_output=True, timeout=10)
    check(result.returncode == 0)
    return result.stdout


def unit(run, role, case):
    check(re.fullmatch(r"exp-[0-9a-f]{32}", run))
    check(role in ("manager", "worker", "origin") and re.fullmatch(r"[a-z][a-z0-9_-]{0,39}", case))
    return f"flowdc-{run}-{role}-{case}.service"


def settings(root):
    return json.loads((root / "guest.json").read_bytes())


def probe(root, role, worker, disk, mode):
    check(sys.version_info >= (3, 12) and os.getuid() != 0)
    private_path(root)
    versions = {}
    packages = ("polars", "aiohttp", "tqdm") if role != "origin" else ()
    if role == "manager":
        packages += ("ndcctools.taskvine",)
    for package in packages:
        module = importlib.import_module(package)
        versions[package] = getattr(module, "__version__", "available")
    if role == "worker":
        check(Path(worker).is_file() and os.access(worker, os.X_OK))
        versions["vine_worker"] = command([worker, "--version"]).decode().strip()[:256]
        help_text = command([worker, "--help"])
        check(all(flag in help_text for flag in (b"--workspace", b"--wall-time", b"--single-shot")))
    check(shutil.disk_usage(root).free >= int(disk) * 1024 * 1024)
    check(mode in ("system", "user"))
    if mode == "user":
        check(
            command(
                ["/usr/bin/loginctl", "show-user", str(os.getuid()), "--property=Linger", "--value"]
            ).strip()
            == b"yes"
        )
        command(["/usr/bin/systemctl", "--user", "show-environment"])
    else:
        command(["sudo", "-n", "-l", "/usr/bin/systemd-run"])
        command(["sudo", "-n", "-l", "/usr/bin/systemctl"])
    command(["/usr/bin/systemd-run", "--version"])
    return {
        "service_mode": mode,
        "hostname": socket.gethostname(),
        "uid": os.getuid(),
        "python": sys.version,
        "executable": sys.executable,
        "packages": versions,
    }


def deploy(root, expected, maximum):
    private_path(root.parent)
    raw = sys.stdin.buffer.read(maximum + 1)
    check(len(raw) <= maximum and hashlib.sha256(raw).hexdigest() == expected)
    # Exclusive directory creation prevents replay and preserves incomplete deployment.
    root.mkdir(mode=0o700)
    seen, total = set(), 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        for index, item in enumerate(archive):
            path = PurePosixPath(item.name)
            check(index < 4096 and item.isfile() and not path.is_absolute() and ".." not in path.parts)
            check(item.name not in seen and path.parts and not any(c in item.name for c in ("\\", "\x00")))
            seen.add(item.name)
            total += item.size
            check(0 <= item.size <= maximum and total <= maximum)
            destination = root / item.name
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            private_path(destination.parent)
            content = archive.extractfile(item).read(item.size + 1)
            check(len(content) == item.size)
            write(destination, content)
    write(root / "bundle.sha256", expected.encode())
    return {"deployed": True}


def service_command(root, tool):
    check(tool in ("systemd-run", "systemctl"))
    mode = settings(root)["service_mode"]
    check(mode in ("system", "user"))
    if mode == "user":
        return ["/usr/bin/" + tool, "--user"]
    return ["sudo", "-n", "/usr/bin/" + tool]


@contextmanager
def service_lock(root, role, case):
    service = unit(root.name, role, case)
    fd = os.open(root / (service + ".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield service
    finally:
        os.close(fd)


def launch(root, role, case, seconds):
    with service_lock(root, role, case) as service:
        check(not (root / (service + ".cancel")).exists())
        write(root / (service + ".intent"), b"pending")
        result = launch_service(root, role, case, seconds)
        write(root / (service + ".accepted"), b"accepted")
        return result


def stop(root, role, case):
    with service_lock(root, role, case) as service:
        marker = root / (service + ".cancel")
        if not marker.exists():
            write(marker, b"cancelled")
        result = stop_service(root, role, case)
        # A killed helper can leave an unacknowledged systemd request. Do not
        # turn a momentarily absent unit into proof that it cannot still start.
        check(not (root / (service + ".intent")).exists() or (root / (service + ".accepted")).exists())
        return result


def launch_service(root, role, case, seconds):
    config = settings(root)
    service = unit(root.name, role, case)
    check(1 <= seconds <= 1800)
    bounds = config["bounds"]
    output = root / "results" / case
    output.parent.mkdir(mode=0o700, exist_ok=True)
    output.mkdir(mode=0o700)
    private_path(output)
    # Precreate as the guest user so privileged systemd does not leave root-owned logs.
    write(output / (role + ".log"), b"")
    args = [
        *service_command(root, "systemd-run"),
        "--quiet",
        f"--unit={service}",
        f"--property=RuntimeMaxSec={seconds}",
        "--property=TimeoutStopSec=10",
        "--property=KillMode=control-group",
        f"--property=MemoryMax={bounds['memory_mb']}M",
        f"--property=LimitFSIZE={bounds['output_bytes']}",
        "--property=TasksMax=256",
        f"--property=CPUQuota={bounds['cores'] * 100}%",
        "--property=UMask=0077",
        f"--property=WorkingDirectory={root}",
        f"--property=StandardOutput=append:{output / (role + '.log')}",
        f"--property=StandardError=append:{output / (role + '.log')}",
        "--setenv=PYTHONDONTWRITEBYTECODE=1",
        f"--setenv=PATH={Path(config['python']).parent}:/usr/bin:/bin",
    ]
    if config["service_mode"] == "system":
        args += ["--uid", str(os.getuid()), "--gid", str(os.getgid())]
    if role == "worker":
        args += [
            config["worker"],
            "--single-shot",
            "--wall-time",
            str(seconds),
            "--cores",
            str(bounds["cores"]),
            "--memory",
            str(bounds["memory_mb"]),
            "--disk",
            str(bounds["disk_mb"]),
            "--workspace",
            str(output / "worker"),
            config["addresses"]["manager"],
            "9123",
        ]
    else:
        args += [
            config["python"],
            str(root / "guest.py"),
            "task" if role == "manager" else "origin",
            str(root),
            role,
            case,
        ]
    command(args)
    return {"launched": service}


def service_status(root, role, case):
    service = unit(root.name, role, case)
    raw = subprocess.run(
        [
            *service_command(root, "systemctl"),
            "show",
            service,
            "--property=LoadState",
            "--property=ActiveState",
            "--property=SubState",
            "--property=Result",
            "--property=ExecMainStatus",
        ],
        capture_output=True,
        timeout=10,
    )
    values = dict(line.split("=", 1) for line in raw.stdout.decode().splitlines() if "=" in line)
    check(values.get("LoadState") in ("loaded", "not-found"))
    return values


def stop_service(root, role, case):
    service = unit(root.name, role, case)
    state = service_status(root, role, case)
    if state.get("LoadState") != "not-found":
        subprocess.run(
            [*service_command(root, "systemctl"), "stop", service], capture_output=True, timeout=15
        )
    state = service_status(root, role, case)
    check(state.get("LoadState") == "not-found" or state.get("ActiveState") in ("inactive", "failed"))
    return {"stopped": True}


def task(root, case):
    config = settings(root)
    output = root / "results" / case
    sys.path.insert(0, str(root / "bin"))
    module = importlib.import_module("TaskvineFLOWDC")
    selected = json.loads((root / "configs" / f"{case}.json").read_bytes())
    selected.update(
        port_number=9123,
        parquets_directory=str(root / "inputs" / case),
        output_directory=str(output),
        create_tar=True,
        compress_tar=True,
        create_overview=True,
        output_format="imagefolder",
        naming_mode="sequential",
        ulimit_nofile=None,
        task_cores=config["bounds"]["cores"],
        task_memory_mb=config["bounds"]["memory_mb"],
        task_disk_mb=config["bounds"]["disk_mb"],
        max_retries=0,
    )
    write(output / "resolved-config.json", json.dumps(selected).encode())
    manager = module.vine.Manager(9123)
    partitions = module.declare_parquet_files(manager, selected["parquets_directory"])
    temporary = output / "task-configs"
    temporary.mkdir(mode=0o700)
    submitted = module.submit_tasks(
        manager,
        str(root / "bin/download_batch.py"),
        str(root / "bin/single_download.py"),
        partitions,
        selected,
        str(temporary),
    )
    tasks = []
    while not manager.empty():
        finished = manager.wait(1)
        if finished is None:
            continue
        raw = (finished.output or "").encode()
        write(output / f"task-{finished.id}.log", raw[:1048576])
        tasks.append(
            {
                "id": finished.id,
                "successful": finished.successful(),
                "exit_code": finished.exit_code,
                "log_truncated": len(raw) > 1048576,
            }
        )
    write(output / "tasks.json", json.dumps({"submitted": submitted, "tasks": tasks}).encode())
    check(
        submitted == len(partitions)
        and submitted > 0
        and len(tasks) == submitted
        and all(t["successful"] and t["exit_code"] == 0 for t in tasks)
    )
    return {"tasks_completed": submitted}


def origin_server(root, port=8000):
    config = settings(root)
    counts = {}
    allowed = set(config["cases"])
    record = root / "origin.jsonl"
    fd = os.open(record, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    log = os.fdopen(fd, "w", buffering=1)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            match = re.fullmatch(r"/([a-z][a-z0-9_-]{0,39})/([0-9]{1,2})\.png", self.path)
            if not match or match[1] not in allowed or int(match[2]) >= 64:
                self.send_error(404)
                return
            check(sum(counts.values()) < 2048)
            counts[self.path] = counts.get(self.path, 0) + 1
            injected = int(match[2]) == 0 and counts[self.path] == 1
            log.write(
                json.dumps(
                    {"path": self.path, "source": self.client_address[0], "status": 503 if injected else 200}
                )
                + "\n"
            )
            if injected:
                self.send_error(503)
                return
            raw = (root / "images" / (match[2] + ".png")).read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args):
            pass

    try:
        server = HTTPServer((config["addresses"]["origin"], port), Handler)
    except BaseException:
        log.close()
        raise
    server.fixture_log = log
    return server


def origin(root):
    server = origin_server(root)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        server.fixture_log.close()


def collect(root, role, case, maximum):
    base = root if role == "origin" else root / "results" / case
    private_path(base)
    paths = [root / "origin.jsonl"] if role == "origin" else sorted(base.rglob("*"))
    if role == "worker":
        paths = [base / "worker.log"]
    # Bound packaging BEFORE producing any transfer; refuse symlinks even in logs.
    total, regular = 0, []
    for path in paths:
        info = path.lstat()
        check(not stat.S_ISLNK(info.st_mode))
        if stat.S_ISDIR(info.st_mode):
            continue
        check(stat.S_ISREG(info.st_mode) and info.st_nlink == 1)
        total += info.st_size + 1024
        check(total + 10240 <= maximum and len(regular) < 4096)
        regular.append(path)
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as archive:
        for path in regular:
            archive.add(path, arcname=str(path.relative_to(base)), recursive=False)


def main():
    os.umask(0o077)
    action, location, role, case, *extra = sys.argv[1:]
    check(role in ("manager", "worker", "origin"))
    root = Path(location)
    if action == "probe":
        value = probe(root, role, extra[0], extra[1], extra[2])
    elif action == "deploy":
        value = deploy(root, extra[0], int(extra[1]))
    else:
        private_path(root)
        check(re.fullmatch(r"exp-[0-9a-f]{32}", root.name))
        if action == "launch":
            value = launch(root, role, case, int(extra[0]))
        elif action == "status":
            value = service_status(root, role, case)
        elif action == "stop":
            value = stop(root, role, case)
        elif action == "task":
            value = task(root, case)
        elif action == "origin":
            value = origin(root)
        elif action == "collect":
            collect(root, role, case, int(extra[0]))
            return
        else:
            raise ValueError("guest_protocol_refused")
    print(json.dumps(value))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print('{"error":"guest_operation_failed"}')
        sys.exit(1)
