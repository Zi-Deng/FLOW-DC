"""Strict experiment inputs and owner-private, durable records (no cloud calls)."""

import fcntl
import hashlib
import io
import json
import math
import os
import re
import stat
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import flowdc_ops as ops

LIMIT = 64 * 1024 * 1024
ARCHIVE_MEMBER_LIMIT = 4096
# Maintained imagefolder tar: root directory, image subdirectory, overview, and images.
MAX_PARTITION_ROWS = ARCHIVE_MEMBER_LIMIT - 3
ROLES = ("manager", "worker", "origin")


class ExperimentError(ValueError):
    """Only fixed diagnostic codes cross the public CLI boundary."""


def require(condition, code="invalid_specification"):
    if not condition:
        raise ExperimentError(code)


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def parse(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate_json_key")
            result[key] = value
        return result

    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: require(False))
    except (ValueError, UnicodeError):
        raise ExperimentError("invalid_json") from None


def fields(value, required, optional=()):
    require(isinstance(value, dict))
    require(set(required) <= value.keys() <= set(required) | set(optional))
    return value


def integer(value, low, high):
    require(type(value) is int and low <= value <= high)
    return value


def name(value):
    require(isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9_-]{0,39}", value))
    return value


def run_id(value):
    require(isinstance(value, str) and re.fullmatch(r"exp-[0-9a-f]{32}", value), "invalid_run_id")
    return value


def guest_path(value):
    require(isinstance(value, str) and re.fullmatch(r"/[a-zA-Z0-9_./-]{1,200}", value))
    require(".." not in Path(value).parts and "//" not in value and value != "/")
    return value


def read_file(path, maximum=LIMIT, private=False):
    path = ops.absolute_path(str(path))
    # Input data can reside in a checkout, but never traverse a symlink.
    fd = os.open("/", ops.DIRECTORY_FLAGS)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, ops.DIRECTORY_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = child
        file = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        with os.fdopen(file, "rb") as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, "unsafe_input")
            if private:
                ops.private_metadata(info)
            require(info.st_size <= maximum, "input_size_limit")
            raw = stream.read(maximum + 1)
            require(len(raw) <= maximum, "input_size_limit")
            return raw
    finally:
        os.close(fd)


def specification(value):
    fields(
        value,
        ("schema_version", "state_root", "registration_id", "source", "ssh", "guest", "cases"),
        ("partitions", "fixture", "bounds"),
    )
    require(type(value["schema_version"]) is int and value["schema_version"] == 1)
    ops.absolute_path(value["state_root"])
    ops.uuid_value(value["registration_id"])
    fields(value["source"], ("repository", "revision"))
    ops.absolute_path(value["source"]["repository"])
    require(isinstance(value["source"]["revision"], str))
    ssh = fields(value["ssh"], ("user", "identity_file", "known_hosts"))
    require(isinstance(ssh["user"], str) and re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", ssh["user"]))
    require(ssh["user"] != "root", "guest_nonroot_user_required")
    for key in ("identity_file", "known_hosts"):
        ops.absolute_path(ssh[key])
    guest = fields(value["guest"], ("root", "python", "worker"), ("service_mode",))
    for key in ("root", "python", "worker"):
        guest_path(guest[key])
    guest.setdefault("service_mode", "system")
    require(guest["service_mode"] in ("system", "user"))
    require(guest["python"].endswith("/bin/python"))
    require(guest["worker"].endswith("/vine_worker"))
    fixture = value.get("fixture", False)
    require(type(fixture) is bool)
    require(fixture != ("partitions" in value))
    cases = value["cases"]
    require(isinstance(cases, list) and 1 <= len(cases) <= 8)
    names = set()
    for case in cases:
        fields(case, ("name", "config"))
        require(
            name(case["name"]) not in names
            and case["name"] != "origin"
            and not case["name"].endswith("-worker")
        )
        names.add(case["name"])
        ops.absolute_path(case["config"])
    if not fixture:
        parts = value["partitions"]
        require(isinstance(parts, list) and 1 <= len(parts) <= 32)
        for part in parts:
            fields(part, ("path", "rows"), ("expected_sha256",))
            ops.absolute_path(part["path"])
            integer(part["rows"], 1, MAX_PARTITION_ROWS)
            if "expected_sha256" in part:
                hashes = part["expected_sha256"]
                require(isinstance(hashes, list) and len(hashes) == part["rows"])
                require(all(isinstance(h, str) and re.fullmatch(r"[0-9a-f]{64}", h) for h in hashes))
    bounds = {
        "window_seconds": 1800,
        "stop_after_seconds": 900,
        "phase_seconds": 300,
        "min_case_seconds": 30,
        "deployment_min_seconds": 30,
        "collect_seconds": 60,
        "stop_seconds": 300,
        "output_bytes": 268435456,
        "memory_mb": 8192,
        "disk_mb": 2048,
        "cores": 4,
    }
    supplied = value.get("bounds", {})
    fields(supplied, (), bounds)
    bounds.update(supplied)
    for key in ("window_seconds", "stop_after_seconds", "phase_seconds", "collect_seconds", "stop_seconds"):
        integer(bounds[key], 1, 1800)
    integer(bounds["min_case_seconds"], 10, bounds["phase_seconds"])
    integer(bounds["deployment_min_seconds"], 10, bounds["phase_seconds"])
    require(600 < bounds["window_seconds"] <= 1800)
    require(bounds["phase_seconds"] + bounds["collect_seconds"] < bounds["stop_after_seconds"])
    require(bounds["stop_after_seconds"] <= bounds["window_seconds"] - 780)
    require(
        bounds["deployment_min_seconds"] + len(cases) * bounds["min_case_seconds"] + bounds["collect_seconds"]
        < bounds["stop_after_seconds"]
    )
    integer(bounds["output_bytes"], 1048576, 1073741824)
    integer(bounds["memory_mb"], 512, 32768)
    integer(bounds["disk_mb"], 256, 16384)
    integer(bounds["cores"], 1, 16)
    value["bounds"] = bounds
    value["fixture"] = fixture
    return value


# Deliberately exclude path/output/shell/resource controls owned by the runner.
INTEGER_CONFIG = {
    "concurrent_downloads": (1, 2048),
    "C_init": (1, 2048),
    "C_min": (1, 2048),
    "C_max": (1, 2048),
    "max_retry_attempts": (1, 10),
    "startup_additive_increase": (1, 100),
    "probe_bw_additive_increase": (1, 100),
    "efficiency_window": (1, 100),
}
FLOAT_CONFIG = {
    "timeout_sec",
    "retry_backoff_sec",
    "mu",
    "beta",
    "theta_50",
    "theta_95",
    "startup_theta_50",
    "startup_theta_95",
    "probe_rtt_period",
    "rtprop_window",
    "cooldown_floor",
    "alpha_ema",
    "efficiency_threshold",
}


def case_config(value):
    fields(value, ("enable_paarc",), set(INTEGER_CONFIG) | FLOAT_CONFIG | {"url_col"})
    require(type(value["enable_paarc"]) is bool)
    for key, item in value.items():
        if key in INTEGER_CONFIG:
            integer(item, *INTEGER_CONFIG[key])
        if key in FLOAT_CONFIG:
            require(type(item) in (int, float) and math.isfinite(item) and 0 < item <= 120)
        if key in ("mu", "beta", "alpha_ema", "efficiency_threshold"):
            require(item <= 1)
    require(value.get("C_min", 2) <= value.get("C_init", 8) <= value.get("C_max", 2000))
    require(
        isinstance(value.get("url_col", "url"), str)
        and re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]{0,63}", value.get("url_col", "url"))
    )
    return value


def parquet_rows(raw, column):
    try:
        import polars as pl
    except ImportError:
        raise ExperimentError("local_polars_required") from None
    try:
        frame = pl.read_parquet(io.BytesIO(raw), columns=[column], n_rows=MAX_PARTITION_ROWS + 1)
        require(1 <= frame.height <= MAX_PARTITION_ROWS, "partition_row_limit")
        require(frame[column].dtype == pl.String and frame[column].null_count() == 0, "invalid_partition")
        require(all(re.match(r"https?://[^\s]+$", url) for url in frame[column]), "invalid_partition")
        require(frame[column].n_unique() == frame.height, "duplicate_partition_urls")
        return frame.height
    except Exception as exc:
        if isinstance(exc, ExperimentError):
            raise
        raise ExperimentError("invalid_partition") from None


class Store:
    """All file operations are anchored at private directory descriptors."""

    def __init__(self, root):
        self.root = ops.absolute_path(str(root))

    @contextmanager
    def directory(self, selected=None):
        path = self.root / "runs"
        if selected is not None:
            path /= run_id(selected)
        with ops.private_directory(path) as fd:
            yield fd

    @contextmanager
    def lock(self):
        with self.directory() as parent:
            fd = os.open("experiment.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=parent)
            try:
                ops.private_metadata(os.fstat(fd))
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise ExperimentError("experiment_busy") from None
                yield
            finally:
                os.close(fd)

    def create(self):
        selected = "exp-" + uuid4().hex
        with self.directory() as parent:
            os.mkdir(selected, 0o700, dir_fd=parent)
            os.fsync(parent)
        return selected

    def write(self, selected, filename, raw, *, replace=False):
        require(re.fullmatch(r"[a-zA-Z0-9_.-]{1,100}", filename) and filename not in (".", ".."))
        with self.directory(selected) as parent:
            temporary = ".write-" + uuid4().hex
            fd = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent
            )
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                if replace:
                    ops.inspect_child(parent, filename)
                    os.replace(temporary, filename, src_dir_fd=parent, dst_dir_fd=parent)
                else:
                    os.link(temporary, filename, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
                    os.unlink(temporary, dir_fd=parent)
                os.fsync(parent)
            finally:
                try:
                    os.unlink(temporary, dir_fd=parent)
                except FileNotFoundError:
                    pass

    def read(self, selected, filename, maximum=LIMIT):
        require(isinstance(filename, str) and Path(filename).name == filename and filename not in (".", ".."))
        with self.directory(selected) as parent, ops.open_private_at(parent, filename) as fd:
            with os.fdopen(os.dup(fd), "rb") as stream:
                raw = stream.read(maximum + 1)
                require(len(raw) <= maximum, "record_size_limit")
                return raw

    def json(self, selected, filename):
        return parse(self.read(selected, filename))

    def save(self, selected, filename, value):
        self.write(selected, filename, encode(value), replace=True)

    def owner(self):
        try:
            value = self.json(None, "active-experiment.json")
        except FileNotFoundError:
            return None
        fields(value, ("run_id",))
        return None if value["run_id"] is None else run_id(value["run_id"])

    def claim(self, selected):
        owner = self.owner()
        require(owner in (None, selected), "another_experiment_incomplete")
        self.save(None, "active-experiment.json", {"run_id": selected})

    def release(self, selected):
        require(self.owner() == selected, "experiment_ownership_changed")
        self.save(None, "active-experiment.json", {"run_id": None})
