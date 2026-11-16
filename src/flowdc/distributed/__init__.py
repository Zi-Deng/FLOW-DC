"""Package-bound TaskVine execution with manager-side output verification."""

from __future__ import annotations

import hashlib
import os
import signal
import subprocess
import sys
import time
import zipfile
from dataclasses import asdict, dataclass, replace
from importlib.resources import files
from pathlib import Path

from .. import integrity
from ..config import normalize_config
from ..partition import partition_frame
from .protocol import require


@dataclass(frozen=True)
class TaskVineConfig:
    manager_python: str
    runtime_receipt: str
    worker_binary: str | None = None
    python_executable: str = sys.executable
    workers: int = 2
    cores: int = 1
    memory_mb: int = 2048
    disk_mb: int = 8192
    timeout_s: int = 1800
    port: int = 0
    start_local_workers: bool = False
    environment_archive: str | None = None
    environment_sha256: str | None = None
    password_file: str | None = None

    def __post_init__(self):
        for name in ("workers", "cores", "memory_mb", "disk_mb", "timeout_s"):
            require(
                type(getattr(self, name)) is int and getattr(self, name) > 0,
                f"{name} must be a positive integer",
            )
        require(self.workers <= 64, "at most 64 worker partitions are supported")
        require(type(self.port) is int and 0 <= self.port <= 65535, "invalid manager port")
        require(type(self.start_local_workers) is bool, "start_local_workers must be boolean")
        require(
            not self.start_local_workers or self.worker_binary is not None,
            "local workers require worker_binary",
        )
        require(
            bool(self.environment_archive) == bool(self.environment_sha256),
            "environment archive and digest must be supplied together",
        )
        require(
            self.start_local_workers or self.environment_archive is not None,
            "remote workers require a pinned portable environment",
        )
        for name in ("manager_python", "worker_binary", "runtime_receipt"):
            value = getattr(self, name)
            if value is not None:
                require(
                    Path(value).is_absolute() and Path(value).is_file(),
                    f"{name} must name an existing absolute executable",
                )


def bundle_package(destination):
    """Stage one source artifact, excluding caches and private workspace files."""
    root = files("flowdc")

    def walk(path, prefix):
        for item in sorted(path.iterdir(), key=lambda p: p.name):
            if item.is_dir() and item.name not in ("__pycache__", "ops"):
                yield from walk(item, prefix + item.name + "/")
            elif item.is_file() and item.name.endswith((".py", ".json")):
                yield prefix + item.name, item.read_bytes()

    with zipfile.ZipFile(destination, "x", zipfile.ZIP_DEFLATED) as archive:
        for name, content in walk(root, "flowdc/"):
            info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, content)
    return hashlib.sha256(Path(destination).read_bytes()).hexdigest()


def verify_partition(directory, partition, *, archive_path=None, decode_images=False):
    """Recompute archive integrity and original-row accounting after task return."""
    import polars as pl

    expected = pl.read_parquet(partition)
    original = {r["__key__"]: r for r in expected.iter_rows(named=True)}
    snapshot = integrity.parse((directory / "outcome-index.json").read_bytes())
    rows = snapshot["rows"]
    require(
        len(rows) == len(original) and {r["row_id"] for r in rows} == set(original),
        "returned original-row population differs",
    )
    counts = {
        k: sum(r["disposition"] == k for r in rows) for k in ("verified", "failed", "skipped", "unattempted")
    }
    require(
        sum(counts.values()) == len(rows) and counts == snapshot["counts"],
        "returned disposition counts differ",
    )
    verified = [r for r in rows if r["disposition"] == "verified"]
    require(
        all(type(r["payload_bytes"]) is int and r["payload_bytes"] >= 0 for r in verified),
        "invalid credited byte count",
    )
    credited = sum(r["payload_bytes"] for r in verified)
    require(credited == snapshot["verified_payload_bytes"], "returned byte numerator differs")
    for row in rows:
        source = original[row["row_id"]]
        require(
            row["source_manifest"] == source["__flowdc_manifest__"]
            and row["source_position"] == source["__flowdc_position__"],
            "returned row identity differs",
        )
    payloads = {}
    decode_failures = 0
    if verified:
        archive = Path(archive_path) if archive_path else directory / "native.tar"
        with archive.open("rb") as stream:
            integrity.verify_archive(stream, snapshot, "native")
        import tarfile

        with tarfile.open(archive, "r:") as tar:
            for row in verified:
                metadata = integrity.parse(tar.extractfile("native/" + row["metadata"]).read())
                require(
                    metadata["row"] == integrity.row_metadata(original[row["row_id"]]),
                    "returned original metadata differs",
                )
                if decode_images:
                    from PIL import Image

                    stream = tar.extractfile("native/" + row["payload"])
                    try:
                        with Image.open(stream) as image:
                            image.verify()
                    except (OSError, ValueError):
                        decode_failures += 1
                        continue
                payloads[row["row_id"]] = {"bytes": row["payload_bytes"], "sha256": row["payload_sha256"]}
    if decode_images:
        credited = sum(r["bytes"] for r in payloads.values())
        counts = {
            **counts,
            "verified": counts["verified"] - decode_failures,
            "failed": counts["failed"] + decode_failures,
        }
    return {
        "original_rows": len(rows),
        "counts": counts,
        "credited_bytes": credited,
        "coverage": counts["verified"] / len(rows) if rows else 0.0,
        "independently_verified": True,
        "payloads": payloads,
        "decode_failures": decode_failures,
    }


def run_distributed(config, settings, *, tool=None, experiment=None):
    config = normalize_config(config)
    try:
        options = TaskVineConfig(**settings)
    except TypeError as exc:
        raise ValueError(f"Invalid TaskVine configuration: {exc}") from exc
    require(
        config.c_max // options.workers >= config.c_min, "aggregate ceiling is too small for the worker count"
    )
    from ..manifests import load_manifest

    # Validate mappings, provenance and format before consuming the destination.
    frame, _ = load_manifest(config)
    root = Path(config.output_dir).absolute()
    require(not root.exists(), "distributed destination already exists; preserve the prior attempt")
    with integrity.Files(root.parent, create=True):
        pass
    root.mkdir(parents=True, mode=0o700)
    package = root / "application.zip"
    package_sha = bundle_package(package)
    partitions = partition_frame(
        frame,
        root / "inputs",
        groups=options.workers,
        url_column=config.url_column,
        method="rows",
    )
    task_records = []
    for i, part in enumerate(partitions):
        maximum = config.c_max // options.workers + (i < config.c_max % options.workers)
        task = replace(
            config,
            input_path="input.parquet",
            input_format="parquet",
            output_dir="native",
            c_max=maximum,
            c_init=min(config.c_init, maximum),
            workers=min(config.workers or maximum, maximum),
            force=False,
            create_tar=True,
            compress_archive=False,
        )
        record = {
            "download": asdict(task),
            "partition": str(part),
            "partition_sha256": hashlib.sha256(part.read_bytes()).hexdigest(),
            "package_sha256": package_sha,
        }
        if options.start_local_workers:
            registration = root / "registration"
            registration.mkdir(exist_ok=True, mode=0o700)
            record["local_registration"] = str(registration / f"task-{i}")
        if experiment:
            record["experiment"] = {
                **experiment,
                "body_limit_bytes": experiment["body_limit_bytes"] // options.workers,
            }
        if tool:
            require(tool["threads"] >= options.workers, "not enough threads to split across workers")
            record["tool"] = {
                **tool,
                "threads": tool["threads"] // options.workers + (i < tool["threads"] % options.workers),
                "body_limit_bytes": tool["body_limit_bytes"] // options.workers,
            }
        path = root / f"task-{i}.json"
        path.write_bytes(integrity.encode(record))
        task_records.append(str(path))
    password = Path(options.password_file) if options.password_file else root / "native-password"
    if options.password_file:
        require(
            password.is_file() and not password.is_symlink() and password.stat().st_mode & 0o077 == 0,
            "native password must be an owner-private regular file",
        )
    else:
        import secrets

        with password.open("x") as stream:
            os.chmod(password, 0o600)
            stream.write(secrets.token_hex(32))
    native = {
        **asdict(options),
        "root": str(root),
        "tasks": task_records,
        "package": str(package),
        "password": str(password),
    }
    if options.environment_archive:
        with Path(options.environment_archive).open("rb") as stream:
            require(
                hashlib.file_digest(stream, "sha256").hexdigest() == options.environment_sha256,
                "portable environment digest differs",
            )
    native_path = root / "manager.json"
    native_path.write_bytes(integrity.encode(native))
    started = time.monotonic()
    with (root / "manager.log").open("xb") as log:
        process = subprocess.Popen(
            [options.manager_python, "-B", "-m", "flowdc.distributed.native", str(native_path)],
            env={**os.environ, "PYTHONPATH": str(package)},
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            code = process.wait(timeout=options.timeout_s + 45)
        except BaseException:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise
    result_path = root / "native-result.json"
    require(result_path.exists(), f"native manager failed (exit {code}); inspect manager.log")
    native_result = integrity.parse(result_path.read_bytes())
    require(native_result.get("cleanup", {}).get("all_stopped") is True, "owned worker cleanup is unverified")
    outcomes = []
    for i, part in enumerate(partitions):
        directory = root / f"part-{i}"
        if directory.is_dir() and (directory / "outcome-index.json").exists():
            worker = integrity.parse((directory / "worker.json").read_bytes())
            require(
                worker["package_sha256"] == package_sha
                and worker["partition_sha256"] == hashlib.sha256(part.read_bytes()).hexdigest(),
                "worker source/input identity differs",
            )
            result = verify_partition(directory, part, decode_images=bool(experiment))
            if experiment:
                from ..experiments.metrics import transport

                task_spec = integrity.parse(Path(task_records[i]).read_bytes())
                ceiling = (
                    task_spec["tool"]["threads"] * task_spec["tool"]["processes"]
                    if tool
                    else task_spec["download"]["c_max"]
                )
                result["transport"] = transport(directory / "request-audit.jsonl", ceiling)
            result.update(
                partition=i,
                status="complete"
                if not (result["counts"]["failed"] or result["counts"]["unattempted"])
                else "incomplete",
            )
        else:
            import polars as pl

            result = {
                "partition": i,
                "status": "failed",
                "original_rows": pl.read_parquet(part).height,
                "credited_bytes": 0,
                "independently_verified": False,
            }
        outcomes.append(result)
    elapsed = time.monotonic() - started
    credited = sum(r["credited_bytes"] for r in outcomes)
    result = {
        "backend": "taskvine",
        "elapsed_s": elapsed,
        "credited_bytes": credited,
        "goodput_Bps": credited / elapsed,
        "original_rows": sum(r["original_rows"] for r in outcomes),
        "partitions": outcomes,
        "execution_complete": code == 0 and all(r["independently_verified"] for r in outcomes),
        "run_complete": code == 0 and all(r["status"] == "complete" for r in outcomes),
        "cleanup": native_result["cleanup"],
    }
    (root / "result.json").write_bytes(integrity.encode(result))
    return result
