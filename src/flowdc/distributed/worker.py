"""One source-bound TaskVine task; no cloud credentials enter the task sandbox."""

from __future__ import annotations

import asyncio
import hashlib
import shutil
import sys
from pathlib import Path

from .. import downloader, integrity
from ..config import DownloadConfig


def main():
    settings = integrity.parse(Path(sys.argv[1]).read_bytes())
    if hashlib.sha256(Path("application.zip").read_bytes()).hexdigest() != settings["package_sha256"]:
        raise ValueError("staged package changed")
    if hashlib.sha256(Path("input.parquet").read_bytes()).hexdigest() != settings["partition_sha256"]:
        raise ValueError("staged partition changed")
    if settings.get("local_registration"):
        import os
        import time

        from .protocol import write_new

        registration = {"pid": os.getpid(), "package_sha256": settings["package_sha256"]}
        prefix = settings["local_registration"]
        write_new(prefix + ".request.json", registration)
        accepted = Path(prefix + ".accepted.json")
        deadline = time.monotonic() + 10
        while not accepted.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not accepted.exists() or integrity.parse(accepted.read_bytes()) != registration:
            raise ValueError("local process ownership was not established before HTTP")
    config = DownloadConfig.from_dict(settings["download"])
    if config.input_path != "input.parquet" or config.output_dir != "native" or config.force:
        raise ValueError("invalid task-local paths")
    destination = Path("return")
    destination.mkdir()
    downloader._setup_signal_handlers()
    result = None
    error = None
    try:
        if settings.get("tool"):
            from ..experiments.comparator import acquire

            result = acquire("input.parquet", destination, settings["tool"])
            for path in Path(".").glob("tool-request-*.jsonl"):
                shutil.copy2(path, destination / path.name)
            shutil.copy2("request-audit.jsonl", destination / "request-audit.jsonl")
        elif settings.get("experiment"):
            from ..experiments.launch import flow

            result = flow(config, settings["experiment"], destination / "request-audit.jsonl")
        else:
            result = asyncio.run(downloader.run_acquisition(config))
    except (OSError, ValueError) as exc:
        error = str(exc)
    finally:
        for name in ["outcome-index.json", "overview.json"]:
            source = Path("native") / name
            if source.exists():
                shutil.copy2(source, destination / name)
        if Path("native.tar").exists():
            shutil.copy2("native.tar", destination / "native.tar")
        # Preserve first-attempt metadata/observations without loose payload aliases.
        for source in Path("native/.flowdc").rglob("*"):
            if source.is_file() and source.suffix in [".json", ".jsonl"]:
                target = destination / "journal" / source.relative_to("native/.flowdc")
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
        (destination / "worker.json").write_bytes(
            integrity.encode(
                {
                    "partition_sha256": settings["partition_sha256"],
                    "package_sha256": settings["package_sha256"],
                    "run_complete": (result.get("output_integrity", result).get("run_complete", False))
                    if result
                    else False,
                    "error": error,
                    "python": sys.version,
                }
            )
        )
    return 0 if result is not None else 1


if __name__ == "__main__":
    raise SystemExit(main())
