"""One isolated, guarded native public acquisition; guest identity checked first."""

import argparse
import asyncio
import sys
from pathlib import Path

import polars as pl

from .common import read_json, require
from .prepare import guest_check
from .public_guard import FlowGuard, ToolGuard


class GuardedDownloader:
    """Picklable native shard delegate: one shard/process per bounded job."""

    def __init__(self, native, source, urls, audit, gate, ceiling=192):
        self.native, self.source, self.urls, self.audit, self.gate = native, source, urls, audit, gate
        self.ceiling = ceiling

    def __call__(self, row):
        with Path(self.audit).open("x", buffering=65536) as stream:
            guard = ToolGuard(
                self.source,
                self.urls,
                stream,
                self.gate["baseline_s"],
                ignore_time=self.gate["ignore_time"],
                ceiling=self.ceiling,
            )
            guard.emit("start")
            restore = guard.install()
            try:
                return self.native(row)
            finally:
                restore()
                guard.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--study", required=True, type=Path)
    p.add_argument("--run-id", required=True)
    a = p.parse_args()
    study = a.study.resolve()
    guest_check(study)
    read_json(study / "protocol.json")
    source = Path(read_json(study / "execution.json")["source_root"])
    sys.path[:0] = [str(source / "bin"), str(source)]
    directory = study / "runs" / a.run_id
    attempt = read_json(directory / "attempt.json")
    require(attempt["run_id"] == a.run_id, "attempt identity changed")
    cfg = read_json(directory / "config.json")
    gate = read_json(study / "gates" / f"{attempt['source']}.json")
    urls = pl.read_parquet(directory / "input.parquet")["url"].to_list()
    with (directory / "request-audit.jsonl").open("x", buffering=65536) as stream:
        if attempt["method"] == "img2dataset-1.47.0":
            import importlib.metadata

            require(importlib.metadata.version("img2dataset") == "1.47.0", "pinned tool required")
            # The pinned distributor uses spawn: install guards inside its worker.
            import importlib

            import img2dataset

            native_main = importlib.import_module("img2dataset.main")
            native_class = native_main.Downloader

            def wrapped(**kwargs):
                return GuardedDownloader(
                    native_class(**kwargs),
                    attempt["source"],
                    urls,
                    str(directory / "tool-request-audit.jsonl"),
                    gate,
                    cfg["thread_count"],
                )

            native_main.Downloader = wrapped
            try:
                img2dataset.download(**cfg)
            finally:
                native_main.Downloader = native_class
            tool = directory / "tool-request-audit.jsonl"
            require(tool.is_file(), "spawned tool worker produced no audit")
            stream.write(tool.read_text())
            tool.unlink()
        else:
            import download_batch

            guard = FlowGuard(attempt["source"], urls, stream, gate["baseline_s"], gate["ignore_time"])
            guard.emit("start", run_id=a.run_id)
            restore = guard.install()
            try:
                sys.argv = [
                    str(source / "bin/download_batch.py"),
                    "--config",
                    str(directory / "flowdc-config.json"),
                ]
                asyncio.run(download_batch.main())
            finally:
                restore()
                if guard.sampler:
                    guard.sampler.cancel()
                guard.emit(
                    "closed",
                    active=len(guard.active),
                    logical_attempts=len(guard.counts),
                    body_bytes=guard.body_bytes,
                    stop_reason=guard.policy.reason,
                )


if __name__ == "__main__":
    main()
