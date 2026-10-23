"""One isolated, guarded native public acquisition; guest identity checked first."""

import argparse
import asyncio
import heapq
import json
import os
import sys
from pathlib import Path

import polars as pl

from .common import file_hash, read_json, require
from .cooldown import user_seconds
from .prepare import guest_check
from .public_guard import FlowGuard, ToolGuard, SharedBodyBudget, latency_stop_enabled


class GuardedDownloader:
    """Picklable delegate; each native shard owns a distinct immutable audit."""

    def __init__(self, native, source, urls, audit, gate, ceiling=192, body_limit=2**30,
                 shard_size=None, shared_path=None, *, latency_stop=True, cooldown_seconds=0):
        self.native, self.source, self.urls, self.audit, self.gate = native, source, urls, audit, gate
        self.ceiling = ceiling
        self.body_limit, self.shard_size, self.shared_path = body_limit, shard_size, shared_path
        self.latency_stop = latency_stop
        self.cooldown_seconds = user_seconds({"user_cooldown_seconds": cooldown_seconds})

    def __call__(self, row):
        path, urls = Path(self.audit), self.urls
        if self.shard_size is not None:
            import pyarrow.ipc as ipc
            index, source = row
            require(type(index) is int and index >= 0, "invalid native shard identity")
            with ipc.open_file(source) as reader:
                urls = reader.read_all()["url"].to_pylist()
            require(urls == self.urls[index*self.shard_size:(index+1)*self.shard_size],
                    "native shard differs from frozen rows")
            path = path.with_name(f"tool-request-{index:06}.jsonl")
        with path.open("x", buffering=65536) as stream:
            guard = ToolGuard(
                self.source,
                urls,
                stream,
                self.gate["baseline_s"],
                ignore_time=self.gate["ignore_time"],
                ceiling=self.ceiling,
                body_limit=self.body_limit,
                shared=SharedBodyBudget(self.shared_path, self.body_limit) if self.shared_path else None,
                latency_stop=self.latency_stop,
                cooldown_seconds=self.cooldown_seconds,
            )
            guard.emit("start", process_id=os.getpid(), shard_id=row[0] if self.shard_size else None,
                       latency_stop_enabled=guard.policy.latency_stop,
                       user_cooldown_seconds=self.cooldown_seconds)
            restore = guard.install()
            try:
                return self.native(row)
            finally:
                restore()
                guard.close()


def merge_audits(paths, output, ceiling):
    """Merge independent shard clocks, recomputing global inflight counts."""
    streams = [p.open() for p in paths]
    active, attempts = set(), set()
    body, closed, reason = 0, 0, None
    try:
        output.write(json.dumps(dict(event="start", monotonic_s=0)) + "\n")
        merged = heapq.merge(*(map(json.loads, s) for s in streams), key=lambda e: e["monotonic_s"])
        last = 0
        for event in merged:
            last = event["monotonic_s"]
            kind = event["event"]
            if kind == "start":
                continue
            if kind == "closed":
                require(event["active"] == 0, "unclosed native shard")
                closed += 1
                body += event["body_bytes"]
                reason = reason or event.get("stop_reason")
                continue
            if kind == "attempt":
                require(event["url"] not in attempts, "duplicate URL across shards")
                attempts.add(event["url"])
            if kind == "dispatch":
                require(event["token"] not in active and len(active) < ceiling, "native global ceiling exceeded")
                active.add(event["token"])
            if kind == "network_end":
                require(event["token"] in active, "native shard end without start")
                active.remove(event["token"])
            if "inflight" in event:
                event["inflight"] = len(active)
            output.write(json.dumps(event) + "\n")
        require(closed == len(paths) and not active, "missing shard closure")
        output.write(json.dumps(dict(event="closed", monotonic_s=last, active=0,
                                    logical_attempts=len(attempts), body_bytes=body, stop_reason=reason)) + "\n")
    finally:
        for stream in streams:
            stream.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--study", required=True, type=Path)
    p.add_argument("--run-id", required=True)
    a = p.parse_args()
    study = a.study.resolve()
    guest_check(study)
    protocol = read_json(study / "protocol.json")
    latency_stop = latency_stop_enabled(protocol)
    cooldown_seconds = user_seconds(protocol)
    source = Path(read_json(study / "execution.json")["source_root"])
    sys.path[:0] = [str(source / "bin"), str(source)]
    directory = study / "runs" / a.run_id
    attempt = read_json(directory / "attempt.json")
    require(attempt["run_id"] == a.run_id, "attempt identity changed")
    if "protocol_sha256" in attempt:
        require(file_hash(study / "protocol.json") == attempt["protocol_sha256"], "attempt protocol changed")
    cfg = read_json(directory / "config.json")
    gate = read_json(study / "gates" / f"{attempt['source']}.json")
    urls = pl.read_parquet(directory / "input.parquet")["url"].to_list()
    body_limit = attempt.get("body_limit", 2**30)
    with (directory / "request-audit.jsonl").open("x", buffering=65536) as stream:
        if attempt["method"] == "img2dataset-1.47.0":
            import importlib.metadata

            require(importlib.metadata.version("img2dataset") == "1.47.0", "pinned tool required")
            # The pinned distributor uses spawn: install guards inside its worker.
            import importlib

            import img2dataset

            native_main = importlib.import_module("img2dataset.main")
            native_class = native_main.Downloader
            shared_path = directory / "tool-body-budget"
            SharedBodyBudget.create(shared_path, body_limit)

            def wrapped(**kwargs):
                return GuardedDownloader(
                    native_class(**kwargs),
                    attempt["source"],
                    urls,
                    str(directory / "tool-request-audit.jsonl"),
                    gate,
                    cfg["thread_count"],
                    body_limit,
                    cfg["number_sample_per_shard"],
                    str(shared_path),
                    latency_stop=latency_stop,
                    cooldown_seconds=cooldown_seconds,
                )

            native_main.Downloader = wrapped
            try:
                img2dataset.download(**cfg)
            finally:
                native_main.Downloader = native_class
            paths = sorted(directory.glob("tool-request-*.jsonl"))
            require(len(paths) == (len(urls)+cfg["number_sample_per_shard"]-1)//cfg["number_sample_per_shard"],
                    "spawned tool shard audit missing")
            merge_audits(paths, stream, cfg["thread_count"] * cfg["processes_count"])
            # Per-shard logs remain authoritative diagnostic evidence.
        else:
            import download_batch

            guard = FlowGuard(attempt["source"], urls, stream, gate["baseline_s"], gate["ignore_time"], body_limit,
                              latency_stop=latency_stop, cooldown_seconds=cooldown_seconds)
            guard.emit("start", run_id=a.run_id, latency_stop_enabled=guard.policy.latency_stop,
                       user_cooldown_seconds=cooldown_seconds)
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
