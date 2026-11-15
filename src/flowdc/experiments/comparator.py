"""Pinned comparator and per-shard observations; isolated from analysis dependencies."""

import heapq
import json
import os
from pathlib import Path

from .common import require
from .cooldown import user_seconds
from .guard import SharedBodyBudget, ToolGuard


class GuardedDownloader:
    """Picklable delegate; each native shard owns a distinct immutable audit."""

    def __init__(
        self,
        native,
        source,
        urls,
        audit,
        gate,
        ceiling=192,
        body_limit=2**30,
        shard_size=None,
        shared_path=None,
        *,
        latency_stop=True,
        cooldown_seconds=0,
    ):
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
            require(
                urls == self.urls[index * self.shard_size : (index + 1) * self.shard_size],
                "native shard differs from frozen rows",
            )
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
            guard.emit(
                "start",
                process_id=os.getpid(),
                shard_id=row[0] if self.shard_size else None,
                latency_stop_enabled=guard.policy.latency_stop,
                user_cooldown_seconds=self.cooldown_seconds,
            )
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
                require(
                    event["token"] not in active and len(active) < ceiling, "native global ceiling exceeded"
                )
                active.add(event["token"])
            if kind == "network_end":
                require(event["token"] in active, "native shard end without start")
                active.remove(event["token"])
            if "inflight" in event:
                event["inflight"] = len(active)
            output.write(json.dumps(event) + "\n")
        require(closed == len(paths) and not active, "missing shard closure")
        output.write(
            json.dumps(
                dict(
                    event="closed",
                    monotonic_s=last,
                    active=0,
                    logical_attempts=len(attempts),
                    body_bytes=body,
                    stop_reason=reason,
                )
            )
            + "\n"
        )
    finally:
        for stream in streams:
            stream.close()


def acquire(input_path, destination, settings):
    """Run the pinned tool unchanged, then seal a common original-row archive."""
    import importlib
    import importlib.metadata
    import math

    import img2dataset
    import polars as pl

    from .. import integrity
    from .archive import seal_tool

    require(importlib.metadata.version("img2dataset") == "1.47.0", "img2dataset 1.47.0 is required")
    frame = pl.read_parquet(input_path)
    require("study_row_id" not in frame.columns, "study_row_id is reserved for comparator accounting")
    tool_frame = frame.select(
        [c for c in frame.columns if c not in (*integrity.PROVENANCE, "__key__")]
    ).with_columns(pl.Series("study_row_id", frame["__key__"]))
    tool_input = Path("tool-input.parquet")
    tool_frame.write_parquet(tool_input)
    urls = frame["url"].to_list()
    processes = settings["processes"]
    threads = settings["threads"]
    require(frame.height >= processes, "not enough rows to exercise every comparator process")
    output = Path("tool-output")
    audit = Path("tool-request-audit.jsonl")
    shared = Path("tool-body-budget")
    budget = SharedBodyBudget.create(shared, settings["body_limit_bytes"])
    budget.close()
    native_main = importlib.import_module("img2dataset.main")
    native_class = native_main.Downloader
    shard_size = max(1, math.ceil(frame.height / processes))

    def wrapped(**kwargs):
        return GuardedDownloader(
            native_class(**kwargs),
            settings["source"],
            urls,
            str(audit.absolute()),
            {
                "baseline_s": settings.get("baseline_s", 0.1),
                "ignore_time": settings.get("ignore_time", False),
            },
            threads,
            settings["body_limit_bytes"],
            shard_size,
            str(shared.absolute()),
            latency_stop=False,
            cooldown_seconds=settings.get("user_cooldown_seconds", 0),
        )

    native_main.Downloader = wrapped
    try:
        img2dataset.download(
            url_list=str(tool_input),
            input_format="parquet",
            url_col="url",
            output_folder=str(output),
            output_format="webdataset",
            thread_count=threads,
            processes_count=processes,
            timeout=30,
            retries=0,
            max_shard_retry=0,
            resize_mode="no",
            disable_all_reencoding=True,
            extract_exif=False,
            encode_format="jpg",
            compute_hash="sha256",
            distributor="multiprocessing",
            number_sample_per_shard=shard_size,
            user_agent_token="flowdc-public-study",
            save_additional_columns=[c for c in tool_frame.columns if c != "url"],
        )
    finally:
        native_main.Downloader = native_class
    paths = sorted(Path(".").glob("tool-request-*.jsonl"))
    require(len(paths) == math.ceil(len(urls) / shard_size), "comparator shard audits missing")
    with Path("request-audit.jsonl").open("x") as stream:
        merge_audits(paths, stream, threads * processes)
    return seal_tool(frame, tool_frame, output, Path(destination))
