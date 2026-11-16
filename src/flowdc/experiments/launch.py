"""One isolated guarded acquisition, shared by local and TaskVine experiments."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from .. import downloader, integrity
from ..config import DownloadConfig
from .common import require
from .guard import FlowGuard


def flow(config, settings, audit_path):
    import polars as pl

    urls = pl.read_parquet(config.input_path)["url"].to_list()
    with Path(audit_path).open("x", buffering=65536) as stream:
        guard = FlowGuard(
            settings["source"],
            urls,
            stream,
            settings.get("baseline_s", 0.1),
            settings.get("ignore_time", False),
            settings["body_limit_bytes"],
            latency_stop=False,
            cooldown_seconds=settings.get("user_cooldown_seconds", 0),
        )
        guard.emit("start", source=settings["source"], latency_stop_enabled=False)
        restore = guard.install()

        async def execute():
            try:
                return await downloader.run_acquisition(config)
            finally:
                if guard.sampler:
                    guard.sampler.cancel()
                    import contextlib

                    with contextlib.suppress(asyncio.CancelledError):
                        await guard.sampler

        try:
            return asyncio.run(execute())
        finally:
            restore()
            guard.emit(
                "closed",
                active=len(guard.active),
                logical_attempts=len(guard.counts),
                body_bytes=guard.body_bytes,
                stop_reason=guard.policy.reason,
            )


def main():
    settings = integrity.parse(Path(sys.argv[1]).read_bytes())
    require(Path("input.parquet").is_file(), "frozen input missing")
    downloader._setup_signal_handlers()
    if settings.get("tool"):
        from .comparator import acquire

        acquire("input.parquet", "return", settings["tool"])
    else:
        config = DownloadConfig.from_dict(settings["download"])
        flow(config, settings["experiment"], "request-audit.jsonl")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
