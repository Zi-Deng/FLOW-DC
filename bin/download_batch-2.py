#!/usr/bin/env python3
"""
FLOW-DC Batch Image Downloader (Refactored)

What this script does
- Loads a tabular input (Parquet/CSV/Excel/XML via single_download_gbif.load_input_file)
- Downloads images asynchronously with:
  * bounded global concurrency (N worker tasks)
  * optional per-host “polite” control:
      - per-host token bucket pacing
      - per-host concurrency cap
      - per-host interval controller driven by TTFB distribution + 429 ratio
- Retries transient failures (429 / 5xx / certain connection errors)
- Writes a tar.gz archive of the output folder (optional)
- Writes a small JSON overview report (optional)

Compatibility notes
- This script calls single_download_gbif.download_single(). It assumes download_single:
    * can download a single URL to disk
    * returns (key, file_path, class_name, error, status_code)
    * can operate with enable_rate_limiting=False (recommended mode)
- If your download_single REQUIRES token_bucket.acquire() internally, set
  --token_bucket_in_download_single so we pass the per-host TokenBucket through.

Author: (refactor generated)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import signal
import tarfile
import time
from collections import Counter, deque
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit

import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm

from single_download_gbif import download_single, load_input_file, extract_extension

# ---------------------------
# Globals / shutdown handling
# ---------------------------

shutdown_flag = False

def _signal_handler(sig, frame):
    global shutdown_flag
    print("\n[Shutdown] Interrupt received. Attempting graceful shutdown...")
    shutdown_flag = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# Trace context used by aiohttp TraceConfig
TRACE_CTX: ContextVar[dict | None] = ContextVar("TRACE_CTX", default=None)

# ---------------------------
# Small utilities
# ---------------------------

def _now() -> float:
    return time.time()

def _sanitize_filename(name: str, max_len: int = 180) -> str:
    # Remove path separators and overly exotic chars; keep it filesystem-safe.
    name = name.strip().replace(os.sep, "_")
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("._")
    if not name:
        name = "file"
    return name[:max_len]

from typing import Optional

def _percentile(values: list[float], percentile: float) -> Optional[float]: # Important to describe how we indicate a percentile
    if not values:
        return None

    sorted_values = sorted(values)

    # Map percentile in [0, 1] onto an index in [0, n - 1]
    fractional_index = (len(sorted_values) - 1) * percentile

    lower_index = int(fractional_index)  # floor
    upper_index = min(lower_index + 1, len(sorted_values) - 1)

    # If the index is exact (or we're at the end), return the exact element
    if upper_index == lower_index:
        return sorted_values[lower_index]

    # Linear interpolation between the two surrounding points
    weight = fractional_index - lower_index
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]

    return lower_value + (upper_value - lower_value) * weight


def _is_connection_error(msg: Any) -> bool:
    if msg is None:
        return False
    s = str(msg).lower()
    patterns = [
        "connection reset by peer",
        "server disconnected",
        "connection refused",
        "cannot connect",
        "connection aborted",
        "broken pipe",
        "timeout",
    ]
    return any(p in s for p in patterns)

def _is_retryable(status_code: Optional[int], error: Any) -> bool: #NEEDS EDITING TO IMPROVE FUNCTIONALITY, status code should be checked
    # Conservative: retry on explicit policy/server signals or connection failures.
    if status_code == 429 or status_code == 408:
        return True
    if status_code is not None and status_code >= 500:
        return True
    return _is_connection_error(error)

# ---------------------------
# Config
# ---------------------------

@dataclass(frozen=True)
class Config:
    input_path: str
    output_folder: str

    input_format: str = "parquet"
    url_col: str = "photo_url"
    label_col: Optional[str] = None

    output_format: str = "imagefolder"

    concurrent_downloads: int = 256
    timeout_sec: int = 30

    enable_polite_controller: bool = False
    initial_rate: float = 100.0
    min_rate: float = 1.0
    max_rate: float = 10_000.0
    per_host_conc_init: int = 16
    per_host_conc_cap: int = 256
    control_interval_sec: float = 5.0

    # PolicyBBR parameters
    startup_growth_factor: float = 1.5        # Rate multiplier in STARTUP
    latency_degradation_factor: float = 1.5   # p50 > rtprop * this triggers exit
    headroom: float = 0.85                    # Operate at this fraction of ceiling
    probe_interval_sec: float = 60.0          # Seconds between probes in PROBE_BW
    probe_increment: float = 0.05             # +5% per probe
    backoff_ceiling_factor: float = 0.7       # Cut ceiling by 30% on 429
    backoff_cooldown_sec: float = 300.0       # 5-minute cooldown after 429
    startup_max_intervals: int = 20           # Safety cap on STARTUP duration

    max_retry_attempts: int = 3
    retry_backoff_sec: float = 2.0

    # Filenameing
    naming_mode: str = "url_based"  # url_based | sequential
    file_name_pattern: str = "{segment[-2]}"  # only used for url_based

    # Tar and reporting
    create_tar: bool = True
    create_overview: bool = True

    # Integration switch: where to apply token-bucket pacing
    token_bucket_in_download_single: bool = False

def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Asynchronous batch image downloader (FLOW-DC refactor).")

    p.add_argument("--config", type=str, help="Path to a JSON config file. If set, other args are ignored.")

    ############################################################
    p.add_argument("--input", dest="input_path", type=str, help="Path to input file (parquet/csv/excel/xml).")
    p.add_argument("--input_format", type=str, default="parquet")

    p.add_argument("--url", dest="url_col", type=str, default="photo_url")
    p.add_argument("--label", dest="label_col", type=str, default=None)

    p.add_argument("--output", dest="output_folder", type=str, help="Output folder (images are written here).")
    p.add_argument("--output_format", type=str, default="imagefolder")

    p.add_argument("--concurrent_downloads", type=int, default=256)
    p.add_argument("--timeout", dest="timeout_sec", type=int, default=30)

    p.add_argument("--enable_polite_controller", action="store_true", help="Enable per-host polite controller.") # new

    p.add_argument("--initial_rate", dest="initial_rate", type=float, default=100.0)
    p.add_argument("--min_rate", type=float, default=1.0) # new
    p.add_argument("--max_rate", type=float, default=10000.0) # new

    p.add_argument("--per_host_conc_init", type=int, default=16)
    p.add_argument("--per_host_conc_cap", type=int, default=256)
    p.add_argument("--control_interval", dest="control_interval_sec", type=float, default=5.0)

    # PolicyBBR parameters
    p.add_argument("--startup_growth_factor", type=float, default=1.5, help="Rate multiplier in STARTUP phase")
    p.add_argument("--latency_degradation_factor", type=float, default=1.5, help="p50 > rtprop * this triggers STARTUP exit")
    p.add_argument("--headroom", type=float, default=0.85, help="Operate at this fraction of ceiling rate")
    p.add_argument("--probe_interval_sec", type=float, default=60.0, help="Seconds between probes in PROBE_BW")
    p.add_argument("--probe_increment", type=float, default=0.05, help="Rate increment per probe (+5%)")
    p.add_argument("--backoff_ceiling_factor", type=float, default=0.7, help="Cut ceiling by this factor on 429")
    p.add_argument("--backoff_cooldown_sec", type=float, default=300.0, help="Cooldown seconds after 429")
    p.add_argument("--startup_max_intervals", type=int, default=20, help="Safety cap on STARTUP duration")

    p.add_argument("--max_retry_attempts", type=int, default=3)
    p.add_argument("--retry_backoff_sec", type=float, default=2.0)

    p.add_argument("--naming_mode", type=str, default="url_based", choices=["url_based", "sequential"])
    p.add_argument("--file_name_pattern", type=str, default="{segment[-2]}")

    p.add_argument("--no_tar", action="store_true", help="Disable tar.gz creation.")
    p.add_argument("--no_overview", action="store_true", help="Disable overview JSON creation.")

    p.add_argument(
        "--token_bucket_in_download_single",
        action="store_true",
        help="If set, download_single is responsible for awaiting token_bucket.acquire().",
    )

    args = p.parse_args()

    if args.config:
        cfg_path = Path(args.config)
        with cfg_path.open("r") as f:
            data = json.load(f)
        # JSON keys use your original naming; map to Config fields.
        mapped = {
            "input_path": data["input"],
            "output_folder": data["output"],
            "input_format": data.get("input_format", "parquet"),
            "output_format": data.get("output_format", "imagefolder"),
            "url_col": data.get("url", "photo_url"),
            "label_col": data.get("label", None),
            "concurrent_downloads": int(data.get("concurrent_downloads", 1000)),
            "timeout_sec": int(data.get("timeout", 30)),
            "enable_polite_controller": bool(data.get("enable_polite_controller", True)),
            "initial_rate": float(data.get("initial_rate", data.get("rate_limit", 100.0))),
            "min_rate": float(data.get("min_rate", 1.0)),
            "max_rate": float(data.get("max_rate", 10000.0)),
            "per_host_conc_init": int(data.get("per_host_conc_init", 16)),
            "per_host_conc_cap": int(data.get("per_host_conc_cap", 512)),
            "control_interval_sec": float(data.get("control_interval_sec", 5.0)),
            # PolicyBBR parameters
            "startup_growth_factor": float(data.get("startup_growth_factor", 1.5)),
            "latency_degradation_factor": float(data.get("latency_degradation_factor", 1.5)),
            "headroom": float(data.get("headroom", 0.85)),
            "probe_interval_sec": float(data.get("probe_interval_sec", 60.0)),
            "probe_increment": float(data.get("probe_increment", 0.05)),
            "backoff_ceiling_factor": float(data.get("backoff_ceiling_factor", 0.7)),
            "backoff_cooldown_sec": float(data.get("backoff_cooldown_sec", 300.0)),
            "startup_max_intervals": int(data.get("startup_max_intervals", 20)),
            # Other parameters
            "max_retry_attempts": int(data.get("max_retry_attempts", 3)),
            "retry_backoff_sec": float(data.get("retry_backoff_sec", 2.0)),
            "naming_mode": data.get("naming_mode", "sequential"),
            "file_name_pattern": data.get("file_name_pattern", "{segment[-2]}"),
            "create_tar": bool(data.get("create_tar", True)),
            "create_overview": bool(data.get("create_overview", True)),
            "token_bucket_in_download_single": bool(data.get("token_bucket_in_download_single", False)),
        }
        return Config(
            **mapped,
        )

    if not args.input_path or not args.output_folder:
        p.error("--input and --output are required unless --config is provided.")

    return Config(
        input_path=args.input_path,
        output_folder=args.output_folder,
        input_format=args.input_format,
        url_col=args.url_col,
        label_col=args.label_col,
        output_format=args.output_format,
        concurrent_downloads=args.concurrent_downloads,
        timeout_sec=args.timeout_sec,
        enable_polite_controller=args.enable_polite_controller,
        initial_rate=args.initial_rate,
        min_rate=args.min_rate,
        max_rate=args.max_rate,
        per_host_conc_init=args.per_host_conc_init,
        per_host_conc_cap=args.per_host_conc_cap,
        control_interval_sec=args.control_interval_sec,
        # PolicyBBR parameters
        startup_growth_factor=args.startup_growth_factor,
        latency_degradation_factor=args.latency_degradation_factor,
        headroom=args.headroom,
        probe_interval_sec=args.probe_interval_sec,
        probe_increment=args.probe_increment,
        backoff_ceiling_factor=args.backoff_ceiling_factor,
        backoff_cooldown_sec=args.backoff_cooldown_sec,
        startup_max_intervals=args.startup_max_intervals,
        # Other parameters
        max_retry_attempts=args.max_retry_attempts,
        retry_backoff_sec=args.retry_backoff_sec,
        naming_mode=args.naming_mode,
        file_name_pattern=args.file_name_pattern,
        create_tar=not args.no_tar,
        create_overview=not args.no_overview,
        token_bucket_in_download_single=args.token_bucket_in_download_single,
    )

# ---------------------------
# I/O validation
# ---------------------------

def validate_and_load(cfg: Config) -> pd.DataFrame:
    in_path = Path(cfg.input_path)
    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    out_dir = Path(cfg.output_folder)
    if out_dir.exists():
        print(f"[I/O] Output folder exists; deleting: {out_dir}")
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Determine input format if not explicitly consistent
    input_format = cfg.input_format
    if input_format is None:
        input_format = in_path.suffix.lstrip(".").lower()

    df = load_input_file(str(in_path), input_format)

    if cfg.url_col not in df.columns:
        raise ValueError(f"URL column '{cfg.url_col}' not in input file columns: {list(df.columns)[:30]}...")

    if cfg.label_col is not None and cfg.label_col not in df.columns:
        raise ValueError(f"Label column '{cfg.label_col}' not in input file columns: {list(df.columns)[:30]}...")

    df = df.dropna(subset=[cfg.url_col]).copy()
    df[cfg.url_col] = df[cfg.url_col].astype(str).str.strip()
    df = df[df[cfg.url_col].str.len() > 0]

    if df.empty:
        raise ValueError("No valid URLs found after filtering.")

    # Preserve stable keys across retries
    df["__key__"] = df.index.astype(str)

    return df

# ---------------------------
# aiohttp tracing (TTFB proxy)
# ---------------------------

def build_trace_config() -> aiohttp.TraceConfig:
    """
    Write per-request timings into TRACE_CTX (ContextVar).
    We approximate TTFB as time to first response chunk.
    """
    trace = aiohttp.TraceConfig()

    async def _ctx():
        return TRACE_CTX.get()

    async def on_request_start(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        d["t0"] = time.monotonic()
        d["ttfb"] = None
        d["exc"] = None

    async def on_response_chunk_received(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        if d.get("ttfb") is None and d.get("t0") is not None:
            d["ttfb"] = time.monotonic() - d["t0"]

    async def on_request_exception(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        d["exc"] = str(params.exception)

    trace.on_request_start.append(on_request_start)
    trace.on_response_chunk_received.append(on_response_chunk_received)
    trace.on_request_exception.append(on_request_exception)
    return trace

# ---------------------------
# Rate / concurrency primitives
# ---------------------------

class TokenBucket:
    """
    Async token bucket, used as per-host pacer.

    Capacity scales with rate to keep burst duration ~2 seconds (min 100 tokens).
    """
    def __init__(self, rate: float, capacity: Optional[int] = None):
        self._rate = max(1.0, float(rate))
        self._capacity = capacity if capacity is not None else max(100, int(self._rate * 2))
        self._tokens = float(self._capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    def get_rate(self) -> float:
        return float(self._rate)

    def adjust_rate(self, new_rate: float, reason: str = "") -> None:
        new_rate = max(1.0, float(new_rate))
        if abs(new_rate - self._rate) < 0.1:
            return
        self._rate = new_rate
        self._capacity = max(100, int(self._rate * 2))
        self._tokens = min(self._tokens, float(self._capacity))
        r = f" ({reason})" if reason else ""
        print(f"[PolicyBBR] rate={self._rate:.2f} req/s cap={self._capacity}{r}")

    async def acquire(self) -> None:
        while not shutdown_flag:
            async with self._lock:
                self._refill_locked()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                sleep_s = max(0.005, (1.0 - self._tokens) / self._rate)
            await asyncio.sleep(min(0.05, sleep_s))

    def _refill_locked(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed <= 0:
            return
        self._tokens = min(float(self._capacity), self._tokens + elapsed * self._rate)
        self._last = now

class AsyncSemaphoreLimiter:
    """A simple adjustable semaphore-like limiter."""
    def __init__(self, initial: int, cap: int):
        self._limit = max(2, int(initial))
        self._cap = max(2, int(cap))
        self._inflight = 0
        self._cond = asyncio.Condition()

    async def acquire(self) -> None:
        async with self._cond:
            while self._inflight >= self._limit and not shutdown_flag:
                await self._cond.wait()
            self._inflight += 1

    async def release(self) -> None:
        async with self._cond:
            self._inflight = max(0, self._inflight - 1)
            self._cond.notify_all()

    def get_limit(self) -> int:
        return int(self._limit)

    def set_limit(self, new_limit: int) -> None:
        new_limit = max(2, min(int(new_limit), self._cap))
        if new_limit != self._limit:
            print(f"[PolicyBBR] conc {self._limit} -> {new_limit}")
            self._limit = new_limit

# ---------------------------
# Host signals + controller
# ---------------------------

class HostSignals:
    """Per-host metrics aggregated per control interval."""
    def __init__(self, interval_sec: float, q_hist_seconds: float = 300.0):
        self.interval_sec = float(interval_sec)
        self._lock = asyncio.Lock()

        self._ttfb_ok: list[float] = []
        self._total = 0
        self._n429 = 0
        self._bytes = 0
        self.last_429_time: Optional[float] = None

        maxlen = max(10, int(q_hist_seconds / self.interval_sec))
        self.q_history: deque[float] = deque(maxlen=maxlen)

    async def record(self, status_code: Optional[int], ttfb: Optional[float], bytes_downloaded: int = 0) -> None:
        async with self._lock:
            self._total += 1
            self._bytes += int(bytes_downloaded or 0)

            if status_code == 429:
                self._n429 += 1
                self.last_429_time = _now()

            if status_code is not None and status_code < 400 and ttfb is not None:
                self._ttfb_ok.append(float(ttfb))

    async def finish_interval(self, now: float) -> dict[str, Any]:
        async with self._lock:
            total = self._total
            n429 = self._n429
            ttfb_ok = self._ttfb_ok
            interval_bytes = self._bytes

            self._total = 0
            self._n429 = 0
            self._ttfb_ok = []
            self._bytes = 0

        r429 = (n429 / total) if total > 0 else 0.0
        T_since429 = (now - self.last_429_time) if self.last_429_time else float("inf")

        p10 = _percentile(ttfb_ok, 0.10) if len(ttfb_ok) >= 10 else None
        p50 = _percentile(ttfb_ok, 0.50) if len(ttfb_ok) >= 10 else None
        p95 = _percentile(ttfb_ok, 0.95) if len(ttfb_ok) >= 10 else None
        q = (p95 - p10) if (p95 is not None and p10 is not None) else None
        q_baseline = (sorted(self.q_history)[len(self.q_history)//2] if len(self.q_history) >= 6 else None)

        return {
            "total": total,
            "n429": n429,
            "r429": r429,
            "T_since429": T_since429,
            "p10": p10,
            "p50": p50,
            "p95": p95,
            "q": q,
            "q_baseline": q_baseline,
            "bytes": interval_bytes,
        }

class HostPolicyBBRController:
    """
    Policy-Aware BBR Controller for rate-limited APIs.
    
    Discovers optimal throughput via STARTUP phase, uses latency as early warning
    before 429s, and maintains headroom below discovered ceiling.
    
    States:
        STARTUP: Exponential growth (1.5x) to discover ceiling via latency degradation
        DRAIN: Settle at ceiling * headroom after ceiling discovery
        PROBE_BW: Steady state with conservative probing (+5% every 60s)
        PROBE_RTT: Refresh RTprop estimate by reducing concurrency briefly
        BACKOFF: Aggressive retreat after 429 with long cooldown
    """
    STARTUP = "STARTUP"
    DRAIN = "DRAIN"
    PROBE_BW = "PROBE_BW"
    PROBE_RTT = "PROBE_RTT"
    BACKOFF = "BACKOFF"

    def __init__(
        self,
        host: str,
        tb: TokenBucket,
        conc: AsyncSemaphoreLimiter,
        signals: HostSignals,
        *,
        min_rate: float,
        max_rate: float,
        per_host_conc_cap: int = 256,
        # PolicyBBR parameters
        startup_growth_factor: float = 1.5,
        latency_degradation_factor: float = 1.5,
        headroom: float = 0.85,
        probe_interval_sec: float = 60.0,
        probe_increment: float = 0.05,
        backoff_ceiling_factor: float = 0.7,
        backoff_cooldown_sec: float = 300.0,
        startup_max_intervals: int = 20,
    ):
        self.host = host
        self.tb = tb
        self.conc = conc
        self.signals = signals

        self.min_rate = float(min_rate)
        self.max_rate = float(max_rate)
        self.per_host_conc_cap = int(per_host_conc_cap)

        # PolicyBBR parameters
        self.startup_growth_factor = float(startup_growth_factor)
        self.latency_degradation_factor = float(latency_degradation_factor)
        self.headroom = float(headroom)
        self.probe_interval_sec = float(probe_interval_sec)
        self.probe_increment = float(probe_increment)
        self.backoff_ceiling_factor = float(backoff_ceiling_factor)
        self.backoff_cooldown_sec = float(backoff_cooldown_sec)
        self.startup_max_intervals = int(startup_max_intervals)

        # State
        self.state = self.STARTUP
        self.safe_rate = float(self.tb.get_rate())
        
        # PolicyBBR tracking
        self.rtprop: Optional[float] = None  # Min observed TTFB (baseline latency)
        self.ceiling_rate: float = float('inf')  # Upper bound discovered during STARTUP
        self.policy_bw: float = 0.0  # Max safe rate observed without 429/latency degradation
        
        # STARTUP tracking
        self._startup_intervals = 0
        self._startup_baseline_p50: Optional[float] = None
        
        # PROBE_BW tracking
        self._last_probe_time: float = 0.0
        self._probe_prev_rate: Optional[float] = None
        
        # BACKOFF tracking
        self._backoff_until: float = 0.0
        self._last_backoff_time: float = 0.0
        self._min_backoff_interval_sec: float = 0.5  # Minimum time between successive backoffs
        
        # PROBE_RTT tracking
        self._last_probe_rtt_time: float = 0.0
        self._saved_rate_for_probe_rtt: Optional[float] = None

    def _set_rate(self, new_rate: float, reason: str) -> None:
        new_rate = max(self.min_rate, min(self.max_rate, float(new_rate)))
        self.safe_rate = new_rate
        self.tb.adjust_rate(new_rate, f"{self.host}:{reason}")

    def _set_conc(self, new_conc: int, reason: str) -> None:
        new_conc = max(4, min(int(new_conc), self.per_host_conc_cap))
        self.conc.set_limit(new_conc)

    def _update_rtprop(self, p10: Optional[float]) -> None:
        """Update RTprop (min observed latency) from p10 TTFB."""
        if p10 is not None and p10 > 0:
            if self.rtprop is None:
                self.rtprop = p10
            else:
                self.rtprop = min(self.rtprop, p10)

    def _derive_concurrency_from_bdp(self) -> None:
        """Set concurrency based on BDP = rate * RTprop."""
        if self.rtprop is not None and self.rtprop > 0:
            target_conc = int(max(4, self.safe_rate * self.rtprop * 2.0))
            self._set_conc(min(target_conc, self.per_host_conc_cap), "bdp")

    def _is_latency_degraded(self, p50: Optional[float]) -> bool:
        """Check if latency has degraded significantly from baseline."""
        if self.rtprop is None or p50 is None:
            return False
        return p50 > self.rtprop * self.latency_degradation_factor

    async def step_interval(self, snap: dict[str, Any], now: float) -> None:
        had_429 = snap["n429"] > 0
        p10 = snap["p10"]
        p50 = snap["p50"]
        
        # Always update RTprop (min latency baseline)
        self._update_rtprop(p10)
        
        # Always derive concurrency from BDP (except in PROBE_RTT)
        if self.state != self.PROBE_RTT:
            self._derive_concurrency_from_bdp()
        
        latency_degraded = self._is_latency_degraded(p50)
        
        # ========== STARTUP PHASE ==========
        if self.state == self.STARTUP:
            self._startup_intervals += 1
            
            # Exit on 429 → set ceiling, enter BACKOFF
            if had_429:
                self._last_backoff_time = now
                self.ceiling_rate = self.safe_rate * self.backoff_ceiling_factor
                self._set_rate(self.ceiling_rate * self.headroom, "startup_429")
                self._backoff_until = now + self.backoff_cooldown_sec
                self.state = self.BACKOFF
                print(f"[PolicyBBR] {self.host}: STARTUP→BACKOFF on 429 at rate={self.safe_rate:.1f}")
                return
            
            # Establish baseline latency on first good sample
            if self._startup_baseline_p50 is None and p50 is not None:
                self._startup_baseline_p50 = p50
                print(f"[PolicyBBR] {self.host}: STARTUP baseline p50={p50*1000:.1f}ms")
            
            # Exit if latency degraded → set ceiling, enter DRAIN
            if latency_degraded:
                self.ceiling_rate = self.safe_rate
                self._set_rate(self.ceiling_rate * self.headroom, "startup_latency")
                self.state = self.DRAIN
                print(f"[PolicyBBR] {self.host}: STARTUP→DRAIN on latency at ceiling={self.ceiling_rate:.1f}")
                return
            
            # Exit if near max_rate → enter DRAIN
            if self.safe_rate >= self.max_rate * 0.95:
                self.ceiling_rate = self.max_rate
                self._set_rate(self.ceiling_rate * self.headroom, "startup_max")
                self.state = self.DRAIN
                print(f"[PolicyBBR] {self.host}: STARTUP→DRAIN at max_rate={self.max_rate}")
                return
            
            # Safety cap on STARTUP duration
            if self._startup_intervals >= self.startup_max_intervals:
                self.ceiling_rate = self.safe_rate
                self._set_rate(self.ceiling_rate * self.headroom, "startup_timeout")
                self.state = self.DRAIN
                print(f"[PolicyBBR] {self.host}: STARTUP→DRAIN after {self._startup_intervals} intervals")
                return
            
            # Otherwise, grow rate exponentially
            new_rate = min(self.safe_rate * self.startup_growth_factor, self.max_rate)
            self._set_rate(new_rate, "startup_grow")
            
            # Track policy_bw (max safe rate observed)
            self.policy_bw = max(self.policy_bw, self.safe_rate)
            return
        
        # ========== DRAIN PHASE ==========
        elif self.state == self.DRAIN:
            # Exit on 429 → enter BACKOFF
            if had_429:
                self._last_backoff_time = now
                self.ceiling_rate *= self.backoff_ceiling_factor
                self._set_rate(self.ceiling_rate * self.headroom, "drain_429")
                self._backoff_until = now + self.backoff_cooldown_sec
                self.state = self.BACKOFF
                print(f"[PolicyBBR] {self.host}: DRAIN→BACKOFF on 429")
                return
            
            # Wait for latency to stabilize before entering PROBE_BW
            if not latency_degraded:
                self._last_probe_time = now
                self._last_probe_rtt_time = now
                self.state = self.PROBE_BW
                print(f"[PolicyBBR] {self.host}: DRAIN→PROBE_BW at rate={self.safe_rate:.1f}")
            return
        
        # ========== PROBE_BW PHASE (steady state) ==========
        elif self.state == self.PROBE_BW:
            # Exit on 429 → enter BACKOFF
            if had_429:
                self._last_backoff_time = now
                self.ceiling_rate *= self.backoff_ceiling_factor
                self._set_rate(self.ceiling_rate * self.headroom, "probe_bw_429")
                self._backoff_until = now + self.backoff_cooldown_sec
                self.state = self.BACKOFF
                print(f"[PolicyBBR] {self.host}: PROBE_BW→BACKOFF on 429")
                return
            
            # If latency degraded, revert any probe and stay in PROBE_BW
            if latency_degraded and self._probe_prev_rate is not None:
                self._set_rate(self._probe_prev_rate, "probe_abort_latency")
                self._probe_prev_rate = None
            
            # Update policy_bw if stable
            if not latency_degraded and not had_429:
                self.policy_bw = max(self.policy_bw, self.safe_rate)
            
            # Periodically probe for more bandwidth
            if now - self._last_probe_time > self.probe_interval_sec:
                if not latency_degraded and self.safe_rate < self.ceiling_rate:
                    self._probe_prev_rate = self.safe_rate
                    probe_rate = min(self.safe_rate * (1.0 + self.probe_increment), self.ceiling_rate)
                    self._set_rate(probe_rate, "probe_up")
                    self._last_probe_time = now
                    print(f"[PolicyBBR] {self.host}: PROBE_BW probing to {probe_rate:.1f}")
            
            # Periodically enter PROBE_RTT to refresh RTprop
            if now - self._last_probe_rtt_time > self.probe_interval_sec * 2:
                self._saved_rate_for_probe_rtt = self.safe_rate
                self._set_conc(4, "probe_rtt_enter")  # Reduce concurrency to get unqueued RTT
                self.state = self.PROBE_RTT
                print(f"[PolicyBBR] {self.host}: PROBE_BW→PROBE_RTT")
            return
        
        # ========== PROBE_RTT PHASE ==========
        elif self.state == self.PROBE_RTT:
            # 429 in PROBE_RTT → enter BACKOFF
            if had_429:
                self._last_backoff_time = now
                self.ceiling_rate *= self.backoff_ceiling_factor
                self._set_rate(self.ceiling_rate * self.headroom, "probe_rtt_429")
                self._backoff_until = now + self.backoff_cooldown_sec
                self.state = self.BACKOFF
                print(f"[PolicyBBR] {self.host}: PROBE_RTT→BACKOFF on 429")
                return
            
            # After one interval, return to PROBE_BW
            self._last_probe_rtt_time = now
            if self._saved_rate_for_probe_rtt is not None:
                self._set_rate(self._saved_rate_for_probe_rtt, "probe_rtt_exit")
            self._derive_concurrency_from_bdp()  # Restore concurrency
            self.state = self.PROBE_BW
            print(f"[PolicyBBR] {self.host}: PROBE_RTT→PROBE_BW")
            return
        
        # ========== BACKOFF PHASE ==========
        elif self.state == self.BACKOFF:
            # CRITICAL: Even during backoff, if we still hit 429, back off MORE.
            # The cooldown blocks upward probing, not downward protection.
            # Use minimum interval to prevent instant collapse from many concurrent 429s.
            # Apply backoff factor up to 3 times based on number of 429s in this interval.
            n429 = snap["n429"]
            if n429 > 0 and (now - self._last_backoff_time >= self._min_backoff_interval_sec):
                self._last_backoff_time = now
                # Apply backoff factor up to 3 times (0.7^1, 0.7^2, or 0.7^3)
                backoff_multiplier = min(n429, 3)
                combined_factor = self.backoff_ceiling_factor ** backoff_multiplier
                self.ceiling_rate *= combined_factor
                new_rate = max(self.min_rate, self.ceiling_rate * self.headroom)
                self._set_rate(new_rate, f"backoff_429x{backoff_multiplier}")
                # Extend cooldown since we haven't found safe rate yet
                self._backoff_until = now + self.backoff_cooldown_sec
                print(f"[PolicyBBR] {self.host}: BACKOFF {n429} 429s (x{backoff_multiplier}), ceiling→{self.ceiling_rate:.1f}")
                return
            
            # Stay in BACKOFF until cooldown expires
            if now >= self._backoff_until:
                self._last_probe_time = now
                self._last_probe_rtt_time = now
                self.state = self.PROBE_BW
                print(f"[PolicyBBR] {self.host}: BACKOFF→PROBE_BW after cooldown")
            return

class PerHostControllerManager:
    def __init__(
        self,
        *,
        interval_sec: float,
        initial_rate: float,
        min_rate: float,
        max_rate: float,
        per_host_conc_init: int,
        per_host_conc_cap: int,
        # PolicyBBR parameters
        startup_growth_factor: float = 1.5,
        latency_degradation_factor: float = 1.5,
        headroom: float = 0.85,
        probe_interval_sec: float = 60.0,
        probe_increment: float = 0.05,
        backoff_ceiling_factor: float = 0.7,
        backoff_cooldown_sec: float = 300.0,
        startup_max_intervals: int = 20,
    ):
        self.interval_sec = float(interval_sec)
        self.initial_rate = float(initial_rate)
        self.min_rate = float(min_rate)
        self.max_rate = float(max_rate)
        self.per_host_conc_init = int(per_host_conc_init)
        self.per_host_conc_cap = int(per_host_conc_cap)
        
        # PolicyBBR parameters
        self.startup_growth_factor = float(startup_growth_factor)
        self.latency_degradation_factor = float(latency_degradation_factor)
        self.headroom = float(headroom)
        self.probe_interval_sec = float(probe_interval_sec)
        self.probe_increment = float(probe_increment)
        self.backoff_ceiling_factor = float(backoff_ceiling_factor)
        self.backoff_cooldown_sec = float(backoff_cooldown_sec)
        self.startup_max_intervals = int(startup_max_intervals)

        self._lock = asyncio.Lock()
        self._hosts: dict[str, HostPolicyBBRController] = {}

    async def get(self, url: str) -> HostPolicyBBRController:
        host = urlsplit(url).netloc.lower() or "unknown"
        async with self._lock:
            if host not in self._hosts:
                tb = TokenBucket(rate=self.initial_rate)
                sig = HostSignals(interval_sec=self.interval_sec)
                conc = AsyncSemaphoreLimiter(initial=self.per_host_conc_init, cap=self.per_host_conc_cap)
                self._hosts[host] = HostPolicyBBRController(
                    host=host,
                    tb=tb,
                    conc=conc,
                    signals=sig,
                    min_rate=self.min_rate,
                    max_rate=self.max_rate,
                    per_host_conc_cap=self.per_host_conc_cap,
                    # PolicyBBR parameters
                    startup_growth_factor=self.startup_growth_factor,
                    latency_degradation_factor=self.latency_degradation_factor,
                    headroom=self.headroom,
                    probe_interval_sec=self.probe_interval_sec,
                    probe_increment=self.probe_increment,
                    backoff_ceiling_factor=self.backoff_ceiling_factor,
                    backoff_cooldown_sec=self.backoff_cooldown_sec,
                    startup_max_intervals=self.startup_max_intervals,
                )
            return self._hosts[host]

    async def all(self) -> list[HostPolicyBBRController]:
        async with self._lock:
            return list(self._hosts.values())

async def per_host_controller_loop(mgr: PerHostControllerManager, interval_sec: float) -> None:
    print(f"[PolicyBBR] Per-host controller loop started (interval={interval_sec}s)")
    while not shutdown_flag:
        await asyncio.sleep(interval_sec)
        if shutdown_flag:
            break
        now = _now()
        ctrls = await mgr.all()
        for ctrl in ctrls:
            snap = await ctrl.signals.finish_interval(now)
            if snap["total"] == 0:
                continue
            await ctrl.step_interval(snap, now)

# ---------------------------
# Filename allocation
# ---------------------------

class SequentialNamer:
    """Stable sequential namer keyed by __key__ so retries do not change filenames."""
    def __init__(self):
        self._lock = asyncio.Lock()
        self._counter = 0
        self._map: dict[str, str] = {}

    async def filename_for(self, key: str, url: str) -> str:
        async with self._lock:
            if key in self._map:
                return self._map[key]
            _, ext = extract_extension(str(url))
            if not ext:
                ext = ".jpg"
            self._counter += 1
            fn = f"{self._counter:08d}{ext}"
            self._map[key] = fn
            return fn

def render_filename(pattern: str, url: str, key: str) -> str:
    seg = [s for s in urlsplit(url).path.split("/") if s]
    ext = Path(urlsplit(url).path).suffix or ""
    safe_key = _sanitize_filename(key)
    env = {"segment": seg, "ext": ext, "key": safe_key}
    try:
        stem = pattern.format(**env)
    except Exception:
        stem = safe_key
    stem = _sanitize_filename(stem)
    if ext and not stem.endswith(ext):
        stem = f"{stem}{ext}"
    if not Path(stem).suffix:
        stem = f"{stem}.jpg"
    return stem

# ---------------------------
# Download execution (bounded)
# ---------------------------

@dataclass
class DownloadOutcome:
    key: str
    url: str
    success: bool
    file_path: Optional[str]
    class_name: Optional[str]
    status_code: Optional[int]
    error: Optional[str]
    bytes_downloaded: int = 0

async def download_one(
    *,
    row: pd.Series,
    cfg: Config,
    session: aiohttp.ClientSession,
    total_bytes: list[int],
    mgr: Optional[PerHostControllerManager],
    sequential_namer: SequentialNamer,
    global_written_paths: dict[str, str],
) -> DownloadOutcome:
    url = str(row[cfg.url_col]).strip()
    key = str(row.get("__key__", row.name))
    class_name = str(row[cfg.label_col]) if cfg.label_col is not None else None

    # Determine filename
    filename_override = None
    if cfg.naming_mode == "sequential":
        filename_override = await sequential_namer.filename_for(key, url)
    else:
        filename_override = render_filename(cfg.file_name_pattern, url, key)

    host_ctrl = None
    if mgr is not None:
        host_ctrl = await mgr.get(url)

    # Limit per-host concurrency before making the request
    if host_ctrl is not None:
        await host_ctrl.conc.acquire()

    trace_dict: dict[str, Any] = {}
    trace_token = TRACE_CTX.set(trace_dict)

    t0 = _now()
    try:
        # Apply pacing either here OR inside download_single (config switch)
        if host_ctrl is not None and not cfg.token_bucket_in_download_single:
            await host_ctrl.tb.acquire()

        # Run the actual download
        k, file_path, cls, err, status = await download_single(
            url=url,
            key=key,
            class_name=class_name,
            output_folder=cfg.output_folder,
            output_format=cfg.output_format,
            session=session,
            timeout=cfg.timeout_sec,
            filename=filename_override,
            token_bucket=(host_ctrl.tb if (host_ctrl is not None and cfg.token_bucket_in_download_single) else None),
            enable_rate_limiting=bool(host_ctrl is not None and cfg.token_bucket_in_download_single),
            total_bytes=total_bytes,
        )

        bytes_dl = 0
        if err is None and file_path and os.path.exists(file_path):
            try:
                bytes_dl = os.path.getsize(file_path)
            except Exception:
                bytes_dl = 0

        # Per-host signal recording (TTFB + 429 ratio)
        if host_ctrl is not None:
            await host_ctrl.signals.record(status_code=status, ttfb=trace_dict.get("ttfb"), bytes_downloaded=bytes_dl)

        # Log errors when they occur (especially 429s which affect rate)
        if err is not None or (status is not None and status >= 400):
            host = urlsplit(url).netloc.lower() or "unknown"
            rate_info = f", rate={host_ctrl.safe_rate:.1f}" if host_ctrl else ""
            conc_info = f", conc={host_ctrl.conc.get_limit()}" if host_ctrl else ""
            if status == 429:
                print(f"[Error] 429 Too Many Requests from {host}{rate_info}{conc_info}")
            elif status is not None and status >= 400:
                print(f"[Error] HTTP {status} from {host}: {err or 'unknown'}{rate_info}")
            elif err:
                print(f"[Error] {host}: {err}{rate_info}")

        # Collision detection: track written paths across attempts
        if err is None and file_path:
            prev = global_written_paths.get(file_path)
            if prev is not None and prev != key:
                print(f"[Warning] Filename collision: {file_path} (prev key={prev}, current key={key})")
            else:
                global_written_paths[file_path] = key

        return DownloadOutcome(
            key=str(k),
            url=url,
            success=(err is None),
            file_path=file_path,
            class_name=cls,
            status_code=status,
            error=str(err) if err is not None else None,
            bytes_downloaded=bytes_dl,
        )

    finally:
        TRACE_CTX.reset(trace_token)
        if host_ctrl is not None:
            await host_ctrl.conc.release()

async def download_batch_bounded(
    *,
    cfg: Config,
    session: aiohttp.ClientSession,
    df: pd.DataFrame,
    mgr: Optional[PerHostControllerManager],
    sequential_namer: SequentialNamer,
    global_written_paths: dict[str, str],
) -> dict[str, DownloadOutcome]:
    """
    Bounded scheduler:
      - enqueue rows
      - spawn cfg.concurrent_downloads workers
      - each worker processes one row at a time
    """
    q: asyncio.Queue[pd.Series] = asyncio.Queue()
    for _, row in df.iterrows():
        q.put_nowait(row)

    total_bytes: list[int] = []
    outcomes: dict[str, DownloadOutcome] = {}
    pbar = tqdm(total=len(df), desc="URLs", unit="url")

    async def worker():
        while not shutdown_flag:
            try:
                row = q.get_nowait()
            except asyncio.QueueEmpty:
                return
            out = await download_one(
                row=row,
                cfg=cfg,
                session=session,
                total_bytes=total_bytes,
                mgr=mgr,
                sequential_namer=sequential_namer,
                global_written_paths=global_written_paths,
            )
            outcomes[out.key] = out
            q.task_done()
            pbar.update(1)

    workers = [asyncio.create_task(worker()) for _ in range(max(1, int(cfg.concurrent_downloads)))]
    await asyncio.gather(*workers)
    pbar.close()

    return outcomes

# ---------------------------
# Tar + overview
# ---------------------------

def create_tar(output_folder: str) -> str:
    out = Path(output_folder)
    tar_path = out.with_suffix(out.suffix + ".tar.gz") if out.suffix else Path(str(out) + ".tar.gz")
    if not out.exists():
        raise FileNotFoundError(f"Output folder not found: {out}")
    with tarfile.open(str(tar_path), "w:gz") as tf:
        tf.add(str(out), arcname=out.name)
    return str(tar_path.resolve())

def write_overview(
    *,
    cfg: Config,
    df_total: int,
    outcomes: dict[str, DownloadOutcome],
    elapsed_sec: float,
    tar_path: Optional[str],
) -> str:
    successes = [o for o in outcomes.values() if o.success]
    failures = [o for o in outcomes.values() if not o.success]
    err_counter = Counter((o.status_code, o.error) for o in failures)

    total_bytes = sum(o.bytes_downloaded for o in successes)
    mb = total_bytes / 1e6
    speed_MBps = (mb / elapsed_sec) if elapsed_sec > 0 else 0.0

    report = {
        "script_inputs": {
            "input": cfg.input_path,
            "input_format": cfg.input_format,
            "output_folder": cfg.output_folder,
            "output_format": cfg.output_format,
            "url_column": cfg.url_col,
            "label_column": cfg.label_col,
            "concurrent_downloads": cfg.concurrent_downloads,
            "timeout_sec": cfg.timeout_sec,
            "enable_polite_controller": cfg.enable_polite_controller,
            "initial_rate": cfg.initial_rate,
            "min_rate": cfg.min_rate,
            "max_rate": cfg.max_rate,
            "per_host_conc_init": cfg.per_host_conc_init,
            "per_host_conc_cap": cfg.per_host_conc_cap,
            "control_interval_sec": cfg.control_interval_sec,
            "startup_growth_factor": cfg.startup_growth_factor,
            "latency_degradation_factor": cfg.latency_degradation_factor,
            "headroom": cfg.headroom,
            "probe_interval_sec": cfg.probe_interval_sec,
            "probe_increment": cfg.probe_increment,
            "backoff_ceiling_factor": cfg.backoff_ceiling_factor,
            "backoff_cooldown_sec": cfg.backoff_cooldown_sec,
            "startup_max_intervals": cfg.startup_max_intervals,
            "max_retry_attempts": cfg.max_retry_attempts,
            "naming_mode": cfg.naming_mode,
            "file_name_pattern": cfg.file_name_pattern,
            "token_bucket_in_download_single": cfg.token_bucket_in_download_single,
        },
        "summary": {
            "total_urls": df_total,
            "successful_downloads": len(successes),
            "failed_downloads": len(failures),
            "success_rate_percent": round((len(successes) / df_total) * 100.0, 2) if df_total else 0.0,
            "downloaded_mb": round(mb, 3),
            "elapsed_sec": round(elapsed_sec, 3),
            "avg_speed_MBps": round(speed_MBps, 3),
            "shutdown_requested": shutdown_flag,
            "tar_path": tar_path,
        },
        "error_breakdown": [
            {
                "status_code": sc,
                "error": err,
                "count": cnt,
            }
            for (sc, err), cnt in err_counter.most_common()
        ],
        "timestamp_local": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    }

    out = Path(cfg.output_folder)
    overview_path = out.with_name(out.name + "_overview.json")
    with overview_path.open("w") as f:
        json.dump(report, f, indent=2)
    return str(overview_path.resolve())

# ---------------------------
# Main
# ---------------------------

async def main() -> None:
    cfg = parse_args()

    df = validate_and_load(cfg)
    print(f"[Load] URLs after filtering: {len(df)}")

    mgr: Optional[PerHostControllerManager] = None
    ctrl_task: Optional[asyncio.Task] = None
    if cfg.enable_polite_controller:
        mgr = PerHostControllerManager(
            interval_sec=cfg.control_interval_sec,
            initial_rate=cfg.initial_rate,
            min_rate=cfg.min_rate,
            max_rate=cfg.max_rate,
            per_host_conc_init=cfg.per_host_conc_init,
            per_host_conc_cap=cfg.per_host_conc_cap,
            # PolicyBBR parameters
            startup_growth_factor=cfg.startup_growth_factor,
            latency_degradation_factor=cfg.latency_degradation_factor,
            headroom=cfg.headroom,
            probe_interval_sec=cfg.probe_interval_sec,
            probe_increment=cfg.probe_increment,
            backoff_ceiling_factor=cfg.backoff_ceiling_factor,
            backoff_cooldown_sec=cfg.backoff_cooldown_sec,
            startup_max_intervals=cfg.startup_max_intervals,
        )
        ctrl_task = asyncio.create_task(per_host_controller_loop(mgr, cfg.control_interval_sec))

    connector = aiohttp.TCPConnector(
        limit=max(50, int(cfg.concurrent_downloads * 2)),
        ttl_dns_cache=300,
        use_dns_cache=True,
    )
    trace_config = build_trace_config()

    sequential_namer = SequentialNamer()
    global_written_paths: dict[str, str] = {}
    final_outcomes: dict[str, DownloadOutcome] = {}

    start = time.monotonic()
    try:
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=max(1, cfg.timeout_sec * 2)),
            headers={"User-Agent": "FLOW-DC-ImageDownloader/2.0"},
            trace_configs=[trace_config],
        ) as session:
            current_df = df.copy()
            attempt = 1

            while attempt <= cfg.max_retry_attempts and not current_df.empty and not shutdown_flag:
                print(f"\n[Attempt {attempt}] processing {len(current_df)} URLs...")
                outcomes = await download_batch_bounded(
                    cfg=cfg,
                    session=session,
                    df=current_df,
                    mgr=mgr,
                    sequential_namer=sequential_namer,
                    global_written_paths=global_written_paths,
                )

                # Merge outcomes, overwriting older attempts for the same key
                final_outcomes.update(outcomes)

                # Build retry set
                retry_rows = []
                for _, row in current_df.iterrows():
                    key = str(row["__key__"])
                    out = outcomes.get(key)
                    if out is None:
                        continue
                    if out.success:
                        continue
                    if _is_retryable(out.status_code, out.error):
                        retry_rows.append(row)

                # Per-attempt summary
                succ = sum(1 for o in outcomes.values() if o.success)
                fail = sum(1 for o in outcomes.values() if not o.success)
                retryable = len(retry_rows)
                print(f"[Attempt {attempt}] success={succ} fail={fail} retryable={retryable}")

                if retry_rows and attempt < cfg.max_retry_attempts and not shutdown_flag:
                    await asyncio.sleep(cfg.retry_backoff_sec)
                    current_df = pd.DataFrame(retry_rows)
                    attempt += 1
                else:
                    break

    finally:
        if ctrl_task is not None:
            ctrl_task.cancel()
            with contextlib.suppress(Exception):
                if ctrl_task is not None:
                    ctrl_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await ctrl_task

    elapsed = time.monotonic() - start

    # Final summary
    successes = [o for o in final_outcomes.values() if o.success]
    failures = [o for o in final_outcomes.values() if not o.success]
    print("\n" + "=" * 72)
    print("FINAL STATUS SUMMARY")
    print("=" * 72)
    print(f"Total URLs in input:   {len(df)}")
    print(f"Successful downloads:  {len(successes)}")
    print(f"Permanent failures:    {len(failures)}")
    print(f"Elapsed time (sec):    {elapsed:.2f}")
    if len(df) > 0:
        print(f"Success rate:          {(len(successes) / len(df)) * 100:.2f}%")

    # Tar + overview
    tar_path = None
    if cfg.create_tar and (not shutdown_flag) and len(successes) > 0:
        try:
            tar_path = create_tar(cfg.output_folder)
            print(f"[Tar] Created: {tar_path}")
        except Exception as e:
            print(f"[Tar] Failed: {e}")

    if cfg.create_overview:
        try:
            overview = write_overview(cfg=cfg, df_total=len(df), outcomes=final_outcomes, elapsed_sec=elapsed, tar_path=tar_path)
            print(f"[Report] Overview: {overview}")
        except Exception as e:
            print(f"[Report] Failed to write overview: {e}")

if __name__ == "__main__":
    import contextlib
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
