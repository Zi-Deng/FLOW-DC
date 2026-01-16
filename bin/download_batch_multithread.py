#!/usr/bin/env python3
"""
FLOW-DC Batch Downloader with PAARC v2.0 - Multithreaded Version

This is a multithreaded implementation of the PAARC-based downloader,
using threading primitives instead of asyncio for comparison/benchmarking.

Key Differences from Async Version:
- Uses threading.Condition instead of asyncio.Condition
- Uses requests instead of aiohttp
- Uses ThreadPoolExecutor for worker management
- TTFB measured via streaming response first chunk

Author: FLOW-DC Team
Version: 2.0.0-multithread
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import queue
import re
import shutil
import signal
import tarfile
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import polars as pl
from tqdm import tqdm

from single_download_gbif import load_input_file, extract_extension


# =============================================================================
# GLOBALS AND SHUTDOWN HANDLING
# =============================================================================

shutdown_flag = False
shutdown_lock = threading.Lock()


def _signal_handler(sig, frame):
    global shutdown_flag
    with shutdown_lock:
        print("\n[Shutdown] Interrupt received. Attempting graceful shutdown...")
        shutdown_flag = True


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# Thread-local storage for per-thread HTTP sessions
_thread_local = threading.local()


def get_session() -> requests.Session:
    """Get or create thread-local HTTP session."""
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100,
            max_retries=0,  # We handle retries ourselves
        )
        _thread_local.session.mount('http://', adapter)
        _thread_local.session.mount('https://', adapter)
        _thread_local.session.headers.update({
            'User-Agent': 'FLOW-DC/2.0 PAARC/2.0-multithread'
        })
    return _thread_local.session


# =============================================================================
# PLATEAU DETECTION MODE
# =============================================================================
PLATEAU_DETECTION_MODE = "latency"


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def _now() -> float:
    return time.time()


def _monotonic() -> float:
    return time.monotonic()


def _sanitize_filename(name: str, max_len: int = 180) -> str:
    name = name.strip().replace(os.sep, "_")
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("._")
    if not name:
        name = "file"
    return name[:max_len]


def _percentile(values: list[float], p: float) -> Optional[float]:
    if not values:
        return None
    sorted_values = sorted(values)
    n = len(sorted_values)
    if n == 1:
        return sorted_values[0]
    idx = (n - 1) * p
    lower = int(idx)
    upper = min(lower + 1, n - 1)
    if lower == upper:
        return sorted_values[lower]
    weight = idx - lower
    return sorted_values[lower] + weight * (sorted_values[upper] - sorted_values[lower])


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
        "timed out",
    ]
    return any(p in s for p in patterns)


def _is_retryable(status_code: Optional[int], error: Any) -> bool:
    if status_code == 429 or status_code == 408:
        return True
    if status_code is not None and status_code >= 500:
        return True
    return _is_connection_error(error)


# =============================================================================
# CONFIGURATION (Same as async version)
# =============================================================================

@dataclass(frozen=True)
class PAARCConfig:
    """Complete configuration for PAARC v2.0 controller."""
    C_min: int = 2
    C_max: int = 10_000
    C_init: int = 4
    N_min: int = 50
    k_interval: int = 8
    T_floor: float = 0.2
    N_init: int = 100
    startup_theta_50: float = 4
    startup_theta_95: float = 8
    efficiency_threshold: float = 0.5
    efficiency_window: int = 5
    mu: float = 0.85
    stable_intervals_required: int = 1
    probe_rtt_period: float = 10.0
    probe_rtt_min_samples: int = 100
    probe_rtt_concurrency_factor: float = 0.5
    n_restore: int = 5
    beta: float = 0.5
    cooldown_rtprop_mult: int = 5
    cooldown_floor: float = 2.0
    overload_check_rtprop_mult: float = 10.0
    overload_check_floor: float = 3.0
    underperformance_threshold: float = 0.5
    underperformance_intervals: int = 5
    revision_factor: float = 0.99
    theta_50: float = 4
    theta_95: float = 8
    alpha_ema: float = 0.3
    rtprop_window: float = 15.0


@dataclass(frozen=True)
class Config:
    """Main application configuration."""
    input_path: str
    output_folder: str
    input_format: str = "parquet"
    url_col: str = "url"
    label_col: Optional[str] = None
    output_format: str = "imagefolder"
    concurrent_downloads: int = 256
    timeout_sec: int = 30
    enable_paarc: bool = True
    C_init: int = 4
    C_min: int = 2
    C_max: int = 10_000
    mu: float = 0.85
    startup_theta_50: float = 3.0
    startup_theta_95: float = 4.0
    efficiency_threshold: float = 0.5
    efficiency_window: int = 5
    beta: float = 0.5
    theta_50: float = 1.5
    theta_95: float = 2.0
    probe_rtt_period: float = 10.0
    rtprop_window: float = 15.0
    cooldown_floor: float = 2.0
    alpha_ema: float = 0.3
    max_retry_attempts: int = 3
    retry_backoff_sec: float = 2.0
    naming_mode: str = "sequential"
    file_name_pattern: str = "{segment[-2]}"
    create_tar: bool = True
    create_overview: bool = True

    def to_paarc_config(self) -> PAARCConfig:
        return PAARCConfig(
            C_init=self.C_init,
            C_min=self.C_min,
            C_max=self.C_max,
            mu=self.mu,
            startup_theta_50=self.startup_theta_50,
            startup_theta_95=self.startup_theta_95,
            efficiency_threshold=self.efficiency_threshold,
            efficiency_window=self.efficiency_window,
            beta=self.beta,
            theta_50=self.theta_50,
            theta_95=self.theta_95,
            probe_rtt_period=self.probe_rtt_period,
            rtprop_window=self.rtprop_window,
            cooldown_floor=self.cooldown_floor,
            alpha_ema=self.alpha_ema,
        )


def parse_args() -> Config:
    """Parse command line arguments or JSON config file."""
    p = argparse.ArgumentParser(
        description="FLOW-DC Batch Downloader (Multithreaded) with PAARC v2.0",
    )

    p.add_argument("--config", type=str, help="Path to JSON config file")
    p.add_argument("--input", dest="input_path", type=str)
    p.add_argument("--input_format", type=str, default="parquet")
    p.add_argument("--url", dest="url_col", type=str, default="url")
    p.add_argument("--label", dest="label_col", type=str, default=None)
    p.add_argument("--output", dest="output_folder", type=str)
    p.add_argument("--output_format", type=str, default="imagefolder")
    p.add_argument("--concurrent_downloads", type=int, default=256)
    p.add_argument("--timeout", dest="timeout_sec", type=int, default=30)
    p.add_argument("--enable_paarc", action="store_true", default=True)
    p.add_argument("--disable_paarc", action="store_true")
    p.add_argument("--C_init", type=int, default=4)
    p.add_argument("--C_min", type=int, default=2)
    p.add_argument("--C_max", type=int, default=10000)
    p.add_argument("--mu", type=float, default=0.75)
    p.add_argument("--startup_theta_50", type=float, default=3.0)
    p.add_argument("--startup_theta_95", type=float, default=4.0)
    p.add_argument("--efficiency_threshold", type=float, default=0.5)
    p.add_argument("--efficiency_window", type=int, default=5)
    p.add_argument("--beta", type=float, default=0.5)
    p.add_argument("--theta_50", type=float, default=1.5)
    p.add_argument("--theta_95", type=float, default=2.0)
    p.add_argument("--probe_rtt_period", type=float, default=10.0)
    p.add_argument("--cooldown_floor", type=float, default=2.0)
    p.add_argument("--alpha_ema", type=float, default=0.3)
    p.add_argument("--max_retry_attempts", type=int, default=3)
    p.add_argument("--retry_backoff_sec", type=float, default=2.0)
    p.add_argument("--naming_mode", type=str, default="sequential")
    p.add_argument("--file_name_pattern", type=str, default="{segment[-2]}")
    p.add_argument("--no_tar", action="store_true")
    p.add_argument("--no_overview", action="store_true")

    args = p.parse_args()

    # Load from JSON if provided
    if args.config:
        with open(args.config) as f:
            data = json.load(f)

        return Config(
            input_path=data.get("input", args.input_path),
            output_folder=data.get("output", args.output_folder),
            input_format=data.get("input_format", args.input_format),
            url_col=data.get("url", args.url_col),
            label_col=data.get("label", args.label_col),
            output_format=data.get("output_format", args.output_format),
            concurrent_downloads=data.get("concurrent_downloads", args.concurrent_downloads),
            timeout_sec=data.get("timeout", args.timeout_sec),
            enable_paarc=data.get("enable_paarc", True),
            C_init=data.get("C_init", args.C_init),
            C_min=data.get("C_min", args.C_min),
            C_max=data.get("C_max", args.C_max),
            mu=data.get("mu", args.mu),
            startup_theta_50=data.get("startup_theta_50", args.startup_theta_50),
            startup_theta_95=data.get("startup_theta_95", args.startup_theta_95),
            efficiency_threshold=data.get("efficiency_threshold", args.efficiency_threshold),
            efficiency_window=data.get("efficiency_window", args.efficiency_window),
            beta=data.get("beta", args.beta),
            theta_50=data.get("theta_50", args.theta_50),
            theta_95=data.get("theta_95", args.theta_95),
            probe_rtt_period=data.get("probe_rtt_period", args.probe_rtt_period),
            rtprop_window=data.get("rtprop_window", 15.0),
            cooldown_floor=data.get("cooldown_floor", args.cooldown_floor),
            alpha_ema=data.get("alpha_ema", args.alpha_ema),
            max_retry_attempts=data.get("max_retry_attempts", args.max_retry_attempts),
            retry_backoff_sec=data.get("retry_backoff_sec", args.retry_backoff_sec),
            naming_mode=data.get("naming_mode", args.naming_mode),
            file_name_pattern=data.get("file_name_pattern", args.file_name_pattern),
            create_tar=data.get("create_tar", not args.no_tar),
            create_overview=data.get("create_overview", not args.no_overview),
        )

    if not args.input_path or not args.output_folder:
        p.error("--input and --output are required (or use --config)")

    return Config(
        input_path=args.input_path,
        output_folder=args.output_folder,
        input_format=args.input_format,
        url_col=args.url_col,
        label_col=args.label_col,
        output_format=args.output_format,
        concurrent_downloads=args.concurrent_downloads,
        timeout_sec=args.timeout_sec,
        enable_paarc=not args.disable_paarc,
        C_init=args.C_init,
        C_min=args.C_min,
        C_max=args.C_max,
        mu=args.mu,
        startup_theta_50=args.startup_theta_50,
        startup_theta_95=args.startup_theta_95,
        efficiency_threshold=args.efficiency_threshold,
        efficiency_window=args.efficiency_window,
        beta=args.beta,
        theta_50=args.theta_50,
        theta_95=args.theta_95,
        probe_rtt_period=args.probe_rtt_period,
        cooldown_floor=args.cooldown_floor,
        alpha_ema=args.alpha_ema,
        max_retry_attempts=args.max_retry_attempts,
        retry_backoff_sec=args.retry_backoff_sec,
        naming_mode=args.naming_mode,
        file_name_pattern=args.file_name_pattern,
        create_tar=not args.no_tar,
        create_overview=not args.no_overview,
    )


# =============================================================================
# INPUT VALIDATION AND LOADING
# =============================================================================

def validate_and_load(cfg: Config) -> pl.DataFrame:
    """Load and validate input data."""
    in_path = Path(cfg.input_path)
    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    out_dir = Path(cfg.output_folder)
    if out_dir.exists():
        print(f"[I/O] Output folder exists; deleting: {out_dir}")
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_input_file(str(in_path), cfg.input_format)

    if cfg.url_col not in df.columns:
        raise ValueError(f"URL column '{cfg.url_col}' not found.")

    if cfg.label_col is not None and cfg.label_col not in df.columns:
        raise ValueError(f"Label column '{cfg.label_col}' not found.")

    df = df.filter(pl.col(cfg.url_col).is_not_null())
    df = df.with_columns(
        pl.col(cfg.url_col).cast(pl.Utf8).str.strip_chars().alias(cfg.url_col)
    )
    df = df.filter(pl.col(cfg.url_col).str.len_chars() > 0)

    if df.height == 0:
        raise ValueError("No valid URLs found after filtering.")

    df = df.with_row_index("__key__").with_columns(
        pl.col("__key__").cast(pl.Utf8)
    )

    return df


# =============================================================================
# PAARC STATE MACHINE
# =============================================================================

class PAARCState(Enum):
    INIT = auto()
    STARTUP = auto()
    PROBE_BW = auto()
    PROBE_RTT = auto()
    BACKOFF = auto()


# =============================================================================
# THREAD-SAFE ADAPTIVE SEMAPHORE
# =============================================================================

class ThreadSafeSemaphore:
    """Thread-safe adaptive semaphore for concurrency control."""

    def __init__(self, initial: int, minimum: int = 2, maximum: int = 10000):
        self._limit = max(minimum, min(initial, maximum))
        self._minimum = minimum
        self._maximum = maximum
        self._inflight = 0
        self._cond = threading.Condition()

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire a slot, blocking if at limit."""
        with self._cond:
            deadline = None if timeout is None else _monotonic() + timeout
            while self._inflight >= self._limit and not shutdown_flag:
                if deadline is not None:
                    remaining = deadline - _monotonic()
                    if remaining <= 0:
                        return False
                    if not self._cond.wait(timeout=remaining):
                        return False
                else:
                    self._cond.wait()
            if shutdown_flag:
                return True  # Allow through on shutdown
            self._inflight += 1
            return True

    def release(self) -> None:
        """Release a slot."""
        with self._cond:
            self._inflight = max(0, self._inflight - 1)
            self._cond.notify(1)  # Only wake ONE waiter (fixes thundering herd)

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def inflight(self) -> int:
        with self._cond:
            return self._inflight

    def set_limit(self, new_limit: int, reason: str = "") -> None:
        """Adjust the concurrency limit."""
        with self._cond:
            new_limit = max(self._minimum, min(new_limit, self._maximum))
            if new_limit != self._limit:
                old = self._limit
                self._limit = new_limit
                reason_str = f" ({reason})" if reason else ""
                print(f"[PAARC] Concurrency: {old} → {new_limit}{reason_str}")
                # Notify waiters if limit increased
                if new_limit > old:
                    self._cond.notify(new_limit - old)


# =============================================================================
# THREAD-SAFE LEAKY BUCKET SMOOTHER
# =============================================================================

class ThreadSafeLeakyBucket:
    """Thread-safe leaky bucket for request smoothing."""

    def __init__(self, concurrency: int, rtprop: float):
        self._concurrency = concurrency
        self._rtprop = rtprop
        self._min_delay = self._calculate_min_delay()
        self._last_request_time: float = 0.0
        self._lock = threading.Lock()

    def _calculate_min_delay(self) -> float:
        if self._rtprop > 0 and self._concurrency > 0:
            return self._rtprop / self._concurrency
        return 0.01

    def update(self, concurrency: int, rtprop: float) -> None:
        with self._lock:
            self._concurrency = max(1, concurrency)
            self._rtprop = max(0.001, rtprop)
            self._min_delay = self._calculate_min_delay()

    def acquire(self) -> None:
        """Wait if necessary to maintain smooth request spacing."""
        # Calculate wait time while holding lock
        with self._lock:
            now = _monotonic()
            elapsed = now - self._last_request_time
            wait_time = max(0.0, self._min_delay - elapsed)
            self._last_request_time = now + wait_time

        # Sleep OUTSIDE the lock (fixes serialization bug)
        if wait_time > 0:
            time.sleep(wait_time)

    @property
    def implied_rate(self) -> float:
        if self._min_delay > 0:
            return 1.0 / self._min_delay
        return float('inf')


# =============================================================================
# THREAD-SAFE HOST METRICS
# =============================================================================

class ThreadSafeHostMetrics:
    """Thread-safe per-host metrics collection."""

    def __init__(self, alpha_ema: float = 0.3, rtprop_window: float = 15.0):
        self._lock = threading.Lock()
        self._alpha = alpha_ema
        self._rtprop_window = rtprop_window

        # Per-interval data
        self._ttfb_samples: list[float] = []
        self._bytes_downloaded = 0
        self._interval_start = _monotonic()

        # Success/error counters
        self._n_success = 0
        self._n_errors = 0
        self._last_retry_after: Optional[float] = None

        # RTprop tracking
        self._rtprop: Optional[float] = None
        self._rtprop_samples: deque = deque()

        # EMA-smoothed percentiles
        self._ema_p10: Optional[float] = None
        self._ema_p50: Optional[float] = None
        self._ema_p95: Optional[float] = None

        # Goodput history
        self._goodput_history: deque = deque(maxlen=10)

    def record(
        self,
        status_code: Optional[int],
        ttfb: Optional[float],
        bytes_downloaded: int,
        is_conn_error: bool,
        retry_after_sec: Optional[float] = None,
    ) -> None:
        """Record metrics from a completed request."""
        with self._lock:
            is_success = (
                status_code is not None
                and 200 <= status_code < 400
                and not is_conn_error
            )

            if is_success:
                self._n_success += 1
                if ttfb is not None and ttfb > 0:
                    self._ttfb_samples.append(ttfb)
                self._bytes_downloaded += bytes_downloaded
            else:
                self._n_errors += 1

            if retry_after_sec is not None:
                self._last_retry_after = retry_after_sec

    def finish_interval(self, full_rtprop_update: bool = False) -> dict:
        """Finalize interval and return statistics."""
        with self._lock:
            now = _monotonic()
            duration = now - self._interval_start

            samples = self._ttfb_samples
            n_samples = len(samples)

            # Calculate percentiles
            p10 = _percentile(samples, 0.10)
            p50 = _percentile(samples, 0.50)
            p95 = _percentile(samples, 0.95)

            # Update RTprop
            if p10 is not None:
                if full_rtprop_update:
                    self._rtprop_samples.clear()
                    self._rtprop_samples.append((now, p10))
                    self._rtprop = p10
                else:
                    # Sliding window update
                    self._rtprop_samples.append((now, p10))
                    cutoff = now - self._rtprop_window
                    while self._rtprop_samples and self._rtprop_samples[0][0] < cutoff:
                        self._rtprop_samples.popleft()
                    if self._rtprop_samples:
                        self._rtprop = min(s[1] for s in self._rtprop_samples)

            # EMA update
            if p10 is not None:
                self._ema_p10 = p10 if self._ema_p10 is None else (
                    self._alpha * p10 + (1 - self._alpha) * self._ema_p10
                )
            if p50 is not None:
                self._ema_p50 = p50 if self._ema_p50 is None else (
                    self._alpha * p50 + (1 - self._alpha) * self._ema_p50
                )
            if p95 is not None:
                self._ema_p95 = p95 if self._ema_p95 is None else (
                    self._alpha * p95 + (1 - self._alpha) * self._ema_p95
                )

            # Goodput calculation
            total = self._n_success + self._n_errors
            goodput_rps = self._n_success / duration if duration > 0 else 0
            goodput_bps = self._bytes_downloaded / duration if duration > 0 else 0

            self._goodput_history.append(goodput_rps)

            # Prepare result
            result = {
                "total": total,
                "n_success": self._n_success,
                "n_errors": self._n_errors,
                "n_samples": n_samples,
                "has_overload": self._n_errors > 0,
                "p10": self._ema_p10,
                "p50": self._ema_p50,
                "p95": self._ema_p95,
                "rtprop": self._rtprop,
                "goodput_rps": goodput_rps,
                "goodput_bps": goodput_bps,
                "bytes": self._bytes_downloaded,
                "duration": duration,
                "retry_after": self._last_retry_after,
            }

            # Reset interval data
            self._ttfb_samples = []
            self._bytes_downloaded = 0
            self._n_success = 0
            self._n_errors = 0
            self._last_retry_after = None
            self._interval_start = now

            return result

    @property
    def rtprop(self) -> Optional[float]:
        with self._lock:
            return self._rtprop

    @property
    def sample_count(self) -> int:
        with self._lock:
            return len(self._ttfb_samples) + self._n_success + self._n_errors

    def get_goodput_history(self) -> list[float]:
        with self._lock:
            return list(self._goodput_history)


# =============================================================================
# THREAD-SAFE PAARC CONTROLLER
# =============================================================================

class ThreadSafePAARCController:
    """Thread-safe PAARC controller for a single host."""

    def __init__(self, host: str, config: PAARCConfig):
        self.host = host
        self.config = config
        self._lock = threading.Lock()

        # State
        self.state = PAARCState.INIT
        self._concurrency = config.C_init
        self._C_ceiling: Optional[int] = None
        self._C_operating: Optional[int] = None

        # Components
        self.semaphore = ThreadSafeSemaphore(config.C_init, config.C_min, config.C_max)
        self.metrics = ThreadSafeHostMetrics(config.alpha_ema, config.rtprop_window)
        self.smoother: Optional[ThreadSafeLeakyBucket] = None

        # Tracking
        self._init_samples = 0
        self._stable_intervals = 0
        self._last_probe_rtt_time = _monotonic()
        self._samples_since_probe_rtt = 0
        self._cooldown_until = 0.0
        self._restoring = False
        self._saved_concurrency: Optional[int] = None
        self._underperformance_intervals = 0
        self._last_overload_reduction_time = 0.0
        self._prev_goodput: Optional[float] = None

    def _get_rtprop(self) -> float:
        rtprop = self.metrics.rtprop
        return rtprop if rtprop is not None else 0.5

    def _set_concurrency(self, new_C: int, reason: str = "") -> None:
        new_C = max(self.config.C_min, min(new_C, self.config.C_max))
        if new_C != self._concurrency:
            self._concurrency = new_C
            self.semaphore.set_limit(new_C, reason)

            if self.smoother is not None:
                self.smoother.update(new_C, self._get_rtprop())

    def _is_latency_degraded(self, snap: dict) -> bool:
        rtprop = self._get_rtprop()
        p50 = snap.get("p50")
        p95 = snap.get("p95")

        if p50 is not None and p50 > rtprop * self.config.theta_50:
            return True
        if p95 is not None and p95 > rtprop * self.config.theta_95:
            return True
        return False

    def _calculate_additive_increase(self) -> int:
        return max(1, int(self._concurrency * 0.02))

    def _calculate_cooldown(self, retry_after: Optional[float]) -> float:
        rtprop = self._get_rtprop()
        rtprop_cooldown = rtprop * self.config.cooldown_rtprop_mult
        cooldown = max(rtprop_cooldown, self.config.cooldown_floor)
        if retry_after is not None:
            cooldown = max(cooldown, retry_after)
        return cooldown

    def step(self) -> None:
        """Execute one control step."""
        with self._lock:
            now = _monotonic()
            full_update = self.state in (PAARCState.INIT, PAARCState.PROBE_RTT)
            snap = self.metrics.finish_interval(full_rtprop_update=full_update)

            if self.state == PAARCState.INIT:
                self._step_init(snap, now)
            elif self.state == PAARCState.STARTUP:
                self._step_startup(snap, now)
            elif self.state == PAARCState.PROBE_BW:
                self._step_probe_bw(snap, now)
            elif self.state == PAARCState.PROBE_RTT:
                self._step_probe_rtt(snap, now)
            elif self.state == PAARCState.BACKOFF:
                self._step_backoff(snap, now)

    def _step_init(self, snap: dict, now: float) -> None:
        self._init_samples += snap.get("n_samples", 0)

        if self._init_samples >= self.config.N_init:
            rtprop = self._get_rtprop()
            if rtprop is not None and rtprop > 0:
                self.smoother = ThreadSafeLeakyBucket(self._concurrency, rtprop)

            print(f"[PAARC] {self.host}: INIT→STARTUP | rtprop={rtprop:.3f}s")
            self.state = PAARCState.STARTUP

    def _step_startup(self, snap: dict, now: float) -> None:
        # Check for overload
        if snap.get("has_overload", False):
            self._C_ceiling = max(self.config.C_min, self._concurrency - 1)
            self._C_operating = int(self._C_ceiling * self.config.mu)
            self._set_concurrency(self._C_operating, "startup_overload")

            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown
            self._last_overload_reduction_time = now
            self.state = PAARCState.BACKOFF

            print(f"[PAARC] {self.host}: STARTUP→BACKOFF")
            return

        # Check for plateau (latency-based)
        rtprop = self._get_rtprop()
        p50 = snap.get("p50")
        p95 = snap.get("p95")

        if p50 is not None and p50 > rtprop * self.config.startup_theta_50:
            self._transition_to_probe_bw(now, "latency_plateau")
            return
        if p95 is not None and p95 > rtprop * self.config.startup_theta_95:
            self._transition_to_probe_bw(now, "latency_plateau")
            return

        # Exponential growth
        new_C = min(self._concurrency + 10, self.config.C_max)
        if new_C > self._concurrency:
            self._set_concurrency(new_C, "startup_growth")
        else:
            self._transition_to_probe_bw(now, "max_reached")

    def _transition_to_probe_bw(self, now: float, reason: str) -> None:
        self._C_ceiling = self._concurrency
        self._C_operating = int(self._C_ceiling * self.config.mu)
        self._set_concurrency(self._C_operating, f"transition_{reason}")
        self._last_probe_rtt_time = now
        self._samples_since_probe_rtt = 0
        self.state = PAARCState.PROBE_BW

        print(f"[PAARC] {self.host}: STARTUP→PROBE_BW | ceiling={self._C_ceiling}")

    def _step_probe_bw(self, snap: dict, now: float) -> None:
        self._samples_since_probe_rtt += snap.get("n_samples", 0)

        # Check for overload
        if snap.get("has_overload", False):
            if self._C_ceiling is not None:
                self._C_ceiling = max(
                    self.config.C_min,
                    int(self._C_ceiling * self.config.beta)
                )
                self._C_operating = int(self._C_ceiling * self.config.mu)

            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown
            self._last_overload_reduction_time = now
            self.state = PAARCState.BACKOFF

            print(f"[PAARC] {self.host}: PROBE_BW→BACKOFF")
            return

        # Check PROBE_RTT trigger
        time_since = now - self._last_probe_rtt_time
        if (time_since >= self.config.probe_rtt_period and
            self._samples_since_probe_rtt >= self.config.probe_rtt_min_samples):
            self._saved_concurrency = self._concurrency
            new_C = max(
                self.config.C_min,
                int(self._concurrency * self.config.probe_rtt_concurrency_factor)
            )
            self._set_concurrency(new_C, "probe_rtt_drain")
            self._restoring = False
            self.state = PAARCState.PROBE_RTT

            print(f"[PAARC] {self.host}: PROBE_BW→PROBE_RTT")
            return

        # Check for latency degradation
        if self._is_latency_degraded(snap):
            self._stable_intervals = 0
            return

        # Upward ceiling revision if operating above ceiling and stable
        if self._C_ceiling is not None and self._concurrency > self._C_ceiling:
            old_ceiling = self._C_ceiling
            self._C_ceiling = self._concurrency
            self._C_operating = int(self._C_ceiling * self.config.mu)
            print(f"[PAARC] {self.host}: Ceiling revised {old_ceiling}→{self._C_ceiling} (probe success)")

        # Check ceiling revision downward
        self._check_ceiling_revision(snap)

        # Additive increase after stable intervals
        self._stable_intervals += 1

        if self._stable_intervals >= self.config.stable_intervals_required:
            if self._concurrency < self.config.C_max:
                delta = self._calculate_additive_increase()
                new_C = min(self._concurrency + delta, self.config.C_max)
                self._set_concurrency(new_C, "probe_bw_increase")
                self._stable_intervals = 0

    def _check_ceiling_revision(self, snap: dict) -> None:
        if self._C_ceiling is None:
            return

        actual_goodput = snap.get("goodput_rps", 0)

        # Don't revise if goodput is suspiciously low
        if actual_goodput < 1.0:
            return

        rtprop = self._get_rtprop()
        expected_goodput = self._C_operating / rtprop if rtprop > 0 else 0

        if actual_goodput < expected_goodput * self.config.underperformance_threshold:
            self._underperformance_intervals += 1

            if self._underperformance_intervals >= self.config.underperformance_intervals:
                old_ceiling = self._C_ceiling
                revised = min(
                    int(self._C_ceiling * self.config.revision_factor),
                    self._C_ceiling - 1
                )
                self._C_ceiling = max(self.config.C_min, revised)
                self._C_operating = int(self._C_ceiling * self.config.mu)
                self._set_concurrency(self._C_operating, "ceiling_revision")
                self._underperformance_intervals = 0
        else:
            self._underperformance_intervals = 0

    def _step_probe_rtt(self, snap: dict, now: float) -> None:
        # Check for overload during PROBE_RTT
        if snap.get("has_overload", False):
            if self._C_ceiling is not None:
                self._C_ceiling = max(
                    self.config.C_min,
                    int(self._C_ceiling * self.config.beta)
                )
                self._C_operating = int(self._C_ceiling * self.config.mu)

            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown
            self._last_overload_reduction_time = now
            self.state = PAARCState.BACKOFF

            print(f"[PAARC] {self.host}: PROBE_RTT→BACKOFF")
            return

        if not self._restoring:
            self._restoring = True
            return

        # Gradual restoration
        self._restore_concurrency(now)

    def _restore_concurrency(self, now: float) -> None:
        if self._saved_concurrency is None:
            self._saved_concurrency = self._C_operating

        target = self._saved_concurrency
        current = self._concurrency

        if current >= target:
            self._last_probe_rtt_time = now
            self._samples_since_probe_rtt = 0
            self._restoring = False
            self._saved_concurrency = None
            self.state = PAARCState.PROBE_BW

            print(f"[PAARC] {self.host}: PROBE_RTT→PROBE_BW (restored)")
            return

        # Fast restoration (2 steps instead of 5)
        step = max(1, (target - current) // 2)
        new_C = min(current + step, target)
        self._set_concurrency(new_C, "probe_rtt_restore")

    def _step_backoff(self, snap: dict, now: float) -> None:
        rtprop = self._get_rtprop()
        overload_check_cooldown = max(
            self.config.overload_check_floor,
            rtprop * self.config.overload_check_rtprop_mult
        )

        time_since_reduction = now - self._last_overload_reduction_time

        if snap.get("has_overload", False) and time_since_reduction >= overload_check_cooldown:
            if self._C_ceiling is not None:
                self._C_ceiling = max(
                    self.config.C_min,
                    int(self._C_ceiling * self.config.beta)
                )
                self._C_operating = int(self._C_ceiling * self.config.mu)
                self._set_concurrency(self._C_operating, "backoff_reduction")

            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown
            self._last_overload_reduction_time = now
            return

        # Check if cooldown expired
        if now >= self._cooldown_until:
            self._last_probe_rtt_time = now
            self._samples_since_probe_rtt = 0
            self.state = PAARCState.PROBE_BW

            print(f"[PAARC] {self.host}: BACKOFF→PROBE_BW")


# =============================================================================
# THREAD-SAFE HOST MANAGER
# =============================================================================

class ThreadSafeHostManager:
    """Thread-safe manager for per-host PAARC controllers."""

    def __init__(self, config: PAARCConfig):
        self.config = config
        self._lock = threading.Lock()
        self._controllers: dict[str, ThreadSafePAARCController] = {}

    def get_controller(self, url: str) -> Optional[ThreadSafePAARCController]:
        """Get or create controller for URL's host."""
        try:
            host = urlsplit(url).netloc or "unknown"
        except Exception:
            host = "unknown"

        with self._lock:
            if host not in self._controllers:
                self._controllers[host] = ThreadSafePAARCController(host, self.config)
            return self._controllers[host]

    def all_controllers(self) -> list[ThreadSafePAARCController]:
        with self._lock:
            return list(self._controllers.values())


# =============================================================================
# THREAD-SAFE SEQUENTIAL NAMER
# =============================================================================

class ThreadSafeSequentialNamer:
    """Thread-safe sequential filename generator."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counter = 0
        self._map: dict[str, str] = {}

    def filename_for(self, key: str, url: str) -> str:
        with self._lock:
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
    """Render filename from pattern and URL."""
    seg = [s for s in urlsplit(url).path.split("/") if s]
    ext = Path(urlsplit(url).path).suffix or ""
    safe_key = _sanitize_filename(key)

    env = {"segment": seg, "ext": ext, "key": safe_key}
    try:
        return eval(f'f"{pattern}"', {}, env)
    except Exception:
        return safe_key + ext if ext else safe_key


# =============================================================================
# DOWNLOAD OUTCOME
# =============================================================================

@dataclass
class DownloadOutcome:
    """Result of a single download attempt."""
    key: str
    url: str
    success: bool
    file_path: Optional[str]
    class_name: Optional[str]
    status_code: Optional[int]
    error: Optional[str]
    bytes_downloaded: int = 0


# =============================================================================
# SYNCHRONOUS DOWNLOAD FUNCTION
# =============================================================================

def download_one_sync(
    url: str,
    key: str,
    class_name: Optional[str],
    cfg: Config,
    ctrl: Optional[ThreadSafePAARCController],
    sequential_namer: ThreadSafeSequentialNamer,
    global_written_paths: dict,
    paths_lock: threading.Lock,
) -> DownloadOutcome:
    """Execute a single download synchronously with PAARC control."""

    # Determine filename
    if cfg.naming_mode == "sequential":
        filename = sequential_namer.filename_for(key, url)
    else:
        filename = render_filename(cfg.file_name_pattern, url, key)

    # Acquire concurrency slot
    if ctrl is not None:
        ctrl.semaphore.acquire()

    try:
        # Apply smoothing
        if ctrl is not None and ctrl.smoother is not None:
            ctrl.smoother.acquire()

        # Execute download
        session = get_session()
        t0 = _monotonic()
        ttfb = None
        content = b""
        status_code = None
        error = None
        retry_after_sec = None

        try:
            with session.get(url, stream=True, timeout=cfg.timeout_sec) as resp:
                status_code = resp.status_code

                # Check for Retry-After header
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        retry_after_sec = float(retry_after)
                    except ValueError:
                        pass

                # Stream content and measure TTFB
                chunks = []
                for chunk in resp.iter_content(chunk_size=8192):
                    if ttfb is None:
                        ttfb = _monotonic() - t0
                    chunks.append(chunk)
                content = b"".join(chunks)

                # Check status
                if resp.status_code >= 400:
                    error = f"HTTP {resp.status_code}"

        except requests.exceptions.Timeout:
            error = "Timeout"
        except requests.exceptions.ConnectionError as e:
            error = f"Connection error: {e}"
        except Exception as e:
            error = str(e)

        # Save file if successful
        file_path = None
        bytes_dl = 0

        if error is None and status_code is not None and 200 <= status_code < 400 and content:
            # Sanitize class name
            sanitized_class = None
            if class_name is not None:
                sanitized_class = str(class_name).replace("'", "").replace('"', "").replace(" ", "_").replace("/", "_")

            # Determine path
            if cfg.output_format == "imagefolder":
                folder_name = sanitized_class if sanitized_class else "output"
                file_path = os.path.join(cfg.output_folder, folder_name, filename)
            else:
                file_path = os.path.join(cfg.output_folder, filename)

            # Create directory and save
            try:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, "wb") as f:
                    f.write(content)
                bytes_dl = len(content)
            except Exception as e:
                error = f"Save error: {e}"
                file_path = None

        # Record metrics
        is_conn_error = _is_connection_error(error)
        if ctrl is not None:
            ctrl.metrics.record(
                status_code=status_code,
                ttfb=ttfb,
                bytes_downloaded=bytes_dl,
                is_conn_error=is_conn_error,
                retry_after_sec=retry_after_sec,
            )

        # Track written paths
        if error is None and file_path:
            with paths_lock:
                prev = global_written_paths.get(file_path)
                if prev is not None and prev != key:
                    print(f"[Warning] Filename collision: {file_path}")
                else:
                    global_written_paths[file_path] = key

        return DownloadOutcome(
            key=key,
            url=url,
            success=(error is None and status_code is not None and 200 <= status_code < 400),
            file_path=file_path,
            class_name=class_name,
            status_code=status_code,
            error=error,
            bytes_downloaded=bytes_dl,
        )

    finally:
        if ctrl is not None:
            ctrl.semaphore.release()


# =============================================================================
# WORKER AND CONTROL LOOP
# =============================================================================

def worker(
    work_queue: queue.Queue,
    results: list,
    results_lock: threading.Lock,
    cfg: Config,
    manager: Optional[ThreadSafeHostManager],
    sequential_namer: ThreadSafeSequentialNamer,
    global_written_paths: dict,
    paths_lock: threading.Lock,
    pbar,
    pbar_lock: threading.Lock,
):
    """Worker thread - processes URLs from queue."""
    while not shutdown_flag:
        try:
            item = work_queue.get(timeout=0.1)
        except queue.Empty:
            continue

        if item is None:  # Poison pill
            work_queue.task_done()
            break

        url, key, class_name = item

        # Get controller for this URL's host
        ctrl = None
        if manager is not None:
            ctrl = manager.get_controller(url)

        outcome = download_one_sync(
            url=url,
            key=key,
            class_name=class_name,
            cfg=cfg,
            ctrl=ctrl,
            sequential_namer=sequential_namer,
            global_written_paths=global_written_paths,
            paths_lock=paths_lock,
        )

        with results_lock:
            results.append(outcome)

        with pbar_lock:
            pbar.update(1)

        work_queue.task_done()


def controller_loop(manager: ThreadSafeHostManager, stop_event: threading.Event):
    """PAARC control loop running in separate thread."""
    while not stop_event.is_set():
        time.sleep(1.0)  # Control interval

        for ctrl in manager.all_controllers():
            try:
                ctrl.step()
            except Exception as e:
                print(f"[PAARC] Controller error: {e}")


# =============================================================================
# TAR AND OVERVIEW
# =============================================================================

def create_tar(output_folder: str) -> str:
    """Create tar.gz archive of output folder."""
    out = Path(output_folder)
    tar_path = out.with_suffix(out.suffix + ".tar.gz") if out.suffix else Path(str(out) + ".tar.gz")

    if not out.exists():
        raise FileNotFoundError(f"Output folder not found: {out}")

    with tarfile.open(str(tar_path), "w:gz") as tf:
        tf.add(str(out), arcname=out.name)

    return str(tar_path.resolve())


def write_overview(
    cfg: Config,
    df_total: int,
    outcomes: list[DownloadOutcome],
    elapsed_sec: float,
    tar_path: Optional[str],
) -> str:
    """Write JSON overview report."""
    successes = [o for o in outcomes if o.success]
    failures = [o for o in outcomes if not o.success]
    err_counter = Counter((o.status_code, o.error) for o in failures)

    total_bytes = sum(o.bytes_downloaded for o in successes)
    mb = total_bytes / 1e6
    speed_MBps = (mb / elapsed_sec) if elapsed_sec > 0 else 0.0

    report = {
        "paarc_version": "2.0.0-multithread",
        "script_inputs": {
            "input": cfg.input_path,
            "output_folder": cfg.output_folder,
            "concurrent_downloads": cfg.concurrent_downloads,
            "enable_paarc": cfg.enable_paarc,
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
            {"status_code": sc, "error": err, "count": cnt}
            for (sc, err), cnt in err_counter.most_common()
        ],
        "timestamp_local": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    }

    out = Path(cfg.output_folder)
    overview_path = out.with_name(out.name + "_overview.json")

    with overview_path.open("w") as f:
        json.dump(report, f, indent=2)

    return str(overview_path.resolve())


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main entry point."""
    cfg = parse_args()

    print("=" * 72)
    print("FLOW-DC Batch Downloader (Multithreaded) with PAARC v2.0")
    print("=" * 72)

    # Load and validate input
    df = validate_and_load(cfg)
    print(f"[Load] URLs after filtering: {df.height}")

    # Initialize PAARC controller manager if enabled
    manager: Optional[ThreadSafeHostManager] = None
    ctrl_thread: Optional[threading.Thread] = None
    stop_event = threading.Event()

    if cfg.enable_paarc:
        paarc_config = cfg.to_paarc_config()
        manager = ThreadSafeHostManager(paarc_config)
        ctrl_thread = threading.Thread(
            target=controller_loop,
            args=(manager, stop_event),
            daemon=True
        )
        ctrl_thread.start()
        print(f"[PAARC] Enabled | C_init={paarc_config.C_init} | μ={paarc_config.mu}")
    else:
        print("[PAARC] Disabled - using fixed concurrency")

    # Setup
    sequential_namer = ThreadSafeSequentialNamer()
    global_written_paths: dict[str, str] = {}
    paths_lock = threading.Lock()
    results: list[DownloadOutcome] = []
    results_lock = threading.Lock()
    pbar_lock = threading.Lock()

    start = _monotonic()

    # Create work queue
    work_queue: queue.Queue = queue.Queue()
    for row in df.iter_rows(named=True):
        url = str(row[cfg.url_col]).strip()
        key = str(row.get("__key__", ""))
        class_name = str(row[cfg.label_col]) if cfg.label_col and row.get(cfg.label_col) is not None else None
        work_queue.put((url, key, class_name))

    # Progress bar
    pbar = tqdm(total=df.height, desc="Downloading", unit="url")

    try:
        # Start workers
        num_workers = min(cfg.concurrent_downloads, df.height)
        threads = []

        for _ in range(num_workers):
            t = threading.Thread(
                target=worker,
                args=(
                    work_queue, results, results_lock, cfg, manager,
                    sequential_namer, global_written_paths, paths_lock,
                    pbar, pbar_lock
                ),
                daemon=True
            )
            t.start()
            threads.append(t)

        # Add poison pills
        for _ in range(num_workers):
            work_queue.put(None)

        # Wait for completion
        for t in threads:
            t.join()

    finally:
        pbar.close()
        stop_event.set()
        if ctrl_thread is not None:
            ctrl_thread.join(timeout=2.0)

    elapsed = _monotonic() - start

    # Convert results dict for retry logic
    outcomes_dict = {o.key: o for o in results}

    # Retry logic
    attempt = 1
    current_df = df.clone()

    while attempt < cfg.max_retry_attempts and not shutdown_flag:
        retry_keys = []
        for row in current_df.iter_rows(named=True):
            key = str(row["__key__"])
            out = outcomes_dict.get(key)
            if out is None or out.success:
                continue
            if _is_retryable(out.status_code, out.error):
                retry_keys.append(key)

        if not retry_keys:
            break

        print(f"\n[Retry {attempt + 1}] Processing {len(retry_keys)} retryable URLs...")
        time.sleep(cfg.retry_backoff_sec)

        current_df = current_df.filter(pl.col("__key__").is_in(retry_keys))

        # Retry work queue
        retry_queue: queue.Queue = queue.Queue()
        for row in current_df.iter_rows(named=True):
            url = str(row[cfg.url_col]).strip()
            key = str(row.get("__key__", ""))
            class_name = str(row[cfg.label_col]) if cfg.label_col and row.get(cfg.label_col) is not None else None
            retry_queue.put((url, key, class_name))

        retry_results: list[DownloadOutcome] = []
        retry_pbar = tqdm(total=current_df.height, desc=f"Retry {attempt + 1}", unit="url")

        num_retry_workers = min(cfg.concurrent_downloads, current_df.height)
        retry_threads = []

        for _ in range(num_retry_workers):
            t = threading.Thread(
                target=worker,
                args=(
                    retry_queue, retry_results, results_lock, cfg, manager,
                    sequential_namer, global_written_paths, paths_lock,
                    retry_pbar, pbar_lock
                ),
                daemon=True
            )
            t.start()
            retry_threads.append(t)

        for _ in range(num_retry_workers):
            retry_queue.put(None)

        for t in retry_threads:
            t.join()

        retry_pbar.close()

        # Update outcomes
        for out in retry_results:
            outcomes_dict[out.key] = out
            results.append(out)

        attempt += 1

    # Final summary
    final_outcomes = list(outcomes_dict.values())
    successes = [o for o in final_outcomes if o.success]
    failures = [o for o in final_outcomes if not o.success]

    print("\n" + "=" * 72)
    print("FINAL SUMMARY")
    print("=" * 72)
    print(f"Total URLs:            {df.height}")
    print(f"Successful downloads:  {len(successes)}")
    print(f"Failed downloads:      {len(failures)}")
    print(f"Elapsed time:          {elapsed:.2f}s")
    if df.height > 0:
        print(f"Success rate:          {(len(successes) / df.height) * 100:.2f}%")

    total_mb = sum(o.bytes_downloaded for o in successes) / 1e6
    print(f"Total downloaded:      {total_mb:.2f} MB")
    if elapsed > 0:
        print(f"Average speed:         {total_mb / elapsed:.2f} MB/s")

    # Create tar archive
    tar_path = None
    if cfg.create_tar and not shutdown_flag and len(successes) > 0:
        try:
            tar_path = create_tar(cfg.output_folder)
            print(f"[Tar] Created: {tar_path}")
        except Exception as e:
            print(f"[Tar] Failed: {e}")

    # Write overview
    if cfg.create_overview:
        try:
            overview = write_overview(
                cfg=cfg,
                df_total=df.height,
                outcomes=final_outcomes,
                elapsed_sec=elapsed,
                tar_path=tar_path,
            )
            print(f"[Report] Overview: {overview}")
        except Exception as e:
            print(f"[Report] Failed: {e}")

    print("=" * 72)


if __name__ == "__main__":
    main()
