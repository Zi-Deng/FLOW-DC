#!/usr/bin/env python3
"""
FLOW-DC Batch Downloader with PAARC v2.0

PAARC: Policy-Aware Adaptive Request Controller
A concurrency-based congestion control algorithm for large-scale HTTP downloading.

Key Design Principles:
- Concurrency-primary control: Semaphore limits concurrent requests; rate emerges naturally
- Self-regulating: When server latency increases, throughput automatically decreases
- Policy-aware: Responds appropriately to server rate limits (HTTP 429) and errors
- Measurement-driven: All control decisions based on observed TTFB and goodput
- Smooth operation: Leaky bucket provides request smoothing without accumulation

Theoretical Foundation:
- Little's Law: C = R × T, where R = C / T (rate is emergent)
- AIMD convergence (Chiu & Jain, 1989)
- Kleinrock's power metric optimal at μ ≈ 0.75

State Machine:
    INIT → STARTUP → PROBE_BW ↔ PROBE_RTT
              ↓           ↓
           BACKOFF ←──────┘

Author: FLOW-DC Team
Version: 2.0.0
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import os
import re
import shutil
import signal
import tarfile
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit

import aiohttp
import polars as pl
from tqdm.asyncio import tqdm
import flowdc_integrity as integrity

from single_download import (
    download_single, load_input_file, extract_extension, HTTPTraceConfig,
    HTTP_TRACE_CTX, HTTP_MEASUREMENT_VERSION, OUTPUT_CTX,
)


# =============================================================================
# GLOBALS AND SHUTDOWN HANDLING
# =============================================================================

shutdown_flag = False


def _signal_handler(sig, frame):
    global shutdown_flag
    print("\n[Shutdown] Interrupt received. Attempting graceful shutdown...")
    shutdown_flag = True


def _setup_signal_handlers():
    """Register signal handlers. Called from main() to avoid side effects on import."""
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

# Trace context for aiohttp TTFB measurement
TRACE_CTX = HTTP_TRACE_CTX


# =============================================================================
# PLATEAU DETECTION MODE
# =============================================================================
# Choose plateau detection method for STARTUP phase:
#   "latency"    - BBR-style: detect when p50/p95 exceeds RTprop × threshold
#   "derivative" - Efficiency-based: detect when goodput gain per +1 C drops
PLATEAU_DETECTION_MODE = "latency"  # Options: "latency", "derivative"


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def _now() -> float:
    """Current time in seconds (monotonic for intervals, time.time for timestamps)."""
    return time.time()


def _monotonic() -> float:
    """Monotonic time for duration measurements."""
    return time.monotonic()


def _sanitize_filename(name: str, max_len: int = 180) -> str:
    """Sanitize a string for use as a filename."""
    name = name.strip().replace(os.sep, "_")
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("._")
    if not name:
        name = "file"
    return name[:max_len]


def _percentile(values: list[float], p: float) -> Optional[float]:
    """
    Calculate percentile using linear interpolation.
    
    Args:
        values: List of numeric values
        p: Percentile in range [0, 1]
    
    Returns:
        Percentile value or None if list is empty
    """
    if not values:
        return None
    
    sorted_values = sorted(values)
    n = len(sorted_values)
    
    if n == 1:
        return sorted_values[0]
    
    # Map percentile to index
    idx = (n - 1) * p
    lower = int(idx)
    upper = min(lower + 1, n - 1)
    
    if lower == upper:
        return sorted_values[lower]
    
    # Linear interpolation
    weight = idx - lower
    return sorted_values[lower] + weight * (sorted_values[upper] - sorted_values[lower])


def _is_connection_error(msg: Any) -> bool:
    """Check if an error message indicates a connection-level failure."""
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
    """Determine if a request failure is retryable."""
    if status_code == 429 or status_code == 408:
        return True
    if status_code is not None and status_code >= 500:
        return True
    return _is_connection_error(error)


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass(frozen=True)
class PAARCConfig:
    """
    Complete configuration for PAARC v2.0 controller.
    
    All parameters have sensible defaults based on theoretical analysis
    and empirical testing.
    """
    # --- Concurrency Bounds ---
    C_min: int = 2                      # Absolute floor; never operate below
    C_max: int = 10_000                 # Absolute ceiling; safety limit
    C_init: int = 4                     # Starting point for INIT phase
    
    # --- Timing Parameters ---
    N_min: int = 50                     # Minimum samples per control interval
    k_interval: int = 8                 # RTprop multiplier for interval duration
    T_floor: float = 0.2                # 200ms minimum interval duration
    
    # --- INIT Phase ---
    N_init: int = 100                    # Samples to collect in INIT phase
    
    # --- STARTUP Phase (Plateau Detection) ---
    # Latency-based plateau detection (PLATEAU_DETECTION_MODE = "latency")
    startup_theta_50: float = 3.0     # p50 > RTprop × threshold = plateau
    startup_theta_95: float = 4.0     # p95 > RTprop × threshold = plateau
    # Derivative-based plateau detection (PLATEAU_DETECTION_MODE = "derivative")
    efficiency_threshold: float = 0.5   # Plateau if efficiency < 50% of expected
    efficiency_window: int = 5          # Compare goodput over N intervals
    startup_additive_increase: int = 30  # Additive increase per interval in STARTUP

    # --- PROBE_BW Phase ---
    mu: float = 0.85                    # Utilization factor (operating margin)
    stable_intervals_required: int = 1  # Stable intervals before probing
    probe_bw_additive_increase: int = 10  # Additive increase per stable interval in PROBE_BW

    # --- PROBE_RTT Phase ---
    probe_rtt_period: float = 10.0      # Seconds between PROBE_RTT entries
    probe_rtt_min_samples: int = 100    # Minimum samples before PROBE_RTT trigger
    probe_rtt_concurrency_factor: float = 0.5  # Reduce to 50% of current C

    # --- BACKOFF Phase ---
    beta: float = 0.5                   # Multiplicative decrease factor
    cooldown_rtprop_mult: int = 5       # Cooldown = max(5 × RTprop, ...)
    cooldown_floor: float = 2.0         # 2 second minimum cooldown
    overload_check_rtprop_mult: float = 10.0  # Wait 10 × RTprop before checking continued overload
    overload_check_floor: float = 3.0         # Minimum 3 seconds between overload checks

    # --- Ceiling Revision ---
    underperformance_threshold: float = 0.5  # Goodput < expected × this
    underperformance_intervals: int = 5       # Consecutive intervals to trigger
    revision_factor: float = 0.99          # Multiplicative ceiling reduction

    # --- Latency Thresholds (PROBE_BW phase) ---
    theta_50: float = 1.5             # Median degradation threshold
    theta_95: float = 2.0             # Tail degradation threshold
    
    # --- Smoothing ---
    alpha_ema: float = 0.3              # EMA smoothing factor
    rtprop_window: float = 35.0         # RTprop tracking window (seconds)


@dataclass(frozen=True)
class Config:
    """Main application configuration."""
    input_path: str
    output_folder: str
    
    input_format: str = "parquet"
    url_col: str = "url"
    label_col: Optional[str] = None
    
    output_format: str = "imagefolder"
    
    concurrent_downloads: Optional[int] = None  # None or 0 = auto-detect based on host count
    timeout_sec: int = 30
    
    # PAARC controller toggle
    enable_paarc: bool = True
    control_method: str | None = None  # Explicit version; None preserves legacy behavior.
    method_options: dict = field(default_factory=dict)
    shared_control_file: str | None = None  # Private descriptor path, never a credential value.
    
    # PAARC parameters (passed to PAARCConfig)
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
    rtprop_window: float = 35.0
    cooldown_floor: float = 2.0
    alpha_ema: float = 0.3
    startup_additive_increase: int = 30
    probe_bw_additive_increase: int = 10

    # Retry configuration
    max_retry_attempts: int = 3
    retry_backoff_sec: float = 2.0
    
    # Filename configuration
    naming_mode: str = "sequential"     # sequential | url_based | row_id
    file_name_pattern: str = "{segment[-2]}"
    
    # Output options
    create_tar: bool = True
    compress_tar: bool = True  # False for uncompressed .tar (faster)
    create_overview: bool = True
    force_overwrite: bool = False  # If True, delete existing output folder without confirmation

    resume: bool = False
    reconcile: bool = False
    research_profile: bool = False

    def to_paarc_config(self) -> PAARCConfig:
        """Convert to PAARCConfig with relevant parameters."""
        return PAARCConfig(
            C_init=self.C_init,
            C_min=self.C_min,
            C_max=self.C_max,
            mu=self.mu,
            startup_theta_50=self.startup_theta_50,
            startup_theta_95=self.startup_theta_95,
            efficiency_threshold=self.efficiency_threshold,
            efficiency_window=self.efficiency_window,
            startup_additive_increase=self.startup_additive_increase,
            probe_bw_additive_increase=self.probe_bw_additive_increase,
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
        description="FLOW-DC Batch Downloader with PAARC v2.0 Congestion Control",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_batch.py --config gbif.json
  python download_batch.py --input urls.parquet --output images/ --enable_paarc
"""
    )
    
    p.add_argument("--config", type=str, help="Path to JSON config file")
    
    # Input/Output
    p.add_argument("--input", dest="input_path", type=str, help="Input file path")
    p.add_argument("--input_format", type=str, default="parquet")
    p.add_argument("--url", dest="url_col", type=str, default="url")
    p.add_argument("--label", dest="label_col", type=str, default=None)
    p.add_argument("--output", dest="output_folder", type=str, help="Output folder")
    p.add_argument("--output_format", type=str, default="imagefolder")
    
    # Download settings
    p.add_argument("--concurrent_downloads", type=int, default=0,
                   help="Worker pool size. 0 = auto-detect based on unique hosts (recommended)")
    p.add_argument("--timeout", dest="timeout_sec", type=int, default=30)
    
    # PAARC toggle
    p.add_argument("--enable_paarc", action="store_true", default=True)
    p.add_argument("--disable_paarc", action="store_true")
    p.add_argument("--control_method", choices=("paarc-base-v2", "gradient-candidate-v1", "fixed-v1", "ratio-v1"),
                   help="Explicit versioned per-origin control method; omitted preserves legacy defaults")
    p.add_argument("--method_options", type=json.loads, default={}, help="JSON object of versioned engineering parameters")
    p.add_argument("--shared_control_file", help="Owner-private authenticated manager descriptor; no local fallback")
    
    # PAARC parameters
    p.add_argument("--C_init", type=int, default=4)
    p.add_argument("--C_min", type=int, default=2)
    p.add_argument("--C_max", type=int, default=10000)
    p.add_argument("--mu", type=float, default=0.85, help="Utilization factor")
    p.add_argument("--startup_theta_50", type=float, default=3.0)
    p.add_argument("--startup_theta_95", type=float, default=4.0)
    p.add_argument("--efficiency_threshold", type=float, default=0.5)
    p.add_argument("--efficiency_window", type=int, default=5)
    p.add_argument("--beta", type=float, default=0.5, help="Backoff factor")
    p.add_argument("--theta_50", type=float, default=1.5)
    p.add_argument("--theta_95", type=float, default=2.0)
    p.add_argument("--probe_rtt_period", type=float, default=10.0)
    p.add_argument("--cooldown_floor", type=float, default=2.0)
    p.add_argument("--alpha_ema", type=float, default=0.3)
    p.add_argument("--startup_additive_increase", type=int, default=30,
                   help="Additive increase per interval in STARTUP phase")
    p.add_argument("--probe_bw_additive_increase", type=int, default=10,
                   help="Additive increase per stable interval in PROBE_BW phase")

    # Retry
    p.add_argument("--max_retry_attempts", type=int, default=3)
    p.add_argument("--retry_backoff_sec", type=float, default=2.0)
    
    # Naming
    p.add_argument("--naming_mode", type=str, default="sequential",
                   choices=["sequential", "url_based", "row_id"])
    p.add_argument("--file_name_pattern", type=str, default="{segment[-2]}")
    
    # Output options
    p.add_argument("--no_tar", action="store_true")
    p.add_argument("--no_compress_tar", action="store_true",
                   help="Create uncompressed .tar instead of .tar.gz (faster)")
    p.add_argument("--no_overview", action="store_true")
    p.add_argument("--force", "-f", action="store_true",
                   help="Force overwrite of existing output folder without confirmation")

    recovery = p.add_mutually_exclusive_group()
    recovery.add_argument("--resume", action="store_true", help="Reconcile owned output and continue eligible rows")
    recovery.add_argument("--reconcile", action="store_true", help="Reconcile owned output offline; no HTTP calls")
    p.add_argument("--research_profile", action="store_true", help="Row-ID naming and verified uncompressed WebDataset archive boundary")
    args = p.parse_args()
    
    # Load from JSON config if provided
    if args.config:
        cfg_path = Path(args.config)
        with cfg_path.open("r") as f:
            data = json.load(f)
        
        return Config(
            input_path=data.get("input", ""),
            output_folder=data.get("output", ""),
            input_format=data.get("input_format", "parquet"),
            url_col=data.get("url", "url"),
            label_col=data.get("label"),
            output_format=data.get("output_format", "imagefolder"),
            concurrent_downloads=data.get("concurrent_downloads"),  # None = auto-detect
            timeout_sec=int(data.get("timeout", 30)),
            enable_paarc=bool(data.get("enable_paarc", True)),
            C_init=int(data.get("C_init", 4)),
            C_min=int(data.get("C_min", 2)),
            C_max=int(data.get("C_max", 10000)),
            mu=float(data.get("mu", 0.85)),
            startup_theta_50=float(data.get("startup_theta_50", 3.0)),
            startup_theta_95=float(data.get("startup_theta_95", 4.0)),
            efficiency_threshold=float(data.get("efficiency_threshold", 0.5)),
            efficiency_window=int(data.get("efficiency_window", 5)),
            beta=float(data.get("beta", 0.5)),
            theta_50=float(data.get("theta_50", 1.5)),
            theta_95=float(data.get("theta_95", 2.0)),
            probe_rtt_period=float(data.get("probe_rtt_period", 10.0)),
            rtprop_window=float(data.get("rtprop_window", 35.0)),
            cooldown_floor=float(data.get("cooldown_floor", 2.0)),
            alpha_ema=float(data.get("alpha_ema", 0.3)),
            startup_additive_increase=int(data.get("startup_additive_increase", 30)),
            probe_bw_additive_increase=int(data.get("probe_bw_additive_increase", 10)),
            max_retry_attempts=int(data.get("max_retry_attempts", 3)),
            retry_backoff_sec=float(data.get("retry_backoff_sec", 2.0)),
            naming_mode=data.get("naming_mode", "sequential"),
            file_name_pattern=data.get("file_name_pattern", "{segment[-2]}"),
            create_tar=bool(data.get("create_tar", True)),
            compress_tar=bool(data.get("compress_tar", True)),
            create_overview=bool(data.get("create_overview", True)),
            force_overwrite=args.force or bool(data.get("force_overwrite", False)),
            resume=args.resume or bool(data.get("resume", False)),
            reconcile=args.reconcile or bool(data.get("reconcile", False)),
            research_profile=args.research_profile or bool(data.get("research_profile", False)),
            control_method=args.control_method or data.get("control_method"),
            method_options=args.method_options or data.get("method_options", {}),
            shared_control_file=args.shared_control_file or data.get("shared_control_file"),
        )

    # Validate required args
    if not args.output_folder or (not args.input_path and not args.reconcile):
        p.error("--input and --output are required unless --config is provided")
    
    return Config(
        input_path=args.input_path or "",
        output_folder=args.output_folder,
        input_format=args.input_format,
        url_col=args.url_col,
        label_col=args.label_col,
        output_format=args.output_format,
        concurrent_downloads=args.concurrent_downloads if args.concurrent_downloads > 0 else None,
        timeout_sec=args.timeout_sec,
        enable_paarc=args.enable_paarc and not args.disable_paarc,
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
        startup_additive_increase=args.startup_additive_increase,
        probe_bw_additive_increase=args.probe_bw_additive_increase,
        max_retry_attempts=args.max_retry_attempts,
        retry_backoff_sec=args.retry_backoff_sec,
        naming_mode=args.naming_mode,
        file_name_pattern=args.file_name_pattern,
        create_tar=not args.no_tar,
        compress_tar=not args.no_compress_tar,
        create_overview=not args.no_overview,
        force_overwrite=args.force,
        resume=args.resume, reconcile=args.reconcile, research_profile=args.research_profile,
        control_method=args.control_method, method_options=args.method_options,
        shared_control_file=args.shared_control_file,
    )


# =============================================================================
# INPUT VALIDATION AND LOADING
# =============================================================================

def normalize_config(cfg):
    if cfg.shared_control_file is not None:
        if not isinstance(cfg.shared_control_file, str) or not cfg.shared_control_file or cfg.control_method is None:
            raise ValueError("shared control requires a private descriptor path and explicit control_method")
    if cfg.control_method is not None:
        from flowdc_methods import MethodConfig
        if not cfg.enable_paarc or not isinstance(cfg.method_options, dict):
            raise ValueError("Explicit control_method requires enabled admission and object method_options")
        if hasattr(cfg, "gradient_threshold"):
            raise ValueError("Select versioned methods through download_batch.py; gradient entrypoint is legacy")
        MethodConfig.from_config(cfg)
    elif cfg.method_options:
        raise ValueError("method_options requires an explicit control_method")
    if cfg.resume and cfg.reconcile:
        raise ValueError("resume and reconcile are mutually exclusive")
    if (cfg.resume or cfg.reconcile) and cfg.force_overwrite:
        raise ValueError("overwrite and recovery modes are mutually exclusive")
    if cfg.research_profile:
        from dataclasses import replace
        cfg = replace(cfg, naming_mode="row_id", output_format="webdataset", create_tar=True,
                      compress_tar=False, create_overview=True)
    if cfg.naming_mode not in ("sequential", "url_based", "row_id"):
        raise ValueError("Unsupported naming mode")
    if cfg.output_format not in ("imagefolder", "webdataset"):
        raise ValueError("Unsupported output format")
    if type(cfg.max_retry_attempts) is not int or cfg.max_retry_attempts < 1:
        raise ValueError("max_retry_attempts must be positive")
    return cfg


def load_manifest(cfg):
    path = Path(cfg.input_path)
    raw = path.read_bytes()
    df = load_input_file(str(path), cfg.input_format)
    if cfg.url_col not in df.columns:
        raise ValueError(f"URL column '{cfg.url_col}' not found")
    if cfg.label_col is not None and cfg.label_col not in df.columns:
        raise ValueError(f"Label column '{cfg.label_col}' not found")
    if cfg.url_col == "__key__" or (cfg.label_col and cfg.label_col == "__key__"):
        raise ValueError("__key__ is reserved for internal row identity; rename the mapped input column")
    if cfg.control_method is not None:
        from flowdc_methods import validate_research_metadata
        validate_research_metadata(df, cfg.url_col)
    # Verify input has not changed between hashing and loading.
    if path.read_bytes() != raw:
        raise ValueError("Input manifest changed during loading")
    df = integrity.stamp_frame(df, integrity.digest(raw))
    return df, {"sha256": integrity.digest(raw), "original_rows": df.height,
                "source_counts": {row[integrity.PROVENANCE[0]]: row[integrity.PROVENANCE[2]]
                                  for row in df.iter_rows(named=True)}}


def validate_and_load(cfg: Config, prepared=None) -> pl.DataFrame:
    """Validate the entire manifest and output ownership before overwrite consent."""
    cfg = normalize_config(cfg)
    df, _manifest = prepared if prepared is not None else load_manifest(cfg)
    out = Path(os.path.abspath(cfg.output_folder))
    if out == Path(out.anchor) or out == Path.cwd() or out in Path(cfg.input_path).resolve().parents:
        raise ValueError("Output cannot contain the input or be the working/root directory")
    integrity.component(out.name)
    # Opening every existing ancestor rejects symlinks, including the root itself.
    with integrity.Files(out.parent, create=True):
        pass
    if out.exists() or out.is_symlink():
        with integrity.Files(out):
            pass
        if (out / integrity.INTERNAL).exists() or (out / integrity.INTERNAL).is_symlink():
            # Never erase a malformed/newer journal or an active managed run.
            with integrity.RunStore(out):
                pass
        if cfg.resume or cfg.reconcile:
            return df
        if not cfg.force_overwrite:
            response = input(f"[I/O] Output folder exists: {out}\n      Delete and continue? [y/N]: ").strip().lower()
            if response not in ("y", "yes"):
                raise SystemExit("Aborted: output folder exists. Use --force to overwrite.")
        print(f"[I/O] Deleting existing output with consent: {out}")
        if (out / integrity.INTERNAL).exists():
            with integrity.RunStore(out) as previous:
                previous.remove_owned_exports()
                shutil.rmtree(out)
        else:
            shutil.rmtree(out)
    elif cfg.resume or cfg.reconcile:
        raise ValueError("Recovery requires existing owned output")
    out.mkdir(parents=True)
    return df


def count_unique_hosts(df: pl.DataFrame, url_col: str) -> int:
    """
    Count unique hosts in the URL column.

    Used for auto-sizing the worker pool and connection limits.

    Args:
        df: DataFrame with URLs
        url_col: Name of the URL column

    Returns:
        Number of unique hosts found
    """
    # Extract hosts from URLs using Polars string operations
    # URL format: scheme://host/path -> extract host part
    hosts = (
        df.select(pl.col(url_col))
        .with_columns(
            pl.col(url_col)
            .str.replace(r"^https?://", "")  # Remove scheme
            .str.replace(r"/.*$", "")         # Remove path
            .str.to_lowercase()
            .alias("__host__")
        )
        .select("__host__")
        .unique()
    )
    return hosts.height


# =============================================================================
# AIOHTTP TRACING (TTFB MEASUREMENT)
# =============================================================================

def resolve_worker_count(cfg: Config, df: pl.DataFrame) -> int:
    """Resolve explicit or host-based concurrency for either downloader."""
    num_hosts = count_unique_hosts(df, cfg.url_col)
    print(f"[Load] Unique hosts detected: {num_hosts}")
    if cfg.concurrent_downloads is not None and cfg.concurrent_downloads > 0:
        workers = cfg.concurrent_downloads
        print(f"[Config] Using configured concurrent_downloads: {workers}")
    else:
        workers = max(1, min(num_hosts * cfg.C_max, 10000))
        print(f"[Config] Auto-sized worker pool: {workers} "
              f"({num_hosts} hosts × {cfg.C_max} C_max)")
    return workers


def build_trace_config() -> aiohttp.TraceConfig:
    """Shared per-hop admission/headers tracing; body timing lives in the reader."""
    return HTTPTraceConfig()


# =============================================================================
# PAARC v2.0 STATE MACHINE
# =============================================================================

class PAARCState(Enum):
    """PAARC controller states."""
    INIT = auto()
    STARTUP = auto()
    PROBE_BW = auto()
    PROBE_RTT = auto()
    BACKOFF = auto()


# =============================================================================
# CONCURRENCY CONTROL: ADAPTIVE SEMAPHORE
# =============================================================================

class AdaptiveSemaphore:
    """
    Adjustable semaphore for concurrency control.
    
    The primary control mechanism in PAARC v2.0. Limits the number of
    concurrent in-flight requests.
    """
    
    def __init__(self, initial: int, minimum: int = 2, maximum: int = 10000):
        self._limit = max(minimum, min(initial, maximum))
        self._minimum = minimum
        self._maximum = maximum
        self._inflight = 0
        self._cond = asyncio.Condition()
    
    async def acquire(self) -> None:
        """Acquire a slot, blocking if at limit."""
        async with self._cond:
            while self._inflight >= self._limit and not shutdown_flag:
                await self._cond.wait()
            self._inflight += 1
    
    async def release(self) -> None:
        """Release a slot."""
        async with self._cond:
            self._inflight = max(0, self._inflight - 1)
            self._cond.notify(1)  # Only wake ONE waiter (fixes thundering herd)
    
    @property
    def limit(self) -> int:
        """Current concurrency limit."""
        return self._limit
    
    @property
    def inflight(self) -> int:
        """Current in-flight count."""
        return self._inflight
    
    def set_limit(self, new_limit: int, reason: str = "") -> None:
        """Adjust the concurrency limit."""
        new_limit = max(self._minimum, min(new_limit, self._maximum))
        if new_limit != self._limit:
            old = self._limit
            self._limit = new_limit
            reason_str = f" ({reason})" if reason else ""
            print(f"[PAARC] Concurrency: {old} → {new_limit}{reason_str}")
            # Notify waiters if limit increased (fixes stalled limit increases)
            if new_limit > old:
                asyncio.create_task(self._notify_waiters(new_limit - old))

    async def _notify_waiters(self, count: int) -> None:
        """Wake up waiters when limit increases."""
        async with self._cond:
            self._cond.notify(count)


# =============================================================================
# LEAKY BUCKET SMOOTHER (NON-ACCUMULATING)
# =============================================================================

class LeakyBucketSmoother:
    """
    Non-accumulating leaky bucket for request smoothing.
    
    Unlike a token bucket, this does NOT accumulate credit during periods
    of low activity. This prevents bursts after PROBE_RTT or idle periods.
    
    The smoothing rate is derived from Little's Law:
        implied_rate = C / RTprop
        min_delay = 1 / implied_rate = RTprop / C
    """
    
    def __init__(self, concurrency: int, rtprop: float):
        self._concurrency = concurrency
        self._rtprop = rtprop
        self._min_delay = self._calculate_min_delay()
        self._last_request_time: float = 0.0
        self._lock = asyncio.Lock()
    
    def _calculate_min_delay(self) -> float:
        """Calculate minimum inter-request delay."""
        if self._rtprop > 0 and self._concurrency > 0:
            return self._rtprop / self._concurrency
        return 0.01  # 10ms default
    
    def update(self, concurrency: int, rtprop: float) -> None:
        """Update smoothing parameters."""
        self._concurrency = max(1, concurrency)
        self._rtprop = max(0.001, rtprop)  # Minimum 1ms RTprop
        self._min_delay = self._calculate_min_delay()
    
    async def acquire(self) -> None:
        """Wait if necessary to maintain smooth request spacing."""
        # Calculate wait time while holding lock, then sleep OUTSIDE the lock
        # This fixes the serialization bug where lock was held during sleep
        async with self._lock:
            now = _monotonic()
            elapsed = now - self._last_request_time
            wait_time = max(0.0, self._min_delay - elapsed)
            # Reserve our slot by setting the expected request time
            self._last_request_time = now + wait_time

        # Sleep outside the lock to allow other tasks to acquire slots
        if wait_time > 0:
            await asyncio.sleep(wait_time)
    
    @property
    def implied_rate(self) -> float:
        """Current implied rate (requests per second)."""
        if self._min_delay > 0:
            return 1.0 / self._min_delay
        return float('inf')


# =============================================================================
# HOST METRICS COLLECTOR
# =============================================================================

class HostMetrics:
    """
    Per-host metrics collection and statistics.

    Collects TTFB samples, error counts, and bytes downloaded per interval.
    Computes percentiles and EMA-smoothed values for stable control signals.
    """

    def __init__(self, config: PAARCConfig, host: str = "unknown"):
        self.config = config
        self.host = host
        self._lock = asyncio.Lock()
        
        # Per-interval accumulators
        self._ttfb_samples: list[float] = []
        self._file_sizes: list[int] = []
        self._n_success = 0
        self._n_errors = 0
        self._n_http_failures = 0
        self._n_local_failures = 0
        self._n_transport_failures = 0
        self._n_unknown_failures = 0
        self._bytes_downloaded = 0
        self._interval_start = _monotonic()
        
        # Retry-After tracking
        self._retry_after: Optional[float] = None
        
        # EMA-smoothed percentiles
        self._ema_p10: Optional[float] = None
        self._ema_p50: Optional[float] = None
        self._ema_p95: Optional[float] = None
        
        # RTprop tracking (minimum p10 over sliding window)
        self._rtprop: Optional[float] = None
        self._rtprop_samples: deque[tuple[float, float]] = deque()  # (timestamp, p10)
        
        # Goodput history for plateau detection
        self._goodput_history: deque[float] = deque(maxlen=10)
        
        # Lifetime statistics
        self._total_samples = 0
        self._total_bytes = 0
        self._avg_file_size: Optional[float] = None
    
    async def record(
        self,
        status_code: Optional[int],
        ttfb: Optional[float],
        bytes_downloaded: int = 0,
        is_conn_error: bool = False,
        retry_after_sec: Optional[float] = None,
        *,
        acquisition_success: Optional[bool] = None,
        is_local_error: bool = False,
        is_unknown_error: bool = False,
        latency_eligible: Optional[bool] = None,
        dispatch_at: float | None = None,
    ) -> None:
        """Record one completed acquisition attempt, separately from overload.

        Legacy n_errors counts overload feedback; all unsuccessful attempts are
        also counted by a disjoint outcome category. Callers that omit explicit
        saved-output success retain HTTP-200 inference for compatibility.
        Explicit completed-body latency eligibility is independent of local
        output success. Legacy callers retain the saved nonempty-output rule.
        """
        async with self._lock:
            # Track Retry-After if provided
            if retry_after_sec is not None and math.isfinite(retry_after_sec) and retry_after_sec >= 0:
                self._retry_after = max(self._retry_after or 0.0, retry_after_sec)
            
            # Classify as success or error
            is_error = not is_local_error and not is_unknown_error and (
                is_conn_error or
                status_code == 429 or
                status_code == 408 or
                (status_code is not None and status_code >= 500)
            )
            
            if is_error:
                self._n_errors += 1
            success = (status_code == 200 and not is_conn_error and not is_local_error
                       and not is_unknown_error and acquisition_success is not False)
            eligible = success and bytes_downloaded > 0 if latency_eligible is None else latency_eligible
            if (eligible and status_code == 200 and not is_conn_error and not is_unknown_error
                    and ttfb is not None and math.isfinite(ttfb) and ttfb > 0):
                self._ttfb_samples.append(ttfb)
            if success:
                self._n_success += 1
                self._bytes_downloaded += bytes_downloaded
                
                # Record file size
                if bytes_downloaded > 0:
                    self._file_sizes.append(bytes_downloaded)
            elif is_unknown_error:
                self._n_unknown_failures += 1
            elif is_local_error or (status_code == 200 and not is_conn_error):
                self._n_local_failures += 1
            elif is_conn_error or status_code is None:
                self._n_transport_failures += 1
            else:
                self._n_http_failures += 1
    
    def _update_ema(self, current: Optional[float], new_value: Optional[float]) -> Optional[float]:
        """Update EMA with new value."""
        if new_value is None:
            return current
        if current is None:
            return new_value
        return self.config.alpha_ema * new_value + (1 - self.config.alpha_ema) * current
    
    async def finish_interval(self, allow_rtprop_update: bool = True) -> dict[str, Any]:
        """
        Finalize interval and compute statistics.

        Args:
            allow_rtprop_update: If True (INIT/PROBE_RTT phases), add all p10 samples
                to the RTprop window. If False (other phases), only accept new minimums
                to avoid polluting RTprop with inflated samples from high-concurrency periods.

        Returns a snapshot dict with all metrics for controller decisions.
        """
        async with self._lock:
            now = _monotonic()
            duration = now - self._interval_start
            
            # Snapshot and reset interval accumulators
            ttfb_samples = self._ttfb_samples.copy()
            file_sizes = self._file_sizes.copy()
            n_success = self._n_success
            n_errors = self._n_errors
            n_http_failures = self._n_http_failures
            n_local_failures = self._n_local_failures
            n_transport_failures = self._n_transport_failures
            n_unknown_failures = self._n_unknown_failures
            bytes_downloaded = self._bytes_downloaded
            retry_after = self._retry_after
            
            self._ttfb_samples.clear()
            self._file_sizes.clear()
            self._n_success = 0
            self._n_errors = 0
            self._n_http_failures = 0
            self._n_local_failures = 0
            self._n_transport_failures = 0
            self._n_unknown_failures = 0
            self._bytes_downloaded = 0
            self._interval_start = now
            self._retry_after = None
            
            # Update lifetime stats
            self._total_samples += len(ttfb_samples)
            self._total_bytes += bytes_downloaded
            
            # Calculate average file size
            if file_sizes:
                if self._avg_file_size is None:
                    self._avg_file_size = sum(file_sizes) / len(file_sizes)
                else:
                    # EMA update
                    new_avg = sum(file_sizes) / len(file_sizes)
                    self._avg_file_size = 0.1 * new_avg + 0.9 * self._avg_file_size
        
        n_failed = n_http_failures + n_local_failures + n_transport_failures + n_unknown_failures
        total = n_success + n_failed
        n_samples = len(ttfb_samples)
        
        # Calculate raw percentiles (require minimum samples)
        p10_raw = _percentile(ttfb_samples, 0.10) if n_samples >= 5 else None
        p50_raw = _percentile(ttfb_samples, 0.50) if n_samples >= 5 else None
        p95_raw = _percentile(ttfb_samples, 0.95) if n_samples >= 5 else None
        
        # Update EMA-smoothed values
        self._ema_p10 = self._update_ema(self._ema_p10, p10_raw)
        self._ema_p50 = self._update_ema(self._ema_p50, p50_raw)
        self._ema_p95 = self._update_ema(self._ema_p95, p95_raw)
        
        # Update RTprop (minimum p10 over sliding window)
        # BBR-style: only fully update during INIT/PROBE_RTT; otherwise only accept new minimums
        old_rtprop = self._rtprop
        if p10_raw is not None:
            # Always expire old samples to maintain window
            cutoff = now - self.config.rtprop_window
            while self._rtprop_samples and self._rtprop_samples[0][0] < cutoff:
                self._rtprop_samples.popleft()

            if allow_rtprop_update:
                # INIT/PROBE_RTT: Add all samples to window for full RTprop discovery
                self._rtprop_samples.append((now, p10_raw))
            else:
                # STARTUP/PROBE_BW/BACKOFF: Only add if it's a new minimum
                # This prevents inflated samples from polluting RTprop
                if self._rtprop is None or p10_raw < self._rtprop:
                    self._rtprop_samples.append((now, p10_raw))

            # RTprop is minimum p10 in window
            if self._rtprop_samples:
                self._rtprop = min(s[1] for s in self._rtprop_samples)

            # Print RTprop statistics when RTprop is updated
            if self._rtprop != old_rtprop:
                rtprop_ms = self._rtprop * 1000 if self._rtprop else 0
                p10_ms = (self._ema_p10 or 0) * 1000
                p50_ms = (self._ema_p50 or 0) * 1000
                p95_ms = (self._ema_p95 or 0) * 1000
                avg_kb = (self._avg_file_size or 0) / 1024
                update_type = "full" if allow_rtprop_update else "new_min"
                print(f"[PAARC] {self.host}: RTprop updated ({update_type}) → {rtprop_ms:.0f}ms | "
                      f"p10={p10_ms:.0f}ms p50={p50_ms:.0f}ms p95={p95_ms:.0f}ms | "
                      f"AvgSize={avg_kb:.1f}KB | Samples={self._total_samples}")

        # Calculate goodput (requests per second)
        goodput_rps = n_success / duration if duration > 0 else 0.0
        goodput_bps = bytes_downloaded / duration if duration > 0 else 0.0
        self._goodput_history.append(goodput_rps)
        
        return {
            "total": total,
            "n_success": n_success,
            "n_errors": n_errors,
            "n_failed": n_failed,
            "n_http_failures": n_http_failures,
            "n_local_failures": n_local_failures,
            "n_transport_failures": n_transport_failures,
            "n_unknown_failures": n_unknown_failures,
            "n_samples": n_samples,
            "has_overload": n_errors > 0,
            "p10": self._ema_p10,
            "p50": self._ema_p50,
            "p95": self._ema_p95,
            "p10_raw": p10_raw,
            "p50_raw": p50_raw,
            "p95_raw": p95_raw,
            "rtprop": self._rtprop,
            "goodput_rps": goodput_rps,
            "goodput_bps": goodput_bps,
            "bytes": bytes_downloaded,
            "duration": duration,
            "retry_after": retry_after,
            "avg_file_size": self._avg_file_size,
            "total_samples_lifetime": self._total_samples,
        }
    
    def reset_goodput_history(self) -> None:
        """
        Reset goodput history.

        Called when entering STARTUP to ensure plateau detection
        only considers STARTUP phase performance, not INIT phase values.
        """
        self._goodput_history.clear()

    @property
    def rtprop(self) -> Optional[float]:
        """Current RTprop estimate (minimum observed latency)."""
        return self._rtprop
    
    @property
    def total_samples(self) -> int:
        """Total TTFB samples collected lifetime."""
        return self._total_samples
    
    @property
    def avg_file_size(self) -> Optional[float]:
        """Average file size in bytes."""
        return self._avg_file_size

    @property
    def last_goodput(self) -> Optional[float]:
        """Most recent goodput measurement (requests per second)."""
        return self._goodput_history[-1] if self._goodput_history else None

    @property
    def goodput_history(self) -> deque[float]:
        """Goodput history for plateau detection."""
        return self._goodput_history


# =============================================================================
# PAARC v2.0 CONTROLLER
# =============================================================================

class PAARCController:
    """
    PAARC v2.0: Policy-Aware Adaptive Request Controller
    
    Concurrency-primary congestion control for HTTP downloading.
    
    State Machine:
        INIT → STARTUP → PROBE_BW ↔ PROBE_RTT
                  ↓           ↓
               BACKOFF ←──────┘
    
    Core principles:
    - Concurrency (C) is the primary control variable
    - Rate (R) emerges from Little's Law: R = C / RTprop
    - Uses single utilization factor μ = 0.75
    - All errors treated equivalently
    - Gradual restoration after PROBE_RTT
    """
    
    def __init__(self, host: str, config: PAARCConfig):
        self.host = host
        self.config = config
        
        # State
        self.state = PAARCState.INIT
        
        # Concurrency control (primary)
        self.semaphore = AdaptiveSemaphore(
            initial=config.C_init,
            minimum=config.C_min,
            maximum=config.C_max
        )
        
        # Metrics
        self.metrics = HostMetrics(config, host)

        # Smoother (initialized after RTprop known)
        self.smoother: Optional[LeakyBucketSmoother] = None
        
        # PAARC state variables
        self._concurrency = config.C_init
        self._C_ceiling: Optional[int] = None
        self._C_operating: int = config.C_init
        
        # INIT phase
        self._init_samples = 0

        # PROBE_BW phase
        self._stable_intervals = 0
        self._underperformance_intervals = 0
        self._was_latency_degraded = False  # Track previous state to reduce log spam
        
        # PROBE_RTT phase
        self._last_probe_rtt_time: float = 0.0
        self._samples_since_probe_rtt: int = 0
        self._saved_concurrency: Optional[int] = None
        self._restoring: bool = False
        
        # BACKOFF phase
        self._cooldown_until: float = 0.0
        self._last_overload_reduction_time: float = 0.0  # Track last reduction for cooldown

        # Timing
        self._last_interval_time = _monotonic()
    
    def _get_rtprop(self) -> float:
        """Get RTprop with fallback."""
        rtprop = self.metrics.rtprop
        return rtprop if rtprop is not None else 0.2  # 200ms default
    
    def _set_concurrency(self, new_C: int, reason: str) -> None:
        """Set concurrency with bounds enforcement."""
        new_C = max(self.config.C_min, min(new_C, self.config.C_max))
        if new_C != self._concurrency:
            self._concurrency = new_C
            self.semaphore.set_limit(new_C, reason)
            self._update_smoother()

            # Print goodput (measured) and inferred rate (theoretical) with units
            goodput_rps = self.metrics.last_goodput
            rtprop = self.metrics.rtprop
            avg_size = self.metrics.avg_file_size

            # Format helper for bytes/sec
            def fmt_bps(bps: float) -> str:
                if bps >= 1_000_000:
                    return f"{bps / 1_000_000:.2f} MB/s"
                elif bps >= 1_000:
                    return f"{bps / 1_000:.1f} KB/s"
                else:
                    return f"{bps:.0f} B/s"

            # Goodput: actual measured throughput
            if goodput_rps is not None:
                goodput_rps_str = f"{goodput_rps:.1f} req/s"
                if avg_size is not None:
                    goodput_bps = goodput_rps * avg_size
                    goodput_bps_str = fmt_bps(goodput_bps)
                else:
                    goodput_bps_str = "N/A"
            else:
                goodput_rps_str = "N/A"
                goodput_bps_str = "N/A"

            # Inferred rate: theoretical rate from Little's Law (R = C / RTprop)
            if rtprop is not None and rtprop > 0:
                inferred_rps = new_C / rtprop
                inferred_rps_str = f"{inferred_rps:.1f} req/s"
                if avg_size is not None:
                    inferred_bps = inferred_rps * avg_size
                    inferred_bps_str = fmt_bps(inferred_bps)
                else:
                    inferred_bps_str = "N/A"
            else:
                inferred_rps_str = "N/A"
                inferred_bps_str = "N/A"

            print(f"[PAARC]   Goodput(measured): {goodput_rps_str} ({goodput_bps_str}) | "
                  f"InferredRate(C/RTprop): {inferred_rps_str} ({inferred_bps_str})")
    
    def _update_smoother(self) -> None:
        """Update or initialize the leaky bucket smoother."""
        rtprop = self._get_rtprop()
        if self.smoother is None:
            self.smoother = LeakyBucketSmoother(self._concurrency, rtprop)
        else:
            self.smoother.update(self._concurrency, rtprop)
    
    def _is_latency_degraded(self, snap: dict) -> bool:
        """Check if latency has degraded significantly from baseline."""
        rtprop = self.metrics.rtprop
        if rtprop is None:
            return False

        p50 = snap.get("p50")
        p95 = snap.get("p95")

        p50_threshold = rtprop * self.config.theta_50
        p95_threshold = rtprop * self.config.theta_95

        # Check p50 degradation
        p50_degraded = p50 is not None and p50 > p50_threshold

        # Check p95 degradation (tail latency)
        p95_degraded = p95 is not None and p95 > p95_threshold

        degraded = p50_degraded or p95_degraded

        # Log only on state transition (not-degraded → degraded) to reduce spam
        if degraded and not self._was_latency_degraded:
            p50_str = f"{p50:.3f}" if p50 is not None else "None"
            p95_str = f"{p95:.3f}" if p95 is not None else "None"
            print(f"[PAARC] {self.host}: PROBE_BW latency degraded | "
                  f"rtprop={rtprop:.3f}s | "
                  f"p50={p50_str}s (thresh={p50_threshold:.3f}s) | "
                  f"p95={p95_str}s (thresh={p95_threshold:.3f}s)")

        self._was_latency_degraded = degraded
        return degraded

    def _is_latency_plateau(self, snap: dict) -> bool:
        """
        BBR-style plateau detection: latency exceeds RTprop × threshold.

        Detects server saturation when queue builds up, causing latency increase.
        Uses looser thresholds than PROBE_BW since STARTUP wants to find ceiling.
        """
        rtprop = self.metrics.rtprop
        if rtprop is None:
            return False

        p50 = snap.get("p50")
        p95 = snap.get("p95")

        p50_threshold = rtprop * self.config.startup_theta_50
        p95_threshold = rtprop * self.config.startup_theta_95

        p50_degraded = p50 is not None and p50 > p50_threshold
        p95_degraded = p95 is not None and p95 > p95_threshold

        degraded = p50_degraded or p95_degraded

        # Log when degradation detected
        if degraded:
            p50_str = f"{p50:.3f}" if p50 is not None else "None"
            p95_str = f"{p95:.3f}" if p95 is not None else "None"
            print(f"[PAARC] {self.host}: STARTUP latency degraded | "
                  f"rtprop={rtprop:.3f}s | "
                  f"p50={p50_str}s (thresh={p50_threshold:.3f}s) | "
                  f"p95={p95_str}s (thresh={p95_threshold:.3f}s)")

        return degraded

    def _is_derivative_plateau(self, snap: dict) -> bool:
        """
        Efficiency-based plateau detection: goodput gain per +1 C drops below threshold.

        Measures diminishing returns: when adding concurrency no longer helps throughput.
        Uses configurable window and threshold.
        """
        history = self.metrics.goodput_history
        window = self.config.efficiency_window

        if len(history) < window + 1:
            return False

        current = history[-1]
        past = history[-(window + 1)]

        if past <= 0:
            return False

        # Actual efficiency: goodput gained per +1 concurrency
        actual_efficiency = (current - past) / window

        # Expected efficiency if scaling linearly: past_goodput / past_C
        past_C = self._concurrency - window
        if past_C <= 0:
            return False
        expected_efficiency = past / past_C

        # Plateau if actual < threshold × expected
        return actual_efficiency < expected_efficiency * self.config.efficiency_threshold

    def _calculate_cooldown(self, retry_after: Optional[float]) -> float:
        """Calculate cooldown duration: max(5 × RTprop, 2s, retry_after)."""
        rtprop = self._get_rtprop()
        rtprop_based = self.config.cooldown_rtprop_mult * rtprop
        
        candidates = [rtprop_based, self.config.cooldown_floor]
        if retry_after is not None and retry_after > 0:
            candidates.append(retry_after)
        
        return max(candidates)
    
    def _calculate_control_interval(self, snap: dict) -> float:
        """
        Calculate the next control interval duration.

        The live scheduler keeps the existing state-aware cadence:
        - STARTUP/BACKOFF poll faster using 4 × RTprop with a 100ms floor
        - INIT/PROBE_BW/PROBE_RTT use k_interval × RTprop with T_floor

        On top of that, the sample-based term ensures we wait long enough to
        collect roughly N_min successful completions before trusting interval
        percentiles.
        """
        rtprop = self._get_rtprop()
        goodput_rps = snap.get("goodput_rps", 0)

        if self.state in (PAARCState.STARTUP, PAARCState.BACKOFF):
            time_multiplier = 4
            floor = 0.1
        else:
            time_multiplier = self.config.k_interval
            floor = self.config.T_floor

        # Time-based: state-aware RTprop multiplier.
        time_based = time_multiplier * rtprop

        # Sample-based: long enough to collect approximately N_min samples.
        if goodput_rps > 0:
            sample_based = self.config.N_min / goodput_rps
        else:
            sample_based = floor

        return max(time_based, sample_based, floor)

    async def step_interval(self) -> dict[str, Any]:
        """
        Execute one control interval step.

        Called periodically by the controller loop.
        """
        now = _monotonic()

        # Determine if RTprop should be fully updated based on current state
        # - INIT: Always allow (discovering initial RTprop)
        # - PROBE_RTT: Only after first interval (_restoring=True) to skip contaminated samples
        # - Other phases: Only accept new minimums to avoid pollution
        if self.state == PAARCState.INIT:
            allow_rtprop_update = True
        elif self.state == PAARCState.PROBE_RTT:
            # Skip first interval - samples are contaminated from high-concurrency period
            # _restoring is False on first interval, True on subsequent intervals
            allow_rtprop_update = self._restoring
        else:
            allow_rtprop_update = False

        # Get interval snapshot
        snap = await self.metrics.finish_interval(allow_rtprop_update=allow_rtprop_update)
        
        # Update samples since PROBE_RTT
        self._samples_since_probe_rtt += snap.get("n_samples", 0)
        
        # Skip if no activity
        if snap["total"] == 0:
            self._last_interval_time = now
            return snap
        
        # Print status for debugging
        state_str = self.state.name
        C = self._concurrency
        rtprop = self.metrics.rtprop
        rtprop_ms = rtprop * 1000 if rtprop else 0
        goodput = snap.get("goodput_rps", 0)
        n_errors = snap.get("n_errors", 0)
        
        if n_errors > 0:
            print(f"[PAARC] {self.host}: {state_str} | C={C} | RTprop={rtprop_ms:.0f}ms | "
                  f"Goodput={goodput:.1f}/s | Errors={n_errors}")
        
        # Dispatch to state handler
        if self.state == PAARCState.INIT:
            await self._step_init(snap, now)
        elif self.state == PAARCState.STARTUP:
            await self._step_startup(snap, now)
        elif self.state == PAARCState.PROBE_BW:
            await self._step_probe_bw(snap, now)
        elif self.state == PAARCState.PROBE_RTT:
            await self._step_probe_rtt(snap, now)
        elif self.state == PAARCState.BACKOFF:
            await self._step_backoff(snap, now)
        
        self._last_interval_time = now
        return snap
    
    async def _step_init(self, snap: dict, now: float) -> None:
        """
        INIT phase: Establish RTprop baseline.
        
        - Collect N_init samples at C_init concurrency
        - Set RTprop = p10 of samples
        - Calculate avg_file_size
        - Transition to STARTUP
        """
        self._init_samples += snap.get("n_samples", 0)
        
        # Check for overload during INIT
        if snap.get("has_overload"):
            self._C_ceiling = self.config.C_init
            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown
            self._last_overload_reduction_time = now
            self.state = PAARCState.BACKOFF
            print(f"[PAARC] {self.host}: INIT→BACKOFF on error")
            return
        
        # Wait for sufficient samples
        if self._init_samples >= self.config.N_init:
            rtprop = self.metrics.rtprop
            avg_size = self.metrics.avg_file_size
            
            rtprop_ms = rtprop * 1000 if rtprop else 0
            avg_size_kb = avg_size / 1024 if avg_size else 0
            
            print(f"[PAARC] {self.host}: INIT complete | RTprop={rtprop_ms:.0f}ms | "
                  f"AvgSize={avg_size_kb:.1f}KB")

            # Initialize smoother now that we have RTprop
            self._update_smoother()

            # Reset goodput history so plateau detection only considers STARTUP phase
            self.metrics.reset_goodput_history()

            # Transition to STARTUP
            self.state = PAARCState.STARTUP
            print(f"[PAARC] {self.host}: INIT→STARTUP")
    
    async def _step_startup(self, snap: dict, now: float) -> None:
        """
        STARTUP phase: Discover maximum sustainable concurrency.

        - Additive growth: +1 each interval
        - Exit on: overload, plateau (latency or derivative), or C_max
        - Set C_ceiling on exit

        Plateau detection mode controlled by PLATEAU_DETECTION_MODE:
        - "latency": BBR-style, p50/p95 > RTprop × threshold
        - "derivative": Efficiency-based, goodput gain per +1 C drops
        """
        # Check for overload → BACKOFF
        if snap.get("has_overload"):
            # Set ceiling at reduced level
            self._C_ceiling = max(
                self.config.C_min,
                int(self._concurrency * self.config.beta)
            )
            self._C_operating = int(self._C_ceiling * self.config.mu)
            self._set_concurrency(self._C_operating, "startup_overload")

            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown
            self._last_overload_reduction_time = now
            self.state = PAARCState.BACKOFF

            print(f"[PAARC] {self.host}: STARTUP→BACKOFF | C_ceiling={self._C_ceiling}")
            return

        # Check for plateau → PROBE_BW (method depends on PLATEAU_DETECTION_MODE)
        is_plateau = False
        if PLATEAU_DETECTION_MODE == "latency":
            is_plateau = self._is_latency_plateau(snap)
        elif PLATEAU_DETECTION_MODE == "derivative":
            is_plateau = self._is_derivative_plateau(snap)

        if is_plateau:
            self._C_ceiling = self._concurrency
            self._C_operating = int(self._C_ceiling * self.config.mu)
            self._set_concurrency(self._C_operating, "startup_plateau")

            self._last_probe_rtt_time = now
            self._samples_since_probe_rtt = 0
            self.state = PAARCState.PROBE_BW

            print(f"[PAARC] {self.host}: STARTUP→PROBE_BW (plateau/{PLATEAU_DETECTION_MODE}) | C_ceiling={self._C_ceiling}")
            return

        # Check for C_max reached → PROBE_BW
        if self._concurrency >= self.config.C_max:
            self._C_ceiling = self.config.C_max
            self._C_operating = int(self._C_ceiling * self.config.mu)
            self._set_concurrency(self._C_operating, "startup_max")

            self._last_probe_rtt_time = now
            self._samples_since_probe_rtt = 0
            self.state = PAARCState.PROBE_BW

            print(f"[PAARC] {self.host}: STARTUP→PROBE_BW (max) | C_ceiling={self._C_ceiling}")
            return

        # Additive growth per interval
        new_C = self._concurrency + self.config.startup_additive_increase
        new_C = min(new_C, self.config.C_max)
        self._set_concurrency(new_C, "startup_grow")
    
    async def _step_probe_bw(self, snap: dict, now: float) -> None:
        """
        PROBE_BW phase: Steady-state operation with conservative probing.
        
        - Operate at C_operating = C_ceiling × μ
        - Additive increase after stable intervals
        - Check for ceiling revision on underperformance
        - Trigger PROBE_RTT periodically
        """
        # Check for overload → BACKOFF
        if snap.get("has_overload"):
            # Revise ceiling downward
            if self._C_ceiling is not None:
                self._C_ceiling = max(
                    self.config.C_min,
                    int(self._C_ceiling * self.config.beta)
                )
                self._C_operating = int(self._C_ceiling * self.config.mu)
            else:
                self._C_operating = max(
                    self.config.C_min,
                    int(self._concurrency * self.config.beta)
                )
            
            self._set_concurrency(self._C_operating, "probe_bw_overload")

            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown
            self._last_overload_reduction_time = now
            self.state = PAARCState.BACKOFF

            print(f"[PAARC] {self.host}: PROBE_BW→BACKOFF | C_ceiling={self._C_ceiling}")
            return
        
        # Check for PROBE_RTT trigger
        time_since_probe = now - self._last_probe_rtt_time
        if (time_since_probe > self.config.probe_rtt_period and
            self._samples_since_probe_rtt > self.config.probe_rtt_min_samples):
            
            # Save current concurrency for restoration
            self._saved_concurrency = self._concurrency
            
            # Reduce to 50% of current (BBRv2/v3 style)
            probe_C = max(
                self.config.C_min,
                int(self._concurrency * self.config.probe_rtt_concurrency_factor)
            )
            self._set_concurrency(probe_C, "probe_rtt_enter")
            
            self.state = PAARCState.PROBE_RTT
            print(f"[PAARC] {self.host}: PROBE_BW→PROBE_RTT | C={probe_C}")
            return
        
        # Check for latency degradation
        if self._is_latency_degraded(snap):
            self._stable_intervals = 0
            return

        # If operating above ceiling and stable, revise ceiling upward
        if self._C_ceiling is not None and self._concurrency > self._C_ceiling:
            old_ceiling = self._C_ceiling
            self._C_ceiling = self._concurrency
            self._C_operating = int(self._C_ceiling * self.config.mu)
            #print(f"[PAARC] {self.host}: Ceiling revised {old_ceiling}→{self._C_ceiling} (probe success)")

        # Check for ceiling revision downward (sustained underperformance)
        self._check_ceiling_revision(snap)

        # Additive increase after stable intervals
        self._stable_intervals += 1

        if self._stable_intervals >= self.config.stable_intervals_required:
            # Can probe up to C_max (ceiling is soft, C_max is hard limit)
            if self._concurrency < self.config.C_max:
                new_C = min(self._concurrency + self.config.probe_bw_additive_increase, self.config.C_max)
                self._set_concurrency(new_C, "probe_bw_increase")
                self._stable_intervals = 0
    
    def _check_ceiling_revision(self, snap: dict) -> None:
        """
        Check for and apply ceiling revision on sustained underperformance.

        If goodput < expected × 0.70 for 5 consecutive intervals,
        multiplicatively reduce ceiling.
        """
        if self._C_ceiling is None:
            return

        actual_goodput = snap.get("goodput_rps", 0)

        # Don't revise ceiling if goodput is suspiciously low (<1 req/s)
        # This likely indicates a client-side bottleneck, not server issue
        if actual_goodput < 1.0:
            return

        rtprop = self._get_rtprop()
        expected_goodput = self._C_operating / rtprop

        if actual_goodput < expected_goodput * self.config.underperformance_threshold:
            self._underperformance_intervals += 1
            
            if self._underperformance_intervals >= self.config.underperformance_intervals:
                # Multiplicative ceiling revision (ensure at least -1 decrease)
                old_ceiling = self._C_ceiling
                revised = min(
                    int(self._C_ceiling * self.config.revision_factor),
                    self._C_ceiling - 1  # Guarantee at least -1
                )
                self._C_ceiling = max(self.config.C_min, revised)
                self._C_operating = int(self._C_ceiling * self.config.mu)
                self._set_concurrency(self._C_operating, "ceiling_revision")
                self._underperformance_intervals = 0
                
                #print(f"[PAARC] {self.host}: Ceiling revised {old_ceiling}→{self._C_ceiling}")
        else:
            self._underperformance_intervals = 0
    
    async def _step_probe_rtt(self, snap: dict, now: float) -> None:
        """
        PROBE_RTT phase: Refresh RTprop estimate.
        
        - Operate at reduced concurrency (50% of normal)
        - Duration: max(3 × RTprop, 500ms)
        - Gradual restoration back to operating point
        """
        # Check for overload → BACKOFF
        if snap.get("has_overload"):
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
        
        # Check if we should start restoration or continue
        if not self._restoring:
            # First interval in PROBE_RTT - just collect RTprop samples
            # Next interval will start restoration
            self._restoring = True
            return
        
        # Gradual restoration
        await self._restore_concurrency(now)
    
    async def _restore_concurrency(self, now: float) -> None:
        """
        Gradually restore concurrency after PROBE_RTT.
        
        Restores over n_restore steps, each separated by RTprop.
        """
        if self._saved_concurrency is None:
            self._saved_concurrency = self._C_operating
        
        target = self._saved_concurrency
        current = self._concurrency
        
        if current >= target:
            # Restoration complete
            self._last_probe_rtt_time = now
            self._samples_since_probe_rtt = 0
            self._restoring = False
            self._saved_concurrency = None
            self.state = PAARCState.PROBE_BW
            
            print(f"[PAARC] {self.host}: PROBE_RTT→PROBE_BW (restored)")
            return
        
        # Calculate step size - use 2 steps for faster restoration
        # (Previously used n_restore=5 which took 14+ seconds)
        step = max(1, (target - current) // 2)
        new_C = min(current + step, target)
        self._set_concurrency(new_C, "probe_rtt_restore")
    
    async def _step_backoff(self, snap: dict, now: float) -> None:
        """
        BACKOFF phase: Recover from overload.

        - Wait for cooldown period
        - Further reduce on continued errors (with RTprop-based cooldown)
        - Transition to PROBE_BW after recovery
        """
        # Calculate overload check cooldown: max(floor, RTprop × multiplier)
        # This prevents over-reduction due to error bursts from stale high-C requests
        rtprop = self._get_rtprop()
        overload_check_cooldown = max(
            self.config.overload_check_floor,
            rtprop * self.config.overload_check_rtprop_mult
        )
        time_since_last_reduction = now - self._last_overload_reduction_time

        # Check for continued overload (only if cooldown has passed)
        if snap.get("has_overload") and time_since_last_reduction >= overload_check_cooldown:
            # Further multiplicative decrease
            new_C = max(
                self.config.C_min,
                int(self._concurrency * self.config.beta)
            )
            self._set_concurrency(new_C, "backoff_continued")

            # Revise ceiling
            if self._C_ceiling is not None:
                self._C_ceiling = max(
                    self.config.C_min,
                    int(self._C_ceiling * self.config.beta)
                )

            # Extend cooldown
            cooldown = self._calculate_cooldown(snap.get("retry_after"))
            self._cooldown_until = now + cooldown

            # Track this reduction for cooldown
            self._last_overload_reduction_time = now

            print(f"[PAARC] {self.host}: BACKOFF continued | C={new_C} | "
                  f"overload_cooldown={overload_check_cooldown:.1f}s")
            return
        
        # Check if cooldown has expired
        if now >= self._cooldown_until:
            # Recover to operating point
            if self._C_ceiling is not None:
                self._C_operating = int(self._C_ceiling * self.config.mu)
            self._set_concurrency(self._C_operating, "backoff_recover")
            
            self._last_probe_rtt_time = now
            self._samples_since_probe_rtt = 0
            self.state = PAARCState.PROBE_BW
            
            print(f"[PAARC] {self.host}: BACKOFF→PROBE_BW | C={self._C_operating}")


# =============================================================================
# PER-HOST CONTROLLER MANAGER
# =============================================================================

class HostControllerManager:
    """
    Manages PAARC controllers for multiple hosts.
    
    Creates and retrieves per-host controllers lazily.
    Runs the periodic control loop.
    """
    
    def __init__(self, config: PAARCConfig):
        self.config = config
        self._lock = asyncio.Lock()
        self._controllers: dict[str, PAARCController] = {}
    
    async def get_controller(self, url: str) -> PAARCController:
        """Get or create controller for URL's host."""
        host = urlsplit(url).netloc.lower() or "unknown"
        
        async with self._lock:
            if host not in self._controllers:
                self._controllers[host] = PAARCController(host, self.config)
                print(f"[PAARC] Created controller for {host}")
            return self._controllers[host]
    
    async def all_controllers(self) -> list[PAARCController]:
        """Get all active controllers."""
        async with self._lock:
            return list(self._controllers.values())


# async def controller_loop(manager: HostControllerManager) -> None:
#     """
#     Periodic control loop for all host controllers.
    
#     Runs step_interval for each controller based on their individual
#     calculated interval durations.
#     """
#     print("[PAARC] Controller loop started")
    
#     # Use a base interval for checking; individual controllers have their own timing
#     base_interval = 0.2  # 200ms base check interval
    
#     while not shutdown_flag:
#         await asyncio.sleep(base_interval)
        
#         if shutdown_flag:
#             break
        
#         controllers = await manager.all_controllers()
        
#         for ctrl in controllers:
#             try:
#                 await ctrl.step_interval()
#             except Exception as e:
#                 print(f"[PAARC] Error in controller for {ctrl.host}: {e}")

async def controller_loop(manager: HostControllerManager) -> None:
    """
    BBR-consistent control loop with per-host adaptive timing.
    """
    host_next_step: dict[str, float] = {}  # host -> next step time
    
    while not shutdown_flag:
        now = time.monotonic()
        controllers = await manager.all_controllers()
        
        for ctrl in controllers:
            host = ctrl.host
            next_time = host_next_step.get(host, 0)
            
            if now >= next_time:
                try:
                    snap = await ctrl.step_interval()

                    # Use the controller's state-aware, sample-aware scheduling helper.
                    interval = ctrl._calculate_control_interval(snap)

                    host_next_step[host] = now + interval
                    
                except Exception as e:
                    if hasattr(manager, "failure"):
                        manager.failure = e
                        return  # New methods fail closed at subsequent HTTP dispatch.
                    print(f"[PAARC] Error for {host}: {e}")
                    host_next_step[host] = now + 0.2  # Fallback
        
        # Sleep until next host needs attention (or minimum check interval)
        if host_next_step:
            next_wakeup = min(host_next_step.values())
            sleep_time = max(0.01, next_wakeup - time.monotonic())
        else:
            sleep_time = 0.2
        
        await asyncio.sleep(sleep_time)

# =============================================================================
# FILENAME ALLOCATION
# =============================================================================

class SequentialNamer:
    """Stable sequential filename generator."""
    
    def __init__(self):
        self._lock = asyncio.Lock()
        self._counter = 0
        self._map: dict[str, str] = {}
    
    async def filename_for(self, key: str, url: str) -> str:
        """Get filename for a key, creating if needed."""
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
    """Render filename from pattern and URL.

    Available template variables:
        segment: List of URL path segments (non-empty parts)
        segment_last: Last segment (usually the filename)
        segment_parent: Second-to-last segment (often an ID)
        segment_first: First segment
        ext: File extension from URL (e.g., '.jpg')
        key: Sanitized row key/identifier

    Example patterns:
        '{segment_parent}' - Use parent directory name (e.g., photo ID)
        '{segment_last}' - Use original filename
        '{key}_{segment_parent}' - Combine key with parent segment
    """
    seg = [s for s in urlsplit(url).path.split("/") if s]
    ext = Path(urlsplit(url).path).suffix or ""
    safe_key = _sanitize_filename(key)

    # Pre-compute common segment accessors for template use
    # (Python's str.format() doesn't support negative indexing like {segment[-2]})
    env = {
        "segment": seg,
        "ext": ext,
        "key": safe_key,
        # Convenience accessors (handle empty lists gracefully)
        "segment_last": seg[-1] if seg else "",
        "segment_parent": seg[-2] if len(seg) >= 2 else (seg[-1] if seg else ""),
        "segment_first": seg[0] if seg else "",
    }

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


# =============================================================================
# DOWNLOAD EXECUTION
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


async def download_one(
    *,
    row: dict,
    cfg: Config,
    session: aiohttp.ClientSession,
    total_bytes: list[int],
    manager: Optional[HostControllerManager],
    sequential_namer: SequentialNamer,
    global_written_paths: dict[str, str],
    store=None,
    shared=None,
) -> DownloadOutcome:
    """Execute a single download with PAARC control."""
    url = str(row[cfg.url_col]).strip()
    key = str(row.get("__key__", ""))
    class_name = str(row[cfg.label_col]) if cfg.label_col is not None and row.get(cfg.label_col) is not None else None
    
    try:
        directory = store.begin(key) if store is not None else None
    except (OSError, ValueError) as exc:
        # Only a durable row rejection can become an ordinary failed outcome.
        # A journal/storage failure must abort this invocation before HTTP; if
        # swallowed here it can leave an unchanged budget and retry forever.
        if store is None or not store.fs.exists(f".flowdc/rejections/{key}.json"):
            raise
        return DownloadOutcome(key, url, False, None, class_name, None, str(exc))
    # Determine filename
    if store is not None:
        filename_override = Path(store.rows[key]["payload"]).name
    elif cfg.naming_mode == "sequential":
        filename_override = await sequential_namer.filename_for(key, url)
    else:
        filename_override = render_filename(cfg.file_name_pattern, url, key)
    
    # Get host controller if PAARC enabled
    ctrl: Optional[PAARCController] = None
    if manager is not None:
        ctrl = await manager.get_controller(url)
    
    # Acquire concurrency slot
    if ctrl is not None:
        await ctrl.semaphore.acquire()
    
    # Set up tracing context
    trace_dict: dict[str, Any] = {}
    remote = shared.attempt(key) if shared is not None else None
    if remote is not None:
        trace_dict["shared_dispatch"] = remote.dispatch
        trace_dict["shared_headers"] = remote.headers
        async def release_remote_redirect():
            await remote.finish_hop(trace_dict, "redirect")
        trace_dict["shared_redirect_release"] = release_remote_redirect
    if manager is not None and hasattr(manager, "check_health"):
        trace_dict["dispatch_check"] = manager.check_health
    if manager is not None:
        async def redirect_admit(destination: str) -> None:
            nonlocal ctrl
            next_ctrl = await manager.get_controller(destination)
            if next_ctrl is not ctrl:
                # Release before acquiring: reciprocal A->B / B->A redirects
                # must not hold both authorities' permits and deadlock.
                if ctrl is not None:
                    await ctrl.semaphore.release()
                    ctrl = None
                await next_ctrl.semaphore.acquire()
                ctrl = next_ctrl
            if ctrl.smoother is not None:
                await ctrl.smoother.acquire()

        trace_dict["redirect_admit"] = redirect_admit
    trace_token = TRACE_CTX.set(trace_dict)
    output_token = OUTPUT_CTX.set((store, directory, key) if store is not None else None)
    
    try:
        # Apply smoothing if available
        if ctrl is not None and ctrl.smoother is not None:
            await ctrl.smoother.acquire()
        
        # Execute download
        k, file_path, cls, err, status, retry_after_sec = await download_single(
            url=url,
            key=key,
            class_name=class_name,
            output_folder=cfg.output_folder,
            output_format=cfg.output_format,
            session=session,
            timeout=cfg.timeout_sec,
            filename=filename_override,
            # Do not credit the shared tally until the final lookup succeeds.
            total_bytes=[],
        )
        
        # A missing output or failed size lookup is a local failed acquisition,
        # not a successful zero-byte output. Preserve the completed HTTP timing.
        bytes_dl = 0
        if err is None:
            try:
                if not file_path:
                    raise OSError("No saved output path")
                if store is None:
                    bytes_dl = os.path.getsize(file_path)
                else:
                    record = store.fs.json(f".flowdc/commits/{key}.json")
                    bytes_dl = record["payload_bytes"]
            except Exception as exc:
                err = f"Output size verification failed: {exc}"
                trace_dict["failure_kind"] = "local"
            else:
                total_bytes.append(bytes_dl)
        
        if store is not None and err is not None:
            store.fail(directory, key, error=err, status=status,
                       retryable=_is_retryable(status, err) and trace_dict.get("failure_kind") not in ("local", "unknown"),
                       observed_bytes=trace_dict.get("observed_response_body_bytes", 0),
                       latency_eligible=trace_dict.get("latency_eligible", False),
                       body_complete=trace_dict.get("body_completed_at") is not None)
        # Record metrics
        failure_kind = trace_dict.get("failure_kind")
        is_unknown_error = failure_kind == "unknown"
        is_conn_error = failure_kind == "transport" or (
            failure_kind is None and _is_connection_error(err)
        )
        is_local_error = failure_kind in ("local", "admission") or (
            err is not None and not is_conn_error and not is_unknown_error and status in (None, 200)
        )
        if manager is not None:
            # Each redirected hop acquires its destination's permit. Final
            # response feedback belongs to the controller actually holding it.
            # A normalized trace URL (default port / IDNA) may otherwise select
            # another legacy host key even without a redirect. Only fall back to
            # the target lookup when admission failed before acquiring a permit.
            feedback_ctrl = ctrl if ctrl is not None else await manager.get_controller(
                trace_dict.get("feedback_url", url)
            )
            await feedback_ctrl.metrics.record(
                status_code=status,
                ttfb=trace_dict.get("ttfb"),
                bytes_downloaded=bytes_dl,
                is_conn_error=is_conn_error,
                retry_after_sec=retry_after_sec,
                acquisition_success=(err is None),
                is_local_error=is_local_error,
                is_unknown_error=is_unknown_error,
                latency_eligible=trace_dict.get("latency_eligible"),
                dispatch_at=trace_dict.get("t0"),
            )
        
        if remote is not None:
            await remote.finish_hop(trace_dict, "final")

        # Track written paths for collision detection
        if err is None and file_path:
            prev = global_written_paths.get(file_path)
            if prev is not None and prev != key:
                print(f"[Warning] Filename collision: {file_path}")
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
        OUTPUT_CTX.reset(output_token)
        TRACE_CTX.reset(trace_token)
        if ctrl is not None:
            await ctrl.semaphore.release()
        if remote is not None:
            await remote.close(trace_dict)


async def download_batch_bounded(
    *,
    cfg: Config,
    session: aiohttp.ClientSession,
    df: pl.DataFrame,
    manager: Optional[HostControllerManager],
    sequential_namer: SequentialNamer,
    global_written_paths: dict[str, str],
    effective_workers: int,
    store=None,
    shared=None,
) -> dict[str, DownloadOutcome]:
    """
    Bounded batch download scheduler.

    Uses a queue with N worker tasks for bounded parallelism.

    Args:
        effective_workers: Number of worker coroutines (auto-detected or configured)
    """
    integrity.require("__key__" in df.columns and df["__key__"].null_count() == 0
                      and df["__key__"].n_unique() == df.height, "duplicate or missing internal row identity")
    q: asyncio.Queue[dict] = asyncio.Queue()
    for row in df.iter_rows(named=True):
        q.put_nowait(row)

    total_bytes: list[int] = []
    outcomes: dict[str, DownloadOutcome] = {}
    pbar = tqdm(total=df.height, desc="Downloading", unit="url")
    
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
                manager=manager,
                sequential_namer=sequential_namer,
                global_written_paths=global_written_paths,
                store=store,
                shared=shared,
            )
            
            outcomes[out.key] = out
            q.task_done()
            pbar.update(1)
    
    workers = [
        asyncio.create_task(worker())
        for _ in range(max(1, effective_workers))
    ]
    
    async def cancel_on_shutdown():
        while not shutdown_flag:
            await asyncio.sleep(0.1)
        for task in workers:
            task.cancel()

    shutdown_task = asyncio.create_task(cancel_on_shutdown())
    try:
        await asyncio.gather(*workers)
    except asyncio.CancelledError:
        if not shutdown_flag:
            raise
    finally:
        shutdown_task.cancel()
        for task in workers:
            task.cancel()
        await asyncio.gather(shutdown_task, *workers, return_exceptions=True)
        pbar.close()
    
    return outcomes


# =============================================================================
# TAR AND OVERVIEW
# =============================================================================

def create_tar(output_folder: str, compress: bool = True) -> str:
    """Create tar archive of output folder.

    Args:
        output_folder: Path to the folder to archive
        compress: If True, create .tar.gz (gzip compressed). If False, create .tar (uncompressed).

    Returns:
        Path to the created tar archive
    """
    out = Path(output_folder)

    if (out / ".flowdc").exists() or (out / ".flowdc").is_symlink():
        # Compatibility helper: a managed directory must use committed records,
        # never a recursive walk that includes private staging or unknown files.
        with integrity.RunStore(out) as store:
            if store.fs.exists(".flowdc/final.json"):
                final = store.fs.json(".flowdc/final.json")
                integrity.require("completion_boundary" not in final,
                                  "create_tar cannot modify a finalized managed run; use --reconcile")
            snapshot = store.reconcile()
            report = store.fs.json("overview.json") if store.fs.exists("overview.json") else {}
            return store.make_archive(snapshot, report, compress=compress)[0]

    if compress:
        suffix = ".tar.gz"
        mode = "w:gz"
    else:
        suffix = ".tar"
        mode = "w"

    tar_path = out.with_suffix(out.suffix + suffix) if out.suffix else Path(str(out) + suffix)

    if not out.exists():
        raise FileNotFoundError(f"Output folder not found: {out}")

    with tarfile.open(str(tar_path), mode) as tf:
        tf.add(str(out), arcname=out.name)

    return str(tar_path.resolve())


def generate_overview_report(
    *,
    cfg: Config,
    df_total: int,
    outcomes: dict[str, DownloadOutcome],
    elapsed_sec: float,
    tar_path: Optional[str] = None,
) -> dict:
    """
    Generate overview report data.

    Args:
        cfg: Configuration object
        df_total: Total number of URLs in input
        outcomes: Download outcomes dict
        elapsed_sec: Total elapsed time
        tar_path: Path to tar archive (optional, can be filled in later)

    Returns:
        Report dictionary
    """
    successes = [o for o in outcomes.values() if o.success]
    failures = [o for o in outcomes.values() if not o.success]
    err_counter = Counter((o.status_code, o.error) for o in failures)

    total_bytes = sum(o.bytes_downloaded for o in successes)
    mb = total_bytes / 1e6
    speed_MBps = (mb / elapsed_sec) if elapsed_sec > 0 else 0.0

    return {
        "paarc_version": "2.0.0",
        "http_measurement": {
            "version": HTTP_MEASUREMENT_VERSION,
            "clock": "time.monotonic",
            "controller_latency": "final-hop dispatch to first nonempty application body read",
            "latency_eligibility": "HTTP 200, nonempty complete body, positive finite timing; independent of local output success",
            "success": "HTTP 200 and saved output with successful size lookup; bytes count useful saved payload only",
            "n_errors": "overload subset, not all unsuccessful acquisitions",
            "n_unknown_failures": "unexpected acquisition exceptions; cause and overload not inferred",
            "retry_after_scope": "session-local hostname/effective-port authority, all modes and redirects",
            "redirect_control": "destination adaptive permit; 3xx follow-up delay without destination-wide embargo",
            "attempt_timeout": "admission, connection, redirects and body; excludes initial adaptive permit/smoothing",
        },
        "script_inputs": {
            "input": cfg.input_path,
            "input_format": cfg.input_format,
            "output_folder": cfg.output_folder,
            "output_format": cfg.output_format,
            "url_column": cfg.url_col,
            "label_column": cfg.label_col,
            "concurrent_downloads": cfg.concurrent_downloads,
            "timeout_sec": cfg.timeout_sec,
            "enable_paarc": cfg.enable_paarc,
            "control_method": cfg.control_method,
            "method_options": cfg.method_options,
            "paarc_config": {
                "C_init": cfg.C_init,
                "C_min": cfg.C_min,
                "C_max": cfg.C_max,
                "mu": cfg.mu,
                "beta": cfg.beta,
                "theta_50": cfg.theta_50,
                "theta_95": cfg.theta_95,
                "startup_theta_50": cfg.startup_theta_50,
                "startup_theta_95": cfg.startup_theta_95,
                "startup_additive_increase": cfg.startup_additive_increase,
                "probe_bw_additive_increase": cfg.probe_bw_additive_increase,
                "efficiency_threshold": cfg.efficiency_threshold,
                "efficiency_window": cfg.efficiency_window,
                "probe_rtt_period": cfg.probe_rtt_period,
                "rtprop_window": cfg.rtprop_window,
                "cooldown_floor": cfg.cooldown_floor,
                "alpha_ema": cfg.alpha_ema,
            },
            "max_retry_attempts": cfg.max_retry_attempts,
            "naming_mode": cfg.naming_mode,
        },
        "summary": {
            "total_urls": df_total,
            "successful_downloads": len(successes),
            "failed_downloads": len(failures),
            "unattempted_or_cancelled_urls": max(0, df_total - len(outcomes)),
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


def write_overview(
    *,
    cfg: Config,
    df_total: int,
    outcomes: dict[str, DownloadOutcome],
    elapsed_sec: float,
    tar_path: Optional[str] = None,
    report: Optional[dict] = None,
) -> str:
    """
    Write JSON overview report to external file.

    Args:
        cfg: Configuration object
        df_total: Total number of URLs in input
        outcomes: Download outcomes dict
        elapsed_sec: Total elapsed time
        tar_path: Path to tar archive (optional)
        report: Pre-generated report dict (optional, will generate if not provided)

    Returns:
        Path to the written overview file
    """
    if report is None:
        report = generate_overview_report(
            cfg=cfg,
            df_total=df_total,
            outcomes=outcomes,
            elapsed_sec=elapsed_sec,
            tar_path=tar_path,
        )
    else:
        # Update tar_path in existing report if provided
        if tar_path is not None:
            report["summary"]["tar_path"] = tar_path

    out = Path(cfg.output_folder)
    overview_path = out.with_name(out.name + "_overview.json")

    with overview_path.open("w") as f:
        json.dump(report, f, indent=2)

    return str(overview_path.resolve())


# =============================================================================
# MAIN
# =============================================================================

def integrity_report(cfg, store, snapshot, elapsed, report_factory):
    outcomes = {}
    for row in snapshot["rows"]:
        outcomes[row["row_id"]] = DownloadOutcome(
            row["row_id"], str(store.rows[row["row_id"]]["url"]), row["disposition"] == "verified",
            row["payload"], None, row.get("status_code"), row["error"], row["payload_bytes"],
        )
    report = report_factory(cfg, outcomes, elapsed)
    from flowdc_methods import control_records, method_record
    report["control_method"] = method_record(cfg)
    report["control_trajectories"] = control_records(store)
    counts = snapshot["counts"]
    report["report_schema_version"] = integrity.SCHEMA
    report["summary"].update(
        total_urls=snapshot["original_rows"], successful_downloads=counts["verified"], failed_downloads=counts["failed"],
        skipped_rows=counts["skipped"], unattempted_rows=counts["unattempted"],
        unattempted_or_cancelled_urls=counts["unattempted"],
        verified_payload_bytes=snapshot["verified_payload_bytes"], unique_content_bytes=snapshot["unique_content_bytes"],
        artifact_file_bytes=snapshot["artifact_file_bytes"], observed_response_body_bytes=snapshot["observed_response_body_bytes"],
        observed_bytes_complete=snapshot["observed_bytes_complete"],
    )
    boundary = "verified_uncompressed_archive" if cfg.research_profile else (
        "verified_archive" if cfg.output_format == "webdataset" and cfg.create_tar else "committed_local_files")
    report["output_integrity"] = {
        "schema_version": integrity.SCHEMA, "run_id": store.owner["run_id"], "manifest": snapshot["manifest"],
        "counts": counts, "completion_boundary": boundary, "run_complete": False,
        "useful_final_payload_bytes": 0, "outcome_index": "outcome-index.json",
        "outcome_index_sha256": integrity.digest(integrity.encode(snapshot)),
        "byte_semantics": "integer payload bytes; observed response bodies are decoded application bytes, not wire bytes",
        "artifact_file_bytes_scope": "committed payload and row-metadata files plus published archive, excluding reports and staging",
        "successful_downloads_semantics": "rows with verified committed local files",
        "control_trajectories_sha256": integrity.digest(integrity.encode(report["control_trajectories"])),
        "display_mb_divisor": 1000000,
    }
    if cfg.resume or cfg.reconcile:
        report["summary"].update(elapsed_sec=None, avg_speed_MBps=None)
        report["output_integrity"]["elapsed_scope"] = "not reconstructed across interruptions or offline reconciliation"
    return report


def finalize_run(cfg, store, snapshot, elapsed, report_factory):
    store.fs.atomic(".flowdc/final.json", {"run_complete": False, "run_id": store.owner["run_id"]}, replace=True)
    report = integrity_report(cfg, store, snapshot, elapsed, report_factory)
    store.emit("outcome-index.json", integrity.encode(snapshot))
    if cfg.create_overview:
        store.emit("overview.json", integrity.encode(report))
    tar_time, artifact_error = 0.0, None
    if cfg.create_tar and not shutdown_flag:
        try:
            started = _monotonic()
            path, size, sha256 = store.make_archive(snapshot, report, compress=cfg.compress_tar)
            tar_time = _monotonic() - started
            report["summary"]["tar_path"] = path
            report["summary"]["artifact_file_bytes"] += size
            report["output_integrity"]["archive"] = {"path": path, "bytes": size, "sha256": sha256, "verified": True}
        except (OSError, ValueError, tarfile.TarError) as exc:
            artifact_error = str(exc)
            report["output_integrity"]["artifact_error"] = artifact_error
    boundary_ok = report["output_integrity"]["completion_boundary"] == "committed_local_files" or "archive" in report["output_integrity"]
    if boundary_ok:
        report["output_integrity"]["useful_final_payload_bytes"] = snapshot["verified_payload_bytes"]
    complete = boundary_ok and not artifact_error and not shutdown_flag and not (
        snapshot["counts"]["failed"] or snapshot["counts"]["unattempted"])
    report["output_integrity"]["run_complete"] = complete
    final_record = dict(report["output_integrity"])
    if cfg.create_overview:
        # Two reports and the completion record cannot be published atomically.
        # Reports reference the expected last record; only that record confirms
        # final useful-byte credit. A crash/report failure cannot leave a report
        # falsely declaring a completed run before the last publication.
        report["output_integrity"].update(
            run_complete=None, useful_final_payload_bytes=None,
            completion_record=".flowdc/final.json",
            completion_record_sha256=integrity.digest(integrity.encode(final_record)),
        )
        store.emit("overview.json", integrity.encode(report))
        store.emit(store.root.name + "_overview.json", integrity.encode(report), external=True)
    store.fs.atomic(".flowdc/final.json", final_record, replace=True)
    report["output_integrity"] = final_record
    print(f"Download time:         {elapsed:.2f}s")
    print(f"Tar creation time:     {tar_time:.2f}s")
    print(f"Total time:            {elapsed + tar_time:.2f}s")
    print(f"Reconciled rows:        {snapshot['counts']}")
    print(f"Completion record:     {store.root / '.flowdc/final.json'}")
    if artifact_error:
        raise integrity.IntegrityError("Archive/report finalization failed: " + artifact_error)
    return report


async def run_acquisition(cfg, *, manager_factory=HostControllerManager, report_factory=None):
    """Shared acquisition/reconciliation runner for both controller variants."""
    cfg = normalize_config(cfg)
    if report_factory is None:
        report_factory = lambda config, outcomes, elapsed: generate_overview_report(
            cfg=config, df_total=len(outcomes), outcomes=outcomes, elapsed_sec=elapsed)
    if cfg.reconcile:
        # Offline mode needs only owned output. It neither loads input nor opens
        # an HTTP session; effective acquisition settings come from ownership.
        from dataclasses import fields, replace
        with integrity.RunStore(cfg.output_folder) as store:
            integrity.require(
                store.owner["config"]["implementation"] == ("gradient" if hasattr(cfg, "gradient_threshold") else "base"),
                "reconcile controller variant mismatch: use the original downloader entrypoint",
            )
            accepted = {f.name for f in fields(cfg)} - {"input_path", "output_folder", "force_overwrite", "resume", "reconcile"}
            cfg = replace(cfg, **{k: v for k, v in store.owner["config"]["values"].items() if k in accepted})
            if store.fs.exists("overview.json"):
                previous = store.fs.json("overview.json")
                if previous.get("output_integrity", {}).get("run_id") == store.owner["run_id"]:
                    original_factory = report_factory
                    def report_factory(config, outcomes, elapsed):
                        report = original_factory(config, outcomes, elapsed)
                        for field in ("controller_variant", "gradient_summary", "gradient_summary_scope"):
                            if field in previous:
                                report[field] = previous[field]
                        return report
            snapshot = store.reconcile()
            return finalize_run(cfg, store, snapshot, 0.0, report_factory)
    prepared = load_manifest(cfg)
    df, manifest = prepared
    rows = integrity.plan_rows(df, cfg, render_filename)
    if cfg.shared_control_file is not None:
        from flowdc_shared import descriptor
        descriptor(cfg)  # Validate credentials/binding before output mutation or HTTP.
    # Finish all pure validation and provenance checks before destructive consent.
    validate_and_load(cfg, prepared=prepared)
    config = integrity.effective_config(cfg)
    store = integrity.RunStore(cfg.output_folder, manifest=manifest, config=config,
                               rows=None if cfg.resume else rows)
    manager, ctrl_task, trajectory, shared = None, None, None, None
    started = _monotonic()
    try:
        integrity.require(store.owner["rows"] == rows, "resume row provenance or publication plan mismatch")
        snapshot = store.reconcile()
        keys = store.eligible(snapshot)
        if keys and not shutdown_flag:
            current_df = df.filter(pl.col("__key__").is_in(keys))
            effective_workers = resolve_worker_count(cfg, current_df)
            if cfg.shared_control_file is not None:
                from flowdc_methods import ControlTrace
                from flowdc_shared import SharedClient
                trajectory = ControlTrace(store, cfg)
                shared = SharedClient(cfg, trajectory.emit)
                await shared.start()
            elif cfg.enable_paarc:
                if cfg.control_method is not None:
                    from flowdc_methods import ControllerManager, ControlTrace
                    trajectory = ControlTrace(store, cfg)
                    manager = ControllerManager(cfg, AdaptiveSemaphore, PAARCController, trajectory.emit)
                else:
                    manager = manager_factory(cfg.to_paarc_config())
                ctrl_task = asyncio.create_task(controller_loop(manager))
            connector = aiohttp.TCPConnector(limit=max(50, int(effective_workers * 1.1)), ttl_dns_cache=300)
            async with aiohttp.ClientSession(
                connector=connector, timeout=aiohttp.ClientTimeout(total=max(1, cfg.timeout_sec * 2)),
                headers={"User-Agent": "FLOW-DC/2.0 PAARC/2.0"}, trace_configs=[build_trace_config()],
            ) as session:
                namer, written = SequentialNamer(), {}
                while keys and not shutdown_flag:
                    selected = set(keys)
                    attempt = max(row["attempt_intents"] for row in snapshot["rows"] if row["row_id"] in selected) + 1
                    print(f"[Attempt {attempt}] Processing {len(keys)} URLs...")
                    await download_batch_bounded(
                        cfg=cfg, session=session, df=current_df, manager=manager, sequential_namer=namer,
                        global_written_paths=written, effective_workers=effective_workers, store=store,
                        shared=shared,
                    )
                    snapshot = store.reconcile()
                    keys = store.eligible(snapshot)
                    if keys and not shutdown_flag:
                        await asyncio.sleep(cfg.retry_backoff_sec)
                        current_df = df.filter(pl.col("__key__").is_in(keys))
        snapshot = store.reconcile()
        if ctrl_task is not None:
            ctrl_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ctrl_task
            ctrl_task = None
        if trajectory is not None:
            if shared is not None:
                shared.check_health()
                await shared.close()
            # Include the final short interval and close retained trajectories
            # before publishing the result boundary.
            if manager is not None:
                manager.check_health()
                for controller in await manager.all_controllers():
                    await controller.step_interval()
            trajectory.close(complete=not shutdown_flag)
        return finalize_run(cfg, store, snapshot, _monotonic() - started, report_factory)
    finally:
        if ctrl_task is not None:
            ctrl_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ctrl_task
        try:
            if shared is not None:
                await shared.close()
            if trajectory is not None:
                trajectory.close()
        finally:
            store.close()


async def main() -> None:
    _setup_signal_handlers()
    await run_acquisition(parse_args())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
