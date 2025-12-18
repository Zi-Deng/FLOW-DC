#!/usr/bin/env python3
"""
Template script for orchestrating batch image downloads.
Handles rate limiting, metadata generation, and batch processing.
Imports and uses single_download.py for actual download logic.
"""

import pandas as pd
import os
import sys
import aiohttp
import asyncio
import tarfile
import time
import argparse
import json
from pathlib import Path 
from tqdm.asyncio import tqdm
import signal
from collections import deque
import threading
import shutil
from urllib.parse import urlsplit
from contextvars import ContextVar
import statistics

# Import single download functions
from single_download_gbif import (
    download_single,
    load_input_file,
    extract_extension
)

# Global flag for graceful shutdown
shutdown_flag = False
TRACE_CTX: ContextVar[dict | None] = ContextVar("TRACE_CTX", default=None)

# Sequential counter for file naming
_sequential_counter = 0
_sequential_lock = threading.Lock()

def signal_handler(sig, frame):
    global shutdown_flag
    print("\nReceived interrupt signal. Shutting down gracefully...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_next_sequential_number():
    """Get next sequential number (thread-safe)."""
    global _sequential_counter
    with _sequential_lock:
        _sequential_counter += 1
        return _sequential_counter

def reset_sequential_counter():
    """Reset sequential counter to 0."""
    global _sequential_counter
    with _sequential_lock:
        _sequential_counter = 0

def parse_args():
    """
    Parse user inputs from arguments using argparse.
    """
    parser = argparse.ArgumentParser(description="Download images asynchronously, track bandwidth, and tar the output folder.")

    # JSON configuration file option
    parser.add_argument("--config", type=str, help="Path to JSON configuration file. If specified, all other arguments are ignored.")
    ############################################################
    parser.add_argument("--input", type=str, help="Path to the input CSV or Parquet file.")
    parser.add_argument("--input_format", type=str, default="parquet", help="Input file format (default: parquet).")
    parser.add_argument("--url", type=str, default="photo_url", help="Column name containing the image URLs.")
    parser.add_argument("--label", type=str, default=None, help="Column name containing the class names (optional). If not provided, all files will be placed in 'output' folder for imagefolder format, and class_name will be omitted from webdataset JSON metadata.")

    parser.add_argument("--output", type=str, help="Path to the output tar file (e.g., 'images.tar.gz').")
    parser.add_argument("--output_format", type=str, default="imagefolder", help="Output file format (default: tar).")

    parser.add_argument("--concurrent_downloads", type=int, default=1000, help="Number of concurrent downloads (default: 1000).")
    parser.add_argument("--timeout", type=int, default=30, help="Download timeout in seconds (default: 30).")

    parser.add_argument("--rate_limit", type=float, default=100.0, help="Initial rate limit in requests per second (default: 100.0).")
    parser.add_argument("--rate_capacity", type=int, default=200, help="Token bucket capacity (default: 200).")
    parser.add_argument("--enable_rate_limiting", action="store_true", help="Enable token bucket rate limiting.")
    parser.add_argument("--max_retry_attempts", type=int, default=3, help="Maximum retry attempts for 429 errors (default: 3).")
    parser.add_argument("--create_overview", action="store_true", default=True, help="Create JSON overview file (default: True).")
    parser.add_argument("--croissant", type=str, default="no_croissant", choices=["no_croissant", "basic_croissant", "comprehensive_croissant"], help="Generate Croissant metadata: no_croissant (default), basic_croissant, or comprehensive_croissant.")
    
    # File naming pattern
    parser.add_argument("--file_name_pattern", type=str, default="{segment[-2]}", help="Pattern for filename generation (default: {segment[-2]}).")
    parser.add_argument("--naming_mode", type=str, default="url_based", choices=["url_based", "sequential"], help="File naming mode: url_based (default) or sequential (00000001.ext, 00000002.ext, etc.).")
    
    # Croissant metadata optional fields
    parser.add_argument("--dataset_name", type=str, help="Name for the dataset (default: output folder name).")
    parser.add_argument("--dataset_description", type=str, help="Custom description of the dataset.")
    parser.add_argument("--dataset_license", type=str, default="Unspecified", help="Dataset license (e.g., CC-BY-4.0, MIT, Apache-2.0).")
    parser.add_argument("--dataset_creator", type=str, help="Dataset creator name or organization.")
    parser.add_argument("--dataset_url", type=str, help="URL/homepage for the dataset.")
    parser.add_argument("--dataset_version", type=str, default="1.0.0", help="Dataset version (default: 1.0.0).")
    parser.add_argument("--dataset_citation", type=str, help="Citation information for the dataset.")
    parser.add_argument("--dataset_keywords", type=str, help="Comma-separated keywords for the dataset.")

    args = parser.parse_args()

    if args.config:
        args = load_json_config(args.config)
    else:
        # Validate required arguments when not using JSON config
        if not args.input:
            parser.error("--input is required when not using --config")
        if not args.output:
            parser.error("--output is required when not using --config")
        return args

    return args

def load_json_config(config_path):
    """
    Load configuration from JSON file and return as argparse.Namespace object.
    """
    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in configuration file '{config_path}': {e}")
        sys.exit(1)

    # Define required fields and defaults
    required_fields = ['input', 'output']
    defaults = {
        'url': 'photo_url',
        'label': None,
        'input_format': 'parquet',
        'output_format': 'imagefolder',
        'concurrent_downloads': 1000,
        'timeout': 30,
        'rate_limit': 100.0,
        'rate_capacity': 200,
        'enable_rate_limiting': False,
        'max_retry_attempts': 0,
        'create_overview': True,
        'croissant': 'no_croissant',
        'file_name_pattern': '{segment[-2]}',
        'naming_mode': 'url_based',
        'dataset_name': None,
        'dataset_description': None,
        'dataset_license': 'Unspecified',
        'dataset_creator': None,
        'dataset_url': None,
        'dataset_version': '1.0.0',
        'dataset_citation': None,
        'dataset_keywords': None,
    }
    
    # Check required fields
    for field in required_fields:
        if field not in config_data:
            print(f"Error: Required field '{field}' not found in JSON configuration.")
            sys.exit(1)

    # Apply defaults for missing optional fields
    for field, default_value in defaults.items():
        if field not in config_data:
            config_data[field] = default_value

    # Validate data types
    type_validators = {
        'input': str,
        'output': str,
        'input_format': str,
        'output_format': str,
        'concurrent_downloads': int,
        'timeout': int,
        'rate_limit': (int, float),
        'rate_capacity': int,
        'enable_rate_limiting': bool,
        'max_retry_attempts': int,
        'file_name_pattern': str,
        'naming_mode': str
    }
    for field, expected_type in type_validators.items():
        if not isinstance(config_data[field], expected_type):
            print(f"Error: Field '{field}' has invalid type. Expected {expected_type}, got {type(config_data[field])}.")
            sys.exit(1)
    
    # Special validation for label field (can be None or str)
    if 'label' in config_data and config_data['label'] is not None and not isinstance(config_data['label'], str):
        print(f"Error: Field 'label' has invalid type. Expected str or None, got {type(config_data['label'])}.")
        sys.exit(1)
    
    # Convert to argparse.Namespace for compatibility
    return argparse.Namespace(**config_data)
def _percentile(xs, p: float):
    if not xs:
        return None
    ys = sorted(xs)
    k = (len(ys) - 1) * p
    f = int(k)
    c = min(f + 1, len(ys) - 1)
    if c == f:
        return ys[f]
    return ys[f] + (ys[c] - ys[f]) * (k - f)


class HostSignals:
    """
    Per-host per-interval signals:
      - TTFB samples (successful only)
      - q = p95(ttfb)-p10(ttfb)
      - r429 = #429/#total
      - T_since429
    Also maintains a rolling q_baseline estimate.
    """
    def __init__(self, interval_sec: float, q_hist_seconds: float = 300.0):
        self.interval_sec = interval_sec
        self._lock = threading.Lock()

        self._ttfb_ok = []
        self._total = 0
        self._n429 = 0
        self._bytes = 0

        self.last_429_time = None

        self.q_history = deque(maxlen=max(10, int(q_hist_seconds / interval_sec)))
        self._stable_q_intervals = 0
        self._no429_intervals = 0

    def record(self, status_code: int | None, ttfb: float | None, bytes_downloaded: int = 0):
        with self._lock:
            self._total += 1
            self._bytes += int(bytes_downloaded or 0)

            if status_code == 429:
                self._n429 += 1
                self.last_429_time = time.time()
                self._no429_intervals = 0

            # Successful for q purposes: 2xx/3xx AND a TTFB sample
            if status_code is not None and status_code < 400 and ttfb is not None:
                self._ttfb_ok.append(float(ttfb))

    def finish_interval(self, now: float):
        """
        Compute interval outputs and reset interval accumulators.
        Returns dict with q, q_baseline, r429, T_since429, p10/p50/p95, bytes.
        """
        with self._lock:
            total = self._total
            n429 = self._n429
            ttfb_ok = self._ttfb_ok
            interval_bytes = self._bytes

            # reset interval accumulators
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

        # q_baseline: median of recent q values from “good” periods (added by controller)
        q_baseline = statistics.median(self.q_history) if len(self.q_history) >= 6 else None

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
class HostPoliteController:
    CRUISE = "CRUISE"
    DRAIN = "DRAIN"
    COOLDOWN = "COOLDOWN"
    PROBE = "PROBE"

    def __init__(
        self,
        host: str,
        token_bucket: TokenBucket,
        conc: ConcurrencyLimiter,
        signals: HostSignals,
        *,
        min_rate: float = 1.0,
        max_rate: float = 10_000.0,
        cwnd_gain: float = 2.0,          # request-level inflight gain (Little’s law style)
        q_gamma: float = 2.5,            # DRAIN trigger if q > gamma*q_baseline
        q_exit_eps: float = 0.25,        # exit DRAIN when q <= (1+eps)*baseline
        drain_exit_no429_N: int = 3,
        drain_exit_stable_M: int = 3,
        cooldown_sec: float = 15 * 60,   # “quite a while”
        probe_delta: float = 0.03,       # 3% probe step
        probe_hold_intervals: int = 3,
        probe_lockout_sec: float = 10 * 60,
        beta_policy_rate: float = 0.5,
        beta_policy_cwnd: float = 0.5,
        beta_q_rate: float = 0.8,
        beta_q_cwnd: float = 0.8,
        per_host_conc_cap: int = 256,
    ):
        self.host = host
        self.tb = token_bucket
        self.conc = conc
        self.signals = signals

        self.min_rate = min_rate
        self.max_rate = max_rate
        self.cwnd_gain = cwnd_gain
        self.q_gamma = q_gamma
        self.q_exit_eps = q_exit_eps
        self.drain_exit_no429_N = drain_exit_no429_N
        self.drain_exit_stable_M = drain_exit_stable_M
        self.cooldown_sec = cooldown_sec
        self.probe_delta = probe_delta
        self.probe_hold_intervals = probe_hold_intervals
        self.probe_lockout_sec = probe_lockout_sec
        self.beta_policy_rate = beta_policy_rate
        self.beta_policy_cwnd = beta_policy_cwnd
        self.beta_q_rate = beta_q_rate
        self.beta_q_cwnd = beta_q_cwnd
        self.per_host_conc_cap = per_host_conc_cap

        self.state = self.CRUISE
        self.safe_rate = float(self.tb.get_rate())
        self.ceiling = float("inf")

        self.cooldown_until = 0.0
        self.probe_lockout_until = 0.0
        self._probe_prev_rate = None
        self._probe_remaining = 0

        self._no429_streak = 0
        self._q_stable_streak = 0

    def _set_rate(self, new_rate: float, reason: str):
        new_rate = max(self.min_rate, min(self.max_rate, float(new_rate)))
        self.safe_rate = new_rate
        self.tb.adjust_rate(new_rate, f"{self.host}:{reason}")

    def _set_conc(self, new_conc: int, reason: str):
        new_conc = max(2, min(int(new_conc), self.per_host_conc_cap))
        self.conc.set_limit(new_conc)

    def _derive_concurrency_from_latency(self, p50_ttfb: float | None):
        """
        Request-level analogue of “inflight ≈ rate * latency”.
        If p50_ttfb is missing, do nothing.
        """
        if p50_ttfb is None or p50_ttfb <= 0:
            return

        target_inflight = int(max(2, self.safe_rate * p50_ttfb * self.cwnd_gain))
        self._set_conc(target_inflight, "derived_concurrency")

    def step_interval(self, snap: dict, now: float):
        """
        snap = HostSignals.finish_interval(now)
        """

        had_429 = snap["n429"] > 0
        q = snap["q"]
        q_baseline = snap["q_baseline"]
        p50 = snap["p50"]

        # Maintain streaks
        if had_429:
            self._no429_streak = 0
        else:
            self._no429_streak += 1

        q_ok = False
        if q is not None and q_baseline is not None and q_baseline > 0:
            q_ok = (q <= q_baseline * (1.0 + self.q_exit_eps))
        # “stable” is only meaningful if we can compute it
        if q_ok:
            self._q_stable_streak += 1
        else:
            self._q_stable_streak = 0

        # Update baseline only during “good” non-violation periods
        if (not had_429) and (q is not None) and (self.state in (self.CRUISE, self.COOLDOWN)):
            self.signals.q_history.append(q)

        # Always (re)derive concurrency from latency when possible
        self._derive_concurrency_from_latency(p50)

        # ----- Hard policy violation handling (429) -----
        if had_429:
            # This is the “hard brake + long cooldown” rule.
            self.ceiling = min(self.ceiling, self.safe_rate)
            self._set_rate(self.safe_rate * self.beta_policy_rate, "429_backoff_rate")
            self._set_conc(int(self.conc.get_limit() * self.beta_policy_cwnd), "429_backoff_cwnd")

            self.cooldown_until = now + self.cooldown_sec
            self.probe_lockout_until = now + self.probe_lockout_sec

            self.state = self.DRAIN
            self._probe_prev_rate = None
            self._probe_remaining = 0
            return

        # ----- Queue/pressure proxy handling (q blow-up) -----
        q_blowup = False
        if q is not None and q_baseline is not None and q_baseline > 0:
            q_blowup = (q > q_baseline * self.q_gamma)

        if q_blowup and self.state != self.DRAIN:
            # Gentler than 429: treat as “be nicer”, not “policy limit”.
            self._set_rate(self.safe_rate * self.beta_q_rate, "q_drain_rate")
            self._set_conc(int(self.conc.get_limit() * self.beta_q_cwnd), "q_drain_cwnd")
            self.state = self.DRAIN

        # ----- State machine -----
        if self.state == self.DRAIN:
            # Exit criteria: no 429 for N intervals AND q back near baseline for M intervals.
            if (self._no429_streak >= self.drain_exit_no429_N) and (self._q_stable_streak >= self.drain_exit_stable_M):
                if now < self.cooldown_until:
                    self.state = self.COOLDOWN
                else:
                    self.state = self.CRUISE

        elif self.state == self.COOLDOWN:
            # No probing allowed; keep cruising.
            if now >= self.cooldown_until:
                self.state = self.CRUISE

        elif self.state == self.CRUISE:
            # Decide whether a probe is allowed
            if (now >= self.probe_lockout_until) and (now >= self.cooldown_until):
                # Require q to be “stable” for a while before probing
                if self._q_stable_streak >= 6:  # ~30s at 5s interval
                    # Enter a tiny probe
                    self._probe_prev_rate = self.safe_rate
                    self._probe_remaining = self.probe_hold_intervals
                    self._set_rate(self.safe_rate * (1.0 + self.probe_delta), "probe_up")
                    self.state = self.PROBE

        elif self.state == self.PROBE:
            # Hold probe for a few intervals, then accept or revert based on q
            self._probe_remaining -= 1

            # Abort-fast if q spikes (even without 429)
            if q_blowup:
                if self._probe_prev_rate is not None:
                    self._set_rate(self._probe_prev_rate, "probe_abort_q")
                self.probe_lockout_until = now + self.probe_lockout_sec
                self.state = self.COOLDOWN if now < self.cooldown_until else self.CRUISE
                self._probe_prev_rate = None
                self._probe_remaining = 0
                return

            if self._probe_remaining <= 0:
                # Probe “succeeded” (no 429 and no q blow-up)
                self.probe_lockout_until = now + self.probe_lockout_sec
                self.state = self.CRUISE
                self._probe_prev_rate = None
                self._probe_remaining = 0

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
    ):
        self.interval_sec = interval_sec
        self.initial_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.per_host_conc_init = per_host_conc_init
        self.per_host_conc_cap = per_host_conc_cap

        self._lock = threading.Lock()
        self.hosts: dict[str, HostPoliteController] = {}

    def _mk(self, host: str):
        tb = TokenBucket(rate=self.initial_rate, capacity=max(100, int(self.initial_rate * 2)))
        sig = HostSignals(interval_sec=self.interval_sec)
        conc = ConcurrencyLimiter(initial_limit=self.per_host_conc_init, max_limit=self.per_host_conc_cap)
        ctrl = HostPoliteController(
            host=host,
            token_bucket=tb,
            conc=conc,
            signals=sig,
            min_rate=self.min_rate,
            max_rate=self.max_rate,
            per_host_conc_cap=self.per_host_conc_cap,
        )
        return ctrl

    def get(self, url: str) -> HostPoliteController:
        host = urlsplit(url).netloc.lower()
        if not host:
            host = "unknown"
        with self._lock:
            if host not in self.hosts:
                self.hosts[host] = self._mk(host)
            return self.hosts[host]

    def all(self):
        with self._lock:
            return list(self.hosts.values())

async def per_host_polite_controller_loop(manager: PerHostControllerManager, interval_sec: float):
    print(f"[PoliteCtrl] Starting per-host controller loop (interval={interval_sec}s)")
    while not shutdown_flag:
        await asyncio.sleep(interval_sec)
        if shutdown_flag:
            break
        now = time.time()
        for ctrl in manager.all():
            snap = ctrl.signals.finish_interval(now)
            # If host had no traffic this interval, skip (avoids baseline pollution)
            if snap["total"] == 0:
                continue
            ctrl.step_interval(snap, now)


class TokenBucket:
    def __init__(self, rate, capacity):
        self.rate = rate
        self.last_refill = time.time()
        self._lock = threading.Lock()  # Thread-safe rate adjustments
        # Set capacity dynamically based on rate (2x rate, minimum 100)
        # This ensures capacity scales with rate from initialization
        self._update_capacity(rate)
        # Initialize tokens to capacity
        self.tokens = self.capacity
        # Cooldown tracking for immediate_decrease
        self._last_decrease_time = 0      # Timestamp of last immediate_decrease
        self._decrease_cooldown = 2.0     # Cooldown period in seconds

    async def acquire(self):
        """
        Wait until a token is available and consume it.
        """
        while True:
            self._refill()
            if self.tokens >= 1:
                self.tokens -= 1
                return
            await asyncio.sleep(0.01) 
    # Wait a short time before checking again

    def _refill(self):
        """
        Refill the bucket with tokens based on the elapsed time.
        """
        now = time.time()
        elapsed = now - self.last_refill
        new_tokens = elapsed * self.rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill = now

    def _update_capacity(self, new_rate):
        """
        Update capacity to be proportional to rate (2x rate, minimum 100).
        Maintains consistent burst behavior as rate changes.
        """
        new_capacity = max(100, int(new_rate * 2))  # 2 seconds of burst, min 100
        # Handle initial case where capacity doesn't exist yet
        if not hasattr(self, 'capacity') or new_capacity != self.capacity:
            # If capacity decreases, cap tokens at new capacity
            if hasattr(self, 'capacity') and new_capacity < self.capacity:
                self.tokens = min(self.tokens, new_capacity)
            self.capacity = new_capacity

    def adjust_rate(self, new_rate, reason=""):
        new_rate = max(1, new_rate)  # Minimum 1 request per second
        if abs(new_rate - self.rate) > 0.1:  # Only adjust if significant change
            # Update capacity proportionally to new rate
            self._update_capacity(new_rate)
            reason_str = f" ({reason})" if reason else ""
            print(f"[Rate Limit] {self.rate:.2f} -> {new_rate:.2f} req/sec (capacity: {self.capacity}){reason_str}")
            self.rate = new_rate

    def get_rate(self):
        return self.rate

    def immediate_decrease(self, factor=0.7):
        """
        Immediately decrease rate by factor (thread-safe).
        Used for immediate response to rate-limiting errors.
        Has a cooldown to prevent burst errors from cascading.
        """
        with self._lock:
            now = time.time()
            # Cooldown: only decrease once per cooldown period
            if now - self._last_decrease_time < self._decrease_cooldown:
                return self.rate  # Skip - already decreased recently
            
            new_rate = max(1, self.rate * factor)
            if abs(new_rate - self.rate) > 0.1:
                # Update capacity proportionally to new rate
                self._update_capacity(new_rate)
                print(f"[Rate Limit] Immediate decrease: {self.rate:.2f} -> {new_rate:.2f} req/sec (capacity: {self.capacity})")
                self.rate = new_rate
                self._last_decrease_time = now
                return new_rate
        return self.rate

class RateController:
    """
    Tracks download results and calculates success rates for adaptive rate limiting.
    429, 500+ errors, and connection errors (reset by peer, disconnected, refused)
    count as rate-limiting failures.
    """
    def __init__(self, window_size=100):
        self.results = deque(maxlen=window_size)  # (success, status_code, error_message) tuples
        self.interval_results = []  # Results for current interval
        self._lock = threading.Lock()
        self.current_interval_has_error = False  # Track if current interval has rate-limiting error
    
    def is_connection_error(self, error_message):
        """Check if error message indicates a connection-level rate limiting issue."""
        if error_message is None:
            return False
        error_str = str(error_message).lower()
        connection_error_patterns = [
            "connection reset by peer",
            "server disconnected",
            "connection refused",
        ]
        return any(pattern in error_str for pattern in connection_error_patterns)
    
    def is_rate_limiting_error(self, status_code, error_message=None):
        """Check if this is a rate-limiting error (429, 500+, or connection error)."""
        if status_code == 429 or (status_code is not None and status_code >= 500):
            return True
        return self.is_connection_error(error_message)
    
    def record_result(self, success, status_code, error_message=None):
        """Record a download result (thread-safe)."""
        with self._lock:
            self.results.append((success, status_code, error_message))
            self.interval_results.append((success, status_code, error_message))
            
            # Check if this is a rate-limiting error
            if not success and self.is_rate_limiting_error(status_code, error_message):
                self.current_interval_has_error = True
    
    def record_error(self, status_code, error_message=None):
        """Record a rate-limiting error (triggers immediate decrease)."""
        with self._lock:
            if self.is_rate_limiting_error(status_code, error_message):
                self.current_interval_has_error = True
                self.results.append((False, status_code, error_message))
                self.interval_results.append((False, status_code, error_message))
    
    def start_new_interval(self):
        """Called at start of each interval to reset interval tracking."""
        with self._lock:
            self.current_interval_has_error = False
            self.interval_results = []

def build_trace_config():
    """
    Build an aiohttp TraceConfig that writes timing into TRACE_CTX (ContextVar).
    We approximate TTFB as time-to-first-response-chunk (first on_response_chunk_received).
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
        d["dns_ms"] = None
        d["conn_queue_ms"] = None
        d["conn_create_ms"] = None
        d["reused_conn"] = False
        d["exc"] = None

    async def on_dns_resolvehost_start(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        d["_dns_t0"] = time.monotonic()

    async def on_dns_resolvehost_end(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        t1 = time.monotonic()
        t0 = d.pop("_dns_t0", None)
        if t0 is not None:
            d["dns_ms"] = (t1 - t0) * 1000.0

    async def on_connection_queued_start(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        d["_q_t0"] = time.monotonic()

    async def on_connection_queued_end(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        t1 = time.monotonic()
        t0 = d.pop("_q_t0", None)
        if t0 is not None:
            d["conn_queue_ms"] = (t1 - t0) * 1000.0

    async def on_connection_create_start(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        d["_c_t0"] = time.monotonic()

    async def on_connection_create_end(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        t1 = time.monotonic()
        t0 = d.pop("_c_t0", None)
        if t0 is not None:
            d["conn_create_ms"] = (t1 - t0) * 1000.0

    async def on_connection_reuseconn(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        d["reused_conn"] = True

    async def on_response_chunk_received(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        if d.get("ttfb") is None and d.get("t0") is not None:
            # first chunk time since request start
            d["ttfb"] = time.monotonic() - d["t0"]

    async def on_request_exception(session, ctx, params):
        d = await _ctx()
        if d is None:
            return
        d["exc"] = str(params.exception)

    trace.on_request_start.append(on_request_start)
    trace.on_dns_resolvehost_start.append(on_dns_resolvehost_start)
    trace.on_dns_resolvehost_end.append(on_dns_resolvehost_end)
    trace.on_connection_queued_start.append(on_connection_queued_start)
    trace.on_connection_queued_end.append(on_connection_queued_end)
    trace.on_connection_create_start.append(on_connection_create_start)
    trace.on_connection_create_end.append(on_connection_create_end)
    trace.on_connection_reuseconn.append(on_connection_reuseconn)
    trace.on_response_chunk_received.append(on_response_chunk_received)
    trace.on_request_exception.append(on_request_exception)

    return trace

class DownloadMetrics:
    """
    Collects per-request download metrics for BBR-like rate control.
    Tracks RTT samples, throughput, and file sizes for model estimation.
    """
    
    def __init__(self, window_seconds=60):
        self._lock = threading.Lock()
        self.rtt_samples = deque()           # (timestamp, rtt_seconds)
        self.throughput_samples = deque()     # (timestamp, bytes, duration)
        self.file_size_samples = deque(maxlen=1000)  # Track recent file sizes
        self.window_seconds = window_seconds
        
        # Per-interval accumulators (reset each interval)
        self._interval_bytes = 0
        self._interval_count = 0
        self._interval_start = time.time()
    
    def record_download(self, bytes_downloaded, start_time, end_time):
        """
        Record a completed download for BBR model estimation.
        
        Args:
            bytes_downloaded: Size of downloaded content in bytes
            start_time: Request start time (time.time())
            end_time: Request completion time (time.time())
        """
        rtt = end_time - start_time
        with self._lock:
            now = time.time()
            self.rtt_samples.append((now, rtt))
            self.throughput_samples.append((now, bytes_downloaded, rtt))
            if bytes_downloaded > 0:
                self.file_size_samples.append(bytes_downloaded)
            self._interval_bytes += bytes_downloaded
            self._interval_count += 1
            self._prune_old_samples(now)
    
    def _prune_old_samples(self, now):
        """Remove samples older than window_seconds."""
        cutoff = now - self.window_seconds
        while self.rtt_samples and self.rtt_samples[0][0] < cutoff:
            self.rtt_samples.popleft()
        while self.throughput_samples and self.throughput_samples[0][0] < cutoff:
            self.throughput_samples.popleft()
    
    def get_min_rtt(self):
        """
        Get RTprop estimate (minimum RTT in window).
        
        Returns:
            Minimum RTT in seconds, or None if no samples
        """
        with self._lock:
            if not self.rtt_samples:
                return None
            return min(rtt for _, rtt in self.rtt_samples)
    
    def get_avg_rtt(self):
        """
        Get average RTT from recent samples.
        
        Returns:
            Average RTT in seconds, or None if no samples
        """
        with self._lock:
            if not self.rtt_samples:
                return None
            return sum(rtt for _, rtt in self.rtt_samples) / len(self.rtt_samples)
    
    def get_interval_throughput(self, interval_sec=5.0):
        """
        Get throughput for the current interval and reset counters.
        
        Args:
            interval_sec: Expected interval duration in seconds
            
        Returns:
            Throughput in bytes/second
        """
        with self._lock:
            elapsed = time.time() - self._interval_start
            if elapsed <= 0:
                elapsed = interval_sec
            throughput = self._interval_bytes / elapsed
            # Reset interval counters
            self._interval_bytes = 0
            self._interval_count = 0
            self._interval_start = time.time()
            return throughput
    
    def get_interval_count(self):
        """Get number of downloads in current interval."""
        with self._lock:
            return self._interval_count
    
    def get_avg_file_size(self):
        """
        Get average file size from recent downloads.
        
        Returns:
            Average file size in bytes, or None if no samples
        """
        with self._lock:
            if not self.file_size_samples:
                return None
            return sum(self.file_size_samples) / len(self.file_size_samples)


class BBRController:
    """
    BBR-like model-based rate controller for FLOW-DC.
    
    Uses measured throughput and RTT to estimate bottleneck bandwidth (BtlBw)
    and minimum RTT (RTprop), then computes target rate and concurrency from
    BDP = BtlBw × RTprop.
    
    State machine:
    - STARTUP: Quickly probe for available bandwidth (exponential increase)
    - DRAIN: Reduce in-flight to drain excess queue after STARTUP
    - PROBE_BW: Steady-state cycling through probe-up/probe-down/cruise phases
    - PROBE_RTT: Periodically refresh RTprop estimate with minimal sending
    """
    
    # States
    STARTUP = "STARTUP"
    DRAIN = "DRAIN"
    PROBE_BW = "PROBE_BW"
    PROBE_RTT = "PROBE_RTT"
    
    # Gain cycle for PROBE_BW (8-interval cycle)
    # 1.25 = probe up, 0.75 = probe down, 1.0 = cruise
    GAIN_CYCLE = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    
    def __init__(self,
                 bw_window=10,              # intervals for BtlBw max filter
                 rtprop_window=60,          # seconds for RTprop min filter refresh
                 avg_file_size=None,        # None = learn from first 5 downloads
                 file_size_warmup=5,        # Number of downloads to learn avg_file_size
                 initial_rate=10):          # initial req/s during warmup
        
        self.state = self.STARTUP
        self.btlbw = None              # bytes/sec (estimated bottleneck bandwidth)
        self.rtprop = None             # seconds (estimated min RTT)
        self.rtprop_timestamp = None   # When rtprop was last updated
        
        # BtlBw estimation: windowed max of throughput
        self.bw_history = deque(maxlen=bw_window)
        self.bw_window = bw_window
        self.rtprop_window = rtprop_window
        
        # File size learning
        self.avg_file_size = avg_file_size
        self.file_size_warmup = file_size_warmup
        self.file_size_samples = []
        self.warmup_complete = avg_file_size is not None
        
        # Pacing and cwnd gains
        self.pacing_gain = 2.0         # STARTUP gain (doubles each interval)
        self.cwnd_gain = 2.0           # Target ~2×BDP in-flight
        self.gain_index = 0            # Current position in GAIN_CYCLE
        
        # STARTUP tracking
        self.startup_rounds = 0
        self.last_btlbw = None
        self.btlbw_plateau_count = 0   # Consecutive intervals without BtlBw growth
        
        # Initial rate for warmup/fallback
        self.initial_rate = initial_rate
        
        self._lock = threading.RLock()
        
        # Error recovery tracking
        self.error_suppressed_rate = None  # Rate cap after errors
        self.recovery_intervals = 0        # Consecutive error-free intervals
        self.RECOVERY_THRESHOLD = 20        # Intervals needed before full rate
    
    def update_file_size(self, bytes_downloaded):
        """
        Learn avg_file_size from first N downloads.
        
        Args:
            bytes_downloaded: Size of a completed download in bytes
        """
        if self.warmup_complete or bytes_downloaded <= 0:
            return
        
        with self._lock:
            self.file_size_samples.append(bytes_downloaded)
            
            if len(self.file_size_samples) >= self.file_size_warmup:
                self.avg_file_size = sum(self.file_size_samples) / len(self.file_size_samples)
                self.warmup_complete = True
                print(f"[BBR] Learned avg_file_size: {self.avg_file_size / 1024:.1f} KB from {self.file_size_warmup} samples")
    
    def update_model(self, throughput_bytes_per_sec, min_rtt_sample, now):
        """
        Update BtlBw and RTprop estimates from interval metrics.
        
        Args:
            throughput_bytes_per_sec: Measured throughput this interval
            min_rtt_sample: Minimum RTT observed this interval (seconds)
            now: Current timestamp
        """
        with self._lock:
            # Update BtlBw (windowed max of throughput)
            if throughput_bytes_per_sec > 0:
                self.bw_history.append(throughput_bytes_per_sec)
                new_btlbw = max(self.bw_history)
                
                # Track if BtlBw is still growing (for STARTUP exit)
                if self.btlbw is not None:
                    if new_btlbw <= self.btlbw * 1.1:  # Less than 10% growth
                        self.btlbw_plateau_count += 1
                    else:
                        self.btlbw_plateau_count = 0
                
                self.last_btlbw = self.btlbw
                self.btlbw = new_btlbw
            
            # Update RTprop (windowed min of RTT)
            if min_rtt_sample is not None and min_rtt_sample > 0:
                if self.rtprop is None or min_rtt_sample < self.rtprop:
                    self.rtprop = min_rtt_sample
                    self.rtprop_timestamp = now
    
    def get_bdp_bytes(self):
        """
        Compute BDP (Bandwidth-Delay Product) = BtlBw × RTprop.
        
        Returns:
            BDP in bytes, or None if model not initialized
        """
        with self._lock:
            if self.btlbw is None or self.rtprop is None:
                return None
            return self.btlbw * self.rtprop
    
    def get_target_rate(self):
        """
        Get target pacing rate in requests per second.
        
        Returns:
            Target rate (req/s), or None if model not ready
        """
        with self._lock:
            if not self.warmup_complete or self.btlbw is None:
                return self.initial_rate  # Fallback during warmup
            
            pacing_bytes_per_sec = self.btlbw * self.pacing_gain
            return pacing_bytes_per_sec / self.avg_file_size
    
    def get_target_concurrency(self, avg_rtt=None):
        """
        Get target concurrent downloads (BBR's cwnd analogue).
        
        Args:
            avg_rtt: Optional average RTT to use; defaults to rtprop
            
        Returns:
            Target concurrency (number of requests), or None if not ready
        """
        with self._lock:
            if not self.warmup_complete or self.avg_file_size is None:
                return None
            
            bdp = self.get_bdp_bytes()
            if bdp is None:
                return None
            
            # Target in-flight = BDP × cwnd_gain
            target_bytes = bdp * self.cwnd_gain
            concurrency = int(target_bytes / self.avg_file_size)
            return max(2, concurrency)  # Minimum 2 concurrent
    
    def clamp_btlbw(self, measured_throughput):
        """
        Clamp BtlBw down when 429/5xx errors indicate policy limits.
        
        Args:
            measured_throughput: Current measured throughput (bytes/sec)
        """
        with self._lock:
            if measured_throughput > 0 and self.btlbw is not None:
                # Cap BtlBw at current measured throughput
                self.btlbw = min(self.btlbw, measured_throughput)
                # Clear history to prevent old high values from persisting
                self.bw_history.clear()
                self.bw_history.append(self.btlbw)
    
    def step(self, metrics, rate_controller, now):
        """
        Run one interval of BBR state machine.
        
        Args:
            metrics: DownloadMetrics instance with current measurements
            rate_controller: RateController for 429/5xx awareness
            now: Current timestamp
            
        Returns:
            Tuple of (target_rate, target_concurrency)
        """
        with self._lock:
            # Check if we need to enter PROBE_RTT (rtprop stale)
            if (self.state != self.PROBE_RTT and 
                self.rtprop_timestamp is not None and
                now - self.rtprop_timestamp > self.rtprop_window):
                self._enter_probe_rtt()
            
            # State machine transitions
            if self.state == self.STARTUP:
                self._step_startup(rate_controller)
            elif self.state == self.DRAIN:
                self._step_drain()
            elif self.state == self.PROBE_BW:
                self._step_probe_bw()
            elif self.state == self.PROBE_RTT:
                self._step_probe_rtt(now)
            
            return self.get_target_rate(), self.get_target_concurrency()
    
    def _step_startup(self, rate_controller):
        """STARTUP state: exponentially probe for bandwidth."""
        self.startup_rounds += 1
        
        # Exit conditions:
        # 1. BtlBw stopped growing for 3 consecutive intervals
        # 2. 429/5xx errors detected
        if self.btlbw_plateau_count >= 3:
            print(f"[BBR] STARTUP → DRAIN: BtlBw plateaued at {self.btlbw / 1e6:.2f} MB/s")
            self._enter_drain()
        elif rate_controller.current_interval_has_error:
            print(f"[BBR] STARTUP → DRAIN: Rate-limiting errors detected")
            self._enter_drain()
        else:
            # Keep probing with high gain
            self.pacing_gain = 2.0
    
    def _enter_drain(self):
        """Transition to DRAIN state."""
        self.state = self.DRAIN
        self.pacing_gain = 0.5  # Slow down to drain queue
        self.drain_rounds = 0
    
    def _step_drain(self):
        """DRAIN state: reduce in-flight to ~BDP."""
        self.drain_rounds = getattr(self, 'drain_rounds', 0) + 1
        
        # Exit after 1-2 intervals (queue should be drained)
        if self.drain_rounds >= 2:
            print(f"[BBR] DRAIN → PROBE_BW: Queue drained")
            self._enter_probe_bw()
    
    def _enter_probe_bw(self):
        """Transition to PROBE_BW state."""
        self.state = self.PROBE_BW
        self.pacing_gain = 1.0
        self.gain_index = 0
    
    def _step_probe_bw(self):
        """PROBE_BW state: cycle through gain phases to probe bandwidth."""
        # Cycle through GAIN_CYCLE
        self.pacing_gain = self.GAIN_CYCLE[self.gain_index]
        self.gain_index = (self.gain_index + 1) % len(self.GAIN_CYCLE)
    
    def _enter_probe_rtt(self):
        """Transition to PROBE_RTT state."""
        self.state = self.PROBE_RTT
        self.pacing_gain = 0.1  # Minimal sending
        self.probe_rtt_rounds = 0
        self.rtprop = None  # Reset rtprop to get fresh measurement
        print(f"[BBR] → PROBE_RTT: Refreshing RTprop estimate")
    
    def _step_probe_rtt(self, now):
        """PROBE_RTT state: minimal sending to measure fresh RTT."""
        self.probe_rtt_rounds = getattr(self, 'probe_rtt_rounds', 0) + 1
        
        # Exit after 2 intervals with fresh RTprop
        if self.probe_rtt_rounds >= 2 and self.rtprop is not None:
            self.rtprop_timestamp = now
            print(f"[BBR] PROBE_RTT → PROBE_BW: New RTprop = {self.rtprop * 1000:.1f} ms")
            self._enter_probe_bw()
    
    def get_state(self):
        """Get current BBR state."""
        return self.state
    
    def enter_error_recovery(self, current_rate, error_count=1):
        """
        Enter error recovery mode - cap rate and reduce BtlBw estimate.
        
        Args:
            current_rate: Current token bucket rate after immediate decreases
            error_count: Number of errors in this interval
        """
        with self._lock:
            # Cap rate at current level
            self.error_suppressed_rate = current_rate
            self.recovery_intervals = 0
            
            # Reduce BtlBw proportionally to error severity
            if self.btlbw is not None:
                reduction = max(0.3, 1.0 - (error_count * 0.1))  # At least 70% reduction for many errors
                self.btlbw *= reduction
                self.bw_history.clear()
                self.bw_history.append(self.btlbw)
                print(f"[BBR] Error recovery: BtlBw reduced to {self.btlbw / 1e6:.2f} MB/s, rate capped at {current_rate:.0f} req/s")
    
    def check_recovery(self, has_errors):
        """
        Check if we can exit error recovery mode.
        
        Args:
            has_errors: Whether current interval had rate-limiting errors
            
        Returns:
            True if still in recovery, False if can resume normal operation
        """
        with self._lock:
            if self.error_suppressed_rate is None:
                return False  # Not in recovery
            
            if has_errors:
                self.recovery_intervals = 0
                return True  # Stay in recovery
            
            self.recovery_intervals += 1
            if self.recovery_intervals >= self.RECOVERY_THRESHOLD:
                print(f"[BBR] Exiting error recovery after {self.recovery_intervals} clean intervals")
                self.error_suppressed_rate = None
                return False
            
            return True  # Still in recovery
    
    def get_recovery_rate_cap(self):
        """Get the rate cap during error recovery, or None if not in recovery."""
        with self._lock:
            if self.error_suppressed_rate is None:
                return None
            # Allow gradual increase: 10% per clean interval
            multiplier = 1.0 + (self.recovery_intervals * 0.1)
            return self.error_suppressed_rate * multiplier


class ConcurrencyLimiter:
    """
    Dynamic concurrency limiter (BBR's cwnd analogue).
    Caps concurrent downloads based on BDP estimation.
    """
    
    def __init__(self, initial_limit=10, max_limit=1000):
        self._limit = initial_limit
        self._max_limit = max_limit
        self._current_count = 0
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition()
    
    async def acquire(self):
        """Acquire a slot (blocks if at limit, with timeout for shutdown)."""
        async with self._condition:
            while self._current_count >= self._limit:
                await self._condition.wait()
            self._current_count += 1
    
    async def release(self):
        """Release a slot."""
        async with self._condition:
            self._current_count -= 1
            self._condition.notify()
    
    def set_limit(self, new_limit):
        """
        Dynamically adjust concurrency limit.
        
        Args:
            new_limit: New maximum concurrent requests
        """
        new_limit = max(2, min(new_limit, self._max_limit))
        if new_limit != self._limit:
            print(f"[BBR] Concurrency limit: {self._limit} → {new_limit}")
            self._limit = new_limit
    
    def get_limit(self):
        """Get current concurrency limit."""
        return self._limit
    
    def get_current_count(self):
        """Get current number of in-flight requests."""
        return self._current_count


async def bbr_rate_controller(
    token_bucket,
    rate_controller,        # existing - safety layer for 429/5xx
    concurrency_limiter,    # new - BBR's cwnd analogue
    bbr_controller,         # new - BBR model and state machine
    download_metrics,       # new - RTT and throughput measurements
    max_rate,
    interval=5
):
    """
    BBR-like rate controller loop with error recovery.
    
    Uses measured throughput and RTT to compute optimal pacing rate and
    concurrency, while respecting 429/5xx rate limiting signals.
    
    Args:
        token_bucket: TokenBucket instance for pacing
        rate_controller: RateController for 429/5xx safety
        concurrency_limiter: ConcurrencyLimiter for cwnd
        bbr_controller: BBRController with model and state machine
        download_metrics: DownloadMetrics for measurements
        max_rate: Maximum allowed request rate
        interval: Control loop interval in seconds
    """
    MULTIPLICATIVE_DECREASE = 0.7  # 30% reduction on policy errors
    
    print(f"[BBR] Starting BBR rate controller (interval={interval}s, max_rate={max_rate} req/s)")
    
    while not shutdown_flag:
        await asyncio.sleep(interval)
        if shutdown_flag:
            break
        now = time.time()
        
        # 1. Get metrics from last interval
        throughput = download_metrics.get_interval_throughput(interval)
        min_rtt = download_metrics.get_min_rtt()
        avg_rtt = download_metrics.get_avg_rtt()
        
        # 2. Update BBR model with measurements
        bbr_controller.update_model(throughput, min_rtt, now)
        
        # 3. Check for errors and handle recovery
        has_errors = rate_controller.current_interval_has_error
        in_recovery = bbr_controller.check_recovery(has_errors)
        
        if has_errors and not in_recovery:
            # Just entered error state - enter recovery with current (reduced) rate
            current_rate = token_bucket.get_rate()
            bbr_controller.enter_error_recovery(current_rate, error_count=10)  # Assume burst
        
        # 4. Start new interval for rate controller (safety layer)
        rate_controller.start_new_interval()
        
        # 5. Run BBR state machine to get targets
        target_rate, target_concurrency = bbr_controller.step(
            download_metrics, rate_controller, now
        )
        
        # 6. Apply recovery cap if in recovery mode
        recovery_cap = bbr_controller.get_recovery_rate_cap()
        if recovery_cap is not None and target_rate is not None:
            if target_rate > recovery_cap:
                print(f"[BBR] Recovery: capping rate {target_rate:.0f} → {recovery_cap:.0f} req/s")
                target_rate = recovery_cap
        
        # 7. Apply final rate
        if target_rate:
            final_rate = min(target_rate, max_rate)
            final_rate = max(1, final_rate)  # Minimum 1 req/s
            token_bucket.adjust_rate(final_rate, f"bbr:{bbr_controller.get_state()}")
            
            # Log status
            btlbw_mbps = (bbr_controller.btlbw or 0) / 1e6
            rtprop_ms = (bbr_controller.rtprop or 0) * 1000
            bdp_kb = (bbr_controller.get_bdp_bytes() or 0) / 1024
            recovery_status = " [RECOVERY]" if recovery_cap else ""
            print(f"[BBR] {bbr_controller.get_state()}{recovery_status}: "
                  f"rate={final_rate:.0f} req/s, "
                  f"BtlBw={btlbw_mbps:.2f} MB/s, "
                  f"RTprop={rtprop_ms:.0f}ms, "
                  f"BDP={bdp_kb:.0f}KB")
        
        # 8. Apply concurrency limit
        if target_concurrency:
            concurrency_limiter.set_limit(target_concurrency)


async def download_batch(
    # Network/session
    session, concurrent_downloads,
    # Data batch
    df_batch,
    # Output config
    output_folder, output_format,
    # Column names
    url_col, class_col,
    # Tracking and limits
    total_bytes, timeout,
    # Rate limiting
    token_bucket, enable_rate_limiting, rate_controller,
    # File naming and download method
    file_name_pattern, naming_mode,
    # Retry control
    attempt_number=1,
    # BBR metrics (optional)
    download_metrics=None,
    bbr_controller=None,
    concurrency_limiter=None,
    polite_manager=None,
):
    """
    Download batch of images using single_download module.
    
    When download_metrics is provided (BBR mode), records RTT and throughput
    for each completed download to enable model-based rate control.
    """
    global shutdown_flag

    # Generate sequential filenames if needed
    filenames = {}
    if naming_mode == "sequential":
        reset_sequential_counter()
        for _, row in df_batch.iterrows():
            url = row[url_col]
            if pd.isna(url) or not str(url).strip():
                filenames[row.name] = None
            else:
                _, ext = extract_extension(str(url).strip())
                if not ext:
                    ext = ".png"
                seq_num = get_next_sequential_number()
                filenames[row.name] = f"{seq_num:08d}{ext}"

    # Wrapper for BBR metrics collection
    async def download_with_metrics(row, filename_override):
        url = str(row[url_col]).strip()
        host_ctrl = polite_manager.get(url) if polite_manager else None

        # Prefer per-host concurrency limit; optionally ALSO keep a global limiter if you want.
        if host_ctrl:
            await host_ctrl.conc.acquire()

        # Install a per-request trace dict into the ContextVar so TraceConfig can write into it.
        trace_dict = {}
        trace_token = TRACE_CTX.set(trace_dict)

        start_time = time.time()
        try:
            key, file_path, class_name, error, status_code = await download_single(
                url=url, key=row.name,
                class_name=row[class_col] if class_col is not None else None,
                output_folder=output_folder, output_format=output_format,
                session=session, timeout=timeout,
                filename=filename_override,
                token_bucket=(host_ctrl.tb if host_ctrl else token_bucket),
                enable_rate_limiting=enable_rate_limiting,
                total_bytes=total_bytes,
            )
            end_time = time.time()

            # Per-request bytes: derive from file size if we wrote a file successfully
            bytes_downloaded = 0
            if error is None and file_path and os.path.exists(file_path):
                try:
                    bytes_downloaded = os.path.getsize(file_path)
                except Exception:
                    bytes_downloaded = 0

            # Record per-host signals (TTFB proxy + 429 rate)
            if host_ctrl:
                ttfb = trace_dict.get("ttfb")
                host_ctrl.signals.record(status_code=status_code, ttfb=ttfb, bytes_downloaded=bytes_downloaded)

            # (Optional) keep your existing DownloadMetrics if you still want global reporting
            if download_metrics and error is None and bytes_downloaded > 0:
                download_metrics.record_download(bytes_downloaded, start_time, end_time)

            return key, file_path, class_name, error, status_code

        finally:
            TRACE_CTX.reset(trace_token)
            if host_ctrl:
                await host_ctrl.conc.release()

    
    tasks = [
        download_with_metrics(
            row,
            filenames.get(row.name) if naming_mode == "sequential" else None
        )
        for _, row in df_batch.iterrows()
    ]

    error_details = []
    retry_rows = []
    successful_downloads = 0
    successful_keys = set()  # Track keys that succeeded
    written_file_paths = {}  # Track file_path -> key for collision detection
    collision_count = 0

    print(f"\n--- Attempt #{attempt_number} - Processing {len(tasks)} images ---")
    if enable_rate_limiting and token_bucket:
        print(f"Current rate limit: {token_bucket.get_rate():.2f} req/sec")
    
    try:
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Downloading (attempt {attempt_number})"):
            if shutdown_flag:
                print("\nShutting down gracefully...")
                break

            key, file_path, class_name, error, status_code = await future

            # Record result for rate controller (always record, even if None)
            if rate_controller:
                rate_controller.record_result(error is None, status_code, error)

            if error:
                error_details.append({
                    'key': key,
                    'file_name': file_path,
                    'class_name': class_name,
                    'error': error,
                    'status_code': status_code,
                })

                is_429_error = status_code == 429 or "429" in str(error)
                is_timeout_error = status_code == 504 or "504" in str(error)
                is_server_error = status_code is not None and status_code >= 500
                is_connection_error = any(p in str(error).lower() for p in [
                    "connection reset by peer", "server disconnected", "connection refused"
                ])

                if is_429_error or is_timeout_error or is_server_error or is_connection_error:
                    original_row = df_batch.loc[df_batch.index==key]
                    if not original_row.empty:
                        retry_rows.append(original_row.to_dict(orient='records')[0])
                    
                    # Trigger immediate rate decrease for rate-limiting errors
                    if rate_controller and enable_rate_limiting:
                        rate_controller.record_error(status_code, error)
                
                if not (is_429_error or is_timeout_error or is_server_error or is_connection_error):
                    print(f"\n[Error] Key: {key}, Error: {error}, Status Code: {status_code}")
            else:
                successful_downloads += 1
                successful_keys.add(key)
                
                # Track file paths for collision detection
                if file_path:
                    if file_path in written_file_paths:
                        collision_count += 1
                        print(f"\n[Warning] Filename collision: {file_path}")
                        print(f"  Previous key: {written_file_paths[file_path]}, Current key: {key}")
                    else:
                        written_file_paths[file_path] = key
    except KeyboardInterrupt:
        print("Download interrupted by user")
        shutdown_flag = True

    unique_files_written = len(written_file_paths)
    return successful_downloads, error_details, retry_rows, successful_keys, unique_files_written, collision_count


def validate_and_clean(
    # Input/Output paths
    input, output_folder,
    # Column names
    url_col, class_col
):
    """
    Validate input file and output folder, load data, and clean it.
    Uses single_download.load_input_file() for format handling.
    """
    if not os.path.exists(input):
        print(f"Error: Input file {input} not found")
        sys.exit(1)
    
    if os.path.exists(output_folder):
        print(f"Warning: Output folder {output_folder} already exists. Deleting...")
        #sys.exit(1)
        shutil.rmtree(output_folder)

    # Determine input format from extension
    input_format = None
    if input.endswith(".parquet"):
        input_format = "parquet"
    elif input.endswith(".csv") or input.endswith(".txt"):
        input_format = "csv"
    elif input.endswith(".xlsx") or input.endswith(".xls"):
        input_format = "excel"
    elif input.endswith(".xml"):
        input_format = "xml"
    
    try:
        # Use single_download module to load file
        df = load_input_file(input, input_format)
    except Exception as e:
        print(f"Error: Failed to read input file {input}: {e}")
        sys.exit(1)

    if url_col not in df.columns:
        print(f"Error: URL column {url_col} not found in input file {input}")
        sys.exit(1)

    # Only validate class_col if it's provided
    if class_col is not None and class_col not in df.columns:
        print(f"Error: Label column {class_col} not found in input file {input}")
        sys.exit(1)

    initial_count = len(df)
    df = df.dropna(subset=[url_col])
    filtered_count = len(df)
    if filtered_count < initial_count:
        print(f"Filtered out {initial_count - filtered_count} rows with missing URLs")
    
    if filtered_count == 0:
        print("No valid URLs found in input file")
        sys.exit(1)
    
    return df, filtered_count

def create_json_overview(
        # Input/Output config
        input, input_format, output_path, output_format,
        # Column names
        url_col, class_col,
        # Download settings
        concurrent_downloads, timeout,
        # Results tracking
        error_details, successful_downloads, total_bytes, total_time, total_errors, total_downloaded, filtered_count,
        # Rate limiting
        rate_limit, rate_capacity, token_bucket=None, enable_rate_limiting=False,
        # Retry control
        max_retry_attempts=3
):
    overview_data = {
        "script_inputs": {
            "input": input,
            "input_format": input_format,
            "output": output_path,
            "output_format": output_format,
            "url_column": url_col,
            "label_column": class_col,
            "concurrent_downloads": concurrent_downloads,
            "timeout": timeout,
            "rate_limiting_enabled": enable_rate_limiting,
            "final_rate_limit": token_bucket.get_rate() if token_bucket else None,
            "rate_limit": rate_limit,
            "rate_capacity": rate_capacity,
            "max_retry_attempts": max_retry_attempts,
        },
        "download_summary": {
            "total_records_processed": filtered_count,
            "successful_downloads": successful_downloads,
            "failed_downloads": total_errors,
            "success_rate_percent": round((successful_downloads/(successful_downloads+total_errors)*100), 2) if (successful_downloads + total_errors) > 0 else 0,
            "total_data_mb": round(total_downloaded / 1e6, 2) if total_downloaded > 0 else 0,
            "total_time_seconds": round(total_time, 2),
            "average_speed_mbps": round((total_downloaded / total_time) / 1e6, 2) if total_time > 0 and total_downloaded > 0 else 0
        },
        "error_breakdown": {},
        "execution_info": {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "shutdown_requested": shutdown_flag
        }
    }
    if total_errors > 0:
        error_counts = {}
        for error_info in error_details:
            error_type = error_info['error']
            if error_type in error_counts:
                error_counts[error_type] += 1
            else:
                error_counts[error_type] = 1
        overview_data["error_breakdown"] = error_counts

    json_filename = os.path.splitext(output_path)[0] + "_overview.json"
    try:
        with open(json_filename, 'w') as json_file:
            json.dump(overview_data, json_file, indent=2)
        print(f"Created overview file: {json_filename}")
    except Exception as e:
        print(f"Warning: Could not create overview JSON file: {e}")

    return None

def create_tar_file(
    # Output paths
    output_path,  # This is the FOLDER containing images
    # Results tracking
    successful_downloads, total_errors
):
    tar_filename = output_path + ".tar.gz"  # Generate tar name
    
    if successful_downloads > 0 and not shutdown_flag and os.path.exists(output_path):
        try:
            print(f"\nCreating tar archive: {tar_filename}")
            with tarfile.open(tar_filename, "w:gz") as tar:
                tar.add(output_path, arcname=os.path.basename(output_path))
                
            full_path = Path(tar_filename).resolve()
            tar_size = os.path.getsize(tar_filename)
            print(f"Created tar archive: {full_path} ({tar_size / 1e6:.2f} MB)")          
        except Exception as e:
            print(f"Error creating tar archive: {e}")
            sys.exit(1)
    else:
        if shutdown_flag:
            print("Shutdown was requested, skipping tar creation")
        elif successful_downloads == 0:
            print("No successful downloads, skipping tar creation")
        sys.exit(1 if total_errors > 0 else 0)
    
    return None

def create_croissant_metadata(
    # Output paths
    output_folder, output_format,
    # Mode
    croissant_mode,
    # Dataset statistics
    successful_downloads, filtered_count, class_col, url_col,
    # Timing and size
    download_time, total_downloaded,
    # Optional metadata
    dataset_name=None, dataset_description=None, dataset_license=None,
    dataset_creator=None, dataset_url=None, dataset_version=None,
    dataset_citation=None, dataset_keywords=None
):
    """
    Generate Croissant metadata for the output dataset.
    
    Args:
        output_folder: Path to the folder containing downloaded images
        output_format: Format of output ('imagefolder' or 'webdataset')
        croissant_mode: Mode of metadata ('basic_croissant' or 'comprehensive_croissant')
        successful_downloads: Number of successfully downloaded images
        filtered_count: Total number of images attempted
        class_col: Name of the class/label column
        url_col: Name of the URL column
        download_time: Time taken for downloads in seconds
        total_downloaded: Total bytes downloaded
        dataset_name: Optional custom dataset name
        dataset_description: Optional custom dataset description
        dataset_license: Optional license information
        dataset_creator: Optional creator name or organization
        dataset_url: Optional dataset URL/homepage
        dataset_version: Optional version string
        dataset_citation: Optional citation information
        dataset_keywords: Optional comma-separated keywords
    """
    if not os.path.exists(output_folder):
        print(f"Warning: Output folder {output_folder} does not exist. Skipping Croissant metadata generation.")
        return None
    
    try:
        # Scan dataset structure
        dataset_name = dataset_name or os.path.basename(output_folder)
        tar_filename = output_folder + ".tar.gz"
        
        # Collect class information for imagefolder format
        classes = []
        class_counts = {}
        total_size = 0
        
        if output_format == "imagefolder" and os.path.exists(output_folder):
            # Scan subdirectories for classes
            for class_dir in os.listdir(output_folder):
                class_path = os.path.join(output_folder, class_dir)
                if os.path.isdir(class_path):
                    classes.append(class_dir)
                    # Count images in this class
                    image_files = [f for f in os.listdir(class_path) if os.path.isfile(os.path.join(class_path, f))]
                    class_counts[class_dir] = len(image_files)
                    # Add to total size
                    for img_file in image_files:
                        img_path = os.path.join(class_path, img_file)
                        total_size += os.path.getsize(img_path)
        
        # Build basic metadata structure
        # Use custom description if provided, otherwise generate default
        if dataset_description:
            description = dataset_description
        else:
            if class_col is not None:
                description = f"Image dataset downloaded and organized by {class_col}. Contains {successful_downloads} successfully downloaded images across {len(classes)} classes."
            else:
                description = f"Image dataset containing {successful_downloads} successfully downloaded images."
        
        metadata = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "name": dataset_name,
            "description": description,
            "datePublished": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "distribution": []
        }
        
        # Add optional fields if provided
        if dataset_url:
            metadata["url"] = dataset_url
        
        if dataset_version:
            metadata["version"] = dataset_version
        
        # Add tar file distribution if it exists
        if os.path.exists(tar_filename):
            tar_size = os.path.getsize(tar_filename)
            metadata["distribution"].append({
                "@type": "DataDownload",
                "encodingFormat": "application/gzip",
                "contentUrl": Path(tar_filename).resolve().as_uri(),
                "contentSize": tar_size
            })
        
        # Add folder distribution
        metadata["distribution"].append({
            "@type": "DataDownload",
            "encodingFormat": "application/x-directory",
            "contentUrl": Path(output_folder).resolve().as_uri(),
            "contentSize": total_size if total_size > 0 else total_downloaded
        })
        
        # Basic recordSet structure
        if classes:
            recordSet = {
                "@type": "ml:RecordSet",
                "name": "images",
                "field": [
                    {
                        "@type": "ml:Field",
                        "name": "image",
                        "description": "The image file",
                        "dataType": "ml:Image"
                    }
                ]
            }
            # Add description and class field only if class_col is provided
            if class_col is not None:
                recordSet["description"] = f"Image records organized by {class_col}"
                recordSet["field"].append({
                    "@type": "ml:Field",
                    "name": class_col,
                    "description": "The class label",
                    "dataType": "sc:Text"
                })
            else:
                recordSet["description"] = "Image records"
            metadata["recordSet"] = recordSet
        
        # Add comprehensive metadata if requested
        if croissant_mode == "comprehensive_croissant":
            # Use provided license or default
            metadata["license"] = dataset_license or "Unspecified"
            
            # Add creator if provided
            if dataset_creator:
                # Detect if it's likely an organization (contains certain keywords)
                is_org = any(word in dataset_creator.lower() for word in ['university', 'institute', 'lab', 'laboratory', 'company', 'corporation', 'inc', 'ltd'])
                metadata["creator"] = [
                    {
                        "@type": "Organization" if is_org else "Person",
                        "name": dataset_creator
                    }
                ]
            
            # Use provided keywords or generate default
            if dataset_keywords:
                keywords = [k.strip() for k in dataset_keywords.split(",")]
                metadata["keywords"] = keywords
            else:
                if class_col is not None:
                    metadata["keywords"] = ["images", "classification", class_col]
                else:
                    metadata["keywords"] = ["images"]
            
            # Add detailed class information
            if classes:
                metadata["about"] = f"Image classification dataset with {len(classes)} classes: {', '.join(sorted(classes)[:10])}" + ("..." if len(classes) > 10 else "")
                
                # Add class distribution
                metadata["variableMeasured"] = [
                    {
                        "@type": "PropertyValue",
                        "name": "class_distribution",
                        "value": class_counts
                    }
                ]
            
            # Add provenance information
            metadata["temporalCoverage"] = time.strftime("%Y-%m-%d", time.gmtime())
            
            # Add quality metrics
            metadata["measurementTechnique"] = "Automated download and organization"
            
            if filtered_count > 0:
                metadata["qualityMeasurement"] = {
                    "@type": "PropertyValue",
                    "name": "download_success_rate",
                    "value": round((successful_downloads / filtered_count) * 100, 2),
                    "unitText": "percent"
                }
            
            # Add performance metrics
            if download_time > 0 and total_downloaded > 0:
                metadata["contentSize"] = total_downloaded
                metadata["performanceMetrics"] = {
                    "downloadTime": round(download_time, 2),
                    "downloadSpeed": round((total_downloaded / download_time) / 1e6, 2),
                    "downloadSpeedUnit": "MB/s"
                }
            
            # Add citation if provided
            if dataset_citation:
                metadata["citeAs"] = dataset_citation
            
            # Enhanced recordSet for comprehensive mode
            if classes and "recordSet" in metadata:
                metadata["recordSet"]["field"].extend([
                    {
                        "@type": "ml:Field",
                        "name": "file_path",
                        "description": "Relative path to the image file",
                        "dataType": "sc:Text"
                    },
                    {
                        "@type": "ml:Field",
                        "name": "file_size",
                        "description": "Size of the image file in bytes",
                        "dataType": "sc:Integer"
                    }
                ])
                
                metadata["recordSet"]["totalRecords"] = successful_downloads
                metadata["recordSet"]["classes"] = sorted(classes)
        
        # Save metadata to file
        croissant_filename = os.path.splitext(output_folder)[0] + "_croissant.json"
        with open(croissant_filename, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Created Croissant metadata file: {croissant_filename}")
        
    except Exception as e:
        print(f"Warning: Could not create Croissant metadata file: {e}")
    
    return None


async def measure_baseline_bandwidth(timeout_seconds=30, test_size_mb=10):
    """
    Measure baseline download bandwidth using Cloudflare's speed test endpoint.
    
    Args:
        timeout_seconds: Maximum time to wait for test
        test_size_mb: Size of test file in MB (1-100)
        
    Returns:
        Tuple of (bandwidth_mbps, bandwidth_MBps) or (None, None) on failure
    """
    test_size_bytes = test_size_mb * 1_000_000
    test_url = f"https://speed.cloudflare.com/__down?bytes={test_size_bytes}"
    
    print(f"\n[Baseline] Measuring network bandwidth ({test_size_mb}MB test file)...")
    
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout_seconds)
        ) as session:
            start_time = time.monotonic()
            bytes_received = 0
            
            async with session.get(test_url) as response:
                if response.status != 200:
                    print(f"[Baseline] Warning: Test failed with status {response.status}")
                    return None, None
                
                async for chunk in response.content.iter_chunked(65536):
                    bytes_received += len(chunk)
            
            end_time = time.monotonic()
            duration = end_time - start_time
            
            bandwidth_mbps = (bytes_received * 8) / (duration * 1_000_000)
            bandwidth_MBps = bytes_received / (duration * 1_000_000)
            
            print(f"[Baseline] Network bandwidth: {bandwidth_mbps:.1f} Mbps ({bandwidth_MBps:.2f} MB/s)")
            print(f"[Baseline] Test duration: {duration:.2f}s, Downloaded: {bytes_received / 1_000_000:.1f}MB")
            
            return bandwidth_mbps, bandwidth_MBps
            
    except asyncio.TimeoutError:
        print(f"[Baseline] Warning: Bandwidth test timed out after {timeout_seconds}s")
        return None, None
    except Exception as e:
        print(f"[Baseline] Warning: Bandwidth test failed: {e}")
        return None, None


async def main():
    global shutdown_flag

    args = parse_args()

    # Display configuration source
    if hasattr(args, 'config') and args.config:
        print(f"Using JSON configuration from: {args.config}")
    else:
        print("Using command-line arguments")

    # Measure baseline bandwidth before starting downloads
    baseline_bandwidth_mbps, baseline_bandwidth_MBps = await measure_baseline_bandwidth(
        timeout_seconds=300,
        test_size_mb=300
    )

    input = args.input
    input_format = args.input_format
    url_col = args.url
    class_col = args.label
    output = args.output
    output_format = args.output_format
    concurrent_downloads = args.concurrent_downloads
    timeout = args.timeout
    rate_limit = args.rate_limit
    rate_capacity = args.rate_capacity
    enable_rate_limiting = args.enable_rate_limiting
    max_retry_attempts = args.max_retry_attempts
    create_overview = args.create_overview
    croissant_mode = args.croissant
    file_name_pattern = args.file_name_pattern
    naming_mode = args.naming_mode
    
    # Extract Croissant metadata arguments
    dataset_name = args.dataset_name
    dataset_description = args.dataset_description
    dataset_license = args.dataset_license
    dataset_creator = args.dataset_creator
    dataset_url = args.dataset_url
    dataset_version = args.dataset_version
    dataset_citation = args.dataset_citation
    dataset_keywords = args.dataset_keywords
    
    # Validate and clean input data
    df, filtered_count = validate_and_clean(input, output, url_col, class_col)

    token_bucket = None
    rate_controller = None
    bbr_controller = None
    download_metrics = None
    concurrency_limiter = None
    
    if enable_rate_limiting:
        token_bucket = TokenBucket(rate=rate_limit, capacity=rate_capacity)
        rate_controller = RateController(window_size=100)
        bbr_controller = BBRController(
            bw_window=10,           # 10 intervals (~50s) for BtlBw max filter
            rtprop_window=60,       # 60s for RTprop refresh
            avg_file_size=None,     # Learn from first 5 downloads
            file_size_warmup=5,
            initial_rate=rate_limit
        )
        download_metrics = DownloadMetrics(window_seconds=60)
        concurrency_limiter = ConcurrencyLimiter(initial_limit=concurrent_downloads, max_limit=concurrent_downloads*2)
        print(f"Processing {filtered_count} images with BBR rate control (initial: {rate_limit:.1f} req/s, max concurrency: {concurrent_downloads})")
    else:
        print(f"Processing {filtered_count} images with {concurrent_downloads} concurrent downloads (no rate limiting)")

    # Create output folder
    total_bytes = []
    download_start_time = time.monotonic()

    connector = aiohttp.TCPConnector(
        limit=int(concurrent_downloads * 1.1),
        ttl_dns_cache=300,
        use_dns_cache=True
    )

    recovery_task = None
    if enable_rate_limiting and token_bucket and rate_controller:
        max_rate = concurrent_downloads * 1.1
        print(f"Starting BBR rate controller (max: {max_rate:.1f} req/sec, interval: 5s)")
    polite_manager = None
    controller_task = None

    if enable_rate_limiting:
        interval_sec = 5.0

        # Interpret CLI values as “initial per-host” defaults.
        polite_manager = PerHostControllerManager(
            interval_sec=interval_sec,
            initial_rate=rate_limit,
            min_rate=1.0,
            max_rate=max(10.0, concurrent_downloads * 2.0),  # you can raise this if needed
            per_host_conc_init=max(2, min(concurrent_downloads, 32)),
            per_host_conc_cap=max(32, concurrent_downloads),
        )

        controller_task = asyncio.create_task(
            per_host_polite_controller_loop(polite_manager, interval_sec=interval_sec)
        )


    trace_config = build_trace_config()

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=timeout*2),
        headers={'User-Agent': 'FLOW-DC-ImageDownloader/1.0'},
        trace_configs=[trace_config],
    ) as session:
        all_error_details = []
        total_successful_downloads = 0
        total_unique_files_written = 0
        total_collisions = 0
        successfully_retried_keys = set()  # Track keys that succeeded on retry
        pending_retry_keys = set()  # Track keys pending retry
        current_df = df.copy()
        attempt = 1

        while attempt <= max_retry_attempts and not current_df.empty and not shutdown_flag:
            successful_downloads, error_details, retry_rows, successful_keys, unique_files_written, collision_count = await download_batch(
                session, concurrent_downloads,
                current_df,
                output, output_format,
                url_col, class_col,
                total_bytes, timeout,
                token_bucket, enable_rate_limiting, rate_controller,
                file_name_pattern, naming_mode,
                attempt,
                # BBR metrics (optional)
                download_metrics=download_metrics,
                bbr_controller=bbr_controller,
                concurrency_limiter=concurrency_limiter,
                polite_manager=polite_manager
            )

            total_successful_downloads += successful_downloads
            total_unique_files_written += unique_files_written
            total_collisions += collision_count
            all_error_details.extend(error_details)
            
            # Track which keys succeeded on retry (attempt > 1)
            if attempt > 1:
                retried_this_attempt = successful_keys & pending_retry_keys
                successfully_retried_keys.update(retried_this_attempt)

            count_retry_errors = len(retry_rows)
            non_retry_errors = len(error_details) - count_retry_errors

            # Per-attempt status summary
            print(f"\n{'='*60}")
            print(f"Attempt #{attempt} Status Summary")
            print(f"{'='*60}")
            print(f"  URLs processed:        {len(current_df)}")
            print(f"  Successful:            {successful_downloads}")
            print(f"  Retry errors:          {count_retry_errors}")
            print(f"  Permanent errors:      {non_retry_errors}")
            print(f"  Filename collisions:   {collision_count}")
            print(f"  Unique files written:  {unique_files_written}")
            if attempt > 1:
                print(f"  Retried successfully:  {len(retried_this_attempt)}")
            print(f"{'='*60}")

            if retry_rows and attempt < max_retry_attempts and not shutdown_flag:
                # Track keys from errors that will be retried
                pending_retry_keys = {e['key'] for e in error_details if any([
                    e['status_code'] == 429,
                    "429" in str(e['error']),
                    e['status_code'] == 504,
                    "504" in str(e['error']),
                    (e['status_code'] is not None and e['status_code'] >= 500),
                    any(p in str(e['error']).lower() for p in ["connection reset by peer", "server disconnected", "connection refused"])
                ])}
                
                current_df = pd.DataFrame(retry_rows)
                attempt += 1
                print(f"\nWaiting 2.0 seconds before retry attempt...")
                await asyncio.sleep(2.0)
            else:
                break

        # Final results
        if retry_rows and attempt > max_retry_attempts:
            print(f"\nReached maximum retry attempts ({max_retry_attempts}). {len(retry_rows)} items with retry errors will not be retried.")
        elif not retry_rows:
            print(f"\nAll downloads completed successfully or no retry errors remaining.")

    if controller_task:
        controller_task.cancel()


    # Measure bandwidth again after downloads
    end_bandwidth_mbps, end_bandwidth_MBps = await measure_baseline_bandwidth(
        timeout_seconds=300,
        test_size_mb=300
    )

    # Use the aggregated results
    successful_downloads = total_successful_downloads
    
    # Filter out errors that were successfully retried
    final_error_details = [
        e for e in all_error_details 
        if e['key'] not in successfully_retried_keys
    ]
    
    # Count actual files on disk
    actual_file_count = 0
    if os.path.exists(output):
        for root, dirs, files in os.walk(output):
            actual_file_count += len(files)

    download_end_time = time.monotonic()
    download_time = download_end_time - download_start_time
    total_downloaded = sum(total_bytes)  # Total bytes downloaded
    permanent_errors = len(final_error_details)

    # Print comprehensive final status summary
    print(f"\n{'='*60}")
    print(f"FINAL STATUS SUMMARY")
    print(f"{'='*60}")
    print(f"  Total URLs in input:     {filtered_count}")
    print(f"  Successful downloads:    {successful_downloads}")
    print(f"  Retried successfully:    {len(successfully_retried_keys)}")
    print(f"  Permanent failures:      {permanent_errors}")
    print(f"  Total collisions:        {total_collisions}")
    print(f"  Unique files written:    {total_unique_files_written}")
    print(f"  Actual files on disk:    {actual_file_count}")
    if successful_downloads > 0:
        success_rate = (successful_downloads / filtered_count) * 100
        print(f"  Success rate:            {success_rate:.1f}%")
    
    # Check for file count mismatch
    if actual_file_count != successful_downloads:
        print(f"\n  WARNING: File count mismatch!")
        print(f"    Expected (successful): {successful_downloads}")
        print(f"    Actual on disk:        {actual_file_count}")
        print(f"    Difference:            {successful_downloads - actual_file_count}")
    print(f"{'='*60}")
    
    if download_time > 0 and total_downloaded > 0:
        avg_speed = total_downloaded / download_time  # Bytes per second
        avg_speed_MBps = avg_speed / 1e6
        avg_speed_mbps = (avg_speed * 8) / 1e6
        print(f"\nDownload Performance:")
        print(f"  - Total Data: {total_downloaded / 1e6:.2f} MB")
        print(f"  - Download Time: {download_time:.2f} sec")
        print(f"  - Avg Download Speed: {avg_speed_MBps:.2f} MB/s ({avg_speed_mbps:.1f} Mbps)")
        
        # Compare with baseline bandwidth
        if baseline_bandwidth_mbps is not None or end_bandwidth_mbps is not None:
            print(f"\nBandwidth Comparison:")
            if baseline_bandwidth_mbps is not None:
                print(f"  - Start bandwidth: {baseline_bandwidth_mbps:.1f} Mbps ({baseline_bandwidth_MBps:.2f} MB/s)")
            else:
                print(f"  - Start bandwidth: N/A")
            if end_bandwidth_mbps is not None:
                print(f"  - End bandwidth: {end_bandwidth_mbps:.1f} Mbps ({end_bandwidth_MBps:.2f} MB/s)")
            else:
                print(f"  - End bandwidth: N/A")
            
            # Calculate average baseline
            valid_measurements = [m for m in [baseline_bandwidth_mbps, end_bandwidth_mbps] if m is not None]
            if valid_measurements:
                avg_baseline_mbps = sum(valid_measurements) / len(valid_measurements)
                avg_baseline_MBps = avg_baseline_mbps / 8
                efficiency = (avg_speed_mbps / avg_baseline_mbps) * 100
                print(f"  - Average baseline: {avg_baseline_mbps:.1f} Mbps ({avg_baseline_MBps:.2f} MB/s)")
                print(f"  - Actual download bandwidth: {avg_speed_mbps:.1f} Mbps ({avg_speed_MBps:.2f} MB/s)")
                print(f"  - Efficiency: {efficiency:.1f}% of baseline")
    else:
        print("  - No successful downloads to compute bandwidth statistics.")
    
    # Display detailed error breakdown (only permanent errors, not retried)
    if permanent_errors > 0:
        print(f"\nPermanent Error Breakdown:")
        error_counts = {}
        for error_info in final_error_details:
            error_type = error_info['error']
            if error_type in error_counts:
                error_counts[error_type] += 1
            else:
                error_counts[error_type] = 1
        
        for error_type, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {error_type}: {count} occurrences")

    # Conditionally create Croissant metadata file
    if croissant_mode != "no_croissant":
        create_croissant_metadata(
            output, output_format,
            croissant_mode,
            successful_downloads, filtered_count, class_col, url_col,
            download_time, total_downloaded,
            dataset_name, dataset_description, dataset_license,
            dataset_creator, dataset_url, dataset_version,
            dataset_citation, dataset_keywords
        )
    else:
        print("Skipped creating Croissant metadata file")

    # START TIMING - Tar creation
    tar_start_time = time.monotonic()
    
    # Create tar file if we have successful downloads
    # create_tar_file(
    #     output,
    #     successful_downloads,
    #     total_errors
    # )
    
    # END TIMING - Tar creation
    tar_end_time = time.monotonic()
    tar_time = tar_end_time - tar_start_time
    
    # Print tar creation time (only if tar was created)
    if successful_downloads > 0 and not shutdown_flag:
        print(f"\nTar Creation:")
        print(f"  - Time: {tar_time:.2f} sec")
    
    # Total time
    total_time = download_time + tar_time
    print(f"\nTotal Time: {total_time:.2f} sec")
    print(f"  - Download phase: {download_time:.2f} sec ({(download_time/total_time*100):.1f}%)")
    print(f"  - Tar creation phase: {tar_time:.2f} sec ({(tar_time/total_time*100):.1f}%)")

    # Conditionally create overview JSON file
    if create_overview:
        create_json_overview(
            input, input_format, output, output_format,
            url_col, class_col,
            concurrent_downloads, timeout,
            final_error_details, successful_downloads, total_bytes, download_time, permanent_errors, total_downloaded, filtered_count,
            rate_limit, rate_capacity, token_bucket, enable_rate_limiting, max_retry_attempts
        )
    else:
        print("\nSkipped creating JSON overview file")
    

if __name__ == "__main__":
    asyncio.run(main())

