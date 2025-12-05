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
import shutil
from collections import deque
import threading

# Import single download functions
from single_download_gbif import (
    download_single,
    load_input_file,
    extract_extension
)

# Global flag for graceful shutdown
shutdown_flag = False

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
    parser.add_argument("--rate_control_mode", type=str, default="aimd", choices=["aimd", "bbr"], help="Rate control algorithm: aimd (default) or bbr (model-based).")
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
        'rate_control_mode': 'aimd',
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
        'rate_control_mode': str,
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
        """
        with self._lock:
            new_rate = max(1, self.rate * factor)
            if abs(new_rate - self.rate) > 0.1:
                # Update capacity proportionally to new rate
                self._update_capacity(new_rate)
                print(f"[Rate Limit] Immediate decrease: {self.rate:.2f} -> {new_rate:.2f} req/sec (capacity: {self.capacity})")
                self.rate = new_rate
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
        self.perfect_success_intervals = 0  # Consecutive intervals with 100% success
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
    
    def get_success_rate(self):
        """
        Calculate success rate for probing (only 429/500+/connection errors count as failures).
        Returns success rate as float between 0.0 and 1.0.
        """
        with self._lock:
            if not self.results:
                return 1.0  # Optimistic start
            
            # Count successes and rate-limiting failures
            successes = 0
            rate_limiting_failures = 0
            
            for success, status_code, *rest in self.results:
                error_message = rest[0] if rest else None
                if success:
                    successes += 1
                elif self.is_rate_limiting_error(status_code, error_message):
                    rate_limiting_failures += 1
                # Other errors (404, 403, etc.) are ignored
            
            total_relevant = successes + rate_limiting_failures
            if total_relevant == 0:
                return 1.0
            
            return successes / total_relevant
    
    def has_perfect_success(self):
        """
        Binary check: True if no 429/500+/connection errors in current interval.
        Used for AIMD binary logic.
        """
        with self._lock:
            return not self.current_interval_has_error
    
    def get_perfect_success_intervals(self):
        """Get count of consecutive intervals with 100% success."""
        with self._lock:
            return self.perfect_success_intervals
    
    def start_new_interval(self):
        """
        Called at start of each interval to reset interval tracking.
        Updates perfect_success_intervals counter.
        """
        with self._lock:
            if not self.current_interval_has_error:
                self.perfect_success_intervals += 1
            else:
                self.perfect_success_intervals = 0  # Reset on error
            
            self.current_interval_has_error = False
            self.interval_results = []
    
    def reset_perfect_success_intervals(self):
        """Reset the perfect success intervals counter (thread-safe)."""
        with self._lock:
            self.perfect_success_intervals = 0


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
        """Acquire a slot (blocks if at limit)."""
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


async def aimd_convergence_controller(
    # Rate limiting
    token_bucket, rate_controller, max_rate,
    # Timing
    interval=5
):
    """
    Adaptive rate controller with AIMD, convergence detection, and periodic probing.
    
    States:
    - EXPLORING: Binary AIMD (100% success = increase, 429/500+ error = immediate decrease)
    - CONVERGED: Rate stable, holding at optimal
    - PROBING: Success-rate based check to restart AIMD if conditions changed
    """
    # AIMD parameters (binary logic)
    PERFECT_SUCCESS_INTERVALS = 3  # Must have 100% success for 3 intervals before increasing
    ADDITIVE_INCREASE = 10  # req/s or 10% (whichever larger)
    MULTIPLICATIVE_DECREASE = 0.7  # 30% reduction on errors
    
    # Convergence detection
    CONVERGENCE_WINDOW = 10  # Rate must be stable for 10 intervals
    CONVERGENCE_THRESHOLD = 10  # req/s (rate change < 10 req/s = stable)
    
    # Cruise phase
    CRUISE_DURATION = 10  # Hold for 10 intervals before probing
    
    # Probing thresholds (success-rate based)
    HIGH_THRESHOLD = 0.97  # 97% success = high headroom, restart upward
    LOW_THRESHOLD = 0.80  # 80% success = too low, restart downward
    
    # State tracking
    state = "EXPLORING"
    converged_rate = None
    intervals_in_state = 0
    rate_history = deque(maxlen=CONVERGENCE_WINDOW)  # Track rate changes
    
    while True:
        await asyncio.sleep(interval)
        
        current_rate = token_bucket.get_rate()
        rate_history.append(current_rate)
        intervals_in_state += 1
        
        # Start new interval for rate controller
        rate_controller.start_new_interval()
        
        if state == "EXPLORING":
            # Binary AIMD logic
            perfect_success_count = rate_controller.get_perfect_success_intervals()
            
            # Check if we should increase (100% success for N intervals)
            if perfect_success_count >= PERFECT_SUCCESS_INTERVALS:
                # Additive increase
                increase_amount = max(ADDITIVE_INCREASE, current_rate * 0.1)
                new_rate = min(max_rate, current_rate + increase_amount)
                token_bucket.adjust_rate(new_rate, "additive increase (perfect success)")
                rate_controller.reset_perfect_success_intervals()  # Reset counter
                print(f"[AIMD] ↑ {current_rate:.0f} -> {new_rate:.0f} req/s (perfect success for {perfect_success_count} intervals)")
            
            # Check for convergence
            if len(rate_history) >= CONVERGENCE_WINDOW:
                # Check if rate has been stable (changes < threshold)
                rate_changes = [abs(rate_history[i] - rate_history[i-1]) for i in range(1, len(rate_history))]
                max_change = max(rate_changes) if rate_changes else 0
                
                if max_change < CONVERGENCE_THRESHOLD:
                    converged_rate = current_rate
                    state = "CONVERGED"
                    intervals_in_state = 0
                    print(f"[AIMD] ✓ Converged at {converged_rate:.0f} req/s")
        
        elif state == "CONVERGED":
            # Hold rate constant
            if intervals_in_state >= CRUISE_DURATION:
                # Time to probe
                state = "PROBING"
                intervals_in_state = 0
                print(f"[AIMD] → Entering PROBING phase (held at {converged_rate:.0f} req/s for {CRUISE_DURATION} intervals)")
        
        elif state == "PROBING":
            # Success-rate based probing
            success_rate = rate_controller.get_success_rate()
            
            if success_rate > HIGH_THRESHOLD:
                # High headroom - restart AIMD upward
                state = "EXPLORING"
                intervals_in_state = 0
                rate_history.clear()
                rate_controller.reset_perfect_success_intervals()
                print(f"[AIMD] → High headroom ({success_rate:.1%}), restarting AIMD exploration upward")
            
            elif success_rate < LOW_THRESHOLD:
                # Low success rate - restart AIMD downward
                new_rate = max(10, current_rate * MULTIPLICATIVE_DECREASE)
                token_bucket.adjust_rate(new_rate, "probing: low success rate, restarting downward")
                state = "EXPLORING"
                intervals_in_state = 0
                rate_history.clear()
                rate_controller.reset_perfect_success_intervals()
                print(f"[AIMD] → Low success rate ({success_rate:.1%}), decreasing to {new_rate:.0f} req/s and restarting AIMD")
            
            else:
                # Acceptable range - continue holding
                state = "CONVERGED"
                intervals_in_state = 0
                print(f"[AIMD] → Acceptable success rate ({success_rate:.1%}), returning to CONVERGED state")


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
    BBR-like rate controller loop.
    
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
    
    while True:
        await asyncio.sleep(interval)
        now = time.time()
        
        # 1. Get metrics from last interval
        throughput = download_metrics.get_interval_throughput(interval)
        min_rtt = download_metrics.get_min_rtt()
        avg_rtt = download_metrics.get_avg_rtt()
        
        # 2. Update BBR model with measurements
        bbr_controller.update_model(throughput, min_rtt, now)
        
        # 3. Run BBR state machine to get targets
        target_rate, target_concurrency = bbr_controller.step(
            download_metrics, rate_controller, now
        )
        
        # 4. Start new interval for rate controller (safety layer)
        rate_controller.start_new_interval()
        
        # 5. Apply RateController safety clamps (429/5xx protection)
        if rate_controller.current_interval_has_error:
            if target_rate:
                target_rate *= MULTIPLICATIVE_DECREASE
            bbr_controller.clamp_btlbw(throughput)  # Update model
            print(f"[BBR] Policy error detected, clamping rate by {int((1-MULTIPLICATIVE_DECREASE)*100)}%")
        
        # 6. Apply final rate
        if target_rate:
            final_rate = min(target_rate, max_rate)
            final_rate = max(1, final_rate)  # Minimum 1 req/s
            token_bucket.adjust_rate(final_rate, f"bbr:{bbr_controller.get_state()}")
            
            # Log status
            btlbw_mbps = (bbr_controller.btlbw or 0) / 1e6
            rtprop_ms = (bbr_controller.rtprop or 0) * 1000
            bdp_kb = (bbr_controller.get_bdp_bytes() or 0) / 1024
            print(f"[BBR] {bbr_controller.get_state()}: "
                  f"rate={final_rate:.0f} req/s, "
                  f"BtlBw={btlbw_mbps:.2f} MB/s, "
                  f"RTprop={rtprop_ms:.0f}ms, "
                  f"BDP={bdp_kb:.0f}KB")
        
        # 7. Apply concurrency limit
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
    concurrency_limiter=None
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
        """Wrapper that captures timing and bytes for BBR metrics."""
        start_time = time.time()
        
        # Acquire concurrency slot if using BBR
        if concurrency_limiter:
            await concurrency_limiter.acquire()
        
        try:
            key, file_path, class_name, error, status_code = await download_single(
                # Core identifiers
                url=row[url_col], key=row.name, class_name=row[class_col] if class_col is not None else None,
                # Output config
                output_folder=output_folder, output_format=output_format,
                # Network/session
                session=session, timeout=timeout,
                # File naming
                filename=filename_override,
                # Rate limiting
                token_bucket=token_bucket, enable_rate_limiting=enable_rate_limiting,
                # Tracking
                total_bytes=total_bytes
            )
            
            end_time = time.time()
            
            # Record metrics for BBR if successful
            if download_metrics and error is None:
                # Get bytes from total_bytes (it's appended to during download)
                bytes_downloaded = total_bytes[-1] if total_bytes else 0
                download_metrics.record_download(bytes_downloaded, start_time, end_time)
                
                # Update BBR's file size learning
                if bbr_controller and bytes_downloaded > 0:
                    bbr_controller.update_file_size(bytes_downloaded)
            
            return key, file_path, class_name, error, status_code
        finally:
            # Release concurrency slot
            if concurrency_limiter:
                await concurrency_limiter.release()
    
    tasks = [
        download_with_metrics(
            row,
            filenames.get(row.name) if naming_mode == "sequential" else None
        )
        for _, row in df_batch.iterrows()
    ]

    error_details = []
    retry_rows =[]
    successful_downloads = 0

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
                        # Immediate decrease via rate controller
                        token_bucket.immediate_decrease(0.7)
                
                if not (is_429_error or is_timeout_error or is_server_error or is_connection_error):
                    print(f"\n[Error] Key: {key}, Error: {error}, Status Code: {status_code}")
            else:
                successful_downloads += 1
    except KeyboardInterrupt:
        print("Download interrupted by user")
        shutdown_flag = True

    return successful_downloads, error_details, retry_rows


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
        print(f"Error: Output folder {output_folder} already exists")
        sys.exit(1)
        #shutil.rmtree(output_folder)

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

async def main():
    global shutdown_flag

    args = parse_args()

    # Display configuration source
    if hasattr(args, 'config') and args.config:
        print(f"Using JSON configuration from: {args.config}")
    else:
        print("Using command-line arguments")

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
    rate_control_mode = args.rate_control_mode
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
        
        if rate_control_mode == "bbr":
            # Initialize BBR-specific components
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
            print(f"Processing {filtered_count} images with AIMD rate control ({concurrent_downloads} concurrent, {rate_limit:.1f} req/s)")
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
        
        if rate_control_mode == "bbr" and bbr_controller and download_metrics and concurrency_limiter:
            print(f"Starting BBR rate controller (max: {max_rate:.1f} req/sec, interval: 5s)")
            recovery_task = asyncio.create_task(
                bbr_rate_controller(
                    token_bucket, rate_controller, concurrency_limiter,
                    bbr_controller, download_metrics, max_rate, interval=5
                )
            )
        else:
            print(f"Starting AIMD rate controller (max: {max_rate:.1f} req/sec, interval: 5s)")
            recovery_task = asyncio.create_task(
                aimd_convergence_controller(token_bucket, rate_controller, max_rate, interval=5)
            )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=timeout*2),  # Overall session timeout
        headers={'User-Agent': 'FLOW-DC-ImageDownloader/1.0'}
    ) as session:
        all_error_details = []
        total_successful_downloads = 0
        current_df = df.copy()
        attempt = 1

        while attempt <= max_retry_attempts and not current_df.empty and not shutdown_flag:
            successful_downloads, error_details, retry_rows = await download_batch(
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
                concurrency_limiter=concurrency_limiter
            )

            total_successful_downloads += successful_downloads
            all_error_details.extend(error_details)

            count_retry_errors = len(retry_rows)
            non_retry_errors = len(error_details) - count_retry_errors

            print(f"\nAttempt #{attempt} Results:")
            print(f"  - Successful downloads: {successful_downloads}")
            print(f"  - Retry errors: {count_retry_errors}")
            print(f"  - Other errors: {non_retry_errors}")

            if retry_rows and attempt < max_retry_attempts and not shutdown_flag:
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

    if recovery_task:
        recovery_task.cancel()

    # Use the aggregated results
    successful_downloads = total_successful_downloads
    error_details = all_error_details

    download_end_time = time.monotonic()
    download_time = download_end_time - download_start_time
    total_downloaded = sum(total_bytes)  # Total bytes downloaded
    total_errors = len(error_details)

    # Print download summary
    print(f"\nDownload Summary:")
    print(f"  - Successful downloads: {successful_downloads}")
    print(f"  - Failed downloads: {total_errors}")
    if successful_downloads + total_errors > 0:
        print(f"  - Success rate: {(successful_downloads/(successful_downloads+total_errors)*100):.1f}%")
    
    if download_time > 0 and total_downloaded > 0:
        avg_speed = total_downloaded / download_time  # Bytes per second
        print(f"  - Total Data: {total_downloaded / 1e6:.2f} MB")
        print(f"  - Download Time: {download_time:.2f} sec")
        print(f"  - Avg Download Speed: {avg_speed / 1e6:.2f} MB/s")
    else:
        print("  - No successful downloads to compute bandwidth statistics.")
    
    # Display detailed error breakdown
    if total_errors > 0:
        print(f"\nError Breakdown:")
        error_counts = {}
        for error_info in error_details:
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
            error_details, successful_downloads, total_bytes, download_time, total_errors, total_downloaded, filtered_count,
            rate_limit, rate_capacity, token_bucket, enable_rate_limiting, max_retry_attempts
        )
    else:
        print("\nSkipped creating JSON overview file")
    

if __name__ == "__main__":
    asyncio.run(main())

