"""System resource monitoring using psutil."""

import threading
import time
from dataclasses import dataclass, field
from typing import Optional

import psutil

from .metrics import ResourceMetrics


@dataclass
class ResourceSample:
    """Single resource measurement snapshot."""

    timestamp: float
    cpu_percent: float
    memory_rss_mb: float
    net_bytes_sent: int
    net_bytes_recv: int


class ResourceMonitor:
    """Background thread for monitoring system resources during benchmark runs.

    Can monitor either the whole system or a specific process (and its children).
    """

    def __init__(
        self,
        sample_interval: float = 0.5,
        target_pid: Optional[int] = None,
    ):
        """Initialize the resource monitor.

        Args:
            sample_interval: Seconds between samples
            target_pid: If provided, monitor this process and children.
                       If None, monitor system-wide.
        """
        self._interval = sample_interval
        self._target_pid = target_pid
        self._samples: list[ResourceSample] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        # Baseline network counters
        self._net_baseline_sent = 0
        self._net_baseline_recv = 0

    def start(self) -> None:
        """Start background monitoring."""
        # Record baseline network counters
        net = psutil.net_io_counters()
        self._net_baseline_sent = net.bytes_sent
        self._net_baseline_recv = net.bytes_recv

        self._running = True
        self._samples = []
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> ResourceMetrics:
        """Stop monitoring and return aggregated metrics."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)

        return self._compute_metrics()

    def _sample_loop(self) -> None:
        """Background sampling loop."""
        while self._running:
            try:
                sample = self._collect_sample()
                with self._lock:
                    self._samples.append(sample)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            time.sleep(self._interval)

    def _collect_sample(self) -> ResourceSample:
        """Collect a single resource sample."""
        timestamp = time.time()

        if self._target_pid is not None:
            # Monitor specific process and children
            cpu_percent, memory_mb = self._collect_process_metrics()
        else:
            # Monitor system-wide
            cpu_percent = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()
            memory_mb = mem.used / (1024 * 1024)

        # Network is always system-wide
        net = psutil.net_io_counters()

        return ResourceSample(
            timestamp=timestamp,
            cpu_percent=cpu_percent,
            memory_rss_mb=memory_mb,
            net_bytes_sent=net.bytes_sent,
            net_bytes_recv=net.bytes_recv,
        )

    def _collect_process_metrics(self) -> tuple[float, float]:
        """Collect CPU and memory for target process and children."""
        try:
            proc = psutil.Process(self._target_pid)
            children = proc.children(recursive=True)

            # Aggregate across process tree
            total_cpu = proc.cpu_percent(interval=None)
            total_memory = proc.memory_info().rss

            for child in children:
                try:
                    total_cpu += child.cpu_percent(interval=None)
                    total_memory += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            return total_cpu, total_memory / (1024 * 1024)

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0.0, 0.0

    def _compute_metrics(self) -> ResourceMetrics:
        """Compute aggregated metrics from collected samples."""
        with self._lock:
            samples = list(self._samples)

        if not samples:
            return ResourceMetrics()

        cpu_values = [s.cpu_percent for s in samples]
        memory_values = [s.memory_rss_mb for s in samples]

        # Network delta from baseline
        if samples:
            last = samples[-1]
            net_sent = last.net_bytes_sent - self._net_baseline_sent
            net_recv = last.net_bytes_recv - self._net_baseline_recv
        else:
            net_sent = 0
            net_recv = 0

        return ResourceMetrics(
            cpu_avg_percent=sum(cpu_values) / len(cpu_values),
            cpu_max_percent=max(cpu_values),
            cpu_samples=cpu_values,
            memory_avg_mb=sum(memory_values) / len(memory_values),
            memory_max_mb=max(memory_values),
            memory_samples=memory_values,
            net_bytes_sent=net_sent,
            net_bytes_recv=net_recv,
            sample_count=len(samples),
            sample_interval_sec=self._interval,
        )


class ProcessResourceMonitor(ResourceMonitor):
    """Monitor a subprocess that will be started later.

    Use set_pid() after starting the subprocess to begin monitoring.
    """

    def __init__(self, sample_interval: float = 0.5):
        super().__init__(sample_interval=sample_interval, target_pid=None)
        self._pid_set = threading.Event()

    def set_pid(self, pid: int) -> None:
        """Set the process ID to monitor."""
        self._target_pid = pid
        self._pid_set.set()

    def _sample_loop(self) -> None:
        """Wait for PID to be set, then start sampling."""
        # Wait up to 30 seconds for process to start
        self._pid_set.wait(timeout=30.0)

        if self._target_pid is None:
            return

        super()._sample_loop()
