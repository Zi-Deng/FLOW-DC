"""Unified metrics schema for benchmark results."""

from dataclasses import dataclass, field
from typing import Optional
import json
from pathlib import Path


@dataclass
class ResourceMetrics:
    """Resource usage metrics collected during a benchmark run."""

    cpu_avg_percent: float = 0.0
    cpu_max_percent: float = 0.0
    cpu_samples: list[float] = field(default_factory=list)
    memory_avg_mb: float = 0.0
    memory_max_mb: float = 0.0
    memory_samples: list[float] = field(default_factory=list)
    net_bytes_sent: int = 0
    net_bytes_recv: int = 0
    sample_count: int = 0
    sample_interval_sec: float = 0.5

    def to_dict(self) -> dict:
        return {
            "cpu_avg_percent": round(self.cpu_avg_percent, 2),
            "cpu_max_percent": round(self.cpu_max_percent, 2),
            "memory_avg_mb": round(self.memory_avg_mb, 2),
            "memory_max_mb": round(self.memory_max_mb, 2),
            "net_bytes_sent": self.net_bytes_sent,
            "net_bytes_recv": self.net_bytes_recv,
            "sample_count": self.sample_count,
        }


@dataclass
class BenchmarkResult:
    """Result from a single benchmark run."""

    tool: str
    variant: str
    concurrency: int
    run_number: int

    # Input info
    input_urls: int = 0

    # Core results
    successful_downloads: int = 0
    failed_downloads: int = 0
    success_rate_percent: float = 0.0

    # Throughput
    total_bytes_downloaded: int = 0
    elapsed_seconds: float = 0.0
    throughput_mbps: float = 0.0
    throughput_imgs_per_sec: float = 0.0

    # Error breakdown
    error_counts: dict[str, int] = field(default_factory=dict)

    # Resource usage
    resources: ResourceMetrics = field(default_factory=ResourceMetrics)

    # Tool-specific extras
    extra_metrics: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "tool": self.tool,
            "variant": self.variant,
            "concurrency": self.concurrency,
            "run_number": self.run_number,
            "input_urls": self.input_urls,
            "successful_downloads": self.successful_downloads,
            "failed_downloads": self.failed_downloads,
            "success_rate_percent": round(self.success_rate_percent, 2),
            "total_bytes_downloaded": self.total_bytes_downloaded,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "throughput_mbps": round(self.throughput_mbps, 3),
            "throughput_imgs_per_sec": round(self.throughput_imgs_per_sec, 2),
            "error_counts": self.error_counts,
            "resources": self.resources.to_dict(),
            "extra_metrics": self.extra_metrics,
        }


@dataclass
class AggregatedResult:
    """Aggregated results from multiple runs of the same configuration."""

    tool: str
    variant: str
    concurrency: int
    runs: list[BenchmarkResult] = field(default_factory=list)

    # Aggregated metrics (computed from runs)
    avg_success_rate: float = 0.0
    avg_throughput_mbps: float = 0.0
    avg_throughput_imgs_per_sec: float = 0.0
    avg_elapsed_seconds: float = 0.0
    std_throughput_mbps: float = 0.0
    avg_cpu_percent: float = 0.0
    avg_memory_mb: float = 0.0

    def compute_aggregates(self) -> None:
        """Compute aggregate statistics from individual runs."""
        if not self.runs:
            return

        n = len(self.runs)

        self.avg_success_rate = sum(r.success_rate_percent for r in self.runs) / n
        self.avg_throughput_mbps = sum(r.throughput_mbps for r in self.runs) / n
        self.avg_throughput_imgs_per_sec = (
            sum(r.throughput_imgs_per_sec for r in self.runs) / n
        )
        self.avg_elapsed_seconds = sum(r.elapsed_seconds for r in self.runs) / n
        self.avg_cpu_percent = sum(r.resources.cpu_avg_percent for r in self.runs) / n
        self.avg_memory_mb = sum(r.resources.memory_avg_mb for r in self.runs) / n

        # Standard deviation for throughput
        if n > 1:
            mean = self.avg_throughput_mbps
            variance = sum((r.throughput_mbps - mean) ** 2 for r in self.runs) / (n - 1)
            self.std_throughput_mbps = variance**0.5

    def to_dict(self) -> dict:
        return {
            "tool": self.tool,
            "variant": self.variant,
            "concurrency": self.concurrency,
            "num_runs": len(self.runs),
            "avg_success_rate": round(self.avg_success_rate, 2),
            "avg_throughput_mbps": round(self.avg_throughput_mbps, 3),
            "avg_throughput_imgs_per_sec": round(self.avg_throughput_imgs_per_sec, 2),
            "avg_elapsed_seconds": round(self.avg_elapsed_seconds, 2),
            "std_throughput_mbps": round(self.std_throughput_mbps, 3),
            "avg_cpu_percent": round(self.avg_cpu_percent, 2),
            "avg_memory_mb": round(self.avg_memory_mb, 2),
            "runs": [r.to_dict() for r in self.runs],
        }


@dataclass
class BenchmarkReport:
    """Complete benchmark report containing all results."""

    name: str
    timestamp: str
    dataset_path: str
    dataset_urls: int

    # System info
    platform: str = ""
    cpu_count: int = 0
    memory_gb: float = 0.0
    python_version: str = ""

    # Configuration
    concurrency_levels: list[int] = field(default_factory=list)
    timeout_sec: int = 30
    warmup_runs: int = 1
    measured_runs: int = 3

    # Results by tool/variant/concurrency
    results: dict[str, dict[int, AggregatedResult]] = field(default_factory=dict)

    def add_result(self, result: BenchmarkResult) -> None:
        """Add a single run result to the report."""
        key = f"{result.tool}_{result.variant}"
        if key not in self.results:
            self.results[key] = {}
        if result.concurrency not in self.results[key]:
            self.results[key][result.concurrency] = AggregatedResult(
                tool=result.tool,
                variant=result.variant,
                concurrency=result.concurrency,
            )
        self.results[key][result.concurrency].runs.append(result)

    def compute_all_aggregates(self) -> None:
        """Compute aggregates for all result groups."""
        for tool_results in self.results.values():
            for agg in tool_results.values():
                agg.compute_aggregates()

    def to_dict(self) -> dict:
        return {
            "metadata": {
                "name": self.name,
                "timestamp": self.timestamp,
                "system_info": {
                    "platform": self.platform,
                    "cpu_count": self.cpu_count,
                    "memory_gb": round(self.memory_gb, 1),
                    "python_version": self.python_version,
                },
                "dataset": {
                    "path": self.dataset_path,
                    "total_urls": self.dataset_urls,
                },
                "config": {
                    "concurrency_levels": self.concurrency_levels,
                    "timeout_sec": self.timeout_sec,
                    "warmup_runs": self.warmup_runs,
                    "measured_runs": self.measured_runs,
                },
            },
            "results": {
                tool_key: {
                    str(conc): agg.to_dict() for conc, agg in concurrency_results.items()
                }
                for tool_key, concurrency_results in self.results.items()
            },
        }

    def save_json(self, path: Path) -> None:
        """Save report as JSON file."""
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
