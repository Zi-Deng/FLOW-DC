"""Adapter for running img2dataset benchmarks."""

import asyncio
import json
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .metrics import BenchmarkResult, ResourceMetrics
from .resource_monitor import ProcessResourceMonitor


@dataclass
class Img2DatasetConfig:
    """Configuration for an img2dataset benchmark run."""

    input_path: str
    output_folder: str
    url_column: str
    thread_count: int
    processes_count: int = 1
    timeout_sec: int = 30
    retries: int = 1
    output_format: str = "files"
    resize_mode: str = "no"
    image_size: int = 256


class Img2DatasetAdapter:
    """Adapter for running img2dataset benchmarks."""

    def __init__(self):
        """Initialize adapter."""
        pass

    def build_command(self, config: Img2DatasetConfig) -> list[str]:
        """Build img2dataset CLI command.

        Args:
            config: Run configuration

        Returns:
            Command as list of strings
        """
        cmd = [
            "img2dataset",
            "--url_list",
            str(config.input_path),
            "--output_folder",
            str(config.output_folder),
            "--input_format",
            "parquet",
            "--url_col",
            config.url_column,
            "--output_format",
            config.output_format,
            "--thread_count",
            str(config.thread_count),
            "--processes_count",
            str(config.processes_count),
            "--timeout",
            str(config.timeout_sec),
            "--retries",
            str(config.retries),
            "--resize_mode",
            config.resize_mode,
        ]

        if config.resize_mode != "no":
            cmd.extend(["--image_size", str(config.image_size)])

        return cmd

    async def run(
        self,
        config: Img2DatasetConfig,
        run_number: int,
        monitor_resources: bool = True,
        resource_interval: float = 0.5,
        show_progress: bool = False,
    ) -> BenchmarkResult:
        """Execute img2dataset and collect results.

        Args:
            config: Run configuration
            run_number: Run number for identification
            monitor_resources: Whether to collect resource metrics
            resource_interval: Seconds between resource samples

        Returns:
            BenchmarkResult with metrics
        """
        # Ensure output directory is clean
        output_path = Path(config.output_folder)
        if output_path.exists():
            shutil.rmtree(output_path)

        # Build command
        cmd = self.build_command(config)

        # Start resource monitor
        monitor = None
        if monitor_resources:
            monitor = ProcessResourceMonitor(sample_interval=resource_interval)
            monitor.start()

        # Track timing
        start_time = time.time()

        # Run img2dataset
        if show_progress:
            # Pass through output to terminal
            proc = await asyncio.create_subprocess_exec(*cmd)
            if monitor:
                monitor.set_pid(proc.pid)
            await proc.wait()
            stderr = b""
        else:
            # Capture output
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            if monitor:
                monitor.set_pid(proc.pid)
            stdout, stderr = await proc.communicate()

        elapsed = time.time() - start_time

        # Stop resource monitor
        resources = ResourceMetrics()
        if monitor:
            resources = monitor.stop()

        # Parse results from output directory
        result = self._parse_results(
            output_path=output_path,
            config=config,
            run_number=run_number,
            elapsed=elapsed,
            resources=resources,
            returncode=proc.returncode,
            stderr=stderr.decode(),
        )

        return result

    def _parse_results(
        self,
        output_path: Path,
        config: Img2DatasetConfig,
        run_number: int,
        elapsed: float,
        resources: ResourceMetrics,
        returncode: int,
        stderr: str,
    ) -> BenchmarkResult:
        """Parse img2dataset output into BenchmarkResult."""
        # Count downloaded files
        successful = 0
        failed = 0
        total_bytes = 0

        if output_path.exists():
            # img2dataset creates numbered subdirectories with files
            for item in output_path.rglob("*"):
                if item.is_file():
                    suffix = item.suffix.lower()
                    # Count image files as successful
                    if suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                        successful += 1
                        total_bytes += item.stat().st_size
                    # Count JSON metadata files separately
                    elif suffix == ".json" and item.name != "_stats.json":
                        pass  # Metadata file, don't count

        # Try to parse shard stats if available
        stats = self._parse_shard_stats(output_path)
        if stats:
            # Use stats if available (more accurate)
            if "successes" in stats:
                successful = stats["successes"]
            if "failed" in stats:
                failed = stats["failed"]

        # Calculate derived metrics
        total_urls = successful + failed
        success_rate = (successful / total_urls * 100) if total_urls > 0 else 0.0
        throughput_mbps = (total_bytes / (1024 * 1024) / elapsed) if elapsed > 0 else 0.0
        throughput_imgs = (successful / elapsed) if elapsed > 0 else 0.0

        extra = {}
        if returncode != 0:
            extra["returncode"] = returncode
            extra["stderr"] = stderr[-1000:]
        if stats:
            extra["shard_stats"] = stats

        return BenchmarkResult(
            tool="img2dataset",
            variant="default",
            concurrency=config.thread_count * config.processes_count,
            run_number=run_number,
            input_urls=total_urls,
            successful_downloads=successful,
            failed_downloads=failed,
            success_rate_percent=success_rate,
            total_bytes_downloaded=total_bytes,
            elapsed_seconds=elapsed,
            throughput_mbps=throughput_mbps,
            throughput_imgs_per_sec=throughput_imgs,
            resources=resources,
            extra_metrics=extra,
        )

    def _parse_shard_stats(self, output_path: Path) -> Optional[dict]:
        """Parse img2dataset shard statistics if available."""
        stats_files = list(output_path.rglob("_stats.json"))

        if not stats_files:
            return None

        aggregated = {
            "successes": 0,
            "failed": 0,
            "shards": 0,
        }

        for stats_file in stats_files:
            try:
                with open(stats_file) as f:
                    shard_stats = json.load(f)
                    aggregated["successes"] += shard_stats.get("successes", 0)
                    aggregated["failed"] += shard_stats.get("failed_to_download", 0)
                    aggregated["shards"] += 1
            except (json.JSONDecodeError, IOError):
                continue

        return aggregated if aggregated["shards"] > 0 else None
