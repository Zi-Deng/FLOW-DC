"""Adapter for running FLOW-DC benchmarks."""

import asyncio
import json
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .metrics import BenchmarkResult, ResourceMetrics
from .resource_monitor import ProcessResourceMonitor


@dataclass
class FlowDCConfig:
    """Configuration for a FLOW-DC benchmark run."""

    input_path: str
    output_folder: str
    url_column: str
    label_column: Optional[str]
    concurrent_downloads: int
    timeout_sec: int
    enable_paarc: bool
    paarc_c_init: int = 8
    paarc_c_min: int = 2
    paarc_c_max: Optional[int] = None
    paarc_mu: float = 0.85
    paarc_theta_50: float = 3.0
    paarc_theta_95: float = 4.0
    max_retry_attempts: int = 1


class FlowDCAdapter:
    """Adapter for running FLOW-DC download_batch.py benchmarks."""

    def __init__(self, project_root: Optional[Path] = None):
        """Initialize adapter.

        Args:
            project_root: Root directory of FLOW-DC project.
                         Defaults to parent of benchmark folder.
        """
        if project_root is None:
            # Assume we're in benchmark/core/
            project_root = Path(__file__).parent.parent.parent
        self.project_root = Path(project_root)
        self.bin_path = self.project_root / "bin" / "download_batch.py"

        if not self.bin_path.exists():
            raise FileNotFoundError(f"FLOW-DC script not found: {self.bin_path}")

    def generate_config(self, config: FlowDCConfig) -> Path:
        """Generate a FLOW-DC config JSON file.

        Args:
            config: Configuration parameters

        Returns:
            Path to generated config file
        """
        c_max = config.paarc_c_max or config.concurrent_downloads

        config_dict = {
            "input": str(config.input_path),
            "input_format": "parquet",
            "output": str(config.output_folder),
            "output_format": "imagefolder",
            "url": config.url_column,
            "label": config.label_column,
            "concurrent_downloads": config.concurrent_downloads,
            "timeout": config.timeout_sec,
            "enable_paarc": config.enable_paarc,
            "C_init": config.paarc_c_init,
            "C_min": config.paarc_c_min,
            "C_max": c_max,
            "mu": config.paarc_mu,
            "theta_50": config.paarc_theta_50,
            "theta_95": config.paarc_theta_95,
            "max_retry_attempts": config.max_retry_attempts,
            "create_tar": False,
            "create_overview": True,
            "force_overwrite": True,  # The adapter owns and recreates this output directory.
        }

        # Create temp config file
        config_path = Path(tempfile.mktemp(suffix=".json", prefix="flowdc_config_"))
        with open(config_path, "w") as f:
            json.dump(config_dict, f, indent=2)

        return config_path

    async def run(
        self,
        config: FlowDCConfig,
        run_number: int,
        monitor_resources: bool = True,
        resource_interval: float = 0.5,
        show_progress: bool = False,
    ) -> BenchmarkResult:
        """Execute FLOW-DC and collect results.

        Args:
            config: Run configuration
            run_number: Run number for identification
            monitor_resources: Whether to collect resource metrics
            resource_interval: Seconds between resource samples

        Returns:
            BenchmarkResult with metrics
        """
        config_path = self.generate_config(config)

        try:
            # Ensure output directory is clean
            output_path = Path(config.output_folder)
            if output_path.exists():
                shutil.rmtree(output_path)
            output_path.mkdir(parents=True, exist_ok=True)

            # Start resource monitor
            monitor = None
            if monitor_resources:
                monitor = ProcessResourceMonitor(sample_interval=resource_interval)
                monitor.start()

            # Run FLOW-DC
            cmd = [sys.executable, str(self.bin_path), "--config", str(config_path)]

            if show_progress:
                # Pass through output to terminal
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    cwd=str(self.project_root),
                )
                if monitor:
                    monitor.set_pid(proc.pid)
                await proc.wait()
                stdout, stderr = b"", b""
            else:
                # Capture output
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=str(self.project_root),
                )
                if monitor:
                    monitor.set_pid(proc.pid)
                stdout, stderr = await proc.communicate()

            # Stop resource monitor
            resources = ResourceMetrics()
            if monitor:
                resources = monitor.stop()

            # Parse results
            overview_path = Path(f"{config.output_folder}_overview.json")
            result = self._parse_overview(
                overview_path=overview_path,
                config=config,
                run_number=run_number,
                resources=resources,
            )

            # Add stdout/stderr to extra metrics if there were issues
            if proc.returncode != 0:
                result.extra_metrics["returncode"] = proc.returncode
                result.extra_metrics["stderr"] = stderr.decode()[-1000:]

            return result

        finally:
            # Cleanup temp config
            if config_path.exists():
                config_path.unlink()

    def _parse_overview(
        self,
        overview_path: Path,
        config: FlowDCConfig,
        run_number: int,
        resources: ResourceMetrics,
    ) -> BenchmarkResult:
        """Parse FLOW-DC overview JSON into BenchmarkResult."""
        variant = "paarc_enabled" if config.enable_paarc else "paarc_disabled"

        if not overview_path.exists():
            # Return empty result if overview not found
            return BenchmarkResult(
                tool="flowdc",
                variant=variant,
                concurrency=config.concurrent_downloads,
                run_number=run_number,
                resources=resources,
                extra_metrics={"error": "Overview file not found"},
            )

        with open(overview_path) as f:
            overview = json.load(f)

        summary = overview.get("summary", {})
        inputs = overview.get("script_inputs", {})

        total_urls = summary.get("total_urls", 0)
        successful = summary.get("successful_downloads", 0)
        failed = summary.get("failed_downloads", 0)
        elapsed = summary.get("elapsed_sec", 0.0)
        downloaded_mb = summary.get("downloaded_mb", 0.0)

        # Calculate derived metrics
        success_rate = (successful / total_urls * 100) if total_urls > 0 else 0.0
        throughput_mbps = (downloaded_mb / elapsed) if elapsed > 0 else 0.0
        throughput_imgs = (successful / elapsed) if elapsed > 0 else 0.0

        # Parse error breakdown
        error_counts = {}
        for err in overview.get("error_breakdown", []):
            key = f"{err.get('status_code', 'unknown')}_{err.get('error', 'unknown')}"
            error_counts[key] = err.get("count", 0)

        return BenchmarkResult(
            tool="flowdc",
            variant=variant,
            concurrency=config.concurrent_downloads,
            run_number=run_number,
            input_urls=total_urls,
            successful_downloads=successful,
            failed_downloads=failed,
            success_rate_percent=success_rate,
            total_bytes_downloaded=int(downloaded_mb * 1024 * 1024),
            elapsed_seconds=elapsed,
            throughput_mbps=throughput_mbps,
            throughput_imgs_per_sec=throughput_imgs,
            error_counts=error_counts,
            resources=resources,
            extra_metrics={
                "paarc_version": overview.get("paarc_version"),
                "paarc_config": inputs.get("paarc_config"),
            },
        )
