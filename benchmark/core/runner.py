"""Main benchmark orchestrator."""

import asyncio
import platform
import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import psutil
import yaml

from .flowdc_adapter import FlowDCAdapter, FlowDCConfig
from .img2dataset_adapter import Img2DatasetAdapter, Img2DatasetConfig
from .metrics import BenchmarkReport, BenchmarkResult


@dataclass
class BenchmarkConfig:
    """Configuration for a benchmark run."""

    # Dataset
    dataset_path: str
    url_column: str = "url"
    label_column: Optional[str] = None

    # Output
    output_base_dir: str = "benchmark/results"
    run_name: Optional[str] = None

    # Tools to benchmark
    flowdc_enabled: bool = True
    flowdc_variants: list[str] = field(
        default_factory=lambda: ["paarc_enabled", "paarc_disabled"]
    )
    img2dataset_enabled: bool = True
    img2dataset_processes: int = 1
    img2dataset_threads: Optional[int] = None  # None = use concurrency_levels / processes

    # Concurrency levels
    concurrency_levels: list[int] = field(default_factory=lambda: [64, 128, 256])

    # Timing
    timeout_sec: int = 30
    warmup_runs: int = 1
    measured_runs: int = 3

    # Resource monitoring
    resource_monitoring: bool = True
    resource_sample_interval: float = 0.5

    # Progress output
    show_progress: bool = False  # Show native tool progress output

    # Reports
    generate_json: bool = True
    generate_html: bool = True

    @classmethod
    def from_yaml(cls, path: str) -> "BenchmarkConfig":
        """Load configuration from YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)

        dataset = data.get("dataset", {})
        tools = data.get("tools", {})
        flowdc = tools.get("flowdc", {})
        img2dataset = tools.get("img2dataset", {})
        resources = data.get("resource_monitoring", {})
        reports = data.get("reports", {})

        return cls(
            dataset_path=dataset.get("path", ""),
            url_column=dataset.get("url_column", "url"),
            label_column=dataset.get("label_column"),
            output_base_dir=data.get("output", {}).get("base_dir", "benchmark/results"),
            run_name=data.get("name"),
            flowdc_enabled=flowdc.get("enabled", True),
            flowdc_variants=flowdc.get("variants", ["paarc_enabled", "paarc_disabled"]),
            img2dataset_enabled=img2dataset.get("enabled", True),
            img2dataset_processes=img2dataset.get("processes", 1),
            img2dataset_threads=img2dataset.get("threads"),
            concurrency_levels=data.get("concurrency_levels", [64, 128, 256]),
            timeout_sec=data.get("timeout_sec", 30),
            warmup_runs=data.get("warmup_runs", 1),
            measured_runs=data.get("measured_runs", 3),
            resource_monitoring=resources.get("enabled", True),
            resource_sample_interval=resources.get("sample_interval_sec", 0.5),
            show_progress=data.get("show_progress", False),
            generate_json=reports.get("json", True),
            generate_html=reports.get("html", True),
        )


class BenchmarkRunner:
    """Orchestrates benchmark execution for FLOW-DC vs img2dataset."""

    def __init__(self, config: BenchmarkConfig, verbose: bool = False):
        """Initialize runner.

        Args:
            config: Benchmark configuration
            verbose: Whether to print verbose output
        """
        self.config = config
        self.verbose = verbose

        # Initialize adapters
        self.flowdc = FlowDCAdapter() if config.flowdc_enabled else None
        self.img2dataset = Img2DatasetAdapter() if config.img2dataset_enabled else None

        # Create output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_name = config.run_name or "benchmark"
        self.output_dir = Path(config.output_base_dir) / f"{run_name}_{timestamp}"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Count URLs in dataset
        self.dataset_urls = self._count_urls()

    def _count_urls(self) -> int:
        """Count URLs in the input dataset."""
        try:
            import polars as pl

            df = pl.read_parquet(self.config.dataset_path)
            return len(df)
        except Exception as e:
            self._log(f"Warning: Could not count URLs: {e}")
            return 0

    def _log(self, message: str) -> None:
        """Print log message if verbose."""
        if self.verbose:
            print(f"[Benchmark] {message}")

    async def run_all(self) -> BenchmarkReport:
        """Run all configured benchmarks.

        Returns:
            Complete benchmark report
        """
        report = BenchmarkReport(
            name=self.config.run_name or "flowdc-vs-img2dataset",
            timestamp=datetime.now().isoformat(),
            dataset_path=self.config.dataset_path,
            dataset_urls=self.dataset_urls,
            platform=platform.system(),
            cpu_count=psutil.cpu_count(),
            memory_gb=psutil.virtual_memory().total / (1024**3),
            python_version=sys.version.split()[0],
            concurrency_levels=self.config.concurrency_levels,
            timeout_sec=self.config.timeout_sec,
            warmup_runs=self.config.warmup_runs,
            measured_runs=self.config.measured_runs,
        )

        # Run FLOW-DC benchmarks
        if self.flowdc and self.config.flowdc_enabled:
            for variant in self.config.flowdc_variants:
                enable_paarc = variant == "paarc_enabled"
                for concurrency in self.config.concurrency_levels:
                    results = await self._run_flowdc(
                        concurrency=concurrency,
                        enable_paarc=enable_paarc,
                    )
                    for result in results:
                        report.add_result(result)

        # Run img2dataset benchmarks
        if self.img2dataset and self.config.img2dataset_enabled:
            for concurrency in self.config.concurrency_levels:
                results = await self._run_img2dataset(concurrency=concurrency)
                for result in results:
                    report.add_result(result)

        # Compute aggregates
        report.compute_all_aggregates()

        return report

    async def _run_flowdc(
        self,
        concurrency: int,
        enable_paarc: bool,
    ) -> list[BenchmarkResult]:
        """Run FLOW-DC benchmark at given concurrency.

        Returns measured runs (excludes warmup).
        """
        variant = "paarc_enabled" if enable_paarc else "paarc_disabled"
        self._log(f"Running FLOW-DC ({variant}) at concurrency {concurrency}")

        results = []
        total_runs = self.config.warmup_runs + self.config.measured_runs

        for run_num in range(total_runs):
            is_warmup = run_num < self.config.warmup_runs
            run_label = "warmup" if is_warmup else f"run {run_num - self.config.warmup_runs + 1}"
            self._log(f"  {run_label}...")

            # Create unique output folder for this run
            output_folder = (
                self.output_dir / f"flowdc_{variant}_c{concurrency}_r{run_num}"
            )

            config = FlowDCConfig(
                input_path=self.config.dataset_path,
                output_folder=str(output_folder),
                url_column=self.config.url_column,
                label_column=self.config.label_column,
                concurrent_downloads=concurrency,
                timeout_sec=self.config.timeout_sec,
                enable_paarc=enable_paarc,
                paarc_c_max=concurrency,
            )

            result = await self.flowdc.run(
                config=config,
                run_number=run_num,
                monitor_resources=self.config.resource_monitoring,
                resource_interval=self.config.resource_sample_interval,
                show_progress=self.config.show_progress,
            )

            # Only keep measured runs
            if not is_warmup:
                results.append(result)
                self._log(
                    f"    Success: {result.success_rate_percent:.1f}%, "
                    f"Throughput: {result.throughput_mbps:.2f} MB/s"
                )

            # Clean up output to save disk space
            if output_folder.exists():
                shutil.rmtree(output_folder)
            overview_path = Path(f"{output_folder}_overview.json")
            if overview_path.exists():
                overview_path.unlink()

        return results

    async def _run_img2dataset(self, concurrency: int) -> list[BenchmarkResult]:
        """Run img2dataset benchmark at given concurrency.

        Returns measured runs (excludes warmup).
        """
        # Calculate thread/process configuration
        processes = self.config.img2dataset_processes
        if self.config.img2dataset_threads is not None:
            # Use explicit threads setting
            threads = self.config.img2dataset_threads
            total_concurrency = processes * threads
        else:
            # Fall back to concurrency / processes
            threads = concurrency // processes
            total_concurrency = concurrency

        self._log(
            f"Running img2dataset at {processes} processes × {threads} threads "
            f"= {total_concurrency} total concurrency"
        )

        results = []
        total_runs = self.config.warmup_runs + self.config.measured_runs

        for run_num in range(total_runs):
            is_warmup = run_num < self.config.warmup_runs
            run_label = "warmup" if is_warmup else f"run {run_num - self.config.warmup_runs + 1}"
            self._log(f"  {run_label}...")

            # Create unique output folder for this run
            output_folder = self.output_dir / f"img2dataset_p{processes}t{threads}_r{run_num}"

            config = Img2DatasetConfig(
                input_path=self.config.dataset_path,
                output_folder=str(output_folder),
                url_column=self.config.url_column,
                thread_count=threads,
                processes_count=processes,
                timeout_sec=self.config.timeout_sec,
                retries=1,
                resize_mode="no",  # Disable resize for fair comparison
            )

            result = await self.img2dataset.run(
                config=config,
                run_number=run_num,
                monitor_resources=self.config.resource_monitoring,
                resource_interval=self.config.resource_sample_interval,
                show_progress=self.config.show_progress,
            )

            # Only keep measured runs
            if not is_warmup:
                results.append(result)
                self._log(
                    f"    Success: {result.success_rate_percent:.1f}%, "
                    f"Throughput: {result.throughput_mbps:.2f} MB/s"
                )

            # Clean up output to save disk space
            if output_folder.exists():
                shutil.rmtree(output_folder)

        return results

    async def run_quick(self) -> BenchmarkReport:
        """Run a quick validation benchmark.

        Uses minimal settings: 1 run, 1 concurrency level.
        """
        # Override config for quick run
        original_warmup = self.config.warmup_runs
        original_measured = self.config.measured_runs
        original_levels = self.config.concurrency_levels

        self.config.warmup_runs = 0
        self.config.measured_runs = 1
        self.config.concurrency_levels = [64]

        try:
            return await self.run_all()
        finally:
            # Restore original config
            self.config.warmup_runs = original_warmup
            self.config.measured_runs = original_measured
            self.config.concurrency_levels = original_levels
