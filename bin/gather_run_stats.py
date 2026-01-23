#!/usr/bin/env python3
"""
FLOW-DC Run Statistics Collector

Gathers comprehensive download statistics from completed TaskVine runs by:
1. Parsing TaskVine manager logs for system-level metrics
2. Extracting per-partition statistics from tar file overview.json files

Author: FLOW-DC Team
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TaskVineStats:
    """Statistics extracted from TaskVine performance log."""
    run_start: datetime
    run_end: datetime
    total_wall_time_sec: float
    tasks_completed: int
    tasks_failed: int
    peak_workers: int
    bytes_received: int

    @property
    def total_wall_time_human(self) -> str:
        """Human-readable duration string."""
        secs = int(self.total_wall_time_sec)
        hours, remainder = divmod(secs, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    @property
    def bytes_received_TB(self) -> float:
        """Total bytes received in terabytes."""
        return self.bytes_received / (1024 ** 4)


@dataclass
class PartitionStats:
    """Statistics from a single partition's overview.json."""
    partition_name: str
    total_urls: int
    successful_downloads: int
    failed_downloads: int
    downloaded_mb: float
    elapsed_sec: float
    avg_speed_MBps: float
    success_rate_percent: float
    errors: list[dict] = field(default_factory=list)


@dataclass
class AggregatedStats:
    """Aggregated statistics across all partitions."""
    total_urls: int = 0
    successful_downloads: int = 0
    failed_downloads: int = 0
    total_downloaded_mb: float = 0.0
    error_counts: Counter = field(default_factory=Counter)
    partition_stats: list[PartitionStats] = field(default_factory=list)

    @property
    def success_rate_percent(self) -> float:
        if self.total_urls == 0:
            return 0.0
        return (self.successful_downloads / self.total_urls) * 100

    @property
    def total_downloaded_TB(self) -> float:
        return self.total_downloaded_mb / (1024 ** 2)


# =============================================================================
# TASKVINE LOG PARSING
# =============================================================================

def parse_performance_log(log_path: Path) -> TaskVineStats:
    """
    Parse TaskVine performance log to extract run statistics.

    The performance log has space-separated columns with the header:
    timestamp workers_connected ... tasks_done tasks_failed ... bytes_received ...
    """
    with open(log_path, 'r') as f:
        lines = f.readlines()

    if len(lines) < 2:
        raise ValueError(f"Performance log has insufficient data: {log_path}")

    # Parse header to get column indices
    header = lines[0].strip().lstrip('# ').split()
    col_idx = {name: i for i, name in enumerate(header)}

    # Required columns
    required = ['timestamp', 'workers_connected', 'tasks_done', 'tasks_failed', 'bytes_received']
    for col in required:
        if col not in col_idx:
            raise ValueError(f"Missing required column '{col}' in performance log")

    # Parse first and last data lines for timestamps
    first_data = lines[1].strip().split()
    last_data = lines[-1].strip().split()

    # Timestamps are in microseconds since epoch
    start_ts_us = int(first_data[col_idx['timestamp']])
    end_ts_us = int(last_data[col_idx['timestamp']])

    run_start = datetime.fromtimestamp(start_ts_us / 1_000_000)
    run_end = datetime.fromtimestamp(end_ts_us / 1_000_000)
    total_wall_time_sec = (end_ts_us - start_ts_us) / 1_000_000

    # Find peak workers by scanning all lines
    peak_workers = 0
    for line in lines[1:]:
        parts = line.strip().split()
        if len(parts) > col_idx['workers_connected']:
            workers = int(parts[col_idx['workers_connected']])
            peak_workers = max(peak_workers, workers)

    # Get final values from last line
    tasks_done = int(last_data[col_idx['tasks_done']])
    tasks_failed = int(last_data[col_idx['tasks_failed']])
    bytes_received = int(last_data[col_idx['bytes_received']])

    return TaskVineStats(
        run_start=run_start,
        run_end=run_end,
        total_wall_time_sec=total_wall_time_sec,
        tasks_completed=tasks_done,
        tasks_failed=tasks_failed,
        peak_workers=peak_workers,
        bytes_received=bytes_received
    )


# =============================================================================
# TAR FILE PROCESSING
# =============================================================================

def extract_overview_from_tar(tar_path: Path) -> Optional[PartitionStats]:
    """
    Extract overview.json from a tar file and return partition statistics.

    Uses the system tar command for fast extraction (avoids sequential scan
    through large tar archives that Python's tarfile module performs).

    The overview.json is located at: <partition_name>/overview.json
    """
    partition_name = tar_path.stem  # e.g., "output_partition_001"
    overview_path = f"{partition_name}/overview.json"

    try:
        # Use tar command to extract directly to stdout - much faster than Python tarfile
        # Increased timeout to handle I/O contention when running in parallel
        result = subprocess.run(
            ["tar", "-xf", str(tar_path), "-O", overview_path],
            capture_output=True,
            timeout=300
        )

        if result.returncode != 0:
            stderr = result.stderr.decode('utf-8', errors='replace')
            if "Not found in archive" in stderr or "not found" in stderr.lower():
                print(f"Warning: {overview_path} not found in {tar_path.name}")
            else:
                print(f"Warning: tar extraction failed for {tar_path.name}: {stderr}")
            return None

        data = json.loads(result.stdout)
        summary = data.get('summary', {})

        return PartitionStats(
            partition_name=partition_name,
            total_urls=summary.get('total_urls', 0),
            successful_downloads=summary.get('successful_downloads', 0),
            failed_downloads=summary.get('failed_downloads', 0),
            downloaded_mb=summary.get('downloaded_mb', 0.0),
            elapsed_sec=summary.get('elapsed_sec', 0.0),
            avg_speed_MBps=summary.get('avg_speed_MBps', 0.0),
            success_rate_percent=summary.get('success_rate_percent', 0.0),
            errors=data.get('error_breakdown', [])
        )
    except subprocess.TimeoutExpired:
        print(f"Error: Timeout extracting from {tar_path.name}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {tar_path.name}: {e}")
        return None
    except Exception as e:
        print(f"Error processing {tar_path.name}: {e}")
        return None


def extract_all_overviews(output_dir: Path, max_workers: int = 16, verbose: bool = True) -> list[PartitionStats]:
    """
    Extract overview.json from all tar files in the output directory.
    Uses parallel processing for efficiency.
    """
    tar_files = sorted(output_dir.glob("*.tar"))

    if not tar_files:
        raise ValueError(f"No tar files found in {output_dir}")

    total = len(tar_files)
    print(f"Extracting statistics from {total} tar files using {max_workers} workers...")

    results = []
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(extract_overview_from_tar, tf): tf for tf in tar_files}

        for future in as_completed(futures):
            completed += 1
            result = future.result()
            if result is not None:
                results.append(result)

            # Progress update every 10 files or at completion
            if verbose and (completed % 10 == 0 or completed == total):
                print(f"  Progress: {completed}/{total} tar files processed ({len(results)} successful)")
                sys.stdout.flush()

    # Sort by partition name for consistent ordering
    results.sort(key=lambda x: x.partition_name)

    print(f"Successfully extracted {len(results)} partition statistics")
    return results


# =============================================================================
# AGGREGATION
# =============================================================================

def aggregate_partition_stats(partitions: list[PartitionStats]) -> AggregatedStats:
    """Aggregate statistics from all partitions."""
    agg = AggregatedStats()
    agg.partition_stats = partitions

    for p in partitions:
        agg.total_urls += p.total_urls
        agg.successful_downloads += p.successful_downloads
        agg.failed_downloads += p.failed_downloads
        agg.total_downloaded_mb += p.downloaded_mb

        # Aggregate errors by (status_code, error_message)
        for err in p.errors:
            key = (err.get('status_code', 'unknown'), err.get('error', 'Unknown error'))
            agg.error_counts[key] += err.get('count', 0)

    return agg


# =============================================================================
# REPORT GENERATION
# =============================================================================

def generate_json_report(
    vine_stats: TaskVineStats,
    agg_stats: AggregatedStats,
    vine_run_dir: Path,
    output_dir: Path,
    config_file: Optional[str] = None
) -> dict:
    """Generate a JSON report structure."""

    # Calculate effective throughput
    effective_throughput_MBps = 0.0
    if vine_stats.total_wall_time_sec > 0:
        effective_throughput_MBps = agg_stats.total_downloaded_mb / vine_stats.total_wall_time_sec

    # Partition performance statistics
    success_rates = [p.success_rate_percent for p in agg_stats.partition_stats]
    throughputs = [p.avg_speed_MBps for p in agg_stats.partition_stats]

    def safe_stats(values: list[float]) -> dict:
        if not values:
            return {"min": 0, "max": 0, "avg": 0}
        return {
            "min": round(min(values), 2),
            "max": round(max(values), 2),
            "avg": round(sum(values) / len(values), 2)
        }

    # Error breakdown list
    error_breakdown = []
    for (status_code, error_msg), count in sorted(agg_stats.error_counts.items(), key=lambda x: -x[1]):
        error_breakdown.append({
            "status_code": status_code,
            "error": error_msg,
            "count": count
        })

    report = {
        "run_info": {
            "run_directory": str(vine_run_dir),
            "run_date": vine_stats.run_start.strftime("%Y-%m-%d"),
            "config_file": config_file,
            "output_directory": str(output_dir)
        },
        "taskvine_stats": {
            "run_start": vine_stats.run_start.strftime("%Y-%m-%d %H:%M:%S"),
            "run_end": vine_stats.run_end.strftime("%Y-%m-%d %H:%M:%S"),
            "total_wall_time_sec": round(vine_stats.total_wall_time_sec, 2),
            "total_wall_time_human": vine_stats.total_wall_time_human,
            "tasks_completed": vine_stats.tasks_completed,
            "tasks_failed": vine_stats.tasks_failed,
            "peak_workers": vine_stats.peak_workers,
            "bytes_received_TB": round(vine_stats.bytes_received_TB, 2)
        },
        "download_stats": {
            "total_urls": agg_stats.total_urls,
            "successful_downloads": agg_stats.successful_downloads,
            "failed_downloads": agg_stats.failed_downloads,
            "success_rate_percent": round(agg_stats.success_rate_percent, 2),
            "total_downloaded_TB": round(agg_stats.total_downloaded_TB, 2),
            "effective_throughput_MBps": round(effective_throughput_MBps, 2)
        },
        "error_breakdown": error_breakdown,
        "partition_stats": {
            "count": len(agg_stats.partition_stats),
            "success_rate": safe_stats(success_rates),
            "throughput_MBps": safe_stats(throughputs)
        }
    }

    return report


def print_console_report(report: dict) -> None:
    """Print a human-readable report to the console."""

    sep = "=" * 80

    print(f"\n{sep}")
    print("BIOTROVE-TRAIN DOWNLOAD STATISTICS REPORT")
    print(f"{sep}\n")

    # TaskVine run summary
    ts = report["taskvine_stats"]
    ri = report["run_info"]
    print("TASKVINE RUN SUMMARY")
    print(f"  Run directory:     {ri['run_directory']}")
    if ri.get('config_file'):
        print(f"  Config file:       {ri['config_file']}")
    print(f"  Duration:          {ts['total_wall_time_human']}")
    print(f"  Tasks completed:   {ts['tasks_completed']} / {ts['tasks_completed'] + ts['tasks_failed']} (100%)")
    print(f"  Peak workers:      {ts['peak_workers']}")
    print(f"  Total transferred: {ts['bytes_received_TB']:.2f} TB")
    print()

    # Download statistics
    ds = report["download_stats"]
    print("DOWNLOAD STATISTICS")
    print(f"  Total URLs:        {ds['total_urls']:,}")
    print(f"  Successful:        {ds['successful_downloads']:,} ({ds['success_rate_percent']:.2f}%)")
    print(f"  Failed:            {ds['failed_downloads']:,} ({100 - ds['success_rate_percent']:.2f}%)")
    print(f"  Total downloaded:  {ds['total_downloaded_TB']:.2f} TB")
    print(f"  Throughput:        {ds['effective_throughput_MBps']:.2f} MB/s")
    print()

    # Error breakdown
    if report["error_breakdown"]:
        print("ERROR BREAKDOWN")
        for err in report["error_breakdown"]:
            print(f"  {err['error']:<40} {err['count']:,}")
        print()

    # Partition performance
    ps = report["partition_stats"]
    print("PARTITION PERFORMANCE")
    print(f"  Partitions:        {ps['count']}")
    sr = ps["success_rate"]
    print(f"  Success rate:      min={sr['min']:.2f}%, max={sr['max']:.2f}%, avg={sr['avg']:.2f}%")
    tp = ps["throughput_MBps"]
    print(f"  Throughput:        min={tp['min']:.1f} MB/s, max={tp['max']:.1f} MB/s, avg={tp['avg']:.1f} MB/s")

    print(f"\n{sep}\n")


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Gather download statistics from a completed TaskVine run",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--vine-run",
        type=Path,
        required=True,
        help="Path to TaskVine run directory (e.g., vine-run-info/2026-01-23T035411)"
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Path to output directory containing tar files"
    )

    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Path to output JSON report file (optional)"
    )

    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Name of config file used for this run (for documentation)"
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Number of parallel workers for tar extraction (default: 16)"
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress console output (only write JSON report)"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Validate paths
    vine_run_dir = args.vine_run
    if not vine_run_dir.exists():
        print(f"Error: TaskVine run directory not found: {vine_run_dir}")
        sys.exit(1)

    performance_log = vine_run_dir / "vine-logs" / "performance"
    if not performance_log.exists():
        print(f"Error: Performance log not found: {performance_log}")
        sys.exit(1)

    output_dir = args.output_dir
    if not output_dir.exists():
        print(f"Error: Output directory not found: {output_dir}")
        sys.exit(1)

    # Parse TaskVine performance log
    print(f"Parsing TaskVine performance log: {performance_log}")
    vine_stats = parse_performance_log(performance_log)

    # Extract overview.json from all tar files
    partition_stats = extract_all_overviews(output_dir, max_workers=args.workers)

    # Aggregate statistics
    print("Aggregating statistics...")
    agg_stats = aggregate_partition_stats(partition_stats)

    # Generate report
    report = generate_json_report(
        vine_stats=vine_stats,
        agg_stats=agg_stats,
        vine_run_dir=vine_run_dir,
        output_dir=output_dir,
        config_file=args.config
    )

    # Output results
    if not args.quiet:
        print_console_report(report)

    if args.report:
        with open(args.report, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"JSON report written to: {args.report}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
