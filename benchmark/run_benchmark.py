#!/usr/bin/env python3
"""
FLOW-DC vs img2dataset Benchmark Suite

Usage:
    python benchmark/run_benchmark.py --config benchmark/configs/benchmark_config.yaml
    python benchmark/run_benchmark.py --quick --dataset files/input/sample.parquet
    python benchmark/run_benchmark.py --tools flowdc --concurrency 256
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from benchmark.core.runner import BenchmarkConfig, BenchmarkRunner
from benchmark.reports.generator import ReportGenerator


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Benchmark FLOW-DC against img2dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run full benchmark with config file
    python benchmark/run_benchmark.py --config benchmark/configs/benchmark_config.yaml

    # Quick validation run
    python benchmark/run_benchmark.py --quick --dataset files/input/sample.parquet

    # Test specific tool at specific concurrency
    python benchmark/run_benchmark.py --tools flowdc --concurrency 256 --dataset files/input/test.parquet

    # Compare only (no warmup, single run)
    python benchmark/run_benchmark.py --dataset files/input/test.parquet --warmup 0 --runs 1
        """,
    )

    parser.add_argument(
        "--config",
        type=Path,
        help="Path to benchmark configuration YAML file",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick validation run (1 run, single concurrency level)",
    )
    parser.add_argument(
        "--tools",
        nargs="+",
        choices=["flowdc", "img2dataset", "all"],
        default=["all"],
        help="Tools to benchmark (default: all)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        nargs="+",
        help="Override concurrency levels to test (used by FLOW-DC, and img2dataset if --img2dataset-threads not set)",
    )
    parser.add_argument(
        "--img2dataset-processes",
        type=int,
        default=1,
        help="Number of processes for img2dataset (default: 1)",
    )
    parser.add_argument(
        "--img2dataset-threads",
        type=int,
        help="Threads per process for img2dataset (default: uses --concurrency / processes)",
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        help="Override dataset path",
    )
    parser.add_argument(
        "--url-column",
        default="url",
        help="Name of URL column in dataset (default: url)",
    )
    parser.add_argument(
        "--label-column",
        help="Name of label column in dataset (optional)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default="benchmark/results",
        help="Output directory for results (default: benchmark/results)",
    )
    parser.add_argument(
        "--name",
        default="benchmark",
        help="Name for this benchmark run",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="Number of warmup runs (default: 1)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of measured runs (default: 3)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Download timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--no-resource-monitor",
        action="store_true",
        help="Disable resource monitoring",
    )
    parser.add_argument(
        "--show-progress",
        action="store_true",
        help="Show native progress output from download tools",
    )
    parser.add_argument(
        "--json-only",
        action="store_true",
        help="Generate JSON report only (skip HTML)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity",
    )

    return parser.parse_args()


def build_config(args: argparse.Namespace) -> BenchmarkConfig:
    """Build BenchmarkConfig from command line arguments."""
    # Load from YAML if provided
    if args.config and args.config.exists():
        config = BenchmarkConfig.from_yaml(str(args.config))
    else:
        config = BenchmarkConfig(
            dataset_path=str(args.dataset) if args.dataset else "",
            url_column=args.url_column,
            label_column=args.label_column,
            output_base_dir=str(args.output_dir),
            run_name=args.name,
        )

    # Override with CLI arguments
    if args.dataset:
        config.dataset_path = str(args.dataset)

    if args.concurrency:
        config.concurrency_levels = args.concurrency

    if args.tools and "all" not in args.tools:
        config.flowdc_enabled = "flowdc" in args.tools
        config.img2dataset_enabled = "img2dataset" in args.tools

    # img2dataset process/thread settings
    config.img2dataset_processes = args.img2dataset_processes
    if args.img2dataset_threads:
        config.img2dataset_threads = args.img2dataset_threads

    config.warmup_runs = args.warmup
    config.measured_runs = args.runs
    config.timeout_sec = args.timeout
    config.resource_monitoring = not args.no_resource_monitor
    config.show_progress = args.show_progress
    config.generate_html = not args.json_only

    if args.name:
        config.run_name = args.name

    if args.output_dir:
        config.output_base_dir = str(args.output_dir)

    return config


async def main() -> int:
    """Main entry point."""
    args = parse_args()
    verbose = args.verbose > 0

    # Validate arguments
    if not args.config and not args.dataset:
        print("Error: Either --config or --dataset is required")
        return 1

    # Build configuration
    config = build_config(args)

    if not config.dataset_path:
        print("Error: No dataset specified")
        return 1

    if not Path(config.dataset_path).exists():
        print(f"Error: Dataset not found: {config.dataset_path}")
        return 1

    # Create runner
    runner = BenchmarkRunner(config, verbose=verbose)

    print(f"Starting benchmark: {config.run_name}")
    print(f"  Dataset: {config.dataset_path} ({runner.dataset_urls} URLs)")
    print(f"  Concurrency levels: {config.concurrency_levels}")
    print(f"  Tools: ", end="")
    tools = []
    if config.flowdc_enabled:
        tools.append(f"FLOW-DC ({', '.join(config.flowdc_variants)})")
    if config.img2dataset_enabled:
        tools.append("img2dataset")
    print(", ".join(tools))
    print(f"  Runs: {config.warmup_runs} warmup + {config.measured_runs} measured")
    print()

    # Run benchmark
    if args.quick:
        print("Running quick validation...")
        report = await runner.run_quick()
    else:
        report = await runner.run_all()

    # Generate reports
    print("\nGenerating reports...")
    generator = ReportGenerator(runner.output_dir)

    json_path = generator.generate_json(report)
    print(f"  JSON: {json_path}")

    if config.generate_html:
        html_path = generator.generate_html(report)
        print(f"  HTML: {html_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for tool_key, concurrency_results in report.results.items():
        print(f"\n{tool_key.replace('_', ' ').title()}:")
        for conc, agg in sorted(concurrency_results.items()):
            print(
                f"  C={conc}: "
                f"{agg.avg_throughput_mbps:.2f} MB/s, "
                f"{agg.avg_success_rate:.1f}% success, "
                f"{agg.avg_elapsed_seconds:.1f}s"
            )

    print(f"\nResults saved to: {runner.output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
