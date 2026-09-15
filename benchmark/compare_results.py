#!/usr/bin/env python3
"""
Compare multiple benchmark results.

Usage:
    python benchmark/compare_results.py benchmark/results/run1/report.json benchmark/results/run2/report.json
"""

import argparse
import json
import sys
from pathlib import Path


def load_report(path: Path) -> dict:
    """Load a benchmark report JSON file."""
    with open(path) as f:
        return json.load(f)


def compare_reports(reports: list[tuple[str, dict]]) -> None:
    """Print comparison of multiple benchmark reports."""
    print("\n" + "=" * 80)
    print("BENCHMARK COMPARISON")
    print("=" * 80)

    # Print metadata for each report
    print("\nReports:")
    for name, report in reports:
        meta = report["metadata"]
        print(f"  {name}:")
        print(f"    Date: {meta['timestamp'][:10]}")
        print(f"    Dataset: {meta['dataset']['path']} ({meta['dataset']['total_urls']} URLs)")
        print(f"    System: {meta['system_info']['platform']}, {meta['system_info']['cpu_count']} cores")

    # Collect all tool keys and concurrency levels
    all_tool_keys = set()
    all_concurrencies = set()
    for _, report in reports:
        for tool_key, conc_results in report["results"].items():
            all_tool_keys.add(tool_key)
            for conc in conc_results.keys():
                all_concurrencies.add(int(conc))

    all_concurrencies = sorted(all_concurrencies)

    # Compare throughput for each tool/concurrency
    print("\n" + "-" * 80)
    print("THROUGHPUT COMPARISON (MB/s)")
    print("-" * 80)

    for tool_key in sorted(all_tool_keys):
        print(f"\n{tool_key.replace('_', ' ').title()}:")
        header = "  Concurrency | " + " | ".join(f"{name:>12}" for name, _ in reports)
        print(header)
        print("  " + "-" * (len(header) - 2))

        for conc in all_concurrencies:
            row = f"  {conc:>11} |"
            for name, report in reports:
                results = report["results"].get(tool_key, {})
                conc_data = results.get(str(conc), {})
                throughput = conc_data.get("avg_throughput_mbps", 0)
                row += f" {throughput:>12.2f} |"
            print(row)

    # Compare success rates
    print("\n" + "-" * 80)
    print("SUCCESS RATE COMPARISON (%)")
    print("-" * 80)

    for tool_key in sorted(all_tool_keys):
        print(f"\n{tool_key.replace('_', ' ').title()}:")
        header = "  Concurrency | " + " | ".join(f"{name:>12}" for name, _ in reports)
        print(header)
        print("  " + "-" * (len(header) - 2))

        for conc in all_concurrencies:
            row = f"  {conc:>11} |"
            for name, report in reports:
                results = report["results"].get(tool_key, {})
                conc_data = results.get(str(conc), {})
                success = conc_data.get("avg_success_rate", 0)
                row += f" {success:>12.1f} |"
            print(row)

    # Summary: best throughput per concurrency
    print("\n" + "-" * 80)
    print("BEST THROUGHPUT BY CONCURRENCY")
    print("-" * 80)

    for conc in all_concurrencies:
        best_throughput = 0
        best_config = ""
        for name, report in reports:
            for tool_key, conc_results in report["results"].items():
                conc_data = conc_results.get(str(conc), {})
                throughput = conc_data.get("avg_throughput_mbps", 0)
                if throughput > best_throughput:
                    best_throughput = throughput
                    best_config = f"{name} / {tool_key}"

        print(f"  C={conc}: {best_throughput:.2f} MB/s ({best_config})")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Compare multiple benchmark results"
    )
    parser.add_argument(
        "reports",
        nargs="+",
        type=Path,
        help="Paths to benchmark report JSON files",
    )
    parser.add_argument(
        "--names",
        nargs="+",
        help="Names for each report (default: filenames)",
    )

    args = parser.parse_args()

    # Validate files exist
    for path in args.reports:
        if not path.exists():
            print(f"Error: Report not found: {path}")
            return 1

    # Load reports
    reports = []
    names = args.names or [p.parent.name for p in args.reports]

    if len(names) != len(args.reports):
        print("Error: Number of names must match number of reports")
        return 1

    for name, path in zip(names, args.reports):
        try:
            report = load_report(path)
            reports.append((name, report))
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in {path}: {e}")
            return 1

    # Compare
    compare_reports(reports)

    return 0


if __name__ == "__main__":
    sys.exit(main())
