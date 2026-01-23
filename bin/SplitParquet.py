#!/usr/bin/env python
"""
FLOW-DC Dataset Partitioning Script

Splits dataset manifests (parquet files) into groups for distributed downloading.
Supports multiple grouping strategies:
- simple: Equal-sized random partitions
- greedy: Balance partitions by a grouping column (e.g., class labels)
- host: Extract host from URLs and group by host for optimal rate limiting

The host-based grouping is recommended for distributed downloads as it ensures
workers can maximize throughput by dealing with fewer unique hosts per partition.

Uses Polars for high-performance processing of large datasets (40M+ rows).
"""

import argparse
import polars as pl
import numpy as np
import os
import sys
import json
from math import ceil
from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import urlparse


class GroupingMethod(Enum):
    """Enumeration of available grouping methods"""
    GREEDY = "greedy"
    SIMPLE = "simple"
    HOST = "host"  # New: Group by URL host for distributed downloading


@dataclass
class FilterArguments:
    """Configuration for dataset partitioning"""
    parquet: str = field(
        default=None,
        metadata={"help": "Path to input parquet file"}
    )
    grouping_col: str = field(
        default=None,
        metadata={"help": "Column to group on (required for greedy method)"}
    )
    url_col: str = field(
        default=None,
        metadata={"help": "URL column name"}
    )
    groups: int = field(
        default=None,
        metadata={"help": "Number of groups/partitions to create"}
    )
    output_folder: str = field(
        default=None,
        metadata={"help": "Output folder for partition files"}
    )
    method: str = field(
        default="host",
        metadata={"help": "Grouping method: 'simple', 'greedy', or 'host'"}
    )
    output_format: str = field(
        default="parquet",
        metadata={"help": "Output format: 'parquet' or 'csv'"}
    )
    add_host_column: bool = field(
        default=True,
        metadata={"help": "Add extracted host column to output (useful for PolicyBBR)"}
    )


def extract_host_from_url(url: str) -> str:
    """
    Extract the host/domain from a URL.

    Args:
        url: Full URL string

    Returns:
        Host/domain string (e.g., 'api.gbif.org')
    """
    if url is None or not str(url).strip():
        return "unknown"
    try:
        parsed = urlparse(str(url).strip())
        host = parsed.netloc.lower()
        return host if host else "unknown"
    except Exception:
        return "unknown"


def load_json_config(config_path: str) -> FilterArguments:
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in configuration file '{config_path}': {e}")
        sys.exit(1)

    # Required fields
    required_fields = ['parquet', 'url_col', 'groups', 'output_folder']
    for field in required_fields:
        if field not in config_data:
            print(f"Error: Required field '{field}' not found in JSON configuration.")
            sys.exit(1)

    # Defaults
    defaults = {
        'method': 'host',
        'output_format': 'parquet',
        'grouping_col': None,
        'add_host_column': True
    }
    for field, default_value in defaults.items():
        if field not in config_data:
            config_data[field] = default_value

    # Method-specific validation
    method = config_data.get('method', 'host')
    if method == 'greedy' and not config_data.get('grouping_col'):
        print("Error: 'grouping_col' is required for greedy method.")
        sys.exit(1)

    # Validate method
    valid_methods = [m.value for m in GroupingMethod]
    if method not in valid_methods:
        print(f"Error: Method must be one of: {valid_methods}")
        sys.exit(1)

    return FilterArguments(**config_data)


def parse_args() -> FilterArguments:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="FLOW-DC Dataset Partitioning - Split manifests for distributed downloading",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Host-based grouping (recommended for distributed downloads):
  python SplitParquet.py --parquet data.parquet --url_col photo_url --groups 10 --output_folder partitions --method host

  # Simple equal partitioning:
  python SplitParquet.py --parquet data.parquet --url_col photo_url --groups 5 --output_folder partitions --method simple

  # Greedy grouping by class label:
  python SplitParquet.py --parquet data.parquet --url_col photo_url --grouping_col species --groups 5 --output_folder partitions --method greedy

  # Using JSON configuration:
  python SplitParquet.py --config partition_config.json
"""
    )

    parser.add_argument('--config', type=str, help="Path to JSON configuration file")
    parser.add_argument('--parquet', type=str, help="Input parquet file path")
    parser.add_argument('--grouping_col', type=str, help="Column for greedy grouping")
    parser.add_argument('--url_col', type=str, help="URL column name")
    parser.add_argument('--groups', type=int, help="Number of partitions to create")
    parser.add_argument('--output_folder', type=str, help="Output folder for partitions")
    parser.add_argument(
        '--method',
        type=str,
        choices=[m.value for m in GroupingMethod],
        default='host',
        help="Grouping method (default: host)"
    )
    parser.add_argument(
        '--output_format',
        type=str,
        choices=['parquet', 'csv'],
        default='parquet',
        help="Output file format (default: parquet)"
    )
    parser.add_argument(
        '--add_host_column',
        action='store_true',
        default=True,
        help="Add extracted host column to output"
    )
    parser.add_argument(
        '--no_host_column',
        action='store_true',
        help="Do not add host column to output"
    )

    args = parser.parse_args()

    if args.config:
        return load_json_config(args.config)

    # Validate required args
    required = ['parquet', 'url_col', 'groups', 'output_folder']
    missing = [arg for arg in required if getattr(args, arg) is None]
    if missing:
        parser.error(f"Required arguments: {', '.join('--' + a for a in missing)}")

    if args.method == 'greedy' and args.grouping_col is None:
        parser.error("--grouping_col is required for greedy method")

    return FilterArguments(
        parquet=args.parquet,
        grouping_col=args.grouping_col,
        url_col=args.url_col,
        groups=args.groups,
        output_folder=args.output_folder,
        method=args.method,
        output_format=args.output_format,
        add_host_column=not args.no_host_column
    )


def greedy_grouping(num_partitions: int, df: pl.DataFrame, count_col: str, name_col: str) -> pl.DataFrame:
    """
    Greedy bin-packing algorithm to balance partitions by count.

    Assigns items to the partition with the smallest current sum,
    resulting in balanced partition sizes.
    """
    sorted_df = df.sort(count_col, descending=True)

    partitions = [[] for _ in range(num_partitions)]
    partition_sums = [0 for _ in range(num_partitions)]

    # Convert to rows for iteration (this is the slow part, but unavoidable for greedy)
    for row in sorted_df.iter_rows(named=True):
        min_idx = int(np.argmin(partition_sums))
        partitions[min_idx].append(row)
        partition_sums[min_idx] += row[count_col]

    new_rows = []
    for group_id, partition in enumerate(partitions, 1):
        for row in partition:
            new_rows.append({name_col: row[name_col], count_col: row[count_col], "group": group_id})

    return pl.DataFrame(new_rows)


def simple_grouping(df: pl.DataFrame, num_partitions: int) -> pl.DataFrame:
    """Simple equal-sized partitioning by row count using vectorized operations."""
    total_rows = df.height
    rows_per_group = total_rows // num_partitions
    remainder = total_rows % num_partitions

    # Build group assignments using vectorized operations
    # First 'remainder' groups get (rows_per_group + 1) rows each
    # Remaining groups get rows_per_group rows each
    group_assignments = []
    for group_id in range(1, num_partitions + 1):
        group_size = rows_per_group + (1 if group_id <= remainder else 0)
        group_assignments.extend([group_id] * group_size)

    # Add group column using polars
    return df.with_columns(pl.Series("group", group_assignments))


def host_grouping(df: pl.DataFrame, url_col: str, num_partitions: int) -> pl.DataFrame:
    """
    Group URLs by their host/domain for optimal distributed downloading.

    This method:
    1. Extracts the host from each URL
    2. Counts URLs per host
    3. Uses greedy bin-packing to assign hosts to partitions
    4. Ensures URLs from the same host are in the same partition

    This is optimal for distributed downloading because:
    - Each worker deals with fewer unique hosts
    - PolicyBBR rate limiting works per-host
    - Reduces cross-worker interference on shared hosts
    """
    # Extract host from URLs using polars expression
    print("  Extracting hosts from URLs...")
    df_with_host = df.with_columns(
        pl.col(url_col).map_elements(extract_host_from_url, return_dtype=pl.Utf8).alias("_host")
    )

    # Count URLs per host
    host_counts = (
        df_with_host
        .group_by("_host")
        .agg(pl.len().alias("_count"))
        .sort("_count", descending=True)
    )

    print(f"  Found {host_counts.height} unique hosts")
    print(f"  Top 5 hosts by URL count:")
    for row in host_counts.head(5).iter_rows(named=True):
        print(f"    {row['_host']}: {row['_count']} URLs")

    # Use greedy grouping to assign hosts to partitions
    host_groups = greedy_grouping(num_partitions, host_counts, '_count', '_host')

    # Merge group assignments back to original dataframe
    df_result = df_with_host.join(
        host_groups.select(["_host", "group"]),
        on="_host",
        how="left"
    )

    return df_result


def partition_df_legacy(df: pl.DataFrame, num_partitions: int, taxon_col: str) -> pl.DataFrame:
    """Legacy partitioning by taxon column (kept for backwards compatibility)."""
    partition_size = ceil(df.height / num_partitions)
    sorted_df = df.sort(taxon_col)

    # Add group_num column
    group_nums = [(i // partition_size) + 1 for i in range(sorted_df.height)]
    sorted_df = sorted_df.with_columns(pl.Series("group_num", group_nums))

    # Modify taxon column
    sorted_df = sorted_df.with_columns(
        (pl.col(taxon_col) + "_" + pl.col("group_num").cast(pl.Utf8)).alias(taxon_col)
    )

    return sorted_df.drop("group_num")


def save_partition(df: pl.DataFrame, group_id: int, output_folder: str, output_format: str) -> str:
    """Save a partition to file."""
    filename = f"partition_{group_id:03d}"

    if output_format == 'parquet':
        filepath = os.path.join(output_folder, f"{filename}.parquet")
        df.write_parquet(filepath)
    else:
        filepath = os.path.join(output_folder, f"{filename}.csv")
        df.write_csv(filepath)

    return filepath


def print_summary(grouped_df: pl.DataFrame, method: str, num_partitions: int):
    """Print grouping summary statistics."""
    group_stats = (
        grouped_df
        .group_by("group")
        .agg(pl.len().alias("count"))
        .sort("group")
    )

    print(f"\nPartitioning Summary ({method} method):")
    print(f"  Total rows: {grouped_df.height:,}")
    print(f"  Number of partitions: {group_stats.height}")

    # Only print individual partition sizes if reasonable number
    if num_partitions <= 20:
        print(f"  Rows per partition:")
        for row in group_stats.iter_rows(named=True):
            print(f"    Partition {int(row['group'])}: {row['count']:,} rows")
    else:
        print(f"  (Showing stats only due to large partition count)")

    counts = group_stats["count"]
    print(f"  Min/Max/Mean: {counts.min():,}/{counts.max():,}/{counts.mean():,.1f}")

    # For host method, show host distribution
    if method == 'host' and '_host' in grouped_df.columns:
        hosts_per_partition = (
            grouped_df
            .group_by("group")
            .agg(pl.col("_host").n_unique().alias("unique_hosts"))
            .sort("group")
        )
        if num_partitions <= 20:
            print(f"\n  Unique hosts per partition:")
            for row in hosts_per_partition.iter_rows(named=True):
                print(f"    Partition {int(row['group'])}: {row['unique_hosts']} hosts")


def main():
    inputs = parse_args()

    print("=" * 60)
    print("FLOW-DC Dataset Partitioning (Polars)")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  Input: {inputs.parquet}")
    print(f"  URL column: {inputs.url_col}")
    print(f"  Method: {inputs.method}")
    if inputs.method == 'greedy':
        print(f"  Grouping column: {inputs.grouping_col}")
    print(f"  Partitions: {inputs.groups}")
    print(f"  Output folder: {inputs.output_folder}")
    print(f"  Output format: {inputs.output_format}")
    print(f"  Add host column: {inputs.add_host_column}")

    # Validate input
    if not os.path.exists(inputs.parquet):
        print(f"\nError: Input file '{inputs.parquet}' not found.")
        sys.exit(1)

    # Load data with polars
    print(f"\nLoading data from {inputs.parquet}...")
    try:
        df = pl.read_parquet(inputs.parquet)
    except Exception as e:
        print(f"Error reading parquet: {e}")
        sys.exit(1)

    print(f"  Loaded {df.height:,} rows, {len(df.columns)} columns")

    # Validate columns
    if inputs.url_col not in df.columns:
        print(f"Error: URL column '{inputs.url_col}' not found.")
        print(f"Available columns: {df.columns}")
        sys.exit(1)

    if inputs.method == 'greedy' and inputs.grouping_col not in df.columns:
        print(f"Error: Grouping column '{inputs.grouping_col}' not found.")
        sys.exit(1)

    # Apply grouping method
    print(f"\nApplying {inputs.method} partitioning...")

    if inputs.method == GroupingMethod.SIMPLE.value:
        df_grouped = simple_grouping(df, inputs.groups)

    elif inputs.method == GroupingMethod.HOST.value:
        df_grouped = host_grouping(df, inputs.url_col, inputs.groups)

    elif inputs.method == GroupingMethod.GREEDY.value:
        # Legacy greedy by grouping column
        df = partition_df_legacy(df, inputs.groups, inputs.grouping_col)
        count_df = (
            df
            .group_by(inputs.grouping_col)
            .agg(pl.len().alias("Count"))
            .sort("Count", descending=True)
        )
        groups_df = greedy_grouping(inputs.groups, count_df, "Count", inputs.grouping_col)
        if "group" in df.columns:
            df = df.drop("group")
        df_grouped = df.join(
            groups_df.select([inputs.grouping_col, "group"]),
            on=inputs.grouping_col,
            how="left"
        )

    else:
        print(f"Error: Unknown method '{inputs.method}'")
        sys.exit(1)

    # Print summary
    print_summary(df_grouped, inputs.method, inputs.groups)

    # Create output directory
    os.makedirs(inputs.output_folder, exist_ok=True)

    # Save partitions
    print(f"\nSaving partitions to {inputs.output_folder}/...")
    saved_files = []

    # Get unique groups sorted
    unique_groups = sorted(df_grouped["group"].unique().to_list())

    for group in unique_groups:
        subset = df_grouped.filter(pl.col("group") == group)

        # Remove internal columns from output
        cols_to_drop = ['group']
        if not inputs.add_host_column:
            cols_to_drop.append('_host')

        # Rename _host to 'host' for cleaner output
        if inputs.add_host_column and '_host' in subset.columns:
            subset = subset.rename({'_host': 'host'})

        # Drop columns that exist
        cols_to_actually_drop = [c for c in cols_to_drop if c in subset.columns]
        if cols_to_actually_drop:
            output_df = subset.drop(cols_to_actually_drop)
        else:
            output_df = subset

        filepath = save_partition(output_df, int(group), inputs.output_folder, inputs.output_format)
        saved_files.append(filepath)
        print(f"  Saved {filepath} ({output_df.height:,} rows)")

    print(f"\nCompleted! Created {len(saved_files)} partition files.")
    print("=" * 60)


if __name__ == "__main__":
    main()
