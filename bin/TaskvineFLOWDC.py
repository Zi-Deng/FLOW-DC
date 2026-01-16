#!/usr/bin/env python
"""
FLOW-DC TaskVine Orchestrator

Distributed image downloading using TaskVine workflow manager.
Submits download_batch.py tasks for each partition created by SplitParquet.py.

This script:
1. Reads partition parquet files from a directory
2. Creates a TaskVine task for each partition
3. Each task runs download_batch.py with PAARC rate control
4. Collects output tar.gz archives

Usage:
    python TaskvineFLOWDC.py --config files/config/taskvine_flowdc.json

Config file format:
{
    "port_number": 9123,
    "parquets_directory": "files/input/partitions",
    "output_directory": "files/output/images",
    "url_col": "url",
    "label_col": "species",
    "concurrent_downloads": 1000,
    "enable_paarc": true,
    "timeout_minutes": 60,
    "max_retries": 3,

    // PAARC parameters
    "C_init": 8,
    "C_min": 2,
    "C_max": 2000,
    "mu": 1.0,
    "beta": 0.7
}
"""

import ndcctools.taskvine as vine
import json
import os
import argparse
import time
import sys
import tempfile
from pathlib import Path


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="FLOW-DC TaskVine Orchestrator - Distributed image downloading with PAARC",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python TaskvineFLOWDC.py --config files/config/taskvine_flowdc.json
  python TaskvineFLOWDC.py --config files/config/taskvine_flowdc.json --dry_run
"""
    )
    parser.add_argument(
        "--config_file",
        "--config",
        dest="config_file",
        type=str,
        required=True,
        help="Path to the TaskVine configuration JSON file."
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Print tasks without submitting them."
    )
    return parser.parse_args()


def parse_json_config(file_path: str) -> dict:
    """
    Load and parse the JSON configuration file with validation.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")

    with open(file_path, 'r') as config_file:
        config = json.load(config_file)

    # Required fields
    required_fields = ['port_number', 'parquets_directory']
    for field in required_fields:
        if field not in config:
            raise ValueError(f"Required field '{field}' not found in config")

    # Set defaults for optional fields
    defaults = {
        # Output settings
        'output_directory': 'files/output/images',
        'output_format': 'imagefolder',

        # Column mapping
        'url_col': 'url',
        'label_col': None,

        # Download settings
        'concurrent_downloads': 1000,
        'timeout_sec': 30,

        # PAARC toggle
        'enable_paarc': True,

        # PAARC concurrency bounds
        'C_init': 8,
        'C_min': 2,
        'C_max': 2000,

        # PAARC utilization and backoff
        'mu': 1.0,
        'beta': 0.7,

        # PAARC latency thresholds
        'theta_50': 1.5,
        'theta_95': 2.0,
        'startup_theta_50': 3.0,
        'startup_theta_95': 4.0,

        # PAARC timing
        'probe_rtt_period': 30.0,
        'rtprop_window': 35.0,
        'cooldown_floor': 2.0,

        # PAARC smoothing
        'alpha_ema': 0.3,

        # Retry settings
        'max_retry_attempts': 3,
        'retry_backoff_sec': 2.0,

        # Output options
        'naming_mode': 'sequential',
        'create_tar': True,
        'create_overview': True,

        # TaskVine task resource defaults
        'task_cores': 4,
        'task_memory_mb': 8000,
        'task_disk_mb': 50000,
        'timeout_minutes': 60,
        'max_retries': 3,
    }

    for field, default_value in defaults.items():
        if field not in config:
            config[field] = default_value

    return config


def create_partition_config(base_config: dict, partition_file: str, output_name: str) -> dict:
    """
    Create a download_batch.py config for a specific partition.

    Args:
        base_config: Base TaskVine config with download parameters
        partition_file: Name of the partition parquet file
        output_name: Output folder name for this partition

    Returns:
        Config dict suitable for download_batch.py
    """
    return {
        # Input/Output
        "input": partition_file,
        "input_format": "parquet",
        "output": output_name,
        "output_format": base_config.get('output_format', 'imagefolder'),

        # Column mapping
        "url": base_config.get('url_col', 'url'),
        "label": base_config.get('label_col'),

        # Download settings
        "concurrent_downloads": base_config.get('concurrent_downloads', 1000),
        "timeout": base_config.get('timeout_sec', 30),

        # PAARC toggle
        "enable_paarc": base_config.get('enable_paarc', True),

        # PAARC concurrency bounds
        "C_init": base_config.get('C_init', 8),
        "C_min": base_config.get('C_min', 2),
        "C_max": base_config.get('C_max', 2000),

        # PAARC utilization and backoff
        "mu": base_config.get('mu', 1.0),
        "beta": base_config.get('beta', 0.7),

        # PAARC latency thresholds
        "theta_50": base_config.get('theta_50', 1.5),
        "theta_95": base_config.get('theta_95', 2.0),
        "startup_theta_50": base_config.get('startup_theta_50', 3.0),
        "startup_theta_95": base_config.get('startup_theta_95', 4.0),

        # PAARC timing
        "probe_rtt_period": base_config.get('probe_rtt_period', 30.0),
        "rtprop_window": base_config.get('rtprop_window', 35.0),
        "cooldown_floor": base_config.get('cooldown_floor', 2.0),

        # PAARC smoothing
        "alpha_ema": base_config.get('alpha_ema', 0.3),

        # Retry settings
        "max_retry_attempts": base_config.get('max_retry_attempts', 3),
        "retry_backoff_sec": base_config.get('retry_backoff_sec', 2.0),

        # Output options
        "naming_mode": base_config.get('naming_mode', 'sequential'),
        "create_tar": base_config.get('create_tar', True),
        "create_overview": base_config.get('create_overview', True),
    }


def declare_parquet_files(manager, directory: str) -> dict:
    """
    Declare input parquet files to TaskVine manager.

    Returns:
        Dictionary mapping filename -> declared file object
    """
    if not os.path.exists(directory):
        print(f"Error: Directory {directory} does not exist")
        return {}

    parquet_files = sorted([
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(".parquet")
    ])

    if not parquet_files:
        print(f"Warning: No parquet files found in {directory}")
        return {}

    declared_files = {}
    for parquet_path in parquet_files:
        file_name = os.path.basename(parquet_path)
        try:
            declared_file = manager.declare_file(parquet_path)
            declared_files[file_name] = declared_file
            print(f"  Declared input: {file_name}")
        except Exception as e:
            print(f"  Error declaring {parquet_path}: {e}")

    return declared_files


def submit_tasks(
    manager,
    download_script: str,
    single_download_script: str,
    parquet_files: dict,
    config: dict,
    temp_dir: str
) -> int:
    """
    Submit download tasks to TaskVine.

    Args:
        manager: TaskVine manager instance
        download_script: Path to download_batch.py
        single_download_script: Path to single_download.py (dependency)
        parquet_files: Dict of filename -> declared file
        config: TaskVine configuration
        temp_dir: Temporary directory for config files

    Returns:
        Number of tasks submitted
    """
    # Declare the download scripts
    download_script_vine = manager.declare_file(download_script)
    single_download_vine = manager.declare_file(single_download_script)

    max_retries = config.get('max_retries', 3)
    task_cores = config.get('task_cores', 4)
    task_memory = config.get('task_memory_mb', 8000)
    task_disk = config.get('task_disk_mb', 50000)

    submitted_tasks = 0

    for file_name, declared_file in parquet_files.items():
        try:
            # Create output name from partition name
            base_name = file_name.replace(".parquet", "")
            output_name = f"output_{base_name}"
            tar_name = f"{output_name}.tar.gz"

            # Create partition-specific config
            partition_config = create_partition_config(config, file_name, output_name)

            # Write config to temp file
            config_filename = f"config_{base_name}.json"
            config_path = os.path.join(temp_dir, config_filename)
            with open(config_path, 'w') as f:
                json.dump(partition_config, f, indent=2)

            # Declare config file
            config_vine = manager.declare_file(config_path)

            # Create task command
            command = f"python download_batch.py --config {config_filename}"

            task = vine.Task(command)

            # Set task properties
            task.set_retries(max_retries)
            task.set_cores(task_cores)
            task.set_memory(task_memory)
            task.set_disk(task_disk)

            # Add input files
            task.add_input(download_script_vine, "download_batch.py")
            task.add_input(single_download_vine, "single_download.py")
            task.add_input(config_vine, config_filename)
            task.add_input(declared_file, file_name)

            # Declare and add output file
            output_file = manager.declare_file(
                os.path.join(config.get('output_directory', '.'), tar_name)
            )
            task.add_output(output_file, tar_name)

            # Submit task
            task_id = manager.submit(task)
            submitted_tasks += 1
            print(f"  Submitted task {task_id}: {file_name} -> {tar_name}")

        except Exception as e:
            print(f"  Error submitting task for {file_name}: {e}")

    return submitted_tasks


def monitor_tasks(manager, total_tasks: int) -> tuple:
    """
    Monitor task completion with detailed status reporting.

    Returns:
        Tuple of (successful_tasks, failed_tasks)
    """
    print("\nWaiting for tasks to complete...")
    completed_tasks = 0
    failed_tasks = 0
    start_time = time.time()
    last_status_time = start_time

    while not manager.empty():
        task = manager.wait(5)
        current_time = time.time()

        if task:
            completed_tasks += 1
            elapsed = current_time - start_time

            if task.successful():
                print(f"[OK] Task {task.id} completed ({completed_tasks}/{total_tasks}) - {elapsed:.1f}s elapsed")
                if task.output:
                    # Print last line of output
                    output_lines = task.output.strip().split('\n')
                    if output_lines:
                        print(f"     Output: {output_lines[-1]}")
            else:
                failed_tasks += 1
                print(f"[FAIL] Task {task.id} FAILED ({completed_tasks}/{total_tasks}) - {elapsed:.1f}s elapsed")
                print(f"       Exit code: {task.exit_code}")
                if task.output:
                    # Print last few lines of output for debugging
                    output_lines = task.output.strip().split('\n')
                    for line in output_lines[-3:]:
                        print(f"       {line}")
        else:
            # Print status every 60 seconds when no task completes
            if current_time - last_status_time >= 60:
                elapsed = current_time - start_time
                stats = manager.stats
                print(f"Status: {completed_tasks}/{total_tasks} done, "
                      f"{stats.tasks_running} running, "
                      f"{stats.tasks_waiting} waiting - {elapsed:.1f}s elapsed")
                last_status_time = current_time

    total_time = time.time() - start_time
    print(f"\nAll tasks completed in {total_time:.1f}s")
    print(f"  Successful: {completed_tasks - failed_tasks}")
    print(f"  Failed: {failed_tasks}")

    return completed_tasks - failed_tasks, failed_tasks


def main():
    args = parse_args()

    print("=" * 60)
    print("FLOW-DC TaskVine Orchestrator")
    print("Distributed downloading with PAARC rate control")
    print("=" * 60)

    # Load configuration
    try:
        config = parse_json_config(args.config_file)
        print(f"\nConfiguration: {args.config_file}")
    except Exception as e:
        print(f"Error loading configuration: {e}")
        sys.exit(1)

    # Print configuration summary
    print(f"  Partitions directory: {config['parquets_directory']}")
    print(f"  Output directory: {config.get('output_directory', 'files/output/images')}")
    print(f"  URL column: {config.get('url_col', 'url')}")
    print(f"  Concurrent downloads per task: {config.get('concurrent_downloads', 1000)}")
    print(f"  PAARC enabled: {config.get('enable_paarc', True)}")
    if config.get('enable_paarc', True):
        print(f"    C_init={config.get('C_init', 8)}, C_min={config.get('C_min', 2)}, C_max={config.get('C_max', 2000)}")
        print(f"    mu={config.get('mu', 1.0)}, beta={config.get('beta', 0.7)}")

    # Validate paths
    download_script = os.path.join(os.path.dirname(__file__), "download_batch.py")
    single_download_script = os.path.join(os.path.dirname(__file__), "single_download.py")

    if not os.path.exists(download_script):
        print(f"Error: Download script not found: {download_script}")
        sys.exit(1)

    if not os.path.exists(single_download_script):
        print(f"Error: Single download script not found: {single_download_script}")
        sys.exit(1)

    # Create output directory
    output_dir = config.get('output_directory', 'files/output/images')
    os.makedirs(output_dir, exist_ok=True)

    if args.dry_run:
        print("\n[DRY RUN] Would process the following partitions:")
        parquets_dir = config['parquets_directory']
        if os.path.exists(parquets_dir):
            for f in sorted(os.listdir(parquets_dir)):
                if f.endswith('.parquet'):
                    print(f"  {f}")
        print("\n[DRY RUN] No tasks submitted.")
        return

    # Initialize TaskVine manager
    try:
        manager = vine.Manager(config['port_number'])
        print(f"\nTaskVine Manager listening on port {manager.port}")

        # Set manager tuning parameters
        manager.tune("worker-retrievals", 5)
        manager.tune("transfer-temps-recovery", 1)

    except Exception as e:
        print(f"Error initializing TaskVine manager: {e}")
        sys.exit(1)

    # Declare input files
    print(f"\nDeclaring input files from {config['parquets_directory']}...")
    parquet_files = declare_parquet_files(manager, config['parquets_directory'])

    if not parquet_files:
        print("No partition files found. Exiting.")
        sys.exit(1)

    print(f"  Found {len(parquet_files)} partition files")

    # Create temp directory for config files
    with tempfile.TemporaryDirectory(prefix="flowdc_configs_") as temp_dir:
        # Submit tasks
        print("\nSubmitting tasks...")
        total_tasks = submit_tasks(
            manager,
            download_script,
            single_download_script,
            parquet_files,
            config,
            temp_dir
        )

        if total_tasks == 0:
            print("No tasks were submitted. Exiting.")
            sys.exit(1)

        print(f"\nSubmitted {total_tasks} tasks")

        # Monitor task completion
        successful, failed = monitor_tasks(manager, total_tasks)

    # Final summary
    print("\n" + "=" * 60)
    if failed > 0:
        print(f"Warning: {failed} tasks failed out of {total_tasks}")
        sys.exit(1)
    else:
        print(f"All {successful} tasks completed successfully!")
        print(f"Output files saved to: {output_dir}")
    print("=" * 60)


if __name__ == '__main__':
    main()
