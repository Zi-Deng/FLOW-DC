# FLOW-DC

**Flexible Large-scale Orchestrated Workflow for Data Collection**

A high-performance pipeline for distributed downloading of large-scale machine learning datasets, featuring adaptive per-host rate control and seamless integration with HPC workflow managers.

## Overview

FLOW-DC accelerates dataset acquisition for machine learning research by leveraging distributed parallelism across multiple worker machines. The system uses a manager-worker architecture built on TaskVine, where a central manager partitions datasets, assigns download tasks to workers, and consolidates results.

The core component is **PAARC (Policy-Aware Adaptive Request Controller)**, a congestion control algorithm that dynamically adjusts concurrency for each target host based on observed latency. This enables FLOW-DC to maximize throughput while respecting server rate limits and avoiding overload.

## Installation

### Prerequisites

- Conda (Miniconda or Anaconda)
- Python 3.10+

### Setup

```bash
# Clone the repository
git clone https://github.com/zkdeng-uofa/FLOW-DC
cd FLOW-DC

# Create and activate the conda environment
conda env create -f environment.yml
conda activate FLOW-DC
```

The environment includes all necessary dependencies: polars, aiohttp, pyarrow, ndcctools (TaskVine), tqdm, and NiceGUI.

## Quick Start

### Single-Machine Download

The simplest way to download a dataset is with a JSON configuration file:

```bash
python bin/download_batch.py --config files/config/my_config.json
```

### Example Configuration

Create a configuration file (e.g., `my_config.json`):

```json
{
    "input": "files/input/dataset.parquet",
    "output": "files/output/images",
    "url": "photo_url",
    "label": "species",

    "concurrent_downloads": 1000,
    "timeout": 30,

    "enable_paarc": true,
    "C_init": 8,
    "C_min": 2,
    "C_max": 2000,

    "output_format": "imagefolder",
    "create_tar": true,
    "create_overview": true
}
```

### Command-Line Usage

All configuration options are also available as command-line arguments:

```bash
python bin/download_batch.py \
    --input files/input/dataset.parquet \
    --output files/output/images \
    --url photo_url \
    --label species \
    --concurrent_downloads 1000 \
    --enable_paarc
```

## Scripts Reference

### download_batch.py

The main batch downloader with PAARC rate control. Handles concurrent downloads, automatic retries, and per-host adaptive concurrency.

```bash
python bin/download_batch.py --config config.json
```

**Key Features:**
- Bounded concurrency with adaptive semaphores
- Per-host rate limiting via PAARC
- Automatic retry for transient failures (429, 5xx, timeouts)
- TTFB-based latency monitoring
- Supports imagefolder and webdataset output formats

### SplitParquet.py

Partitions a dataset manifest into multiple files for distributed downloading. Supports three grouping strategies:

```bash
# Host-based grouping (recommended for distributed downloads)
python bin/SplitParquet.py \
    --parquet dataset.parquet \
    --url_col url \
    --groups 10 \
    --output_folder partitions \
    --method host

# Using a configuration file
python bin/SplitParquet.py --config partition_config.json
```

**Partitioning Methods:**

| Method | Description |
|--------|-------------|
| `host` | Groups URLs by domain, ensuring each partition deals with fewer unique hosts |
| `simple` | Equal-sized random partitions |
| `greedy` | Balances partitions by a grouping column (e.g., class labels) |

Host-based partitioning is recommended for distributed downloads because each worker can maximize throughput by focusing on fewer hosts, and the PAARC controller operates more effectively with consistent per-host traffic.

### CalcDatasetSize.py

Estimates total download size by querying URLs for Content-Length headers. Useful for planning storage requirements before starting a large download.

```bash
# Estimate size for a single file
python bin/CalcDatasetSize.py \
    --input dataset.parquet \
    --url_column url

# Estimate size for partitioned datasets
python bin/CalcDatasetSize.py \
    --directory partitions/ \
    --url_column url

# Sample 5000 URLs for faster estimation
python bin/CalcDatasetSize.py \
    --input dataset.parquet \
    --url_column url \
    --sample 5000
```

### TaskvineFLOWDC.py

Orchestrates distributed downloading across multiple worker machines using TaskVine. Creates a download task for each partition and collects the results.

```bash
# Start the manager
python bin/TaskvineFLOWDC.py --config taskvine_config.json

# Preview tasks without executing
python bin/TaskvineFLOWDC.py --config taskvine_config.json --dry_run
```

**TaskVine Configuration Example:**

```json
{
    "port_number": 9123,
    "parquets_directory": "files/input/partitions",
    "output_directory": "files/output/images",

    "url_col": "url",
    "label_col": "species",

    "concurrent_downloads": 1000,
    "enable_paarc": true,
    "C_init": 8,
    "C_max": 2000,

    "task_cores": 4,
    "task_memory_mb": 8000,
    "timeout_minutes": 60
}
```

Workers connect to the manager using standard TaskVine commands. See the [TaskVine documentation](https://cctools.readthedocs.io/en/latest/taskvine/) for details.

### TaskvineFLOWDCCloud.py

Extended version of the TaskVine orchestrator that uploads results directly to cloud storage. Supports iRODS/CyVerse (via gocmd), AWS S3, Google Cloud Storage, and rclone.

```bash
python bin/TaskvineFLOWDCCloud.py --config taskvine_cloud_config.json
```

**Cloud Configuration Example:**

```json
{
    "port_number": 9123,
    "parquets_directory": "files/input/partitions",
    "cloud_destination": "AIIRA_Dataset",
    "cloud_tool": "gocmd",

    "url_col": "url",
    "concurrent_downloads": 1000,
    "enable_paarc": true
}
```

Supported cloud tools: `gocmd` (iRODS), `aws` (S3), `gsutil` (GCS), `rclone`, `scp`.

### ui_app.py

A web-based interface for configuring downloads and monitoring progress. Built with NiceGUI.

```bash
python bin/ui_app.py
```

Opens a browser interface at `http://localhost:8080` where you can:
- Configure all download parameters
- Load and save configuration files
- Start and monitor download jobs
- View real-time progress and PAARC state

### single_download.py / single_download_gbif.py

Low-level modules providing the core download functions. These are used internally by `download_batch.py` and can be imported for custom integrations.

```python
from single_download import download_single, load_input_file

# Load a dataset
df = load_input_file("dataset.parquet")

# Download a single URL
result = await download_single(
    url="https://example.com/image.jpg",
    key="row_001",
    class_name="species_a",
    output_folder="output",
    output_format="imagefolder",
    session=session,
    timeout=30
)
```

## Configuration Reference

### Input/Output Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `input` | string | required | Path to input file (parquet, csv, excel, xml) |
| `input_format` | string | auto | Input format: `parquet`, `csv`, `excel`, `xml` |
| `output` | string | required | Output directory path |
| `output_format` | string | `imagefolder` | Output format: `imagefolder` or `webdataset` |
| `url` | string | `url` | Column name containing URLs |
| `label` | string | null | Column name for class labels (optional) |

### Download Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `concurrent_downloads` | int | 1000 | Maximum concurrent download workers |
| `timeout` | int | 30 | Request timeout in seconds |
| `max_retry_attempts` | int | 3 | Maximum retries for failed downloads |
| `retry_backoff_sec` | float | 2.0 | Delay between retry attempts |

### PAARC Rate Control

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable_paarc` | bool | true | Enable adaptive per-host rate control |
| `C_init` | int | 8 | Initial concurrency per host |
| `C_min` | int | 2 | Minimum concurrency floor |
| `C_max` | int | 2000 | Maximum concurrency ceiling |
| `mu` | float | 1.0 | Utilization factor (0.0-1.0) |
| `beta` | float | 0.7 | Backoff multiplier on overload |
| `theta_50` | float | 1.5 | P50 latency threshold (× RTprop) |
| `theta_95` | float | 2.0 | P95 latency threshold (× RTprop) |
| `probe_rtt_period` | float | 30.0 | Seconds between RTprop refresh |
| `alpha_ema` | float | 0.3 | Latency smoothing factor |

### Output Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `naming_mode` | string | `sequential` | Filename strategy: `sequential` or `url_based` |
| `create_tar` | bool | true | Create tar.gz archive of output |
| `create_overview` | bool | true | Generate JSON overview with statistics |

## Input File Format

FLOW-DC accepts input files in multiple formats. The file must contain a column with URLs to download.

**Supported Formats:**
- Parquet (recommended for large datasets)
- CSV
- Excel (.xlsx, .xls)
- XML

**Example Input Structure:**

| url | species |
|-----|---------|
| https://example.com/img1.jpg | Species_A |
| https://example.com/img2.jpg | Species_B |
| https://example.com/img3.jpg | Species_A |

## Output Formats

### ImageFolder

Organizes images into class-specific subdirectories, compatible with PyTorch's `ImageFolder` dataset class:

```
output/
├── Species_A/
│   ├── 00000001.jpg
│   └── 00000003.jpg
└── Species_B/
    └── 00000002.jpg
```

### WebDataset

Flat structure with JSON metadata files, suitable for creating WebDataset tar archives:

```
output/
├── 00000001.jpg
├── 00000001.json
├── 00000002.jpg
├── 00000002.json
└── ...
```

Each JSON file contains metadata about the corresponding image:

```json
{
    "key": "00000001",
    "url": "https://example.com/img1.jpg",
    "class_name": "Species_A"
}
```

## Distributed Workflow

For large-scale downloads, FLOW-DC supports distributed execution across multiple machines:

1. **Partition the dataset** by host to optimize per-worker performance:
   ```bash
   python bin/SplitParquet.py --parquet dataset.parquet --url_col url --groups 20 --output_folder partitions --method host
   ```

2. **Start the TaskVine manager**:
   ```bash
   python bin/TaskvineFLOWDC.py --config taskvine_config.json
   ```

3. **Connect workers** from other machines:
   ```bash
   vine_worker manager_hostname 9123
   ```

The manager automatically distributes partitions to available workers and collects results. Workers can be added or removed dynamically during execution.

## Performance

FLOW-DC has been used to download large-scale datasets in production:

- **Biotrove-Train Dataset**: 13 TiB, 38.7 million images downloaded in approximately 42 hours using 10 Jetstream cloud workers.

Throughput scales approximately linearly with the number of workers until network or server bandwidth becomes the limiting factor.

## Project Structure

```
FLOW-DC/
├── bin/
│   ├── download_batch.py         # Main batch downloader with PAARC
│   ├── single_download.py        # Core download module
│   ├── single_download_gbif.py   # GBIF-specific download module
│   ├── SplitParquet.py           # Dataset partitioning
│   ├── CalcDatasetSize.py        # Size estimation
│   ├── TaskvineFLOWDC.py         # TaskVine orchestrator
│   ├── TaskvineFLOWDCCloud.py    # TaskVine with cloud storage
│   ├── ui_app.py                 # Web interface
│   └── FLOWDC.ipynb              # Jupyter notebook example
├── files/
│   ├── config/                   # Configuration examples
│   ├── input/                    # Input datasets
│   └── output/                   # Downloaded results
├── docs/                         # Additional documentation
├── environment.yml               # Conda environment
└── README.md
```

## Citation

If you use FLOW-DC in your research, please cite:

```bibtex
@software{flowdc2025,
    author = {Deng, Zi and Merchant, Nirav and Rodriguez, Jeffrey J.},
    title = {FLOW-DC: Flexible Large-scale Orchestrated Workflow for Data Collection},
    year = {2025},
    url = {https://github.com/zkdeng-uofa/FLOW-DC}
}
```

## Contact

- **Author**: Zi Deng (zkdeng@arizona.edu)
- **Affiliation**: Electrical and Computer Engineering, University of Arizona
- **Repository**: https://github.com/zkdeng-uofa/FLOW-DC

## Acknowledgments

FLOW-DC is built using:
- [TaskVine](https://cctools.readthedocs.io/en/latest/taskvine/) for distributed workflow management
- [aiohttp](https://docs.aiohttp.org/) for asynchronous HTTP
- [Polars](https://pola.rs/) for high-performance data processing
- [NiceGUI](https://nicegui.io/) for the web interface
