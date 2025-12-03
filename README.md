# FLOW-DC

**Flexible Large-scale Orchestrated Workflow for Data Collection**

A pipeline for distributed downloading of large-scale machine learning datasets using parallelism and workflow management systems.

## Overview

FLOW-DC is utilized for rapid acquisition of large datasets in machine learning research. FLOW-DC leverages distributed parallelism to significantly speed up dataset acquisition times.

FLOW-DC uses a manager-worker paradigm where a central manager machine efficiently partitions datasets into manageable subsets, assigns them to worker machines for concurrent download jobs, and consolidates the final dataset. The system is built on TaskVine workflow management.

## Installation

### Prerequisites

- Conda (Miniconda or Anaconda)
- Python 3.10.8

### Setup

1. Clone the repository:
```bash
git clone https://github.com/zkdeng-uofa/FLOW-DC
cd FLOW-DC
```

2. Create the conda environment from the provided `environment.yml`:
```bash
conda env create -f environment.yml
```

3. Activate the environment:
```bash
conda activate FLOW-DC
```

The environment includes all necessary dependencies:
- Python 3.10.8
- pandas, numpy for data manipulation
- aiohttp for asynchronous HTTP requests
- ndcctools (TaskVine) for workflow management
- tqdm for progress bars
- pyarrow for Parquet file support

## Quick Start

### Basic Usage

The simplest way to use FLOW-DC is with a JSON configuration file:

```bash
python bin/download.py --config files/config/my_config.json
```

### Command-Line Usage

Alternatively, you can specify all parameters via command-line arguments:

```bash
python bin/download.py \
    --input files/input/gbif_url_10000.parquet \
    --output files/output/gbif_dataset \
    --url photo_url \
    --label taxon_name \
    --output_format webdataset \
    --concurrent_downloads 1000 \
    --timeout 30 \
    --enable_rate_limiting \
    --rate_limit 500 \
    --max_retry_attempts 3
```

### Example Configuration File

Create a JSON configuration file (e.g., `my_config.json`):

```json
{
    "input": "files/input/my_dataset.parquet",
    "input_format": "parquet",
    "output": "files/output/my_dataset_webdataset",
    "output_format": "webdataset",
    "url": "photo_url",
    "label": "taxon_name",
    "concurrent_downloads": 1000,
    "timeout": 30,
    "rate_limit": 500,
    "rate_capacity": 1000,
    "enable_rate_limiting": true,
    "max_retry_attempts": 3,
    "create_overview": true,
    "croissant": "basic_croissant",
    "dataset_name": "My Image Dataset",
    "dataset_description": "A collection of images organized by taxonomic name",
    "dataset_license": "CC-BY-4.0",
    "dataset_creator": "Your Name",
    "dataset_version": "1.0.0"
}
```

See `files/config/gbif.json` for a complete example.

## Usage

### Input File Format

FLOW-DC accepts input files in multiple formats:
- **Parquet** (recommended): Efficient binary format, supports large datasets
- **CSV**: Simple text format (`.csv` or `.txt`)
- **XML**: Structured data format (`.xml`)
- **Excel**: `.xlsx` or `.xls` files (supported via `single_download.py` module)

Your input file must contain the following column:
- A column with URLs (default: `photo_url`, configurable via `--url`)

Example Parquet file structure:
```
photo_url                              | taxon_name
https://example.com/image1.jpg         | Species_A
https://example.com/image2.jpg         | Species_B
```

### Output Formats

#### ImageFolder Format
Organizes images into class-specific subdirectories:
```
output_folder/
├── Species_A/
│   ├── image1.jpg
│   └── image2.jpg
└── Species_B/
    └── image3.jpg
```

#### WebDataset Format
Stores images and JSON metadata files in a flat structure, suitable for tar archiving:
```
output_folder/
├── image1.jpg
├── image1.json
├── image2.jpg
└── image2.json
```

### Configuration Options

#### Download Settings
- `--concurrent_downloads`: Number of simultaneous downloads (default: 1000)
- `--timeout`: Request timeout in seconds (default: 30)
- `--max_retry_attempts`: Maximum retries for failed downloads (default: 3)

#### Rate Limiting
- `--enable_rate_limiting`: Enable adaptive rate limiting
- `--rate_limit`: Initial rate limit in requests per second (default: 100.0)
- `--rate_capacity`: Token bucket capacity (default: 200)

The adaptive rate limiter uses AIMD algorithm to automatically adjust download rates based on server responses, preventing rate-limiting errors (429) and server overload.

#### Metadata Generation
- `--croissant`: Generate Croissant metadata (`no_croissant`, `basic_croissant`, `comprehensive_croissant`)
- `--create_overview`: Generate JSON overview file with download statistics (default: True)

### Distributed Execution with TaskVine

For distributed downloading across multiple machines:

1. **Start the Manager**: On your manager machine, initialize the TaskVine manager
2. **Connect Workers**: On worker machines, connect to the manager using the manager's IP and port
3. **Submit Tasks**: The manager will automatically partition your dataset and distribute tasks to available workers

The manager-worker paradigm allows:
- Dynamic worker addition/removal during execution
- Automatic task rescheduling on worker failure
- Centralized progress monitoring and result aggregation

Refer to the TaskVine documentation for detailed setup instructions.

## Performance

FLOW-DC has been successfully used to download large-scale datasets:

- **Biotrove-Train Dataset**: 13 TiB, 38.7 million images downloaded in ~42 hours using 10 Jetstream workers (average throughput: 85 MiB/s per worker)

Download throughput scales approximately linearly with the number of worker machines until network or server limitations are reached.

## Project Structure

```
FLOW-DC/
├── bin/
│   ├── download.py              # Main download script
│   ├── single_download.py       # Modular single URL download functions
│   └── download_batch.py        # Batch download orchestration
├── files/
│   ├── config/                   # Example configuration files
│   ├── input/                    # Input data files (parquet, CSV, etc.)
│   └── output/                   # Output datasets
├── jupyter/                      # Jupyter notebook examples
├── examples/                     # Usage examples
├── environment.yml               # Conda environment specification
└── README.md                     # This file
```

## Citation

If you use FLOW-DC in your research, please cite:

```
Deng, Z., Merchant, N., & Rodriguez, J. J. (2025). 
Flexible Large-scale Orchestrated Workflow for Data Collection: FLOW-DC.
(https://github.com/zkdeng-uofa/FLOW-DC)
```

## Contact

- **Primary Author**: Zi Deng (zkdeng@arizona.edu)
- **Affiliation**: Electrical and Computer Engineering, University of Arizona
- **GitHub**: [https://github.com/zkdeng-uofa/FLOW-DC]

## Acknowledgments

FLOW-DC is built using:
- [TaskVine](https://cctools.readthedocs.io/en/latest/taskvine/) for workflow management
- [aiohttp](https://docs.aiohttp.org/) for asynchronous HTTP requests
- [pandas](https://pandas.pydata.org/) for data manipulation

## Additional Resources

- For detailed feature documentation, see the paper
- For TaskVine setup and usage, visit: https://cctools.readthedocs.io/en/latest/taskvine/
- For questions and issues, please open an issue on GitHub

