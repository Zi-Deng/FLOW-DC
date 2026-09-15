# FLOW-DC vs img2dataset Benchmark Suite

A reusable benchmark framework for comparing FLOW-DC against img2dataset on image downloading tasks.

## Features

- **Comprehensive Metrics**: Throughput (MB/s, images/sec), success rate, CPU/memory usage
- **Fair Comparison**: Disables img2dataset resizing, matches timeouts and retry settings
- **Multiple Runs**: Warmup runs + multiple measured runs for statistical validity
- **Resource Monitoring**: Real-time CPU, memory, and network tracking via psutil
- **Rich Reports**: JSON data + interactive HTML reports with Chart.js visualizations
- **Flexible Configuration**: YAML config files or CLI arguments

## Quick Start

### 1. Install Dependencies

```bash
pip install -r benchmark/requirements.txt
```

### 2. Run a Quick Benchmark

```bash
# Quick validation with a small dataset
python benchmark/run_benchmark.py --quick --dataset files/input/sample.parquet

# Run with default configuration
python benchmark/run_benchmark.py --config benchmark/configs/benchmark_config.yaml
```

### 3. View Results

Results are saved to `benchmark/results/<run_name>_<timestamp>/`:
- `report.json` - Raw benchmark data
- `report.html` - Interactive visualization

## Usage

### CLI Options

```bash
python benchmark/run_benchmark.py [OPTIONS]

Options:
  --config PATH         Path to benchmark configuration YAML file
  --quick               Quick validation run (1 run, single concurrency)
  --tools {flowdc,img2dataset,all}
                        Tools to benchmark (default: all)
  --concurrency INT...  Concurrency levels to test
  --dataset PATH        Input dataset (parquet with URL column)
  --url-column STR      Name of URL column (default: url)
  --label-column STR    Name of label column (optional)
  --output-dir PATH     Output directory (default: benchmark/results)
  --name STR            Name for this benchmark run
  --warmup INT          Number of warmup runs (default: 1)
  --runs INT            Number of measured runs (default: 3)
  --timeout INT         Download timeout in seconds (default: 30)
  --no-resource-monitor Disable resource monitoring
  --json-only           Generate JSON report only (skip HTML)
  -v, --verbose         Increase verbosity
```

### Examples

```bash
# Full benchmark with all tools
python benchmark/run_benchmark.py \
    --dataset files/input/gbif_url_10000.parquet \
    --concurrency 64 128 256 512 \
    --runs 3

# Test only FLOW-DC with PAARC
python benchmark/run_benchmark.py \
    --dataset files/input/test.parquet \
    --tools flowdc \
    --concurrency 256

# Test only img2dataset
python benchmark/run_benchmark.py \
    --dataset files/input/test.parquet \
    --tools img2dataset \
    --concurrency 128 256
```

### Compare Multiple Runs

```bash
python benchmark/compare_results.py \
    benchmark/results/run1/report.json \
    benchmark/results/run2/report.json \
    --names "Before Optimization" "After Optimization"
```

## Configuration

### YAML Configuration

See `benchmark/configs/benchmark_config.yaml` for a complete example:

```yaml
name: "flowdc-vs-img2dataset"

dataset:
  path: "files/input/gbif_url_10000.parquet"
  url_column: "url"
  label_column: "species"

tools:
  flowdc:
    enabled: true
    variants:
      - "paarc_enabled"
      - "paarc_disabled"
  img2dataset:
    enabled: true
    processes: 1
    resize_mode: "no"

concurrency_levels: [64, 128, 256, 512]
timeout_sec: 30
warmup_runs: 1
measured_runs: 3

resource_monitoring:
  enabled: true
  sample_interval_sec: 0.5

reports:
  json: true
  html: true
```

## Metrics Collected

### Per-Run Metrics

| Metric | Description |
|--------|-------------|
| `successful_downloads` | Number of successful downloads |
| `failed_downloads` | Number of failed downloads |
| `success_rate_percent` | Success rate (%) |
| `throughput_mbps` | Download throughput (MB/s) |
| `throughput_imgs_per_sec` | Download rate (images/second) |
| `elapsed_seconds` | Total run time |
| `cpu_avg_percent` | Average CPU usage |
| `cpu_max_percent` | Peak CPU usage |
| `memory_avg_mb` | Average memory usage (MB) |
| `memory_max_mb` | Peak memory usage (MB) |

### Aggregated Metrics

Multiple runs are averaged with standard deviation calculated for throughput.

## Directory Structure

```
benchmark/
├── __init__.py
├── README.md
├── requirements.txt
├── run_benchmark.py          # Main CLI entry point
├── compare_results.py        # Compare multiple runs
├── configs/
│   └── benchmark_config.yaml # Default configuration
├── core/
│   ├── __init__.py
│   ├── metrics.py            # Unified metrics schema
│   ├── resource_monitor.py   # CPU/memory/network monitoring
│   ├── runner.py             # Benchmark orchestrator
│   ├── flowdc_adapter.py     # FLOW-DC execution adapter
│   └── img2dataset_adapter.py # img2dataset execution adapter
├── reports/
│   ├── __init__.py
│   ├── generator.py          # Report generation
│   └── templates/
│       └── report.html.j2    # HTML report template
└── results/                  # Output directory
```

## Fairness Considerations

To ensure fair comparison:

1. **No Image Resizing**: img2dataset runs with `--resize_mode no`
2. **Same Timeouts**: Both tools use identical timeout values
3. **Same Retries**: Both limited to 1 retry attempt
4. **Fresh Outputs**: Output directories cleaned between runs
5. **Warmup Runs**: First run(s) discarded to stabilize caches
6. **Multiple Runs**: Results averaged over 3+ runs
7. **Sequential Execution**: Tools run sequentially to avoid interference

## Extending the Suite

### Adding a New Tool

1. Create `core/<tool>_adapter.py` with:
   - Config dataclass
   - `run()` async method returning `BenchmarkResult`
2. Update `core/runner.py` to call the new adapter
3. Add configuration options to YAML schema
