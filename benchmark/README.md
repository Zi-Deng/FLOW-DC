# FLOW-DC vs img2dataset Benchmark Suite

A retained benchmark framework with two explicitly separate evidence contracts.
`known_truth.py` prepares a bounded localhost comparison using the real executables,
original bytes, per-row metadata and closed uncompressed archives. Its initial V2
execution passed on the first committed checkpoint; final-head validation remains
required. See the [contract and acceptance map](../docs/research/benchmark-contract.md).
`run_benchmark.py` and its HTML/comparison consumers retain the historical native-counter
contract. Those timing/output boundaries do not establish a fair efficacy comparison.

## Features

- **Comprehensive Metrics**: Throughput (MB/s, images/sec), success rate, CPU/memory usage
- **Original bytes**: Explicit img2dataset byte bypass, no EXIF extraction, zero native retry/replay defaults
- **Retained Runs**: Warmup and measured native artifacts remain on disk; existing output directories are refused
- **Resource Monitoring**: Real-time CPU, memory, and network tracking via psutil
- **Rich Reports**: JSON data + interactive HTML reports with Chart.js visualizations
- **Flexible Configuration**: YAML config files or CLI arguments

## Known-truth local engineering check

Use a task-local Python 3.12 environment; do not install into the shared environment:

```bash
python3 -m venv .agentic-local/research
.agentic-local/research/bin/python -m pip install --report .agentic-local/research-install.json -r benchmark/requirements-research.txt
.agentic-local/research/bin/python -B benchmark/known_truth.py --output benchmark/results/known-truth-smoke-001
```

This command uses nine original rows (six eligible), valid JPEG/PNG bytes, duplicate
URLs/content and basename collisions. Each native invocation has one downloader
process, two download slots/threads, a 180-second deadline and 60-second cleanup
reserve. FLOW-DC keeps mandatory admission and Retry-After behavior. Native timeout
semantics differ; the common outer deadline does not equate them. No external
dataset is contacted. An unavailable package or socket is a failed prerequisite,
never a successful integration or a reason to disable isolation.

The [controlled study harness](../docs/research/STUDY-HARNESS.md) adds explicit
service capacity/queues, fresh seeded scenario origins, frozen cell ordering,
retained failures/resume, fixed-client calibration and run-level paired summaries.
Use `python -B benchmark/study.py --help` for the commands. The tracked 72-cell
evaluation plan and tuning catalog are unexecuted proposals; non-engineering runs
refuse to start without explicit advisor decisions bound into a frozen protocol.

`benchmark/shared_origin.py` exercises real concurrent downloader clients against
one authenticated aggregate authority. See the
[shared-admission contract](../docs/research/SHARED-ADMISSION.md) for bounded commands,
loss/redirect cases and evidence limits. This is separate from required TaskVine
runtime integration.

Retain the output directory, including failures, and the private pip install report.
Do not publish install reports without inspecting source URLs for credentials.
Installed versions and content hashes are recorded by the smoke. This is an
engineering smoke, not a pilot or a confirmatory campaign.

## Historical native-counter runner

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

The historical runner has these limitations; use the known-truth contract for new engineering evidence:

1. **Original bytes**: img2dataset runs with `--disable_all_reencoding True`, `--extract_exif False` and `--resize_mode no`
2. **Native Timeouts**: Equal numeric values do not mean equal urllib/aiohttp timeout semantics
3. **Primary attempt budgets**: FLOW-DC uses one total attempt; img2dataset uses `retries=0` and `max_shard_retry=0`. Actual request work needs origin logs.
4. **Retained Outputs**: Every run has a new directory; collisions fail rather than delete evidence
5. **Warmup Runs**: Excluded from historical aggregates, but their artifacts remain retained
6. **Multiple Runs**: Historical averages do not implement the proposed paired independent-run study
7. **Sequential Execution**: Tools run in fixed order; randomized blocked scheduling remains future work

## Extending the Suite

### Adding a New Tool

1. Create `core/<tool>_adapter.py` with:
   - Config dataclass
   - `run()` async method returning `BenchmarkResult`
2. Update `core/runner.py` to call the new adapter
3. Add configuration options to YAML schema
