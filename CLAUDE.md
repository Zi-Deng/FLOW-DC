# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository. `AGENTS.md` holds the shared agent rules (repo shape, validation, workflow); read it too. Where a doc and the code disagree, the code is the source of truth.

## Coding Preferences

**IMPORTANT: Follow these preferences for all code in this repository:**

- **DataFrames**: Use **Polars** instead of Pandas for all dataframe operations. Polars is faster and more memory-efficient.
- **Database/SQL**: Use **DuckDB** for all database and SQL-based operations.
- Do not rewrite existing pandas-based utilities (e.g. `bin/CalcDatasetSize.py`, XML loading in `bin/single_download.py`) just for style unless the task is about modernizing them.

```python
# Preferred: Polars
import polars as pl
df = pl.read_parquet("file.parquet")
df.filter(pl.col("species").is_not_null())

# Preferred: DuckDB
import duckdb
conn = duckdb.connect()
result = conn.execute("SELECT * FROM 'file.parquet' WHERE species IS NOT NULL").pl()
```

DuckDB is not yet a dependency of any maintained module or of `.venv-agentic`; add it to the relevant requirements file if you introduce it.

## Project Overview

FLOW-DC (Flexible Large-scale Orchestrated Workflow for Data Collection) downloads large ML image datasets from URL manifests. It partitions a manifest (by host), runs per-partition download jobs (locally or as TaskVine tasks), and produces verified, reconciled outputs.

The core contribution is **PAARC (Policy-Aware Adaptive Request Controller)**, an application-layer, per-host concurrency controller driven by time-to-first-byte (TTFB) latency and overload signals (429/408/5xx/connection errors, `Retry-After`).

The repository also contains a research evaluation harness (`benchmark/`) and Jetstream2 cloud-operations tooling (`bin/flowdc_ops.py`, `bin/flowdc_pilot_*`, `bin/flowdc_experiment_*`) used for the in-progress manuscript.

## Architecture

```
bin/download_batch.py  (maintained async downloader, aiohttp)
 ├─ HostControllerManager ── one controller per host
 │    └─ PAARCController (or fixed / ratio / gradient method, see bin/flowdc_methods.py)
 │         ├─ AdaptiveSemaphore      per-host concurrency limit
 │         ├─ LeakyBucketSmoother    request spacing
 │         └─ HostMetrics            TTFB samples, RTprop, goodput
 ├─ bin/single_download.py
 │    ├─ download_single()          HTTP GET, TTFB measurement, redirects
 │    ├─ RetryAfterGate             per-(host, port) Retry-After embargo (per process)
 │    └─ load_input_file()          parquet / csv / excel / xml
 └─ bin/flowdc_integrity.py          .flowdc/ journal, per-row dispositions,
                                     resume/reconcile, verified archives
```

Control flow: worker coroutines acquire a slot from their host's controller → smoother spaces requests → `download_single()` performs the GET (Retry-After admission is enforced at dispatch and on each redirect hop) → metrics are recorded → the controller loop adjusts concurrency → every input row ends with exactly one disposition (verified / failed / skipped / unattempted) in the integrity journal.

## Common Commands

```bash
# Product environment (conda)
conda env create -f environment.yml
conda activate FLOW-DC

# Download with a JSON config (see files/config/ for examples)
python bin/download_batch.py --config files/config/spider_test.json
# Resume an interrupted run, or reconcile existing output offline (no HTTP)
python bin/download_batch.py --config files/config/spider_test.json --resume
python bin/download_batch.py --config files/config/spider_test.json --reconcile

# Partition a manifest by host (also: --method greedy|simple)
python bin/SplitParquet.py --parquet files/input/dataset.parquet --url_col url --groups 8 --output_folder files/partitions

# Estimate dataset size (HEAD requests on a sample)
python bin/CalcDatasetSize.py --input files/input/dataset.parquet --url_column url

# Distributed download with TaskVine
python bin/TaskvineFLOWDC.py --config files/config/taskvine_test.json

# UI prototype (NiceGUI; simulated jobs only)
python bin/ui_app.py

# Historical benchmark vs img2dataset
python benchmark/run_benchmark.py --quick --dataset files/input/sample.parquet
python benchmark/run_benchmark.py --tools flowdc --dataset files/input/test.parquet
```

Research-harness commands (`benchmark/known_truth.py`, `benchmark/study.py origin|run-cell|summarize|calibrate|freeze`) are documented in `benchmark/README.md` and `docs/research/STUDY-HARNESS.md`. Non-engineering `study.py` runs refuse to start without a frozen protocol record (`flowdc-frozen-protocol-v1`).

## Tests and Checks

`python` may not exist on this host and `.venv/bin/python` is broken; use the Makefile, which selects `.venv-agentic/bin/python` (Python 3.12) when present.

```bash
make test-flowdc     # product suite: unittest discover -s tests (~570 tests, ~2 min, local HTTP server only)
make test-agentic    # agentic workflow suite (scripts/agentic/check.py)
make check           # both suites + ruff on agentic code + repository checks
```

TaskVine tests stub the runtime; they do not validate cluster execution. `ruff` currently lints only agentic code, not `bin/` or `benchmark/`.

## Downloader Configuration

JSON config keys mostly mirror the CLI flags. Note: with `--config`, only `--force`, `--resume`, `--reconcile`, `--research_profile`, `--control_method`, `--method_options` and `--shared_control_file` are taken from the command line; other flags (e.g. `--disable_paarc`) are ignored. `--enable_paarc` is a no-op (PAARC is on by default).

```json
{
  "input": "files/input/dataset.parquet",
  "input_format": "parquet",
  "output": "files/output/images",
  "url": "photo_url",
  "label": "species",
  "output_format": "imagefolder",
  "concurrent_downloads": 0,
  "timeout": 30,
  "enable_paarc": true,
  "C_init": 4,
  "C_min": 2,
  "C_max": 10000,
  "theta_50": 1.5,
  "theta_95": 2.0,
  "create_tar": true,
  "create_overview": true
}
```

- `concurrent_downloads: 0` auto-sizes the worker pool from the number of unique hosts. If set, keep it ≈ `C_max` to avoid worker starvation.
- `output_format`: `imagefolder` (class subdirectories, for ML training) or `webdataset` (flat files + JSON metadata).
- `naming_mode`: `sequential` (default), `url_based`, or `row_id`.
- `--research_profile` forces row-ID naming, WebDataset output and a verified uncompressed tar.
- `--control_method`: `paarc-base-v2` (default), `gradient-candidate-v1`, `fixed-v1`, `ratio-v1`; per-method parameters go in `--method_options` (JSON). Example: `files/config/gradient-candidate-v1.json`.
- An existing output directory is not overwritten without confirmation unless `--force` / `force_overwrite: true`.
- Outputs include `overview.json`, `outcome-index.json`, an external `<output>_overview.json`, and `.flowdc/final.json` as the completion record.
- Legacy PolicyBBR keys (`enable_polite_controller`, `initial_rate`, `per_host_conc_*`, `gamma_*`) are not read by the current downloader; some older configs in `files/config/` still contain them.

## PAARC Parameters

Defaults from `PAARCConfig` and the CLI in `bin/download_batch.py`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `C_init` | 4 | Initial concurrency per host |
| `C_min` | 2 | Minimum concurrency floor |
| `C_max` | 10000 | Maximum concurrency ceiling |
| `mu` | 0.85 | Utilization factor (operating point = ceiling × μ) |
| `beta` | 0.5 | Multiplicative decrease on overload |
| `theta_50` | 1.5 | P50 latency threshold (× RTprop) in PROBE_BW; raise (e.g. 6.0) for high-latency servers |
| `theta_95` | 2.0 | P95 latency threshold (× RTprop) in PROBE_BW; raise (e.g. 10.0) for high-latency servers |
| `startup_theta_50` | 3.0 | P50 threshold for STARTUP plateau detection |
| `startup_theta_95` | 4.0 | P95 threshold for STARTUP plateau detection |
| `startup_additive_increase` | 30 | Concurrency added per interval in STARTUP |
| `probe_bw_additive_increase` | 10 | Concurrency added per stable interval in PROBE_BW |
| `probe_rtt_period` | 10.0 | Seconds between PROBE_RTT entries |
| `rtprop_window` | 35.0 | RTprop tracking window (s); JSON/dataclass only, no CLI flag |
| `cooldown_floor` | 2.0 | Minimum BACKOFF cooldown (s) |
| `alpha_ema` | 0.3 | EMA smoothing factor for latency |
| `max_retry_attempts` | 3 | Retries for retryable failures |
| `retry_backoff_sec` | 2.0 | Base retry backoff (s) |

Other entry points carry their own defaults (e.g. `TaskvineFLOWDC.py` uses `C_init` 8, `C_max` 2000, `probe_rtt_period` 30; `ui_app.py` uses `mu` 1.0). Defaults are duplicated across `download_batch.py` (dataclass, `Config`, argparse, JSON `.get`), `TaskvineFLOWDC.py`, `ui_app.py`, `download_batch_multithread.py` and `bin/flowdc_experiment_data.py` — change them together.

## PAARC State Machine

```
    INIT → STARTUP → PROBE_BW ↔ PROBE_RTT
              ↓           ↓
           BACKOFF ←──────┘
```

| State | Behavior |
|-------|----------|
| INIT | Collect `N_init` = 100 samples at `C_init` to establish the RTprop baseline; any overload error → BACKOFF |
| STARTUP | +30 per interval until a latency plateau (startup thetas) or `C_max`; operating point = ceiling × μ |
| PROBE_BW | Steady state, +10 per stable interval; latency degradation (theta_50/theta_95) reduces toward the operating point |
| PROBE_RTT | Every `probe_rtt_period` (after ≥100 samples): drop to 50% concurrency to refresh RTprop, restore in 2 steps |
| BACKOFF | ×β after overload; cooldown = max(5 × RTprop, `cooldown_floor`, Retry-After) |

- A single overload error (429, 408/timeout, 5xx, connection error) in a control interval triggers BACKOFF. Local/unknown failures are excluded.
- Control interval = max(k × RTprop, N_min / goodput, floor): k = 4, floor 0.1 s in STARTUP/BACKOFF; k = 8, floor 0.2 s otherwise.
- Plateau detection mode is the module constant `PLATEAU_DETECTION_MODE = "latency"`, not a config option.
- TTFB is measured from final-hop dispatch to the first non-empty body byte (`HTTP_MEASUREMENT_VERSION = "3-output-independent-latency"`).
- Several docstrings inside `download_batch.py` are stale (e.g. STARTUP "+1", μ 0.75); trust the code values above.

## Input/Output Formats

**Input**: Parquet (recommended), CSV, Excel, XML — must contain a URL column. Manifests and per-row provenance are held in memory, which bounds the practical manifest size.

**Output**: `imagefolder` or `webdataset`, optional `.tar`/`.tar.gz`, overview JSON, and the `.flowdc/` integrity journal.

## Implementation Status

| Script | Status |
|--------|--------|
| `bin/download_batch.py` | Primary, maintained, tested |
| `bin/download_batch_gradient.py` | Gradient candidate controller (subclasses the base controller) |
| `bin/SplitParquet.py` | Maintained (Polars); whole manifest in memory |
| `bin/TaskvineFLOWDC.py` | Maintained; stages the downloader modules via `bin/flowdc_staging.py` |
| `bin/TaskvineFLOWDCCloud.py` | **Stale**: stages only `download_batch.py` + `single_download_gbif.py`, but the downloader now also imports `flowdc_integrity` and `single_download`; uses legacy PolicyBBR keys. Do not rely on it without repair |
| `bin/download_batch_multithread.py` | **Diverged** (last changed Jan 2026): different growth rules and defaults, no integrity/resume/control methods, and it deletes an existing output folder **without prompting**. It does not share config semantics with `download_batch.py` |
| `bin/CalcDatasetSize.py` | Works; pandas-based |
| `bin/ui_app.py` | Prototype; job execution is simulated |
| `bin/flowdc_ops.py`, `bin/flowdc_pilot_*`, `bin/flowdc_experiment_*`, `bin/flowdc_vine_*` | Jetstream2 pilot lifecycle, accounting, experiment runner and native TaskVine tooling; see `docs/jetstream2/` |

## Dependencies

- Python 3.12 for development/tests (`requirements-test.txt`, `.venv-agentic`); `environment.yml`/README still state 3.10+.
- polars, pyarrow, aiohttp, tqdm, pandas (legacy utilities), NiceGUI (UI), psutil (benchmarks).
- TaskVine (ndcctools): `environment.yml` pins 7.15.8 (what the Jetstream2 guests run), while native-runtime code (`bin/flowdc_vine_native.py`) requires 7.17.2. Check which applies before changing TaskVine code.
- gocommands for iRODS/CyVerse transfers.
- Benchmarks: `benchmark/requirements*.txt` (img2dataset 1.47.0 etc.).

## Docs, Private Notes and Workflow

- `docs/PAARC_PERFORMANCE_BUGS.md` (Jan 2026): bugs #1–#3 fixed; #4 (ceiling revision) and #5 (PROBE_RTT restoration) are marked **partially fixed**.
- `docs/jetstream2/`, `docs/PRODUCTION-CHECKPOINT.md`, `docs/BOUNDED-TOPOLOGY.md`: cloud operations. Never activate, unshelve or reconfigure cloud resources without explicit maintainer authorization.
- `docs/research/`: evaluation protocol and study harness.
- `docs/agent-workflow/`: the issue → PR workflow (streamlined on 2026-10-09). Agents never merge; the maintainer does.
- `memory/` (Git-ignored) holds private project notes, manuscript packages and status reports; `paper/` (untracked) holds the imported manuscript draft. Do not commit either or copy them into reviewer snapshots.
- `archives/` and `playground/` are historical; do not edit them unless asked.

## Troubleshooting

**Excessive "latency degraded" messages:** loosen `theta_50`/`theta_95` (e.g. 6.0/10.0); a server's natural p95 may be 5–10× RTprop.

**PAARC not scaling up:** raise `startup_theta_50`/`startup_theta_95` for slow or high-variance servers.

**Frequent BACKOFF:** any single 429/5xx/timeout in an interval triggers it; check the overview's error breakdown and server `Retry-After` behavior.

**High failure rate with few retryable errors:** many 4xx (especially 404) indicate stale URLs; check `*_overview.json` and `outcome-index.json`.

**Low throughput:** keep `concurrent_downloads` at 0 (auto) or ≈ `C_max`; check whether latency thresholds are too tight.

**Interrupted run:** rerun with `--resume`; use `--reconcile` to rebuild dispositions offline without HTTP.

## Bounded review and repair policy

Use at most two automated review invocations and two review-driven repair rounds per task across providers; failed or interrupted invocations count. Plan the first review after implementation qualification and the final review after the remaining material changes. Ordinary development/test fixes and coauthor revisions are not extra review-driven repairs. After the last repair, report the last reviewed SHA, final SHA, changed delta and current-head CI for human assessment; never label an earlier review as final-head review. No automatic third review. Shelve remaining nonblocking findings in a concise existing or consolidated GitHub issue. Material acceptance failures remain blockers. Extra cycles require a documented credential compromise, data loss, uncontrolled spending, or defect invalidating required central evidence, and a bounded corrective scope. Existing finite provider limits and human-only merge/submission still apply.
