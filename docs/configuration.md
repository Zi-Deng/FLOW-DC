# Configuration

`flowdc download --config PATH` reads a JSON object using the same `DownloadConfig`
as programmatic and TaskVine execution. Explicit CLI options override JSON values;
unknown keys and invalid values fail before output deletion or HTTP.

| Field | Default | Meaning |
|---|---|---|
| `input_path`, `output_dir` | Required | Manifest and owned output directory |
| `input_format` | Inferred | `parquet`, `csv`, `excel`, `xml` |
| `url_column`, `label_column` | `url`, null | Source metadata columns |
| `method` | `paarc` | `paarc` or `paarc-ratio` |
| `c_min`, `c_init`, `c_max` | 2, 4, 192 | Per-host request limits |
| `workers` | null | Downloader coroutine pool; null sizes from host count |
| `timeout_s` | 30 | Per-attempt HTTP deadline |
| `max_attempts` | 3 | Includes the first request attempt |
| `retry_backoff_s` | 2 | Delay between retry passes |
| `output_format` | `imagefolder` | Also supports `webdataset` |
| `naming_mode` | `row_id` | Also `sequential`, `url_based` |
| `create_tar`, `compress_archive` | true, false | Verified `.tar` or compressed archive |
| `create_overview` | true | Human-readable report |
| `force` | false | Explicit whole-output replacement consent |
| `http_connection_limit` | null | Transport connector cap, separate from admission |
| `research_profile` | false | Row IDs, WebDataset metadata and uncompressed verified archive |

`method_options` accepts the fields in `controllers.MethodConfig`, excluding method
and concurrency bounds. Shared defaults are: 1-s update opportunities, 2-s sample
window, at least five samples, 5-s stale gap, 120-s baseline age, growth factor 1.5,
delay budget twice baseline, hard reduction 0.5, soft reduction 0.85 and utilization
threshold 0.8. The complete typed model is the authoritative option list.

`resume` and `reconcile` are recovery state supplied by their CLI commands. They
cannot be combined with overwrite. Source, configuration and output ownership must
match. Reconciliation needs no input file or overview and creates no HTTP session.
Resume uses the durable absolute source path, or `--input-path` when explicitly
supplied; changing the path does not bypass the manifest hash check.

Acquisition commands write the result JSON to stdout and diagnostics/progress to
stderr. Existing-output confirmation also appears on stderr.

Manifests retain flat string/boolean/integer/float/null metadata. Unsupported types,
nonfinite research metadata and unsafe output names are rejected explicitly. XML
uses the optional pandas reader; installing format support does not affect the
controller. The manifest and row index are memory resident.

PAARC measures the **normalized excess-delay rate**: the change in smoothed excess
delay divided by elapsed seconds and the low-delay baseline. The raw and smoothed
rates have units of inverse seconds. `delay_rate_tau_s` defaults to 1 second;
`delay_rate_hold_per_s` and `delay_rate_decrease_per_s` default to 0.10 and 0.25
inverse seconds. Two consecutive eligible updates at the reduction threshold are
required. These names describe a finite difference over time, not an optimization
parameter. PAARC-Ratio shares the observation and exploration safeguards but uses
a bounded delay ratio for ordinary regulation.

New control events expose `raw_delay_rate_per_s`, `delay_rate_ewma_per_s`,
`delay_rate_alpha` and `decision_delay_rate_per_s`. Original historical names remain
in historical records only; see the [migration guide](migration.md).
