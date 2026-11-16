# Configuration

`flowdc download --config PATH` reads a JSON object using the same `DownloadConfig`
as programmatic and TaskVine execution. Explicit CLI options override JSON values;
unknown keys and invalid values fail before output deletion or HTTP.

| Field | Default | Meaning |
|---|---|---|
| `input_path`, `output_dir` | Required | Manifest and owned output directory |
| `input_format` | Inferred | `parquet`, `csv`, `excel`, `xml` |
| `url_column`, `label_column` | `url`, null | Source metadata columns |
| `method` | `paarc-grad` | `paarc-grad` or `paarc-ratio` |
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
