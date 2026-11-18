# Interface migration

FLOW-DC 4 uses the installable `flowdc` command. Obsolete script wrappers and
controller variants are removed; configuration is validated consistently.

## FLOW-DC 3 to 4

PAARC is the default controller previously named PAARC-Grad. This is a naming change,
not a new regulation algorithm. PAARC-Ratio remains an explicitly selected alternative.
Retired method and option spellings are rejected rather than accepted as aliases.

| FLOW-DC 3 | FLOW-DC 4 |
|---|---|
| `method: paarc-grad` | `method: paarc` |
| `gradient_tau_s` | `delay_rate_tau_s` |
| `gradient_hold_per_s` | `delay_rate_hold_per_s` |
| `gradient_decrease_per_s` | `delay_rate_decrease_per_s` |
| `gradient_ewma_per_s` | `delay_rate_ewma_per_s` |
| `derivative_per_s` | `raw_delay_rate_per_s` |
| `gradient_alpha` | `delay_rate_alpha` |
| `decision_gradient_per_s` | `decision_delay_rate_per_s` |
| `gradient_hold`, `gradient_persistence_hold`, `gradient_decrease` | Corresponding `delay_rate_*` decision reasons |

Rename configuration fields only when preparing a **new** run or study. Never edit
an existing ownership record, frozen protocol, source hash or control trace to make
it look current. New runs use `flowdc-package-v2` and new traces use
`flowdc-control-trace-v2`. `flowdc verify` can inspect supported earlier outputs
without modifying them. Earlier-version resume/reconcile and frozen-study analysis
require the recorded original runtime; their records are not automatically migrated.
The unchanged historical controller selectors remain explicit provenance identifiers.

## Earlier script interfaces

| Previous interface | Current interface |
|---|---|
| Batch downloader script | `flowdc download` |
| Partition script | `flowdc partition` |
| `--resume` / `--reconcile` | `flowdc resume` / `flowdc reconcile` |
| TaskVine script | `flowdc download --backend taskvine` |
| Operations script | `flowdc ops` |
| Several historical study runners | `flowdc experiment prepare/run/verify/analyze` |
| JSON `input`, `output` | `input_path`, `output_dir` |
| `url`, `label`, `timeout` | `url_column`, `label_column`, `timeout_s` |
| `concurrent_downloads` | `workers` (null for automatic sizing) |
| `C_min`, `C_init`, `C_max` | `c_min`, `c_init`, `c_max` |
| `max_retry_attempts` | `max_attempts` (includes the first attempt) |
| `force_overwrite`, `compress_tar` | `force`, `compress_archive` |
| `control_method` | `method`: `paarc` or `paarc-ratio` |

Historical selectors remain in the evidence mapping; they are not accepted as new
public method names. Explicit CLI values override JSON instead of being silently
ignored when a config file is present. Unknown legacy keys fail with a migration
message. Existing output journals are not silently migrated or rebound. Use the
recorded original runtime for historical recovery.

The current paper lives directly under `paper/`, with all build output under
`build/paper/`. Its build no longer requires private notes, imported drafts or an
older worktree. Bulk scientific artifacts belong outside the checkout.
