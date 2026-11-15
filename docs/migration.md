# Interface migration

FLOW-DC 3 uses the installable `flowdc` command. Obsolete script wrappers and
controller variants are removed; configuration is validated consistently.

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
| `control_method` | `method`: `paarc-grad` or `paarc-ratio` |

Historical selectors remain in the evidence mapping; they are not accepted as new
public method names. Explicit CLI values override JSON instead of being silently
ignored when a config file is present. Unknown legacy keys fail with a migration
message. Existing output journals are not silently migrated or rebound. Use the
recorded original runtime for historical recovery.

The current paper lives directly under `paper/`, with all build output under
`build/paper/`. Its build no longer requires private notes, imported drafts or an
older worktree. Bulk scientific artifacts belong outside the checkout.
