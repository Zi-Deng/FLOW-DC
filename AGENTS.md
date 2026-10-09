# FLOW-DC Agent Notes

## Scope

These instructions apply to the whole repository unless a deeper `AGENTS.md` overrides them.

## Repo Shape

- Treat `bin/` as the primary product code. Besides the downloader, it holds the Jetstream2 operations tooling (`flowdc_ops.py`, `flowdc_pilot_*`, `flowdc_experiment_*`, `flowdc_vine_*`, `flowdc_topology.py`); see `docs/jetstream2/`.
- Treat `benchmark/` as maintained support code for comparing FLOW-DC against `img2dataset`.
- Treat `playground/` as experimental and historical. Prefer not to change it unless the task explicitly targets it.
- Treat `files/config/` as example configs and `files/output/` plus `benchmark/results/` as generated/reference artifacts.
- `benchmark/known_truth.py` and `benchmark/study.py` are the research evaluation harness (`docs/research/`); `benchmark/run_benchmark.py` keeps the historical comparison contract.
- `memory/` (Git-ignored) holds private notes, status reports and manuscript packages; `paper/` (untracked) holds the imported manuscript draft. Never commit them or include them in reviewer snapshots.

## Preferred Stack

- Prefer `polars` for dataframe work.
- Prefer `duckdb` for database or SQL-style data processing.
- Do not rewrite existing `pandas`-based utilities just for style unless the task is specifically about modernizing them.

## Current Downloader Truth

- The main maintained downloader is `bin/download_batch.py` with `bin/single_download.py` (HTTP, TTFB timing, Retry-After admission) and `bin/flowdc_integrity.py` (`.flowdc/` journal, per-row dispositions, `--resume`/`--reconcile`, verified archives).
- The default control algorithm is PAARC v2 (`paarc-base-v2`) with per-host adaptive concurrency. `--control_method` also selects `gradient-candidate-v1`, `fixed-v1` and `ratio-v1` (`bin/flowdc_methods.py`). Current defaults are in `PAARCConfig` in `bin/download_batch.py` (e.g. startup thetas 3.0/4.0).
- `bin/TaskvineFLOWDC.py` is the maintained TaskVine orchestrator and stages the downloader modules through `bin/flowdc_staging.py`.
- `bin/TaskvineFLOWDCCloud.py` is stale: it stages only `download_batch.py` and `single_download_gbif.py` (the downloader now also needs `flowdc_integrity.py` and `single_download.py`) and uses legacy PolicyBBR keys. Repair it before relying on it.
- `bin/download_batch_multithread.py` has diverged (unchanged since January 2026): different growth rules and defaults, no integrity, resume or control-method support. Do not assume config or behavior parity with `download_batch.py`.
- `bin/ui_app.py` is still a UI prototype with simulated job execution, not a fully wired production frontend.

## Config And Schema Discipline

- Config keys are duplicated across multiple places. When changing downloader CLI or JSON config behavior, inspect related code and examples, especially:
  - `bin/download_batch.py` (defaults appear in the `PAARCConfig` dataclass, `Config`, argparse and JSON `.get` calls)
  - `bin/ui_app.py`
  - `bin/TaskvineFLOWDC.py` (has its own defaults, e.g. `C_max` 2000, `probe_rtt_period` 30)
  - `bin/flowdc_experiment_data.py`
  - `benchmark/core/flowdc_adapter.py`
  - `files/config/*.json`
- With `--config`, `download_batch.py` takes only `--force`, `--resume`, `--reconcile`, `--research_profile`, `--control_method`, `--method_options` and `--shared_control_file` from the command line; other CLI flags are ignored.
- JSON uses `timeout` while the internal config uses `timeout_sec`; some experiment configs use `timeout_sec`, which only the experiment runner accepts.
- Some older docs and configs still refer to legacy PolicyBBR names such as `enable_polite_controller`, `initial_rate`, or `per_host_conc_*`. Do not assume those names match the current PAARC implementation without checking the code.
- Keep `README.md` and example configs in sync when changing user-facing flags, config keys, or output report fields.

## Generated And Historical Files

- Avoid editing generated outputs unless the task explicitly asks for it:
  - `files/output/*.json`
  - `benchmark/results/**`
  - large text chunk outputs under `playground/biotrove_tar_paths/`
- Avoid touching data artifacts (`*.parquet`, images, tarballs, notebooks) unless the task specifically requires it.
- Be aware that `.gitignore` excludes some directories that still contain tracked or useful reference material.

## Code Change Guidelines

- Keep changes surgical and consistent with the existing style of the file you are editing.
- Preserve backward-compatible config behavior unless the task explicitly allows breaking changes.
- If you change download behavior, also check retry logic, overview generation, tar creation, and host-controller interactions.
- If you change file naming or output layout, review both downloader helpers and downstream consumers of overview/config fields.
- `download_batch.py` prompts before deleting an existing output directory unless `--force` or JSON `force_overwrite` is set. Preserve that behavior. Automated TaskVine and benchmark jobs opt into overwrite only for their own output directories. `download_batch_multithread.py` currently deletes an existing output folder without prompting; treat that as a known defect, not a pattern to copy.

## Validation

- Run `make test-flowdc` for the product regression suite (`unittest discover -s tests`, about 570 tests in about two minutes). The Makefile uses `.venv-agentic/bin/python` (Python 3.12) when present; plain `python` may not exist on this host and `.venv/bin/python` is broken. The suite uses only a local HTTP server and temporary output directories; TaskVine tests stub the runtime and do not validate cluster execution.
- `make check` adds the agentic workflow suite, `ruff` (agentic code only) and repository checks.
- For Python edits, prefer focused validation such as `python -m py_compile ...` and targeted script smoke checks over broad repo-wide commands.
- For benchmark changes, validate with a small local run rather than a full benchmark matrix unless the task asks for it.

## Documentation Reality Check

- Some documentation is aspirational or historical. Verify behavior against current code before changing implementation to match a doc.
- In particular, PAARC notes in `docs/` include both historical bug writeups and current design notes; use the code as the source of truth. `docs/PAARC_PERFORMANCE_BUGS.md` marks bugs #4 and #5 as only partially fixed.
- Several docstrings inside `bin/download_batch.py` describe older values (e.g. STARTUP "+1", μ 0.75); the dataclass and argparse defaults are authoritative.
- Dependency pins disagree: `environment.yml` pins TaskVine (ndcctools) 7.15.8, which the Jetstream2 guests run, while `bin/flowdc_vine_native.py` requires 7.17.2. README and `environment.yml` say Python 3.10+; tests run on 3.12.

## Cloud Operations

- Jetstream2 resources are real and billed. Do not activate, unshelve, resize or reconfigure VMs, networks or credentials, or grant allowances, without explicit maintainer authorization for that operation.
- Read-only inspection and offline preparation are fine. Follow `docs/jetstream2/README.md` and `docs/jetstream2/LIFECYCLE.md`; only `SHELVED_OFFLOADED` counts as cleaned up.
- The local `flowdc-pilot` systemd user service supervises the pilot. Do not stop, restart or upgrade it as a side effect of another task.

## Consolidation Archives

- `archives/2026-09-14/pre-consolidation/` is an immutable, Git-ignored local backup of source files, Git history, and benchmark results. Do not edit those snapshots.
- `archives/2026-09-14/legacy/` holds historical PolicyBBR notes and old example configs. Treat them as references, not current runnable examples.
- `benchmark/` source and manifests are now tracked; only generated results are excluded.

## Agentic workflow operating instructions

Use the streamlined workflow in `docs/agent-workflow/OPERATING-GUIDE.md`.
The maintainer explicitly replaced the old generation/compatibility/coverage rules
on October9,2026. This applies to this task and all future tasks.

- Implement directly in the assigned issue worktree; preserve user edits. Use a
  separate executor only when it helps. Session continuity is optional, not a gate.
- Use the issue and concise current plan for scope. Existing user authorization
  persists. Do not ask again or create per-phase approval generations.
- Maintain one current implementation. Replace obsolete code/tests; Git history
  retains prior versions. Old reports are historical, not current qualification.
- Routine changes need affected tests; substantial changes need one local check
  pass and current CI. Do not repeat serial/parallel/installed matrices or hash
  every repository file before commits. Repeat only checks affected by a fix.
- Use one fresh bounded independent static review for substantive behavior,
  credentials, dependency or workflow changes. Claude Code is default; explicit
  Copilot remains selectable. Record head, provider/model, findings and limitations.
  A review is professional judgment, not proven exhaustive line inspection.
- No separate paid capability/coverage campaigns, generation-specific grants,
  automatic provider/model fallback, or unbounded paid retries. Stop a failed call
  and diagnose it; continue independent work. Ask only for genuinely missing
  authority/information, not for authorized routine continuation.
- Keep credentials/private memory/datasets out of commits and reviewer snapshots.
  Reviewer tools are read-only; tests execute separately. Included Max usage only
  and paid-extra/API$0 remain the Claude boundary. Use a dedicated login profile.
- Agents never merge or submit papers. Keep current-head CI and human merge.
  Preserve operational grants/accounting and scientific evidence requirements.
- Workflow budget: <=2,500 runtime lines, <=1,200 workflow-test lines; suite target
  <=120seconds locally, CI job<=5minutes. New workflow complexity must justify its
  benefit to FLOW-DC. Do not increase limits or add a framework merely to pass.
