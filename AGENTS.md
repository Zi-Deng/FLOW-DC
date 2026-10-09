# FLOW-DC Agent Notes

## Scope

These instructions apply to the whole repository unless a deeper `AGENTS.md` overrides them.

## Repo Shape

- Treat `bin/` as the primary product code.
- Treat `benchmark/` as maintained support code for comparing FLOW-DC against `img2dataset`.
- Treat `playground/` as experimental and historical. Prefer not to change it unless the task explicitly targets it.
- Treat `files/config/` as example configs and `files/output/` plus `benchmark/results/` as generated/reference artifacts.

## Preferred Stack

- Prefer `polars` for dataframe work.
- Prefer `duckdb` for database or SQL-style data processing.
- Do not rewrite existing `pandas`-based utilities just for style unless the task is specifically about modernizing them.

## Current Downloader Truth

- The main maintained downloader is `bin/download_batch.py`.
- The current control algorithm is PAARC v2 with per-host adaptive concurrency.
- `bin/download_batch_multithread.py` is a comparison/alternate implementation; keep behavior aligned only when the task clearly requires parity.
- `bin/ui_app.py` is still a UI prototype with simulated job execution, not a fully wired production frontend.

## Config And Schema Discipline

- Config keys are duplicated across multiple places. When changing downloader CLI or JSON config behavior, inspect related code and examples, especially:
  - `bin/download_batch.py`
  - `bin/ui_app.py`
  - `bin/TaskvineFLOWDC.py`
  - `benchmark/core/flowdc_adapter.py`
  - `files/config/*.json`
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
- Both maintained downloaders prompt before deleting an existing output directory unless `--force` or JSON `force_overwrite` is set. Preserve that behavior. Automated TaskVine and benchmark jobs opt into overwrite only for their own output directories.

## Validation

- Run `python -m unittest discover -s tests -v` for the focused consolidation regression suite. It uses only a local HTTP server and temporary output directories; TaskVine config tests stub the runtime and do not validate cluster execution.
- For Python edits, prefer focused validation such as `python -m py_compile ...` and targeted script smoke checks over broad repo-wide commands.
- For benchmark changes, validate with a small local run rather than a full benchmark matrix unless the task asks for it.

## Documentation Reality Check

- Some documentation is aspirational or historical. Verify behavior against current code before changing implementation to match a doc.
- In particular, PAARC notes in `docs/` include both historical bug writeups and current design notes; use the code as the source of truth.

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
