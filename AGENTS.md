# FLOW-DC repository instructions

## Source and scope

- `src/flowdc/` is the maintained product. PAARC is the default controller; PAARC-Ratio is its explicit alternative. They share one
  HTTP, admission, journal and archive pipeline. Controller defaults live in
  the typed configuration and policy model; CLI and TaskVine use those models.
- `experiments/` contains explicit protocols and an isolated comparator environment.
  `paper/` contains the current manuscript, compact evidence and offline build tools.
- Prefer Polars for dataframe work and DuckDB for new SQL work. Do not add either
  dependencies or abstractions without a concrete consumer.
- Keep module boundaries explicit and imports package-relative. Product code must
  not modify `sys.path`. CLI commands should delegate to testable functions.
- Reject invalid configuration before output mutation or HTTP. Preserve overwrite
  confirmation, per-row identity, Retry-After, request budgets and no-follow storage.
- Keep historical observations separate from new measurements. Do not manufacture
  completed results, raw records or submission readiness from software checks.

## Validation

- Install `.[research,dev]` in a dedicated development environment.
- `make check`: scoped lint/format, product tests, workflow tests and repository checks.
- `make paper`: build/check the current article, supplement and standalone distribution.
- `make -C paper reproduce`: two isolated builds and source-ZIP reproduction.
- Native tests require `FLOWDC_NATIVE_PYTHON`, `FLOWDC_NATIVE_WORKER` and `FLOWDC_NATIVE_RECEIPT` pointing to
  the qualified TaskVine runtime. Ordinary tests do not establish cluster execution.
- Run affected checks during development, one complete local pass for substantial
  changes, and current-head CI. Repeat only checks affected by later changes.
- Operational runtime files preserve immutable-release formats. Avoid cosmetic
  rewrites of those standalone modules; their tests exercise installed-release
  and accounting compatibility. Test-only import paths are not product interfaces.

## Operations and private records

- Never stop, restart or upgrade the installed `flowdc-pilot` service as a side effect.
- VM activation, resource/network/credential changes, allowance grants and each email
  require explicit maintainer authorization for that operation.
- Only fresh `SHELVED_OFFLOADED` observations establish completed cloud cleanup.
- Preserve credentials, durable accounting, original attempts and installed releases.
  Keep bulk artifacts and private evidence outside the repository. Build output
  belongs under ignored `build/`; never include private records in review snapshots.

## Development workflow

Follow `docs/agent-workflow/OPERATING-GUIDE.md`. Work in an issue worktree and preserve
user edits. Existing authorization persists; do not create per-phase approval chains.
Keep one current implementation and remove superseded consumers/tests together.
Use one fresh bounded independent static review for substantive changes, with at
most two invocations and two review-driven repairs per task; failed calls count.
No automatic paid fallback, third ordinary review or provider capability campaign.
Included usage only; paid extras and API spending remain zero. Record reviewed/final
revisions, material findings, limitations and current CI. Agents never merge or submit
papers. Maintain the existing workflow size/time budgets.

## Method names and evidence

- Current configurations use `paarc` or `paarc-ratio`; do not restore retired aliases.
- Describe the controller signal as the normalized excess-delay rate, in inverse seconds.
- Original names in provenance, frozen transition fixtures, historical verification,
  migration examples and verbatim historical records are intentional. Do not rewrite
  source identities or measurements to eliminate these historical strings.
- Compact paper inputs retain reversible original fields; preserve their prior-byte
  provenance checks and all incomplete/author-reported dispositions.
