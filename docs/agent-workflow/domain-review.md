# FLOW-DC domain and workflow review

Passing software tests establishes only the behavior those tests exercise. A change to collection policy, controller tuning or benchmark interpretation may require additional evidence even when CI passes. Use the current code and [repository notes](../../AGENTS.md) as the behavior reference; historical PolicyBBR documents are not the current PAARC contract.

## Maintained downloader and configuration

| Area | Questions and evidence |
| --- | --- |
| Entry points | Does the change target `bin/download_batch.py`, the maintained downloader? Is parity with alternates explicitly in scope? The UI remains a simulated prototype. |
| PAARC behavior | Check per-host isolation, adaptive concurrency bounds, sample-aware scheduling, idle snapshots, latency sample validity and controller timing. Explain the trigger and measurable effect of tuning changes. |
| Retries and failures | Preserve retryable versus non-retryable status handling, bounded retry behavior, cancellation and accounting of failed attempts. Test transient server failure and terminal failure separately. |
| Overwrite consent | Both maintained downloaders must prompt before replacing an existing output directory unless CLI `--force` or JSON `force_overwrite` permits it. Automated jobs may opt in only for directories they own. |
| Config compatibility | Inspect downloader CLI/JSON, UI, TaskVine builder, benchmark adapter and example configs together. Verify legacy aliases against current code; do not silently revive obsolete PolicyBBR semantics. |
| Output integrity | Check payload bytes, filenames, imagefolder/webdataset layout, compressed and uncompressed tar creation, and embedded/external overview agreement. Trace downstream consumers before renaming fields. |
| Resource behavior | Check worker/connection-pool sizing, per-host backpressure, bounded memory and temporary-file cleanup. Importing a module must not unexpectedly install process signal handlers. |
| Distributed execution | Configuration tests stub TaskVine. A cluster claim needs an authorized small live job, runtime versions, scheduler/worker logs and output verification. |

The required local gate is:

```bash
make test-flowdc
```

The eight current tests use a local HTTP server and temporary directories. They cover both the base and gradient implementations where consolidation required parity, output formats, retries, overwrite behavior, benchmark-owned output, configuration defaults, controller scheduling/snapshots and import behavior. They do not establish throughput on the public internet or a remote cluster.

## Benchmark and research claims

Treat `benchmark/` as maintained support code and its generated results as evidence with provenance. A fair FLOW-DC versus img2dataset comparison should specify:

- Identical input manifests and filtering rules, sample counts, URL/host distributions and expected payload semantics.
- Comparable hardware, storage, network conditions, process/worker budgets and total attempt policy. Record whether caches are warm and how ordering or time-of-day effects were controlled.
- Explicit definitions and denominators for successful samples, failures, downloaded bytes, elapsed time, throughput and latency summaries. Avoid conflating compressed archive size with transferred payload bytes.
- The independent repeat unit, seeds or input ordering, uncertainty method, and exclusions. Report controller warm-up and failed runs rather than selecting only favorable measurements.
- Exact commits, command arguments, configuration/input hashes, environment records and retrievable result artifacts.

An old benchmark remains evidence for the code/configuration that produced it. Do not relabel archived April results as measurements of the September consolidation. A performance hypothesis can justify a targeted local smoke test; a superiority claim requires a suitable comparative experiment. Authorize an expensive dataset, cluster or GPU campaign explicitly before running it.

## Workflow changes

Git operations must preserve unrelated edits, recovery stashes, unmerged commits and ignored artifacts. A task must retain its original executor UUID across repair; an incomplete or failed run cannot be reported as completed. Reviews must be tied to current head/base, use separate model context and disclose omitted files or unexecuted checks. Missing authentication, model access or GitHub checks is a blocker with a recovery action, not a successful validation.

Private memory and machine state must not enter Git, model review packets or public artifacts. Review snapshots export Git blobs with numeric filenames, never follow symlinks/submodules, and refuse known private-path diffs. This is a path guard, not a general content-secret detector. Inspect the committed diff and do not transmit private dataset URLs, credentials or restricted data merely because a path is allowed.

## Record an authorized validation run

The optional `scripts/agentic/evidence.py` runner executes a supplied argv without a shell and records a clean starting commit, command, timestamps, exit status, configuration/input/environment hashes and declared output hashes. Invoke it from a committed worktree with real manifest paths and a new output directory. Inspect `--help` for required arguments; do not invent a project smoke script that does not exist.

A manifest binds files to a run record. It does not prove that an artifact was freshly produced, establish scientific validity or replace inspecting the actual output. Keep large or restricted artifacts in approved storage, with small immutable manifests and retrieval instructions in the public task record when appropriate. Keep `archives/2026-09-14/pre-consolidation/` immutable.
