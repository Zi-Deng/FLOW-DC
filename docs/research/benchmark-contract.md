# Known-truth benchmark contract and issue #26 checkpoint

This records milestone A of
[issue #26's approved plan](https://github.com/Zi-Deng/FLOW-DC/issues/26#issuecomment-5884113901).
It is incomplete engineering work, not evidence of performance superiority or
completion of steps 2–5. The maintainer reports advisor approval; the specific
decisions and numerical constraints have not been supplied. No scientific protocol
is frozen and no pilot, confirmatory campaign, cloud action or journal migration
is authorized by this command.

## Versioned boundary

`benchmark/known_truth.py` invokes the maintained `bin/download_batch.py` and the
real `img2dataset==1.47.0` CLI in the same isolated research environment. It does
not replace either executable. FLOW-DC's research profile produces uncompressed
tar output. img2dataset uses WebDataset, disabled reencoding/EXIF, explicit native
process/thread settings, and saved original columns plus stable provenance.
Its `.jpg` member key can contain original PNG bytes: only independent byte hashes
and lengths establish payload credit. The settings follow the pinned upstream
[CLI](https://github.com/rom1504/img2dataset/blob/1.47.0/img2dataset/main.py),
[byte bypass](https://github.com/rom1504/img2dataset/blob/1.47.0/img2dataset/resizer.py),
[writer](https://github.com/rom1504/img2dataset/blob/1.47.0/img2dataset/writer.py) and
[replay loop](https://github.com/rom1504/img2dataset/blob/1.47.0/img2dataset/distributor.py).

The `flowdc-known-truth-v1` record uses integer bytes and integer monotonic
nanoseconds. Preparation, package/source hashing and fixture generation occur
before timing. Timing starts immediately before process creation and stops after
native exit/cleanup, independent artifact verification, and closure/readback of
the common outcome index. Process and verification component durations remain
separate; verification is included in the primary duration. Rendering and writing
the final timing report are excluded equally. No cross-machine clock subtraction
is involved. Offline re-verification does not reconstruct or replace run timing.

The process group receives bounded TERM/KILL cleanup on timeout or interruption.
Surviving or uncertain descendants prevent verification and successful completion.
A nonzero exit, timeout, interruption or missing/invalid artifact cannot become a
complete run. Verified partial payload credit and whole-run completion are separate.
Resources are explicitly `null/not_measured` in this initial record, not fabricated
zeros. Historical network counters are labelled system-wide, not downloader wire bytes.

Native logs, configurations, original input bytes, eligible input, catalog, generated
original payloads, native files, archives, native indices, origin events and common
outcomes are retained. Names must be new; neither rerunning the smoke nor an adapter
may delete prior evidence. The environment record includes exact installed versions,
installed-file/binary hashes and source file hashes. The private pip `--report`
records package acquisition provenance separately. Requirements pin the primary
research dependencies; the entire transitive environment is recorded after install,
not claimed to be a reproducible complete lockfile yet.

## Denominator and metadata precondition

`Truth.load` reads and hashes the original manifest bytes and validates its complete
schema before `Truth.write` creates output. Stable row IDs hash the original byte
digest and original position, using the product's documented provenance encoding.
Filtering invalid URLs preserves every original row in the common index and passes
parent identity/count/position to both native clients. Duplicate URLs and identical
content remain separate requested rows and receive separate useful credit only
when all required row artifacts verify.

This first schema requires `url: String`; column names use letters, digits and
underscores and may not collide with native/provenance fields. Supported nullable
metadata types are String, Boolean, Int64 restricted to ±(2^53−1), finite Float64,
and Null. Row metadata is capped at 64 KiB. Temporal, decimal, duration, narrow
integer/float, unsigned, binary, categorical and nested types are rejected; no
silent conversion is offered. This guard applies to this new benchmark preparation
path. It does not fix arbitrary typed metadata in legacy product paths (issue #25).

Null, blank, malformed, whitespace-bearing and credential-bearing URLs are common
skipped rows; they never disappear from the denominator. Eligible URLs must have
predeclared catalog truth. A null catalog entry denotes no successful payload.
Expected row payload is capped at 64 MiB and original row count at 256. This initial
CLI's default primary case generates nine rows, six eligible, using only loopback fixtures.

## Independent verification

The verifier does not use FLOW-DC's own verifier or img2dataset success counters to
award credit. It checks original metadata, identity, original byte length/SHA-256,
safe regular archive members, duplicate/missing/unexpected names, uncompressed tar
closure and native outcome sidecars. FLOW-DC's external final record must bind the
index and archive. img2dataset Parquet sidecars account for native failures; shard
stats must agree with those rows. Invalid required artifacts invalidate useful
archive credit. Missing, failed, skipped and invalid rows remain explicit.

Origin logs assign independent sequence IDs and origin-clock arrival/response
timestamps. The primary smoke compares aggregate requests by path against its
one-attempt budget. Duplicate URLs cannot be mapped to original row identities
from native request logs; the record explicitly leaves that attribution unavailable.
Configured retry budgets are not observations. `--case http-failure` adds 404,
429/503 with Retry-After, truncated bodies and delayed first bytes; `empty` checks
zero-byte responses; `retry` uses a predetermined failure-then-success sequence
with two total attempts; `deadline` terminates each process group at its outer
deadline. Policies and payload catalog are retained before either client launches.
Each client's counters reset only after the previous origin work quiesces. The
timeout cases check native semantics and recovery, not equal request deadlines.
`assessment.json` checks partial/failure accounting without requiring those native
runs to be successful. Capacity/service scenarios belong to milestone C.

## Historical compatibility

Existing reports without a benchmark schema are read as
`historical-native-counters-v1`. New historical-runner reports carry that label and
`comparison_eligible=false`; the historical comparison reader refuses the new
known-truth format. Stored measurements are no longer rounded; presentation may
round. Old reports retain their old values and meaning. Historical native timing,
extension-derived success and resource estimates remain unsuitable as the new
research boundary. `run_benchmark.py` and its help print this limitation.

## Combined implementation acceptance map

| Issue criterion | Implemented evidence | Still required |
| --- | --- | --- |
| 1: common truth | Independent schema/verifier, metadata guard, tampering tests; real primary fixture at `63ff9c7`; predetermined HTTP cases | Repeat real HTTP cases and primary on final head |
| 2: timing/work/retention | Common launch-to-verified-index timer, process cleanup, raw precision, retention, source/environment records, aggregate origin logs | Final-head evidence; resource collection if introduced |
| 3: methods/comparators | [Versioned methods](CONTROL-METHODS.md), same acquisition path, deterministic traces and five ablations; legacy defaults retained | Retained real HTTP trajectories on final head |
| 4: origin/study harness | [Controlled origin and study tools](STUDY-HARNESS.md): explicit service model, seven scenarios, frozen order, retained recovery/calibration | Final-head reruns; clean C already passed all scenarios, calibration and in-flight TERM |
| 5: pilot/freeze | Proposed plans/tuning catalog, run-level paired summaries/precision calculation, explicit freeze refusal | Actual advisor decisions and later scientific protocol/campaign |
| 6: shared-origin authority | [Authenticated ledger/HTTP hooks](SHARED-ADMISSION.md), real-client transport and process-restart entrypoints; aggregate limits, Retry-After, uncertainty and fencing | Final-head real-client, adversarial transport and TLS evidence |
| 7: distributed artifacts | [Native TaskVine profile](DISTRIBUTED-WORKFLOW.md), complete staging, owned finite cohorts, independent row/return reconciliation, actual 1/2/4-worker development evidence | Final-head native matrix; retained upstream 7.17.2 FORSAKEN crash remains an explicit failed-acquisition case |
| 8: bounded topology | [Offline 3/4/6-VM examples/plans](../BOUNDED-TOPOLOGY.md), UUID migration/replay/backup, 4→1→2 selection, per-VM mocked cleanup and fake-only systemd gates | Final-head operational gates; actual production migration/install/enrollment/live execution remain unperformed |
| 9: handoff | Contracts, CLI/examples, [human checkpoint packet](../PRODUCTION-CHECKPOINT.md), regression evidence and prepared coordinator commands | Final committed-head product/integration/service gates, both CI jobs and independent review |

`tests/test_benchmark_contract.py` is V1. Its constructed img2dataset-format
artifacts are not real img2dataset execution. The coordinator established V2 on
committed `63ff9c7306408ac31f59e08c92ff85c3df31b7b2`: both real tools verified six
payload rows, 2,157 original bytes, the nine-row denominator and six origin-observed
requests. At clean `23294685cd4bdb5453ff3fc5b539da3173250617`, the coordinator also
validated all four methods, primary/HTTP/empty/retry/deadline cases, twelve frozen
engineering cells, the remaining scenario families, calibration and in-flight TERM.
The full gate passed 472 product and 141 workflow tests, plus check-clean. These
A–C results do not validate the later shared-admission source or final combined head.
These are accounting and semantic checks, not efficacy evidence. The exact
commands, exits, source/environment hashes, retained failures and omissions are
indexed in the coordinator's private evidence and prepared PR body.

## Coordinator execution checkpoint

Run from the registered issue worktree with the same source head. Use existing
coordinator permissions and a task-local environment; do not widen the executor
sandbox. The following is a bounded engineering check, not a campaign:

```bash
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled .agentic-local/research-env/bin/python -B benchmark/known_truth.py --output benchmark/results/issue-26-primary-001
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled .agentic-local/research-env/bin/python -B benchmark/known_truth.py --case http-failure --output benchmark/results/issue-26-http-failure-001
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled .agentic-local/research-env/bin/python -B benchmark/known_truth.py --case empty --output benchmark/results/issue-26-empty-001
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled .agentic-local/research-env/bin/python -B benchmark/known_truth.py --case retry --output benchmark/results/issue-26-retry-001
NO_ALBUMENTATIONS_UPDATE=1 WANDB_MODE=disabled .agentic-local/research-env/bin/python -B benchmark/known_truth.py --case deadline --output benchmark/results/issue-26-deadline-001
make check PYTHON=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python RUFF=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/ruff
make check-clean PYTHON=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python
```

Retain the first failed invocation before choosing a new output name. Check both
native outcomes and `primary_attempts_match` in `smoke.json`; a process exit alone
is insufficient. The coordinator-provisioned environment is task-local; package
versions, hashes and acquisition reports are retained under `.agentic-local/research-setup`.
Source rollback must retain all versioned evidence. No operational state was
migrated or installed by this work. PR #27 is the recorded draft; milestone commits
and development gates are not final-head review readiness. Steps 2–5 are software
delivery. Advisor decisions, production installation/migration/enrollment and live
verification are separate unperformed checkpoints. Future steps 6–7 are the frozen
manuscript campaign and final paper/artifact reproduction, coauthor approval and submission.
