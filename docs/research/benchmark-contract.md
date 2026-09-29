# Known-truth benchmark contract and issue #26 checkpoint

This implements the first portion of milestone A of
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
CLI generates nine rows, six eligible, using only loopback fixtures.

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
Configured retry budgets are not observations. Retry stress, interrupted transfers
and origin service/admission scenarios remain open work, not inferred results.

## Historical compatibility

Existing reports without a benchmark schema are read as
`historical-native-counters-v1`. New historical-runner reports carry that label and
`comparison_eligible=false`; the historical comparison reader refuses the new
known-truth format. Stored measurements are no longer rounded; presentation may
round. Old reports retain their old values and meaning. Historical native timing,
extension-derived success and resource estimates remain unsuitable as the new
research boundary. `run_benchmark.py` and its help print this limitation.

## Acceptance map at this checkpoint

| Issue criterion | Implemented evidence | Still required |
| --- | --- | --- |
| 1: common truth | Independent schema, manifest guard, native artifact adapters; actual FLOW-DC publication verified offline; tampering fixtures | Real two-executable V2; broader HTTP failure/retry/interruption cases |
| 2: timing/work/retention | Common launch-to-verified-index timer, process cleanup, raw precision, retention, source/environment records, aggregate origin logs | Real-origin attempt evidence; verified dependency resolution; resource collection if introduced |
| 3: methods/comparators | No change to legacy controller defaults | Entire milestone B, formulas/traces, shared acquisition selection, ablations |
| 4: origin/study harness | Small concurrent success-fixture origin only | Capacity/service model, scenario families, blocked schedule, calibration, resume |
| 5: pilot/freeze | Advisor decisions explicitly pending | Study/tuning/evaluation plans, paired summaries, precision calculation, freeze gate |
| 6: shared-origin authority | Not implemented | Authentication, admission, fencing/recovery, concurrent client evidence |
| 7: distributed artifacts | Not implemented | Maintained staging and real TaskVine 1/2/4-worker integration |
| 8: bounded topology | Not implemented; existing accounting untouched | Offline UUID-account migration and 3/4/6-VM fixtures |
| 9: handoff | This contract, CLI help, regression evidence, prepared coordinator commands | All remaining implementation, final-head product/service gates, both CI jobs, independent review |

`tests/test_benchmark_contract.py` is V1. Its constructed img2dataset-format
artifacts are not real img2dataset execution. The socket-denied executor has not
established V2 for either executable. The exact environment-specific commands,
exit statuses and omitted checks are in the coordinator's prepared PR body.

## Coordinator execution checkpoint

Run from the registered issue worktree with the same source head. Use existing
coordinator permissions and a task-local environment; do not widen the executor
sandbox. The following is a bounded engineering check, not a campaign:

```bash
/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python -m venv .agentic-local/issue-26/coordinator-research
.agentic-local/issue-26/coordinator-research/bin/python -m pip install --report .agentic-local/issue-26/coordinator-install.json -r benchmark/requirements-research.txt
.agentic-local/issue-26/coordinator-research/bin/python -B benchmark/known_truth.py --output benchmark/results/issue-26-known-truth-001
make check PYTHON=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python RUFF=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/ruff
make check-clean PYTHON=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python
```

Retain the first failed invocation before choosing a new output name. Check both
native outcomes and `primary_attempts_match` in `smoke.json`; a process exit alone
is insufficient. Source rollback must retain all versioned evidence. No operational
state was migrated or installed by this checkpoint. Resume the original managed
Astra session for the remainder of A–D after recording/publicizing the draft.
