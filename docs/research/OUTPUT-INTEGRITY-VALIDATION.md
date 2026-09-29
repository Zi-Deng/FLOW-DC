# Output integrity validation — issue #22

## PR #24 implementation handoff

The approved [issue-22 contract](https://github.com/Zi-Deng/FLOW-DC/issues/22#issuecomment-5881275556)
is implemented in this worktree, following observation checkpoint
`06860add5567bdc1417546032a07b1ea0bee42aa`. Validation remains **blocked**, not complete:
required localhost tests cannot start in this executor, and both CI jobs require
coordinator publication/observation. [PR #24](https://github.com/Zi-Deng/FLOW-DC/pull/24)
remains a draft. This is neither independent review nor merge readiness.

The [protocol specification](OUTPUT-INTEGRITY.md) defines schema 2, the selected
completion boundary, compatibility changes and limits. Base and gradient now share
acquisition/reconciliation. The first checkpoint's D2/D3 timing correction is retained;
this increment adds identity, contained publication, recovery, artifacts and consumers.
The historical first-checkpoint record below is preserved as chronology, not the current
implementation status. No archived dataset/results or workflow source was changed.

### Criterion-to-evidence mapping

| Approved criterion | Implementation and evidence |
| --- | --- |
| Original identity/counts | `stamp_frame`, pre-filter loading and parent-preserving partition CLI. Tests retain null/blank/invalid/all-invalid/empty inputs, repeated URLs, external IDs, parent positions and metadata; conflicting provenance rejects force before deleting a sentinel. |
| Dispositions/attempts | One indexed row per original ID; persisted intents, terminal failures and retry budgets. Tests cover verified/failed/skipped/unattempted, cancellation, interrupted missing intent, exhausted budgets and repeated reconciliation without duplicate credit. |
| Collisions/containment | Preflight payload/metadata/reserved aliases; descriptor-based no-follow exclusive writes/links. Tests preserve pre-existing same-content files and symlinks, reject unsafe/encoded labels and demonstrate distinct row-ID destinations. |
| Recoverable publication | Seven injected publication cuts, actual child `SIGKILL` after payload publication, malformed/torn ownership/intent evidence, unknown journal IDs and repeated recovery. Only verified staged components can finish publication. |
| Offline reconciliation/resume | Tests forbid HTTP session construction during offline/no-work resume, remove original input for offline inspection, preserve prior attempts, skip verified rows, and reject manifest/config mismatch. |
| Final artifacts/bytes | Independently reopen archive payloads and compare fixture bytes/SHA-256; missing/extra/duplicate/corrupt/truncated members and hidden tails fail. Exactly 1,000,000 per-row bytes versus 500,000 unique-content bytes. Archive/report/last-completion failures cannot declare complete. Managed helper archives exclude staging and unowned files. |
| D2/D3 | Seven observation tests plus managed actual staged metadata/stat fault paths, both controllers, completed-body sample retained, zero useful bytes/local failure/no overload. Existing empty/incomplete/invalid timing checks remain. Real localhost variant is present but blocked. |
| Compatibility | TaskVine staging/config tests; committed-source missing-dependency and historical-source tests; partition CLI; schema-2 artifact parser and exact integer adapter with historical schema-1 conversion unchanged. Existing HTTP/consolidation/guest socket assertions remain enabled. |
| D5 chronology | HTTP validation note distinguishes executor handoff from later coordinator evidence on final PR-21 head `5fbe2cb027a55f8a8906173b9b765b73578e7d5a` and merged `32b5ace5b47d2c660557fe89b6fc37c8f02ca5c6`. No new execution on old heads is claimed. |

`tests/test_integrity_protocol.py` has 28 methods; `test_output_integrity.py` has seven.
Controlled HTTP doubles support filesystem fault isolation and do not substitute for
blocked real socket tests. Partition tests run real child CLIs; termination tests kill
a real child after a publication event. All fixtures are temporary owned local files.
Existing HTTP timing fixtures now use distinct output directories because intentional
collision rejection must not obscure timing assertions. Metadata failure is introduced
after the real closed payload/stat, preserving the original D3 assertion path.

### Regression chronology

Before protocol source changes, three `BaseRegressions` methods failed on checkpoint
`06860ad` (which retained the affected merged-base loading/collision paths): duplicate
filename overwrote payload, invalid input deleted existing output with force, and
filtering reduced five original rows to three. Exit **1**, three assertion failures.
After implementation, the same three tests were also confirmed against isolated copies
of the exact merged-base `download_batch.py`, `download_batch_gradient.py` and
`single_download.py` from `32b5ace5b47d2c660557fe89b6fc37c8f02ca5c6`: exit **1**, the
same three assertion failures. The new helper is present only so the test module can
import; those three tests call base entrypoints and do not invoke new protocol APIs.
This latter confirmation occurred after implementation, not before it. The first
checkpoint's separate D2 failing-base evidence is preserved below.

The exact isolated unittest invocation is
`$PY -B -m unittest discover -s tests -p test_integrity_protocol.py -k BaseRegressions -v`,
from a temporary directory containing the three `git show BASE:bin/FILE` outputs,
current `bin/flowdc_integrity.py` and `tests/test_integrity_protocol.py`. No real task
branch/worktree was reset or removed. Logs: `/tmp/issue-22-protocol-base.log` and
`/tmp/issue-22-merged-base-regression.log`.

### Environment, commands and exits

All checks ran from the assigned worktree on September 29, 2026 UTC, using the existing
read-only Python 3.12.12 environment, aiohttp 3.13.3, Polars 1.37.1 and YARL 1.24.5.
`PY=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python` and
`RUFF=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/ruff` below are exact executable
aliases. No packages, sandbox permissions or workflow files were changed. The only
NumPy use in the touched partition path was replaced by equivalent first-minimum
standard-library selection; grouping behavior is tested without installing NumPy.

Checks below ran on the implementation working tree with Git HEAD at `06860ad`; they
do not claim that unchanged checkpoint source implements the new protocol. Runtime/test
hashes below identify the checked content. The coordinator handoff records the resulting
commit and compares its Git blobs to these hashes. A pre-commit check is not CI evidence.

| Command | Exit / result |
| --- | --- |
| `PYTHONPATH=tests $PY -B -m unittest test_integrity_protocol test_output_integrity test_http_measurement.GateTests test_http_measurement.ClassificationTests test_flowdc_experiment_source test_flowdc_experiment -v` | **0**, 97 tests, 17.430 s; focused final log `/tmp/issue-22-focused-final.log`. |
| `$RUFF check bin/flowdc_integrity.py tests/test_integrity_protocol.py tests/test_output_integrity.py tests/test_http_measurement.py` | **0**. |
| `$PY -m py_compile` with all 17 Python paths in the hash table below | **0**; exact argument list is the table order. |
| `make check PYTHON=$PY RUFF=$RUFF` | **2**; full product unittest exits **1**, 393 tests, 76.263 s, 28 socket setup errors, no assertion failures/skips. Log `/tmp/issue-22-full-final.log`. Workflow targets are not reached. |
| `make check-agentic PYTHON=$PY RUFF=$RUFF` | **0**; 141 workflow tests, 23.333 s, scoped lint/format and repository checks. Log `/tmp/issue-22-agentic-final.log`. |
| `$PY -B scripts/check_repository.py` | **0**, links/configuration valid after new protocol documentation was added. |
| `git diff --check` | **0**. |
| `make check-clean PYTHON=$PY` | Run at the clean committed handoff; exact commit/exit is in the coordinator handoff. |
| CI `flowdc-tests`, `agentic-quality` | **Not run/observed by executor**; coordinator must publish and verify both on final head. |

The full gate already invokes `$PY -B -m unittest discover -s tests -v`; it is counted
once. Focused tests overlap it and are not additional distinct full-gate tests. An earlier
intermediate full gate ran 392 tests with the same 28 socket errors before the last
managed-archive/journal guard test was added. An intermediate `check-agentic` ran all
141 workflow tests successfully but exited **2** because README linked the not-yet-written
protocol document; the final command above resolves that documentation-order failure.

The denied capability is AF_INET socket creation: `PermissionError: [Errno 1] Operation
not permitted`. aiohttp reports `could not bind on any address out of [('127.0.0.1', 0)]`.
The 28 errors are consolidation class setup, one actual-downloader experiment fixture,
and 26 localhost HTTP methods. No socket tests were removed or bypassed. The coordinator
must run identical focused/full commands in its existing localhost-capable environment,
then the clean gate and both CI jobs on the published commit. Independent review and
human acceptance of accounting semantics remain subsequent required stages.

### Checked runtime and test hashes

| File | SHA-256 |
| --- | --- |
| `bin/flowdc_integrity.py` | `4438bd9e676e1bbd55c4370f4a55ad035b12be024f1b99aa6f6fd11bfb684815` |
| `bin/download_batch.py` | `6f88552c9f1d576c4153c4c1b73bc77eda97313b990dc20dfe2e3f4c2f859f4f` |
| `bin/download_batch_gradient.py` | `8de54da26f7e6041574673256a25efadf06c500adece01da162cf4119075c08e` |
| `bin/single_download.py` | `248e5c89266d38e9e8c551823c569e2e22b77d9965f61ec172063bca1523ea68` |
| `bin/TaskvineFLOWDC.py` | `b7d6c4822cbd7d961f96bb6bd90e009a341c2353c7946396183227bb72f1a641` |
| `bin/SplitParquet.py` | `96fd042bf3c95f40e70edd901b78dcc38d549b810b623c21d8c456e3b32b3fd4` |
| `bin/flowdc_experiment.py` | `7164c6fcf62099428d7f5dc23b6c1f98ab6ab8f2109a99e3f8f1d61ab04d7818` |
| `bin/flowdc_experiment_artifacts.py` | `368096a124e890505734c2dfc8f4bc06ba58e56b9a2769473ec493e13214ea72` |
| `bin/flowdc_experiment_data.py` | `0da88abaa15aebcf9a357c009ca6dc1fd720f55ae5252e6c43d76176472c017c` |
| `bin/flowdc_experiment_fixture.py` | `bea743441bbe6e853368954ca9eba125c68234eea3e393e3246ba1d34b747eb0` |
| `bin/flowdc_experiment_source.py` | `1b248f20b09fc0357e7197f6a68f3aa6d8423e357d46333adb543ae873150eac` |
| `benchmark/core/flowdc_adapter.py` | `ebcedc125eae03972b496cceefb56f3af8702770bc005d01ffac9e4d9d81e2cd` |
| `tests/test_integrity_protocol.py` | `96d13ec226c6fd81bfa1143c4329a7d4587cf2a5bb7f8c8c2c8acfba3f7d0cb7` |
| `tests/test_http_measurement.py` | `03315715e29aa9951bf2a347bc3cca4650c59b1beae907e02c86d4c11f152d41` |
| `tests/test_output_integrity.py` | `2b813509fc8ccdc65bd6b2b261be24c25986ef4d3545169cd8cfadecf4b9483e` |
| `tests/test_flowdc_experiment_source.py` | `af243b707975712ba3d48867b0cb4768d00ff5fd422c7b90b4275598d452dc9b` |
| `tests/test_flowdc_experiment_guest.py` | `832d398ccbdf263cf0ec060dfaeba0bc730a6626f1cac946bfc77c811cacc27f` |

### Remaining limits

Process interruption is covered; host power loss, hostile concurrent mutation and
non-POSIX filesystems are not. Staging is retained and consumes storage. Resume requires
unchanged source/configuration and directory ownership. Schema-2 consumers must check
the last completion record and integer fields; ImageFolder class discovery must exclude
`.flowdc/`. No migration rewrites old results, and the historical experiment schema is
preserved. Source revert is the rollback, retaining versioned outputs. No cloud runtime,
distributed control, performance/scientific efficacy claim, new review or merge occurred.

## Historical first checkpoint (06860ad)

The remainder is the original checkpoint handoff. Its pending implementation list was
superseded by the PR-24 work described above; its original validation chronology remains.

This is the first implementation checkpoint for [issue #22](https://github.com/Zi-Deng/FLOW-DC/issues/22)
and its [approved contract](https://github.com/Zi-Deng/FLOW-DC/issues/22#issuecomment-5881275556).
It implements the contract's D2/D3 observation semantics. The full output-integrity
contract remains incomplete; this checkpoint is not closure or review readiness.

## Changed behavior and evidence

A completed nonempty HTTP-200 body with valid first-byte timing remains an eligible
latency observation when local saving, metadata creation or the final output size
lookup fails. The acquisition fails, useful saved bytes are zero, and the controller
receives one local-failure outcome without invented overload. A missing saved file
also fails instead of becoming a zero-byte success. The shared batch byte tally is
credited after the final size lookup, once per successful attempt.

The helper records explicit `latency_eligible` independently of local success.
Empty/incomplete bodies and nonpositive/nonfinite timing are ineligible. Direct
legacy metrics callers that omit the new optional keyword retain the prior rule.
Base and gradient use the same acquisition helper and metrics implementation.

`http_measurement.version` is now `3-output-independent-latency`. Version 2 used the
same clock definition but different output-failure eligibility. README and the
[measurement specification](HTTP-MEASUREMENT.md) describe that compatibility boundary.
The report and helper tuple shapes remain compatible; this version does not assert
SHA-256 verification, transactional publication, reconciliation or archive integrity.

Before editing production source, three new regression methods ran against merged
base `32b5ace5b47d2c660557fe89b6fc37c8f02ca5c6`. They produced eight assertion failures:
missing-output success, final-stat success with base/gradient/fixed concurrency,
and lost latency on save-stat/metadata failure with both controllers. The response
double supplies body reads and invokes the real dispatch hook; the real HTTP helper,
save functions, final lookup and metrics run. These are controlled fault injections,
not a reproduction of a remote transport fault. Metadata failure uses an existing
directory at the metadata destination; stat failure is injected only after reopening
the closed payload and checking its bytes against independent fixture content.

The expanded seven-test integrity suite passes after repair, including empty bodies,
incomplete reads, invalid timing, unchanged successful byte credit, no duplicate byte
tally, final overview failure accounting and released controller permits. The existing
HTTP local-output regression now requires one sample instead of zero, as specified by
the approved contract; its failure/byte/overload assertions remain intact. A new real
localhost test covers both controllers and save-stat/final-stat/metadata failure.

## Environment and commands

Commands were run in the assigned `issue-22-output-integrity` worktree on September
28–29, 2026. The base head remained `32b5ace5b47d2c660557fe89b6fc37c8f02ca5c6` during
pre-commit validation; the source changes are those included with this checkpoint.
The coordinator's draft body records the resulting commit. The existing shared
environment was read only: Python 3.12.12, aiohttp 3.13.3, Polars 1.37.1, YARL 1.24.5.
No packages were installed or modified.

In the commands below, `PY` denotes
`/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python` and `RUFF` denotes
`/mnt/storage/github/FLOW-DC/.venv-agentic/bin/ruff`; shell invocations used these
absolute paths. Logs are local `/tmp/issue-22-*.log` handoff artifacts, not tracked
validation output or independent scientific evidence.

| Command | Exit and observed result |
| --- | --- |
| `$PY -B -m unittest discover -s tests -p test_output_integrity.py -v` before production changes | **1**; then three methods, eight expected assertion failures on the base. |
| `PYTHONPATH=tests $PY -B -m unittest test_http_measurement.LocalHTTPTests.test_completed_body_survives_stat_and_metadata_failures -v` before production changes | **1**; one socket-setup error, no regression assertion executed. |
| `$PY -B -m unittest discover -s tests -p test_output_integrity.py -v` after repair | **0**; seven tests. |
| `PYTHONPATH=tests $PY -B -m unittest test_http_measurement.GateTests test_http_measurement.ClassificationTests -v` | **0**; 18 tests, including generated-overview benchmark adapter compatibility. |
| `PYTHONPATH=tests $PY -B -m unittest test_flowdc_experiment_source test_flowdc_experiment -v` | **0**; 43 tests, including committed-source packaging and historical experiment behavior. |
| `$RUFF check tests/test_output_integrity.py tests/test_http_measurement.py` | **0** after binding the localhost test's loop variables explicitly. |
| `$PY -m py_compile bin/single_download.py bin/download_batch.py bin/download_batch_gradient.py tests/test_http_measurement.py tests/test_output_integrity.py` | **0**. |
| `make check PYTHON=$PY RUFF=$RUFF` | **2**; its full unittest command exits **1**, 364 tests run, 28 socket-related setup errors, no assertion failures or skips. The workflow portion is not reached by this command. |
| `make check-agentic PYTHON=$PY RUFF=$RUFF` | **0**; 141 workflow tests, scoped lint/format and repository checks pass. Run separately because the full gate stops at product tests. |
| `git diff --check` | **0**. |

The full gate executes `$PY -B -m unittest discover -s tests -v`; that is the required
full unittest invocation, not a second separately claimed run. Focused tests overlap
the full gate and must not be added to it as distinct tests. After that run, the new
fixtures' direct Config naming value was corrected from `url` to the documented
`url_based`; both take the same URL-naming branch. Focused tests were rerun. The full
gate still needs coordinator execution on the committed checkpoint.

The sandbox rejects AF_INET socket creation with `PermissionError: [Errno 1]
Operation not permitted`; aiohttp then reports `could not bind on any address out
of [('127.0.0.1', 0)]`. The 28 errors are the consolidation class setup, the actual
experiment downloader fixture, and 26 HTTP localhost methods. Tests were not skipped
or weakened, and permissions were not expanded. The coordinator must run the same
commands in its existing localhost-capable environment. A failed full gate is not
reported as a pass. `make check-clean` belongs to the clean committed handoff; its
exact result and both pending CI jobs belong in the coordinator's draft body.

## Consumer inspection and remaining contract

The existing TaskVine route stages `download_batch.py` and `single_download.py`, both
already in `flowdc_experiment_source.SOURCE_PATHS`. This checkpoint adds no runtime
module or identity field. Gradient subclasses the shared metrics and uses base
acquisition. The multithread implementation has separate metrics and the GBIF helper.
The UI simulates execution; it does not consume the new eligibility. The benchmark
adapter selects existing summary fields and tolerates additive measurement metadata.
Its historical MB-to-byte conversion remains pending the approved integer-byte
report adaptation; no benchmark fairness claim is made. Existing JSON config keys
and defaults do not change, so no example-config edit is needed at this checkpoint.

| Approved criterion | Checkpoint disposition |
| --- | --- |
| Original row identity and input coverage | Pending. |
| Duplicate rows and exactly one final outcome; persisted attempts | Pending. Current tests cover completed-attempt categories only. |
| Collision/containment and overwrite consent | Collision/containment implementation pending; overwrite code unchanged, socket-dependent consolidation check blocked. |
| Recoverable publication, offline reconciliation and resume | Pending. Failed saves can still leave partial output. |
| Verified archives, integer/unique/observed byte reporting | Pending. No durable-result boundary is claimed here. |
| D2/D3 observation semantics | Implemented and controlled fault-injection tests pass; real localhost validation blocked. |
| Compatibility | Non-socket HTTP, source/experiment and workflow checks pass; full socket coverage and both CI jobs outstanding. |
| D5 historical validation chronology | Pending; no historical evidence rewritten in this checkpoint. |

Remaining work must resume the same managed Astra session after the coordinator
publishes and records the draft PR. Public writes, pushes and CI observation remain
coordinator responsibilities. There is no model review or human merge approval at
this checkpoint. Source revert restores the prior implementation while leaving
historical outputs intact. These software checks establish no performance,
distributed-control or scientific-efficacy claim.
