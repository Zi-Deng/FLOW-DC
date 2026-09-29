# Output integrity validation — issue #22

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
