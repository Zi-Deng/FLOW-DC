# PR #24 first-review repair evidence

This records repairs against reviewed head
`3b0b4a12ce5c4cee77e572ef6f7a27475d789bcf`, with approved base
`32b5ace5b47d2c660557fe89b6fc37c8f02ca5c6`. The supplied timeline contains the
[first independent COMMENT review](https://github.com/Zi-Deng/FLOW-DC/pull/24#pullrequestreview-5346877567),
four earlier conversation records, and no inline comments. The unchanged
[approved contract](https://github.com/Zi-Deng/FLOW-DC/issues/22#issuecomment-5881275556)
governs these repairs. No new review, publication, cloud action or permission change
was performed by the executor.

## Finding dispositions

All finding identifiers below refer to the linked first review. None is deferred.
The coordinator's public response binds this note and the checks to the repair SHA.

| Finding | Disposition and regression evidence |
| --- | --- |
| P2-1: managed `create_tar` invalidates final record | Fixed by rejecting already finalized managed directories before reconciliation/publication. Error directs callers to `--reconcile`, which handles the entire archive/report/final-record lifecycle. The regression finalizes a run, invokes the helper, compares every file before/after, and independently verifies the final-record SHA binding. The existing helper test still covers committed but unfinalized directories and exclusion of unowned files. Historical unowned-folder behavior remains. |
| P2-2: gradient resume overwrites whole-run evidence with zero/partial counters | Fixed with explicit `gradient_summary=null` and `gradient_summary_scope="unavailable_across_resume"`. There is no persisted whole-run controller state to aggregate. Reconciliation carries this scope forward. The regression resumes both fully committed and partially committed gradient runs, checks exactly zero/one HTTP helper calls respectively, two verified rows, and repeated offline reconciliation with HTTP forbidden. Earlier owned report versions remain preserved in export staging. |
| P2-3: journal failure causes unbounded retry | Fixed by propagating attempt-start exceptions unless a durable row rejection exists. Storage failure aborts the invocation before HTTP and cannot retry an unchanged budget. Injected ENOSPC at attempt mkdir and intent publication each raises after one call, makes no HTTP helper call and leaves an incomplete final marker. A directory with no valid intent reconciles as failed/uncertain and consumes one attempt. No successfully dispatched work can lack a prior intent. No attempt budget is reset. |
| P2-4: unavailable benchmark elapsed becomes zero | Fixed by rejecting schema-2 reports with missing/null, negative, nonnumeric, boolean or nonfinite elapsed measurements, with path/field diagnostics. The existing numeric benchmark result schema cannot carry unavailable timing; rejection prevents invented derived throughput without redesigning benchmark aggregation. Schema-1 interpretation is unchanged. |
| P3-1: missing schema-2 byte field raises KeyError | Fixed with explicit path/field validation of a nonnegative integer. Tests cover missing/null, negative, boolean and fractional values. Existing exact 1,000,000-byte schema-2 and historical schema-1 adapter tests remain. |
| P3-2: owner-only output modes undocumented | Addressed by documenting the existing `0700`/`0600` policy in README and the protocol specification, including ancestors, sidecars, exports, tar members and cross-user/group access limitations. Chose the review's documentation option; permissions are unchanged. No cross-user sharing support is claimed. |
| P3-3: derived host-column migration undocumented in README | Addressed by a README migration note: read `__flowdc_partition_host__` for the derived value; an original `host` column remains original metadata. No extra derived field is inserted into the hashed original row. Existing real partition subprocess tests still validate parent identity/metadata for all three methods. |
| P3-4: empty partition omits operator summary | Fixed with an explicit zero-row/one-empty-partition notice and the saved path. A real CLI subprocess verifies output text and independently reads the sole zero-row Parquet file. |

The regression names are in `tests/test_integrity_review.py`. No pre-existing
assertion, test selection, retry limit, acceptance criterion or workflow source was
weakened.

## Questions and scope evidence

1. **Latency denominators and controller effect.** `HostMetrics.finish_interval`
   derives `n_samples` from the timing list; `_percentile` uses that list's length,
   and EMA/RTprop use those percentiles. Neither percentile uses `n_success`, total
   attempts or payload bytes. Gradient confidence is `min(1, n_samples/N_min)`;
   summary confidence/sample averages are weighted by observed gradient intervals.
   Goodput remains separately normalized: successes/time and useful bytes/time.
   The controller's cadence term still uses `N_min/goodput_rps`; this is an existing
   scheduling heuristic, not a normalization of latency observations.

   The new base/gradient controller regression supplies five known timing values
   (1, 2, 3, 4, 5), each followed by local failure. Both yield p10=1.4, p50=3,
   p95=4.8, five failures, zero success/goodput/overload; gradient confidence is 0.5
   for N_min=10. At fixture N_init=5, these eligible observations advance INIT to
   STARTUP without changing initial concurrency (4); excluding those same samples
   leaves INIT. This is the intended D2/D3 observation effect under contract clause
   7, not a tuning/equation change or an efficacy claim. Latency-driven decisions
   may therefore change during local failures, while no origin overload is invented.
2. **Gradient snapshot concurrency.** No concurrent mutation occurs during the
   synchronous report factory, `collect_gradient_summary` and `finalize_run` path:
   they contain no event-loop suspension and run on the same loop as controller
   updates. Acquisition workers are already joined. Manager dictionary creation and
   metrics/state updates have no other writer thread; `all_controllers`' lock
   protects async creation, not a separate atomic interval transaction. The summary
   reflects observations accumulated so far, not a promise to flush an unfinished
   final controller interval. No material race was established; no locking or
   controller lifecycle change is needed for this question.
3. **Class metadata.** The full shared `generate_overview_report` computes successes,
   failures, bytes and error counts; it never reads `DownloadOutcome.class_name`.
   The gradient report delegates to it and adds controller fields. The config still
   reports `label_column`; original row labels and metadata `class_name` remain in
   `RunStore.metadata`, checked by reconciliation. Thus the synthetic outcome's
   `class_name=None` drops no previously reported per-class aggregate. No change.
4. **Overwrite consent and external conflicts.** Consent permits replacement of the
   selected output; it does not adopt unowned external archives/reports. The
   `remove_owned_exports` path verifies ownership before removal, and
   `shutil.rmtree(out)` follows it. Input and run ownership are checked first.
   A subsequent conflict can still abort consented overwrite; this is the documented
   fail-closed behavior required by contract clause 3. No permission broadening or
   pre-existing user-file deletion is appropriate. No change.
5. **Post-commit lookup failure.** A failed attempt observation and a subsequently
   verified final row are separate facts. The new regression injects the actual
   commit-record lookup failure after publication, confirms failed in-memory/retained
   attempt evidence with zero immediate byte credit, then independently reconciles
   the valid commit to one verified row with the correct digest/length. Repeating
   reconciliation yields identical outcomes/bytes. The protocol now states this
   distinction explicitly; this is not recovery of an uncommitted terminal failure.

## Earlier findings and evidence limitations

The [staging-budget finding](https://github.com/Zi-Deng/FLOW-DC/pull/24#issuecomment-5882043580)
was repaired in `3b0b4a12ce5c4cee77e572ef6f7a27475d789bcf`. Its valid-Parquet,
remaining-budget assertions remain unchanged. The earlier HTTP diagnostics and D5
citation were addressed/verified as recorded in the
[published repair response](https://github.com/Zi-Deng/FLOW-DC/pull/24#issuecomment-5882177923).
The [coordinator's exact-head completion evidence](https://github.com/Zi-Deng/FLOW-DC/pull/24#issuecomment-5882217050)
and [executor verification](https://github.com/Zi-Deng/FLOW-DC/pull/24#issuecomment-5882254856)
establish 401 product plus 141 workflow tests and both CI passes on **3b0b4a1**.
Those passes are historical evidence, not validation of this changed head.

The reviewer did not receive the approved plan and some linked historical evidence;
the links above supply the governing contract and dated completion records. Its
coverage omissions do not prove defects or authorize another review. `Makefile`
explicitly defines `check: test-flowdc check-agentic`; `test-flowdc` invokes
`python -B -m unittest discover -s tests -v`. Thus `make check` includes the named
product gate and is counted once. No claims are made about excluded archived data,
scientific efficacy, live TaskVine/cloud behavior, nonlocal filesystem semantics or
host-power-loss durability.

## Executor validation chronology before this repair commit

All commands run from the assigned worktree. Executables:
`PY=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python` and
`RUFF=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/ruff`, read-only shared environment.
Python 3.12.12, aiohttp 3.13.3, Polars 1.37.1, YARL 1.24.5, Ruff 0.16.7.

| Command | Exit and evidence |
| --- | --- |
| `PYTHONPATH=tests $PY -B -m unittest test_integrity_review -v` with production files still at reviewed 3b0b4a1 | **1**; initial six regression methods, 12 assertion failures and three errors, reproducing the finalized-record change, zeroed resume summary, bounded-time detection of repeating mkdir failure, swallowed intent failure, adapter validation gaps and empty-input notice gap. These are new tests applied to the reviewed source before fixes. |
| Same command after fixes, with two additional question-evidence tests | **0**, eight methods, including no-work/partial gradient resumes, both controller variants and real partition CLI. |
| `PYTHONPATH=tests $PY -B -m unittest test_integrity_review test_integrity_protocol test_output_integrity test_http_measurement.GateTests test_http_measurement.ClassificationTests test_flowdc_experiment_source test_flowdc_experiment -v` | **0**, 105 tests in 17.947 s. The controller question test was then extended from interval metrics to actual INIT/STARTUP state and passed in the eight-test command above. |
| `$PY -m py_compile bin/download_batch.py bin/download_batch_gradient.py bin/SplitParquet.py benchmark/core/flowdc_adapter.py tests/test_integrity_review.py` | **0**. |
| `$RUFF check tests/test_integrity_review.py` | **0** after formatting/binding fixture closures. |
| `$RUFF check tests/test_integrity_review.py benchmark/core/flowdc_adapter.py bin/SplitParquet.py bin/download_batch_gradient.py bin/download_batch.py` | Initial extra lint audit **1**. New fixture warnings were corrected; existing product files have 60 pre-existing diagnostics outside the scoped required lint target. A subsequent JSON comparison against each reviewed Git blob found identical diagnostic code/message counts (3 adapter, 8 partitioner, 40 base, 9 gradient), no new product diagnostics. No lint rule was suppressed or unrelated source reformatted. |
| `git diff --check` | **0**. Runtime and documentation diff inspected; full new regression source inspected before staging. |

The final committed-head commands, SHA and exits will be in the coordinator response.
Required next checks are committed-head focused tests, relevant compilation,
`make check`, `make check-clean`, and both CI jobs. The restricted executor must not
bypass its AF_INET restriction to run localhost fixtures; any such setup errors
require the coordinator's identical checks in its existing capable environment.
The first independent review is used. Changed head/base needs fresh independent
review; these P2/P3 findings do not authorize an extra round. Another round requires
explicit user authorization under the recorded review policy. This repair is not
merge readiness or human acceptance of accounting semantics.
