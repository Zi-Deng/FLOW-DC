# PR #24 second-review repair evidence

Repairs against reviewed head `44f5daff851040f62181ecc3619cefa30302ccd0`, approved
base `32b5ace5b47d2c660557fe89b6fc37c8f02ca5c6`, within the unchanged
[issue #22 contract](https://github.com/Zi-Deng/FLOW-DC/issues/22#issuecomment-5881275556).
The supplied timeline contains two COMMENT reviews, nine conversation comments and
no inline comments. The coordinator records explicit authorization for the second
round in [the previous completion response](https://github.com/Zi-Deng/FLOW-DC/pull/24#issuecomment-5882699787).
That round is now used. This repair does not invoke or authorize a third review.

## Second-review dispositions

All identifiers in this section refer to the
[second review](https://github.com/Zi-Deng/FLOW-DC/pull/24#pullrequestreview-5347162219).
The coordinator's response binds this note and final checks to the repair commit.

| Finding | Disposition and concrete evidence |
| --- | --- |
| P2-1: schema-2 benchmark throughput uses display MB | Fixed: derive throughput from exact integer payload bytes with the benchmark's existing binary divisor, 1,048,576. The legacy `throughput_mbps` name means MiB/s, consistent with the schema-1 and img2dataset adapters. Extra metrics declare the unit/divisor. The regression uses 1,000,001 bytes over 3 seconds and missing, rounded or deliberately inconsistent display MB: all must yield 0.31789175669352215 MiB/s. Historical schema-1 arithmetic and archived files remain unchanged. |
| P2-2: cleared destination rejection cannot resume | Addressed through the review's documentation option. Rejections and recorded local/unknown failures are intentionally terminal; clause 5 promises only eligible unresolved rows, not automatic reopening of terminal evidence. README/protocol now say this explicitly and describe a new output directory with the original input/configuration, preserving the old run and verified artifacts. This is a distinct acquisition, potentially redownloading verified rows; no deletion or manual journal surgery is required. A regression removes an operator-owned fixture conflict, resumes with HTTP forbidden, and verifies zero intents for the still-failed row, unchanged rejection evidence and preservation of the other verified row. No retry policy was broadened. |
| P2-3: whole-manifest memory/scale | Accepted as an implementation limitation and corrected capacity documentation, not a demonstrated violation of the approved integrity invariants. README/protocol and the partitioner docstring now state the in-memory operating boundary and withdraw the unvalidated 40M-row claim for this path. Python row materialization, input bytes and full ownership/outcome indexes must fit alongside Polars frames and acquisition buffers. A numeric row limit would be unsupported because metadata sizes vary; no invented cap or performance claim is introduced. The installed Polars 1.37.1 `DataFrame.clone` documentation states that cloning does not copy data, so the review's second-full-data-copy claim is inaccurate; other whole-manifest allocations are real. Out-of-core redesign and a large-scale performance campaign are not acceptance requirements of this increment. No such redesign is represented as delivered or as an accepted follow-up. |
| P3-1: concurrent export creation references unbound variable | Fixed: initialize the previous owned record and require it before any replacement. The fault-injection regression creates a foreign file at `export_intent` for both internal and external exports, expects an explicit integrity error, and checks preservation of the foreign bytes and owned staged evidence. This improves the diagnostic without claiming protection against hostile concurrent mutation. |
| P3-2: repeated filename validation | No code change: the second check is reachable and required when `filename=None`, after URL-derived naming; the first guard checks only supplied names. An encoded-separator URL regression confirms rejection before HTTP/output. The checks overlap for supplied names but are not dead code overall; removing the second call as suggested would remove validation from generated names. |
| P3-3: cross-entrypoint reconciliation | Fixed: compare the invoking variant with recorded ownership before reconciliation mutates final/report evidence. The regression covers both directions on completed owned runs, with HTTP forbidden and every file's bytes compared before/after refusal. Existing matching-variant recovery tests remain. A normal base report has no `controller_variant` field, so the review's exact mixed-field example is not universal; the wrong-entrypoint metadata change is nevertheless valid. |
| P3-4: quadratic progress lookup | Fixed: build a set once per round for membership in the attempt-number scan. A regression forbids per-row list membership while exercising real acquisition orchestration with a controlled HTTP helper; both requested rows complete. This changes expected lookup complexity from quadratic to linear without changing retry eligibility or budgets. |
| P3-5: ancestor symlink restriction | README/protocol now explicitly include home/scratch/mount aliases and newly created `0700` ancestors, while preserving existing modes. No permission change. The existing no-follow open already names the offending component; on this Linux environment a symlink directory raises `NotADirectoryError`/ENOTDIR rather than necessarily ELOOP. A real symlink fixture checks that diagnostic and verifies no directory/file appears through the link. |

These are fixes, documented supported behavior/limitations, or evidence-backed
rebuttals. No material finding is silently deferred to an unlinked issue. The scale
discussion is not a claim that a specific row count is safe or that measured throughput
has been established.

## Questions

1. **Local-failure latency and controller state:** approved contract clause 7 expressly
   separates completed-body timing from output success. The first repair's actual
   controller fixture demonstrates the input effect (INIT to STARTUP at unchanged
   concurrency 4 for five eligible samples, versus INIT without samples). It does
   not prove real-run scientific efficacy. Human acceptance of accounting semantics
   remains part of merge; this executor does not supply that acceptance. No controller
   tuning/equation change or performance experiment is needed to answer this question.
2. **Terminal local/unknown failures:** intentional. A local error can occur after a
   payload or metadata was published, and reopening it automatically could collide
   with retained evidence. Interruption recovery, already-valid commits and ordinary
   retryable HTTP errors remain separately handled. The terminal-state documentation
   and preserved-run remedy above apply even when storage was transiently unavailable.
3. **Research preset versus compressed TaskVine output:** both paths agree. A new
   regression explicitly supplies both booleans as true, reads the emitted worker
   JSON through the real downloader parser/normalizer, and checks `compress_tar=False`,
   WebDataset tar output and the manager's `.tar` declaration. It passed before repair;
   no configuration or TaskVine runtime change is indicated. This uses a stubbed runtime,
   not a live cluster.
4. **Downstream sample-count assumptions:** `git grep -n -E
   'n_samples|n_success|ttfb_samples' -- benchmark
   bin/flowdc_experiment_artifacts.py bin/flowdc_experiment_report.py` returns exit 1
   (no matches), including maintained benchmark aggregation/comparison. The base and
   gradient consumers remain as inspected in the first response: timing percentiles
   use sample lists, confidence uses sample count, and success/byte goodput has its
   separate time denominator. The multithread alternate uses its own metrics path.
   Existing schema-1 compatibility tests remain; archived results are neither rewritten
   nor reinterpreted as schema 2. No `n_samples <= n_success` requirement was found in
   these maintained consumers. This is not a claim of inspecting every private archive.

## Earlier findings remain accounted for

The [first review's eight findings and five questions](https://github.com/Zi-Deng/FLOW-DC/pull/24#pullrequestreview-5346877567)
retain their [published dispositions](https://github.com/Zi-Deng/FLOW-DC/pull/24#issuecomment-5882531608)
in `476a8be8716e74c1e3e1644a01323006b254fe58`: finalized tar-helper refusal,
unavailable resumed gradient counters, aborting unrecorded attempt starts, rejecting
unavailable benchmark timing, byte validation, permission/host-field documentation,
and empty-input notice. Their regressions are included in the focused command below.
The new throughput fix complements the earlier timing/byte validation rather than
waiving it. The source and permission limits of the first review remain intact.

The [packet-availability correction](https://github.com/Zi-Deng/FLOW-DC/pull/24#issuecomment-5882534410)
remains fixed in `44f5daff851040f62181ecc3619cefa30302ccd0`.
The [staging-budget regression](https://github.com/Zi-Deng/FLOW-DC/pull/24#issuecomment-5882043580)
remains fixed in `3b0b4a12ce5c4cee77e572ef6f7a27475d789bcf` with its original budget
assertions preserved. Earlier HTTP diagnostics and D5 chronology retain the
[prior dispositions](https://github.com/Zi-Deng/FLOW-DC/pull/24#issuecomment-5882177923).
The previously verified 550-test parent gate and both CI passes on `44f5daf` are
[historical evidence](https://github.com/Zi-Deng/FLOW-DC/pull/24#issuecomment-5882635846),
not certification of this new repair head.

## Executor validation before the repair commit

Commands run from the original worktree. `PY` denotes
`/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python`; `RUFF` denotes the same
environment's `ruff`. Python 3.12.12, aiohttp 3.13.3, Polars 1.37.1, YARL 1.24.5,
Ruff 0.16.7. No packages or permissions changed.

| Command | Exit and evidence |
| --- | --- |
| `PYTHONPATH=tests $PY -B -m unittest test_integrity_second_review -v` against unchanged reviewed production source | **1**: eight methods, six assertion failures and two export-race errors after correcting a missing required `input_path` in the new fixture. Four code defects reproduced; terminal rejection, TaskVine preset, generated-name validation and symlink diagnostic checks already passed. |
| Same command after fixes | **0**, eight methods. The cross-entrypoint fixture was subsequently strengthened to generate the owning variant's report and check its version; this final eight-method command also passes. An intermediate fixture assertion incorrectly expected `controller_variant` on base reports, produced one KeyError, and was corrected to assert the actual `paarc_version` schema. |
| `PYTHONPATH=tests $PY -B -m unittest test_integrity_second_review test_integrity_review test_integrity_protocol test_output_integrity test_http_measurement.GateTests test_http_measurement.ClassificationTests test_flowdc_experiment_source test_flowdc_experiment -v` | **0**, 113 tests in 17.543 s before the final fixture strengthening above. Contains earlier-head regressions. Committed-head results are reported separately. |
| `$PY -m py_compile bin/download_batch.py bin/flowdc_integrity.py bin/SplitParquet.py benchmark/core/flowdc_adapter.py tests/test_integrity_second_review.py` | **0**. |
| `$RUFF check tests/test_integrity_second_review.py`; `$RUFF format --check tests/test_integrity_second_review.py` | **0** after formatting. Initial format check **1**, then formatter **0**; no lint suppressions. |
| `git diff --check` | **0**. |

The coordinator response records the coherent repair commit, committed-head focused/full
gate exits, compilation, clean checkpoint and exact omissions. `make check` includes
the required `test-flowdc` unittest discovery and is counted once. Socket restrictions
must be reported, not bypassed or converted into skips. Both new-head CI jobs remain
coordinator work. Changed head/base needs fresh independent review; the used second
round's P2/P3 findings and inspection gaps do not authorize a third. No review, push,
public write, merge or branch/worktree deletion is performed by this executor.
