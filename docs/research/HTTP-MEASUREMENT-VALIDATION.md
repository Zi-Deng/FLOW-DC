# HTTP measurement validation — issue #20

This record covers [issue #20](https://github.com/Zi-Deng/FLOW-DC/issues/20), its
[approved plan](https://github.com/Zi-Deng/FLOW-DC/issues/20#issuecomment-5876154148)
and draft [PR #21](https://github.com/Zi-Deng/FLOW-DC/pull/21). The
[measurement specification](HTTP-MEASUREMENT.md) defines the new semantics; the
[research contract](MANUSCRIPT-READINESS.md) retains the open scientific gates.

## Final implementation validation

The coordinator validated stable commit
`8efd15dafa75258b89ab1b4bd94bf3a83ce51f5f` in its existing localhost-capable
environment: Python 3.12.12 (conda-forge), aiohttp 3.13.3, Polars 1.37.1 and YARL
1.24.5. The validation wrapper exited **0** with identical before/after source and
test hashes and the same head throughout. The executor inspected the logs and
verified each recorded hash against both the Git blobs and working files.
These tests were executed by the coordinator, not inside the socket-restricted
executor. The final evidence update changes documentation only.

| Coordinator command | Exit / result |
| --- | --- |
| `/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python -B -m unittest discover -s tests -p test_http_measurement.py -v` | **0**; **39 tests**, no failures/errors/skips, 27.772 s (27.926 s command time). |
| `make check PYTHON=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python RUFF=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/ruff` | **0**; **360 product tests** in 104.794 s and **141 workflow tests** in 21.537 s; scoped Ruff lint/format and repository configuration/link validation pass. Total command time 126.701 s. |
| `make check-clean PYTHON=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python` | **0**; `git diff --check` and clean tracked/untracked status check pass. |

The `make check` log explicitly executes
`/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python -B -m unittest discover -s tests -v`;
that is the required full unittest run, not an omitted check or a second claimed
execution. It includes consolidation coverage for both downloaders' retries,
archive modes, overview contents and overwrite consent. All HTTP tests use local
fixtures and temporary outputs. No cloud or external dataset campaign ran.

| Validated file | SHA-256 |
| --- | --- |
| `bin/download_batch.py` | `59d78cdc05d4f09232a2e9e2c8062d128e6b189bc342034fe427263c130d85be` |
| `bin/single_download.py` | `1552d27c34018a4ea36e4afc7dd9b279c222d1fb9cbcb3f4cbd2713090cc3b15` |
| `bin/download_batch_gradient.py` | `7194f975dde612347c038aa949128afc2c7ee259d89d93dc489dd2a0bb4d1f0c` |
| `tests/test_http_measurement.py` | `c174fc683f2703285c0cfc420060bd0552cb454d298f395c0fd7afba6679cae3` |

Implementation and required local validation are complete. The executor's socket
restriction was handled through real coordinator execution, not waived or hidden.
At the coordinator handoff, GitHub `agentic-quality` had succeeded and `flowdc-tests`
was still running on `8efd15d`. The coordinator must confirm CI on the final
documentation head and run the configured independent review. Neither CI completion
nor independent-review/merge readiness is asserted here.

## Failing-base evidence

Checkpoint `8977a71659bde7c2da6d1099c62f23f987296956` added tests/documentation
without changing production base `ca52044fa2d4447ce6dd288b5a7b571b9db75c6d`.
Before repair, the coordinator ran this exact command from the issue worktree in
its existing localhost-capable environment:

```bash
/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python -B -m unittest discover -s tests -p test_http_measurement.py -v
```

Exit **1**: **9 tests, 12 failing subcases, zero errors, zero skips**, in 2.531 s.
Python was 3.12.12 (conda-forge), aiohttp 3.13.3 and polars 1.37.1. The committed test
SHA-256 was `d4d86fcfde0322e970ac0b78f80f3f6a75cdb4a591ead20f379c90ddabb39df9`.
The executor verified that hash and all three production-source hashes against the
coordinator's recorded manifest before modifying production code, and read the
failure summary from its private log. These are coordinator-executed observations,
not tests run inside the executor sandbox.

| Regression | Observed failure on unchanged production source |
| --- | --- |
| 404 useful-success classification | Base and gradient each report `n_success == 1` instead of 0. Total remains 1, with zero bytes/samples and no overload. |
| Delayed body tail | The first-byte observation occurs after the delayed whole body, violating the independent server-event bound. |
| Empty body | A first-byte sample is fabricated for an empty response. |
| Numeric Retry-After | Retried requests arrive before the advertised two-second deadline in both CLIs with PAARC enabled and disabled (four subcases). |
| HTTP-date Retry-After | Retried requests arrive before the rounded UTC deadline in the same four modes. |

The installed aiohttp 3.13.3 `ClientResponse.read` was also inspected: it awaits the
complete stream before emitting its response-chunk callback. This explains the old
whole-body measurement but is not a substitute for the observed localhost failures.
The old helper only float-parsed Retry-After and the batch retry loop slept only its
configured retry backoff.

## Intermediate-head validation and redirect repair

The coordinator subsequently ran:

```bash
make check PYTHON=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/python RUFF=/mnt/storage/github/FLOW-DC/.venv-agentic/bin/ruff
```

Exit **0** on production/test contents committed as
`ab0d59fd3af2f56f757a359a7435d5c3d6374be1`: **351 product tests** in 98.190 s,
**141 workflow tests** in 21.513 s, plus lint/format and repository validation.
This includes all 30 focused methods then present and the consolidation suite.
The executor inspected the log and verified the coordinator's SHA-256 manifest
against the committed helper, base downloader and test file. Documentation was
being finalized during that run; the matching source/test hashes establish the
scope of this evidence. It does not certify subsequent changes.

A separate coordinator localhost probe exposed a missing case despite that pass:
a cross-authority 302 with Retry-After 1 was followed in about **0.000506 s**.
The intermediate test incorrectly expected immediate cross-authority following;
it has been replaced with the required delay plus independent destination traffic.
RFC 9110 §10.2.3 asks for a minimum delay before issuing the redirected request,
not only an embargo on later requests to the original authority.

Before the redirect repair, these maintained deterministic regressions ran against
the still-unchanged `ab0d59f` production source (using the interpreter alias below):

| Command | Exit / observed failure before repair |
| --- | --- |
| `PYTHONPATH=tests "$FLOWDC_TEST_PY" -B -m unittest test_http_measurement.GateTests.test_redirect_retry_after_delays_follow_without_embargoing_destination -v` | 1; one test, four failing subcases: the redirect callback performs no wait for either numeric/date header and either destination. For a same-authority hop the later dispatch gate already waited; the cross-authority chain did not. |
| `PYTHONPATH=tests "$FLOWDC_TEST_PY" -B -m unittest test_http_measurement.ClassificationTests.test_redirected_acquisition_uses_destination_adaptive_permit -v` | 1; one test, two failing subcases: base/gradient each hold zero destination permits while dispatching that hop. |

The latter also confirms the intermediate mismatch between final-authority feedback
and the original-authority permit. The repair transfers permits before the next
connector acquisition, releasing the old permit first. It changes admission plumbing,
not controller equations. Tests additionally cover cancellation while acquiring the
destination permit or smoothing, reciprocal redirects, capacity limits, bounded
3xx waits, and a redirect with an unfinished response body. These localhost cases
subsequently passed in the final stable-head run recorded above.

The coordinator also ran an intermediate redirect working tree: the focused command
above exited **0** (37 tests in 27.868 s; 28.034 s command time), and `make check`
exited **0** (358 product and 141 workflow tests, 126.678 s total command time).
Its before/after manifest
shows that the test file changed during these checks, while production hashes were
stable. The validation wrapper therefore exited **1** and explicitly refused to
certify that moving tree. These are diagnostic passes, not stable-head evidence.

At committed production source `afebd453ff01d799add7ef6aa3e315b25714d39f`, one
further maintained regression was added and run before its repair:

```bash
PYTHONPATH=tests "$FLOWDC_TEST_PY" -B -m unittest test_http_measurement.ClassificationTests.test_normalized_dispatch_feedback_uses_held_controller -v
```

Exit **1**: one test, **12 failing subcases**, covering HTTP `:80`, HTTPS `:443`
and Unicode/IDNA hosts, each with success and overload feedback in base/gradient.
The actual dispatch hook normalizes its URL, causing the old feedback lookup to
select a different controller from the one holding the permit. This fixture uses
real manager/trace objects with a simulated download; it requires neither DNS nor
a default-port listener. Crediting the held controller directly fixes the mismatch,
with the existing target-lookup fallback retained for failures before acquisition.
The regression now passes; the manager's broader host-key policy is unchanged.

## Executor environment and results

The executor uses existing dependencies read-only; no installation or permission
change occurred. Its Python is 3.12.3, with aiohttp 3.13.3, polars 1.37.1, psutil
7.2.1, tqdm 4.67.1, PyYAML 6.0.3 and Ruff 0.16.7. Command tables use these aliases
for the explicit interpreter/tool paths actually selected:

```bash
FLOWDC_TEST_PY=/mnt/storage/github/FLOW-DC-control-js2-worktrees/issue-12-reusable-experiment-runner/.venv-agentic/bin/python
FLOWDC_TEST_RUFF=/mnt/storage/github/FLOW-DC-control-js2-worktrees/issue-12-reusable-experiment-runner/.venv-agentic/bin/ruff
```

At the initial checkpoint, `python3 -B -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print("localhost bind available:", s.getsockname()); s.close()'`
exited 1 at socket creation with `PermissionError: [Errno 1] Operation not permitted`.
The coordinator subsequently supplied the real baseline above. This executor's
socket capability has not changed.

| Initial-checkpoint command | Exit / result |
| --- | --- |
| `"$FLOWDC_TEST_PY" -B -m unittest discover -s tests -p test_http_measurement.py -v` | 1; 9 tests, 2 failing classification subtests, 7 socket setup errors, no skips. |
| `"$FLOWDC_TEST_PY" -B -m unittest discover -s tests -v` | 1; 322 tests, 2 failing subtests and 9 socket errors, no skips. The consolidation class fails setup, so retry/tar/overwrite checks do not execute. |
| `make check PYTHON="$FLOWDC_TEST_PY" RUFF="$FLOWDC_TEST_RUFF"` | 2; Make stops at `test-flowdc` (exit 1), before the workflow gate. |
| `make check-agentic PYTHON="$FLOWDC_TEST_PY" RUFF="$FLOWDC_TEST_RUFF"` | 0; scoped lint/format, 141 workflow tests and repository configuration/link checks pass. |

The nine socket errors are seven new HTTP fixture setup failures, the existing
consolidation class setup and the existing experiment guest's real downloader
fixture. They are environment failures, not product assertion failures.

| Implementation-stage command | Exit / result |
| --- | --- |
| `PYTHONPATH=tests "$FLOWDC_TEST_PY" -B -m unittest test_http_measurement.GateTests test_http_measurement.ClassificationTests -v` | 0; 15 tests pass after attribution repair, including all new failing-before-repair regressions and the formerly failing 404 cases. No skips. |
| `"$FLOWDC_TEST_PY" -B -m unittest discover -s tests -p test_flowdc_experiment_source.py -v` | 0; 8 source-packaging tests pass. |
| `"$FLOWDC_TEST_PY" -m py_compile bin/single_download.py bin/download_batch.py bin/download_batch_gradient.py tests/test_http_measurement.py` | 0. |
| `"$FLOWDC_TEST_PY" -B scripts/check_repository.py` | 0; repository configuration/skills/links validate. |
| `"$FLOWDC_TEST_RUFF" check tests/test_http_measurement.py` | 0 after import formatting corrections. |
| `git diff --check` | 0. |

No expected-failure decorators, skips or weakened assertions were used to make the
suite pass. Deterministic callback/permit tests use response fixtures and mocks;
they supplement, rather than replace, the maintained real HTTP integration cases.
The known socket-denied full suites were not repeatedly rerun during repair.
The final stable-head localhost/full-gate passes are recorded above, together with
the earlier intermediate results. CI confirmation and independent review remain
coordinator stages; this record does not assert merge readiness.

## Coverage and compatibility inspection

The expanded focused suite has 39 test methods. Its fifteen tests without sockets cover
classification and disjoint denominators, sample eligibility, overview labeling,
parser forms/invalid values, authority normalization, concurrent extension and
cancellation with a deterministic clock, wall-clock jumps, session isolation and
Webdataset metadata-write byte accounting, 3xx follow-up delays, single observation
of a terminal 3xx header, redirect permit ownership/cancellation, and normalized URL
feedback attribution to the controller holding the permit.

The twenty-four localhost tests exercise header/first-byte/tail timing, empty/failed and
truncated bodies, both header forms in real base/gradient/fixed CLI retry paths,
concurrent deadline extension and later shorter responses, unrelated authorities,
same/cross-authority redirects and metric attribution, connector-wait rechecks,
prompt header observation before body completion, local output failure, timeout,
cancellation and shutdown/permit recovery, cross-authority 3xx delays with independent
destination progress, reciprocal redirects and destination concurrency limits.
All 39 focused tests and the existing consolidation coverage passed on the repaired
source in the coordinator environment, including base/gradient retries, tar modes,
overview contents and overwrite consent.

Fixtures bind only ephemeral `127.0.0.1` origins and use temporary outputs. Timing
uses independent monotonic server events, 400-ms stage delays, a half-tail-gap
comparison and documented 50–100-ms deadline tolerances. CLI retries have a
15-second test bound and subprocess cleanup. These scheduling-level observations
can still be disrupted by a heavily loaded host; they are not packet measurements.

Inspection confirmed TaskVine stages `single_download.py` and `download_batch.py`,
and the experiment source manifest already includes both. No staged module was
added. UI, TaskVine and example configuration keys/defaults remain compatible;
the benchmark adapter reads existing overview fields and tolerates additive metadata.
The gradient policy inherits the shared changes without equation/default edits.
No multithread/cloud-upload implementation was changed. No TaskVine cluster runtime
or distributed gradient execution was tested.

## Remaining evidence and scientific limits

The coordinator owns final-head CI confirmation and the configured independent
review; any supported failure requires repair and fresh validation in this same
executor session. Local checks establish only the exercised software invariants,
not universal network behavior or scientific efficacy. The overall authorized work
ends **20:31 UTC September 28, 2026**.

The previous 64-image, one-worker toggle run remains functional smoke evidence only.
No publication, speedup, gradient-efficacy, output-integrity or distributed-scaling
claim follows from these software checks. No cloud activation, external dataset,
runtime installation, grant, performance campaign or manuscript edit occurred.

Technical references: [aiohttp streams](https://docs.aiohttp.org/en/stable/streams.html),
[aiohttp tracing](https://docs.aiohttp.org/en/stable/tracing_reference.html) and
[RFC 9110 §10.2.3](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after).
The moving aiohttp documentation identified version 3.14.3 when consulted; installed
3.13.3 source and controlled tests determine local behavior.
