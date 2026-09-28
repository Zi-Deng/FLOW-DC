# HTTP measurement validation — issue #20

This record covers [issue #20](https://github.com/Zi-Deng/FLOW-DC/issues/20), its
[approved plan](https://github.com/Zi-Deng/FLOW-DC/issues/20#issuecomment-5876154148)
and draft [PR #21](https://github.com/Zi-Deng/FLOW-DC/pull/21). The
[measurement specification](HTTP-MEASUREMENT.md) defines the new semantics; the
[research contract](MANUSCRIPT-READINESS.md) retains the open scientific gates.

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
3xx waits, and a redirect with an unfinished response body. These added localhost
cases still require coordinator execution on the repaired head.

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
| `PYTHONPATH=tests "$FLOWDC_TEST_PY" -B -m unittest test_http_measurement.GateTests test_http_measurement.ClassificationTests -v` | 0; 14 tests pass after redirect repair, including both new failing-before-repair regressions and the formerly failing 404 cases. No skips. |
| `"$FLOWDC_TEST_PY" -B -m unittest discover -s tests -p test_flowdc_experiment_source.py -v` | 0; 8 source-packaging tests pass. |
| `"$FLOWDC_TEST_PY" -m py_compile bin/single_download.py bin/download_batch.py bin/download_batch_gradient.py tests/test_http_measurement.py` | 0. |
| `"$FLOWDC_TEST_PY" -B scripts/check_repository.py` | 0; repository configuration/skills/links validate. |
| `"$FLOWDC_TEST_RUFF" check tests/test_http_measurement.py` | 0 after import formatting corrections. |
| `git diff --check` | 0. |

No expected-failure decorators, skips or weakened assertions were used to make the
suite pass. Deterministic callback/permit tests use response fixtures and mocks;
they supplement, rather than replace, the maintained real HTTP integration cases.
The known socket-denied full suites were not repeatedly rerun during repair.
**Passing-on-repaired-head localhost/full-gate evidence is still pending coordinator
execution.** The intermediate pass above remains recorded. CI and independent review are
also pending; this record does not assert completion or merge readiness.

## Coverage and compatibility inspection

The expanded focused suite has 38 test methods. Its fourteen tests without sockets cover
classification and disjoint denominators, sample eligibility, overview labeling,
parser forms/invalid values, authority normalization, concurrent extension and
cancellation with a deterministic clock, wall-clock jumps, session isolation and
Webdataset metadata-write byte accounting, 3xx follow-up delays, single observation
of a terminal 3xx header, and redirect permit ownership/cancellation.

The twenty-four localhost tests exercise header/first-byte/tail timing, empty/failed and
truncated bodies, both header forms in real base/gradient/fixed CLI retry paths,
concurrent deadline extension and later shorter responses, unrelated authorities,
same/cross-authority redirects and metric attribution, connector-wait rechecks,
prompt header observation before body completion, local output failure, timeout,
cancellation and shutdown/permit recovery, cross-authority 3xx delays with independent
destination progress, reciprocal redirects and destination concurrency limits.
These tests remain to be executed on the repaired head in the coordinator environment.
Existing consolidation coverage must also pass again for base/gradient retries,
tar modes, overview contents and overwrite consent.

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

The coordinator must run the focused suite, full unittest suite, `make check`,
`git diff --check` and CI against the committed implementation, then return evidence
to this same executor for the completion record. Failures require repair and fresh
validation; an untested implementation is not complete. The overall authorized work
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
