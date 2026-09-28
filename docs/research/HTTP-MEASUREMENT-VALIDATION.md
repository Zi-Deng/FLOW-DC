# HTTP measurement validation — issue #20

## Initial regression checkpoint (September 28, 2026)

Production source is still the base commit
`ca52044fa2d4447ce6dd288b5a7b571b9db75c6d`. This checkpoint adds the
[research contract](MANUSCRIPT-READINESS.md), README link and
[`tests/test_http_measurement.py`](../../tests/test_http_measurement.py).
No HTTP repair or changed measurement semantics are claimed yet. The approved
[plan](https://github.com/Zi-Deng/FLOW-DC/issues/20#issuecomment-5876154148)
requires reproduction before repair and an early draft checkpoint.

### Environment and commands

The executor used an existing environment read-only; nothing was installed.
Python is 3.12.3, aiohttp 3.13.3, polars 1.37.1, psutil 7.2.1, tqdm 4.67.1,
PyYAML 6.0.3 and Ruff 0.16.7. The default system Python lacks aiohttp, and
`python` is absent from its PATH. Commands below use these explicit paths
(the table abbreviates the paths as shell variables):

```bash
FLOWDC_TEST_PY=/mnt/storage/github/FLOW-DC-control-js2-worktrees/issue-12-reusable-experiment-runner/.venv-agentic/bin/python
FLOWDC_TEST_RUFF=/mnt/storage/github/FLOW-DC-control-js2-worktrees/issue-12-reusable-experiment-runner/.venv-agentic/bin/ruff
```

| Command from the issue #20 worktree | Exit / observed result |
| --- | --- |
| `python3 -B -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print("localhost bind available:", s.getsockname()); s.close()'` | 1; socket creation denied with `PermissionError: [Errno 1] Operation not permitted`. |
| `"$FLOWDC_TEST_PY" -B -m unittest discover -s tests -p test_http_measurement.py -v` | 1; 9 tests, 2 failing subtests (404 classification in base and gradient), 7 socket-setup errors. Overload checks pass for 408/429/503 in both variants. No skips. |
| `"$FLOWDC_TEST_PY" -B -m unittest discover -s tests -v` | 1; 322 tests, 2 failing subtests and 9 socket-related errors, no skips. The consolidation class fails setup, so its retry/tar/overwrite checks did not execute. |
| `make check PYTHON="$FLOWDC_TEST_PY" RUFF="$FLOWDC_TEST_RUFF"` | 2; `test-flowdc` repeats the 322-test result above and exits 1; Make stops before `check-agentic`. |
| `make check-agentic PYTHON="$FLOWDC_TEST_PY" RUFF="$FLOWDC_TEST_RUFF"` | 0; run separately because the full gate stopped early. Scoped lint, format checks, 141 workflow tests and repository configuration/link checks pass. |
| `"$FLOWDC_TEST_PY" -m py_compile tests/test_http_measurement.py` | 0. |
| `"$FLOWDC_TEST_RUFF" check tests/test_http_measurement.py` | 0 after correcting import layout and using `datetime.UTC`; the initial lint run exited 1. |
| `git diff --check` | 0 at checkpoint preparation. |

The nine full-suite errors are seven new HTTP fixture setup failures, the existing
consolidation class's setup failure and the existing experiment guest's real
downloader fixture. All fail at socket creation/binding. Other locally executable
checks pass; the required full gate does not. CI has not run for this checkpoint.

### What failed on the unchanged base

`ClassificationTests.test_404_is_neither_useful_success_nor_overload` exercises the
real base and inherited gradient metrics without network mocks. Each records one
404 with zero bytes and no latency sample. Both preserve `total == 1`, no overload,
zero bytes and zero samples, but report `n_success == 1`; the required assertion
`n_success == 0` fails. This is actual failing-on-base evidence for classification.

The localhost fixtures could not bind in this executor. Their setup errors are
**environment failures**, not reproduced timing or Retry-After defects. The tests
remain enabled and assert required behavior; they have not been marked expected
failures or skipped to make the suite pass. No passing-on-head evidence exists yet.

The installed aiohttp 3.13.3 `ClientResponse.read` was inspected with `inspect.getsource`:
it awaits the complete `self.content.read()` before sending the response-chunk
trace callback. The unchanged FLOW-DC helper calls `response.read()`, and the trace
sets `ttfb` at that callback. This supports the suspected mechanism but does not
replace localhost timing calibration. The helper currently converts Retry-After
using only `float`, while the batch retry loop sleeps only its configured backoff.

### Fixture design and limits

- An aiohttp origin binds only `127.0.0.1` on an ephemeral port. Its independent
  monotonic events record request receipt, header preparation and body writes.
  Temporary manifests, configs and downloaded output are removed by test cleanup.
- Header, first-body and tail fixtures delay the corresponding stage by 400 ms.
  The tail assertion compares the client's first-byte observation with half the
  observed server tail gap. Header/first-body checks use a 350-ms lower bound.
  These are application scheduling observations; overloaded test hosts can still
  invalidate timing tolerances. They are not packet-level measurements.
- Empty and failed responses must not fabricate first-byte samples. New timestamp
  fields, redirect semantics and local-output eligibility still need implementation
  and additional assertions after the initial reproduction run.
- Retry fixtures execute both real downloader CLIs with PAARC on and off,
  `max_retry_attempts=2`, one worker and zero configured retry backoff. They assert
  exactly two server requests, one useful final success and no retry more than
  100 ms before the advertised deadline. Numeric headers specify two seconds;
  HTTP dates use the actual rounded UTC deadline. Each subprocess has a 15-second
  test bound and is killed/reaped if it fails to finish.
- Concurrent extensions/waiters, invalid/nonfinite headers, authority isolation,
  redirects, timeout/cancellation, permit recovery and shutdown tests remain to be
  added. Initial fixtures alone do not satisfy AC5.

### Required continuation

The coordinator needs to run the exact focused suite against this checkpoint in
its existing localhost-capable environment, retaining the unchanged production
source, and record the actual assertion failures. The executor must not expand
its permissions or substitute mocks for that required evidence. Resume the same
executor session after the draft PR and evidence are recorded.

Then repair shared timing/classification and authority admission, add the remaining
coverage, inspect packaging, label report semantics and run focused tests, the full
unittest suite, `make check` and `git diff --check` again. CI and independent review
belong to the coordinator. Downloader retry/tar/overwrite regression evidence is
still required. Overall authorized work stops by **20:31 UTC September 28, 2026**.

No cluster, external dataset, cloud operation, runtime/dependency installation,
performance campaign, manuscript edit or scientific speedup result is included.

### Technical references

The [aiohttp streaming API](https://docs.aiohttp.org/en/stable/streams.html) describes
partial body reads and the [trace reference](https://docs.aiohttp.org/en/stable/tracing_reference.html)
describes lifecycle callbacks. Those moving documentation pages currently identify
3.14.3; installed 3.13.3 source and controlled tests determine local behavior.
[RFC 9110 §10.2.3](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after)
defines the numeric delay and HTTP-date forms of Retry-After. These references
inform the repair design; they do not attest that the current source implements it.
