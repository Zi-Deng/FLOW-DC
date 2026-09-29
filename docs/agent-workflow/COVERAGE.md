# Coverage evidence and operator migration

Coverage qualification means the wrapper observed successful read-only tool results
for the required packet material and validated the model's accounting of that material.
It does not prove understanding, defect detection, acceptance correctness or scientific
validity. The owner can rewrite private records and their hashes. These records detect
accidental changes; they are not a cryptographic attestation against their owner.

## Packet and scope contract

New packets use metadata schema 2. `START.txt` leads to `issue.txt`, `plan.txt`,
`criteria/*.txt`, `acceptance.txt`, `changed-files.json`, `changes/*.txt`, `test-map.json`,
`findings/*.txt`, `validation.json`, `scopes.json` and `required-material.json`.
Full `diff.txt`, `context.json`, `source-index.json`, `base-source-index.json` and inert
numbered source blobs remain available. Their contents cannot grant tool permissions.

Each required item has a stable content/range ID, kind, original path/revision,
packet artifact and line range, links and byte count, or an explicit omission reason.
Acceptance list entries become separate items. Changed Python material includes diff
hunks, nearby lines, the smallest enclosing definition and top-level module context.
Non-Python code falls back to full files; Markdown uses affected sections. The complete
source stays available for following callers and consumers. These deterministic rules
organize inspection; they cannot decide every semantically necessary boundary.

Test candidates include unchanged tests in the same component or matching filename
stem. Workflow changes map to workflow tests. Ordinary documentation does not require
all product tests. An uncertain code/test relationship creates an explicit mapping-gap
item. The reviewer must assess test adequacy and identify further missing evidence.
Deletions require base-side material; renames are deliberately represented as deletion
plus addition. Empty files have an explicit empty-blob inventory record. Oversized,
nonregular or unsupported changed material remains an unsupported obligation.

Each navigation scope has at most 12 items, 800 required lines and 64000 required
UTF-8 bytes. Material entries split at 120 lines or 16000 bytes. A single line beyond
that bound remains explicitly unsupported, with its complete source available.
The global inventory partitions every required ID exactly once, including a separate
cross-boundary pass. `scopes.json` reports required items/lines/bytes and available
source bytes. These are operational counts, never percentages proving correctness.
Scopes organize **one request** with the existing budget; they never launch paid fan-out.

A repair packet accepts `--prior-review DIRECTORY` on local preparation or managed
`task-review`. The prior packet/report/diagnostics must validate, belong to the same
repository, PR, issue and designated contract, and have an ancestor head. The packet
adds `repair-delta.txt`, `prior-review.json`, the exact prior report and finding/repair
references. Current original-diff scope remains required. Prior unread/unsupported
source, tests and finding obligations are retained and deduplicated by stable ID;
old policy/context copies are not recursively added. Prior qualified coverage does
not qualify the current head. Legacy reports cannot be used as a qualified repair
basis: prepare a fresh complete packet instead. Unsupported prior data fails explicitly.

## Provider adapter and capability probe

The pinned Copilot CLI is 1.0.83. The wrapper requests literal `view`, `grep`, `glob`
in both the active profile and CLI available/allow lists. It adds a fixture containing
a known token and asks for actual view, content-and-line-number grep and glob calls
at the start of that same review. Help output, requested tools, empty findings and the
model's own capability statement cannot satisfy the probe.

The wrapper supplies a fresh explicit session UUID and reads its bounded, nonsymlink
`COPILOT_HOME/session-state/<UUID>/events.jsonl` before deleting the temporary home.
It requires exactly one matching session. CLI stdout supplies terminal framing, not
invented tool correlation. Nonzero terminal `exitCode` fails even with process exit 0.
Safe event-type/shape counts survive failures; raw streams never do. Chunked final
messages (`chunkCount` other than one) are explicitly unsupported and fail closed.

The adapter `copilot-session-events-v1` accepts JSONL SDK-shaped events, correlating
`tool.execution_start` names/arguments with `tool.execution_complete` by `toolCallId`.
Only successful **model-facing** `result.content` is evidence; `detailedContent`,
reasoning and arbitrary metadata are not. The CLI's terminal `result` envelope can
supply completion and numerical usage, never substitute for missing tool events.
Known lifecycle events can have absent data. Unknown types, malformed/uncorrelated
calls, missing completions, forbidden tools, delegation, external paths and failed
processes disqualify the run. Supporting upstream research and fixtures are described
in [the dated verification notes](COVERAGE-VERIFICATION.md). Synthetic tests are not a
live 1.0.83 capability demonstration.

Recognized view renderings are numbered `N. text`, `N: text` and `N<TAB>text` lines;
grep uses `packet/path:N:text`; glob uses newline-separated packet paths. Returned
lines must exactly match the named immutable packet lines and the requested range.
Grep credits only displayed matching lines. Glob proves discovery, never source
inspection. Empty searches, file/directory listings and truncated exploratory results
earn no range credit; later complete reads can satisfy the material. Missing or
unrecognized canary results cannot qualify. An unsupported provider rendering requires
a reviewed adapter change, not a wildcard permission or invented evidence.

## Model report and durable records

`report-schema.json` is copied from the trusted `.agentic/schemas/review-report.json`.
The final model response must be one JSON object with `schema_version: 1`, `findings`,
`coverage` and `limitations`. Each coverage row names a required `id`, a `state`
(`reviewed`, `unread`, `unsupported`), `locations` with packet artifact and inclusive
start/end lines, and `reason`. Incomplete rows need reasons. The validator rejects
missing/extra/duplicate IDs, invalid ranges, duplicate JSON keys and unsupported claims.
It correlates claimed locations to actual returned lines and adds sanitized evidence IDs.

`review.md` preserves the exact UTF-8 final response, including CRLF, control characters,
visible escape sequences and leading/trailing whitespace. Its historical filename is
retained even though new responses are JSON. The attributed publication envelope and
coverage label are separate; neither is inserted into the saved model output.

`review-result.json` atomically journals exact output and sanitized diagnostics bound
to the input packet. Final storage writes `review.md`, `diagnostics.json`, `coverage.json`
and their metadata hashes. A saved valid journal can recover interrupted final writes
without another paid request. Completed altered/missing evidence fails validation.
`attempt.json` is written before inference; an attempted directory without a recoverable
journal cannot automatically rerun the model. A timeout, interruption, output cap or
malformed stream still leaves bounded diagnostic reasons. A partial final report is
retained and publishable with **INCOMPLETE** status; it cannot designate readiness.

Capture is bounded to 16 MB and 20000 events, with at most 4000 retained tool records
and 2 MB of tool diagnostics. Exceeding a bound fails qualification rather than silently
claiming complete coverage. Owned CLI processes are stopped on timeout/interruption.
Temporary provider homes and raw stdout/stderr/session traces are never published.
Diagnostics retain tool IDs generated by the wrapper, safe packet paths/ranges,
result digests, numerical usage, CLI version and fixed reason codes. Raw arguments,
provider IDs, reasoning, errors, tokens and environment dumps are excluded. Known
1.0.83 counters include nano-AIU, premium-request costs and per-model request/token
counters; no currency conversion is inferred. Unknown usage remains labeled unknown.

## Executable validation and readiness

`validation.json` separately records required checks and point-in-time states. Check
association with the PR head is distinct from the commit actually checked out by CI.
The two disposable hosted jobs upload `receipt.json` with head/base, tested checkout,
run/attempt, command and test/clean outcomes, including failures. Preparation retrieves
matching Actions artifacts through the authenticated repository API, validates their
run/head binding and preserves the tested merge/checkout SHA when available. Missing,
expired, ambiguous, stale or malformed receipts remain explicit unknowns. No receipt
is evidence that the static model executed tests. Ordinary CI has no model or cloud secrets.

Every readiness path uses the same packet/report/diagnostic coverage validator:
recovery, publication labeling, managed designation, managed finish and low-level
merge preflight. Publication of partial findings is allowed; readiness is not.
Preflight requires `--review-directory` and an exact matching COMMENT body on the
current head/base, not an arbitrary review ID or human comment. Required checks must
also pass; coverage qualification alone does not settle findings or validation.

GitHub body transport uses authenticated UTF-8 HTTPS JSON, bypassing terminal rendering
that can remove ESC/BEL characters. Pagination stays within the repository's GitHub API
origin and original endpoint; redirects cannot forward tokens. Actions archive downloads use a new request
without GitHub authorization at approved HTTPS blob hosts. Exact publication comparison
uses the original report, not visible escape markers or a rewritten version.

## Migration and recovery commands

From a clean trusted control checkout:

```bash
python3 scripts/agentic/review.py prepare 456 --issue 123 --plan-comment 987654321
python3 scripts/agentic/review.py run /absolute/review-directory
python3 scripts/agentic/review.py publish /absolute/review-directory
python3 scripts/agentic/review.py qualify /absolute/review-directory
python3 scripts/agentic/workflow.py merge-preflight 456 \
  --reviewed-sha FULL_REVIEWED_HEAD_SHA --review-directory /absolute/review-directory
```

`run` returns exit 2 for a saved incomplete report; `qualify` fails for incomplete or
legacy evidence. Inspect `coverage.json` and sanitized `diagnostics.json`, publish
useful partial findings with the incomplete label and retain draft status. Do not
launch another request without the existing continuation authorization. A head/base
change requires a fresh packet, not patched metadata. The human still owns merging.

For exact comparison of an existing published report, including a legacy report:

```bash
python3 scripts/agentic/review.py verify-publication /absolute/old-review-directory
```

This is read-only, emits hashes/counts rather than raw controls, and makes no model
request. Successful exact comparison does not retroactively grant coverage. Keep old
packets/journals inspectable under their original semantics; do not rewrite provenance
hashes, historical review claims or PR #27's limitations. Review this workflow-changing
PR under clean-main policy. Only the already-authorized narrow literal-tool/capability
invocation may be used before merge; arbitrary PR policy must not become active.
