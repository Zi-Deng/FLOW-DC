# Coverage evidence and operator migration

Coverage qualification means the wrapper observed successful read-only tool results
for the required packet material and validated the model's accounting of that material.
It does not prove understanding, defect detection, acceptance correctness or scientific
validity. The owner can rewrite private records and their hashes. These records detect
accidental changes; they are not a cryptographic attestation against their owner.

## Packet and scope contract

New packets use metadata schema 5 with immutable provider policy. Schema 4 is reserved
for separate batch work and explicitly unsupported. `START.txt` leads to `issue.txt`, `plan.txt`,
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
not qualify the current head. Validated schema-2 historical packets may carry their
original assessment and unread material into a repair; their old qualification cannot establish current readiness.
Schema-1 packets without coverage require a fresh complete packet. Unsupported prior
data fails explicitly.

## Provider adapters and capability probe

Claude native stream JSON is handled separately from Copilot session events. Its
model-facing successful Read/Grep/Glob results normalize to shared read/search/list
evidence; UI metadata does not earn credit. Numbered native Read uses tab or colon
separators with exact source matching. Grep must return content and line numbers; Glob
proves discovery only. A unique successful terminal result supplies exact report text;
assistant fragments are never concatenated. Session/model mismatches, malformed events,
unknown rendering, delegation and forbidden tools remain incomplete. Synthetic fixtures
and static binary inspection do not establish live capability. Claude remains blocked
before inference: see [PROVIDERS.md](PROVIDERS.md).

The following Copilot-specific meanings remain unchanged:

The pinned Copilot CLI is 1.0.83, defined with its archive digest in
`scripts/agentic/copilot_policy.py`. Installer, invocation and current assessment share
that pin. Changing it requires verified adapter compatibility; unknown versions fail
closed. The wrapper requests literal `view`, `grep`, `glob`
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

The adapter `copilot-session-events-v2` accepts JSONL SDK-shaped events, correlating
`tool.execution_start` names/arguments with `tool.execution_complete` by `toolCallId`.
Only successful **model-facing** `result.content` is evidence; `detailedContent`,
reasoning and arbitrary metadata are not. The CLI's terminal `result` envelope can
supply completion and numerical usage, never substitute for missing tool events.
Known lifecycle events can have absent data. Unknown types, malformed/uncorrelated
calls, missing completions, forbidden tools, delegation, external paths and failed
processes disqualify the run. Supporting upstream research and fixtures are described
in [the dated verification notes](COVERAGE-VERIFICATION.md). Synthetic tests are not a
live 1.0.83 capability demonstration.

Recognized unnumbered view results must match the whole artifact or an unambiguous
contiguous slice anchored to the requested range. Matching allows omission of exactly
one final LF or complete CRLF separator, preserving all internal bytes and every
character of the final source line. Empty output and omission of a final blank line's
separator cannot establish that line. Partial prefixes, internal newline conversion
and arbitrary whitespace/Unicode normalization receive no credit. Actual retained
result hashes establish the omitted-final-LF form; CRLF elision has synthetic evidence.
The original provider arguments were not retained, so that comparison proves returned
content, not an independently observed request range.

Raw source matching takes precedence over numbered rendering. Raw content matching
multiple slices, a different range, or only a source prefix cannot be reinterpreted
as numbered output to credit another source line. Otherwise numbered `N. text`,
`N: text` and `N<TAB>text` lines remain supported;
grep uses `packet/path:N:text`; glob uses newline-separated packet paths. Returned
lines must exactly match the named immutable packet lines and the requested range.
Grep credits only displayed matching lines. Glob proves discovery, never source
inspection. Empty searches, file/directory listings and truncated exploratory results
earn no range credit; later complete reads can satisfy the material. Missing or
unrecognized canary results cannot qualify. An unsupported provider rendering requires
a reviewed adapter change, not a wildcard permission or invented evidence.

Root `subagent.selected` is supported only for `independent-reviewer` with exactly
`view`, `grep`, `glob`; null/all-tools, other agents and top-level `agentId` are refused.
The SDK uses top-level `agentId` for a delegated instance, absent on root events.
A well-formed root `system.message` is recognized, but its content is never retained.
These supported shapes come from primary schema research; the first live run's two
unknown event identities were not retained and have not been retrospectively identified.
New unknown names retain only up to 64 SHA-256 digest/count entries plus an overflow
count per stream. They still fail qualification; arbitrary names/payloads are not saved.

## Model report and durable records

`report-schema.json` is copied from the trusted `.agentic/schemas/review-report.json`.
The final model response is one JSON object with `schema_version: 2`, `inventory_sha256`,
`findings`, `reviewed`, `incomplete` and `limitations`. Copy the exact digest provided in
`inventory-sha256.txt`. `reviewed` is a unique list of positively inspected required IDs.
`incomplete` groups specific limitations as `{ids: [...], state: "unread" | "unsupported",
reason: "..."}`. General limitations appear once; repetitive unread rows are unnecessary.

The wrapper materializes **every** immutable inventory item in `coverage.json`, with
its original location, state and observed evidence. Omitted claims become unread;
extra/duplicate/conflicting IDs or a wrong inventory digest fail the contract. A
positive claim receives credit only for actual returned evidence covering its complete
immutable range. Even observed reads do not override an explicit unread claim. Source
omissions remain unsupported. No percentage or reduced checklist can qualify a review.

Bare JSON or one complete outer lowercase `json` code fence is accepted. No surrounding
prose, second fence or JSON substring is extracted. Parsing never rewrites report bytes.

`review.md` preserves the exact UTF-8 final response, including CRLF, control characters,
visible escape sequences and leading/trailing whitespace. Its historical filename is
retained even though new responses are JSON. The attributed publication envelope and
coverage label are separate; neither is inserted into the saved model output.

`review-capture.json` atomically saves exact output and sanitized diagnostics bound
to the input packet **before assessment reads packet files**. `review-result.json` then
journals the assessment hash. Both new records use schema 5. A pending capture can recover
a transient assessment/storage failure after the original packet is restored; it never
authorizes another model call. Strict UTF-8/IO failures produce fixed diagnostic reasons
without lossy replacement decoding or raw error text. Final storage writes `review.md`,
`diagnostics.json`, `coverage.json`
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

Metadata/result schema 2 and coverage schema 1 are now historical. The frozen
`review_coverage_v1.py` reproduces their original assessment hashes, recovery and
publication envelopes, including formerly malformed fenced reports. It cannot qualify
new reviews. Schema-3 assessment and telemetry are frozen in `review_coverage_v2.py`
and `review_telemetry_v2.py`, preserving original hashes and publication envelopes.
Current readiness and managed designation require schema-5 metadata/result and schema-2
evidence/report. Existing schema-1 records retain their older meaning too. Schema 4 is
reserved and refused, never reinterpreted.
Never rewrite a historical journal or retrofit new claims. A new packet/authorized
invocation is needed for current evidence. `verify-publication` remains byte-exact.

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
hashes, historical review claims or PR #27's limitations. The migration-only review
exception for issue #33 is documented in [PROVIDERS.md](PROVIDERS.md); it introduces no
generic bypass and establishes no normal finish readiness. PR #32 remains separate.
