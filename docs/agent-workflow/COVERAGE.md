# Coverage evidence and operator migration

Coverage qualification means the wrapper observed successful read-only tool results
for the required packet material and validated the model's accounting of that material.
It does not prove understanding, defect detection, acceptance correctness or scientific
validity. The owner can rewrite private records and their hashes. These records detect
accidental changes; they are not a cryptographic attestation against their owner.

## Packet and scope contract

New packets use metadata schema 3. `START.txt` leads to `issue.txt`, `plan.txt`,
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

## Provider adapter and capability probe

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

A successful glob with blank output records `glob_no_discovery`; nonblank output
without recognized packet paths records `glob_unrecognized_or_outside_packet`.
These bounded diagnostics preserve tool success and do not retain unknown paths or
provider text. Unsupported rendering and outside-packet output cannot always be
distinguished, so neither is credited. Recognized paths are deduplicated and earn
discovery credit only; the capability fixture still requires its actual path.

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
journals the assessment hash. Both records use schema 3. A pending capture can recover
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
new reviews. Current readiness and managed designation require schema 3 metadata/result
and schema 2 evidence/report. Existing schema-1 records retain their older meaning too.
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
hashes, historical review claims or PR #27's limitations. Review this workflow-changing
PR under clean-main policy. Only the already-authorized narrow literal-tool/capability
invocation may be used before merge; arbitrary PR policy must not become active.

## Explicit bounded batches (metadata schema 4)

Single-request schema-3 records keep their existing semantics. A batch is an explicit
opt-in before inference; a deterministic preview makes no model call. From the clean
trusted control checkout, prepare the original packet, then inspect:

```bash
python3 scripts/agentic/review.py batch-preview /absolute/review-directory
```

The preview partitions every original required ID, omissions included, into the
existing bounded component scopes and a separate integration assignment. Links and
source/test mapping supply navigation context. Each component owns its assigned IDs;
reading another component's material does not transfer credit. Overlapping source
ranges can support distinct obligations, but each parent ID counts once. Every unit
retains the complete original packet. Assignments are not proof of coherent reasoning
or exhaustive semantic coverage; the integration pass must assess interactions and
test adequacy across those assignments.

Execution needs **all five explicit bounds**: `--batch-requests`, `--batch-credits`,
`--batch-seconds`, `--batch-unit-credits`, and `--batch-unit-seconds`. Supply them to
`review.py batch-run DIRECTORY`; there are no paid batch defaults. Requests count
fresh Copilot CLI invocations, including failed or uncertain attempts, not internal
provider API calls. Per-unit allocations are reserved durably before dispatch and
cannot exceed remaining aggregate allocations. A deliberately insufficient allocation
stops incomplete; the wrapper never silently increases it to finish a checklist.

`batch.json` freezes repository/PR/issue/plan, exact head/base, every parent artifact,
inventory digest, policy, assignments and budget. `batch-state.json` persists the
start, deadline and ordered reservations. Each `units/UNIT` holds its own packet,
assignment, exact capture/report, sanitized diagnostics and recovery journal. Unit
reports remain schema 3 and cannot qualify the parent independently. For integration,
exact component reports become additional required material; changed, missing or
incomplete dependencies prevent completion. The parent inventory is never replaced
by a reduced checklist.

Each invocation uses the existing isolated Copilot path and must establish actual
`view`, `grep`, `glob` capability. The aggregate credits only assigned positive
inspections supported by immutable ranges and validated telemetry. Missing, masked,
truncated, malformed, omitted and unsupported material stays incomplete. A complete
parent source count cannot replace required integration report reads.

`review.py batch-recover DIRECTORY` recovers saved captures without inference.
`review.py batch-resume DIRECTORY` additionally permits never-started eligible units
under the **original** budget and deadline. Attempted units without recoverable reports
are not retried. Incomplete prior units, unknown usage, exhausted limits, stale
snapshots/contracts and ambiguous state stop further requests. Clock rollback before
the persisted start is refused. A process interruption after reservation but before
inference conservatively consumes that reservation; inspect it rather than retrying.

The retained `totalNanoAiu` counter uses the SDK's `1e9` nano-unit scaling for AI-credit
accounting; other counters remain retained but cannot substitute for it. See GitHub's
[usage metric definitions](https://docs.github.com/en/copilot/how-tos/copilot-sdk/features/usage-and-billing)
and [CLI unit reference](https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-command-reference).
No currency conversion is inferred. Provider limits are **soft**: an in-flight request
can overshoot. Known actual usage is retained, unknown or over-allocation usage blocks
readiness and further spending. Reservation bounds are not a hard monetary cap.

The parent `review.md` is explicitly attributed aggregate bookkeeping, not a model
response. Publication retains each exact unit report in a separate attributed COMMENT,
then publishes the aggregate status with report hashes. Reports are never concatenated
and presented as one response. Readiness validates all exact published unit reports
and the aggregate on the current head/base. Partial output remains publishable as
incomplete. Each COMMENT retains the existing 60000-byte transport ceiling; an
oversized report fails publication without truncating or editing its bytes.

The managed equivalent is `workflow.py task-review ISSUE --batch` for preview, adding
`--execute` and the five bounds for the initial run. Use `--batch --execute
--batch-resume` for eligible continuation, or `--batch --execute` to recover only after
an attempted run. `--publish` publishes retained evidence. The complete batch is one
explicitly budgeted managed round; starting a fresh batch still obeys the existing
round-continuation authorization. `--prior-review` can retain validated batch findings,
exact unit reports and original uncovered obligations without inheriting readiness.

All qualification, managed designation, hosted qualification, preflight and finish
use the same version-aware gate. Hosted automation keeps its single-request default;
it does not silently fan out. Installer payload discovery includes the new module and
tests without a path-manifest change. Synthetic tests cover budgeting, isolation,
recovery, report binding and publication. No live multi-invocation completion has been
validated by those tests. A separate finite trial budget and explicit trust in the
invocation code are required before applying unmerged batch policy to its own PR.

### Session warning diagnostics (offline repair)

The pinned [GitHub SDK WarningEvent/WarningData source](https://github.com/github/copilot-sdk/blob/4dc774c91aff609c338563aadc699d5c2dc596f7/nodejs/src/generated/session-events.ts#L2240-L2285)
uses an open string `warningType`, a string `message`, optional string `url`, and
optional `remediation`. The downloaded source SHA-256 is
`8bc4ec9dea0577c5ae6f6244ce1e30b51fc5855a8ba15ccc109f22d3feba2cb6`.
Its examples (`subscription`, `policy`, `mcp`) do not establish benign semantics.
All `session.warning` events therefore still fail the existing unsupported-event
gates in either stdout or session telemetry. No category is allowlisted for readiness.

Future captures add an optional `telemetry.warnings` summary only when warnings
occur. Separate stdout/session counters retain those three literal example labels;
all other nonempty string categories become `other`, and absent/invalid categories
become `missing_or_invalid`. Fixed counters describe field presence/types and extra
fields. They do not certify schema validity or harmlessness. No warning messages,
URLs, remediation contents, arbitrary category names or extra field names are saved.
Existing stream/event/diagnostic bounds and isolation checks still apply. Historical
summaries remain accepted without this optional field and are never rewritten.

This is synthetic-test-backed diagnostic collection, not live compatibility or
completion evidence. The earlier event-name digest identifies `session.warning`
but cannot recover its historical category or payload. A coordinator-run diagnostic
under finite approved execution bounds must gather fresh sanitized evidence before
any further compatibility decision; `other` may still require source investigation.
The original malformed report, unsupported-event failures and 0/211 aggregate stay
incomplete. Fresh head/base review and independent validation remain required.

### Assigned inspection navigation

New batch plan version 2 expands test-map context to transitive closure, independent
of mapping order. Saved version-1 plans retain their original single-pass semantics
for validation; they are not silently upgraded or credited with extra inspection.

New unit assignments include deterministic `inspection_suggestions` for each
assigned readable inventory entry (including integration report obligations).
`view_range` uses 1-based inclusive start/end positions, not start/count. If the
required end is blank, the suggestion extends through the next nonblank line when
available. At EOF it also suggests a blank-line grep pattern and the needed line
interval. Only actual numbered `path:line:text` matches support those lines; a
search request or empty result proves nothing. Read the other required context with
view. Omitted material stays explicitly unavailable.

These suggestions neither change the required inventory nor bypass exact-byte
validation. The historical omitted-final-blank-line result remains unsupported.
Whitespace is not stripped, missing text is not reconstructed, and suggestions do
not establish inspection or understanding. Tests use synthetic returned output
against real local packet text; live completion remains a separate review gate.
